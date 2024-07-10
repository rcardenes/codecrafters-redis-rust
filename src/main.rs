use std::env::{self, Args};
use std::string::ToString;
use anyhow::{bail, Error, Result};
use itertools::Itertools;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use redis_starter_rust::{get_string, TcpReader};
use redis_starter_rust::config::Configuration;
use redis_starter_rust::rdb::Rdb;
use redis_starter_rust::replica::replica_setup;
use redis_starter_rust::server::{self, RedisServer};

type Command = Vec<String>;

async fn read_bulk_string(stream: &mut TcpReader) -> Result<Option<String>> {
    fn format_error<'a>(chr: char) -> String {
        format!("Protocol error: expected '$', got '{}'", chr)
    }

    if let Some(size_string) = get_string(stream).await? {
        if size_string.len() == 0 {
            bail!(format_error(' '))
        } else if !size_string.starts_with("$") {
            bail!(format_error(size_string.chars().next().unwrap()))
        } else {
            let string_size = size_string[1..].parse::<usize>()
                .map_err(|_| Error::msg("Protocol error: invalid bulk length"))?;
            let mut buf: Vec<u8> = vec![0; string_size + 2];
            stream.read_exact(buf.as_mut_slice()).await?;
            let bulk_string = String::from_utf8_lossy(&buf[..string_size]).to_string();
            Ok(Some(bulk_string))
        }
    } else {
        Ok(None)
    }
}

async fn read_command(stream: &mut TcpReader) -> Result<Option<Command>> {
    if let Some(text) = get_string(stream).await? {
        if text.starts_with("*") {
            let chunks = text[1..].parse::<usize>()
                .map_err(|_| Error::msg("Protocol error: invalid multibulk length"))?;
            let mut cmd = Command::new();
            for _ in 0..chunks {
                if let Some(cmd_part) = read_bulk_string(stream).await? {
                    cmd.push(cmd_part);
                } else {
                    return Ok(None)
                }
            }
            Ok(Some(cmd))
        } else {
            Ok(Some(text.split_whitespace().map(|s| s.to_string()).collect()))
        }
    } else {
        Ok(None)
    }
}

async fn send_error_message(stream: &mut TcpReader, msg: &str) {
    let msg = format!("-ERR {}\r\n", msg);
    let _ = stream.write(msg.as_bytes()).await;
}

async fn handle_events(mut server: RedisServer, stream: TcpStream) {
    let addr = stream.local_addr().unwrap();
    eprintln!("Handling events from {addr}");
    let mut stream = BufReader::new(stream);
    loop {
        match read_command(&mut stream).await {
            Ok(cnt) => match cnt {
                Some(cmd) => {
                    let strs = cmd.iter().map(|s| s.as_str()).collect::<Vec<_>>();
                    if let Err(error) = server.dispatch(&mut stream, strs.as_slice()).await {
                        send_error_message(&mut stream, &error.to_string()).await;
                    }
                }
                None => {}
            },
            Err(error) => {
                send_error_message(&mut stream, &error.to_string()).await;
                break;
            }
        }
    }
}

fn parse_arguments(mut args: Args) -> Result<Vec<(String, String)>> {
    let mut pairs = vec![];
    let _ = args.next(); // Discard the 1st argument (binary path)
    while let Some(arg) = args.next() {
        if arg.starts_with("--") {
            if arg == "--replicaof" {
                if let Some(address) = args.next() {
                    let split_address = address.split_whitespace().collect_vec();

                    if split_address.len() != 2 {
                        bail!("--replicaof: Expect a single argument '<host> <port>'")
                    }

                    pairs.push((String::from("replicaof"),
                                format!("{}:{}", split_address[0], split_address[1])));
                } else {
                    bail!("--replicaof: Expected an argument")
                }
            } else if let Some(value) = args.next() {
                pairs.push(((&arg[2..]).to_string(), value));
            } else {
                bail!("No value for option {}", arg)
            }
        }
    }
    Ok(pairs)
}

#[tokio::main]
async fn main() -> Result<()> {
    server::init_static_data();
    let mut config = Configuration::default();
    config.bulk_update(parse_arguments(env::args())?)?;

    let db_path = config.get_database_path();

    let listener = TcpListener::bind(config.get_binding_address()?).await?;
    let mut server = RedisServer::new(config.clone());

    // Don't read from the Rdb file if this is a replica
    if !config.is_replica() {
        if let Ok(db_path) = db_path {
            if let Ok(mut rdb) = Rdb::open(db_path.as_path()).await {
                while let Some(entry) = rdb.read_next_entry().await? {
                    server.write(&entry.key, entry.value, entry.expires).await;
                }
            } else {
                eprintln!("Couldn't open database at {}", db_path.to_string_lossy());
            }
        }
    }

    if let Some(address) = config.get("replicaof") {
        tokio::spawn(async move {
            replica_setup(address, &config).await;
        });
    }

    loop {
        let (stream, addr) = listener.accept().await?;
        eprintln!("Accepted connection from: {}", addr);
        let server = server.clone();
        tokio::spawn(async move {
            handle_events(server, stream).await;
        });
    };
}
