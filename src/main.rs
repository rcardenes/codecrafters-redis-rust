use std::collections::HashMap;
use std::string::ToString;
use std::sync::{Arc, OnceLock};
use anyhow::{bail, Error, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::time::{Duration, sleep};

type TcpReader = BufReader<TcpStream>;

#[derive(Debug, Clone)]
enum RedisType {
    String(String),
    Int(i64),
    Array(Vec<RedisType>),
}

#[derive(Clone)]
struct RedisServer {
    store: Arc<RwLock<HashMap<String, RedisType>>>,
}

impl RedisServer {
    fn new() -> Self {
        Self {
            store: Arc::new(RwLock::new(HashMap::new()))
        }
    }

    async fn spawn_expire(&mut self, key: String, how_long: Duration) {
        tokio::spawn({
            let store = Arc::clone(&self.store);
            async move {
                sleep(how_long).await;
                store.write().await.remove(&key);
            }
        });
    }

    async fn handle_set(&mut self, stream: &mut TcpReader, args: &[&str]) -> Result<()> {
        match args.len() {
            2 | 4 => {
                let duration = if args.len() == 4 {
                    if args[2].to_ascii_lowercase() == "px" {
                        Some(Duration::from_millis(args[3]
                            .parse::<u64>()
                            .map_err(|_| Error::msg("value is not an integer or out of range"))?
                        ))
                    } else {
                        bail!("syntax error")
                    }
                } else {
                    None
                };
                let name = String::from(args[0]);

                self.store
                    .write()
                    .await
                    .insert(name.clone(), RedisType::String(args[1].into()));
                if let Some(dur) = duration {
                    self.spawn_expire(name, dur).await;
                }

                write_ok(stream).await
            }
            _ => bail!("wrong number of arguments for 'set' command")
        }
    }

    async fn handle_get(&mut self, stream: &mut TcpReader, args: &[&str]) -> Result<()> {
        match args.len() {
            1 => {
                match self.store.read().await.get(args[0].into()) {
                    Some(RedisType::String(string)) => {
                        write_string(stream, string).await
                    }
                    Some(RedisType::Int(number)) => {
                        write_integer(stream, *number).await
                    }
                    Some(RedisType::Array(_)) => {
                        write_wrongtype(stream).await
                    }
                    None => write_nil(stream).await
                }
            },
            _ => bail!("wrong number of arguments for 'get' command")
        }
    }

    async fn dispatch(&mut self, stream: &mut TcpReader, cmd_vec: &[&str]) -> Result<()> {
        let name = cmd_vec[0];
        let args = &cmd_vec[1..];
        match name.to_ascii_lowercase().as_str() {
            "ping" => { handle_ping(stream, args).await? }
            "echo" => { handle_echo(stream, args).await? }
            "hello" => { handle_hello(stream, args).await? }
            "set" => { self.handle_set(stream, args).await? }
            "get" => { self.handle_get(stream, args).await? }
            _ => {
                let args = cmd_vec[1..]
                    .iter()
                    .map(|s| format!("'{}'", *s))
                    .collect::<Vec<_>>()
                    .join(" ");
                let error_msg = format!("unknown command '{}', with args beginning with: {}", name, args);
                bail!(error_msg)
            }
        }
        Ok(())
    }
}

async fn write_ok(stream: &mut TcpReader) -> Result<()> {
    stream.write(b"+OK\r\n").await.map(|_| Ok(()))?
}

async fn write_nil(stream: &mut TcpReader) -> Result<()> {
    stream.write(b"$-1\r\n").await.map(|_| Ok(()))?
}

async fn write_wrongtype(stream: &mut TcpReader) -> Result<()> {
    stream.write(b"-WRONGTYPE Operation against a key holding the wrong kind of value")
        .await.map(|_| Ok(()))?
}

async fn write_string(stream: &mut TcpReader, string: &str) -> Result<()> {
    let output = format!("${}\r\n{}\r\n", string.len(), string);
    stream.write(output.as_bytes()).await.map(|_| Ok(()))?
}

async fn write_integer(stream: &mut TcpReader, number: i64) -> Result<()> {
    let output = format!(":{number}\r\n");
    stream.write(output.as_bytes()).await.map(|_| Ok(()))?
}

async fn write_array_size(stream: &mut TcpReader, array: &[RedisType]) -> Result<()> {
    let size = format!("*{}\r\n", array.len());
    stream.write(size.as_bytes()).await.map(|_| Ok(()))?
}

impl RedisType {
    async fn write(&self, stream: &mut TcpReader) -> Result<()> {
        match &self {
            RedisType::String(string) => {
                write_string(stream, string).await?
            }
            RedisType::Int(number) => {
                write_integer(stream, *number).await?
            }
            RedisType::Array(array) => {
                write_array_size(stream, array).await?;
                let mut stack = vec![array.iter()];
                while let Some(last) = stack.last_mut() {
                    if let Some(element) = last.next() {
                        match element {
                            RedisType::Array(array) => {
                                write_array_size(stream, array).await?;
                                stack.push(array.iter())
                            },
                            // Duplicated code because async functions can't be recursive
                            // as-is. There ways to circumvent this, but they are a pain
                            // in the ass or require the use of crates not provided by the
                            // project (and CodeCrafters don't support modifying Cargo.toml
                            RedisType::String(string) => {
                                write_string(stream, string).await?
                            },
                            RedisType::Int(number) => {
                                write_integer(stream, *number).await?
                            }
                        }
                    } else {
                        stack.pop();
                    }
                }
            }
        }
        Ok(())
    }
}

static HELLO_INFO: OnceLock<RedisType> = OnceLock::new();

/// Respond to a PING command
async fn handle_ping(stream: &mut TcpReader, args: &[&str]) -> Result<()> {
    match args.len() {
        0 => stream.write(b"+PONG\r\n").await.map(|_| Ok(()))?,
        1 => write_string(stream, args[0]).await,
        _ => bail!("wrong number of arguments for 'ping' command")
    }
}

/// Respond to an ECHO command
async fn handle_echo(stream: &mut TcpReader, args: &[&str]) -> Result<()> {
    match args.len() {
        1 => write_string(stream, args[0]).await,
        _ => bail!("wrong number of arguments for 'echo' command")
    }
}

async fn handle_hello(stream: &mut TcpReader, args: &[&str]) -> Result<()> {
    match args.len() {
        0 => {
            HELLO_INFO.get().unwrap().write(stream).await
        }
        // This should be a NOPROTO, we'll deal with that later
        _ => bail!("wrong number of arguments for 'hello' command")
    }
}

async fn get_string(stream: &mut TcpReader) -> Result<Option<String>> {
    let mut buf = String::new();
    let read_chars = stream.read_line(&mut buf).await?;

    if read_chars == 0 {
        Ok(None)
    } else {
        Ok(Some((&buf[0..read_chars -2]).to_string()))
    }
}

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

fn init_static_data() {
    HELLO_INFO.set(RedisType::Array(vec![
        RedisType::String("server".into()),
        RedisType::String("codecrafters-redis".into()),
        RedisType::String("version".into()),
        RedisType::String("0.2".into()),
        RedisType::String("proto".into()),
        RedisType::Int(2),
        RedisType::String("mode".into()),
        RedisType::String("standalone".into()),
        RedisType::String("role".into()),
        RedisType::String("master".into()),
        RedisType::String("modules".into()),
        RedisType::Array(vec![]),
    ])).unwrap();
}

const BINDING_ADDRESS: &str = "127.0.0.1:6379";

#[tokio::main]
async fn main() -> Result<()> {
    init_static_data();

    let listener = TcpListener::bind(BINDING_ADDRESS).await?;
    let server = RedisServer::new();

    loop {
        let (stream, addr) = listener.accept().await?;
        eprintln!("Accepted connection from: {}", addr);
        let server = server.clone();
        tokio::spawn(async move {
            handle_events(server, stream).await;
        });
    };
}

