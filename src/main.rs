use std::collections::HashMap;
use std::env;
use std::env::Args;
use std::string::ToString;
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;
use anyhow::{bail, Error, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::time::Duration;

use redis_starter_rust::config::Configuration;
use redis_starter_rust::io::*;
use redis_starter_rust::rdb::Rdb;
use redis_starter_rust::TcpReader;
use redis_starter_rust::types::RedisType;

const HELP_LINES: [&str; 5] = [
    "CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "GET <pattern>",
    "    Return parameters matching the glob-like <pattern> and their values.",
    "HELP",
    "    Prints this help."
];

#[derive(Clone)]
struct RedisServer {
    store: Arc<RwLock<HashMap<String, RedisType>>>,
    expiry: Arc<RwLock<HashMap<String, SystemTime>>>,
    config: Arc<RwLock<Configuration>>,
}

impl RedisServer {
    fn new(config: Configuration) -> Self {
        Self {
            store: Arc::new(RwLock::new(HashMap::new())),
            expiry: Arc::new(RwLock::new(HashMap::new())),
            config: Arc::new(RwLock::new(config)),
        }
    }

    async fn write(&mut self, key: &str, value: RedisType, expire_at: Option<SystemTime>) {
        let key = String::from(key);
        {
            let mut lock = self.expiry.write().await;
            if let Some(instant) = expire_at {
                lock.insert(key.clone(), instant);
            } else {
                lock.remove(&key);
            }
        }
        self.store.write().await.insert(key, value);
    }

    async fn try_expire(&self, key: &str) -> bool {
        let mut remove = false;

        {
            if let Some(expiry) = self.expiry.read().await.get(key) {
                if SystemTime::now() >= *expiry {
                    remove = true;
                }
            }
        }
        if remove {
            self.store.write().await.remove(key);
            self.expiry.write().await.remove(key);
            true
        } else {
            false
        }
    }

    async fn get(&self, key: &str) -> Option<RedisType> {
        if self.try_expire(key).await {
            // Shortcut return, as we just removed the key
            return None;
        }

        self.store.read().await.get(key).map(|ref_val| ref_val.clone())
    }

    async fn handle_set(&mut self, stream: &mut TcpReader, args: &[&str]) -> Result<()> {
        let now = SystemTime::now();
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
                let expiry = if let Some(dur) = duration { now.checked_add(dur) } else { None};
                self.write(&name, RedisType::String(args[1].into()), expiry).await;

                write_ok(stream).await
            }
            _ => bail!("wrong number of arguments for 'set' command")
        }
    }

    async fn handle_get(&self, stream: &mut TcpReader, args: &[&str]) -> Result<()> {
        match args.len() {
            1 => {
                match self.get(args[0]).await {
                    Some(RedisType::String(string)) => {
                        write_string(stream, &string).await
                    }
                    Some(RedisType::Int(number)) => {
                        write_integer(stream, number).await
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

    async fn handle_config_get(&self, stream: &mut TcpReader, args: &[&str]) -> Result<()> {
        match args.len() {
            0 => {
                bail!("wrong number of arguments for 'config|get' command")
            }
            _ => {
                let mut values = vec![];
                for arg in args.iter().map(|arg| arg.to_lowercase()) {
                    if let Some(value) = self.config.read().await.get(&arg) {
                        values.push(RedisType::String(arg));
                        values.push(RedisType::String(value.clone()));
                    }
                }

                RedisType::Array(values).write(stream).await
            }
        }
    }

    async fn handle_config_help(&self, stream: &mut TcpReader, args: &[&str]) -> Result<()> {
        match args.len() {
            0 => {
                write_array_size(stream, HELP_LINES.len()).await?;
                for arg in HELP_LINES {
                    write_simple_string(stream, arg).await?;
                }
            }
            _ => {
                bail!("wrong number of arguments for 'config|help' command")
            }
        }
        Ok(())
    }

    async fn handle_config(&self, stream: &mut TcpReader, args: &[&str]) -> Result<()> {
        if args.len() == 0 {
            bail!("wrong number of arguments for 'config' command")
        }
        match args[0].to_lowercase().as_str() {
            "get" => self.handle_config_get(stream, &args[1..]).await?,
            "help" => self.handle_config_help(stream, &args[1..]).await?,
            _ => {
                bail!("unknown subcommand '{}'. Try CONFIG HELP", args[0])
            }
        }
        Ok(())
    }

    async fn dispatch(&mut self, stream: &mut TcpReader, cmd_vec: &[&str]) -> Result<()> {
        let name = cmd_vec[0];
        let args = &cmd_vec[1..];
        match name.to_ascii_lowercase().as_str() {
            "ping" => handle_ping(stream, args).await?,
            "echo" => handle_echo(stream, args).await?,
            "hello" => handle_hello(stream, args).await?,
            "set" => self.handle_set(stream, args).await?,
            "get" => self.handle_get(stream, args).await?,
            "config" => self.handle_config(stream, args).await?,
            _ => {
                let args = cmd_vec[1..]
                    .iter()
                    .map(|s| format!("'{}'", *s))
                    .collect::<Vec<_>>()
                    .join(" ");
                bail!("unknown command '{}', with args beginning with: {}", name, args)
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

fn parse_arguments(mut args: Args) -> Result<Vec<(String, String)>> {
    let mut pairs = vec![];
    let _ = args.next(); // Discard the 1st argument (binary path)
    while let Some(arg) = args.next() {
        if arg.starts_with("--") {
            if let Some(value) = args.next() {
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
    init_static_data();
    let mut config = Configuration::default();
    config.bulk_update(parse_arguments(env::args())?)?;
    let db_path = config.get_database_path();

    let listener = TcpListener::bind(config.get_binding_address()?).await?;
    let mut server = RedisServer::new(config);

    if let Ok(db_path) = db_path {
        if let Ok(mut rdb) = Rdb::open(db_path.as_path()).await {
            while let Some(entry) = rdb.read_next_entry().await? {
                server.write(&entry.key, entry.value, entry.expires).await;
            }
        } else {
            eprintln!("Couldn't open database at {}", db_path.to_string_lossy());
        }
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