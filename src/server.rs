use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

use anyhow::{bail, Error, Result};

use itertools::Itertools;

use tokio::io::AsyncWriteExt;
use tokio::sync::{RwLock, RwLockWriteGuard};
use tokio::time::Duration;

use crate::TcpReader;
use crate::config::Configuration;
use crate::io::*;
use crate::info;
use crate::types::RedisType;

const HELP_LINES: [&str; 5] = [
    "CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "GET <pattern>",
    "    Return parameters matching the glob-like <pattern> and their values.",
    "HELP",
    "    Prints this help."
];

static HELLO_INFO: OnceLock<RedisType> = OnceLock::new();

pub fn init_static_data() {
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

/// Respond to a PING command
async fn handle_ping(stream: &mut TcpReader, args: &[&str]) -> Result<()> {
    match args.len() {
        0 => stream.write(b"+PONG\r\n").await.map(|_| Ok(()))?,
        1 => write_string(stream, args[0]).await,
        _ => bail!("wrong number of arguments for 'ping' command") }
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

#[derive(Clone)]
pub struct RedisServer {
    store: Arc<RwLock<HashMap<String, RedisType>>>,
    expiry: Arc<RwLock<HashMap<String, SystemTime>>>,
    config: Arc<RwLock<Configuration>>,
}

impl RedisServer {
    pub fn new(config: Configuration) -> Self {
        Self {
            store: Arc::new(RwLock::new(HashMap::new())),
            expiry: Arc::new(RwLock::new(HashMap::new())),
            config: Arc::new(RwLock::new(config)),
        }
    }

    pub async fn write(&mut self, key: &str, value: RedisType, expire_at: Option<SystemTime>) {
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

    async fn try_expire(&self, key: &str, expiry_hash: &mut RwLockWriteGuard<'_, HashMap<String, SystemTime>>) -> bool {
        {
            if let Some(expiry) = expiry_hash.get(key) {
                if SystemTime::now() >= *expiry {
                    if let Ok(mut guard) = self.store.try_write() {
                        guard.remove(key);
                        expiry_hash.remove(key);
                    }
                    return true
                }
            }
        }

        false
    }

    async fn get(&self, key: &str) -> Option<RedisType> {
        {
            if self.try_expire(key, &mut self.expiry.write().await).await {
                // Shortcut return, as we just removed the key
                return None;
            }
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

    async fn handle_keys(&self, stream: &mut TcpReader, args: &[&str]) -> Result<()> {
        if args.len() != 1 {
            bail!("wrong number of arguments for 'keys' command")
        }
        match args[0] {
            "*" => {
                let store = self.store.read().await;
                let mut acc = vec![];

                {
                    let mut expiry = self.expiry.write().await;
                    for key in store.keys() {
                        if !self.try_expire(key, &mut expiry).await {
                            acc.push(key);
                        }
                    }
                }

                write_array_size(stream, acc.len()).await?;
                for key in acc {
                    write_string(stream, key).await?;
                }
            }
            other => {
                if other.contains("*") {
                    bail!("general pattern matching unsupported")
                }

                let mut acc = vec![];
                if self.store.read().await.contains_key(other) {
                    acc.push(RedisType::String(other.into()));
                }
                RedisType::Array(acc).write(stream).await?;
            }
        }
        Ok(())
    }

    async fn handle_info(&self, stream: &mut TcpReader, args: &[&str]) -> Result<()> {
        let config = self.config.read().await;

        let info = if args.len() == 0 {
            info::all_info(&config) + "\r\n"
        } else {
            let section = args
                .iter()
                .map(|s| s.to_lowercase())
                .unique()
                .map(|s| info::info_on(&config, &s))
                .join("\r\n");

            if section.len() > 0 {
                section + "\r\n"
            } else {
                String::from("")
            }
        };
        write_string(stream, info.as_str()).await?;
        Ok(())
    }

    async fn handle_replconf(&self, stream: &mut TcpReader, _: &[&str]) -> Result<()> {
        // Trivial implementation. We're ignoring all the REPLCONF details for now
        write_simple_string(stream, "OK").await
    }

    async fn handle_psync(&self, stream: &mut TcpReader, args: &[&str]) -> Result<()> {
        if args != &["?", "-1"] {
            write_simple_error(stream, "ERR Unsupported PSYNC arguments").await?;
            bail!("wrong arguments for PSYNC");
        }

        let id = self.config.read().await.replica_info().digest_string();
        write_simple_string(stream, &format!("FULLRESYNC {id} 0")).await?;
        // Empty RDB transfer for the time being. The file was generated using
        // the official Redis server.
        let empty_rdb = b"REDIS0010\xfa\tredis-ver\x067.0.11\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2\xc4\xcf\x8ef\xfa\x08used-mem\xc2\xf0\xdf\x12\x00\xfa\x0erepl-stream-db\xc0\x00\xfa\x07repl-id(d784536f43b93857ad3b55d53d84a53b05dc3709\xfa\x0brepl-offset\xc0\x00\xfa\x08aof-base\xc0\x00\xff\x06\xd0\x8b\xb5\x939j`";
        write_bytes(stream, empty_rdb).await
    }

    pub async fn dispatch(&mut self, stream: &mut TcpReader, cmd_vec: &[&str]) -> Result<()> {
        let name = cmd_vec[0];
        let args = &cmd_vec[1..];
        match name.to_ascii_lowercase().as_str() {
            "ping" => handle_ping(stream, args).await?,
            "echo" => handle_echo(stream, args).await?,
            "hello" => handle_hello(stream, args).await?,
            "set" => self.handle_set(stream, args).await?,
            "get" => self.handle_get(stream, args).await?,
            "config" => self.handle_config(stream, args).await?,
            "keys" => self.handle_keys(stream, args).await?,
            "info" => self.handle_info(stream, args).await?,
            "replconf" => self.handle_replconf(stream, args).await?,
            "psync" => self.handle_psync(stream, args).await?,
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
