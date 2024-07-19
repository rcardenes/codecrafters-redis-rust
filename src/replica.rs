use std::time::{Duration, SystemTime};

use anyhow::{bail, Error, Result};
use sha1::{Sha1, Digest};

use tokio::{
    io::BufReader,
    net::TcpStream,
    sync::mpsc::Sender,
    time::timeout,
};

use crate::{
    config::Configuration,
    io::*,
    store::StoreCommand,
    types::RedisType,
};

#[derive(Clone)]
pub struct ReplicaInfo {
    hasher: Sha1,
    offset: usize,
}

impl ReplicaInfo {
    pub fn new() -> Self {
        ReplicaInfo {
            hasher: Sha1::new(),
            offset: 0,
        }
    }

    pub fn digest_string(&self) -> String {
        let cl = self.hasher.clone();
        let digest = cl.finalize();

        format!("{digest:x}")
    }
    
    pub fn offset(&self) -> usize {
        self.offset
    }
}

static TIMEOUT: Duration = Duration::from_millis(1000);

async fn ping(stream: &mut TcpReader) -> Result<()> {
    let cmd = RedisType::Array(vec![RedisType::from("PING")]);
    cmd.write(stream).await?;

    match timeout(TIMEOUT, get_string(stream)).await? {
        Ok(Some(s)) => if s != "+PONG" { bail!("expected PONG") },
        _ => bail!("Unknown error!")
    }

    Ok(())
}

async fn send_replconf(stream: &mut TcpReader, config: &Configuration) -> Result<()> {
    let cmd = RedisType::Array(vec![
                    RedisType::from("REPLCONF"),
                    RedisType::from("listening-port"),
                    RedisType::from(config.get("port").unwrap()),
    ]);

    cmd.write(stream).await?;
    match timeout(TIMEOUT, get_string(stream)).await {
        Ok(Ok(Some(s))) => if s != "+OK" { bail !("expected OK at first REPLCONF") },
        Ok(Err(_)) => eprintln!("Error when reading the answer for the first REPLCONF"),
        Err(_) => eprintln!("Timeout when waiting for an answer for the first REPLCONF"),
        _ => {},
    }


    let cmd = RedisType::Array(vec![
                    RedisType::from("REPLCONF"),
                    RedisType::from("capa"),
                    RedisType::from("psync2"),
    ]);

    cmd.write(stream).await?;
    match timeout(TIMEOUT, get_string(stream)).await {
        Ok(Ok(Some(s))) => if s != "+OK" { bail !("expected OK at second REPLCONF") },
        Ok(Err(_)) => eprintln!("Error when reading the answer for the second REPLCONF"),
        Err(_) => eprintln!("Timeout when waiting for an answer for the second REPLCONF"),
        _ => {},
    }

    Ok(())
}

async fn send_psync(stream: &mut TcpReader) -> Result<()> {
    let cmd = RedisType::Array(vec![
                    RedisType::from("PSYNC"),
                    RedisType::from("?"),
                    RedisType::from("-1"),
    ]);

    cmd.write(stream).await?;
    match timeout(TIMEOUT, get_string(stream)).await {
        Ok(Ok(Some(s))) => {
            if !s.starts_with("+FULLRESYNC") {
                bail !("expected FULLRESYNC at initial PSYNC. Got: {s:?}")
            }
            else {
                // Read the transmitted RDB file
                let _rdb = read_bulk_bytes(stream).await?;
                eprintln!("PSYNC -> {s:?}");
            }
        }
        Ok(Err(_)) => eprintln!("Error when reading the answer PSYNC"),
        Err(_) => eprintln!("Timeout when waiting for an answer for PSYNC"),
        _ => {},
    }

    Ok(())
}

async fn handshake(stream: &mut TcpReader, config: &Configuration) -> Result<()> {
    if let Err(error) = ping(stream).await {
        eprintln!("Replica handshake error at PING: {error}");
        bail!("Error during handshake");
    }
    if let Err(error) = send_replconf(stream, config).await {
        eprintln!("Replica handshake error at REPLCONF: {error}");
        bail!("Error during handshake");
    }
    if let Err(error) = send_psync(stream).await {
        eprintln!("Replica handshake error at PSYNC: {error}");
        bail!("Error during handshake");
    }

    Ok(())
}

async fn handle_set(stream: &mut TcpReader, store_tx: &Sender<StoreCommand>, args: &[&str]) -> Result<()> {
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
            let key = String::from(args[0]);
            let value = RedisType::String(args[1].into());
            store_tx.send(
                if let Some(dur) = duration {
                    let until = now.checked_add(dur).unwrap();

                    StoreCommand::SetEx { key, value, until }
                } else {
                    StoreCommand::Set { key, value }
                }).await.unwrap();

            write_ok(stream).await
        }
        _ => bail!("wrong number of arguments for 'set' command")
    }
}

pub async fn replica_loop(address: String, config: Configuration, store_tx: Sender<StoreCommand>) {
    let stream = match TcpStream::connect(address.clone()).await {
        Ok(stream) => stream,
        Err(error) => {
            eprintln!("Replica setup: error when connecting to {address:?}");
            eprintln!("Replica setup: {error}");
            return
        }
    };

    let mut stream = BufReader::new(stream);

    if let Err(_) = handshake(&mut stream, &config).await {
        eprintln!("Replica setup: error when trying to handshake");
        return
    }

    loop {
        match read_command(&mut stream).await {
            Ok(cnt) => match cnt {
                Some(cmd) => {
                    let strs = cmd.iter().map(|s| s.as_str()).collect::<Vec<_>>();
                    if strs[0].to_ascii_lowercase() == "set" {
                        let _ = handle_set(&mut stream, &store_tx, &strs.as_slice()[1..]).await;
                    }
                }
                None => {},
            },
            Err(error) => {
                eprintln!("Replica: {error}");
            }
        }
    }
}
