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
    common_cli_rep::handle_set,
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

struct Replica {
    stream: TcpReader,
    store_tx: Sender<StoreCommand>,
}

impl Replica {
    async fn ping(&mut self) -> Result<()> {
        let cmd = RedisType::Array(vec![RedisType::from("PING")]);
        cmd.write(&mut self.stream).await?;

        match timeout(TIMEOUT, get_string(&mut self.stream)).await? {
            Ok(Some(s)) => if s != "+PONG" { bail!("expected PONG") },
            _ => bail!("Unknown error!")
        }

        Ok(())
    }

    async fn send_replconf(&mut self, config: &Configuration) -> Result<()> {
        let cmd = RedisType::Array(vec![
            RedisType::from("REPLCONF"),
            RedisType::from("listening-port"),
            RedisType::from(config.get("port").unwrap()),
        ]);

        cmd.write(&mut self.stream).await?;
        match timeout(TIMEOUT, get_string(&mut self.stream)).await {
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

        cmd.write(&mut self.stream).await?;
        match timeout(TIMEOUT, get_string(&mut self.stream)).await {
            Ok(Ok(Some(s))) => if s != "+OK" { bail !("expected OK at second REPLCONF") },
            Ok(Err(_)) => eprintln!("Error when reading the answer for the second REPLCONF"),
            Err(_) => eprintln!("Timeout when waiting for an answer for the second REPLCONF"),
            _ => {},
        }

        Ok(())
    }

    async fn send_psync(&mut self) -> Result<()> {
        let cmd = RedisType::Array(vec![
            RedisType::from("PSYNC"),
            RedisType::from("?"),
            RedisType::from("-1"),
        ]);

        cmd.write(&mut self.stream).await?;
        match timeout(TIMEOUT, get_string(&mut self.stream)).await {
            Ok(Ok(Some(s))) => {
                if !s.starts_with("+FULLRESYNC") {
                    bail !("expected FULLRESYNC at initial PSYNC. Got: {s:?}")
                }
                else {
                    // Read the transmitted RDB file
                    let _rdb = read_bulk_bytes(&mut self.stream).await?;
                    eprintln!("PSYNC -> {s:?}");
                }
            }
            Ok(Err(_)) => eprintln!("Error when reading the answer PSYNC"),
            Err(_) => eprintln!("Timeout when waiting for an answer for PSYNC"),
            _ => {},
        }

        Ok(())
    }

    async fn handshake(&mut self, config: &Configuration) -> Result<()> {
        if let Err(error) = self.ping().await {
            eprintln!("Replica handshake error at PING: {error}");
            bail!("Error during handshake");
        }
        if let Err(error) = self.send_replconf(config).await {
            eprintln!("Replica handshake error at REPLCONF: {error}");
            bail!("Error during handshake");
        }
        if let Err(error) = self.send_psync().await {
            eprintln!("Replica handshake error at PSYNC: {error}");
            bail!("Error during handshake");
        }

        Ok(())
    }

    async fn handle_set(&mut self, args: &[&str]) -> Result<()> {
        handle_set(&mut self.stream, &self.store_tx, args).await
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

    let mut replica = Replica {
        stream: BufReader::new(stream),
        store_tx,
    };

    if let Err(_) = replica.handshake(&config).await {
        eprintln!("Replica setup: error when trying to handshake");
        return
    }

    loop {
        match read_command(&mut replica.stream).await {
            Ok(cnt) => match cnt {
                Some(cmd) => {
                    let strs = cmd.iter().map(|s| s.as_str()).collect::<Vec<_>>();
                    if strs[0].to_ascii_lowercase() == "set" {
                        let _ = replica.handle_set(&strs.as_slice()[1..]).await;
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
