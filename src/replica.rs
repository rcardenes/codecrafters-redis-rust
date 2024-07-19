use std::time::Duration;

use anyhow::{bail, Result};
use sha1::{Sha1, Digest};

use tokio::net::TcpStream;
use tokio::io::BufReader;
use tokio::time::timeout;

use crate::{TcpReader, get_string};
use crate::config::Configuration;
use crate::types::RedisType;

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
        Ok(Ok(Some(s))) => if !s.starts_with("+FULLRESYNC") { bail !("expected FULLRESYNC at initial PSYNC. Got: {s:?}") },
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

pub async fn replica_setup(address: String, config: &Configuration) {
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
}
