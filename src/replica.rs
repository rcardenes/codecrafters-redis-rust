use std::time::Duration;

use anyhow::{bail, Result};
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
    total_bytes: usize,
}

impl Replica {
    async fn ping(&mut self) -> Result<()> {
        let cmd = RedisType::from(vec!["PING"]);
        cmd.write(&mut self.stream).await?;

        match timeout(TIMEOUT, get_string(&mut self.stream)).await? {
            Ok(Some(RedisString { string, .. } )) => if string != "+PONG" { bail!("expected PONG") },
            _ => bail!("Unknown error!")
        }

        Ok(())
    }

    async fn handshake_replconf(&mut self, config: &Configuration) -> Result<()> {
        let port = config.get("port").unwrap();
        let cmd = RedisType::from(vec![
            "REPLCONF",
            "listening-port",
            port.as_str()
        ]);

        cmd.write(&mut self.stream).await?;
        match timeout(TIMEOUT, get_string(&mut self.stream)).await {
            Ok(Ok(Some(RedisString { string, .. }))) => {
                if string != "+OK" { bail !("expected OK at first REPLCONF") }
            }
            Ok(Err(_)) => eprintln!("Error when reading the answer for the first REPLCONF"),
            Err(_) => eprintln!("Timeout when waiting for an answer for the first REPLCONF"),
            _ => {},
        }


        let cmd = RedisType::from(vec!["REPLCONF", "capa", "psync2"]);

        cmd.write(&mut self.stream).await?;
        match timeout(TIMEOUT, get_string(&mut self.stream)).await {
            Ok(Ok(Some(RedisString { string, .. }))) => {
                if string != "+OK" { bail !("expected OK at second REPLCONF") }
            }
            Ok(Err(_)) => eprintln!("Error when reading the answer for the second REPLCONF"),
            Err(_) => eprintln!("Timeout when waiting for an answer for the second REPLCONF"),
            _ => {},
        }

        Ok(())
    }

    async fn handshake_psync(&mut self) -> Result<()> {
        let cmd = RedisType::from(vec!["PSYNC", "?", "-1",]);

        cmd.write(&mut self.stream).await?;
        match timeout(TIMEOUT, get_string(&mut self.stream)).await {
            Ok(Ok(Some(RedisString { string, .. }))) => {
                if !string.starts_with("+FULLRESYNC") {
                    bail !("expected FULLRESYNC at initial PSYNC. Got: {string:?}")
                }
                else {
                    // Read the transmitted RDB file
                    let _rdb = read_bulk_bytes(&mut self.stream).await?;
                    eprintln!("PSYNC -> {string:?}");
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
        if let Err(error) = self.handshake_replconf(config).await {
            eprintln!("Replica handshake error at REPLCONF: {error}");
            bail!("Error during handshake");
        }
        if let Err(error) = self.handshake_psync().await {
            eprintln!("Replica handshake error at PSYNC: {error}");
            bail!("Error during handshake");
        }

        Ok(())
    }

    async fn handle_set(&mut self, args: &[&str]) -> Result<()> {
        handle_set(&mut self.stream, &self.store_tx, args, false).await
    }

    async fn handle_replconf(&mut self, args: &[&str]) -> Result<()> {
        match args.len() {
            2 => {
                if args[0].to_ascii_lowercase() == "getack" {
                    if args[1] == "*" {
                        RedisType::from(vec![
                            "REPLCONF",
                            "ACK",
                            self.total_bytes.to_string().as_str()
                        ]).write(&mut self.stream)
                          .await
                    } else {
                        bail!("unsupported argument {:?} for REPLCONF GETACK", args[1]);
                    }
                } else {
                    bail!("unsupported argument {:?} for REPLCONF", args[0]);
                }
            }
            _ => bail!("wrong number of arguments for 'replconf'"),
        }
    }

    async fn dispatch(&mut self, cmd_vec: &[&str]) -> Result<()> {
        let name = cmd_vec[0];
        let args = &cmd_vec[1..];
        match name.to_ascii_lowercase().as_str() {
            "set" => self.handle_set(args).await,
            "replconf" => self.handle_replconf(args).await,
            "ping" => {
                Ok(())
            }
            _ => {
                eprintln!("Replica: got unsupported command {name:?}");
                let args = cmd_vec[1..]
                    .iter()
                    .map(|s| format!("'{}'", *s))
                    .collect::<Vec<_>>()
                    .join(" ");
                bail!("Replica: unknown command '{}', with args beginning with: {}", name, args)
            }
        }
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
        total_bytes: 0,
    };

    if let Err(_) = replica.handshake(&config).await {
        eprintln!("Replica setup: error when trying to handshake");
        return
    }

    loop {
        match read_command(&mut replica.stream).await {
            Ok(cnt) => match cnt {
                Some(Command { payload, length } ) => {
                    eprintln!("Replica: get {length} bytes with command {payload:?}");
                    let strs = payload.iter().map(|s| s.as_str()).collect::<Vec<_>>();
                    // Don't do error handling right now
                    let _ = replica.dispatch(strs.as_slice()).await;
                    replica.total_bytes += length;
                }
                None => {},
            },
            Err(error) => {
                eprintln!("Replica: {error}");
            }
        }
    }
}
