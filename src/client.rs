use std::sync::OnceLock;

use anyhow::{bail, Result};
use itertools::Itertools;

use tokio::{
    sync::mpsc::{Receiver, Sender, self},
    sync::oneshot,
    io::{AsyncWriteExt, BufReader}, net::TcpStream,
};

use crate::{
    io::*,
    store::{CommandResponse, StoreCommand},
    common_cli_rep::handle_set,
    config::ConfigCommand,
    types::RedisType,
};

const CLIENT_BUFFER: usize = 32;
static HELLO_INFO: OnceLock<RedisType> = OnceLock::new();

const HELP_LINES: [&str; 5] = [
    "CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "GET <pattern>",
    "    Return parameters matching the glob-like <pattern> and their values.",
    "HELP",
    "    Prints this help."
];

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


struct Client {
    id: usize,
    stream: TcpReader,
    rx: Receiver<CommandResponse>,
    store_tx: Sender<StoreCommand>,
    config_tx: Sender<ConfigCommand>,
}

enum ClientStatus {
    Normal,
    Replica,
}

impl Client {
    async fn send_error_message(&mut self, msg: &str) {
        let msg = format!("-ERR {}\r\n", msg);
        let _ = self.stream.write(msg.as_bytes()).await;
    }

    /// Respond to a PING command
    async fn handle_ping(&mut self, args: &[&str]) -> Result<()> {
        match args.len() {
            0 => self.stream.write(b"+PONG\r\n").await.map(|_| Ok(()))?,
            1 => write_string(&mut self.stream, args[0]).await,
            _ => bail!("wrong number of arguments for 'ping' command") }
    }

    /// Respond to an ECHO command
    async fn handle_echo(&mut self, args: &[&str]) -> Result<()> {
        match args.len() {
            1 => write_string(&mut self.stream, args[0]).await,
            _ => bail!("wrong number of arguments for 'echo' command")
        }
    }

    async fn handle_hello(&mut self, args: &[&str]) -> Result<()> {
        match args.len() {
            0 => {
                HELLO_INFO.get().unwrap().write(&mut self.stream).await
            }
            // This should be a NOPROTO, we'll deal with that later
            _ => bail!("wrong number of arguments for 'hello' command")
        }
    }

    async fn handle_set(&mut self, args: &[&str]) -> Result<()> {
        handle_set(&mut self.stream, &self.store_tx, args, true).await
    }

    async fn handle_get(&mut self, args: &[&str]) -> Result<()> {
        match args.len() {
            1 => {
                let key = String::from(args[0]);
                self.store_tx.send(StoreCommand::Get { id: self.id, key }).await.unwrap();
                if let Some(CommandResponse::Get(resp)) = self.rx.recv().await {
                    match resp {
                        Some(RedisType::String(string)) => {
                            write_string(&mut self.stream, &string).await
                        }
                        Some(RedisType::Int(number)) => {
                            write_integer(&mut self.stream, number).await
                        }
                        Some(RedisType::Array(_)) => {
                            write_wrongtype(&mut self.stream).await
                        }
                        Some(RedisType::Timestamp(_)) => todo!(),
                        None => write_nil(&mut self.stream).await,
                    }
                } else {
                    bail!("internal error trying to get the value")
                }
            },
            _ => bail!("wrong number of arguments for 'get' command")
        }
    }

    async fn handle_config_get(&mut self, args: &[&str]) -> Result<()> {
         match args.len() {
             0 => {
                 bail!("wrong number of arguments for 'config|get' command")
             }
             _ => {
                 let keys = args.iter()
                     .map(|arg| arg.to_lowercase().to_string())
                     .collect();
                 let (tx, rx) = oneshot::channel();
                 self.config_tx.send(ConfigCommand::Get { tx, items: keys }).await.unwrap();
                 // There is going to be an answer, ignore the possible Error (for the time being)
                 let values = rx.await.unwrap();
                 let redis_values = values.into_iter().map(RedisType::from).collect();
                 RedisType::Array(redis_values).write(&mut self.stream).await
             }
         }
    }

    async fn handle_config_help(&mut self, args: &[&str]) -> Result<()> {
        match args.len() {
            0 => {
                write_array_size(&mut self.stream, HELP_LINES.len()).await?;
                for arg in HELP_LINES {
                    write_simple_string(&mut self.stream, arg).await?;
                }
            }
            _ => {
                bail!("wrong number of arguments for 'config|help' command")
            }
        }
        Ok(())
    }

    async fn handle_config(&mut self, args: &[&str]) -> Result<()> {
        if args.is_empty() {
            bail!("wrong number of arguments for 'config' command")
        }
        match args[0].to_lowercase().as_str() {
            "get" => self.handle_config_get(&args[1..]).await?,
            "help" => self.handle_config_help(&args[1..]).await?,
            _ => {
                bail!("unknown subcommand '{}'. Try CONFIG HELP", args[0])
            }
        }
        Ok(())
    }

    async fn handle_keys(&mut self, args: &[&str]) -> Result<()> {
        if args.len() != 1 {
            bail!("wrong number of arguments for 'keys' command")
        }
        match args[0] {
            "*" => {
                self.store_tx.send(StoreCommand::AllKeys(self.id)).await.unwrap();
                if let Some(CommandResponse::Keys(res)) = self.rx.recv().await {
                    res.write(&mut self.stream).await?;
                } else {
                    bail!("internal error obtaining the keys");
                }
            }
            other => {
                if other.contains('*') {
                    bail!("general pattern matching unsupported")
                }

                let key = String::from(other);

                let mut acc = vec![];
                let cmd = StoreCommand::Get { id: self.id, key: key.clone() };
                self.store_tx.send(cmd).await.unwrap();

                if let Some(CommandResponse::Get(Some(_))) = self.rx.recv().await {
                    acc.push(RedisType::String(key));
                }

                RedisType::Array(acc).write(&mut self.stream).await?;
            }
        }
        Ok(())
    }

    async fn handle_info(&mut self, args: &[&str]) -> Result<()> {
         let answer = if args.is_empty() {
             let (tx, rx) = oneshot::channel();
             self.config_tx.send(ConfigCommand::AllInfo(tx)).await.unwrap();
             rx.await.unwrap() + "\r\n"
             // info::all_info(&config) + "\r\n"
         } else {
             let (tx, rx) = oneshot::channel();
             let sections = args.iter().map(|s| s.to_lowercase()).unique().collect();

             self.config_tx.send(ConfigCommand::InfoOn {tx, sections}).await.unwrap();
             let answer = rx.await.unwrap();
        
             if answer.len() > 0 {
                 answer.join("") + "\r\n"
             } else {
                 String::from("")
             }
         };

         RedisType::from(answer).write(&mut self.stream).await
    }

    async fn handle_replconf(&mut self, _: &[&str]) -> Result<()> {
        // Trivial implementation. We're ignoring all the REPLCONF details for now
        write_simple_string(&mut self.stream, "OK").await
    }

    async fn handle_psync(&mut self) -> Result<Receiver<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        self.config_tx.send(ConfigCommand::ReplicaDigest(tx)).await.unwrap();
        let id = rx.await.unwrap();

        let (replica_tx, replica_rx) = mpsc::channel(16);
        self.store_tx.send(StoreCommand::InitReplica(replica_tx)).await.unwrap();
        write_simple_string(&mut self.stream, &format!("FULLRESYNC {id} 0")).await?;
        // Empty RDB transfer for the time being. The file was generated using
        // the official Redis server.
        let empty_rdb = b"REDIS0010\xff\x00\x00\x00\x00\x00\x00\x00\x00";
        write_bytes(&mut self.stream, empty_rdb).await?;

        Ok(replica_rx)
    }

    pub async fn dispatch(&mut self, cmd_vec: &[&str]) -> Result<ClientStatus> {
        let name = cmd_vec[0];
        let args = &cmd_vec[1..];
        match name.to_ascii_lowercase().as_str() {
            "ping" => self.handle_ping(args).await?,
            "echo" => self.handle_echo(args).await?,
            "hello" => self.handle_hello(args).await?,
            "set" => self.handle_set(args).await?,
            "get" => self.handle_get(args).await?,
            "config" => self.handle_config(args).await?,
            "keys" => self.handle_keys(args).await?,
            "info" => self.handle_info(args).await?,
            "replconf" => self.handle_replconf(args).await?,
            "psync" => {
                if args != &["?", "-1"] {
                    write_simple_error(&mut self.stream, "ERR Unsupported PSYNC arguments").await?;
                    bail!("wrong arguments for PSYNC");
                }

                return Ok(ClientStatus::Replica);
            }
            _ => {
                let args = cmd_vec[1..]
                    .iter()
                    .map(|s| format!("'{}'", *s))
                    .collect::<Vec<_>>()
                    .join(" ");
                bail!("Client: unknown command '{}', with args beginning with: {}", name, args)
            }
        }
        Ok(ClientStatus::Normal)
    }
}


async fn client_replica_loop(mut client: Client) {
    let mut replica_rx = client.handle_psync().await.unwrap();

    loop {
        let data = replica_rx.recv().await.unwrap();

        client.stream.write(&data).await.unwrap();
    }
}

pub async fn client_loop(stream: TcpStream, store_tx: Sender<StoreCommand>, config_tx: Sender<ConfigCommand>) {
    let addr = stream.local_addr().unwrap();
    eprintln!("Handling events from {addr}");
    let stream = BufReader::new(stream);

    // Send an endpoint to the store so that we can receive responses
    // to certain commands.
    let (client_tx, mut client_rx) = mpsc::channel::<CommandResponse>(CLIENT_BUFFER);

    eprintln!("Client: registering with the store");
    match store_tx.send(StoreCommand::InitClient(client_tx)).await {
        Err(error) => { eprintln!("Error: {error}"); return },
        _ => {}
    }

    let client_id = match client_rx.recv().await.unwrap() {
        CommandResponse::ClientId(id) => id,
        _ => panic!("Client didn't receive an ID!"),
    };
    eprintln!("Client: registered with id {client_id}");

    let mut client = Client {
        id: client_id,
        stream,
        rx: client_rx,
        store_tx,
        config_tx,
    };

    loop {
        match read_command(&mut client.stream).await {
            Ok(cnt) => match cnt {
                Some(Command { payload, .. }) => {
                    let strs = payload.iter().map(|s| s.as_str()).collect::<Vec<_>>();
                    match client.dispatch(strs.as_slice()).await {
                        Err(error) => {
                            client.send_error_message(&error.to_string()).await;
                        }
                        Ok(ClientStatus::Replica) => {
                            client_replica_loop(client).await;
                            break;
                        }
                        _ => {} // All good
                    }
                }
                None => {}
            },
            Err(error) => {
                client.send_error_message(&error.to_string()).await;
                break;
            }
        }
    }
}
