use std::time::{Duration, SystemTime};

use anyhow::{bail, Error, Result};

use tokio::sync::mpsc::Sender;

use crate::io::*;
use crate::store::StoreCommand;
use crate::types::RedisType;

pub async fn handle_set(stream: &mut TcpReader, store_tx: &Sender<StoreCommand>, args: &[&str]) -> Result<()> {
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

