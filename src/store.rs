use std::{
    collections::HashMap,
    path::PathBuf, time::SystemTime
};

use tokio::sync::mpsc::{Sender, Receiver};

use crate::types::RedisType;

pub const CMD_BUFFER: usize = 1024;

pub enum CommandResponse {
    RdbFile(PathBuf),
    ClientId(usize),
    Set { key: String, value: RedisType },
    Get(Option<RedisType>),
    Keys(RedisType),
}

pub enum StoreCommand {
    InitClient(Sender<CommandResponse>),
    InitReplica(Sender<CommandResponse>),
    Set { key: String, value: RedisType },
    SetEx { key: String, value: RedisType, until: SystemTime },
    Get { id: usize, key: String },
    AllKeys(usize),
}

enum StoreValue {
    Permanent(RedisType),
    Expirable { value: RedisType, until: SystemTime },
}

#[derive(Default)]
pub struct Store {
    data: HashMap<String, StoreValue>,
}

impl Store {
    pub fn write(&mut self, key: &str, value: RedisType, maybe_until: Option<SystemTime>) {
        let store_val = match maybe_until {
            Some(until) => StoreValue::Expirable { value, until },
            None        => StoreValue::Permanent(value),
        };

        self.data.insert(key.to_string(), store_val);
    }

    pub fn read(&mut self, key: &str) -> Option<RedisType> {
        if let Some(val) = self.data.get(key) {
            match val {
                StoreValue::Permanent(value) => Some(value.clone()),
                StoreValue::Expirable { value, until } => {
                    if SystemTime::now() < *until {
                        Some(value.clone())
                    } else {
                        self.data.remove(key);
                        None
                    }
                }
            }
        } else {
            None
        }
    }
}

pub async fn store_loop(mut store: Store, mut rx: Receiver<StoreCommand>) {
    // Naive implementation. Clients and replicas might
    // close their connection, which will result on the channel
    // being dropped. We should use a different structure and
    // sends should not blindly be accepted as OK
    let mut clients: Vec<Sender<CommandResponse>> = Vec::new();
    let mut replicas: Vec<Sender<CommandResponse>> = Vec::new();

    loop {
        if let Some(cmd) = rx.recv().await {
            match cmd {
                StoreCommand::InitClient(tx) => {
                    let id = clients.len();
                    clients.push(tx.clone());
                    tx.send(CommandResponse::ClientId(id)).await.unwrap();
                }
                StoreCommand::InitReplica(tx) => replicas.push(tx),
                StoreCommand::Set { key, value } => {
                    store.write(&key, value, None);
                    // Todo: implement the replica
                }
                StoreCommand::SetEx { key, value, until } => {
                    store.write(&key, value, Some(until));
                    // Todo: implement the replica
                }
                StoreCommand::Get { id, key } => {
                    clients[id].send(CommandResponse::Get(store.read(&key))).await.unwrap()
                }
                StoreCommand::AllKeys(id) => {
                    let keys = store.data
                        .keys()
                        .map(|s| RedisType::from(s.as_str()))
                        .collect::<Vec<_>>();
                    clients[id].send(CommandResponse::Keys(RedisType::Array(keys))).await.unwrap()
                }
            }
        }
    }
}
