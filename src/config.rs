use anyhow::{bail, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};

use crate::{
    info,
    replica::ReplicaInfo
};

pub const CMD_BUFFER: usize = 32;

const ACCEPTABLE_KEYS: &[&str] = &[
    "bind-source-addr",
    "dbfilename",
    "dir",
    "port",
    "replicaof",
    "master_replid",
];

const DEFAULT_CONFIG: &[(&str, &str)] = &[
    ("bind-source-addr", "127.0.0.1"), // "" in the original, but I decided to translate it already
    ("dbfilename", "dump.rdb"),
    ("dir", "."),
    ("port", "6379"),
];

pub enum ConfigCommand {
    Get { tx: oneshot::Sender<Vec<String>>, items: Vec<String> },
    AllInfo(oneshot::Sender<String>),
    InfoOn { tx: oneshot::Sender<Vec<String>>, sections: Vec<String> },
    ReplicaDigest(oneshot::Sender<String>),
}

#[derive(Clone)]
pub struct Configuration {
    store: HashMap<String, String>,
    replica: ReplicaInfo,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            store: DEFAULT_CONFIG.iter().map(|&(k, v)| (k.to_string(), v.to_string())).collect(),
            replica: ReplicaInfo::new(),
        }
    }
}
impl Configuration {
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
            replica: ReplicaInfo::new(),
        }
    }

    pub fn update(&mut self, key: String, value: String) -> Result<Option<String>> {
        if ACCEPTABLE_KEYS.contains(&key.as_str()) {
            let current = self.store.remove(key.as_str());
            self.store.insert(key, value);
            Ok(current)
        } else {
            bail!("Attempting to set unknown config entry: '{}'", key)
        }
    }

    pub fn bulk_update(&mut self, pairs: Vec<(String, String)>) -> Result<()> {
        for (key, value) in pairs {
            self.update(key, value)?;
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.store.get(key).map(|value| value.to_string())
    }

    pub fn get_binding_address(&self) -> Result<String> {
        if let (Some(addr), Some(port)) = (self.get("bind-source-addr"), self.get("port")) {
            Ok(format!("{addr}:{port}"))
        } else {
            bail!("Something is wrong with the configuration for the binding address. Missing default data")
        }
    }

    pub fn get_database_path(&self) -> Result<PathBuf> {
        let mut data_dir = PathBuf::from(self.get("dir").unwrap());
        data_dir.push(PathBuf::from(self.get("dbfilename").unwrap()).as_path());

        Ok(data_dir)
    }

    pub fn as_hash(&self) -> HashMap<String, String> {
        self.store.clone()
    }

    pub fn is_replica(&self) -> bool {
        self.get("replicaof").is_some()
    }

    pub fn replica_info(&self) -> &ReplicaInfo {
        &self.replica
    }
}

pub async fn config_loop(config: Configuration, mut rx: mpsc::Receiver<ConfigCommand>) {
    loop {
        if let Some(cmd) = rx.recv().await {
            match cmd {
                ConfigCommand::Get { tx, items } => {
                    let values = items.into_iter()
                        .map(|arg| config.get(&arg).and_then(|val| Some(vec![arg, val])))
                        .flatten()
                        .flatten()
                        .collect();
                    tx.send(values).unwrap();
                }
                ConfigCommand::AllInfo(tx) => {
                    tx.send(info::all_info(&config)).unwrap();
                }
                ConfigCommand::InfoOn { tx, sections } => {
                    tx.send(sections.into_iter()
                                    .map(|sec| info::info_on(&config, sec.as_str()))
                                    .collect()).unwrap();
                }
                ConfigCommand::ReplicaDigest(tx) => {
                    tx.send(config.replica_info().digest_string()).unwrap();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{Configuration, DEFAULT_CONFIG};

    #[test]
    fn test_default_keys() {
        let config = Configuration::default(); // Loaded with defaults

        for &(key, value) in DEFAULT_CONFIG {
            assert_eq!(config.get(key), Some(String::from(value)));
        }
    }

    #[test]
    fn test_update_existing_key() {
        let mut config = Configuration::default();
        let key = String::from("dbfilename");
        let prev = config.get(&key).unwrap();
        let new = String::from("new_stuff");

        match config.update(key.clone(), new.clone()) {
            Ok(Some(value)) => assert_eq!(value, prev),
            Err(error) => panic!("{}", error),
            _ => panic!("Unexpected value"),
        }
        assert_eq!(config.get(&key), Some(new));
    }

    #[test]
    fn test_update_wrong_key() {
        let mut config = Configuration::default();

        assert!(config.update(String::from("foo"), String::from("bar")).is_err());
    }
}
