use anyhow::{bail, Result};
use std::collections::HashMap;

const DEFAULT_CONFIG: [(&str, &str); 4] = [
    ("bind-source-addr", "127.0.0.1"), // "" in the original, but I decided to translate it already
    ("dbfilename", "dump.rdb"),
    ("dir", "."),
    ("port", "6379"),
];

pub struct Configuration {
    store: HashMap<String, String>
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            store: DEFAULT_CONFIG.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
        }
    }
}
impl Configuration {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, key: String, value: String) -> Result<()> {
        if !self.store.contains_key(key.as_str()) {
            bail!("Attempting to set unknown config entry: {}", key)
        }
        self.store.insert(key, value);
        Ok(())
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
}
