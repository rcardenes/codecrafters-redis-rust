use std::env::{self, Args};
use std::string::ToString;
use anyhow::{bail, Result};
use itertools::Itertools;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use redis_starter_rust::client;
use redis_starter_rust::config::{config_loop, Configuration, self};
use redis_starter_rust::store::{store_loop, Store, self};
use redis_starter_rust::rdb::Rdb;
use redis_starter_rust::replica::replica_setup;

fn parse_arguments(mut args: Args) -> Result<Vec<(String, String)>> {
    let mut pairs = vec![];
    let _ = args.next(); // Discard the 1st argument (binary path)
    while let Some(arg) = args.next() {
        if arg.starts_with("--") {
            if arg == "--replicaof" {
                if let Some(address) = args.next() {
                    let split_address = address.split_whitespace().collect_vec();

                    if split_address.len() != 2 {
                        bail!("--replicaof: Expect a single argument '<host> <port>'")
                    }

                    pairs.push((String::from("replicaof"),
                                format!("{}:{}", split_address[0], split_address[1])));
                } else {
                    bail!("--replicaof: Expected an argument")
                }
            } else if let Some(value) = args.next() {
                pairs.push(((&arg[2..]).to_string(), value));
            } else {
                bail!("No value for option {}", arg)
            }
        }
    }
    Ok(pairs)
}

#[tokio::main]
async fn main() -> Result<()> {
    client::init_static_data();
    let mut config = Configuration::default();
    config.bulk_update(parse_arguments(env::args())?)?;

    let db_path = config.get_database_path();

    let listener = TcpListener::bind(config.get_binding_address()?).await?;

    let mut store = Store::default();

    // Don't read from the Rdb file if this is a replica
    if config.is_replica() {
        // Contact the master server and get the initial
        // Rdb file
        let address = config.get("replicaof").unwrap();
        replica_setup(address, &config).await;

        // TODO: Eventually we want to do this right...
        // todo!();
    } else {
        if let Ok(db_path) = db_path {
            if let Ok(mut rdb) = Rdb::open(db_path.as_path()).await {
                while let Some(entry) = rdb.read_next_entry().await? {
                    store.write(&entry.key, entry.value, entry.expires);
                }
            } else {
                eprintln!("Couldn't open database at {}", db_path.to_string_lossy());
            }
        }
    }

    // Spin the Store tas and start listening for connections
    let (store_tx, store_rx) = mpsc::channel(store::CMD_BUFFER);
    tokio::spawn(async move {
        store_loop(store, store_rx).await;
    });

    // Spin the Config task
    let (config_tx, config_rx) = mpsc::channel(config::CMD_BUFFER);
    tokio::spawn(async move {
        config_loop(config, config_rx).await;
    });

    // Start listening for connections
    loop {
        let (stream, addr) = listener.accept().await?;
        eprintln!("Accepted connection from: {}", addr);
        let stx2 = store_tx.clone();
        let ctx2 = config_tx.clone();
        tokio::spawn(async move {
            client::client_loop(stream, stx2, ctx2).await;
        });
    };
}
