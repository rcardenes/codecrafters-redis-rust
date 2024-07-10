use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use anyhow::Result;

use crate::config::Configuration;

async fn handshake(stream: &mut TcpStream) -> Result<()> {
    stream.write(b"*1\r\n$4\r\nPING\r\n").await?;

    Ok(())
}

pub async fn replica_loop(address: String, _config: &Configuration) {
    let mut stream = match TcpStream::connect(address.clone()).await {
        Ok(stream) => stream,
        Err(error) => {
            eprintln!("Replica: error when connecting to {address:?}");
            eprintln!("Replica: {error}");
            return
        }
    };

    eprintln!("Replica: starting");

    if let Err(error) = handshake(&mut stream).await {
        eprintln!("Replica: error when trying to handshake");
        eprintln!("Replica: {error}");
        return
    }

    eprintln!("Replica: stopping");
}
