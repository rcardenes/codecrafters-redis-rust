use anyhow::Result;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;

pub mod config;
pub mod rdb;
pub mod types;
pub mod io;
pub mod info;
pub mod server;
pub mod replica;

pub type TcpReader = BufReader<TcpStream>;

pub async fn get_string(stream: &mut TcpReader) -> Result<Option<String>> {
    let mut buf = String::new();
    let read_chars = stream.read_line(&mut buf).await?;

    if read_chars == 0 {
        Ok(None)
    } else {
        Ok(Some((&buf[0..read_chars -2]).to_string()))
    }
}
