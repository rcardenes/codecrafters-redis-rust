use anyhow::Result;
use tokio::io::AsyncWriteExt;
use crate::TcpReader;

pub async fn write_ok(stream: &mut TcpReader) -> Result<()> {
    stream.write(b"+OK\r\n").await.map(|_| Ok(()))?
}

pub async fn write_nil(stream: &mut TcpReader) -> Result<()> {
    stream.write(b"$-1\r\n").await.map(|_| Ok(()))?
}

pub async fn write_wrongtype(stream: &mut TcpReader) -> Result<()> {
    stream.write(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
        .await.map(|_| Ok(()))?
}

pub async fn write_simple_error(stream: &mut TcpReader, message: &str) -> Result<()> {
    let output = format!("-{message}\r\n");
    stream.write(output.as_bytes()).await.map(|_| Ok(()))?
}

pub async fn write_string(stream: &mut TcpReader, string: &str) -> Result<()> {
    let output = format!("${}\r\n{}\r\n", string.len(), string);
    stream.write(output.as_bytes()).await.map(|_| Ok(()))?
}

pub async fn write_simple_string(stream: &mut TcpReader, string: &str) -> Result<()> {
    let output = format!("+{string}\r\n");
    stream.write(output.as_bytes()).await.map(|_| Ok(()))?
}

pub async fn write_integer(stream: &mut TcpReader, number: i64) -> Result<()> {
    let output = format!(":{number}\r\n");
    stream.write(output.as_bytes()).await.map(|_| Ok(()))?
}

pub async fn write_array_size(stream: &mut TcpReader, size: usize) -> Result<()> {
    let size = format!("*{size}\r\n",);
    stream.write(size.as_bytes()).await.map(|_| Ok(()))?
}
