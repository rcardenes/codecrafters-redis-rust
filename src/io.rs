use anyhow::{bail, Error, Result};
use tokio::io::{AsyncReadExt, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

pub type TcpReader = BufReader<TcpStream>;
pub type Command = Vec<String>;

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

pub async fn write_bytes(stream: &mut TcpReader, bytes: &[u8]) -> Result<()> {
    let length = format!("${}\r\n", bytes.len());
    stream.write(length.as_bytes()).await?;
    stream.write(bytes).await.map(|_| Ok(()))?
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

pub async fn get_string(stream: &mut TcpReader) -> Result<Option<String>> {
    let mut buf = String::new();
    let read_chars = stream.read_line(&mut buf).await?;

    if read_chars == 0 {
        Ok(None)
    } else {
        Ok(Some((&buf[0..read_chars -2]).to_string()))
    }
}

fn format_error<'a>(chr: char) -> String {
    format!("Protocol error: expected '$', got '{}'", chr)
}

async fn read_bulk_length(stream: &mut TcpReader) -> Result<Option<usize>> {
    if let Some(size_string) = get_string(stream).await? {
        if size_string.is_empty() {
            bail!(format_error(' '))
        } else if !size_string.starts_with("$") {
            bail!(format_error(size_string.chars().next().unwrap()))
        } else {
            let string_size = size_string[1..].parse::<usize>()
                .map_err(|_| Error::msg("Protocol error: invalid bulk length"))?;
            Ok(Some(string_size))
        }
    } else {
        Ok(None)
    }
}

pub async fn read_bulk_bytes(stream: &mut TcpReader) -> Result<Option<Vec<u8>>> {
    if let Some(string_size) = read_bulk_length(stream).await? {
        let mut buf: Vec<u8> = vec![0; string_size];
        stream.read_exact(buf.as_mut_slice()).await?;
        Ok(Some(buf))
    } else {
        Ok(None)
    }
}

async fn read_bulk_string(stream: &mut TcpReader) -> Result<Option<String>> {
    if let Some(string_size) = read_bulk_length(stream).await? {
        let mut buf: Vec<u8> = vec![0; string_size + 2];
        stream.read_exact(buf.as_mut_slice()).await?;
        let bulk_string = String::from_utf8_lossy(&buf[..string_size]).to_string();
        Ok(Some(bulk_string))
    } else {
        Ok(None)
    }
}

pub async fn read_command(stream: &mut TcpReader) -> Result<Option<Command>> {
    if let Some(text) = get_string(stream).await? {
        eprintln!("read_command: {text:?}");
        if text.starts_with("*") {
            let chunks = text[1..].parse::<usize>()
                .map_err(|_| Error::msg("Protocol error: invalid multibulk length"))?;
            let mut cmd = Command::new();
            for _ in 0..chunks {
                if let Some(cmd_part) = read_bulk_string(stream).await? {
                    cmd.push(cmd_part);
                } else {
                    return Ok(None)
                }
            }
            Ok(Some(cmd))
        } else {
            Ok(Some(text.split_whitespace().map(|s| s.to_string()).collect()))
        }
    } else {
        Ok(None)
    }
}
