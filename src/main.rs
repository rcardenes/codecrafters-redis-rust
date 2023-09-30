use anyhow::{bail, Error, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

type TcpReader = BufReader<TcpStream>;

async fn write_string(stream: &mut TcpReader, string: &str) -> Result<()> {
    let output = format!("${}\r\n{}\r\n", string.len(), string);
    stream.write(output.as_bytes()).await.map(|_| Ok(()))?
}

/// Respond to a PING command
async fn handle_ping(stream: &mut TcpReader, args: &[&str]) -> Result<()> {
    match args.len() {
        0 => stream.write(b"+PONG\r\n").await.map(|_| Ok(()))?,
        1 => write_string(stream, args[0]).await,
        _ => bail!("wrong number of arguments for 'ping' command")
    }
}

/// Respond to an ECHO command
async fn handle_echo(stream: &mut TcpReader, args: &[&str]) -> Result<()> {
    match args.len() {
        1 => write_string(stream, args[0]).await,
        _ => bail!("wrong number of arguments for 'echo' command")
    }
}

async fn dispatch(stream: &mut TcpReader, cmd_vec: &[&str]) -> Result<()> {
    let name = cmd_vec[0];
    match name.to_ascii_lowercase().as_str() {
        "ping" => { handle_ping(stream, &cmd_vec[1..]).await? }
        "echo" => { handle_echo(stream, &cmd_vec[1..]).await? }
        _ => {
            let args = cmd_vec[1..]
                .iter()
                .map(|s| format!("'{}'", *s))
                .collect::<Vec<_>>()
                .join(" ");
            let error_msg = format!("unknown command '{}', with args beginning with: {}", name, args);
            bail!(error_msg)
        }
    }
    Ok(())
}

async fn get_string(stream: &mut TcpReader) -> Result<Option<String>> {
    let mut buf = String::new();
    let read_chars = stream.read_line(&mut buf).await?;

    if read_chars == 0 {
        Ok(None)
    } else {
        Ok(Some((&buf[0..read_chars -2]).to_string()))
    }
}

type Command = Vec<String>;

async fn read_bulk_string(stream: &mut TcpReader) -> Result<Option<String>> {
    fn format_error<'a>(chr: char) -> String {
        format!("Protocol error: expected '$', got '{}'", chr)
    }

    if let Some(size_string) = get_string(stream).await? {
        if size_string.len() == 0 {
            bail!(format_error(' '))
        } else if !size_string.starts_with("$") {
            bail!(format_error(size_string.chars().next().unwrap()))
        } else {
            let string_size = size_string[1..].parse::<usize>()
                .map_err(|_| Error::msg("Protocol error: invalid bulk length"))?;
            let mut buf: Vec<u8> = vec![0; string_size + 2];
            stream.read_exact(buf.as_mut_slice()).await?;
            let bulk_string = String::from_utf8_lossy(&buf[..string_size]).to_string();
            Ok(Some(bulk_string))
        }
    } else {
        Ok(None)
    }
}

async fn read_command(stream: &mut TcpReader) -> Result<Option<Command>> {
    if let Some(text) = get_string(stream).await? {
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

async fn send_error_message(stream: &mut TcpReader, msg: &str) {
    let msg = format!("-ERR {}\r\n", msg);
    let _ = stream.write(msg.as_bytes()).await;
}

async fn handle_events<'a>(stream: TcpStream) {
    let addr = stream.local_addr().unwrap();
    eprintln!("Handling events from {addr}");
    let mut stream = BufReader::new(stream);
    loop {
        match read_command(&mut stream).await {
            Ok(cnt) => match cnt {
                Some(cmd) => {
                    let strs = cmd.iter().map(|s| s.as_str()).collect::<Vec<_>>();
                    if let Err(error) = dispatch(&mut stream, strs.as_slice()).await {
                        send_error_message(&mut stream, &error.to_string()).await;
                    }
                }
                None => {}
            },
            Err(error) => {
                send_error_message(&mut stream, &error.to_string()).await;
                break;
            }
        }
    }
}

const BINDING_ADDRESS: &str = "127.0.0.1:6379";

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind(BINDING_ADDRESS).await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        eprintln!("Accepted connection from: {}", addr);
        tokio::spawn(async move {
            handle_events(stream).await;
        });
    };
}

