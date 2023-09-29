use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

/// Respond to a PING command
async fn handle_ping(stream: &mut BufReader<TcpStream>) -> io::Result<()> {
    stream.write(b"+PONG\r\n").await.map(|_| ())
}

/// Dispatch commands to their handlers
async fn dispatch(stream: &mut BufReader<TcpStream>, data: &str) -> io::Result<()> {
    match data.chars().next() {
        Some(a) if a.is_ascii_alphabetic() => {
            handle_ping(stream).await
        }
        _ => { Ok(()) }
    }
}

async fn handle_events(mut stream: TcpStream) {
    let addr = stream.local_addr().unwrap();
    let mut stream = BufReader::new(stream);
    let mut buf = String::new();
    loop {
        buf.clear();
        match stream.read_line(&mut buf).await {
            Ok(size) if size == 0 => {
                // Most probably reset by peer (closed stream)
                eprintln!("handle_events({addr}): closed endpoint");
                break;
            }
            Ok(_) => {
                dispatch(&mut stream, buf.trim_end())
                    .await
                    .map_err(|error| {
                        eprintln!("handle_events({addr}): {error}");
                        Ok::<_, io::Error>(())
                    })
                    .unwrap();
            }
            Err(error) => {
                eprintln!("handle_events({addr}): {error}");
            }
        }
    }
}

const BINDING_ADDRESS: &str = "127.0.0.1:6379";

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind(BINDING_ADDRESS).await?;

    loop {
        let (mut stream, addr) = listener.accept().await?;
        eprintln!("Accepted connection from: {}", addr);
        tokio::spawn(async move {
            handle_events(stream).await;
        });
    };
}

