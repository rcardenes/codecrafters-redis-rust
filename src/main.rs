use std::io::{self, Write};
use std::net::{TcpListener, TcpStream};

fn handle_ping(mut stream: TcpStream) -> io::Result<()> {
    stream.write(b"+PONG\r\n")?;
    Ok(())
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                match handle_ping(stream) {
                    Err(e) => {
                        println!("error: {}", e);
                    }
                    _ => { }
                };
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
