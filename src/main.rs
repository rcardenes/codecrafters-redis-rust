use std::io::{self, BufReader, BufWriter, BufRead, Write};
use std::net::{TcpListener, TcpStream};

/// Respond to a PING command
fn handle_ping<W: Write>(_input: &mut String, output: &mut W) -> io::Result<()> {
    println!("Writing 'PONG'");
    output.write(b"+PONG\r\n")?;
    output.flush()?;
    Ok(())
}

/// Handle a single connection until disconnected
fn handle_connection(stream: TcpStream) -> io::Result<()> {
    let (mut input, mut output) = (
        BufReader::new(stream.try_clone()?),
        BufWriter::new(stream)
    );
    println!("Accepted connection");

    loop {
        let mut buf = String::new();
        input.read_line(&mut buf)?;
        println!("{}", buf);

        match handle_ping(&mut buf, &mut output) {
            Err(e) => {
                println!("error: {}", e);
            }
            _ => {}
        };
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_connection(stream).or_else(|err| {
                    println!("error: {}", err);
                    Ok::<_, io::Error>(())
                }).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
