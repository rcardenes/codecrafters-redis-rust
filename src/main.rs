use std::collections::HashMap;
use std::io::{self, Read, Write, ErrorKind};
use std::net::SocketAddr;
use std::time::Duration;
use mio::{
    event::{self, Event, Events},
    net::{TcpListener, TcpStream},
    Interest,
    Poll,
    Token
};
use tokio::io::AsyncReadExt;

struct SocketInfo {
    pending: usize,
    stream: TcpStream,
}
struct Context {
    poller: Poll,
    listener: TcpListener,
    events: Events,
    sockets: HashMap<usize, SocketInfo>,
    next_socket_index: usize,
}

impl Context {
    pub fn new(poller: Poll, mut listener: TcpListener) -> Self {
        poller
            .registry()
            .register(&mut listener, Token(0), Interest::READABLE)
            .unwrap();
        Self {
            poller,
            listener,
            sockets: HashMap::new(),
            events: Events::with_capacity(MAX_EVENTS),
            next_socket_index: MIN_SOCKET
        }
    }

    pub fn iter(&self) -> IterEvents {
        IterEvents {
            events_iterator: self.events.iter()
        }
    }

    pub fn poll(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.poller.poll(&mut self.events, timeout)
    }

    fn at_max(&self) -> bool {
        self.next_socket_index > MAX_SOCKET
    }

    fn add_stream(&mut self, mut stream: TcpStream) -> io::Result<()> {
        if !self.at_max() {
            self.poller
                .registry()
                .register(
                    &mut stream,
                    Token(self.next_socket_index),
                    Interest::READABLE
                )?;
            self.sockets.insert(self.next_socket_index, SocketInfo { pending: 0, stream });
            self.next_socket_index += 1;
            Ok(())
        } else {
            Err(io::Error::new(
                ErrorKind::Other,
                "No more sockets available"
            ))
        }
    }

    pub fn accept_connection(&mut self) -> io::Result<()> {
        if self.at_max() {
            Err(io::Error::new(
                ErrorKind::Other,
                "error: maximum connections reached"
            ))
        } else {
            let (stream, addr) =  self.listener.accept()?;
            println!("Connection from: {}", get_addr_as_text(addr));
            self.add_stream(stream)?;
            Ok(())
        }
    }

    pub fn get_string_from(&mut self, token: usize) -> io::Result<String> {
        match self.sockets.get_mut(&token) {
            Some(sinfo) => {
                let mut string_buf = String::new();
                loop {
                    let mut buf = vec![0;64];
                    let bytes_peeked = sinfo.stream.peek(&mut buf)?;
                    string_buf += &*String::from_utf8_lossy(&buf).to_string();
                    if let Some(pos) = string_buf.find("\r\n") {
                        let mut buf = vec![0;pos+2];
                        let bytes_read = sinfo.stream.read(&mut buf)?;
                        sinfo.pending = bytes_peeked - bytes_read;
                        let (a, _) = string_buf.split_at(pos);
                        return Ok(a.to_string());
                    } else {
                        println!("Reading the whole buffer");
                        sinfo.pending -= sinfo.stream.read(&mut buf)?;
                    }
                }
            }
            None => Err(io::Error::new(
                ErrorKind::NotFound,
                "Got petition for unregistered stream"
            ))
        }
    }

    pub fn write_to(&mut self, token: usize, bug: &[u8]) -> io::Result<()> {
        match self.sockets.get_mut(&token) {
            Some(SocketInfo { pending: _, stream }) => {
                stream.write(bug).map(|_| ())
            }
            None => Err(io::Error::new(
                ErrorKind::NotFound,
                "Got petition for unregistered stream"
            ))
        }
    }

    pub fn is_stream_empty(&self, token: usize) -> Option<bool> {
        self.sockets.get(&token).map(|SocketInfo { pending, stream: _ }| {
            *pending == 0
        })
    }
}

struct IterEvents<'a> {
    events_iterator: event::Iter<'a>,
}

impl<'a> Iterator for IterEvents<'a> {
    type Item = &'a Event;

    fn next(&mut self) -> Option<Self::Item> {
        self.events_iterator.next()
    }
}

/// Respond to a PING command
fn handle_ping(ctx: &mut Context, stream_token: usize, _input: &mut String) -> io::Result<()> {
    ctx.write_to(stream_token, b"+PONG\r\n").unwrap();
    Ok(())
}

/// Handle a single connection until disconnected
fn dispatch(ctx: &mut Context, stream_token: usize) -> io::Result<()> {
    let mut buf = ctx.get_string_from(stream_token)?;
    println!("Current buf: {buf}");
    let is_alpha = buf
        .as_str()
        .chars()
        .next()
        .map(|c| c.is_alphabetic())
        .or_else(|| Some(false))
        .unwrap();

    if is_alpha {
        match handle_ping(ctx, stream_token, &mut buf) {
            Err(e) => {
                eprintln!("handle_ping: {}", e);
            }
            _ => {}
        }
    };
    Ok(())
}

fn get_addr_as_text(addr: SocketAddr) -> String {
    String::from(format!("{}", addr).as_str())
}

fn would_block(err: &io::Error) -> bool {
    err.kind() == ErrorKind::WouldBlock
}
fn handle_events(ctx: &mut Context) {
    let events = ctx.iter().cloned().collect::<Vec<_>>();
    for event in events {
        match event.token().0 {
            LISTENER => {
                match ctx.accept_connection() {
                    Err(error) => eprintln!("Accepting connection: {error}"),
                    _ => { }
                }
            }
            token => {
                loop {
                    match dispatch (ctx, token) {
                        Err(ref err) if would_block(err) => {
                            println!("Breaking...");
                            break
                        },
                        Err(error) => eprintln!("Dispatching: {error}"),
                        _ => { }
                    }
                    if ctx.is_stream_empty(token) != Some(false) {
                        break;
                    }
                }
            }
        }
    }
}

const MAX_EVENTS: usize = 1024;
const LISTENER: usize = 0;
const MIN_SOCKET: usize = 1;  // Must be > 0
const MAX_SOCKET: usize = MAX_EVENTS - 1; // Must be < MAX_EVENTS
const POLLING_TIMEOUT: Duration = Duration::from_millis(500);

fn main() {
    let mut ctx = Context::new(
        Poll::new().unwrap(),
        TcpListener::bind(
            "127.0.0.1:6379".parse().unwrap()
        ).unwrap()
    );

    loop {
        match ctx.poll(Some(POLLING_TIMEOUT)) {
            Err(error) => println!("error: {}", error),
            _ => handle_events(&mut ctx),
        }
    };
}

