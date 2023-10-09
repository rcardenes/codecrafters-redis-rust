use tokio::io::BufReader;
use tokio::net::TcpStream;

pub mod config;
pub mod rdb;
pub mod types;
pub mod io;

pub type TcpReader = BufReader<TcpStream>;
