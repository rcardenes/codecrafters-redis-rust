use tokio::io::BufReader;
use tokio::net::TcpStream;

pub mod config;
pub mod rdb;
pub mod types;
pub mod io;
pub mod info;

pub type TcpReader = BufReader<TcpStream>;
