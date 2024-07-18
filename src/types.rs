use anyhow::Result;

use crate::io::*;
use crate::TcpReader;

#[derive(Debug, Clone)]
pub enum RedisType {
    String(String),
    Int(i64),
    Array(Vec<RedisType>),
}

impl RedisType {
    pub async fn write(&self, stream: &mut TcpReader) -> Result<()> {
        match self {
            RedisType::String(string) => {
                write_string(stream, string).await?
            }
            RedisType::Int(number) => {
                write_integer(stream, *number).await?
            }
            RedisType::Array(array) => {
                write_array_size(stream, array.len()).await?;
                let mut stack = vec![array.iter()];
                while let Some(last) = stack.last_mut() {
                    if let Some(element) = last.next() {
                        match element {
                            RedisType::Array(array) => {
                                write_array_size(stream, array.len()).await?;
                                stack.push(array.iter())
                            },
                            // Duplicated code because async functions can't be recursive
                            // as-is. There ways to circumvent this, but they are a pain
                            // in the ass or require the use of crates not provided by the
                            // project (and CodeCrafters don't support modifying Cargo.toml
                            RedisType::String(string) => {
                                write_string(stream, string).await?
                            },
                            RedisType::Int(number) => {
                                write_integer(stream, *number).await?
                            },
                        }
                    } else {
                        stack.pop();
                    }
                }
            }
        }
        Ok(())
    }
}

impl From<&str> for RedisType {
    fn from(value: &str) -> Self {
        RedisType::String(String::from(value))
    }
}

impl From<String> for RedisType {
    fn from(value: String) -> Self {
        RedisType::String(value)
    }
}
