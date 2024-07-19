use anyhow::Result;

use crate::io::*;

#[derive(Debug, Clone)]
pub enum RedisType {
    String(String),
    Int(i64),
    Timestamp(u128),
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
                            RedisType::Timestamp(_) => todo!(),
                        }
                    } else {
                        stack.pop();
                    }
                }
            }
            RedisType::Timestamp(_) => todo!(),
        }
        Ok(())
    }

    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            RedisType::String(string) => {
                format!("${}\r\n{}\r\n", string.len(), string)
                    .as_bytes()
                    .to_vec()
            }
            RedisType::Int(number) => {
                format!(":{number}\r\n").as_bytes().to_vec()
            }
            RedisType::Timestamp(millis) => {
                format!(":{millis}\r\n").as_bytes().to_vec()
            }
            RedisType::Array(array) => {
                let mut size = format!("*{}\r\n", array.len()).as_bytes().to_vec();

                size.extend( 
                    array.iter() .map(|comp| comp.to_vec())
                    .collect::<Vec<_>>()
                    .concat());

                size
            }
        }
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
