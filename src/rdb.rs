use anyhow::{bail, Result};
use std::collections::HashMap;
use std::future::Future;
use std::io::SeekFrom;
use std::path::Path;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncSeekExt, BufReader};
use crate::types::RedisType;

#[derive(Debug)]
pub struct RedisFileEntry {
    pub key: String,
    pub value: RedisType,
    pub expires: Option<SystemTime>,
}

pub struct Rdb {
    file: Box<dyn AsyncBufRead + Unpin>,
    version: u16,
    metadata: HashMap<String, String>,
    db0_offset: u64,
}

#[derive(Debug)]
enum EncodedLength {
    Int(u32),
    Special(u8)
}

async fn read_length_encoded<Buf>(file: &mut Buf) -> Result<EncodedLength>
where
    Buf: AsyncBufRead + Unpin
{
    let first_byte = file.read_u8().await?;
    let (length_type, remaining_bits) = ((first_byte >> 6), first_byte & 0x3f);
    Ok(match length_type {
        0 => EncodedLength::Int(remaining_bits as u32),
        1 => EncodedLength::Int(file.read_u8().await? as u32 | ((remaining_bits as u32) << 8)),
        2 => EncodedLength::Int(file.read_u32().await?),
        3 => EncodedLength::Special(remaining_bits & 0x3f),
        _ => bail!("Can't happen!")
    })
}

// String encoding
//   The string might be either a regular string prefixed by a length-encoded size or,
//   if a special format is detected, the encoded value will indicate:
//     0 - an 8 bit integer
//     1 - a 16 bit integer
//     2 - a 32 bit integer
//     3 - an LZF compressed string
//   In the case of an LZF string, the payload is as follows:
//       - a length-encoded compressed length (`clen`)
//       - a length-encoded uncompressed length
//       - `clen` bytes of compressed string
async fn read_string<Buf>(file: &mut Buf) -> Result<String>
where
    Buf: AsyncBufRead + Unpin
{
    Ok(match read_length_encoded(file).await? {
        EncodedLength::Int(length) => {
            let mut string = String::with_capacity(length as usize);
            file.take(length as u64).read_to_string(&mut string).await?;
            string
        }
        EncodedLength::Special(0) => file.read_i8().await?.to_string(),
        EncodedLength::Special(1) => file.read_i16().await?.to_string(),
        EncodedLength::Special(2) => file.read_i32().await?.to_string(),
        EncodedLength::Special(3) => { bail!("Unimplemented: reading compressed string")}
        _ => { bail!("Unknown encoding")}
    })
}

impl Rdb {
    pub async fn open(path: &Path) -> Result<Self> {
        let mut file = BufReader::new(File::open(path).await?);
        let mut magic =[0; 9];
        let mut metadata = HashMap::new();

        file.read_exact(&mut magic).await?;
        if &magic[0..5] != b"REDIS" {
            bail!("Not a Redis database: {}", path.to_string_lossy())
        }

        while let Ok(first)= file.read_u8().await {
            match first {
                0xFA => {
                    // Read an auxiliary field
                    let (key, value) = (read_string(&mut file).await?, read_string(&mut file).await?);
                    metadata.insert(key, value);
                }
                0xFE => {
                    // DB marker
                    if file.read_u8().await? != 0 {
                        bail!("Corrupt file. Couldn't find the marker for DB 0");
                    }
                    break;
                }
                byte => {
                    let offset = file.seek(SeekFrom::Current(0)).await?;
                    bail!("Unknown byte {byte:#x} at offset {offset}");
                }
            }
        }

        if file.read_u8().await? != 0xFB {
            bail!("Corrupt file. Couldn't find the marker for DB 0's hash size info");
        }

        let _hash_table_size = read_length_encoded(&mut file).await?;
        let _expire_hash_table_size = read_length_encoded(&mut file).await?;
        let current_offset = file.seek(SeekFrom::Current(0)).await?;

        Ok(Self {
            file: Box::new(file),
            version: String::from_utf8_lossy(&magic[5..]).parse::<u16>()?,
            metadata,
            db0_offset: current_offset,
        })
    }

    pub fn print_debug_info(&self) {
        eprintln!("RDB Version: {}", self.version);
        eprintln!("Offset to DB 0: {}", self.db0_offset);
        eprintln!("Metadata:\n{:#?}", self.metadata);
    }

    fn priv_next_entry(&mut self) -> Pin<Box<dyn Future<Output=Result<Option<RedisFileEntry>>> + '_>> {
        Box::pin(async move {
            let first = self.file.read_u8().await?;

            Ok(match first {
                0..=14 => {
                    let key = read_string(&mut self.file).await?;
                    match first {
                        0 => Some(RedisFileEntry {
                            key,
                            value: RedisType::String(read_string(&mut self.file).await?),
                            expires: None,
                        }),
                        _ => bail!("Reading entry: unsupported data type {first} for key: {key}")
                    }
                }
                0xFC|0xFD => {
                    let expires_at = if first == 0xFC {
                        Some(UNIX_EPOCH + Duration::from_millis(self.file.read_u64_le().await?))
                    } else if first == 0xFD {
                        Some(UNIX_EPOCH + Duration::from_secs(self.file.read_u32_le().await? as u64))
                    } else {
                        None
                    };


                    self.priv_next_entry().await?
                        .map(|mut rec| {
                            rec.expires = expires_at;
                            rec
                        })
                }
                0xFF|0xFE => {
                    // End of File, next DB. We support reading only from DB 0
                    None
                }
                unknown => {
                    bail!("Reading entry: unrecognized code '{unknown:#x}'")
                }
            })
        })
    }

    pub async fn read_next_entry(&mut self) -> Result<Option<RedisFileEntry>> {
        self.priv_next_entry().await
    }
}