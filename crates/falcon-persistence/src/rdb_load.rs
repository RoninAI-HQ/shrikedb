use std::io::{self, Read};

use falcon_core::compact_obj::PrimeValue;

use crate::rdb_format::*;

/// A key-value entry loaded from an RDB file.
#[derive(Debug)]
pub struct RdbEntry {
    pub db_index: u32,
    pub key: Vec<u8>,
    pub value: PrimeValue,
    /// Absolute expiry timestamp in milliseconds, or None.
    pub expire_ms: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum RdbLoadError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("invalid RDB magic header")]
    BadMagic,
    #[error("unsupported RDB version: {0}")]
    UnsupportedVersion(u32),
    #[error("unsupported object type: {0}")]
    UnsupportedType(u8),
    #[error("unexpected EOF")]
    UnexpectedEof,
    #[error("CRC mismatch: expected {expected:#x}, got {actual:#x}")]
    CrcMismatch { expected: u64, actual: u64 },
    #[error("corrupt data: {0}")]
    Corrupt(String),
}

/// Load entries from an RDB file.
pub struct RdbLoader<R: Read> {
    reader: R,
    crc: u64,
    current_db: u32,
    rdb_version: u32,
}

impl<R: Read> RdbLoader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            crc: 0,
            current_db: 0,
            rdb_version: 0,
        }
    }

    /// Read and validate the RDB header. Returns the RDB version.
    pub fn read_header(&mut self) -> Result<u32, RdbLoadError> {
        let mut magic = [0u8; 5];
        self.read_exact(&mut magic)?;
        if &magic != b"REDIS" {
            return Err(RdbLoadError::BadMagic);
        }
        let mut ver = [0u8; 4];
        self.read_exact(&mut ver)?;
        let version: u32 = std::str::from_utf8(&ver)
            .map_err(|_| RdbLoadError::BadMagic)?
            .parse()
            .map_err(|_| RdbLoadError::BadMagic)?;
        if version > 12 {
            return Err(RdbLoadError::UnsupportedVersion(version));
        }
        self.rdb_version = version;
        Ok(version)
    }

    /// Read the next entry from the RDB file. Returns None at EOF.
    pub fn read_entry(&mut self) -> Result<Option<RdbEntry>, RdbLoadError> {
        loop {
            let opcode = self.read_byte()?;
            match opcode {
                RDB_OPCODE_EOF => {
                    // Read and verify CRC64 checksum
                    let expected_crc = self.crc;
                    let stored_crc = self.read_u64_le_no_crc()?;
                    if stored_crc != 0 && stored_crc != expected_crc {
                        return Err(RdbLoadError::CrcMismatch {
                            expected: expected_crc,
                            actual: stored_crc,
                        });
                    }
                    return Ok(None);
                }
                RDB_OPCODE_SELECTDB => {
                    self.current_db = self.read_length()? as u32;
                }
                RDB_OPCODE_RESIZEDB => {
                    let _db_size = self.read_length()?;
                    let _expire_size = self.read_length()?;
                }
                RDB_OPCODE_AUX => {
                    let _key = self.read_string()?;
                    let _value = self.read_string()?;
                    // Auxiliary fields are metadata, skip them
                }
                RDB_OPCODE_EXPIRETIME => {
                    // 4-byte seconds
                    let secs = self.read_u32_le()? as u64;
                    let expire_ms = Some(secs * 1000);
                    return self.read_typed_entry(expire_ms);
                }
                RDB_OPCODE_EXPIRETIME_MS => {
                    let ms = self.read_u64_le()?;
                    return self.read_typed_entry(Some(ms));
                }
                RDB_OPCODE_FREQ => {
                    let _freq = self.read_byte()?;
                    // LFU frequency, skip - will be followed by the actual entry
                }
                RDB_OPCODE_IDLE => {
                    let _idle = self.read_length()?;
                    // LRU idle time, skip
                }
                RDB_OPCODE_MODULE_AUX | RDB_OPCODE_FUNCTION | RDB_OPCODE_FUNCTION2
                | RDB_OPCODE_SLOT_INFO => {
                    return Err(RdbLoadError::UnsupportedType(opcode));
                }
                _ => {
                    // This is a type byte for a key-value entry
                    return self.read_entry_with_type(opcode, None);
                }
            }
        }
    }

    fn read_typed_entry(
        &mut self,
        expire_ms: Option<u64>,
    ) -> Result<Option<RdbEntry>, RdbLoadError> {
        let type_byte = self.read_byte()?;
        self.read_entry_with_type(type_byte, expire_ms)
    }

    fn read_entry_with_type(
        &mut self,
        type_byte: u8,
        expire_ms: Option<u64>,
    ) -> Result<Option<RdbEntry>, RdbLoadError> {
        let key = self.read_string()?;

        let value = match type_byte {
            RDB_TYPE_STRING => {
                let data = self.read_string()?;
                // Try to store as integer if possible
                PrimeValue::from_bytes(&data)
            }
            _ => {
                // Skip unsupported types by reading and discarding
                tracing::warn!("skipping unsupported RDB type {} for key {:?}", type_byte,
                    String::from_utf8_lossy(&key));
                return Ok(self.read_entry()?);
            }
        };

        Ok(Some(RdbEntry {
            db_index: self.current_db,
            key,
            value,
            expire_ms,
        }))
    }

    // -- Internal reading methods --

    fn read_byte(&mut self) -> Result<u8, RdbLoadError> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), RdbLoadError> {
        self.reader
            .read_exact(buf)
            .map_err(|e| {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    RdbLoadError::UnexpectedEof
                } else {
                    RdbLoadError::Io(e)
                }
            })?;
        self.crc = crc64(self.crc, buf);
        Ok(())
    }

    fn read_u32_le(&mut self) -> Result<u32, RdbLoadError> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    fn read_u64_le(&mut self) -> Result<u64, RdbLoadError> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    /// Read u64 LE without updating CRC (for the final checksum).
    fn read_u64_le_no_crc(&mut self) -> Result<u64, RdbLoadError> {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf).map_err(|e| {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                RdbLoadError::UnexpectedEof
            } else {
                RdbLoadError::Io(e)
            }
        })?;
        Ok(u64::from_le_bytes(buf))
    }

    /// Read a length-encoded value. Returns (length, is_encoded).
    fn read_length_with_encoding(&mut self) -> Result<(u64, bool), RdbLoadError> {
        let first = self.read_byte()?;
        let enc_type = (first >> 6) & 0x03;

        match enc_type {
            RDB_6BITLEN => Ok(((first & 0x3F) as u64, false)),
            RDB_14BITLEN => {
                let second = self.read_byte()?;
                Ok(((((first & 0x3F) as u64) << 8) | second as u64, false))
            }
            2 => {
                // 32-bit or 64-bit
                let sub = first & 0x3F;
                if sub == 0 {
                    // 32-bit big-endian
                    let mut buf = [0u8; 4];
                    self.read_exact(&mut buf)?;
                    Ok((u32::from_be_bytes(buf) as u64, false))
                } else if sub == 1 {
                    // 64-bit big-endian
                    let mut buf = [0u8; 8];
                    self.read_exact(&mut buf)?;
                    Ok((u64::from_be_bytes(buf), false))
                } else {
                    Err(RdbLoadError::Corrupt(format!(
                        "unknown length encoding sub-type {}",
                        sub
                    )))
                }
            }
            RDB_ENCVAL => {
                // Special encoding
                Ok(((first & 0x3F) as u64, true))
            }
            _ => unreachable!(),
        }
    }

    fn read_length(&mut self) -> Result<u64, RdbLoadError> {
        let (len, _) = self.read_length_with_encoding()?;
        Ok(len)
    }

    /// Read a string (length-prefixed or integer-encoded).
    fn read_string(&mut self) -> Result<Vec<u8>, RdbLoadError> {
        let (len, is_encoded) = self.read_length_with_encoding()?;

        if is_encoded {
            match len as u8 {
                RDB_ENC_INT8 => {
                    let v = self.read_byte()? as i8;
                    Ok(v.to_string().into_bytes())
                }
                RDB_ENC_INT16 => {
                    let mut buf = [0u8; 2];
                    self.read_exact(&mut buf)?;
                    let v = i16::from_le_bytes(buf);
                    Ok(v.to_string().into_bytes())
                }
                RDB_ENC_INT32 => {
                    let mut buf = [0u8; 4];
                    self.read_exact(&mut buf)?;
                    let v = i32::from_le_bytes(buf);
                    Ok(v.to_string().into_bytes())
                }
                RDB_ENC_LZF => {
                    let clen = self.read_length()? as usize;
                    let _ulen = self.read_length()? as usize;
                    // Skip LZF-compressed strings for now
                    let mut buf = vec![0u8; clen];
                    self.read_exact(&mut buf)?;
                    tracing::warn!("LZF-compressed strings not supported, skipping");
                    Ok(vec![])
                }
                _ => Err(RdbLoadError::Corrupt(format!(
                    "unknown string encoding {}",
                    len
                ))),
            }
        } else {
            let mut buf = vec![0u8; len as usize];
            self.read_exact(&mut buf)?;
            Ok(buf)
        }
    }
}

/// Convenience: load all entries from an RDB file into a Vec.
pub fn load_all<R: Read>(reader: R) -> Result<Vec<RdbEntry>, RdbLoadError> {
    let mut loader = RdbLoader::new(reader);
    loader.read_header()?;
    let mut entries = Vec::new();
    while let Some(entry) = loader.read_entry()? {
        entries.push(entry);
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdb_save::RdbSaver;
    use falcon_core::compact_obj::PrimeKey;

    #[test]
    fn test_roundtrip() {
        // Save
        let mut buf = Vec::new();
        {
            let mut saver = RdbSaver::new(&mut buf);
            saver.write_header().unwrap();
            saver.write_aux("redis-ver", "7.0.0").unwrap();
            saver.write_select_db(0).unwrap();
            saver.write_resize_db(3, 1).unwrap();

            saver
                .write_key_value(
                    &PrimeKey::new(b"hello"),
                    &PrimeValue::String(b"world".to_vec()),
                    None,
                )
                .unwrap();
            saver
                .write_key_value(
                    &PrimeKey::new(b"counter"),
                    &PrimeValue::Integer(42),
                    None,
                )
                .unwrap();
            saver
                .write_key_value(
                    &PrimeKey::new(b"temp"),
                    &PrimeValue::String(b"expiring".to_vec()),
                    Some(1735689600000),
                )
                .unwrap();

            saver.write_eof().unwrap();
        }

        // Load
        let entries = load_all(&buf[..]).unwrap();
        assert_eq!(entries.len(), 3);

        assert_eq!(entries[0].key, b"hello");
        assert_eq!(entries[0].value.as_bytes(), b"world");
        assert!(entries[0].expire_ms.is_none());

        assert_eq!(entries[1].key, b"counter");
        assert_eq!(entries[1].value.as_integer(), Some(42));

        assert_eq!(entries[2].key, b"temp");
        assert_eq!(entries[2].expire_ms, Some(1735689600000));
    }

    #[test]
    fn test_roundtrip_large_string() {
        let big = vec![b'x'; 100_000];
        let mut buf = Vec::new();
        {
            let mut saver = RdbSaver::new(&mut buf);
            saver.write_header().unwrap();
            saver.write_select_db(0).unwrap();
            saver
                .write_key_value(
                    &PrimeKey::new(b"big"),
                    &PrimeValue::String(big.clone()),
                    None,
                )
                .unwrap();
            saver.write_eof().unwrap();
        }

        let entries = load_all(&buf[..]).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].value.as_bytes(), big);
    }

    #[test]
    fn test_roundtrip_multiple_dbs() {
        let mut buf = Vec::new();
        {
            let mut saver = RdbSaver::new(&mut buf);
            saver.write_header().unwrap();
            saver.write_select_db(0).unwrap();
            saver
                .write_key_value(
                    &PrimeKey::new(b"a"),
                    &PrimeValue::String(b"db0".to_vec()),
                    None,
                )
                .unwrap();
            saver.write_select_db(3).unwrap();
            saver
                .write_key_value(
                    &PrimeKey::new(b"b"),
                    &PrimeValue::String(b"db3".to_vec()),
                    None,
                )
                .unwrap();
            saver.write_eof().unwrap();
        }

        let entries = load_all(&buf[..]).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].db_index, 0);
        assert_eq!(entries[0].key, b"a");
        assert_eq!(entries[1].db_index, 3);
        assert_eq!(entries[1].key, b"b");
    }
}
