use std::io::{self, Write};

use shrikedb_core::compact_obj::{PrimeKey, PrimeValue};

use crate::rdb_format::*;

/// Serialize database contents to RDB format.
pub struct RdbSaver<W: Write> {
    writer: W,
    crc: u64,
    bytes_written: u64,
}

impl<W: Write> RdbSaver<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            crc: 0,
            bytes_written: 0,
        }
    }

    /// Write the RDB header: magic string + version.
    pub fn write_header(&mut self) -> io::Result<()> {
        self.write_bytes(b"REDIS")?;
        // RDB version 9 (compatible with Redis 6+)
        self.write_bytes(b"0009")?;
        Ok(())
    }

    /// Write an auxiliary field (metadata).
    pub fn write_aux(&mut self, key: &str, value: &str) -> io::Result<()> {
        self.write_byte(RDB_OPCODE_AUX)?;
        self.write_string(key.as_bytes())?;
        self.write_string(value.as_bytes())?;
        Ok(())
    }

    /// Write a database selector.
    pub fn write_select_db(&mut self, db_index: u32) -> io::Result<()> {
        self.write_byte(RDB_OPCODE_SELECTDB)?;
        self.write_length(db_index as u64)?;
        Ok(())
    }

    /// Write a resize-db hint (prime table size, expire table size).
    pub fn write_resize_db(&mut self, db_size: u64, expire_size: u64) -> io::Result<()> {
        self.write_byte(RDB_OPCODE_RESIZEDB)?;
        self.write_length(db_size)?;
        self.write_length(expire_size)?;
        Ok(())
    }

    /// Write a key-value pair with optional expiry.
    pub fn write_key_value(
        &mut self,
        key: &PrimeKey,
        value: &PrimeValue,
        expire_ms: Option<u64>,
    ) -> io::Result<()> {
        // Write expiry if present
        if let Some(ms) = expire_ms {
            self.write_byte(RDB_OPCODE_EXPIRETIME_MS)?;
            self.write_u64_le(ms)?;
        }

        // Write type + key + value
        match value {
            PrimeValue::String(data) => {
                self.write_byte(RDB_TYPE_STRING)?;
                self.write_string(key.as_bytes())?;
                self.write_string(data)?;
            }
            PrimeValue::Integer(n) => {
                self.write_byte(RDB_TYPE_STRING)?;
                self.write_string(key.as_bytes())?;
                self.write_integer_as_string(*n)?;
            }
            // TODO: serialize List, Set, Hash, ZSet to RDB format
            _ => {
                // Skip non-string types for now (they won't be persisted)
            }
        }

        Ok(())
    }

    /// Write the EOF marker and CRC64 checksum.
    pub fn write_eof(&mut self) -> io::Result<()> {
        self.write_byte(RDB_OPCODE_EOF)?;
        let checksum = self.crc;
        // Write CRC64 checksum (not included in CRC itself)
        let bytes = checksum.to_le_bytes();
        self.writer.write_all(&bytes)?;
        self.bytes_written += 8;
        Ok(())
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Flush the writer.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    // -- Internal encoding methods --

    fn write_byte(&mut self, b: u8) -> io::Result<()> {
        self.write_bytes(&[b])
    }

    fn write_bytes(&mut self, data: &[u8]) -> io::Result<()> {
        self.writer.write_all(data)?;
        self.crc = crc64(self.crc, data);
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    fn write_u64_le(&mut self, v: u64) -> io::Result<()> {
        self.write_bytes(&v.to_le_bytes())
    }

    /// Write a length-encoded integer.
    fn write_length(&mut self, len: u64) -> io::Result<()> {
        if len < 64 {
            // 6-bit: 00|xxxxxx
            self.write_byte(len as u8)?;
        } else if len < 16384 {
            // 14-bit: 01|xxxxxx yyyyyyyy
            self.write_byte(0x40 | ((len >> 8) as u8))?;
            self.write_byte((len & 0xFF) as u8)?;
        } else if len <= u32::MAX as u64 {
            // 32-bit: 10|000000 + 4 bytes big-endian
            self.write_byte(0x80)?;
            self.write_bytes(&(len as u32).to_be_bytes())?;
        } else {
            // 64-bit: 10|000001 + 8 bytes big-endian
            self.write_byte(0x81)?;
            self.write_bytes(&len.to_be_bytes())?;
        }
        Ok(())
    }

    /// Write a string (length-prefixed bytes).
    fn write_string(&mut self, data: &[u8]) -> io::Result<()> {
        // Try integer encoding for small integers
        if data.len() <= 11 {
            if let Some(n) = try_parse_integer(data) {
                return self.write_integer_as_string(n);
            }
        }
        self.write_length(data.len() as u64)?;
        self.write_bytes(data)?;
        Ok(())
    }

    /// Write an integer using RDB's compact integer-as-string encoding.
    fn write_integer_as_string(&mut self, n: i64) -> io::Result<()> {
        if n >= i8::MIN as i64 && n <= i8::MAX as i64 {
            // RDB_ENC_INT8: 11|00 + 1 byte
            self.write_byte(0xC0)?;
            self.write_byte(n as u8)?;
        } else if n >= i16::MIN as i64 && n <= i16::MAX as i64 {
            // RDB_ENC_INT16: 11|01 + 2 bytes LE
            self.write_byte(0xC1)?;
            self.write_bytes(&(n as i16).to_le_bytes())?;
        } else if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
            // RDB_ENC_INT32: 11|10 + 4 bytes LE
            self.write_byte(0xC2)?;
            self.write_bytes(&(n as i32).to_le_bytes())?;
        } else {
            // Doesn't fit in integer encoding, write as string
            let s = n.to_string();
            self.write_length(s.len() as u64)?;
            self.write_bytes(s.as_bytes())?;
        }
        Ok(())
    }
}

fn try_parse_integer(data: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(data).ok()?;
    let n: i64 = s.parse().ok()?;
    // Verify round-trip (no leading zeros, no whitespace)
    if n.to_string().as_bytes() == data {
        Some(n)
    } else {
        None
    }
}
