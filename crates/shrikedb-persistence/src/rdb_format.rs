// RDB format constants (compatible with Redis 6+ / RDB version 9).

// Object type opcodes
pub const RDB_TYPE_STRING: u8 = 0;

// Special opcodes
pub const RDB_OPCODE_SLOT_INFO: u8 = 244;
pub const RDB_OPCODE_FUNCTION2: u8 = 245;
pub const RDB_OPCODE_FUNCTION: u8 = 246;
pub const RDB_OPCODE_MODULE_AUX: u8 = 247;
pub const RDB_OPCODE_IDLE: u8 = 248;
pub const RDB_OPCODE_FREQ: u8 = 249;
pub const RDB_OPCODE_AUX: u8 = 250;
pub const RDB_OPCODE_RESIZEDB: u8 = 251;
pub const RDB_OPCODE_EXPIRETIME_MS: u8 = 252;
pub const RDB_OPCODE_EXPIRETIME: u8 = 253;
pub const RDB_OPCODE_SELECTDB: u8 = 254;
pub const RDB_OPCODE_EOF: u8 = 255;

// Length encoding
pub const RDB_6BITLEN: u8 = 0;
pub const RDB_14BITLEN: u8 = 1;
pub const RDB_ENCVAL: u8 = 3;

// Integer-as-string encodings (used with RDB_ENCVAL)
pub const RDB_ENC_INT8: u8 = 0;
pub const RDB_ENC_INT16: u8 = 1;
pub const RDB_ENC_INT32: u8 = 2;
pub const RDB_ENC_LZF: u8 = 3;

/// CRC64 using the Redis polynomial (Jones).
/// Polynomial: 0xad93d23594c935a9 (reflected)
pub fn crc64(crc: u64, data: &[u8]) -> u64 {
    let mut c = !crc;
    for &byte in data {
        let idx = ((c as u8) ^ byte) as usize;
        c = CRC64_TABLE[idx] ^ (c >> 8);
    }
    !c
}

// Pre-computed CRC64 lookup table for the Redis polynomial.
// Generated with polynomial 0xad93d23594c935a9 (reflected).
static CRC64_TABLE: [u64; 256] = {
    const POLY: u64 = 0xad93d23594c935a9;
    let mut table = [0u64; 256];
    let mut i = 0u64;
    while i < 256 {
        let mut crc = i;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i as usize] = crc;
        i += 1;
    }
    table
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc64_empty() {
        assert_eq!(crc64(0, b""), 0);
    }

    #[test]
    fn test_crc64_hello() {
        // Just verify it produces a non-zero deterministic result
        let c1 = crc64(0, b"hello");
        let c2 = crc64(0, b"hello");
        assert_eq!(c1, c2);
        assert_ne!(c1, 0);
    }

    #[test]
    fn test_crc64_incremental() {
        let full = crc64(0, b"helloworld");
        let partial = crc64(0, b"hello");
        let incremental = crc64(partial, b"world");
        assert_eq!(full, incremental);
    }
}
