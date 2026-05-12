/// Compute which shard a key belongs to.
///
/// If the key contains a hash tag `{...}`, only the content between the first `{` and the
/// next `}` is hashed. This allows related keys to land on the same shard
/// (e.g., `{user:1}:name` and `{user:1}:email`).
pub fn shard(key: &[u8], num_shards: u32) -> u32 {
    let tag = hash_tag(key);
    crc32(tag) % num_shards
}

/// Extract the hash tag from a key. If the key contains `{...}` with at least one byte
/// between the braces, return that substring. Otherwise return the full key.
fn hash_tag(key: &[u8]) -> &[u8] {
    if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
            if end > 0 {
                return &key[start + 1..start + 1 + end];
            }
        }
    }
    key
}

/// Simple CRC32 hash (same polynomial as used by Redis cluster).
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xFFFFFFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_deterministic() {
        assert_eq!(shard(b"foo", 8), shard(b"foo", 8));
    }

    #[test]
    fn test_shard_distribution() {
        let n = 8u32;
        let mut counts = vec![0u32; n as usize];
        for i in 0..1000 {
            let key = format!("key:{}", i);
            let s = shard(key.as_bytes(), n);
            assert!(s < n);
            counts[s as usize] += 1;
        }
        // All shards should get some keys
        for c in &counts {
            assert!(*c > 50, "poor distribution: {:?}", counts);
        }
    }

    #[test]
    fn test_hash_tag() {
        // Keys with same hash tag should land on the same shard
        let s1 = shard(b"{user:1}:name", 16);
        let s2 = shard(b"{user:1}:email", 16);
        assert_eq!(s1, s2);

        // Empty hash tag is ignored
        let s3 = shard(b"{}:name", 16);
        let s4 = shard(b"{}:email", 16);
        // These hash the full key, so they'll differ
        assert_ne!(s3, s4);
    }

    #[test]
    fn test_hash_tag_extraction() {
        assert_eq!(hash_tag(b"{user}:1"), b"user");
        assert_eq!(hash_tag(b"no_tag"), b"no_tag");
        assert_eq!(hash_tag(b"{}empty"), b"{}empty");
        assert_eq!(hash_tag(b"{a}b}c"), b"a"); // first { to next }
    }
}
