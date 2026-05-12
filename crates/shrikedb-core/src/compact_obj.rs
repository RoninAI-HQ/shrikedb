use std::fmt;
use std::hash::{Hash, Hasher};

/// Maximum bytes that can be stored inline in a PrimeKey without heap allocation.
const INLINE_LEN: usize = 15;

/// Memory-efficient key representation. Small keys (up to 15 bytes) are stored inline,
/// avoiding heap allocation. Larger keys are heap-allocated.
///
/// Future optimization: ASCII compression, Huffman encoding, SDS-embedded TTL.
#[derive(Clone)]
pub enum PrimeKey {
    Inline { data: [u8; INLINE_LEN], len: u8 },
    Heap(Box<[u8]>),
}

impl PrimeKey {
    pub fn new(data: &[u8]) -> Self {
        if data.len() <= INLINE_LEN {
            let mut buf = [0u8; INLINE_LEN];
            buf[..data.len()].copy_from_slice(data);
            PrimeKey::Inline {
                data: buf,
                len: data.len() as u8,
            }
        } else {
            PrimeKey::Heap(data.into())
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        match self {
            PrimeKey::Inline { data, len } => &data[..*len as usize],
            PrimeKey::Heap(b) => b,
        }
    }
}

impl PartialEq for PrimeKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for PrimeKey {}

impl Hash for PrimeKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl fmt::Debug for PrimeKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(self.as_bytes()) {
            Ok(s) => write!(f, "PrimeKey({:?})", s),
            Err(_) => write!(f, "PrimeKey({:?})", self.as_bytes()),
        }
    }
}

/// The value stored in the primary table. Supports multiple Redis data types.
#[derive(Debug, Clone)]
pub enum PrimeValue {
    /// Raw string bytes.
    String(Vec<u8>),
    /// Integer value (stored compactly, rendered as string for GET).
    Integer(i64),
    /// List (doubly-ended queue).
    List(std::collections::VecDeque<Vec<u8>>),
    /// Set of unique byte strings.
    Set(std::collections::HashSet<Vec<u8>>),
    /// Hash map of field -> value.
    Hash(std::collections::HashMap<Vec<u8>, Vec<u8>>),
    /// Sorted set: member -> score, with ordering by score.
    ZSet {
        members: std::collections::HashMap<Vec<u8>, f64>,
        scores: std::collections::BTreeMap<SortedSetEntry, ()>,
    },
}

/// Entry in the sorted set's BTreeMap, ordered by (score, member).
#[derive(Debug, Clone, PartialEq)]
pub struct SortedSetEntry {
    pub score: f64,
    pub member: Vec<u8>,
}

impl Eq for SortedSetEntry {}

impl PartialOrd for SortedSetEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortedSetEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| self.member.cmp(&other.member))
    }
}

impl PrimeValue {
    /// Get the Redis type name for TYPE command.
    pub fn type_name(&self) -> &'static str {
        match self {
            PrimeValue::String(_) | PrimeValue::Integer(_) => "string",
            PrimeValue::List(_) => "list",
            PrimeValue::Set(_) => "set",
            PrimeValue::Hash(_) => "hash",
            PrimeValue::ZSet { .. } => "zset",
        }
    }

    /// Get the value as bytes (for string types).
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            PrimeValue::String(s) => s.clone(),
            PrimeValue::Integer(n) => n.to_string().into_bytes(),
            _ => Vec::new(),
        }
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self {
            PrimeValue::Integer(n) => Some(*n),
            PrimeValue::String(s) => std::str::from_utf8(s).ok()?.trim().parse().ok(),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            PrimeValue::Integer(n) => Some(*n as f64),
            PrimeValue::String(s) => std::str::from_utf8(s).ok()?.trim().parse().ok(),
            _ => None,
        }
    }

    pub fn strlen(&self) -> usize {
        match self {
            PrimeValue::String(s) => s.len(),
            PrimeValue::Integer(n) => n.to_string().len(),
            _ => 0,
        }
    }

    pub fn is_string(&self) -> bool {
        matches!(self, PrimeValue::String(_) | PrimeValue::Integer(_))
    }

    /// Ensure the value is stored as a String variant (not Integer).
    /// This is needed for operations like APPEND or SETRANGE that modify bytes directly.
    pub fn ensure_string(&mut self) {
        if let PrimeValue::Integer(n) = self {
            *self = PrimeValue::String(n.to_string().into_bytes());
        }
    }

    /// Set from bytes, auto-detecting integer encoding.
    pub fn from_bytes(data: &[u8]) -> Self {
        if let Ok(s) = std::str::from_utf8(data) {
            if let Ok(n) = s.trim().parse::<i64>() {
                if n.to_string().as_bytes() == data {
                    return PrimeValue::Integer(n);
                }
            }
        }
        PrimeValue::String(data.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prime_key_inline() {
        let key = PrimeKey::new(b"hello");
        assert!(matches!(key, PrimeKey::Inline { .. }));
        assert_eq!(key.as_bytes(), b"hello");
    }

    #[test]
    fn test_prime_key_heap() {
        let data = b"this is a longer key that exceeds inline";
        let key = PrimeKey::new(data);
        assert!(matches!(key, PrimeKey::Heap(_)));
        assert_eq!(key.as_bytes(), data.as_slice());
    }

    #[test]
    fn test_prime_key_equality() {
        let k1 = PrimeKey::new(b"test");
        let k2 = PrimeKey::new(b"test");
        assert_eq!(k1, k2);
    }

    #[test]
    fn test_prime_value_integer() {
        let v = PrimeValue::from_bytes(b"42");
        assert!(matches!(v, PrimeValue::Integer(42)));
        assert_eq!(v.as_bytes(), b"42");
        assert_eq!(v.as_integer(), Some(42));
    }

    #[test]
    fn test_prime_value_string() {
        let v = PrimeValue::from_bytes(b"hello");
        assert!(matches!(v, PrimeValue::String(_)));
        assert_eq!(v.as_bytes(), b"hello");
        assert_eq!(v.as_integer(), None);
    }

    #[test]
    fn test_prime_value_strlen() {
        assert_eq!(PrimeValue::Integer(100).strlen(), 3);
        assert_eq!(PrimeValue::String(b"hi".to_vec()).strlen(), 2);
    }
}
