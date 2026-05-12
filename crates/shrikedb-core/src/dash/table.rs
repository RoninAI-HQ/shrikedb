use std::hash::{BuildHasher, Hash, Hasher};

use super::bucket;
use super::cursor::Cursor;
use super::segment::{Segment, SegmentPos};

/// DashTable: a scalable hash table with segment-based buckets.
///
/// Uses fingerprint-based lookup within segments for cache efficiency.
/// Grows by doubling the number of segments when any segment is full.
pub struct DashTable<K, V, S = ahash::RandomState> {
    segments: Vec<Segment<K, V>>,
    size: usize,
    hash_builder: S,
}

impl<K, V> DashTable<K, V, ahash::RandomState>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self::with_hasher(ahash::RandomState::new())
    }

    pub fn with_capacity(_capacity: usize) -> Self {
        Self::new()
    }
}

impl<K, V> Default for DashTable<K, V, ahash::RandomState>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, S> DashTable<K, V, S>
where
    K: Eq + Hash + Clone,
    V: Clone,
    S: BuildHasher,
{
    pub fn with_hasher(hash_builder: S) -> Self {
        let mut segments = Vec::with_capacity(4);
        for _ in 0..4 {
            segments.push(Segment::new(1));
        }
        Self {
            segments,
            size: 0,
            hash_builder,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn version(&self) -> u64 {
        self.size as u64
    }

    #[inline]
    fn hash_key(&self, key: &K) -> u64 {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[inline]
    fn seg_id(&self, hash: u64) -> usize {
        // Use high bits for segment selection (independent from bucket selection which uses bits 8+)
        (hash >> 48) as usize % self.segments.len()
    }

    /// Look up a key.
    pub fn find(&self, key: &K) -> Option<&V> {
        let hash = self.hash_key(key);
        let sid = self.seg_id(hash);
        let pos = self.segments[sid].find(hash, |k| k == key)?;
        Some(self.segments[sid].buckets[pos.bucket_idx].value(pos.slot_idx))
    }

    /// Look up a key, returning a mutable reference.
    pub fn find_mut(&mut self, key: &K) -> Option<&mut V> {
        let hash = self.hash_key(key);
        let sid = self.seg_id(hash);
        let pos = self.segments[sid].find(hash, |k| k == key)?;
        Some(self.segments[sid].buckets[pos.bucket_idx].value_mut(pos.slot_idx))
    }

    /// Insert a key-value pair. Returns the previous value if the key existed.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let hash = self.hash_key(&key);
        let sid = self.seg_id(hash);

        // Check for existing key
        if let Some(pos) = self.segments[sid].find(hash, |k| k == &key) {
            let old = std::mem::replace(
                self.segments[sid].buckets[pos.bucket_idx].value_mut(pos.slot_idx),
                value,
            );
            return Some(old);
        }

        // Try to insert
        match self.segments[sid].insert_new(key, value, hash) {
            Ok(_) => {
                self.size += 1;
                None
            }
            Err((k, v)) => {
                // Segment full — grow and retry
                self.grow();
                self.insert_no_dup(k, v, hash);
                None
            }
        }
    }

    /// Insert when we know there's no duplicate (after failed insert + grow).
    fn insert_no_dup(&mut self, mut key: K, mut value: V, hash: u64) {
        for _ in 0..10 {
            let sid = self.seg_id(hash);
            match self.segments[sid].insert_new(key, value, hash) {
                Ok(_) => {
                    self.size += 1;
                    return;
                }
                Err((k, v)) => {
                    key = k;
                    value = v;
                    self.grow();
                }
            }
        }
        panic!("DashTable: insert failed after repeated growth");
    }

    /// Remove a key. Returns the value if it existed.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let hash = self.hash_key(key);
        let sid = self.seg_id(hash);
        let pos = self.segments[sid].find(hash, |k| k == key)?;
        let (_k, v) = self.segments[sid].delete(pos, hash);
        self.size -= 1;
        Some(v)
    }

    /// Check if a key exists.
    pub fn contains(&self, key: &K) -> bool {
        self.find(key).is_some()
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.segments.clear();
        for _ in 0..4 {
            self.segments.push(Segment::new(1));
        }
        self.size = 0;
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        let mut entries: Vec<(*const K, *const V)> = Vec::with_capacity(self.size);
        for seg in &self.segments {
            seg.iter(|_bid, _slot, k, v| {
                entries.push((k as *const K, v as *const V));
            });
        }
        // SAFETY: entries reference data owned by segments which live as long as &self
        entries.into_iter().map(|(k, v)| unsafe { (&*k, &*v) })
    }

    /// Mutable iteration.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        let mut entries: Vec<(*const K, *mut V)> = Vec::with_capacity(self.size);
        for seg in &mut self.segments {
            seg.iter_mut(|_bid, _slot, k, v| {
                entries.push((k as *const K, v as *mut V));
            });
        }
        entries.into_iter().map(|(k, v)| unsafe { (&*k, &mut *v) })
    }

    /// Cursor-based scan.
    pub fn scan(&self, cursor: Cursor, count: usize) -> (Vec<(&K, &V)>, Cursor) {
        let start = cursor.value() as usize;
        let entries: Vec<(&K, &V)> = self.iter().collect();
        let total = entries.len();
        if start >= total {
            return (vec![], Cursor::DONE);
        }
        let end = (start + count).min(total);
        let result = entries[start..end].to_vec();
        let next = if end >= total {
            Cursor::DONE
        } else {
            Cursor::new(end as u64)
        };
        (result, next)
    }

    /// Retain entries matching a predicate. Returns count removed.
    pub fn retain(&mut self, mut pred: impl FnMut(&K, &mut V) -> bool) -> usize {
        let mut to_remove: Vec<(usize, usize, usize, u64)> = Vec::new();

        for (seg_idx, seg) in self.segments.iter().enumerate() {
            seg.iter(|bid, slot, k, _v| {
                let hash = {
                    let mut h = self.hash_builder.build_hasher();
                    k.hash(&mut h);
                    h.finish()
                };
                to_remove.push((seg_idx, bid, slot, hash));
            });
        }

        let mut removed = 0;
        // Process in reverse to keep slot indices valid
        for (seg_idx, bid, slot, hash) in to_remove.into_iter().rev() {
            let bucket = &mut self.segments[seg_idx].buckets[bid];
            if (bucket.slots.busy() >> slot) & 1 == 0 {
                continue;
            }
            let keep = {
                // SAFETY: key and value at different offsets, no aliasing
                let k = unsafe { &*bucket.keys[slot].as_ptr() };
                let v = unsafe { &mut *bucket.values[slot].as_mut_ptr() };
                pred(k, v)
            };
            if !keep {
                self.segments[seg_idx].delete(
                    SegmentPos { bucket_idx: bid, slot_idx: slot },
                    hash,
                );
                self.size -= 1;
                removed += 1;
            }
        }
        removed
    }

    /// Get a random entry.
    pub fn random_entry(&self) -> Option<(&K, &V)> {
        self.iter().next()
    }

    /// Grow by doubling segments and rebuilding from scratch.
    fn grow(&mut self) {
        // Collect all items
        let mut items: Vec<(K, V, u64)> = Vec::with_capacity(self.size);
        for seg in &mut self.segments {
            let mut seg_items = Vec::new();
            seg.iter(|bid, slot, k, _v| {
                let hash = {
                    let mut h = self.hash_builder.build_hasher();
                    k.hash(&mut h);
                    h.finish()
                };
                seg_items.push((bid, slot, hash));
            });
            // Delete in reverse order to keep indices valid
            seg_items.sort_by(|a, b| b.cmp(a));
            for (bid, slot, hash) in seg_items {
                if (seg.buckets[bid].slots.busy() >> slot) & 1 == 0 {
                    continue;
                }
                let (k, v) = seg.delete(SegmentPos { bucket_idx: bid, slot_idx: slot }, hash);
                items.push((k, v, hash));
            }
        }

        // Double segment count
        let new_count = self.segments.len() * 2;
        self.segments.clear();
        for _ in 0..new_count {
            self.segments.push(Segment::new(1));
        }
        self.size = 0;

        // Re-insert all items
        for (k, v, hash) in items {
            let sid = (hash >> 48) as usize % self.segments.len();
            match self.segments[sid].insert_new(k, v, hash) {
                Ok(_) => self.size += 1,
                Err(_) => panic!("DashTable: item lost during grow"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut table: DashTable<Vec<u8>, Vec<u8>> = DashTable::new();
        assert!(table.is_empty());

        table.insert(b"key1".to_vec(), b"val1".to_vec());
        assert_eq!(table.len(), 1);
        assert_eq!(table.find(&b"key1".to_vec()), Some(&b"val1".to_vec()));

        table.insert(b"key1".to_vec(), b"val2".to_vec());
        assert_eq!(table.len(), 1);
        assert_eq!(table.find(&b"key1".to_vec()), Some(&b"val2".to_vec()));

        assert_eq!(table.remove(&b"key1".to_vec()), Some(b"val2".to_vec()));
        assert!(table.is_empty());
    }

    #[test]
    fn test_many_inserts() {
        let mut table: DashTable<i32, i32> = DashTable::new();
        for i in 0..10000 {
            table.insert(i, i * 10);
        }
        assert_eq!(table.len(), 10000);

        for i in 0..10000 {
            assert_eq!(table.find(&i), Some(&(i * 10)), "key {} not found", i);
        }
    }

    #[test]
    fn test_remove() {
        let mut table: DashTable<i32, i32> = DashTable::new();
        for i in 0..1000 {
            table.insert(i, i);
        }
        for i in 0..500 {
            assert_eq!(table.remove(&i), Some(i));
        }
        assert_eq!(table.len(), 500);

        for i in 500..1000 {
            assert_eq!(table.find(&i), Some(&i));
        }
    }

    #[test]
    fn test_scan() {
        let mut table: DashTable<i32, i32> = DashTable::new();
        for i in 0..10 {
            table.insert(i, i * 10);
        }

        let mut all = vec![];
        let mut cursor = Cursor::new(0);
        loop {
            let (entries, next) = table.scan(cursor, 3);
            all.extend(entries.into_iter().map(|(&k, &v)| (k, v)));
            cursor = next;
            if cursor.is_done() {
                break;
            }
        }
        assert_eq!(all.len(), 10);
    }

    #[test]
    fn test_retain() {
        let mut table: DashTable<i32, i32> = DashTable::new();
        for i in 0..10 {
            table.insert(i, i);
        }
        let removed = table.retain(|_, v| *v % 2 == 0);
        assert_eq!(removed, 5);
        assert_eq!(table.len(), 5);
    }

    #[test]
    fn test_growth() {
        let mut table: DashTable<i32, i32> = DashTable::new();
        for i in 0..10000 {
            table.insert(i, i);
        }
        assert_eq!(table.len(), 10000);

        for i in 0..5000 {
            table.remove(&i);
        }
        assert_eq!(table.len(), 5000);

        for i in 5000..10000 {
            assert_eq!(table.find(&i), Some(&i));
        }
    }
}
