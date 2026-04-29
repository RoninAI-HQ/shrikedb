/// Number of regular slots per bucket.
pub const NUM_SLOTS: usize = 12;
/// Number of regular buckets per segment.
pub const NUM_BUCKETS: usize = 64;
/// Number of stash buckets per segment.
pub const NUM_STASH: usize = 4;
/// Total buckets per segment (regular + stash).
pub const TOTAL_BUCKETS: usize = NUM_BUCKETS + NUM_STASH;
/// Fingerprint mask (8 bits).
pub const FP_MASK: u64 = 0xFF;

/// Bitmap tracking which slots are occupied and which are "probing"
/// (hosted but not owned by this bucket). Fits in a single u32 for 12 slots.
///
/// Layout (32 bits):
///   bits 0-3:   size counter (4 bits)
///   bits 4-17:  probing mask (14 bits) - 1 if slot is probing (not owned)
///   bits 18-31: busy mask (14 bits) - 1 if slot is occupied
#[derive(Debug, Clone, Copy, Default)]
pub struct SlotBitmap(u32);

const ALLOC_MASK: u32 = (1 << NUM_SLOTS) - 1; // 0xFFF for 12 slots
const SIZE_MASK: u32 = 0xF; // 4-bit size counter

impl SlotBitmap {
    /// Returns the busy (occupied) mask. Bit i = 1 means slot i is occupied.
    #[inline]
    pub fn busy(&self) -> u32 {
        self.0 >> 18
    }

    /// Returns the probe mask (bits where probing=true XOR'd appropriately).
    /// If `probe` is true, returns mask of probing (non-owned) slots.
    /// If `probe` is false, returns mask of owned slots.
    #[inline]
    pub fn probe_mask(&self, probe: bool) -> u32 {
        let raw = (self.0 >> 4) & ALLOC_MASK;
        if probe {
            raw
        } else {
            raw ^ ALLOC_MASK
        }
    }

    /// Number of occupied slots.
    #[inline]
    pub fn size(&self) -> usize {
        (self.0 & SIZE_MASK) as usize
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.size() == NUM_SLOTS
    }

    /// Find the first empty slot. Returns None if full.
    #[inline]
    pub fn find_empty(&self) -> Option<usize> {
        let free = !self.busy() & ALLOC_MASK;
        if free == 0 {
            return None;
        }
        Some(free.trailing_zeros() as usize)
    }

    /// Mark a slot as occupied.
    #[inline]
    pub fn set_slot(&mut self, index: usize, probing: bool) {
        debug_assert!(index < NUM_SLOTS);
        // Set busy bit
        self.0 |= 1 << (18 + index);
        // Set probing bit
        if probing {
            self.0 |= 1 << (4 + index);
        } else {
            self.0 &= !(1 << (4 + index));
        }
        // Increment size
        let sz = self.0 & SIZE_MASK;
        self.0 = (self.0 & !SIZE_MASK) | (sz + 1);
    }

    /// Clear a slot.
    #[inline]
    pub fn clear_slot(&mut self, index: usize) {
        debug_assert!(index < NUM_SLOTS);
        self.0 &= !(1 << (18 + index)); // clear busy
        self.0 &= !(1 << (4 + index)); // clear probing
        let sz = self.0 & SIZE_MASK;
        self.0 = (self.0 & !SIZE_MASK) | (sz - 1);
    }

    /// Clear all slots.
    #[inline]
    pub fn clear(&mut self) {
        self.0 = 0;
    }
}

/// A bucket holds up to NUM_SLOTS key-value pairs with fingerprints and stash metadata.
pub struct Bucket<K, V> {
    pub slots: SlotBitmap,
    pub fingerprints: [u8; NUM_SLOTS],
    // Stash metadata (stored in the home bucket to track overflow items)
    pub stash_fps: [u8; NUM_STASH],      // fingerprints of stash items
    pub stash_busy: u8,                   // bit 0-3: which stash fps are active, bit 4: has_stash flag
    pub stash_pos: u8,                    // 2 bits per fp: which stash bucket (0-3)
    pub stash_probe_mask: u8,             // bit per fp: owned vs probing
    pub overflow_count: u8,               // items in stash without fp tracking
    pub keys: [std::mem::MaybeUninit<K>; NUM_SLOTS],
    pub values: [std::mem::MaybeUninit<V>; NUM_SLOTS],
}

impl<K, V> Bucket<K, V> {
    pub fn new() -> Self {
        Self {
            slots: SlotBitmap::default(),
            fingerprints: [0u8; NUM_SLOTS],
            stash_fps: [0u8; NUM_STASH],
            stash_busy: 0,
            stash_pos: 0,
            stash_probe_mask: 0,
            overflow_count: 0,
            keys: unsafe { std::mem::MaybeUninit::uninit().assume_init() },
            values: unsafe { std::mem::MaybeUninit::uninit().assume_init() },
        }
    }

    /// Find slots matching a fingerprint with the given ownership.
    /// Returns a bitmask of matching slot indices.
    #[inline]
    pub fn find_matching(&self, fp: u8, probe: bool) -> u32 {
        let busy = self.slots.busy();
        let ownership = self.slots.probe_mask(probe);

        let mut mask = 0u32;
        for i in 0..NUM_SLOTS {
            if (busy >> i) & 1 != 0
                && (ownership >> i) & 1 != 0
                && self.fingerprints[i] == fp
            {
                mask |= 1 << i;
            }
        }
        mask
    }

    /// Try to insert into this bucket. Returns slot index on success, None if full.
    #[inline]
    pub fn try_insert(&mut self, key: K, value: V, fp: u8, probing: bool) -> Option<usize> {
        let slot = self.slots.find_empty()?;
        self.fingerprints[slot] = fp;
        self.keys[slot] = std::mem::MaybeUninit::new(key);
        self.values[slot] = std::mem::MaybeUninit::new(value);
        self.slots.set_slot(slot, probing);
        Some(slot)
    }

    /// Delete a slot, returning the key and value.
    #[inline]
    pub fn delete(&mut self, slot: usize) -> (K, V) {
        debug_assert!(slot < NUM_SLOTS);
        debug_assert!((self.slots.busy() >> slot) & 1 == 1);
        self.slots.clear_slot(slot);
        let k = std::mem::replace(&mut self.keys[slot], std::mem::MaybeUninit::uninit());
        let v = std::mem::replace(&mut self.values[slot], std::mem::MaybeUninit::uninit());
        // SAFETY: slot was occupied, so key and value were initialized
        unsafe { (k.assume_init(), v.assume_init()) }
    }

    /// Get a reference to the key at a slot.
    #[inline]
    pub fn key(&self, slot: usize) -> &K {
        debug_assert!((self.slots.busy() >> slot) & 1 == 1);
        // SAFETY: slot is occupied
        unsafe { self.keys[slot].assume_init_ref() }
    }

    /// Get a reference to the value at a slot.
    #[inline]
    pub fn value(&self, slot: usize) -> &V {
        debug_assert!((self.slots.busy() >> slot) & 1 == 1);
        unsafe { self.values[slot].assume_init_ref() }
    }

    /// Get a mutable reference to the value at a slot.
    #[inline]
    pub fn value_mut(&mut self, slot: usize) -> &mut V {
        debug_assert!((self.slots.busy() >> slot) & 1 == 1);
        unsafe { self.values[slot].assume_init_mut() }
    }

    /// Check if this bucket has stash references.
    #[inline]
    pub fn has_stash(&self) -> bool {
        self.stash_busy & 0x10 != 0
    }

    /// Check if stash has overflowed (more items than fingerprint slots).
    #[inline]
    pub fn has_stash_overflow(&self) -> bool {
        self.overflow_count > 0
    }

    /// Set a stash pointer for a fingerprint.
    pub fn set_stash_ptr(&mut self, stash_bucket_id: u8, fp: u8, is_probing: bool) {
        // Find a free stash fp slot
        for i in 0..NUM_STASH {
            if self.stash_busy & (1 << i) == 0 {
                self.stash_fps[i] = fp;
                self.stash_pos =
                    (self.stash_pos & !(3 << (i * 2))) | (stash_bucket_id << (i * 2));
                self.stash_busy |= 1 << i;
                self.stash_busy |= 0x10; // has_stash flag
                if is_probing {
                    self.stash_probe_mask |= 1 << i;
                } else {
                    self.stash_probe_mask &= !(1 << i);
                }
                return;
            }
        }
        // All stash fp slots full, increment overflow
        self.overflow_count += 1;
        self.stash_busy |= 0x10;
    }

    /// Unset a stash pointer matching the given fingerprint and stash position.
    pub fn unset_stash_ptr(&mut self, fp: u8, stash_pos: u8) -> bool {
        for i in 0..NUM_STASH {
            if self.stash_busy & (1 << i) != 0
                && self.stash_fps[i] == fp
                && ((self.stash_pos >> (i * 2)) & 3) == stash_pos
            {
                self.stash_busy &= !(1 << i);
                // Clear has_stash flag if no more stash refs
                if self.stash_busy & 0x0F == 0 && self.overflow_count == 0 {
                    self.stash_busy &= !0x10;
                }
                return true;
            }
        }
        // Might be in overflow
        if self.overflow_count > 0 {
            self.overflow_count -= 1;
            if self.stash_busy & 0x0F == 0 && self.overflow_count == 0 {
                self.stash_busy &= !0x10;
            }
            return true;
        }
        false
    }
}

impl<K, V> Drop for Bucket<K, V> {
    fn drop(&mut self) {
        let busy = self.slots.busy();
        for i in 0..NUM_SLOTS {
            if (busy >> i) & 1 == 1 {
                unsafe {
                    self.keys[i].assume_init_drop();
                    self.values[i].assume_init_drop();
                }
            }
        }
    }
}

/// Compute the home bucket index from a hash.
#[inline]
pub fn home_bucket(hash: u64) -> usize {
    ((hash >> 8) as usize) % NUM_BUCKETS
}

/// Compute the fingerprint from a hash.
#[inline]
pub fn fingerprint(hash: u64) -> u8 {
    (hash & FP_MASK) as u8
}

/// Next bucket index (wraps at NUM_BUCKETS).
#[inline]
pub fn next_bucket(bid: usize) -> usize {
    if bid < NUM_BUCKETS - 1 {
        bid + 1
    } else {
        0
    }
}

/// Previous bucket index (wraps at 0).
#[inline]
pub fn prev_bucket(bid: usize) -> usize {
    if bid > 0 {
        bid - 1
    } else {
        NUM_BUCKETS - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_bitmap_basic() {
        let mut bm = SlotBitmap::default();
        assert_eq!(bm.size(), 0);
        assert!(!bm.is_full());

        bm.set_slot(0, false);
        assert_eq!(bm.size(), 1);
        assert_eq!(bm.busy() & 1, 1);

        bm.set_slot(5, true);
        assert_eq!(bm.size(), 2);
        assert_eq!((bm.busy() >> 5) & 1, 1);
        // Slot 5 is probing
        assert_eq!((bm.probe_mask(true) >> 5) & 1, 1);
        // Slot 0 is owned
        assert_eq!((bm.probe_mask(false) >> 0) & 1, 1);

        bm.clear_slot(0);
        assert_eq!(bm.size(), 1);
        assert_eq!(bm.busy() & 1, 0);
    }

    #[test]
    fn test_slot_bitmap_full() {
        let mut bm = SlotBitmap::default();
        for i in 0..NUM_SLOTS {
            assert!(!bm.is_full());
            bm.set_slot(i, false);
        }
        assert!(bm.is_full());
        assert_eq!(bm.find_empty(), None);
    }

    #[test]
    fn test_bucket_insert_find() {
        let mut bucket: Bucket<u64, u64> = Bucket::new();
        let slot = bucket.try_insert(42, 100, 0xAB, false).unwrap();
        assert_eq!(*bucket.key(slot), 42);
        assert_eq!(*bucket.value(slot), 100);

        let mask = bucket.find_matching(0xAB, false);
        assert_ne!(mask, 0);
        let found_slot = mask.trailing_zeros() as usize;
        assert_eq!(found_slot, slot);

        // Wrong fingerprint
        assert_eq!(bucket.find_matching(0xCD, false), 0);
        // Wrong ownership
        assert_eq!(bucket.find_matching(0xAB, true), 0);
    }

    #[test]
    fn test_bucket_delete() {
        let mut bucket: Bucket<String, String> = Bucket::new();
        let slot = bucket
            .try_insert("key".to_string(), "val".to_string(), 0x42, false)
            .unwrap();
        let (k, v) = bucket.delete(slot);
        assert_eq!(k, "key");
        assert_eq!(v, "val");
        assert_eq!(bucket.slots.size(), 0);
    }
}
