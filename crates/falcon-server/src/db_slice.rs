use falcon_core::compact_obj::{PrimeKey, PrimeValue};
use falcon_core::dash::cursor::Cursor;

use crate::db_table::DbTable;

const NUM_DBS: usize = 16;

pub struct DbSlice {
    dbs: Vec<DbTable>,
    now_ms: u64,
}

pub struct FindResult<'a> {
    pub value: &'a PrimeValue,
    pub has_expire: bool,
}

pub struct FindResultMut<'a> {
    pub value: &'a mut PrimeValue,
    pub has_expire: bool,
}

/// Check if a key is expired and remove it. Returns true if expired.
fn check_and_expire(db: &mut DbTable, pk: &PrimeKey, now_ms: u64) -> bool {
    if let Some(&deadline) = db.expire.find(pk) {
        if now_ms >= deadline {
            db.prime.remove(pk);
            db.expire.remove(pk);
            return true;
        }
    }
    false
}

impl DbSlice {
    pub fn new() -> Self {
        let mut dbs = Vec::with_capacity(NUM_DBS);
        for _ in 0..NUM_DBS {
            dbs.push(DbTable::new());
        }
        Self { dbs, now_ms: 0 }
    }

    pub fn update_time(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    pub fn now_ms(&self) -> u64 {
        self.now_ms
    }

    pub fn db(&self, index: u16) -> &DbTable {
        &self.dbs[index as usize]
    }

    pub fn db_mut(&mut self, index: u16) -> &mut DbTable {
        &mut self.dbs[index as usize]
    }

    pub fn db_size(&self, index: u16) -> usize {
        self.dbs[index as usize].prime.len()
    }

    pub fn find(&mut self, db_index: u16, key: &[u8]) -> Option<FindResult<'_>> {
        let pk = PrimeKey::new(key);
        let db = &mut self.dbs[db_index as usize];

        if check_and_expire(db, &pk, self.now_ms) {
            return None;
        }

        db.prime.find(&pk).map(|value| {
            let has_expire = db.expire.contains(&pk);
            FindResult { value, has_expire }
        })
    }

    pub fn find_mut(&mut self, db_index: u16, key: &[u8]) -> Option<FindResultMut<'_>> {
        let pk = PrimeKey::new(key);
        let db = &mut self.dbs[db_index as usize];

        if check_and_expire(db, &pk, self.now_ms) {
            return None;
        }

        let has_expire = db.expire.contains(&pk);
        db.prime
            .find_mut(&pk)
            .map(|value| FindResultMut { value, has_expire })
    }

    pub fn add_or_update(&mut self, db_index: u16, key: &[u8], value: PrimeValue) -> bool {
        let pk = PrimeKey::new(key);
        let db = &mut self.dbs[db_index as usize];
        db.prime.insert(pk, value).is_none()
    }

    pub fn add_if_absent(&mut self, db_index: u16, key: &[u8], value: PrimeValue) -> bool {
        let pk = PrimeKey::new(key);
        let db = &mut self.dbs[db_index as usize];

        if check_and_expire(db, &pk, self.now_ms) {
            // Key expired, treat as absent
        } else if db.prime.contains(&pk) {
            return false;
        }

        db.prime.insert(pk, value);
        true
    }

    pub fn del(&mut self, db_index: u16, key: &[u8]) -> bool {
        let pk = PrimeKey::new(key);
        let db = &mut self.dbs[db_index as usize];
        db.expire.remove(&pk);
        db.prime.remove(&pk).is_some()
    }

    pub fn exists(&mut self, db_index: u16, key: &[u8]) -> bool {
        self.find(db_index, key).is_some()
    }

    pub fn add_expire(&mut self, db_index: u16, key: &[u8], deadline_ms: u64) {
        let pk = PrimeKey::new(key);
        let db = &mut self.dbs[db_index as usize];
        if db.prime.contains(&pk) {
            db.expire.insert(pk, deadline_ms);
        }
    }

    pub fn remove_expire(&mut self, db_index: u16, key: &[u8]) -> bool {
        let pk = PrimeKey::new(key);
        self.dbs[db_index as usize].expire.remove(&pk).is_some()
    }

    pub fn ttl_ms(&mut self, db_index: u16, key: &[u8]) -> TtlResult {
        let pk = PrimeKey::new(key);
        let db = &mut self.dbs[db_index as usize];

        if !db.prime.contains(&pk) {
            return TtlResult::KeyNotFound;
        }

        match db.expire.find(&pk) {
            Some(&deadline) => {
                if self.now_ms >= deadline {
                    db.prime.remove(&pk);
                    db.expire.remove(&pk);
                    TtlResult::KeyNotFound
                } else {
                    TtlResult::Expires(deadline - self.now_ms)
                }
            }
            None => TtlResult::NoExpiry,
        }
    }

    pub fn rename(&mut self, db_index: u16, from: &[u8], to: &[u8]) -> bool {
        let from_pk = PrimeKey::new(from);
        let db = &mut self.dbs[db_index as usize];

        let expire = db.expire.remove(&from_pk);

        match db.prime.remove(&from_pk) {
            Some(value) => {
                let to_pk = PrimeKey::new(to);
                db.expire.remove(&to_pk);
                db.prime.insert(to_pk.clone(), value);
                if let Some(deadline) = expire {
                    db.expire.insert(to_pk, deadline);
                }
                true
            }
            None => false,
        }
    }

    pub fn scan(
        &self,
        db_index: u16,
        cursor: Cursor,
        count: usize,
        pattern: Option<&str>,
    ) -> (Vec<Vec<u8>>, Cursor) {
        let db = &self.dbs[db_index as usize];
        let (entries, next_cursor) = db.prime.scan(cursor, count);

        let keys: Vec<Vec<u8>> = entries
            .into_iter()
            .filter_map(|(k, _)| {
                let key_bytes = k.as_bytes();
                if let Some(pat) = pattern {
                    if glob_match(pat, key_bytes) {
                        Some(key_bytes.to_vec())
                    } else {
                        None
                    }
                } else {
                    Some(key_bytes.to_vec())
                }
            })
            .collect();

        (keys, next_cursor)
    }

    pub fn keys(&mut self, db_index: u16, pattern: &str) -> Vec<Vec<u8>> {
        let now = self.now_ms;
        let db = &mut self.dbs[db_index as usize];
        let mut result = Vec::new();
        let mut expired = Vec::new();

        for (k, _) in db.prime.iter() {
            if let Some(&deadline) = db.expire.find(k) {
                if now >= deadline {
                    expired.push(k.clone());
                    continue;
                }
            }
            if glob_match(pattern, k.as_bytes()) {
                result.push(k.as_bytes().to_vec());
            }
        }

        for k in expired {
            db.prime.remove(&k);
            db.expire.remove(&k);
        }

        result
    }

    pub fn flush_db(&mut self, db_index: u16) {
        self.dbs[db_index as usize].clear();
    }

    /// Sweep expired keys in a database. Returns count of expired keys removed.
    pub fn expire_sweep(&mut self, db_index: u16, count: usize) -> usize {
        let now = self.now_ms;
        let db = &mut self.dbs[db_index as usize];
        let mut expired_keys = Vec::new();

        let (entries, _) = db.expire.scan(Cursor::new(0), count);
        for (k, &deadline) in &entries {
            if now >= deadline {
                expired_keys.push((*k).clone());
            }
        }

        let removed = expired_keys.len();
        for k in expired_keys {
            db.prime.remove(&k);
            db.expire.remove(&k);
        }
        removed
    }
}

impl Default for DbSlice {
    fn default() -> Self {
        Self::new()
    }
}

pub enum TtlResult {
    KeyNotFound,
    NoExpiry,
    Expires(u64),
}

fn glob_match(pattern: &str, input: &[u8]) -> bool {
    let input = match std::str::from_utf8(input) {
        Ok(s) => s,
        Err(_) => return false,
    };
    glob_match_str(pattern.as_bytes(), input.as_bytes())
}

fn glob_match_str(pattern: &[u8], input: &[u8]) -> bool {
    let mut pi = 0;
    let mut ii = 0;
    let mut star_pi = usize::MAX;
    let mut star_ii = 0;

    while ii < input.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == input[ii]) {
            pi += 1;
            ii += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_ii = ii;
            pi += 1;
        } else if pi < pattern.len() && pattern[pi] == b'[' {
            pi += 1;
            let negate = pi < pattern.len() && pattern[pi] == b'^';
            if negate {
                pi += 1;
            }
            let mut matched = false;
            while pi < pattern.len() && pattern[pi] != b']' {
                if pi + 2 < pattern.len() && pattern[pi + 1] == b'-' {
                    if input[ii] >= pattern[pi] && input[ii] <= pattern[pi + 2] {
                        matched = true;
                    }
                    pi += 3;
                } else {
                    if input[ii] == pattern[pi] {
                        matched = true;
                    }
                    pi += 1;
                }
            }
            if pi < pattern.len() {
                pi += 1;
            }
            if matched == negate {
                if star_pi != usize::MAX {
                    pi = star_pi;
                    star_ii += 1;
                    ii = star_ii;
                } else {
                    return false;
                }
            } else {
                ii += 1;
            }
        } else if star_pi != usize::MAX {
            pi = star_pi;
            star_ii += 1;
            ii = star_ii;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use falcon_core::compact_obj::PrimeValue;

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    #[test]
    fn test_basic_crud() {
        let mut ds = DbSlice::new();
        ds.update_time(now());

        assert!(ds.add_or_update(0, b"key1", PrimeValue::String(b"val1".to_vec())));
        assert!(ds.exists(0, b"key1"));
        assert_eq!(ds.find(0, b"key1").unwrap().value.as_bytes(), b"val1");

        assert!(!ds.add_or_update(0, b"key1", PrimeValue::String(b"val2".to_vec())));
        assert_eq!(ds.find(0, b"key1").unwrap().value.as_bytes(), b"val2");

        assert!(ds.del(0, b"key1"));
        assert!(!ds.exists(0, b"key1"));
    }

    #[test]
    fn test_expiry() {
        let mut ds = DbSlice::new();
        let t = now();
        ds.update_time(t);

        ds.add_or_update(0, b"key1", PrimeValue::String(b"val".to_vec()));
        ds.add_expire(0, b"key1", t + 1000);
        assert!(ds.exists(0, b"key1"));

        ds.update_time(t + 2000);
        assert!(!ds.exists(0, b"key1"));
    }

    #[test]
    fn test_glob() {
        assert!(glob_match("*", b"anything"));
        assert!(glob_match("he*", b"hello"));
        assert!(glob_match("h?llo", b"hello"));
        assert!(glob_match("h[ae]llo", b"hello"));
        assert!(!glob_match("h[ae]llo", b"hillo"));
        assert!(glob_match("*", b""));
    }
}
