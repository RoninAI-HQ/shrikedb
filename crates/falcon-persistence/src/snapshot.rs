use std::fs;
use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};

use falcon_core::compact_obj::{PrimeKey, PrimeValue};

use crate::rdb_load::{self, RdbEntry, RdbLoadError};
use crate::rdb_save::RdbSaver;

/// Data from a single shard needed to create a snapshot.
#[derive(Debug)]
pub struct ShardSnapshot {
    /// (db_index, key_bytes, value, expire_ms)
    pub entries: Vec<(u16, Vec<u8>, PrimeValue, Option<u64>)>,
}

/// Save a complete snapshot to an RDB file.
/// Takes snapshots from all shards and merges them into one file.
pub fn save_rdb(
    path: &Path,
    shard_snapshots: &[ShardSnapshot],
) -> Result<u64, io::Error> {
    // Write to a temp file first, then rename atomically
    let tmp_path = path.with_extension("rdb.tmp");
    let file = fs::File::create(&tmp_path)?;
    let writer = BufWriter::new(file);
    let mut saver = RdbSaver::new(writer);

    saver.write_header()?;
    saver.write_aux("redis-ver", "7.0.0")?;
    saver.write_aux("falcon-ver", "0.1.0")?;

    // Group entries by database
    for db_idx in 0..16u16 {
        let mut db_entries: Vec<(&[u8], &PrimeValue, Option<u64>)> = Vec::new();
        let mut expire_count = 0u64;

        for snapshot in shard_snapshots {
            for (entry_db, key, value, expire_ms) in &snapshot.entries {
                if *entry_db == db_idx {
                    db_entries.push((key.as_slice(), value, *expire_ms));
                    if expire_ms.is_some() {
                        expire_count += 1;
                    }
                }
            }
        }

        if db_entries.is_empty() {
            continue;
        }

        saver.write_select_db(db_idx as u32)?;
        saver.write_resize_db(db_entries.len() as u64, expire_count)?;

        for (key, value, expire_ms) in &db_entries {
            saver.write_key_value(&PrimeKey::new(key), value, *expire_ms)?;
        }
    }

    saver.write_eof()?;
    saver.flush()?;
    let bytes = saver.bytes_written();

    // Atomic rename
    fs::rename(&tmp_path, path)?;

    Ok(bytes)
}

/// Load an RDB file and return all entries.
pub fn load_rdb(path: &Path) -> Result<Vec<RdbEntry>, RdbLoadError> {
    let file = fs::File::open(path).map_err(RdbLoadError::Io)?;
    let reader = io::BufReader::new(file);
    rdb_load::load_all(reader)
}

/// Get the default RDB file path.
pub fn default_rdb_path() -> PathBuf {
    PathBuf::from("dump.rdb")
}
