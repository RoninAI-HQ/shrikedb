use crate::db_slice::DbSlice;

/// Per-thread shard that owns a DbSlice and execution statistics.
/// In the shared-nothing architecture, each shard is accessed by exactly one thread.
pub struct EngineShard {
    pub db_slice: DbSlice,
    pub stats: ShardStats,
}

#[derive(Debug, Default)]
pub struct ShardStats {
    pub commands_processed: u64,
}

impl EngineShard {
    pub fn new() -> Self {
        Self {
            db_slice: DbSlice::new(),
            stats: ShardStats::default(),
        }
    }

    /// Update the shard's time (call periodically from the event loop).
    pub fn update_time(&mut self, now_ms: u64) {
        self.db_slice.update_time(now_ms);
    }
}

impl Default for EngineShard {
    fn default() -> Self {
        Self::new()
    }
}
