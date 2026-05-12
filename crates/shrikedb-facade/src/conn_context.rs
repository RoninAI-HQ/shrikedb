/// Per-connection context tracking auth state, selected database, etc.
#[derive(Debug)]
pub struct ConnContext {
    /// Currently selected database index (0-15).
    pub db_index: u16,
}

impl ConnContext {
    pub fn new() -> Self {
        Self { db_index: 0 }
    }
}

impl Default for ConnContext {
    fn default() -> Self {
        Self::new()
    }
}
