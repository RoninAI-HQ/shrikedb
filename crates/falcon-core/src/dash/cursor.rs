/// Cursor for incremental iteration over a DashTable, used by the SCAN command.
///
/// The cursor encodes enough state to resume iteration from where it left off,
/// even if the table has grown (new segments added) between calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Cursor(u64);

impl Cursor {
    /// A cursor value of 0 means "start from the beginning".
    pub const DONE: Cursor = Cursor(0);

    pub fn new(value: u64) -> Self {
        Cursor(value)
    }

    pub fn value(self) -> u64 {
        self.0
    }

    pub fn is_done(self) -> bool {
        self.0 == 0
    }
}
