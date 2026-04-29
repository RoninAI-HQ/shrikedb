use falcon_core::compact_obj::{PrimeKey, PrimeValue};
use falcon_core::dash::table::DashTable;

/// Type alias for the primary key-value table.
pub type PrimeTable = DashTable<PrimeKey, PrimeValue>;

/// Type alias for the expiration table: maps keys to expiry timestamps (milliseconds since epoch).
pub type ExpireTable = DashTable<PrimeKey, u64>;

/// A single database (Redis DB 0-15). Contains the primary table and an expire table.
pub struct DbTable {
    pub prime: PrimeTable,
    pub expire: ExpireTable,
}

impl DbTable {
    pub fn new() -> Self {
        Self {
            prime: PrimeTable::new(),
            expire: ExpireTable::new(),
        }
    }

    pub fn clear(&mut self) {
        self.prime.clear();
        self.expire.clear();
    }
}

impl Default for DbTable {
    fn default() -> Self {
        Self::new()
    }
}
