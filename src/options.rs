use std::time::Duration;

use common::clock::{BitcaskClock, ClockImpl, DebugClock};
use database::DatabaseOptions;

/// Bitcask optional options. Used on opening Bitcask instance.
#[derive(Debug, Clone, Copy)]
pub struct BitcaskOptions {
    pub database: DatabaseOptions,
    // maximum key size, default: 1 KB
    pub max_key_size: usize,
    // maximum value size, default: 100 KB
    pub max_value_size: usize,
    // clock to get time,
    pub clock: BitcaskClock,
}

/// Default Bitcask Options
impl Default for BitcaskOptions {
    fn default() -> Self {
        Self {
            database: DatabaseOptions::default(),
            max_key_size: 1024,
            max_value_size: 100 * 1024,
            clock: BitcaskClock::default(),
        }
    }
}

impl BitcaskOptions {
    // maximum data file size, default: 128 MB
    pub fn max_data_file_size(mut self, size: usize) -> BitcaskOptions {
        assert!(size > 0);
        self.database.storage.max_data_file_size = size;
        self
    }

    // data file initial capacity, default: 1 MB
    pub fn init_data_file_capacity(mut self, capacity: usize) -> BitcaskOptions {
        assert!(capacity > 0);
        self.database.storage.init_data_file_capacity = capacity;
        self
    }

    // hint file initial capacity, default: 1 MB
    pub fn init_hint_file_capacity(mut self, capacity: usize) -> BitcaskOptions {
        assert!(capacity > 0);
        self.database.init_hint_file_capacity = capacity;
        self
    }

    // maximum key size, default: 1 KB
    pub fn max_key_size(mut self, size: usize) -> BitcaskOptions {
        assert!(size > 0);
        self.max_key_size = size;
        self
    }

    // maximum value size, default: 100 KB
    pub fn max_value_size(mut self, size: usize) -> BitcaskOptions {
        assert!(size > 0);
        self.max_value_size = size;
        self
    }

    // How frequent can we sync data to file. 0 to stop auto sync. default: 1 min
    pub fn sync_interval(mut self, interval: Duration) -> BitcaskOptions {
        assert!(!interval.is_zero());
        self.database.sync_interval_sec = interval.as_secs();
        self
    }

    // Use debug clock
    pub fn debug_clock(mut self, clock: DebugClock) -> BitcaskOptions {
        self.clock = BitcaskClock {
            clock: ClockImpl::Debug(clock),
        };
        self
    }
}
