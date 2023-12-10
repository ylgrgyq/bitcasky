use std::time::Duration;

use crate::clock::{BitcaskClock, ClockImpl, DebugClock};

#[derive(Debug, Clone, Copy)]
pub enum DataSotrageType {
    Mmap,
}

#[derive(Debug, Clone, Copy)]
pub struct DataStorageOptions {
    pub max_data_file_size: usize,
    pub init_data_file_capacity: usize,
    pub storage_type: DataSotrageType,
}

impl Default for DataStorageOptions {
    fn default() -> Self {
        Self {
            max_data_file_size: 128 * 1024 * 1024,
            init_data_file_capacity: 1024 * 1024,
            storage_type: DataSotrageType::Mmap,
        }
    }
}

impl DataStorageOptions {
    pub fn max_data_file_size(mut self, size: usize) -> DataStorageOptions {
        assert!(size > 0);
        self.max_data_file_size = size;
        self
    }

    pub fn init_data_file_capacity(mut self, capacity: usize) -> DataStorageOptions {
        assert!(capacity > 0);
        self.init_data_file_capacity = capacity;
        self
    }

    pub fn storage_type(mut self, storage_type: DataSotrageType) -> DataStorageOptions {
        self.storage_type = storage_type;
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DatabaseOptions {
    pub storage: DataStorageOptions,
    /// How frequent can we flush data
    pub sync_interval_sec: u64,
    pub init_hint_file_capacity: usize,
}

impl DatabaseOptions {
    pub fn storage(mut self, storage: DataStorageOptions) -> Self {
        self.storage = storage;
        self
    }
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            storage: DataStorageOptions::default(),
            init_hint_file_capacity: 1024 * 1024,
            sync_interval_sec: 60,
        }
    }
}

/// Bitcask optional options. Used on opening Bitcask instance.
#[derive(Debug, Clone)]
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

    pub fn storage_type(mut self, storage_type: DataSotrageType) -> BitcaskOptions {
        self.database.storage.storage_type = storage_type;
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
