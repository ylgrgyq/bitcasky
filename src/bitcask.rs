use std::fs::File;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::{
    io,
    sync::atomic::{AtomicUsize, Ordering},
};

use log::{debug, error};
use parking_lot::RwLock;
use tokio::runtime::{self, Runtime};
use uuid::Uuid;

use crate::error::{BitcaskError, BitcaskResult};
use crate::keydir::KeyDir;
use crate::merge::MergeManager;
use common::{
    fs::{self},
    storage_id::StorageIdGenerator,
    tombstone::is_tombstone,
};
use database::{deleted_value, DataBaseOptions, DataStorageOptions, Database, TimedValue};

/// Bitcask optional options. Used on opening Bitcask instance.
#[derive(Debug, Clone, Copy)]
pub struct BitcaskOptions {
    // maximum datafile size
    pub max_file_size: u64,
    // maximum key size
    pub max_key_size: usize,
    // maximum value size
    pub max_value_size: usize,
    // How frequent can we sync data to file. 0 to stop auto sync
    pub sync_interval_sec: u64,
}

impl BitcaskOptions {
    fn validate(&self) -> Option<BitcaskError> {
        if self.max_file_size == 0 {
            return Some(BitcaskError::InvalidParameter(
                "max_file_size".into(),
                "need a positive value".into(),
            ));
        }
        if self.max_key_size == 0 {
            return Some(BitcaskError::InvalidParameter(
                "max_key_size".into(),
                "need a positive value".into(),
            ));
        }
        if self.max_value_size == 0 {
            return Some(BitcaskError::InvalidParameter(
                "max_value_size".into(),
                "need a positive value".into(),
            ));
        }
        None
    }

    fn get_database_options(&self) -> DataBaseOptions {
        DataBaseOptions {
            storage_options: DataStorageOptions {
                max_file_size: self.max_file_size,
            },
            sync_interval_sec: self.sync_interval_sec,
        }
    }
}

/// Default Bitcask Options
impl Default for BitcaskOptions {
    fn default() -> Self {
        Self {
            max_file_size: 128 * 1024 * 1024,
            max_key_size: 64,
            max_value_size: 100 * 1024,
            sync_interval_sec: 60,
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct BitcaskStats {
    pub number_of_data_files: usize,
    pub total_data_size_in_bytes: u64,
    pub number_of_keys: usize,
    pub number_of_pending_hint_files: usize,
}

pub struct Bitcask {
    instance_id: String,
    directory_lock_file: File,
    keydir: RwLock<KeyDir>,
    options: BitcaskOptions,
    database: Database,
    merge_manager: MergeManager,
}

impl Bitcask {
    /// Open opens the database at the given path with optional options.
    pub fn open(directory: &Path, options: BitcaskOptions) -> BitcaskResult<Bitcask> {
        let valid_opt = options.validate();
        if let Some(e) = valid_opt {
            return Err(e);
        }
        let directory_lock_file = match fs::lock_directory(directory)? {
            Some(f) => f,
            None => {
                return Err(BitcaskError::LockDirectoryFailed(
                    directory.display().to_string(),
                ));
            }
        };

        validate_database_directory(directory)?;

        let id = Uuid::new_v4();
        let storage_id_generator = Arc::new(StorageIdGenerator::default());
        let merge_manager = MergeManager::new(
            id.to_string(),
            directory,
            storage_id_generator.clone(),
            options.get_database_options(),
        );
        merge_manager.recover_merge()?;

        let database = Database::open(
            directory,
            storage_id_generator,
            options.get_database_options(),
        )?;
        let keydir = RwLock::new(KeyDir::new(&database)?);

        debug!(target: "Bitcask", "Bitcask created. instanceId: {}", id);
        Ok(Bitcask {
            instance_id: id.to_string(),
            directory_lock_file,
            keydir,
            database,
            options,
            merge_manager,
        })
    }

    /// Stores the key and value in the database.
    pub fn put<V: Deref<Target = [u8]>>(&self, key: Vec<u8>, value: V) -> BitcaskResult<()> {
        if key.len() > self.options.max_key_size {
            return Err(BitcaskError::InvalidParameter(
                "key".into(),
                "key size overflow".into(),
            ));
        }
        if value.len() > self.options.max_value_size {
            return Err(BitcaskError::InvalidParameter(
                "value".into(),
                "values size overflow".into(),
            ));
        }

        self.database.check_db_error()?;

        let kd = self.keydir.write();
        let ret = self
            .database
            .write(&key, TimedValue::immortal_value(value))
            .map_err(|e| {
                error!(target: "BitcaskPut", "put data failed with error: {}", &e);

                self.database.mark_db_error(e.to_string());
                e
            })?;

        debug!(target: "Bitcask", "put data success. key: {:?}, storage_id: {}, row_offset: {}", 
            key, ret.storage_id, ret.row_offset);
        kd.put(key, ret);
        Ok(())
    }

    /// Fetches value for a key
    pub fn get(&self, key: &Vec<u8>) -> BitcaskResult<Option<Vec<u8>>> {
        self.database.check_db_error()?;

        let row_pos = { self.keydir.read().get(key).map(|r| *r.value()) };

        match row_pos {
            Some(e) => {
                let v = self.database.read_value(&e)?;
                if is_tombstone(&v) {
                    return Ok(None);
                }
                Ok(Some(v.value.to_vec()))
            }
            None => Ok(None),
        }
    }

    /// Returns true if the key exists in the database, false otherwise.
    pub fn has(&self, key: &Vec<u8>) -> BitcaskResult<bool> {
        self.database.check_db_error()?;

        Ok(self.keydir.read().get(key).map(|r| *r.value()).is_some())
    }

    /// Iterates all the keys in database and apply each of them to the function f
    pub fn foreach_key<F>(&self, mut f: F) -> BitcaskResult<()>
    where
        F: FnMut(&Vec<u8>),
    {
        self.database.check_db_error()?;
        let kd = self.keydir.read();
        for k in kd.iter() {
            f(k.key());
        }
        Ok(())
    }

    /// Iterates all the keys in database and apply them to the function f with a initial accumulator.
    pub fn fold_key<T, F>(&self, mut f: F, init: Option<T>) -> BitcaskResult<Option<T>>
    where
        F: FnMut(&Vec<u8>, Option<T>) -> BitcaskResult<Option<T>>,
    {
        self.database.check_db_error()?;
        let mut acc = init;
        for kd in self.keydir.read().iter() {
            acc = f(kd.key(), acc)?;
        }
        Ok(acc)
    }

    /// Iterates all the key value pair in database and apply each of them to the function f
    pub fn foreach<F>(&self, mut f: F) -> BitcaskResult<()>
    where
        F: FnMut(&Vec<u8>, &Vec<u8>),
    {
        self.database.check_db_error()?;
        let _kd = self.keydir.read();
        for row_ret in self.database.iter()? {
            if let Ok(row) = row_ret {
                f(&row.key, &row.value);
            } else {
                return Err(BitcaskError::DatabaseError(row_ret.unwrap_err()));
            }
        }

        Ok(())
    }

    /// Iterates all the key value pair in database and apply them to the function f with a initial accumulator.
    pub fn fold<T, F>(&self, mut f: F, init: Option<T>) -> BitcaskResult<Option<T>>
    where
        F: FnMut(&Vec<u8>, &Vec<u8>, Option<T>) -> BitcaskResult<Option<T>>,
    {
        self.database.check_db_error()?;
        let _kd = self.keydir.read();
        let mut acc = init;
        for row_ret in self.database.iter()? {
            if let Ok(row) = row_ret {
                acc = f(&row.key, &row.value, acc)?;
            } else {
                return Err(BitcaskError::DatabaseError(row_ret.unwrap_err()));
            }
        }
        Ok(acc)
    }

    /// Deletes the named key.
    pub fn delete(&self, key: &Vec<u8>) -> BitcaskResult<()> {
        self.database.check_db_error()?;
        let kd = self.keydir.write();

        if kd.contains_key(key) {
            self.database.write(key, deleted_value())?;
            kd.delete(key);
        }

        Ok(())
    }

    /// Drop this entire database
    pub fn drop(&self) -> BitcaskResult<()> {
        let kd = self.keydir.write();

        if let Err(e) = self.database.drop() {
            self.database
                .mark_db_error(format!("drop database failed. {}", e));
            return Err(BitcaskError::DatabaseError(e));
        }

        kd.clear();
        Ok(())
    }

    /// Flushes all buffers to disk ensuring all data is written
    pub fn sync(&self) -> BitcaskResult<()> {
        Ok(self.database.sync()?)
    }

    /// Merges all datafiles in the database. Old keys are squashed and deleted keys removes.
    /// Duplicate key/value pairs are also removed. Call this function periodically to reclaim disk space.
    pub fn merge(&self) -> BitcaskResult<()> {
        self.database.check_db_error()?;

        self.merge_manager.merge(&self.database, &self.keydir)
    }

    /// Returns statistics about the database, like the number of data files,
    /// keys and overall size on disk of the data
    pub fn stats(&self) -> BitcaskResult<BitcaskStats> {
        let kd = self.keydir.read();
        let key_size = kd.len();
        let db_stats = self.database.stats()?;
        Ok(BitcaskStats {
            number_of_data_files: db_stats.number_of_data_files,
            number_of_pending_hint_files: db_stats.number_of_pending_hint_files,
            total_data_size_in_bytes: db_stats.total_data_size_in_bytes,
            number_of_keys: key_size,
        })
    }
}

impl Drop for Bitcask {
    fn drop(&mut self) {
        fs::unlock_directory(&self.directory_lock_file);
        debug!(target: "Bitcask", "Bitcask shutdown. instanceId = {}", self.instance_id);
    }
}

fn validate_database_directory(dir: &Path) -> BitcaskResult<()> {
    std::fs::create_dir_all(dir)?;
    if !fs::check_directory_is_writable(dir) {
        return Err(BitcaskError::PermissionDenied(format!(
            "do not have writable permission for path: {}",
            dir.display()
        )));
    }
    Ok(())
}

pub fn create_optimization_runtime(max_optimization_threads: usize) -> io::Result<Runtime> {
    let mut optimize_runtime_builder = runtime::Builder::new_multi_thread();

    optimize_runtime_builder
        .enable_time()
        .thread_name_fn(move || {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let optimizer_id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("optimize-{optimizer_id}")
        });

    if max_optimization_threads > 0 {
        // panics if val is not larger than 0.
        optimize_runtime_builder.max_blocking_threads(max_optimization_threads);
    }
    optimize_runtime_builder.build()
}
