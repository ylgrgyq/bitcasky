use std::fs::File;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::options::BitcaskOptions;
use log::{debug, error};
use parking_lot::RwLock;
use uuid::Uuid;

use crate::error::{BitcaskError, BitcaskResult};
use crate::keydir::{KeyDir, KeyDirTelemetry};
use crate::merge::{MergeManager, MergeManagerTelemetry};
use common::{
    fs::{self},
    storage_id::StorageIdGenerator,
};
use database::{deleted_value, Database, DatabaseTelemetry, TimedValue};

#[derive(Debug)]
pub struct BitcaskTelemetry {
    pub keydir: KeyDirTelemetry,
    pub database: DatabaseTelemetry,
    pub merge_manager: MergeManagerTelemetry,
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
            options.clone(),
        );
        merge_manager.recover_merge()?;

        let database = Database::open(directory, storage_id_generator, options.clone())?;
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
        self.do_put(key, TimedValue::immortal_value(value))
    }

    /// Stores the key, value in the database and set a expire time with this value.
    pub fn put_with_ttl<V: Deref<Target = [u8]>>(
        &self,
        key: Vec<u8>,
        value: V,
        ttl: Duration,
    ) -> BitcaskResult<()> {
        if ttl.is_zero() {
            return Err(BitcaskError::InvalidParameter(
                "ttl".into(),
                "ttl cannot be zero".into(),
            ));
        }

        let expire_timestamp =
            (SystemTime::now().duration_since(UNIX_EPOCH).unwrap() + ttl).as_millis() as u64;

        self.do_put(key, TimedValue::expirable_value(value, expire_timestamp))
    }

    /// Fetches value for a key
    pub fn get(&self, key: &Vec<u8>) -> BitcaskResult<Option<Vec<u8>>> {
        self.database.check_db_error()?;

        let row_pos = { self.keydir.read().get(key).map(|r| *r.value()) };

        match row_pos {
            Some(e) => {
                if let Some(v) = self.database.read_value(&e)? {
                    return Ok(Some(v.value.to_vec()));
                }
                Ok(None)
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
                f(&row.key, &row.value.value);
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
                acc = f(&row.key, &row.value.value, acc)?;
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
    pub fn get_telemetry_data(&self) -> BitcaskTelemetry {
        let kd = self.keydir.read();
        let keydir = kd.get_telemetry_data();
        BitcaskTelemetry {
            keydir,
            database: self.database.get_telemetry_data(),
            merge_manager: self.merge_manager.get_telemetry_data(),
        }
    }

    pub fn do_put<V: Deref<Target = [u8]>>(
        &self,
        key: Vec<u8>,
        value: TimedValue<V>,
    ) -> BitcaskResult<()> {
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
        let ret = self.database.write(&key, value).map_err(|e| {
            error!(target: "BitcaskPut", "put data failed with error: {}", &e);

            self.database.mark_db_error(e.to_string());
            e
        })?;

        debug!(target: "Bitcask", "put data success. key: {:?}, storage_id: {}, row_offset: {}", 
            key, ret.storage_id, ret.row_offset);
        kd.put(key, ret);
        Ok(())
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
