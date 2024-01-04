use std::fs::File;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bitcasky_common::options::BitcaskyOptions;
use log::{debug, error};
use parking_lot::RwLock;
use uuid::Uuid;

use crate::error::{BitcaskyError, BitcaskyResult};
use crate::keydir::{KeyDir, KeyDirTelemetry};
use crate::merge::{MergeManager, MergeManagerTelemetry};
use bitcasky_common::{
    fs::{self},
    storage_id::StorageIdGenerator,
};
use bitcasky_database::{deleted_value, Database, DatabaseTelemetry, TimedValue};

#[derive(Debug)]
pub struct BitcaskTelemetry {
    pub keydir: KeyDirTelemetry,
    pub database: DatabaseTelemetry,
    pub merge_manager: MergeManagerTelemetry,
}

pub struct Bitcasky {
    instance_id: String,
    directory_lock_file: File,
    keydir: RwLock<KeyDir>,
    options: Arc<BitcaskyOptions>,
    database: Database,
    merge_manager: MergeManager,
}

impl Bitcasky {
    /// Open opens the database at the given path with optional options.
    pub fn open(directory: &Path, options: BitcaskyOptions) -> BitcaskyResult<Bitcasky> {
        let directory_lock_file = match fs::lock_directory(directory)? {
            Some(f) => f,
            None => {
                return Err(BitcaskyError::LockDirectoryFailed(
                    directory.display().to_string(),
                ));
            }
        };

        validate_database_directory(directory)?;

        let options = Arc::new(options);
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

        debug!(target: "Bitcasky", "Bitcask created. instanceId: {}", id);
        Ok(Bitcasky {
            instance_id: id.to_string(),
            directory_lock_file,
            keydir,
            database,
            options,
            merge_manager,
        })
    }

    /// Stores the key and value in the database.
    pub fn put<K: AsRef<[u8]>, V: Deref<Target = [u8]>>(
        &self,
        key: K,
        value: V,
    ) -> BitcaskyResult<()> {
        self.do_put(key, TimedValue::immortal_value(value))
    }

    /// Stores the key, value in the database and set a expire time with this value.
    pub fn put_with_ttl<K: AsRef<[u8]>, V: Deref<Target = [u8]>>(
        &self,
        key: K,
        value: V,
        ttl: Duration,
    ) -> BitcaskyResult<()> {
        if ttl.is_zero() {
            return Err(BitcaskyError::InvalidParameter(
                "ttl".into(),
                "ttl cannot be zero".into(),
            ));
        }

        let expire_timestamp =
            (SystemTime::now().duration_since(UNIX_EPOCH).unwrap() + ttl).as_millis() as u64;

        self.do_put(key, TimedValue::expirable_value(value, expire_timestamp))
    }

    /// Fetches value for a key
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> BitcaskyResult<Option<Vec<u8>>> {
        self.database.check_db_error()?;

        let row_pos = {
            self.keydir
                .read()
                .get(&key.as_ref().into())
                .map(|r| *r.value())
        };

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
    pub fn has(&self, key: &Vec<u8>) -> BitcaskyResult<bool> {
        self.database.check_db_error()?;

        Ok(self.keydir.read().get(key).map(|r| *r.value()).is_some())
    }

    /// Iterates all the keys in database and apply each of them to the function f
    pub fn foreach_key<F>(&self, mut f: F) -> BitcaskyResult<()>
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
    pub fn fold_key<T, F>(&self, mut f: F, init: Option<T>) -> BitcaskyResult<Option<T>>
    where
        F: FnMut(&Vec<u8>, Option<T>) -> BitcaskyResult<Option<T>>,
    {
        self.database.check_db_error()?;
        let mut acc = init;
        for kd in self.keydir.read().iter() {
            acc = f(kd.key(), acc)?;
        }
        Ok(acc)
    }

    /// Iterates all the key value pair in database and apply each of them to the function f
    pub fn foreach<F>(&self, mut f: F) -> BitcaskyResult<()>
    where
        F: FnMut(&Vec<u8>, &Vec<u8>),
    {
        self.database.check_db_error()?;
        let _kd = self.keydir.read();
        for row_ret in self.database.iter()? {
            if let Ok(row) = row_ret {
                f(&row.key, &row.value.value);
            } else {
                return Err(BitcaskyError::DatabaseError(row_ret.unwrap_err()));
            }
        }

        Ok(())
    }

    /// Iterates all the key value pair in database and apply them to the function f with a initial accumulator.
    pub fn fold<T, F>(&self, mut f: F, init: Option<T>) -> BitcaskyResult<Option<T>>
    where
        F: FnMut(&Vec<u8>, &Vec<u8>, Option<T>) -> BitcaskyResult<Option<T>>,
    {
        self.database.check_db_error()?;
        let _kd = self.keydir.read();
        let mut acc = init;
        for row_ret in self.database.iter()? {
            if let Ok(row) = row_ret {
                acc = f(&row.key, &row.value.value, acc)?;
            } else {
                return Err(BitcaskyError::DatabaseError(row_ret.unwrap_err()));
            }
        }
        Ok(acc)
    }

    /// Deletes the named key.
    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> BitcaskyResult<()> {
        self.database.check_db_error()?;
        let kd = self.keydir.write();

        if kd.contains_key(&key.as_ref().into()) {
            self.database.write(&key, deleted_value())?;
            kd.delete(&key.as_ref().into());
        }

        Ok(())
    }

    /// Drop this entire database
    pub fn drop(&self) -> BitcaskyResult<()> {
        let kd = self.keydir.write();

        if let Err(e) = self.database.drop() {
            self.database
                .mark_db_error(format!("drop database failed. {}", e));
            return Err(BitcaskyError::DatabaseError(e));
        }

        kd.clear();
        Ok(())
    }

    /// Flushes all buffers to disk ensuring all data is written
    pub fn sync(&self) -> BitcaskyResult<()> {
        Ok(self.database.sync()?)
    }

    /// Merges all datafiles in the database. Old keys are squashed and deleted keys removes.
    /// Duplicate key/value pairs are also removed. Call this function periodically to reclaim disk space.
    pub fn merge(&self) -> BitcaskyResult<()> {
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

    pub fn do_put<K: AsRef<[u8]>, V: Deref<Target = [u8]>>(
        &self,
        key: K,
        value: TimedValue<V>,
    ) -> BitcaskyResult<()> {
        if key.as_ref().len() > self.options.max_key_size {
            return Err(BitcaskyError::InvalidParameter(
                "key".into(),
                "key size overflow".into(),
            ));
        }
        if value.len() > self.options.max_value_size {
            return Err(BitcaskyError::InvalidParameter(
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

        debug!(target: "Bitcasky", "put data success. key: {:?}, storage_id: {}, row_offset: {}", 
            key.as_ref(), ret.storage_id, ret.row_offset);
        kd.put(key.as_ref().into(), ret);
        Ok(())
    }
}

impl Drop for Bitcasky {
    fn drop(&mut self) {
        fs::unlock_directory(&self.directory_lock_file);
        debug!(target: "Bitcasky", "Bitcask shutdown. instanceId = {}", self.instance_id);
    }
}

fn validate_database_directory(dir: &Path) -> BitcaskyResult<()> {
    std::fs::create_dir_all(dir)?;
    if !fs::check_directory_is_writable(dir) {
        return Err(BitcaskyError::PermissionDenied(format!(
            "do not have writable permission for path: {}",
            dir.display()
        )));
    }
    Ok(())
}
