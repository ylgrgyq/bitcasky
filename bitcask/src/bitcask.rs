use std::fs::File;
use std::path::Path;
use std::sync::{Arc, RwLock};

use log::{debug, error};
use uuid::Uuid;

use crate::database::{DataBaseOptions, Database};
use crate::error::{BitcaskError, BitcaskResult};
use crate::file_id::FileIdGenerator;
use crate::fs::{self};
use crate::keydir::KeyDir;
use crate::merge::MergeManager;
use crate::utils::{is_tombstone, TOMBSTONE_VALUE};

#[derive(Debug, Clone, Copy)]
pub struct BitcaskOptions {
    pub max_file_size: u64,
    pub max_key_size: usize,
    pub max_value_size: usize,
    pub tolerate_data_file_corrption: bool,
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
            max_file_size: self.max_file_size,
            tolerate_data_file_corruption: self.tolerate_data_file_corrption,
        }
    }
}

impl Default for BitcaskOptions {
    fn default() -> Self {
        Self {
            max_file_size: 128 * 1024 * 1024,
            max_key_size: 64,
            max_value_size: 100 * 1024,
            tolerate_data_file_corrption: true,
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
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let merge_manager = MergeManager::new(
            id.to_string(),
            directory,
            file_id_generator.clone(),
            options.get_database_options(),
        );
        merge_manager.recover_merge()?;

        let database =
            Database::open(directory, file_id_generator, options.get_database_options())?;
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

    pub fn put(&self, key: Vec<u8>, value: &[u8]) -> BitcaskResult<()> {
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

        let kd = self.keydir.write().unwrap();
        let ret = self.database.write(&key, value).map_err(|e| {
            error!(target: "BitcaskPut", "put data failed with error: {}", &e);

            if match e {
                BitcaskError::DataFileCorrupted(_, _, _) => {
                    !self.options.tolerate_data_file_corrption
                }
                _ => true,
            } {
                self.database.mark_db_error(e.to_string());
            }
            e
        })?;

        debug!(target: "Bitcask", "put data success. key: {:?}, value: {:?}, file_id: {}, row_offset: {}, row_size: {}, timestamp: {}", 
            key, value, ret.file_id, ret.row_offset, ret.row_size, ret.timestamp);
        kd.put(key, ret);
        Ok(())
    }

    pub fn get(&self, key: &Vec<u8>) -> BitcaskResult<Option<Vec<u8>>> {
        self.database.check_db_error()?;

        let row_pos = { self.keydir.read().unwrap().get(key).map(|r| *r.value()) };

        match row_pos {
            Some(e) => {
                let v = self.database.read_value(&e)?;
                if is_tombstone(&v) {
                    return Ok(None);
                }
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }

    pub fn has(&self, key: &Vec<u8>) -> BitcaskResult<bool> {
        self.database.check_db_error()?;

        Ok(self
            .keydir
            .read()
            .unwrap()
            .get(key)
            .map(|r| *r.value())
            .is_some())
    }

    pub fn foreach_key<F>(&self, mut f: F) -> BitcaskResult<()>
    where
        F: FnMut(&Vec<u8>),
    {
        self.database.check_db_error()?;
        let kd = self.keydir.read().unwrap();
        for k in kd.iter() {
            f(k.key());
        }
        Ok(())
    }

    pub fn fold_key<T, F>(&self, mut f: F, init: Option<T>) -> BitcaskResult<Option<T>>
    where
        F: FnMut(&Vec<u8>, Option<T>) -> BitcaskResult<Option<T>>,
    {
        self.database.check_db_error()?;
        let mut acc = init;
        for kd in self.keydir.read().unwrap().iter() {
            acc = f(kd.key(), acc)?;
        }
        Ok(acc)
    }

    pub fn foreach<F>(&self, mut f: F) -> BitcaskResult<()>
    where
        F: FnMut(&Vec<u8>, &Vec<u8>),
    {
        self.database.check_db_error()?;
        let _kd = self.keydir.read().unwrap();
        for row_ret in self.database.iter()? {
            if let Ok(row) = row_ret {
                f(&row.key, &row.value);
            } else {
                return Err(row_ret.unwrap_err());
            }
        }

        Ok(())
    }

    pub fn fold<T, F>(&self, mut f: F, init: Option<T>) -> BitcaskResult<Option<T>>
    where
        F: FnMut(&Vec<u8>, &Vec<u8>, Option<T>) -> BitcaskResult<Option<T>>,
    {
        self.database.check_db_error()?;
        let _kd = self.keydir.read().unwrap();
        let mut acc = init;
        for row_ret in self.database.iter()? {
            if let Ok(row) = row_ret {
                acc = f(&row.key, &row.value, acc)?;
            } else {
                return Err(row_ret.unwrap_err());
            }
        }
        Ok(acc)
    }

    pub fn delete(&self, key: &Vec<u8>) -> BitcaskResult<()> {
        self.database.check_db_error()?;
        let kd = self.keydir.write().unwrap();

        if kd.contains_key(key) {
            self.database.write(key, TOMBSTONE_VALUE.as_bytes())?;
            kd.delete(key);
        }

        Ok(())
    }

    pub fn drop(&self) -> BitcaskResult<()> {
        let kd = self.keydir.write().unwrap();

        if let Err(e) = self.database.drop() {
            self.database
                .mark_db_error(format!("drop database failed. {}", e));
            return Err(e);
        }

        kd.clear();
        Ok(())
    }

    pub fn sync(&self) -> BitcaskResult<()> {
        self.database.sync()
    }

    pub fn merge(&self) -> BitcaskResult<()> {
        self.database.check_db_error()?;

        self.merge_manager.merge(&self.database, &self.keydir)
    }

    pub fn stats(&self) -> BitcaskResult<BitcaskStats> {
        let kd = self.keydir.read().unwrap();
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
