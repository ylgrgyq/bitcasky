use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use log::{error, info};

use crate::database::{DataBaseOptions, Database};
use crate::error::{BitcaskError, BitcaskResult};
use crate::file_id::FileIdGenerator;
use crate::file_manager::{self, MergeMeta};
use crate::keydir::KeyDir;
use crate::utils::{is_tombstone, TOMBSTONE_VALUE};

pub const DEFAULT_BITCASK_OPTIONS: BitcaskOptions = BitcaskOptions {
    max_file_size: 128 * 1024 * 1024,
    max_key_size: 64,
    max_value_size: 100 * 1024,
    tolerate_data_file_corrption: true,
};

#[derive(Debug, Clone, Copy)]
pub struct BitcaskOptions {
    pub max_file_size: usize,
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

#[derive(PartialEq)]
enum FoldStatus {
    Stopped,
    Continue,
}
pub struct FoldResult<T> {
    accumulator: T,
    status: FoldStatus,
}

#[derive(Debug)]
pub struct BitcaskStats {
    pub number_of_data_files: usize,
    pub number_of_hint_files: usize,
    pub total_data_size_in_bytes: u64,
    pub number_of_keys: usize,
}

pub struct Bitcask {
    directory_lock_file: File,
    keydir: RwLock<KeyDir>,
    file_id_generator: Arc<FileIdGenerator>,
    options: BitcaskOptions,
    database: Database,
    merge_lock: Mutex<()>,
    is_error: Mutex<Option<String>>,
}

impl Bitcask {
    pub fn open(directory: &Path, options: BitcaskOptions) -> BitcaskResult<Bitcask> {
        let valid_opt = options.validate();
        if let Some(e) = valid_opt {
            return Err(e);
        }
        let directory_lock_file = match file_manager::lock_directory(directory)? {
            Some(f) => f,
            None => {
                return Err(BitcaskError::LockDirectoryFailed(
                    directory.display().to_string(),
                ));
            }
        };

        let file_id_generator = Arc::new(FileIdGenerator::new());
        let database = Database::open(
            directory,
            file_id_generator.clone(),
            options.get_database_options(),
        )?;
        let keydir = KeyDir::new(&database)?;
        Ok(Bitcask {
            directory_lock_file,
            keydir: RwLock::new(keydir),
            file_id_generator,
            database,
            options,
            merge_lock: Mutex::new(()),
            is_error: Mutex::new(None),
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

        self.check_db_error()?;

        let kd = self.keydir.write().unwrap();
        let ret = self.database.write(&key, value).map_err(|e| {
            error!(target: "BitcaskPut", "put data failed with error: {}", &e);

            if match e {
                BitcaskError::DataFileCorrupted(_, _, _) => {
                    !self.options.tolerate_data_file_corrption
                }
                _ => true,
            } {
                self.mark_db_error(e.to_string());
            }
            e
        })?;

        kd.put(key, ret);
        Ok(())
    }

    pub fn get(&self, key: &Vec<u8>) -> BitcaskResult<Option<Vec<u8>>> {
        self.check_db_error()?;

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

    pub fn foreach_key<T>(&self, func: fn(key: &Vec<u8>) -> FoldResult<T>) {
        let kd = self.keydir.read().unwrap();
        for r in kd.iter() {
            if func(r.key()).status == FoldStatus::Stopped {
                break;
            }
        }
    }

    pub fn delete(&self, key: &Vec<u8>) -> BitcaskResult<()> {
        self.check_db_error()?;
        let kd = self.keydir.write().unwrap();

        if kd.contains_key(key) {
            self.database.write(key, TOMBSTONE_VALUE.as_bytes())?;
            kd.delete(key);
        }

        Ok(())
    }

    pub fn merge(&self) -> BitcaskResult<()> {
        self.check_db_error()?;

        let lock_ret = self.merge_lock.try_lock();

        if lock_ret.is_err() {
            return Err(BitcaskError::MergeInProgress());
        }

        let (kd, known_max_file_id) = self.flush_writing_file()?;
        let dir_path = file_manager::create_merge_file_dir(self.database.get_database_dir())?;
        let (file_ids, new_kd) = self.write_merged_files(&dir_path, &kd, known_max_file_id)?;

        info!(target: "BitcaskMerge", "database merged to files with ids: {:?}", &file_ids);

        {
            // stop read/write
            let kd = self.keydir.write().unwrap();
            self.database
                .load_merged_files(&file_ids, known_max_file_id)
                .map_err(|e| {
                    self.mark_db_error(e.to_string());
                    error!(target: "BitcaskMerge", "database load merged files failed with error: {}", &e);
                    e
                })?;

            for (k, v) in new_kd.into_iter() {
                kd.checked_put(k, v)
            }
        }

        info!(target: "BitcaskMerge", "purge files with id smaller than: {}", known_max_file_id);

        file_manager::purge_outdated_data_files(&self.database.database_dir, known_max_file_id)?;
        Ok(())
    }

    pub fn stats(&self) -> BitcaskResult<BitcaskStats> {
        let kd = self.keydir.read().unwrap();
        let key_size = kd.len();
        let db_stats = self.database.stats()?;
        Ok(BitcaskStats {
            number_of_data_files: db_stats.number_of_data_files,
            number_of_hint_files: db_stats.number_of_hint_files,
            total_data_size_in_bytes: db_stats.total_data_size_in_bytes,
            number_of_keys: key_size,
        })
    }

    fn mark_db_error(&self, error_string: String) {
        let mut err = self.is_error.lock().expect("lock db is error mutex failed");
        *err = Some(error_string)
    }

    fn check_db_error(&self) -> Result<(), BitcaskError> {
        let err = self.is_error.lock().expect("lock db is error mutex failed");
        if err.is_some() {
            return Err(BitcaskError::DatabaseBroken(err.as_ref().unwrap().clone()));
        }
        Ok(())
    }

    fn flush_writing_file(&self) -> BitcaskResult<(KeyDir, u32)> {
        // stop writing and switch the writing file to stable files
        let _kd = self.keydir.write().unwrap();
        self.database.flush_writing_file()?;
        let known_max_file_id = self.database.get_max_file_id();
        Ok((_kd.clone(), known_max_file_id))
    }

    fn write_merged_files(
        &self,
        merge_file_dir: &Path,
        key_dir_to_write: &KeyDir,
        known_max_file_id: u32,
    ) -> BitcaskResult<(Vec<u32>, KeyDir)> {
        file_manager::write_merge_meta(merge_file_dir, MergeMeta { known_max_file_id })?;

        let new_kd = KeyDir::new_empty_key_dir();
        let merge_db = Database::open(
            merge_file_dir,
            self.file_id_generator.clone(),
            self.options.get_database_options(),
        )?;

        for r in key_dir_to_write.iter() {
            let k = r.key();
            let v = self.database.read_value(r.value())?;
            if !is_tombstone(&v) {
                let pos = merge_db.write_with_timestamp(k, &v, r.value().tstmp)?;
                new_kd.checked_put(k.clone(), pos)
            }
        }
        merge_db.flush_writing_file()?;
        let file_ids = merge_db.get_file_ids();
        Ok((file_ids, new_kd))
    }
}

impl Drop for Bitcask {
    fn drop(&mut self) {
        file_manager::unlock_directory(&self.directory_lock_file);
    }
}
