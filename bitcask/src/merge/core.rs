use std::{
    path::Path,
    sync::{Arc, Mutex, RwLock},
};

use log::{debug, error, info, warn};

use crate::{
    database::{self, DataBaseOptions, Database, MergeMeta},
    error::{BitcaskError, BitcaskResult},
    file_id::FileIdGenerator,
    fs,
    keydir::KeyDir,
    utils::is_tombstone,
};

pub struct MergeManager {
    instance_id: String,
    merge_lock: Mutex<()>,
    file_id_generator: Arc<FileIdGenerator>,
    options: DataBaseOptions,
}

impl MergeManager {
    pub fn new(
        instance_id: String,
        file_id_generator: Arc<FileIdGenerator>,
        options: DataBaseOptions,
    ) -> MergeManager {
        MergeManager {
            instance_id,
            merge_lock: Mutex::new(()),
            file_id_generator,
            options,
        }
    }
    pub fn merge(&self, database: &Database, keydir: &RwLock<KeyDir>) -> BitcaskResult<()> {
        let lock_ret = self.merge_lock.try_lock();

        if lock_ret.is_err() {
            return Err(BitcaskError::MergeInProgress());
        }

        debug!(target: "Bitcask", "Bitcask start merging. instanceId: {}", self.instance_id);

        let (kd, known_max_file_id) = self.flush_writing_file(database, keydir)?;

        debug!(target: "Bitcask", "Bitcask start merging. instanceId: {}, knownMaxFileId {}", self.instance_id, known_max_file_id);

        let merge_dir_path = database::create_merge_file_dir(database.get_database_dir())?;
        let (file_ids, new_kd) =
            self.write_merged_files(database, &merge_dir_path, &kd, known_max_file_id)?;

        info!(target: "BitcaskMerge", "database merged to files with ids: {:?}", &file_ids);

        {
            // stop read/write
            let kd = keydir.write().unwrap();
            database
                .load_merged_files(&file_ids, known_max_file_id)
                .map_err(|e| {
                    // self.mark_db_error(e.to_string());
                    error!(target: "BitcaskMerge", "database load merged files failed with error: {}", &e);
                    e
                })?;

            for (k, v) in new_kd.into_iter() {
                kd.checked_put(k, v)
            }
        }

        info!(target: "Bitcask", "purge files with id smaller than: {}", known_max_file_id);

        fs::purge_outdated_data_files(&database.database_dir, known_max_file_id)?;
        let clear_ret = fs::clear_dir(&merge_dir_path);
        if clear_ret.is_err() {
            warn!(target: "Bitcask", "clear merge directory failed. {}", clear_ret.unwrap_err());
        }
        Ok(())
    }

    fn flush_writing_file(
        &self,
        database: &Database,
        keydir: &RwLock<KeyDir>,
    ) -> BitcaskResult<(KeyDir, u32)> {
        // stop writing and switch the writing file to stable files
        let _kd = keydir.write().unwrap();
        database.flush_writing_file()?;
        let known_max_file_id = database.get_max_file_id();
        Ok((_kd.clone(), known_max_file_id))
    }

    fn write_merged_files(
        &self,
        database: &Database,
        merge_file_dir: &Path,
        key_dir_to_write: &KeyDir,
        known_max_file_id: u32,
    ) -> BitcaskResult<(Vec<u32>, KeyDir)> {
        database::write_merge_meta(merge_file_dir, MergeMeta { known_max_file_id })?;

        let new_kd = KeyDir::new_empty_key_dir();
        let merge_db =
            Database::open(merge_file_dir, self.file_id_generator.clone(), self.options)?;

        for r in key_dir_to_write.iter() {
            let k = r.key();
            let v = database.read_value(r.value())?;
            if !is_tombstone(&v) {
                let pos = merge_db.write_with_timestamp(k, &v, r.value().timestamp)?;
                new_kd.checked_put(k.clone(), pos);
                debug!(target: "Bitcask", "put data to merged file success. key: {:?}, value: {:?}, file_id: {}, row_offset: {}, row_size: {}, timestamp: {}", 
                    k, v, pos.file_id, pos.row_offset, pos.row_size, pos.timestamp);
            }
        }
        merge_db.flush_writing_file()?;
        let file_ids = merge_db.get_file_ids();
        // we do not write anything in writing file
        // so we can only use stable files
        Ok((file_ids.stable_file_ids, new_kd))
    }
}
