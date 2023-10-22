use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::{Buf, Bytes};

use log::{debug, error, info, warn};
use parking_lot::{Mutex, RwLock};

use crate::{
    database::{DataBaseOptions, Database, TimedValue},
    error::{BitcaskError, BitcaskResult},
    fs::{self, FileType},
    keydir::KeyDir,
    storage_id::{StorageId, StorageIdGenerator},
    utils::is_tombstone,
};

const MERGE_FILES_DIRECTORY: &str = "Merge";
const DEFAULT_LOG_TARGET: &str = "DatabaseMerge";

pub struct MergeManager {
    instance_id: String,
    database_dir: PathBuf,
    merge_lock: Mutex<()>,
    storage_id_generator: Arc<StorageIdGenerator>,
    options: DataBaseOptions,
}

impl MergeManager {
    pub fn new(
        instance_id: String,
        database_dir: &Path,
        storage_id_generator: Arc<StorageIdGenerator>,
        options: DataBaseOptions,
    ) -> MergeManager {
        MergeManager {
            instance_id,
            database_dir: database_dir.to_path_buf(),
            merge_lock: Mutex::new(()),
            storage_id_generator,
            options,
        }
    }

    pub fn merge(&self, database: &Database, keydir: &RwLock<KeyDir>) -> BitcaskResult<()> {
        let lock_ret = self.merge_lock.try_lock();

        if lock_ret.is_none() {
            return Err(BitcaskError::MergeInProgress());
        }

        let (kd, known_max_file_id) = self.flush_writing_file(database, keydir)?;

        debug!(target: "Bitcask", "start merging. instanceId: {}, knownMaxFileId {}", self.instance_id, known_max_file_id);

        let merge_dir_path = create_merge_file_dir(database.get_database_dir())?;
        let (file_ids, merged_key_dir) =
            self.write_merged_files(database, &merge_dir_path, &kd, known_max_file_id)?;

        {
            // stop read/write
            let kd = keydir.write();
            database.flush_writing_file()?;
            self.commit_merge(&file_ids, known_max_file_id)
                .and_then(|file_ids| database.reload_data_files(file_ids))
                .map_err(|e| {
                    database.mark_db_error(e.to_string());
                    error!(target: "Bitcask", "database commit merge failed with error: {}", &e);
                    e
                })?;

            for (k, v) in merged_key_dir.into_iter() {
                kd.checked_put(k, v)
            }
        }

        info!(target: "Bitcask", "purge files with id smaller than: {}", known_max_file_id);

        purge_outdated_data_files(&database.database_dir, known_max_file_id)?;
        let delete_ret = fs::delete_dir(&merge_dir_path);
        if delete_ret.is_err() {
            warn!(target: "Bitcask", "delete merge directory failed. {}", delete_ret.unwrap_err());
        }

        debug!(target: "Bitcask", "merge success. instanceId: {}, knownMaxFileId {}", self.instance_id, known_max_file_id);

        Ok(())
    }

    pub fn recover_merge(&self) -> BitcaskResult<()> {
        debug!(target: "Bitcask", "start recover merge");
        let recover_ret = self.do_recover_merge();
        if let Err(err) = recover_ret {
            let merge_dir = merge_file_dir(&self.database_dir);
            warn!(
                "recover merge under path: {} failed with error: \"{}\"",
                merge_dir.display(),
                err
            );
            match err {
                BitcaskError::InvalidMergeDataFile(_, _) => {
                    // clear Merge directory when recover merge failed
                    fs::delete_dir(&merge_file_dir(&self.database_dir))?;
                }
                _ => return Err(err),
            }
        }
        Ok(())
    }

    fn do_recover_merge(&self) -> BitcaskResult<()> {
        let merge_file_dir = merge_file_dir(&self.database_dir);

        if !merge_file_dir.exists() {
            return Ok(());
        }

        let mut merge_data_file_ids =
            fs::get_storage_ids_in_dir(&merge_file_dir, FileType::DataFile);
        if merge_data_file_ids.is_empty() {
            return Ok(());
        }

        merge_data_file_ids.sort();
        let merge_meta = read_merge_meta(&merge_file_dir)?;
        if *merge_data_file_ids.first().unwrap() <= merge_meta.known_max_file_id {
            return Err(BitcaskError::InvalidMergeDataFile(
                merge_meta.known_max_file_id,
                *merge_data_file_ids.first().unwrap(),
            ));
        }

        self.storage_id_generator
            .update_id(*merge_data_file_ids.last().unwrap());

        self.shift_data_files(merge_meta.known_max_file_id)?;

        commit_merge_files(&self.database_dir, &merge_data_file_ids)?;

        purge_outdated_data_files(&self.database_dir, merge_meta.known_max_file_id)?;

        let delete_ret = fs::delete_dir(&merge_file_dir);
        if delete_ret.is_err() {
            warn!(target: "Database", "delete merge directory failed. {}", delete_ret.unwrap_err());
        }
        Ok(())
    }

    fn flush_writing_file(
        &self,
        database: &Database,
        keydir: &RwLock<KeyDir>,
    ) -> BitcaskResult<(KeyDir, StorageId)> {
        // stop writing and switch the writing file to stable files
        let _kd = keydir.write();
        database.flush_writing_file()?;
        let known_max_file_id = database.get_max_storage_id();
        Ok((_kd.clone(), known_max_file_id))
    }

    fn write_merged_files(
        &self,
        database: &Database,
        merge_file_dir: &Path,
        key_dir_to_write: &KeyDir,
        known_max_file_id: StorageId,
    ) -> BitcaskResult<(Vec<StorageId>, KeyDir)> {
        write_merge_meta(merge_file_dir, MergeMeta { known_max_file_id })?;

        let merged_key_dir = KeyDir::new_empty_key_dir();
        let merge_db = Database::open(
            merge_file_dir,
            self.storage_id_generator.clone(),
            self.options,
        )?;

        let mut write_key_count = 0;
        for r in key_dir_to_write.iter() {
            let k = r.key();
            let v = database.read_value(r.value())?;
            if !is_tombstone(&v.value) {
                let pos = merge_db.write(k, TimedValue::has_time_value(v.value, v.timestamp))?;
                merged_key_dir.checked_put(k.clone(), pos);
                debug!(target: "Bitcask", "put data to merged file success. key: {:?}, file_id: {}, row_offset: {}, row_size: {}, timestamp: {}", 
                    k, pos.storage_id, pos.row_offset, pos.row_size, v.timestamp);
                write_key_count += 1;
            }
        }

        merge_db.flush_writing_file()?;
        let file_ids = merge_db.get_storage_ids();
        info!(target: "Bitcask", "{} keys in database merged to files with ids: {:?}", write_key_count, &file_ids.stable_storage_ids);
        // we do not write anything in writing file
        // so we can only use stable files
        Ok((file_ids.stable_storage_ids, merged_key_dir))
    }

    fn commit_merge(
        &self,
        merged_file_ids: &Vec<StorageId>,
        known_max_file_id: StorageId,
    ) -> BitcaskResult<Vec<StorageId>> {
        let mut data_file_ids = self.shift_data_files(known_max_file_id)?;

        commit_merge_files(&self.database_dir, merged_file_ids)?;

        data_file_ids.extend(merged_file_ids.iter());

        Ok(data_file_ids)
    }

    fn shift_data_files(&self, known_max_file_id: StorageId) -> BitcaskResult<Vec<StorageId>> {
        let mut data_file_ids = fs::get_storage_ids_in_dir(&self.database_dir, FileType::DataFile)
            .into_iter()
            .filter(|id| *id >= known_max_file_id)
            .collect::<Vec<StorageId>>();
        // must change name in descending order to keep data file's order even when any change name operation failed
        data_file_ids.sort_by(|a, b| b.cmp(a));

        // rename files which file id >= knwon_max_file_id to files which file id greater than all merged files
        // because values in these files is written after merged files
        let mut new_file_ids = vec![];
        for from_id in data_file_ids {
            let new_file_id = &self.storage_id_generator.generate_next_id();
            fs::change_file_id(
                &self.database_dir,
                FileType::DataFile,
                from_id,
                *new_file_id,
            )?;
            new_file_ids.push(*new_file_id);
        }
        Ok(new_file_ids)
    }
}

fn merge_file_dir(base_dir: &Path) -> PathBuf {
    base_dir.join(MERGE_FILES_DIRECTORY)
}

fn create_merge_file_dir(base_dir: &Path) -> BitcaskResult<PathBuf> {
    let merge_dir_path = merge_file_dir(base_dir);

    fs::create_dir(&merge_dir_path)?;

    let mut merge_dir_empty = true;
    let paths = std::fs::read_dir(merge_dir_path.clone())?;
    for path in paths {
        let file_path = path?;
        if file_path.path().is_dir() {
            continue;
        }
        if FileType::MergeMeta.check_file_belongs_to_type(&file_path.path()) {
            continue;
        }
        warn!(
            target: DEFAULT_LOG_TARGET,
            "Merge file directory:{} is not empty, it at least has file: {}",
            merge_dir_path.display().to_string(),
            file_path.path().display()
        );

        merge_dir_empty = false;
        break;
    }
    if !merge_dir_empty {
        let delete_ret = fs::delete_dir(&merge_dir_path).and_then(|_| {
            std::fs::create_dir(merge_dir_path.clone())?;
            Ok(())
        });
        if delete_ret.is_err() {
            warn!(
                target: DEFAULT_LOG_TARGET,
                "delete merge directory failed. {}",
                delete_ret.unwrap_err()
            );
            return Err(BitcaskError::MergeFileDirectoryNotEmpty(
                merge_dir_path.display().to_string(),
            ));
        }
    }

    Ok(merge_dir_path)
}

fn commit_merge_files(base_dir: &Path, file_ids: &Vec<StorageId>) -> BitcaskResult<()> {
    let merge_dir_path = merge_file_dir(base_dir);
    for file_id in file_ids {
        fs::move_file(
            FileType::DataFile,
            Some(*file_id),
            &merge_dir_path,
            base_dir,
        )?;
        fs::move_file(
            FileType::HintFile,
            Some(*file_id),
            &merge_dir_path,
            base_dir,
        )?;
    }
    Ok(())
}

fn purge_outdated_data_files(base_dir: &Path, max_file_id: StorageId) -> BitcaskResult<()> {
    fs::get_storage_ids_in_dir(base_dir, FileType::DataFile)
        .iter()
        .filter(|id| **id < max_file_id)
        .for_each(|id| {
            fs::delete_file(base_dir, FileType::DataFile, Some(*id)).unwrap_or_default();
            fs::delete_file(base_dir, FileType::HintFile, Some(*id)).unwrap_or_default();
        });
    Ok(())
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
struct MergeMeta {
    pub known_max_file_id: StorageId,
}

fn read_merge_meta(merge_file_dir: &Path) -> BitcaskResult<MergeMeta> {
    let mut merge_meta_file = fs::open_file(merge_file_dir, FileType::MergeMeta, None)?;
    let mut buf = vec![0; 4];
    merge_meta_file.file.read_exact(&mut buf)?;
    let mut bs = Bytes::from(buf);
    let known_max_file_id = bs.get_u32();
    Ok(MergeMeta { known_max_file_id })
}

fn write_merge_meta(merge_file_dir: &Path, merge_meta: MergeMeta) -> BitcaskResult<()> {
    let mut merge_meta_file = fs::create_file(merge_file_dir, FileType::MergeMeta, None)?;
    merge_meta_file.write_all(&merge_meta.known_max_file_id.to_be_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::vec;

    use crate::{
        database::database_tests_utils::{
            assert_database_rows, assert_rows_value, write_kvs_to_db, TestingRow, DEFAULT_OPTIONS,
        },
        fs::FileType,
    };

    use super::*;
    use bitcask_tests::common::{get_temporary_directory_path, TestingKV};
    use test_log::test;

    const INSTANCE_ID: String = String::new();

    #[test]
    fn test_create_merge_file_dir() {
        let dir = get_temporary_directory_path();
        let merge_file_path = create_merge_file_dir(&dir).unwrap();
        fs::create_file(&merge_file_path, FileType::DataFile, Some(0)).unwrap();

        create_merge_file_dir(&dir).unwrap();

        let paths = std::fs::read_dir(merge_file_path).unwrap();
        assert!(!paths.into_iter().any(|p| {
            let file_path = p.unwrap();
            if file_path.path().is_dir() {
                return false;
            }
            if FileType::MergeMeta.check_file_belongs_to_type(&file_path.path()) {
                return false;
            }
            true
        }));
    }

    #[test]
    fn test_commit_merge_files() {
        let dir_path = get_temporary_directory_path();

        let merge_file_path = create_merge_file_dir(&dir_path).unwrap();
        fs::create_file(&merge_file_path, FileType::DataFile, Some(0)).unwrap();
        fs::create_file(&merge_file_path, FileType::DataFile, Some(1)).unwrap();
        fs::create_file(&merge_file_path, FileType::DataFile, Some(2)).unwrap();

        assert_eq!(
            vec![0, 1, 2,],
            fs::get_storage_ids_in_dir(&merge_file_path, FileType::DataFile)
        );
        assert!(fs::get_storage_ids_in_dir(&dir_path, FileType::DataFile).is_empty());

        commit_merge_files(&dir_path, &vec![0, 1, 2]).unwrap();

        assert!(fs::is_empty_dir(&merge_file_path).unwrap());

        assert_eq!(
            vec![0, 1, 2,],
            fs::get_storage_ids_in_dir(&dir_path, FileType::DataFile)
        );
    }

    #[test]
    fn test_read_write_merge_meta() {
        let dir_path = get_temporary_directory_path();
        let merge_file_path = create_merge_file_dir(&dir_path).unwrap();
        let expect_meta = MergeMeta {
            known_max_file_id: 10101,
        };
        write_merge_meta(&merge_file_path, expect_meta).unwrap();
        let actual_meta = read_merge_meta(&merge_file_path).unwrap();
        assert_eq!(expect_meta, actual_meta);
    }

    #[test]
    fn test_recover_merge_with_only_merge_meta() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let storage_id_generator = Arc::new(StorageIdGenerator::new());
        {
            let db = Database::open(&dir, storage_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }
        let merge_file_dir = create_merge_file_dir(&dir).unwrap();
        let merge_meta = MergeMeta {
            known_max_file_id: 101,
        };
        write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        let merge_manager = MergeManager::new(
            INSTANCE_ID,
            &dir,
            storage_id_generator.clone(),
            DEFAULT_OPTIONS,
        );
        merge_manager.recover_merge().unwrap();
        let db = Database::open(&dir, storage_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        assert_eq!(1, storage_id_generator.get_id());
        assert_eq!(0, db.get_storage_ids().stable_storage_ids.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_recover_merge_with_invalid_merge_meta() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let storage_id_generator = Arc::new(StorageIdGenerator::new());
        {
            let db = Database::open(&dir, storage_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }
        let merge_file_dir = create_merge_file_dir(&dir).unwrap();
        {
            // write something to data file in merge dir
            let db = Database::open(
                &merge_file_dir,
                storage_id_generator.clone(),
                DEFAULT_OPTIONS,
            )
            .unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            write_kvs_to_db(&db, kvs);
        }

        let merge_meta = MergeMeta {
            known_max_file_id: storage_id_generator.generate_next_id(),
        };
        write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        let merge_manager = MergeManager::new(
            INSTANCE_ID,
            &dir,
            storage_id_generator.clone(),
            DEFAULT_OPTIONS,
        );
        merge_manager.recover_merge().unwrap();
        let db = Database::open(&dir, storage_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        assert_eq!(3, storage_id_generator.get_id());
        assert_eq!(0, db.get_storage_ids().stable_storage_ids.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
        assert!(!merge_file_dir.exists());
    }

    #[test]
    fn test_recover_merge() {
        let dir = get_temporary_directory_path();
        let storage_id_generator = Arc::new(StorageIdGenerator::new());
        {
            let db = Database::open(&dir, storage_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            write_kvs_to_db(&db, kvs);
        }
        let merge_meta = MergeMeta {
            known_max_file_id: storage_id_generator.generate_next_id(),
        };
        let merge_file_dir = create_merge_file_dir(&dir).unwrap();
        write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        let merge_manager = MergeManager::new(
            INSTANCE_ID,
            &dir,
            storage_id_generator.clone(),
            DEFAULT_OPTIONS,
        );
        merge_manager.recover_merge().unwrap();
        let mut rows: Vec<TestingRow> = vec![];
        {
            // write something to data file in merge dir
            let db = Database::open(
                &merge_file_dir,
                storage_id_generator.clone(),
                DEFAULT_OPTIONS,
            )
            .unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value3"),
                TestingKV::new("k2", "value4"),
                TestingKV::new("k3", "value5"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }

        let merge_manager = MergeManager::new(
            INSTANCE_ID,
            &dir,
            storage_id_generator.clone(),
            DEFAULT_OPTIONS,
        );
        merge_manager.recover_merge().unwrap();
        let db = Database::open(&dir, storage_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        assert_eq!(3, storage_id_generator.get_id());
        assert_eq!(0, db.get_storage_ids().stable_storage_ids.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
        assert!(!merge_file_dir.exists());
    }

    #[test]
    fn test_recover_merge_failed_with_unexpeded_error() {
        let dir = get_temporary_directory_path();
        let storage_id_generator = Arc::new(StorageIdGenerator::new());
        let mut rows: Vec<TestingRow> = vec![];
        {
            let db = Database::open(&dir, storage_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }
        let merge_meta = MergeMeta {
            known_max_file_id: storage_id_generator.generate_next_id(),
        };
        let merge_file_dir = create_merge_file_dir(&dir).unwrap();
        write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        {
            // write something to data file in merge dir
            let db = Database::open(
                &merge_file_dir,
                storage_id_generator.clone(),
                DEFAULT_OPTIONS,
            )
            .unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value3"),
                TestingKV::new("k2", "value4"),
            ];
            write_kvs_to_db(&db, kvs);
        }

        // change one data file under merge directory to readonly
        // so this file cannot recover and move to base directory
        let meta = std::fs::metadata(&merge_file_dir).unwrap();
        let mut perms = meta.permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&merge_file_dir, perms).unwrap();

        let merge_manager =
            MergeManager::new(INSTANCE_ID, &dir, storage_id_generator, DEFAULT_OPTIONS);
        let ret = merge_manager.recover_merge();
        assert!(ret.is_err());
    }

    #[test]
    fn test_load_merged_files() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let storage_id_generator = Arc::new(StorageIdGenerator::new());
        let old_db = Database::open(&dir, storage_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        let kvs = vec![
            TestingKV::new("k1", "value1"),
            TestingKV::new("k2", "value2"),
        ];
        rows.append(&mut write_kvs_to_db(&old_db, kvs));
        {
            let merge_path = create_merge_file_dir(&dir).unwrap();
            let db =
                Database::open(&merge_path, storage_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k3", "hello world"),
                TestingKV::new("k1", "value4"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
            db.flush_writing_file().unwrap();
            old_db.flush_writing_file().unwrap();
            let merge_manager = MergeManager::new(
                INSTANCE_ID,
                &dir,
                storage_id_generator.clone(),
                DEFAULT_OPTIONS,
            );

            let files = merge_manager
                .commit_merge(
                    &db.get_storage_ids().stable_storage_ids,
                    old_db.get_max_storage_id(),
                )
                .unwrap();

            old_db.reload_data_files(files).unwrap();
        }

        assert_eq!(5, storage_id_generator.get_id());
        assert_eq!(1, old_db.get_storage_ids().stable_storage_ids.len());
    }
}
