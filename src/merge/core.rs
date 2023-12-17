use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use bytes::Bytes;

use log::{debug, error, info, warn};
use parking_lot::{Mutex, RwLock};

use common::{
    formatter::{
        get_formatter_from_file, initialize_new_file, BitcaskFormatter, Formatter, MergeMeta,
    },
    fs::{self, FileType},
    options::BitcaskOptions,
    storage_id::{StorageId, StorageIdGenerator},
};
use database::{Database, TimedValue};

use crate::{
    error::{BitcaskError, BitcaskResult},
    keydir::KeyDir,
};

const MERGE_FILES_DIRECTORY: &str = "Merge";
const DEFAULT_LOG_TARGET: &str = "DatabaseMerge";

#[derive(Debug)]
pub struct MergeManagerTelemetry {
    pub is_merging: bool,
}

pub struct MergeManager {
    instance_id: String,
    database_dir: PathBuf,
    merge_lock: Mutex<()>,
    storage_id_generator: Arc<StorageIdGenerator>,
    options: BitcaskOptions,
}

impl MergeManager {
    pub fn new(
        instance_id: String,
        database_dir: &Path,
        storage_id_generator: Arc<StorageIdGenerator>,
        options: BitcaskOptions,
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

        let start = Instant::now();
        let (kd, known_max_storage_id) = self.flush_writing_file(database, keydir)?;

        debug!(target: "Bitcask", "start merging. instanceId: {}, knownMaxFileId {}", self.instance_id, known_max_storage_id);

        let merge_dir_path = create_merge_file_dir(database.get_database_dir())?;
        let (storage_ids, merged_key_dir) =
            self.write_merged_files(database, &merge_dir_path, &kd, known_max_storage_id)?;

        {
            // stop read/write
            let kd = keydir.write();
            database.flush_writing_file()?;
            self.commit_merge(&storage_ids, known_max_storage_id)
                .and_then(|storage_ids| {
                    database
                        .reload_data_files(storage_ids)
                        .map_err(BitcaskError::DatabaseError)
                })
                .map_err(|e| {
                    database.mark_db_error(e.to_string());
                    error!(target: "Bitcask", "database commit merge failed with error: {}", &e);
                    e
                })?;

            for (k, v) in merged_key_dir.into_iter() {
                kd.checked_put(k, v)
            }
        }

        info!(target: "Bitcask", "purge files with id smaller than: {}", known_max_storage_id);

        purge_outdated_data_files(&database.database_dir, known_max_storage_id)?;
        let delete_ret = fs::delete_dir(&merge_dir_path);
        if delete_ret.is_err() {
            warn!(target: "Bitcask", "delete merge directory failed. {}", delete_ret.unwrap_err());
        }

        info!(target: "Bitcask", "merge success. instanceId: {}, knownMaxFileId {}, cost: {} millis",
          self.instance_id, known_max_storage_id, start.elapsed().as_millis());

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

    pub fn get_telemetry_data(&self) -> MergeManagerTelemetry {
        MergeManagerTelemetry {
            is_merging: self.merge_lock.is_locked(),
        }
    }

    fn do_recover_merge(&self) -> BitcaskResult<()> {
        let merge_file_dir = merge_file_dir(&self.database_dir);

        if !merge_file_dir.exists() {
            return Ok(());
        }

        let mut merge_data_storage_ids =
            fs::get_storage_ids_in_dir(&merge_file_dir, FileType::DataFile);
        if merge_data_storage_ids.is_empty() {
            return Ok(());
        }

        merge_data_storage_ids.sort();
        let merge_meta = read_merge_meta(&merge_file_dir)?;
        if *merge_data_storage_ids.first().unwrap() <= merge_meta.known_max_storage_id {
            return Err(BitcaskError::InvalidMergeDataFile(
                merge_meta.known_max_storage_id,
                *merge_data_storage_ids.first().unwrap(),
            ));
        }

        self.storage_id_generator
            .update_id(*merge_data_storage_ids.last().unwrap());

        self.shift_data_files(merge_meta.known_max_storage_id)?;

        commit_merge_files(&self.database_dir, &merge_data_storage_ids)?;

        purge_outdated_data_files(&self.database_dir, merge_meta.known_max_storage_id)?;

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
        let known_max_storage_id = database.get_max_storage_id();
        Ok((_kd.clone(), known_max_storage_id))
    }

    fn write_merged_files(
        &self,
        database: &Database,
        merge_file_dir: &Path,
        key_dir_to_write: &KeyDir,
        known_max_storage_id: StorageId,
    ) -> BitcaskResult<(Vec<StorageId>, KeyDir)> {
        write_merge_meta(
            merge_file_dir,
            MergeMeta {
                known_max_storage_id,
            },
        )?;

        let merged_key_dir = KeyDir::new_empty_key_dir();
        let merge_db = Database::open(
            merge_file_dir,
            self.storage_id_generator.clone(),
            self.options.clone(),
        )?;

        let mut write_key_count = 0;
        for r in key_dir_to_write.iter() {
            let k = r.key();
            if let Some(v) = database.read_value(r.value())? {
                let pos =
                    merge_db.write(k, TimedValue::expirable_value(v.value, v.expire_timestamp))?;
                merged_key_dir.checked_put(k.clone(), pos);
                debug!(target: "Bitcask", "put data to merged file success. key: {:?}, storage_id: {}, row_offset: {}, expire_timestamp: {}", 
                k, pos.storage_id, pos.row_offset, v.expire_timestamp);
                write_key_count += 1;
            }
        }

        merge_db.flush_writing_file()?;
        let storage_ids = merge_db.get_storage_ids();
        info!(target: "Bitcask", "{} keys in database merged to files with ids: {:?}", write_key_count, &storage_ids.stable_storage_ids);
        // we do not write anything in writing file
        // so we can only use stable files
        Ok((storage_ids.stable_storage_ids, merged_key_dir))
    }

    fn commit_merge(
        &self,
        merged_storage_ids: &Vec<StorageId>,
        known_max_storage_id: StorageId,
    ) -> BitcaskResult<Vec<StorageId>> {
        let mut data_storage_ids = self.shift_data_files(known_max_storage_id)?;

        commit_merge_files(&self.database_dir, merged_storage_ids)?;

        data_storage_ids.extend(merged_storage_ids.iter());

        Ok(data_storage_ids)
    }

    fn shift_data_files(&self, known_max_storage_id: StorageId) -> BitcaskResult<Vec<StorageId>> {
        let mut data_storage_ids =
            fs::get_storage_ids_in_dir(&self.database_dir, FileType::DataFile)
                .into_iter()
                .filter(|id| *id >= known_max_storage_id)
                .collect::<Vec<StorageId>>();
        // must change name in descending order to keep data file's order even when any change name operation failed
        data_storage_ids.sort_by(|a, b| b.cmp(a));

        // rename files which file id >= knwon_max_storage_id to files which file id greater than all merged files
        // because values in these files is written after merged files
        let mut new_storage_ids = vec![];
        for from_id in data_storage_ids {
            let new_storage_id = &self.storage_id_generator.generate_next_id();
            fs::change_storage_id(
                &self.database_dir,
                FileType::DataFile,
                from_id,
                *new_storage_id,
            )?;
            new_storage_ids.push(*new_storage_id);
        }
        Ok(new_storage_ids)
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

fn commit_merge_files(base_dir: &Path, storage_ids: &Vec<StorageId>) -> BitcaskResult<()> {
    let merge_dir_path = merge_file_dir(base_dir);
    for storage_id in storage_ids {
        fs::move_file(
            FileType::DataFile,
            Some(*storage_id),
            &merge_dir_path,
            base_dir,
        )?;
        fs::move_file(
            FileType::HintFile,
            Some(*storage_id),
            &merge_dir_path,
            base_dir,
        )?;
    }
    Ok(())
}

fn purge_outdated_data_files(base_dir: &Path, max_storage_id: StorageId) -> BitcaskResult<()> {
    fs::get_storage_ids_in_dir(base_dir, FileType::DataFile)
        .iter()
        .filter(|id| **id < max_storage_id)
        .for_each(|id| {
            fs::delete_file(base_dir, FileType::DataFile, Some(*id)).unwrap_or_default();
            fs::delete_file(base_dir, FileType::HintFile, Some(*id)).unwrap_or_default();
        });
    Ok(())
}

fn read_merge_meta(merge_file_dir: &Path) -> BitcaskResult<MergeMeta> {
    let mut merge_meta_file = fs::open_file(merge_file_dir, FileType::MergeMeta, None)?;
    let formatter = get_formatter_from_file(&mut merge_meta_file.file).map_err(|e| {
        BitcaskError::MergeMetaFileCorrupted(e, merge_meta_file.path.display().to_string())
    })?;

    let mut buf = vec![0; formatter.merge_meta_size()];
    merge_meta_file.file.read_exact(&mut buf)?;
    let bs = Bytes::from(buf);
    Ok(formatter.decode_merge_meta(bs))
}

fn write_merge_meta(merge_file_dir: &Path, merge_meta: MergeMeta) -> BitcaskResult<()> {
    let mut merge_meta_file = fs::create_file(merge_file_dir, FileType::MergeMeta, None)?;
    let formater = BitcaskFormatter::default();
    initialize_new_file(&mut merge_meta_file, formater.version())?;
    merge_meta_file.write_all(&formater.encode_merge_meta(&merge_meta))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{time::Duration, vec};

    use common::{
        formatter::{initialize_new_file, BitcaskFormatter},
        fs::FileType,
    };
    use database::RowLocation;

    use super::*;
    use test_log::test;
    use utilities::common::{get_temporary_directory_path, TestingKV};

    #[derive(Debug)]
    pub struct TestingRow {
        pub kv: TestingKV,
        pub pos: RowLocation,
    }

    impl TestingRow {
        pub fn new(kv: TestingKV, pos: RowLocation) -> Self {
            TestingRow { kv, pos }
        }
    }

    fn get_options() -> BitcaskOptions {
        BitcaskOptions::default()
            .sync_interval(Duration::from_secs(60))
            .init_hint_file_capacity(1024)
            .max_data_file_size(1024)
            .init_data_file_capacity(100)
    }

    pub fn assert_row_value(db: &Database, expect: &TestingRow) {
        let actual = db.read_value(&expect.pos).unwrap();
        assert_eq!(*expect.kv.value(), *actual.unwrap().value);
    }

    pub fn assert_database_rows(db: &Database, expect_rows: &Vec<TestingRow>) {
        let mut i = 0;
        for actual_row in db.iter().unwrap().map(|r| r.unwrap()) {
            let expect_row = expect_rows.get(i).unwrap();
            assert_eq!(expect_row.kv.key(), actual_row.key);
            assert_eq!(expect_row.kv.value(), actual_row.value.value);
            assert_eq!(expect_row.pos, actual_row.row_location);
            i += 1;
        }
        assert_eq!(expect_rows.len(), i);
    }

    pub fn assert_rows_value(db: &Database, expect: &Vec<TestingRow>) {
        for row in expect {
            assert_row_value(db, row);
        }
    }

    pub fn write_kvs_to_db(db: &Database, kvs: Vec<TestingKV>) -> Vec<TestingRow> {
        kvs.into_iter()
            .map(|kv| {
                let pos = db
                    .write(&kv.key(), TimedValue::immortal_value(kv.value()))
                    .unwrap();
                TestingRow::new(
                    kv,
                    RowLocation {
                        storage_id: pos.storage_id,
                        row_offset: pos.row_offset,
                    },
                )
            })
            .collect::<Vec<TestingRow>>()
    }

    const INSTANCE_ID: String = String::new();

    #[test]
    fn test_create_merge_file_dir() {
        let dir = get_temporary_directory_path();
        let merge_file_path = create_merge_file_dir(&dir).unwrap();
        let mut file = fs::create_file(&merge_file_path, FileType::DataFile, Some(0)).unwrap();
        initialize_new_file(&mut file, BitcaskFormatter::default().version()).unwrap();

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
        initialize_new_file(
            &mut fs::create_file(&merge_file_path, FileType::DataFile, Some(0)).unwrap(),
            BitcaskFormatter::default().version(),
        )
        .unwrap();
        initialize_new_file(
            &mut fs::create_file(&merge_file_path, FileType::DataFile, Some(1)).unwrap(),
            BitcaskFormatter::default().version(),
        )
        .unwrap();
        initialize_new_file(
            &mut fs::create_file(&merge_file_path, FileType::DataFile, Some(2)).unwrap(),
            BitcaskFormatter::default().version(),
        )
        .unwrap();

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
            known_max_storage_id: 10101,
        };
        write_merge_meta(&merge_file_path, expect_meta).unwrap();
        let actual_meta = read_merge_meta(&merge_file_path).unwrap();
        assert_eq!(expect_meta, actual_meta);
    }

    #[test]
    fn test_recover_merge_with_only_merge_meta() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let storage_id_generator = Arc::new(StorageIdGenerator::default());
        {
            let db = Database::open(&dir, storage_id_generator.clone(), get_options()).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }
        let merge_file_dir = create_merge_file_dir(&dir).unwrap();
        let merge_meta = MergeMeta {
            known_max_storage_id: 101,
        };
        write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        let merge_manager = MergeManager::new(
            INSTANCE_ID,
            &dir,
            storage_id_generator.clone(),
            get_options(),
        );
        merge_manager.recover_merge().unwrap();
        let db = Database::open(&dir, storage_id_generator.clone(), get_options()).unwrap();
        assert_eq!(1, storage_id_generator.get_id());
        assert_eq!(0, db.get_storage_ids().stable_storage_ids.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_recover_merge_with_invalid_merge_meta() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let storage_id_generator = Arc::new(StorageIdGenerator::default());
        {
            let db = Database::open(&dir, storage_id_generator.clone(), get_options()).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }
        let merge_file_dir = create_merge_file_dir(&dir).unwrap();
        {
            // write something to data file in merge dir
            let db = Database::open(&merge_file_dir, storage_id_generator.clone(), get_options())
                .unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            write_kvs_to_db(&db, kvs);
        }

        let merge_meta = MergeMeta {
            known_max_storage_id: storage_id_generator.generate_next_id(),
        };
        write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        let merge_manager = MergeManager::new(
            INSTANCE_ID,
            &dir,
            storage_id_generator.clone(),
            get_options(),
        );
        merge_manager.recover_merge().unwrap();
        let db = Database::open(&dir, storage_id_generator.clone(), get_options()).unwrap();
        assert_eq!(3, storage_id_generator.get_id());
        assert_eq!(0, db.get_storage_ids().stable_storage_ids.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
        assert!(!merge_file_dir.exists());
    }

    #[test]
    fn test_recover_merge() {
        let dir = get_temporary_directory_path();
        let storage_id_generator = Arc::new(StorageIdGenerator::default());
        {
            let db = Database::open(&dir, storage_id_generator.clone(), get_options()).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            write_kvs_to_db(&db, kvs);
        }
        let merge_meta = MergeMeta {
            known_max_storage_id: storage_id_generator.generate_next_id(),
        };
        let merge_file_dir = create_merge_file_dir(&dir).unwrap();
        write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        let merge_manager = MergeManager::new(
            INSTANCE_ID,
            &dir,
            storage_id_generator.clone(),
            get_options(),
        );
        merge_manager.recover_merge().unwrap();
        let mut rows: Vec<TestingRow> = vec![];
        {
            // write something to data file in merge dir
            let db = Database::open(&merge_file_dir, storage_id_generator.clone(), get_options())
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
            get_options(),
        );
        merge_manager.recover_merge().unwrap();
        let db = Database::open(&dir, storage_id_generator.clone(), get_options()).unwrap();
        assert_eq!(3, storage_id_generator.get_id());
        assert_eq!(0, db.get_storage_ids().stable_storage_ids.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
        assert!(!merge_file_dir.exists());
    }

    #[test]
    fn test_recover_merge_failed_with_unexpeded_error() {
        let dir = get_temporary_directory_path();
        let storage_id_generator = Arc::new(StorageIdGenerator::default());
        let mut rows: Vec<TestingRow> = vec![];
        {
            let db = Database::open(&dir, storage_id_generator.clone(), get_options()).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }
        let merge_meta = MergeMeta {
            known_max_storage_id: storage_id_generator.generate_next_id(),
        };
        let merge_file_dir = create_merge_file_dir(&dir).unwrap();
        write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        {
            // write something to data file in merge dir
            let db = Database::open(&merge_file_dir, storage_id_generator.clone(), get_options())
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
            MergeManager::new(INSTANCE_ID, &dir, storage_id_generator, get_options());
        let ret = merge_manager.recover_merge();
        assert!(ret.is_err());
    }

    #[test]
    fn test_load_merged_files() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let storage_id_generator = Arc::new(StorageIdGenerator::default());
        let old_db = Database::open(&dir, storage_id_generator.clone(), get_options()).unwrap();
        let kvs = vec![
            TestingKV::new("k1", "value1"),
            TestingKV::new("k2", "value2"),
        ];
        rows.append(&mut write_kvs_to_db(&old_db, kvs));
        {
            let merge_path = create_merge_file_dir(&dir).unwrap();
            let db =
                Database::open(&merge_path, storage_id_generator.clone(), get_options()).unwrap();
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
                get_options(),
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
