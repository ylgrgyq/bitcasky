use std::{
    cell::Cell,
    mem,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use dashmap::{mapref::one::RefMut, DashMap};
use parking_lot::{Mutex, MutexGuard};

use crate::{
    database::hint::{self, HintWriter},
    error::{BitcaskError, BitcaskResult},
    formatter::RowToWrite,
    fs::{self as SelfFs, FileType},
    storage_id::{StorageId, StorageIdGenerator},
    utils,
};
use log::{debug, error, info, trace};

use super::{
    common::{RecoveredRow, TimedValue, Value},
    data_storage::{
        DataStorage, DataStorageOptions, DataStorageReader, DataStorageWriter, StorageIter,
    },
    DataStorageError,
};
use super::{
    common::{RowLocation, RowToRead},
    hint::HintFile,
};
/**
 * Statistics of a Database.
 * Some of the metrics may not accurate due to concurrent access.
 */
pub struct DatabaseStats {
    /**
     * Number of data files in Database
     */
    pub number_of_data_files: usize,
    /**
     * Data size in bytes of this Database
     */
    pub total_data_size_in_bytes: u64,
    /**
     * Number of hint files waiting to write
     */
    pub number_of_pending_hint_files: usize,
}

#[derive(Debug)]
pub struct StorageIds {
    pub stable_storage_ids: Vec<StorageId>,
    pub writing_storage_id: StorageId,
}

#[derive(Debug, Clone, Copy)]
pub struct DataBaseOptions {
    pub storage_options: DataStorageOptions,
    /// How frequent can we flush data
    pub sync_interval_sec: u64,
}

#[derive(Debug)]
pub struct Database {
    pub database_dir: PathBuf,
    storage_id_generator: Arc<StorageIdGenerator>,
    writing_storage: Arc<Mutex<DataStorage>>,
    stable_storages: DashMap<StorageId, Mutex<DataStorage>>,
    options: DataBaseOptions,
    hint_file_writer: HintWriter,
    /// Process that periodically flushes writing storage
    sync_worker: Option<JoinHandle<()>>,
    is_error: Mutex<Option<String>>,
}

impl Database {
    pub fn open(
        directory: &Path,
        storage_id_generator: Arc<StorageIdGenerator>,
        options: DataBaseOptions,
    ) -> BitcaskResult<Database> {
        let database_dir: PathBuf = directory.into();

        debug!(target: "Database", "opening database at directory {:?}", directory);

        hint::clear_temp_hint_file_directory(&database_dir);

        let data_storage_ids = SelfFs::get_storage_ids_in_dir(&database_dir, FileType::DataFile);
        if let Some(id) = data_storage_ids.iter().max() {
            storage_id_generator.update_id(*id);
        }

        let hint_file_writer = HintWriter::start(&database_dir, options.storage_options);

        let (writing_storage, storages) = prepare_load_storages(
            &database_dir,
            &data_storage_ids,
            &storage_id_generator,
            options.storage_options,
        )?;

        let stable_storages = storages.into_iter().fold(DashMap::new(), |m, s| {
            m.insert(s.storage_id(), Mutex::new(s));
            m
        });

        let writing_storage = Arc::new(Mutex::new(writing_storage));
        let mut db = Database {
            writing_storage,
            storage_id_generator,
            database_dir,
            stable_storages,
            options,
            hint_file_writer,
            sync_worker: None,
            is_error: Mutex::new(None),
        };

        if options.sync_interval_sec > 0 {
            db.sync_worker = Some(db.start_sync_worker(db.writing_storage.clone()));
        }

        info!(target: "Database", "database opened at directory: {:?}, with {} data files", directory, data_storage_ids.len());
        Ok(db)
    }

    pub fn get_database_dir(&self) -> &Path {
        &self.database_dir
    }

    pub fn get_max_storage_id(&self) -> StorageId {
        let writing_file_ref = self.writing_storage.lock();
        writing_file_ref.storage_id()
    }

    pub fn write<V: Deref<Target = [u8]>>(
        &self,
        key: &Vec<u8>,
        value: TimedValue<V>,
    ) -> BitcaskResult<RowLocation> {
        let row: RowToWrite<'_, TimedValue<V>> = RowToWrite::new(key, value);
        let mut writing_file_ref = self.writing_storage.lock();

        match writing_file_ref.write_row(&row) {
            Err(DataStorageError::StorageOverflow(id)) => {
                debug!("Flush writing storage with id: {} on overflow", id);
                self.do_flush_writing_file(&mut writing_file_ref)?;
                Ok(writing_file_ref.write_row(&row)?)
            }
            r => Ok(r?),
        }
    }

    pub fn flush_writing_file(&self) -> BitcaskResult<()> {
        let mut writing_file_ref = self.writing_storage.lock();
        debug!(
            "Flush writing file with id: {}",
            writing_file_ref.storage_id()
        );
        // flush file only when we actually wrote something
        self.do_flush_writing_file(&mut writing_file_ref)?;

        Ok(())
    }

    pub fn recovery_iter(&self) -> BitcaskResult<DatabaseRecoverIter> {
        let mut storage_ids: Vec<StorageId>;
        {
            let writing_storage = self.writing_storage.lock();
            let writing_storage_id = writing_storage.storage_id();

            storage_ids = self
                .stable_storages
                .iter()
                .map(|f| f.lock().storage_id())
                .collect::<Vec<StorageId>>();
            storage_ids.push(writing_storage_id);
            storage_ids.sort();
            storage_ids.reverse();
        }
        DatabaseRecoverIter::new(
            self.database_dir.clone(),
            storage_ids,
            self.options.storage_options,
        )
    }

    pub fn iter(&self) -> BitcaskResult<DatabaseIter> {
        let mut storage_ids: Vec<StorageId>;
        {
            let writing_storage = self.writing_storage.lock();
            let writing_storage_id = writing_storage.storage_id();

            storage_ids = self
                .stable_storages
                .iter()
                .map(|f| f.lock().storage_id())
                .collect::<Vec<StorageId>>();
            storage_ids.push(writing_storage_id);
        }

        let files: BitcaskResult<Vec<DataStorage>> = storage_ids
            .iter()
            .map(|f| {
                DataStorage::open(&self.database_dir, *f, self.options.storage_options)
                    .map_err(BitcaskError::StorageError)
            })
            .collect();

        let mut opened_stable_files = files?;
        opened_stable_files.sort_by_key(|e| e.storage_id());
        let iters: crate::database::data_storage::Result<Vec<StorageIter>> =
            opened_stable_files.iter().rev().map(|f| f.iter()).collect();

        Ok(DatabaseIter::new(iters?))
    }

    pub fn read_value(&self, row_location: &RowLocation) -> BitcaskResult<TimedValue<Value>> {
        {
            let mut writing_file_ref = self.writing_storage.lock();
            if row_location.storage_id == writing_file_ref.storage_id() {
                return Ok(writing_file_ref.read_value(row_location.row_offset)?);
            }
        }

        let l = self.get_file_to_read(row_location.storage_id)?;
        let mut f = l.lock();
        let ret = f.read_value(row_location.row_offset)?;
        Ok(ret)
    }

    pub fn reload_data_files(&self, data_storage_ids: Vec<StorageId>) -> BitcaskResult<()> {
        let (writing, stables) = prepare_load_storages(
            &self.database_dir,
            &data_storage_ids,
            &self.storage_id_generator,
            self.options.storage_options,
        )?;

        {
            let mut writing_storage_ref = self.writing_storage.lock();
            debug!(
                "reload writing storage with id: {}",
                writing_storage_ref.storage_id()
            );
            let _ = mem::replace(&mut *writing_storage_ref, writing);
        }

        self.stable_storages.clear();

        for s in stables {
            if self.stable_storages.contains_key(&s.storage_id()) {
                core::panic!("file id: {} already loaded in database", s.storage_id());
            }
            debug!("reload stable file with id: {}", s.storage_id());
            self.stable_storages.insert(s.storage_id(), Mutex::new(s));
        }
        Ok(())
    }

    pub fn get_storage_ids(&self) -> StorageIds {
        let writing_file_ref = self.writing_storage.lock();
        let writing_storage_id = writing_file_ref.storage_id();
        let stable_storage_ids: Vec<StorageId> = self
            .stable_storages
            .iter()
            .map(|f| f.value().lock().storage_id())
            .collect();
        StorageIds {
            stable_storage_ids,
            writing_storage_id,
        }
    }

    pub fn stats(&self) -> BitcaskResult<DatabaseStats> {
        let writing_file_size: u64;
        {
            writing_file_size = self.writing_storage.lock().storage_size() as u64;
        }
        let mut total_data_size_in_bytes: u64 = self
            .stable_storages
            .iter()
            .map(|f| {
                let file = f.value().lock();
                file.storage_size() as u64
            })
            .collect::<Vec<u64>>()
            .iter()
            .sum();

        total_data_size_in_bytes += writing_file_size;

        Ok(DatabaseStats {
            number_of_data_files: self.stable_storages.len() + 1,
            total_data_size_in_bytes,
            number_of_pending_hint_files: self.hint_file_writer.len(),
        })
    }

    pub fn close(&self) -> BitcaskResult<()> {
        let mut writing_file_ref = self.writing_storage.lock();
        writing_file_ref.flush()?;
        Ok(())
    }

    pub fn drop(&self) -> BitcaskResult<()> {
        debug!("Drop database called");

        {
            let mut writing_file_ref = self.writing_storage.lock();
            debug!(
                "Flush writing file with id: {} on drop database",
                writing_file_ref.storage_id()
            );
            // flush file only when we actually wrote something
            self.do_flush_writing_file(&mut writing_file_ref)?;
        }
        for storage_id in self.stable_storages.iter().map(|v| v.lock().storage_id()) {
            SelfFs::delete_file(&self.database_dir, FileType::DataFile, Some(storage_id))?;
        }
        self.stable_storages.clear();
        Ok(())
    }

    pub fn sync(&self) -> BitcaskResult<()> {
        let mut f = self.writing_storage.lock();
        f.flush()?;
        Ok(())
    }

    pub fn mark_db_error(&self, error_string: String) {
        let mut err = self.is_error.lock();
        *err = Some(error_string)
    }

    pub fn check_db_error(&self) -> Result<(), BitcaskError> {
        let err = self.is_error.lock();
        if err.is_some() {
            return Err(BitcaskError::DatabaseBroken(err.as_ref().unwrap().clone()));
        }
        Ok(())
    }

    fn start_sync_worker(&self, datastorage: Arc<Mutex<DataStorage>>) -> JoinHandle<()> {
        let sync_duration = Duration::from_secs(self.options.sync_interval_sec);
        let receiver = crossbeam_channel::tick(sync_duration);
        thread::spawn(move || loop {
            let mut last_sync = Instant::now();
            while let Ok(_) = receiver.recv() {
                if last_sync.elapsed() < sync_duration {
                    continue;
                }

                trace!("Attempting syncing");
                let mut f = datastorage.lock();
                if let Err(e) = f.flush() {
                    error!(target: "Database", "flush database failed: {}", e);
                }
                last_sync = Instant::now();
            }
            info!(target: "Database", "sync worker done");
        })
    }

    fn do_flush_writing_file(
        &self,
        writing_file_ref: &mut MutexGuard<DataStorage>,
    ) -> BitcaskResult<()> {
        if writing_file_ref.storage_size() == 0 {
            debug!(
                "Skip flush empty wirting file with id: {}",
                writing_file_ref.storage_id()
            );
            return Ok(());
        }
        let next_storage_id = self.storage_id_generator.generate_next_id();
        let next_writing_file = DataStorage::new(
            &self.database_dir,
            next_storage_id,
            self.options.storage_options,
        )?;
        let old_file = mem::replace(&mut **writing_file_ref, next_writing_file);

        let stable_storage = old_file.transit_to_readonly()?;

        let storage_id = stable_storage.storage_id();
        self.stable_storages
            .insert(storage_id, Mutex::new(stable_storage));
        self.hint_file_writer.async_write_hint_file(storage_id);
        debug!(target: "Database", "writing file with id: {} flushed, new writing file with id: {} created", storage_id, next_storage_id);
        Ok(())
    }

    fn get_file_to_read(
        &self,
        storage_id: StorageId,
    ) -> BitcaskResult<RefMut<StorageId, Mutex<DataStorage>>> {
        self.stable_storages
            .get_mut(&storage_id)
            .ok_or(BitcaskError::TargetFileIdNotFound(storage_id))
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let ret = self.close();
        if ret.is_err() {
            error!(target: "Database", "close database failed: {}", ret.err().unwrap())
        }
        info!(target: "Database", "database on directory: {:?} closed", self.database_dir)
    }
}

pub struct DatabaseIter {
    current_iter: Cell<Option<StorageIter>>,
    remain_iters: Vec<StorageIter>,
}

impl DatabaseIter {
    fn new(mut iters: Vec<StorageIter>) -> Self {
        if iters.is_empty() {
            DatabaseIter {
                remain_iters: iters,
                current_iter: Cell::new(None),
            }
        } else {
            let current_iter = iters.pop();
            DatabaseIter {
                remain_iters: iters,
                current_iter: Cell::new(current_iter),
            }
        }
    }
}

impl Iterator for DatabaseIter {
    type Item = BitcaskResult<RowToRead>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.current_iter.get_mut() {
                None => break,
                Some(iter) => match iter.next() {
                    None => {
                        self.current_iter.replace(self.remain_iters.pop());
                    }
                    other => return other.map(|r| r.map_err(BitcaskError::StorageError)),
                },
            }
        }
        None
    }
}

fn recovered_iter(
    database_dir: &Path,
    storage_id: StorageId,
    storage_options: DataStorageOptions,
) -> BitcaskResult<Box<dyn Iterator<Item = BitcaskResult<RecoveredRow>>>> {
    if FileType::HintFile
        .get_path(database_dir, Some(storage_id))
        .exists()
    {
        debug!(target: "Database", "recover from hint file with id: {}", storage_id);
        Ok(Box::new(HintFile::open_iterator(database_dir, storage_id)?))
    } else {
        debug!(target: "Database", "recover from data file with id: {}", storage_id);
        let stable_file = DataStorage::open(database_dir, storage_id, storage_options)?;
        let i = stable_file.iter().map(|iter| {
            iter.map(|row| {
                row.map(|r| RecoveredRow {
                    row_location: r.row_location,
                    timestamp: r.timestamp,
                    key: r.key,
                    is_tombstone: utils::is_tombstone(&r.value),
                })
                .map_err(BitcaskError::StorageError)
            })
        })?;
        Ok(Box::new(i))
    }
}

pub struct DatabaseRecoverIter {
    current_iter: Cell<Option<Box<dyn Iterator<Item = BitcaskResult<RecoveredRow>>>>>,
    data_storage_ids: Vec<StorageId>,
    database_dir: PathBuf,

    storage_options: DataStorageOptions,
}

impl DatabaseRecoverIter {
    fn new(
        database_dir: PathBuf,
        mut iters: Vec<StorageId>,

        storage_options: DataStorageOptions,
    ) -> BitcaskResult<Self> {
        if let Some(id) = iters.pop() {
            let iter: Box<dyn Iterator<Item = BitcaskResult<RecoveredRow>>> =
                recovered_iter(&database_dir, id, storage_options)?;
            Ok(DatabaseRecoverIter {
                database_dir,
                data_storage_ids: iters,
                current_iter: Cell::new(Some(iter)),
                storage_options,
            })
        } else {
            Ok(DatabaseRecoverIter {
                database_dir,
                data_storage_ids: iters,
                current_iter: Cell::new(None),
                storage_options,
            })
        }
    }
}

impl Iterator for DatabaseRecoverIter {
    type Item = BitcaskResult<RecoveredRow>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.current_iter.get_mut() {
                None => break,
                Some(iter) => match iter.next() {
                    None => {
                        if let Some(id) = self.data_storage_ids.pop() {
                            match recovered_iter(&self.database_dir, id, self.storage_options) {
                                Ok(iter) => {
                                    self.current_iter.replace(Some(iter));
                                }
                                Err(e) => return Some(Err(e)),
                            }
                        } else {
                            break;
                        }
                    }
                    other => return other,
                },
            }
        }
        None
    }
}

fn open_storages<P: AsRef<Path>>(
    database_dir: P,
    data_storage_ids: &[u32],
    storage_options: DataStorageOptions,
) -> BitcaskResult<Vec<DataStorage>> {
    let mut storage_ids = data_storage_ids.to_owned();
    storage_ids.sort();

    Ok(storage_ids
        .iter()
        .map(|id| DataStorage::open(&database_dir, *id, storage_options))
        .collect::<crate::database::data_storage::Result<Vec<DataStorage>>>()?)
}

fn prepare_load_storages<P: AsRef<Path>>(
    database_dir: P,
    data_storage_ids: &[u32],
    storage_id_generator: &StorageIdGenerator,
    storage_options: DataStorageOptions,
) -> BitcaskResult<(DataStorage, Vec<DataStorage>)> {
    let mut storages = open_storages(&database_dir, data_storage_ids, storage_options)?;
    let writing_storage = if storages.last().map_or(Ok(true), |s| s.is_readonly())? {
        let writing_storage_id = storage_id_generator.generate_next_id();
        let storage = DataStorage::new(&database_dir, writing_storage_id, storage_options)?;
        debug!(target: "Database", "create writing file with id: {}", writing_storage_id);
        storage
    } else {
        let storage = storages.pop().unwrap();
        debug!(target: "Database", "reuse writing file with id: {}", storage.storage_id());
        storage
    };

    Ok((writing_storage, storages))
}

#[cfg(test)]
pub mod database_tests_utils {
    use bitcask_tests::common::TestingKV;

    use crate::database::{common::TimedValue, data_storage::DataStorageOptions, RowLocation};

    use super::{DataBaseOptions, Database};

    pub const DEFAULT_OPTIONS: DataBaseOptions = DataBaseOptions {
        storage_options: DataStorageOptions {
            max_file_size: 1024,
        },
        sync_interval_sec: 60,
    };

    pub struct TestingRow {
        pub kv: TestingKV,
        pub pos: RowLocation,
    }

    impl TestingRow {
        pub fn new(kv: TestingKV, pos: RowLocation) -> Self {
            TestingRow { kv, pos }
        }
    }

    pub fn assert_rows_value(db: &Database, expect: &Vec<TestingRow>) {
        for row in expect {
            assert_row_value(db, row);
        }
    }

    pub fn assert_row_value(db: &Database, expect: &TestingRow) {
        let actual = db.read_value(&expect.pos).unwrap();
        assert_eq!(*expect.kv.value(), *actual.value);
    }

    pub fn assert_database_rows(db: &Database, expect_rows: &Vec<TestingRow>) {
        let mut i = 0;
        for actual_row in db.iter().unwrap().map(|r| r.unwrap()) {
            let expect_row = expect_rows.get(i).unwrap();
            assert_eq!(expect_row.kv.key(), actual_row.key);
            assert_eq!(expect_row.kv.value(), actual_row.value);
            assert_eq!(expect_row.pos, actual_row.row_location);
            i += 1;
        }
        assert_eq!(expect_rows.len(), i);
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
}

#[cfg(test)]
mod tests {

    use crate::database::database_tests_utils::{
        assert_database_rows, assert_rows_value, write_kvs_to_db, TestingRow, DEFAULT_OPTIONS,
    };

    use super::*;

    use bitcask_tests::common::{get_temporary_directory_path, TestingKV};
    use test_log::test;

    #[test]
    fn test_read_write_writing_file() {
        let dir = get_temporary_directory_path();
        let storage_id_generator = Arc::new(StorageIdGenerator::new());
        let db = Database::open(&dir, storage_id_generator, DEFAULT_OPTIONS).unwrap();
        let kvs = vec![
            TestingKV::new("k1", "value1"),
            TestingKV::new("k2", "value2"),
            TestingKV::new("k3", "value3"),
            TestingKV::new("k1", "value4"),
        ];
        let rows = write_kvs_to_db(&db, kvs);
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_read_write_with_stable_files() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let storage_id_generator = Arc::new(StorageIdGenerator::new());
        let db = Database::open(&dir, storage_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        let kvs = vec![
            TestingKV::new("k1", "value1"),
            TestingKV::new("k2", "value2"),
        ];
        rows.append(&mut write_kvs_to_db(&db, kvs));
        db.flush_writing_file().unwrap();

        let kvs = vec![
            TestingKV::new("k3", "hello world"),
            TestingKV::new("k1", "value4"),
        ];
        rows.append(&mut write_kvs_to_db(&db, kvs));
        db.flush_writing_file().unwrap();

        assert_eq!(3, storage_id_generator.get_id());
        assert_eq!(2, db.stable_storages.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_recovery() {
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
        {
            let db = Database::open(&dir, storage_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k3", "hello world"),
                TestingKV::new("k1", "value4"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }

        let db = Database::open(&dir, storage_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        assert_eq!(1, storage_id_generator.get_id());
        assert_eq!(0, db.stable_storages.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_wrap_file() {
        let storage_id_generator = Arc::new(StorageIdGenerator::new());
        let dir = get_temporary_directory_path();
        let db = Database::open(
            &dir,
            storage_id_generator,
            DataBaseOptions {
                storage_options: DataStorageOptions { max_file_size: 104 },
                sync_interval_sec: 60,
            },
        )
        .unwrap();
        let kvs = vec![
            TestingKV::new("k1", "value1_value1_value1"),
            TestingKV::new("k2", "value2_value2_value2"),
            TestingKV::new("k3", "value3_value3_value3"),
            TestingKV::new("k1", "value4_value4_value4"),
        ];
        assert_eq!(0, db.stable_storages.len());
        let rows = write_kvs_to_db(&db, kvs);
        assert_rows_value(&db, &rows);
        assert_eq!(1, db.stable_storages.len());
        assert_database_rows(&db, &rows);
    }
}
