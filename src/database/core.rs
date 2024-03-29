use std::{
    cell::Cell,
    collections::HashMap,
    mem,
    path::{Path, PathBuf},
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crossbeam_channel::{select, Receiver, Sender};
use dashmap::{mapref::one::RefMut, DashMap};
use parking_lot::{Mutex, MutexGuard};

use crate::options::{BitcaskyOptions, SyncStrategy};
use crate::{
    clock::Clock,
    formatter::{BitcaskyFormatter, RowToWrite},
    fs::{self as SelfFs, FileType},
    storage_id::{StorageId, StorageIdGenerator},
};

use crate::database::{
    common::{DatabaseError, DatabaseResult},
    data_storage::DataStorageTelemetry,
    hint::{self, HintWriter},
};

use log::{debug, error, info, trace, warn};

use super::{
    common::{RecoveredRow, TimedValue},
    data_storage::{DataStorage, DataStorageReader, DataStorageWriter, StorageIter},
    DataStorageError,
};
use super::{
    common::{RowLocation, RowToRead},
    hint::HintFile,
};

#[derive(Debug)]
pub struct StorageAggregatedTelemetry {
    pub total_data_capacity: usize,
    pub total_data_size: usize,
    pub total_usage: f64,
    pub total_fragment: f64,
    pub total_read_value_times: u64,
    pub total_write_times: u64,
    pub total_dead_bytes: usize,
}

/**
 * Statistics of a Database.
 * Some of the metrics may not accurate due to concurrent access.
 */
#[derive(Debug)]
pub struct DatabaseTelemetry {
    pub writing_storage: DataStorageTelemetry,
    pub stable_storages: HashMap<StorageId, DataStorageTelemetry>,
    pub storage_aggregate: StorageAggregatedTelemetry,
    pub hint_file_writer: hint::HintWriterTelemetry,
}

#[derive(Debug)]
pub struct StorageIds {
    pub stable_storage_ids: Vec<StorageId>,
    pub writing_storage_id: StorageId,
}

#[derive(Debug)]
pub struct Database {
    pub database_dir: PathBuf,
    storage_id_generator: Arc<StorageIdGenerator>,
    writing_storage: Arc<Mutex<DataStorage>>,
    stable_storages: DashMap<StorageId, Mutex<DataStorage>>,
    options: Arc<BitcaskyOptions>,
    hint_file_writer: Option<HintWriter>,
    /// Process that periodically flushes writing storage
    sync_worker: Option<SyncWorker>,
    formatter: Arc<BitcaskyFormatter>,
    is_error: Mutex<Option<String>>,
}

impl Database {
    pub fn open(
        directory: &Path,
        storage_id_generator: Arc<StorageIdGenerator>,
        options: Arc<BitcaskyOptions>,
    ) -> DatabaseResult<Database> {
        let database_dir: PathBuf = directory.into();

        debug!(target: "Database", "opening database at directory {:?}", directory);

        hint::clear_temp_hint_file_directory(&database_dir);

        let data_storage_ids = SelfFs::get_storage_ids_in_dir(&database_dir, FileType::DataFile);
        if let Some(id) = data_storage_ids.iter().max() {
            storage_id_generator.update_id(*id);
        }

        let hint_file_writer = Some(HintWriter::start(&database_dir, options.clone()));

        let formatter = Arc::new(BitcaskyFormatter::default());
        let (writing_storage, storages) = prepare_db_storages(
            &database_dir,
            &data_storage_ids,
            &storage_id_generator,
            formatter.clone(),
            options.clone(),
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
            options: options.clone(),
            hint_file_writer,
            sync_worker: None,
            formatter,
            is_error: Mutex::new(None),
        };

        if let SyncStrategy::Interval(interval) = options.database.sync_strategy {
            let secs = interval.as_secs();
            if secs > 0 {
                db.sync_worker = Some(SyncWorker::start_sync_worker(
                    db.writing_storage.clone(),
                    secs,
                ));
            }
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

    pub fn write<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: TimedValue<V>,
    ) -> DatabaseResult<RowLocation> {
        let ts = value.expire_timestamp;
        let row: RowToWrite<K, TimedValue<V>> = RowToWrite::new_with_timestamp(key, value, ts);
        let mut writing_storage_ref = self.writing_storage.lock();

        match writing_storage_ref.write_row(&row) {
            Err(DataStorageError::StorageOverflow(id)) => {
                debug!("Flush writing storage with id: {} on overflow", id);
                self.do_flush_writing_file(&mut writing_storage_ref)?;
                Ok(writing_storage_ref.write_row(&row)?)
            }
            r => {
                let ret = r?;
                #[cfg(not(unix))]
                if let SyncStrategy::OSync = self.options.database.sync_strategy {
                    if let Err(e) = self.sync() {
                        error!(target: "Database", "flush database failed: {}", e);
                    }
                };
                Ok(ret)
            }
        }
    }

    pub fn add_dead_bytes(&self, storage_id: StorageId, dead_bytes: usize) {
        let mut writing_storage_ref = self.writing_storage.lock();
        if storage_id.eq(&writing_storage_ref.storage_id()) {
            writing_storage_ref.add_dead_bytes(dead_bytes);
        } else if let Some(storage) = self.stable_storages.get(&storage_id) {
            storage.lock().add_dead_bytes(dead_bytes);
        }
    }

    pub fn flush_writing_file(&self) -> DatabaseResult<()> {
        let mut writing_file_ref = self.writing_storage.lock();
        debug!(
            "Flush writing file with id: {}",
            writing_file_ref.storage_id()
        );
        // flush file only when we actually wrote something
        self.do_flush_writing_file(&mut writing_file_ref)?;

        Ok(())
    }

    pub fn recovery_iter(&self) -> DatabaseResult<DatabaseRecoverIter> {
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
        DatabaseRecoverIter::new(self.database_dir.clone(), storage_ids, self.options.clone())
    }

    pub fn iter(&self) -> DatabaseResult<DatabaseIter> {
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

        let files: DatabaseResult<Vec<DataStorage>> = storage_ids
            .iter()
            .map(|f| {
                DataStorage::open(&self.database_dir, *f, self.options.clone())
                    .map_err(DatabaseError::StorageError)
            })
            .collect();

        let mut opened_stable_files = files?;
        opened_stable_files.sort_by_key(|e| e.storage_id());
        let iters: crate::database::data_storage::Result<Vec<StorageIter>> =
            opened_stable_files.iter().rev().map(|f| f.iter()).collect();

        Ok(DatabaseIter::new(iters?))
    }

    pub fn read_value(
        &self,
        row_location: &RowLocation,
    ) -> DatabaseResult<Option<TimedValue<Vec<u8>>>> {
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

    pub fn reload_data_files(&self, data_storage_ids: Vec<StorageId>) -> DatabaseResult<()> {
        let (writing, stables) = prepare_db_storages(
            &self.database_dir,
            &data_storage_ids,
            &self.storage_id_generator,
            self.formatter.clone(),
            self.options.clone(),
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

    pub fn get_telemetry_data(&self) -> DatabaseTelemetry {
        let writing_storage = { self.writing_storage.lock().get_telemetry_data() };
        let stable_storages: HashMap<StorageId, DataStorageTelemetry> = HashMap::from_iter(
            self.stable_storages
                .iter()
                .map(|s| {
                    let d = s.lock();
                    (d.storage_id(), d.get_telemetry_data())
                })
                .collect::<Vec<_>>(),
        );

        let total_telemetry =
            stable_storages
                .values()
                .fold(writing_storage.clone(), |mut acc, next| {
                    acc.data_size += next.data_size;
                    acc.data_capacity += next.data_capacity;
                    acc.dead_bytes += next.dead_bytes;
                    acc.read_value_times += next.read_value_times;
                    acc.write_times += next.write_times;
                    acc
                });
        let total_fragment = total_telemetry.dead_bytes as f64 / total_telemetry.data_size as f64;
        let total_usage = total_telemetry.data_size as f64 / total_telemetry.data_capacity as f64;
        let storage_aggregate = StorageAggregatedTelemetry {
            total_data_capacity: total_telemetry.data_capacity,
            total_data_size: total_telemetry.data_size,
            total_fragment,
            total_usage,
            total_read_value_times: total_telemetry.read_value_times,
            total_write_times: total_telemetry.write_times,
            total_dead_bytes: total_telemetry.dead_bytes,
        };
        DatabaseTelemetry {
            hint_file_writer: self
                .hint_file_writer
                .as_ref()
                .map(|h| h.get_telemetry_data())
                .unwrap_or_default(),
            writing_storage,
            stable_storages,
            storage_aggregate,
        }
    }

    // Clear this database completely. Delete data physically and delete all data files.
    pub fn drop(&self) -> DatabaseResult<()> {
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
            SelfFs::delete_file(&self.database_dir, FileType::HintFile, Some(storage_id))?;
        }
        self.stable_storages.clear();
        Ok(())
    }

    pub fn sync(&self) -> DatabaseResult<()> {
        let mut f = self.writing_storage.lock();
        f.flush()?;
        Ok(())
    }

    pub fn mark_db_error(&self, error_string: String) {
        let mut err = self.is_error.lock();
        *err = Some(error_string)
    }

    pub fn check_db_error(&self) -> Result<(), DatabaseError> {
        let err = self.is_error.lock();
        if err.is_some() {
            return Err(DatabaseError::DatabaseBroken(err.as_ref().unwrap().clone()));
        }
        Ok(())
    }

    fn do_flush_writing_file(
        &self,
        writing_file_ref: &mut MutexGuard<DataStorage>,
    ) -> DatabaseResult<()> {
        if !writing_file_ref.is_dirty() {
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
            self.formatter.clone(),
            self.options.clone(),
        )?;
        let mut old_storage = mem::replace(&mut **writing_file_ref, next_writing_file);
        old_storage.flush()?;
        let storage_id = old_storage.storage_id();
        self.stable_storages
            .insert(storage_id, Mutex::new(old_storage));
        if let Some(w) = self.hint_file_writer.as_ref() {
            w.async_write_hint_file(storage_id);
        }
        debug!(target: "Database", "writing file with id: {} flushed, new writing file with id: {} created", storage_id, next_storage_id);
        Ok(())
    }

    fn get_file_to_read(
        &self,
        storage_id: StorageId,
    ) -> DatabaseResult<RefMut<StorageId, Mutex<DataStorage>>> {
        self.stable_storages
            .get_mut(&storage_id)
            .ok_or(DatabaseError::TargetFileIdNotFound(storage_id))
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let mut writing_file_ref = self.writing_storage.lock();
        if let Err(e) = writing_file_ref.flush() {
            warn!(target: "Database", "sync database failed: {}", e)
        }

        if let Some(worker) = self.sync_worker.take() {
            drop(worker);
        }

        if let Some(hint_w) = self.hint_file_writer.take() {
            drop(hint_w);
        }

        info!(target: "Database", "database on directory: {:?} closed", self.database_dir)
    }
}

#[derive(Debug)]
struct SyncWorker {
    stop_sender: Sender<()>,
    handle: Option<JoinHandle<()>>,
}

impl SyncWorker {
    fn start_sync_worker(
        datastorage: Arc<Mutex<DataStorage>>,
        sync_interval_sec: u64,
    ) -> SyncWorker {
        let channel = crossbeam_channel::bounded(1);
        let stop_sender = channel.0;
        let stop_receiver: Receiver<()> = channel.1;

        let sync_duration = Duration::from_secs(sync_interval_sec);
        let receiver = crossbeam_channel::tick(sync_duration);
        let handle = thread::spawn(move || {
            let mut last_sync = Instant::now();
            loop {
                select! {
                    recv(stop_receiver) -> _ => {
                        info!(target: "Database", "stopping sync worker");
                        return
                    }

                    recv(receiver) -> _ => {
                        if last_sync.elapsed() < sync_duration {
                            continue;
                        }

                        trace!("Attempting syncing");
                        let mut f = datastorage.lock();
                        if let Err(e) = f.flush() {
                            error!(target: "Database", "flush database failed: {}", e);
                        }
                        last_sync = Instant::now();
                    },
                }
            }
        });
        SyncWorker {
            stop_sender,
            handle: Some(handle),
        }
    }
}

impl Drop for SyncWorker {
    fn drop(&mut self) {
        if self.stop_sender.send(()).is_err() {
            warn!("Failed to stop sync worker.");
        }

        if let Some(handle) = self.handle.take() {
            if handle.join().is_err() {
                warn!(target: "Database", "wait sync worker done failed");
            }
        }
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
    type Item = DatabaseResult<RowToRead>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.current_iter.get_mut() {
                None => break,
                Some(iter) => match iter.next() {
                    None => {
                        self.current_iter.replace(self.remain_iters.pop());
                    }
                    other => return other.map(|r| r.map_err(DatabaseError::StorageError)),
                },
            }
        }
        None
    }
}

fn recovered_iter(
    database_dir: &Path,
    storage_id: StorageId,
    options: Arc<BitcaskyOptions>,
) -> DatabaseResult<Box<dyn Iterator<Item = DatabaseResult<RecoveredRow>>>> {
    if FileType::HintFile
        .get_path(database_dir, Some(storage_id))
        .exists()
    {
        debug!(target: "Database", "recover from hint file with id: {}", storage_id);
        Ok(Box::new(HintFile::open_iterator(database_dir, storage_id)?))
    } else {
        debug!(target: "Database", "recover from data file with id: {}", storage_id);
        let stable_file = DataStorage::open(database_dir, storage_id, options.clone())?;
        let i = stable_file.iter().map(move |iter| {
            iter.map(move |row| {
                row.map(|r| RecoveredRow {
                    row_location: r.row_location,
                    key: r.key,
                    invalid: !r.value.is_valid(options.clock.now()),
                })
                .map_err(DatabaseError::StorageError)
            })
        })?;
        Ok(Box::new(i))
    }
}

pub struct DatabaseRecoverIter {
    current_iter: Cell<Option<Box<dyn Iterator<Item = DatabaseResult<RecoveredRow>>>>>,
    data_storage_ids: Vec<StorageId>,
    database_dir: PathBuf,
    options: Arc<BitcaskyOptions>,
}

impl DatabaseRecoverIter {
    fn new(
        database_dir: PathBuf,
        mut iters: Vec<StorageId>,
        options: Arc<BitcaskyOptions>,
    ) -> DatabaseResult<Self> {
        if let Some(id) = iters.pop() {
            let iter: Box<dyn Iterator<Item = DatabaseResult<RecoveredRow>>> =
                recovered_iter(&database_dir, id, options.clone())?;
            Ok(DatabaseRecoverIter {
                database_dir,
                data_storage_ids: iters,
                current_iter: Cell::new(Some(iter)),
                options,
            })
        } else {
            Ok(DatabaseRecoverIter {
                database_dir,
                data_storage_ids: iters,
                current_iter: Cell::new(None),
                options,
            })
        }
    }
}

impl Iterator for DatabaseRecoverIter {
    type Item = DatabaseResult<RecoveredRow>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.current_iter.get_mut() {
                None => break,
                Some(iter) => match iter.next() {
                    None => {
                        if let Some(id) = self.data_storage_ids.pop() {
                            match recovered_iter(&self.database_dir, id, self.options.clone()) {
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
    options: Arc<BitcaskyOptions>,
) -> DatabaseResult<Vec<DataStorage>> {
    let mut storage_ids = data_storage_ids.to_owned();
    storage_ids.sort();

    Ok(storage_ids
        .iter()
        .map(|id| DataStorage::open(&database_dir, *id, options.clone()))
        .collect::<crate::database::data_storage::Result<Vec<DataStorage>>>()?)
}

fn prepare_db_storages<P: AsRef<Path>>(
    database_dir: P,
    data_storage_ids: &[u32],
    storage_id_generator: &StorageIdGenerator,
    formatter: Arc<BitcaskyFormatter>,
    options: Arc<BitcaskyOptions>,
) -> DatabaseResult<(DataStorage, Vec<DataStorage>)> {
    let mut storages = open_storages(&database_dir, data_storage_ids, options.clone())?;
    let mut writing_storage;
    if storages.is_empty() {
        let writing_storage_id = storage_id_generator.generate_next_id();
        let storage = DataStorage::new(&database_dir, writing_storage_id, formatter, options)?;
        debug!(target: "Database", "create writing file with id: {}", writing_storage_id);
        writing_storage = storage;
    } else {
        writing_storage = storages.pop().unwrap();
        if let Err(e) = writing_storage.seek_to_end() {
            match e {
                DataStorageError::EofError() => {
                    warn!(target: "Database", "got EOF in writing file with id: {}", writing_storage.storage_id());
                }
                DataStorageError::DataStorageFormatter(e) => {
                    warn!(target: "Database", "has invalid data in writing file with id: {}, reason: {}", writing_storage.storage_id(), e);
                }
                _ => return Err(DatabaseError::StorageError(e)),
            }
        }
        debug!(target: "Database", "reuse writing file with id: {}", writing_storage.storage_id());
    }

    Ok((writing_storage, storages))
}

#[cfg(test)]
pub mod database_tests {
    use std::{
        io::{Seek, Write},
        sync::Arc,
        time::Duration,
    };

    use crate::options::{BitcaskyOptions, SyncStrategy};
    use crate::test_utils::{get_temporary_directory_path, TestingKV};
    use crate::{clock::DebugClock, fs, fs::FileType, storage_id::StorageIdGenerator};

    use test_log::test;

    use crate::database::{data_storage::DataStorageReader, RowLocation, TimedValue};

    use super::Database;

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

    fn get_database_options() -> BitcaskyOptions {
        BitcaskyOptions::default()
            .max_data_file_size(1024)
            .init_data_file_capacity(100)
            .sync_strategy(SyncStrategy::Interval(Duration::from_secs(60)))
            .init_hint_file_capacity(1024)
    }

    pub fn assert_rows_value(db: &Database, expect: &Vec<TestingRow>) {
        for row in expect {
            assert_row_value(db, row);
        }
    }

    pub fn assert_row_value(db: &Database, expect: &TestingRow) {
        let actual = db.read_value(&expect.pos).unwrap();
        if expect.kv.expire_timestamp() > 0 {
            assert!(actual.is_none());
        } else {
            assert_eq!(*expect.kv.value(), *actual.unwrap().value);
        }
    }

    pub fn assert_database_rows(db: &Database, expect_rows: &[TestingRow]) {
        let mut i = 0;
        for actual_row in db.iter().unwrap().map(|r| r.unwrap()) {
            let expect_row = expect_rows.get(i).unwrap();
            assert_eq!(expect_row.kv.key(), actual_row.key);
            assert_eq!(
                expect_row.kv.expire_timestamp(),
                actual_row.value.expire_timestamp
            );
            if expect_row.kv.expire_timestamp() > 0 {
                assert!(actual_row.value.value.is_empty());
            } else {
                assert_eq!(expect_row.kv.value(), actual_row.value.value);
            }

            assert_eq!(expect_row.pos, actual_row.row_location);
            i += 1;
        }
        assert_eq!(expect_rows.len(), i);
    }

    pub fn write_kvs_to_db(db: &Database, kvs: Vec<TestingKV>) -> Vec<TestingRow> {
        kvs.into_iter()
            .map(|kv| write_kv_to_db(db, kv))
            .collect::<Vec<TestingRow>>()
    }

    pub fn write_kv_to_db(db: &Database, kv: TestingKV) -> TestingRow {
        let pos = db
            .write(
                kv.key(),
                TimedValue::expirable_value(kv.value(), kv.expire_timestamp()),
            )
            .unwrap();
        TestingRow::new(kv, pos)
    }

    #[test]
    fn test_read_write_writing_file() {
        let dir = get_temporary_directory_path();
        let storage_id_generator = Arc::new(StorageIdGenerator::default());
        let db =
            Database::open(&dir, storage_id_generator, Arc::new(get_database_options())).unwrap();
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
    fn test_read_write_expirable_value_in_writing_file() {
        let dir = get_temporary_directory_path();
        let storage_id_generator = Arc::new(StorageIdGenerator::default());
        let clock = Arc::new(DebugClock::new(1000));
        let db = Database::open(
            &dir,
            storage_id_generator,
            Arc::new(get_database_options().debug_clock(clock)),
        )
        .unwrap();
        let kvs = vec![
            TestingKV::new_expirable("k1", "value1", 100),
            TestingKV::new("k2", "value2"),
            TestingKV::new_expirable("k3", "value3", 100),
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
        let storage_id_generator = Arc::new(StorageIdGenerator::default());
        let db = Database::open(
            &dir,
            storage_id_generator.clone(),
            Arc::new(get_database_options()),
        )
        .unwrap();
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
    fn test_read_write_expirable_value_in_stable_files() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let storage_id_generator = Arc::new(StorageIdGenerator::default());
        let db = Database::open(
            &dir,
            storage_id_generator.clone(),
            Arc::new(get_database_options()),
        )
        .unwrap();
        let kvs = vec![
            TestingKV::new("k1", "value1"),
            TestingKV::new_expirable("k2", "value2", 100),
        ];
        rows.append(&mut write_kvs_to_db(&db, kvs));
        db.flush_writing_file().unwrap();

        let kvs = vec![
            TestingKV::new_expirable("k3", "hello world", 100),
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
        let storage_id_generator = Arc::new(StorageIdGenerator::default());
        {
            let db = Database::open(
                &dir,
                storage_id_generator.clone(),
                Arc::new(get_database_options()),
            )
            .unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new_expirable("k2", "value2", 100),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
            assert_rows_value(&db, &rows);
        }
        {
            let db = Database::open(
                &dir,
                storage_id_generator.clone(),
                Arc::new(get_database_options()),
            )
            .unwrap();
            let kvs = vec![
                TestingKV::new("k3", "hello world"),
                TestingKV::new_expirable("k1", "value4", 100),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
            assert_rows_value(&db, &rows);
        }

        let db = Database::open(
            &dir,
            storage_id_generator.clone(),
            Arc::new(get_database_options()),
        )
        .unwrap();
        assert_eq!(1, storage_id_generator.get_id());
        assert_eq!(0, db.stable_storages.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_recovery_from_key_value_not_fully_written() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let storage_id_generator = Arc::new(StorageIdGenerator::default());

        {
            let db = Database::open(
                &dir,
                storage_id_generator.clone(),
                Arc::new(get_database_options()),
            )
            .unwrap();

            rows.push(write_kv_to_db(&db, TestingKV::new("k1", "value1")));
            write_kv_to_db(&db, TestingKV::new_expirable("k2", "value2", 100));

            let storage_id = db.writing_storage.lock().storage_id();
            let offset = db.writing_storage.lock().offset();
            let f = fs::open_file(&dir, FileType::DataFile, Some(storage_id))
                .unwrap()
                .file;

            // data file broken, key value not fully written
            f.set_len(offset as u64 - 1).unwrap();
        }
        {
            let db = Database::open(
                &dir,
                storage_id_generator.clone(),
                Arc::new(get_database_options()),
            )
            .unwrap();
            // can only recover one value
            assert_rows_value(&db, &rows);
            assert_database_rows(&db, &rows);
            // overwrite broken value
            rows.push(write_kv_to_db(&db, TestingKV::new("k3", "hello")));
        }

        let db = Database::open(
            &dir,
            storage_id_generator.clone(),
            Arc::new(get_database_options()),
        )
        .unwrap();
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_recovery_from_header_not_fully_written() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let storage_id_generator = Arc::new(StorageIdGenerator::default());

        {
            let db = Database::open(
                &dir,
                storage_id_generator.clone(),
                Arc::new(get_database_options()),
            )
            .unwrap();

            rows.push(write_kv_to_db(&db, TestingKV::new("k1", "value1")));
            let pos = write_kv_to_db(&db, TestingKV::new_expirable("k2", "value2", 100)).pos;

            let storage_id = db.writing_storage.lock().storage_id();
            let f = fs::open_file(&dir, FileType::DataFile, Some(storage_id))
                .unwrap()
                .file;

            // data file broken, header not fully written
            f.set_len((pos.row_offset + 1) as u64).unwrap();
        }

        {
            let db = Database::open(
                &dir,
                storage_id_generator.clone(),
                Arc::new(get_database_options()),
            )
            .unwrap();
            // can only recover one value
            assert_rows_value(&db, &rows);
            assert_database_rows(&db, &rows);
            // overwrite broken value
            rows.push(write_kv_to_db(&db, TestingKV::new("k3", "hello")));
        }

        let db = Database::open(
            &dir,
            storage_id_generator.clone(),
            Arc::new(get_database_options()),
        )
        .unwrap();
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_recovery_from_crc_failed() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let storage_id_generator = Arc::new(StorageIdGenerator::default());

        {
            let db = Database::open(
                &dir,
                storage_id_generator.clone(),
                Arc::new(get_database_options()),
            )
            .unwrap();

            rows.push(write_kv_to_db(&db, TestingKV::new("k1", "value1")));
            write_kv_to_db(&db, TestingKV::new_expirable("k2", "value2", 100));

            let storage_id = db.writing_storage.lock().storage_id();
            let offset = db.writing_storage.lock().offset();
            let mut f = fs::open_file(&dir, FileType::DataFile, Some(storage_id))
                .unwrap()
                .file;

            // data file broken, change last byte to break crc check
            f.set_len(offset as u64 - 1).unwrap();
            f.seek(std::io::SeekFrom::End(0)).unwrap();
            f.write_all(&[1_u8]).unwrap();
        }

        {
            let db = Database::open(
                &dir,
                storage_id_generator.clone(),
                Arc::new(get_database_options()),
            )
            .unwrap();
            // can only recover one value
            assert_rows_value(&db, &rows);
            assert_database_rows(&db, &rows);
            // overwrite broken value
            rows.push(write_kv_to_db(&db, TestingKV::new("k3", "hello")));
        }

        let db = Database::open(
            &dir,
            storage_id_generator.clone(),
            Arc::new(get_database_options()),
        )
        .unwrap();
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_wrap_file() {
        let storage_id_generator = Arc::new(StorageIdGenerator::default());
        let dir = get_temporary_directory_path();
        let db = Database::open(
            &dir,
            storage_id_generator,
            Arc::new(
                BitcaskyOptions::default()
                    .max_data_file_size(120)
                    .init_data_file_capacity(100),
            ),
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

    #[test]
    fn test_add_dead_bytes() {
        let storage_id_generator = Arc::new(StorageIdGenerator::default());
        let dir = get_temporary_directory_path();
        let db = Database::open(
            &dir,
            storage_id_generator,
            Arc::new(
                BitcaskyOptions::default()
                    .max_data_file_size(120)
                    .init_data_file_capacity(100),
            ),
        )
        .unwrap();
        let stable_row_lo = db
            .write("key", TimedValue::permanent_value("value"))
            .unwrap();
        db.flush_writing_file().unwrap();
        let writing_row_lo = db
            .write("key2", TimedValue::permanent_value("value2"))
            .unwrap();

        db.add_dead_bytes(stable_row_lo.storage_id, stable_row_lo.row_size);
        db.add_dead_bytes(writing_row_lo.storage_id, writing_row_lo.row_size);
        let telemetry = db.get_telemetry_data();
        assert_eq!(
            writing_row_lo.row_size,
            telemetry.writing_storage.dead_bytes
        );
        assert_eq!(
            stable_row_lo.row_size,
            telemetry
                .stable_storages
                .get(&stable_row_lo.storage_id)
                .unwrap()
                .dead_bytes
        );
    }
}
