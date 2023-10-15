use std::{
    cell::Cell,
    mem,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

use dashmap::{mapref::one::RefMut, DashMap};
use parking_lot::{Mutex, MutexGuard};

use crate::{
    database::hint::{self, HintFileWriter},
    error::{BitcaskError, BitcaskResult},
    file_id::{FileId, FileIdGenerator},
    fs::{self as SelfFs, FileType},
    utils,
};
use log::{debug, error, info};

use super::{
    common::{RecoveredRow, TimedValue, Value},
    storage::{RowStorage, StableFileIter, StorageReader, StorageWriter},
};
use super::{
    common::{RowLocation, RowToRead, RowToWrite},
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
pub struct FileIds {
    pub stable_file_ids: Vec<FileId>,
    pub writing_file_id: FileId,
}

#[derive(Debug, Clone, Copy)]
pub struct DataBaseOptions {
    pub max_file_size: u64,
}

#[derive(Debug)]
pub struct Database {
    pub database_dir: PathBuf,
    file_id_generator: Arc<FileIdGenerator>,
    writing_file: Mutex<RowStorage>,
    stable_files: DashMap<FileId, Mutex<RowStorage>>,
    options: DataBaseOptions,
    hint_file_writer: HintFileWriter,
    is_error: Mutex<Option<String>>,
}

impl Database {
    pub fn open(
        directory: &Path,
        file_id_generator: Arc<FileIdGenerator>,
        options: DataBaseOptions,
    ) -> BitcaskResult<Database> {
        let database_dir: PathBuf = directory.into();

        debug!(target: "Database", "opening database at directory {:?}", directory);

        hint::clear_temp_hint_file_directory(&database_dir);

        let data_file_ids = SelfFs::get_file_ids_in_dir(&database_dir, FileType::DataFile);
        if let Some(id) = data_file_ids.iter().max() {
            file_id_generator.update_file_id(*id);
        }
        let writing_file_id = file_id_generator.generate_next_file_id();
        let writing_file = Mutex::new(RowStorage::new(&database_dir, writing_file_id)?);
        debug!(target: "Database", "create writing file with id: {}", writing_file_id);
        let hint_file_writer = HintFileWriter::start(&database_dir);

        let db = Database {
            writing_file,
            file_id_generator,
            database_dir,
            stable_files: DashMap::new(),
            options,
            hint_file_writer,
            is_error: Mutex::new(None),
        };
        db.reload_data_files(data_file_ids)?;
        info!(target: "Database", "database opened at directory: {:?}, with {} data files", directory, db.get_file_ids().stable_file_ids.len());
        Ok(db)
    }

    pub fn get_database_dir(&self) -> &Path {
        &self.database_dir
    }

    pub fn get_max_file_id(&self) -> FileId {
        let writing_file_ref = self.writing_file.lock();
        writing_file_ref.file_id()
    }

    pub fn write<V: Deref<Target = [u8]>>(
        &self,
        key: &Vec<u8>,
        value: TimedValue<V>,
    ) -> BitcaskResult<RowLocation> {
        let row = RowToWrite::new(key, value);
        self.do_write(row)
    }

    pub fn flush_writing_file(&self) -> BitcaskResult<()> {
        let mut writing_file_ref = self.writing_file.lock();
        debug!("Flush writing file with id: {}", writing_file_ref.file_id());
        // flush file only when we actually wrote something
        self.do_flush_writing_file(&mut writing_file_ref)?;

        Ok(())
    }

    pub fn recovery_iter(&self) -> BitcaskResult<DatabaseRecoverIter> {
        let mut file_ids: Vec<FileId>;
        {
            let writing_file = self.writing_file.lock();
            let writing_file_id = writing_file.file_id();

            file_ids = self
                .stable_files
                .iter()
                .map(|f| f.lock().file_id())
                .collect::<Vec<FileId>>();
            file_ids.push(writing_file_id);
            file_ids.sort();
            file_ids.reverse();
        }
        DatabaseRecoverIter::new(self.database_dir.clone(), file_ids)
    }

    pub fn iter(&self) -> BitcaskResult<DatabaseIter> {
        let mut file_ids: Vec<FileId>;
        {
            let writing_file = self.writing_file.lock();
            let writing_file_id = writing_file.file_id();

            file_ids = self
                .stable_files
                .iter()
                .map(|f| f.lock().file_id())
                .collect::<Vec<FileId>>();
            file_ids.push(writing_file_id);
        }

        let files: BitcaskResult<Vec<RowStorage>> = file_ids
            .iter()
            .map(|f| {
                RowStorage::open(&self.database_dir, *f).map_err(|e| BitcaskError::StorageError(e))
            })
            .collect();

        let mut opened_stable_files = files?;
        opened_stable_files.sort_by_key(|e| e.file_id());
        let iters: crate::database::storage::Result<Vec<StableFileIter>> =
            opened_stable_files.iter().rev().map(|f| f.iter()).collect();

        Ok(DatabaseIter::new(iters?))
    }

    pub fn read_value(&self, row_position: &RowLocation) -> BitcaskResult<TimedValue<Value>> {
        {
            let mut writing_file_ref = self.writing_file.lock();
            if row_position.file_id == writing_file_ref.file_id() {
                return Ok(
                    writing_file_ref.read_value(row_position.row_offset, row_position.row_size)?
                );
            }
        }

        let l = self.get_file_to_read(row_position.file_id)?;
        let mut f = l.lock();
        let ret = f.read_value(row_position.row_offset, row_position.row_size)?;
        Ok(ret)
    }

    pub fn reload_data_files(&self, data_file_ids: Vec<FileId>) -> BitcaskResult<()> {
        self.stable_files.clear();

        for file_id in data_file_ids {
            if self.stable_files.contains_key(&file_id) {
                core::panic!("file id: {} already loaded in database", file_id);
            }
            if let Ok(f) = RowStorage::open(&self.database_dir, file_id) {
                if f.is_empty() {
                    info!(
                        target: "Database",
                        "skip load empty data file with id: {}", &file_id
                    );
                    continue;
                }
                self.stable_files.insert(file_id, Mutex::new(f));
            }
        }
        Ok(())
    }

    pub fn get_file_ids(&self) -> FileIds {
        let writing_file_ref = self.writing_file.lock();
        let writing_file_id = writing_file_ref.file_id();
        let stable_file_ids: Vec<FileId> = self
            .stable_files
            .iter()
            .map(|f| f.value().lock().file_id())
            .collect();
        FileIds {
            stable_file_ids,
            writing_file_id,
        }
    }

    pub fn stats(&self) -> BitcaskResult<DatabaseStats> {
        let writing_file_size: u64;
        {
            writing_file_size = self.writing_file.lock().file_size() as u64;
        }
        let mut total_data_size_in_bytes: u64 = self
            .stable_files
            .iter()
            .map(|f| {
                let file = f.value().lock();
                file.file_size() as u64
            })
            .collect::<Vec<u64>>()
            .iter()
            .sum();

        total_data_size_in_bytes += writing_file_size;

        Ok(DatabaseStats {
            number_of_data_files: self.stable_files.len() + 1,
            total_data_size_in_bytes,
            number_of_pending_hint_files: self.hint_file_writer.len(),
        })
    }

    pub fn close(&self) -> BitcaskResult<()> {
        let mut writing_file_ref = self.writing_file.lock();
        writing_file_ref.flush()?;
        Ok(())
    }

    pub fn drop(&self) -> BitcaskResult<()> {
        debug!("Drop database called");

        {
            let mut writing_file_ref = self.writing_file.lock();
            debug!(
                "Flush writing file with id: {} on drop database",
                writing_file_ref.file_id()
            );
            // flush file only when we actually wrote something
            self.do_flush_writing_file(&mut writing_file_ref)?;
        }
        for file_id in self.stable_files.iter().map(|v| v.lock().file_id()) {
            SelfFs::delete_file(&self.database_dir, FileType::DataFile, Some(file_id))?;
        }
        self.stable_files.clear();
        Ok(())
    }

    pub fn sync(&self) -> BitcaskResult<()> {
        let mut f = self.writing_file.lock();
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

    fn do_write<V: Deref<Target = [u8]>>(&self, row: RowToWrite<V>) -> BitcaskResult<RowLocation> {
        let mut writing_file_ref = self.writing_file.lock();
        if self.check_file_overflow(&writing_file_ref, &row) {
            debug!(
                "Flush writing file with id: {} on overflow",
                writing_file_ref.file_id()
            );
            self.do_flush_writing_file(&mut writing_file_ref)?;
        }
        Ok(writing_file_ref.write_row(row)?)
    }

    fn check_file_overflow<V: Deref<Target = [u8]>>(
        &self,
        writing_file_ref: &MutexGuard<RowStorage>,
        row: &RowToWrite<V>,
    ) -> bool {
        row.size + writing_file_ref.file_size() as u64 > self.options.max_file_size
    }

    fn do_flush_writing_file(
        &self,
        writing_file_ref: &mut MutexGuard<RowStorage>,
    ) -> BitcaskResult<()> {
        if writing_file_ref.file_size() <= 0 {
            debug!(
                "Skip flush empty wirting file with id: {}",
                writing_file_ref.file_id()
            );
            return Ok(());
        }
        let next_file_id = self.file_id_generator.generate_next_file_id();
        let next_writing_file = RowStorage::new(&self.database_dir, next_file_id)?;
        let old_file = mem::replace(&mut **writing_file_ref, next_writing_file);

        let stable_storage = old_file.transit_to_readonly()?;

        let file_id = stable_storage.file_id();
        self.stable_files
            .insert(file_id, Mutex::new(stable_storage));
        self.hint_file_writer.async_write_hint_file(file_id);
        debug!(target: "Database", "writing file with id: {} flushed, new writing file with id: {} created", file_id, next_file_id);
        Ok(())
    }

    fn get_file_to_read(
        &self,
        file_id: FileId,
    ) -> BitcaskResult<RefMut<FileId, Mutex<RowStorage>>> {
        self.stable_files
            .get_mut(&file_id)
            .ok_or(BitcaskError::TargetFileIdNotFound(file_id))
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
    current_iter: Cell<Option<StableFileIter>>,
    remain_iters: Vec<StableFileIter>,
}

impl DatabaseIter {
    fn new(mut iters: Vec<StableFileIter>) -> Self {
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
                    other => return other.map(|r| r.map_err(|e| BitcaskError::StorageError(e))),
                },
            }
        }
        None
    }
}

fn recovered_iter(
    database_dir: &Path,
    file_id: FileId,
) -> BitcaskResult<Box<dyn Iterator<Item = BitcaskResult<RecoveredRow>>>> {
    if FileType::HintFile
        .get_path(database_dir, Some(file_id))
        .exists()
    {
        debug!(target: "Database", "recover from hint file with id: {}", file_id);
        Ok(Box::new(HintFile::open_iterator(database_dir, file_id)?))
    } else {
        debug!(target: "Database", "recover from data file with id: {}", file_id);
        let stable_file = RowStorage::open(database_dir, file_id)?;
        let i = stable_file.iter().map(|iter| {
            iter.map(|row| {
                row.map(|r| RecoveredRow {
                    file_id: r.row_position.file_id,
                    timestamp: r.timestamp,
                    row_offset: r.row_position.row_offset,
                    row_size: r.row_position.row_size,
                    key: r.key,
                    is_tombstone: utils::is_tombstone(&r.value),
                })
                .map_err(|e| BitcaskError::StorageError(e))
            })
        })?;
        Ok(Box::new(i))
    }
}

pub struct DatabaseRecoverIter {
    current_iter: Cell<Option<Box<dyn Iterator<Item = BitcaskResult<RecoveredRow>>>>>,
    data_file_ids: Vec<FileId>,
    database_dir: PathBuf,
}

impl DatabaseRecoverIter {
    fn new(database_dir: PathBuf, mut iters: Vec<FileId>) -> BitcaskResult<Self> {
        if let Some(file_id) = iters.pop() {
            let iter: Box<dyn Iterator<Item = BitcaskResult<RecoveredRow>>> =
                recovered_iter(&database_dir, file_id)?;
            Ok(DatabaseRecoverIter {
                database_dir,
                data_file_ids: iters,
                current_iter: Cell::new(Some(iter)),
            })
        } else {
            Ok(DatabaseRecoverIter {
                database_dir,
                data_file_ids: iters,
                current_iter: Cell::new(None),
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
                        if let Some(file_id) = self.data_file_ids.pop() {
                            match recovered_iter(&self.database_dir, file_id) {
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

#[cfg(test)]
pub mod database_tests_utils {
    use bitcask_tests::common::TestingKV;

    use crate::database::{common::TimedValue, RowLocation};

    use super::{DataBaseOptions, Database};

    pub const DEFAULT_OPTIONS: DataBaseOptions = DataBaseOptions {
        max_file_size: 1024,
    };

    pub struct TestingRow {
        kv: TestingKV,
        pos: RowLocation,
    }

    impl TestingRow {
        fn new(kv: TestingKV, pos: RowLocation) -> Self {
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
            assert_eq!(expect_row.pos, actual_row.row_position);
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
                        file_id: pos.file_id,
                        row_offset: pos.row_offset,
                        row_size: pos.row_size,
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
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let db = Database::open(&dir, file_id_generator, DEFAULT_OPTIONS).unwrap();
        let kvs = vec![
            TestingKV::new("k1", "value1奥森"),
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
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
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

        assert_eq!(3, file_id_generator.get_file_id());
        assert_eq!(2, db.stable_files.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_recovery() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let file_id_generator = Arc::new(FileIdGenerator::new());
        {
            let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }
        {
            let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k3", "hello world"),
                TestingKV::new("k1", "value4"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }

        let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        assert_eq!(3, file_id_generator.get_file_id());
        assert_eq!(2, db.stable_files.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_wrap_file() {
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let dir = get_temporary_directory_path();
        let db = Database::open(
            &dir,
            file_id_generator,
            DataBaseOptions { max_file_size: 100 },
        )
        .unwrap();
        let kvs = vec![
            TestingKV::new("k1", "value1_value1_value1"),
            TestingKV::new("k2", "value2_value2_value2"),
            TestingKV::new("k3", "value3_value3_value3"),
            TestingKV::new("k1", "value4_value4_value4"),
        ];
        assert_eq!(0, db.stable_files.len());
        let rows = write_kvs_to_db(&db, kvs);
        assert_rows_value(&db, &rows);
        assert_eq!(1, db.stable_files.len());
        assert_database_rows(&db, &rows);
    }
}
