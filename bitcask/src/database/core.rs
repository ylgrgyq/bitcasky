use std::{
    cell::Cell,
    fs::{self},
    mem,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    vec,
};

use dashmap::{mapref::one::RefMut, DashMap};

use crate::{
    error::{BitcaskError, BitcaskResult},
    file_id::FileIdGenerator,
    file_manager::{
        self, get_valid_data_file_ids, open_data_files_under_path, open_file, FileType,
    },
};
use log::{error, info, warn};

use super::writing_file::WritingFile;
use super::{
    common::{RowPosition, RowToRead, RowToWrite},
    hint::{HintFile, RowHint},
    stable_file::{StableFile, StableFileIter},
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
     * Number of data files in Database
     */
    pub number_of_hint_files: usize,
    /**
     * Data size in bytes of this Database
     */
    pub total_data_size_in_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct DataBaseOptions {
    pub max_file_size: usize,
    pub tolerate_data_file_corruption: bool,
}

fn validate_database_directory(dir: &Path) -> BitcaskResult<()> {
    fs::create_dir_all(dir)?;
    if !file_manager::check_directory_is_writable(dir) {
        return Err(BitcaskError::PermissionDenied(format!(
            "do not have writable permission for path: {}",
            dir.display()
        )));
    }
    Ok(())
}

fn shift_data_files(
    database_dir: &Path,
    known_max_file_id: u32,
    file_id_generator: &Arc<FileIdGenerator>,
) -> BitcaskResult<Vec<u32>> {
    let mut data_file_ids = get_valid_data_file_ids(database_dir)
        .into_iter()
        .filter(|id| *id >= known_max_file_id)
        .collect::<Vec<u32>>();
    // must change name in descending order to keep data file's order even when any change name operation failed
    data_file_ids.sort_by(|a, b| b.cmp(a));

    // rename files which file id >= knwon_max_file_id to files which file id greater than all merged files
    // because values in these files is written after merged files
    let mut new_file_ids = vec![];
    for from_id in data_file_ids {
        let new_file_id = file_id_generator.generate_next_file_id();
        file_manager::change_data_file_id(database_dir, from_id, new_file_id)?;
        new_file_ids.push(new_file_id);
    }
    Ok(new_file_ids)
}

fn recover_merge(
    database_dir: &Path,
    file_id_generator: &Arc<FileIdGenerator>,
) -> BitcaskResult<()> {
    let merge_file_dir = file_manager::merge_file_dir(database_dir);

    if !merge_file_dir.exists() {
        return Ok(());
    }

    let mut merge_data_file_ids = file_manager::get_valid_data_file_ids(&merge_file_dir);
    if merge_data_file_ids.is_empty() {
        return Ok(());
    }

    merge_data_file_ids.sort();
    let merge_meta = file_manager::read_merge_meta(&merge_file_dir)?;
    if *merge_data_file_ids.first().unwrap() <= merge_meta.known_max_file_id {
        return Err(BitcaskError::InvalidMergeDataFile(
            merge_meta.known_max_file_id,
            *merge_data_file_ids.first().unwrap(),
        ));
    }

    file_id_generator.update_file_id(*merge_data_file_ids.last().unwrap());

    shift_data_files(
        database_dir,
        merge_meta.known_max_file_id,
        file_id_generator,
    )?;

    file_manager::commit_merge_files(database_dir, &merge_data_file_ids)?;

    file_manager::purge_outdated_data_files(database_dir, merge_meta.known_max_file_id)?;

    let clear_ret = file_manager::clear_dir(&merge_file_dir);
    if clear_ret.is_err() {
        warn!(
            "clear merge directory failed after merge recovered. {}",
            clear_ret.unwrap_err()
        );
    }

    Ok(())
}

#[derive(Debug)]
pub struct Database {
    pub database_dir: PathBuf,
    file_id_generator: Arc<FileIdGenerator>,
    writing_file: Mutex<WritingFile>,
    stable_files: DashMap<u32, Mutex<StableFile>>,
    options: DataBaseOptions,
}

impl Database {
    pub fn open(
        directory: &Path,
        file_id_generator: Arc<FileIdGenerator>,
        options: DataBaseOptions,
    ) -> BitcaskResult<Database> {
        let database_dir: PathBuf = directory.into();
        validate_database_directory(&database_dir)?;

        let recover_ret = recover_merge(&database_dir, &file_id_generator);
        if let Err(err) = recover_ret {
            let merge_dir = file_manager::merge_file_dir(&database_dir);
            warn!(
                "recover merge under path: {} failed with error: \"{}\"",
                merge_dir.display(),
                err
            );
            match err {
                BitcaskError::InvalidMergeDataFile(_, _) => {
                    // clear Merge directory when recover merge failed
                    file_manager::clear_dir(&file_manager::merge_file_dir(&database_dir))?;
                }
                _ => return Err(err),
            }
        }

        let opened_stable_files = open_data_files_under_path(&database_dir)?;
        if !opened_stable_files.is_empty() {
            let writing_file_id = opened_stable_files.keys().max().unwrap_or(&0);
            file_id_generator.update_file_id(*writing_file_id);
        }
        let writing_file = Mutex::new(WritingFile::new(
            &database_dir,
            file_id_generator.generate_next_file_id(),
        )?);
        let stable_files = opened_stable_files
            .into_iter()
            .map(|(k, v)| {
                StableFile::new(&database_dir, k, v, options.tolerate_data_file_corruption)
                    .map(|stable_file| (k, Mutex::new(stable_file)))
            })
            .collect::<BitcaskResult<DashMap<u32, Mutex<StableFile>>>>()?;

        info!(target: "Database", "database opened at directory: {:?}, with {} file recovered", directory, stable_files.len());
        Ok(Database {
            writing_file,
            file_id_generator,
            database_dir,
            stable_files,
            options,
        })
    }

    pub fn get_database_dir(&self) -> &Path {
        &self.database_dir
    }

    pub fn get_max_file_id(&self) -> u32 {
        let writing_file_ref = self.writing_file.lock().unwrap();
        writing_file_ref.file_id()
    }

    pub fn write(&self, key: &Vec<u8>, value: &[u8]) -> BitcaskResult<RowPosition> {
        let row = RowToWrite::new(key, value);
        self.do_write(row)
    }

    pub fn write_with_timestamp(
        &self,
        key: &Vec<u8>,
        value: &[u8],
        timestamp: u64,
    ) -> BitcaskResult<RowPosition> {
        let row = RowToWrite::new_with_timestamp(key, value, timestamp);
        self.do_write(row)
    }

    pub fn flush_writing_file(&self) -> BitcaskResult<()> {
        let mut writing_file_ref = self.writing_file.lock().unwrap();
        // flush file only when we actually wrote something
        if writing_file_ref.file_size() > 0 {
            self.do_flush_writing_file(&mut writing_file_ref)?;
        }
        Ok(())
    }

    pub fn iter(&self) -> BitcaskResult<DatabaseIter> {
        let mut file_ids: Vec<u32>;
        {
            let writing_file = self.writing_file.lock().unwrap();
            let writing_file_id = writing_file.file_id();

            file_ids = self
                .stable_files
                .iter()
                .map(|f| f.lock().unwrap().file_id)
                .collect::<Vec<u32>>();
            file_ids.push(writing_file_id);
        }

        let files: BitcaskResult<Vec<StableFile>> = file_ids
            .iter()
            .map(|id| file_manager::open_file(&self.database_dir, FileType::DataFile, Some(*id)))
            .map(|f| {
                f.and_then(|f| match f.file_type {
                    FileType::DataFile => StableFile::new(
                        &self.database_dir,
                        f.file_id.unwrap(),
                        f.file,
                        self.options.tolerate_data_file_corruption,
                    ),
                    _ => unreachable!(),
                })
            })
            .collect();
        let mut opened_stable_files = files?;
        opened_stable_files.sort_by_key(|e| e.file_id);
        let iters: BitcaskResult<Vec<StableFileIter>> =
            opened_stable_files.iter().rev().map(|f| f.iter()).collect();
        Ok(DatabaseIter::new(iters?))
    }

    pub fn read_value(&self, row_position: &RowPosition) -> BitcaskResult<Vec<u8>> {
        {
            let mut writing_file_ref = self.writing_file.lock().unwrap();
            if row_position.file_id == writing_file_ref.file_id() {
                return writing_file_ref.read_value(row_position.row_offset, row_position.row_size);
            }
        }

        let l = self.get_file_to_read(row_position.file_id)?;
        let mut f = l.lock().unwrap();
        f.read_value(row_position.row_offset, row_position.row_size)
    }

    pub fn load_merged_files(
        &self,
        merged_file_ids: &Vec<u32>,
        known_max_file_id: u32,
    ) -> BitcaskResult<()> {
        if merged_file_ids.is_empty() {
            return Ok(());
        }
        self.flush_writing_file()?;

        let data_file_ids = shift_data_files(
            &self.database_dir,
            known_max_file_id,
            &self.file_id_generator,
        )?;

        // rebuild stable files with file id >= known_max_file_id files and merged files
        self.stable_files.clear();

        for file_id in data_file_ids {
            self.open_stable_file(file_id)?;
        }

        file_manager::commit_merge_files(&self.database_dir, merged_file_ids)?;

        for file_id in merged_file_ids {
            if self.stable_files.contains_key(file_id) {
                core::panic!("merged file id: {} already loaded in database", file_id);
            }
            self.open_stable_file(*file_id)?;
        }

        Ok(())
    }

    pub fn get_file_ids(&self) -> Vec<u32> {
        let writing_file_ref = self.writing_file.lock().unwrap();
        let writing_file_id = writing_file_ref.file_id();
        let mut ids: Vec<u32> = self
            .stable_files
            .iter()
            .map(|f| f.value().lock().unwrap().file_id)
            .collect();
        ids.push(writing_file_id);
        ids
    }

    pub fn write_hint_file(&self, file_id: u32) -> BitcaskResult<()> {
        let row_hint_file =
            file_manager::create_file(&self.database_dir, FileType::HintFile, Some(file_id))?;
        let mut hint_file = HintFile::new(&self.database_dir, file_id, row_hint_file);

        let data_file =
            file_manager::open_file(&self.database_dir, FileType::DataFile, Some(file_id))?;
        let stable_file_iter = StableFile::new(
            &self.database_dir,
            file_id,
            data_file.file,
            self.options.tolerate_data_file_corruption,
        )?
        .iter()?;

        let boxed_iter = Box::new(stable_file_iter.map(|ret| {
            ret.map(|row| RowHint {
                timestamp: row.row_position.tstmp,
                key_size: row.key.len(),
                value_size: row.value.len(),
                row_offset: row.row_position.row_offset,
                key: row.key,
            })
        }));
        hint_file.write_file(boxed_iter)
    }

    pub fn stats(&self) -> BitcaskResult<DatabaseStats> {
        let writing_file_size: u64;
        {
            writing_file_size = self.writing_file.lock().unwrap().file_size() as u64;
        }
        let mut total_data_size_in_bytes: u64 = self
            .stable_files
            .iter()
            .map(|f| {
                let file = f.value().lock().unwrap();
                file.file_size
            })
            .collect::<Vec<u64>>()
            .iter()
            .sum();
        total_data_size_in_bytes += writing_file_size;

        Ok(DatabaseStats {
            number_of_data_files: self.stable_files.len() + 1,
            number_of_hint_files: 0,
            total_data_size_in_bytes,
        })
    }

    pub fn close(&self) -> BitcaskResult<()> {
        let mut writing_file_ref = self.writing_file.lock().unwrap();
        writing_file_ref.flush()?;
        Ok(())
    }

    fn open_stable_file(&self, file_id: u32) -> BitcaskResult<()> {
        let data_file = open_file(&self.database_dir, FileType::DataFile, Some(file_id))?;
        let meta = data_file.file.metadata()?;
        if meta.len() == 0 {
            info!(target: "Database", "skip load empty data file with id: {}", &file_id);
            return Ok(());
        }
        let stable_file = StableFile::new(
            &self.database_dir,
            file_id,
            data_file.file,
            self.options.tolerate_data_file_corruption,
        )?;
        self.stable_files.insert(file_id, Mutex::new(stable_file));
        Ok(())
    }

    fn do_write(&self, row: RowToWrite) -> BitcaskResult<RowPosition> {
        let mut writing_file_ref = self.writing_file.lock().unwrap();
        if self.check_file_overflow(&writing_file_ref, &row) {
            self.do_flush_writing_file(&mut writing_file_ref)?;
        }
        let write_row_ret = writing_file_ref.write_row(row);
        if self.options.tolerate_data_file_corruption {
            if let Err(BitcaskError::DataFileCorrupted(_, _, _)) = write_row_ret {
                // tolerate data file corruption, so when write data file failed
                // we switch to a new data file
                self.do_flush_writing_file(&mut writing_file_ref)?;
            }
        }
        write_row_ret
    }

    fn check_file_overflow(
        &self,
        writing_file_ref: &MutexGuard<WritingFile>,
        row: &RowToWrite,
    ) -> bool {
        row.size + writing_file_ref.file_size() > self.options.max_file_size
    }

    fn do_flush_writing_file(
        &self,
        writing_file_ref: &mut MutexGuard<WritingFile>,
    ) -> BitcaskResult<()> {
        let next_file_id = self.file_id_generator.generate_next_file_id();
        let next_writing_file = WritingFile::new(&self.database_dir, next_file_id)?;
        let mut old_file = mem::replace(&mut **writing_file_ref, next_writing_file);
        old_file.flush()?;
        let (file_id, file) = old_file.transit_to_readonly()?;
        let stable_file = StableFile::new(
            &self.database_dir,
            file_id,
            file,
            self.options.tolerate_data_file_corruption,
        )?;
        self.stable_files.insert(file_id, Mutex::new(stable_file));
        Ok(())
    }

    fn get_file_to_read(&self, file_id: u32) -> BitcaskResult<RefMut<u32, Mutex<StableFile>>> {
        self.stable_files
            .get_mut(&file_id)
            .ok_or(BitcaskError::TargetFileIdNotFound(file_id))
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let ret = self.close();
        if ret.is_err() {
            error!(target: "Database", "Close database failed: {}", ret.err().unwrap())
        }
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
                    other => return other,
                },
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::{database::writing_file, file_manager::MergeMeta};

    use super::super::mocks::MockWritingFile;
    use super::*;
    use bitcask_tests::common::{get_temporary_directory_path, TestingKV};
    use test_log::test;

    const DEFAULT_OPTIONS: DataBaseOptions = DataBaseOptions {
        max_file_size: 1024,
        tolerate_data_file_corruption: true,
    };

    struct TestingRow {
        kv: TestingKV,
        pos: RowPosition,
    }

    impl TestingRow {
        fn new(kv: TestingKV, pos: RowPosition) -> Self {
            TestingRow { kv, pos }
        }
    }

    fn assert_rows_value(db: &Database, expect: &Vec<TestingRow>) {
        for row in expect {
            assert_row_value(db, row);
        }
    }

    fn assert_row_value(db: &Database, expect: &TestingRow) {
        let actual = db.read_value(&expect.pos).unwrap();
        assert_eq!(*expect.kv.value(), actual);
    }

    fn assert_database_rows(db: &Database, expect_rows: &Vec<TestingRow>) {
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

    fn write_kvs_to_db(db: &Database, kvs: Vec<TestingKV>) -> Vec<TestingRow> {
        kvs.into_iter()
            .map(|kv| {
                let pos = db.write(&kv.key(), &kv.value()).unwrap();
                TestingRow::new(kv, pos)
            })
            .collect::<Vec<TestingRow>>()
    }

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
    fn test_recover_merge_with_only_merge_meta() {
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
        let merge_file_dir = file_manager::create_merge_file_dir(&dir).unwrap();
        let merge_meta = MergeMeta {
            known_max_file_id: 101,
        };
        file_manager::write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        assert_eq!(2, file_id_generator.get_file_id());
        assert_eq!(1, db.stable_files.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_recover_merge_with_invalid_merge_meta() {
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
        let merge_file_dir = file_manager::create_merge_file_dir(&dir).unwrap();
        {
            // write something to data file in merge dir
            let db = Database::open(&merge_file_dir, file_id_generator.clone(), DEFAULT_OPTIONS)
                .unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            write_kvs_to_db(&db, kvs);
        }

        let merge_meta = MergeMeta {
            known_max_file_id: file_id_generator.generate_next_file_id(),
        };
        file_manager::write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        assert_eq!(4, file_id_generator.get_file_id());
        assert_eq!(1, db.stable_files.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
        assert!(!merge_file_dir.exists());
    }

    #[test]
    fn test_recover_merge() {
        let dir = get_temporary_directory_path();
        let file_id_generator = Arc::new(FileIdGenerator::new());
        {
            let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            write_kvs_to_db(&db, kvs);
        }
        let merge_meta = MergeMeta {
            known_max_file_id: file_id_generator.generate_next_file_id(),
        };
        let merge_file_dir = file_manager::create_merge_file_dir(&dir).unwrap();
        file_manager::write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        let mut rows: Vec<TestingRow> = vec![];
        {
            // write something to data file in merge dir
            let db = Database::open(&merge_file_dir, file_id_generator.clone(), DEFAULT_OPTIONS)
                .unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value3"),
                TestingKV::new("k2", "value4"),
                TestingKV::new("k3", "value5"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }

        let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        assert_eq!(4, file_id_generator.get_file_id());
        assert_eq!(1, db.stable_files.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
        assert!(!merge_file_dir.exists());
    }

    #[test]
    fn test_recover_merge_failed_with_unexpeded_error() {
        let dir = get_temporary_directory_path();
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let mut rows: Vec<TestingRow> = vec![];
        {
            let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }
        let merge_meta = MergeMeta {
            known_max_file_id: file_id_generator.generate_next_file_id(),
        };
        let merge_file_dir = file_manager::create_merge_file_dir(&dir).unwrap();
        file_manager::write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        {
            // write something to data file in merge dir
            let db = Database::open(&merge_file_dir, file_id_generator.clone(), DEFAULT_OPTIONS)
                .unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value3"),
                TestingKV::new("k2", "value4"),
            ];
            write_kvs_to_db(&db, kvs);
        }

        // change one data file under merge directory to readonly
        // so this file cannot recover and move to base directory
        let meta = fs::metadata(&merge_file_dir).unwrap();
        let mut perms = meta.permissions();
        perms.set_readonly(true);
        fs::set_permissions(&merge_file_dir, perms).unwrap();

        let ret = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS);
        assert!(ret.is_err());
    }

    #[test]
    fn test_wrap_file() {
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let dir = get_temporary_directory_path();
        let db = Database::open(
            &dir,
            file_id_generator,
            DataBaseOptions {
                max_file_size: 100,
                tolerate_data_file_corruption: true,
            },
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

    #[test]
    fn test_load_merged_files() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let old_db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        let kvs = vec![
            TestingKV::new("k1", "value1"),
            TestingKV::new("k2", "value2"),
        ];
        rows.append(&mut write_kvs_to_db(&old_db, kvs));
        {
            let merge_path = file_manager::create_merge_file_dir(&dir).unwrap();
            let db =
                Database::open(&merge_path, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k3", "hello world"),
                TestingKV::new("k1", "value4"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
            old_db
                .load_merged_files(&db.get_file_ids(), old_db.get_max_file_id())
                .unwrap();
        }

        assert_eq!(5, file_id_generator.get_file_id());
        assert_eq!(2, old_db.stable_files.len());
    }

    #[test]
    fn test_hint_file() {
        let dir = get_temporary_directory_path();
        let mut offset_values: Vec<(RowPosition, &str)> = vec![];
        {
            let file_id_generator = Arc::new(FileIdGenerator::new());
            let db = Database::open(&dir, file_id_generator, DEFAULT_OPTIONS).unwrap();
            let kvs = [("k1", "value1"), ("k2", "value2")];
            offset_values.append(
                &mut kvs
                    .into_iter()
                    .map(|(k, v)| (db.write(&k.into(), v.as_bytes()).unwrap(), v))
                    .collect::<Vec<(RowPosition, &str)>>(),
            );
        }
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let db = Database::open(&dir, file_id_generator, DEFAULT_OPTIONS).unwrap();
        db.write_hint_file(1);
    }
}
