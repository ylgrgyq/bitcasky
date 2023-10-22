use bytes::{Buf, Bytes};
use crc::{Crc, CRC_32_CKSUM};
use log::{debug, error};
use std::{
    fs::{File, Metadata},
    io::{Read, Seek, SeekFrom, Write},
    ops::Deref,
    path::{Path, PathBuf},
};
use thiserror::Error;

use crate::{
    file_id::FileId,
    fs::{self, create_file, FileType},
};

use super::{
    common::{RowToRead, RowToWrite, Value},
    constants::{
        DATA_FILE_KEY_OFFSET, DATA_FILE_KEY_SIZE_OFFSET, DATA_FILE_TSTAMP_OFFSET,
        DATA_FILE_VALUE_SIZE_OFFSET, KEY_SIZE_SIZE, VALUE_SIZE_SIZE,
    },
    RowLocation, TimedValue,
};

#[derive(Error, Debug)]
#[error("{}")]
pub enum DataStorageError {
    #[error("Write data file with id: {0} failed. error: {1}")]
    WriteRowFailed(FileId, String),
    #[error("Read data file with id: {0} failed. error: {1}")]
    ReadRowFailed(FileId, String),
    #[error("Flush writing data file with id: {0} failed. error: {1}")]
    FlushStorageFailed(FileId, String),
    #[error("Transit writing data file with id: {0} to readonly failed. error: {1}")]
    TransitToReadOnlyFailed(FileId, String),
    #[error("Writing storage overflow, need replace with a new one")]
    StorageOverflow(),
    #[error("Got IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Crc check failed on reading value with file id: {0}, offset: {1}. expect crc is: {2}, actual crc is: {3}")]
    CrcCheckFailed(u32, u64, u32, u32),
}

pub type Result<T> = std::result::Result<T, DataStorageError>;

pub trait DataStorageWriter {
    fn write_row<V: Deref<Target = [u8]>>(&mut self, row: &RowToWrite<V>) -> Result<RowLocation>;

    fn transit_to_readonly(self) -> Result<DataStorage>;

    fn flush(&mut self) -> Result<()>;
}

pub trait DataStorageReader {
    fn file_size(&self) -> usize;

    fn read_value(&mut self, row_offset: u64, row_size: u64) -> Result<TimedValue<Value>>;

    fn read_next_row(&mut self) -> Result<Option<RowToRead>>;
}

#[derive(Debug)]
enum DataStorageImpl {
    FileStorage(FileDataStorage),
}

#[derive(Debug, Clone, Copy)]
pub struct DataStorageOptions {
    pub max_file_size: u64,
}

#[derive(Debug)]
pub struct DataStorage {
    database_dir: PathBuf,
    file_id: FileId,
    storage_impl: DataStorageImpl,
    readonly: bool,
    options: DataStorageOptions,
}

impl DataStorage {
    pub fn new<P: AsRef<Path>>(
        database_dir: P,
        file_id: FileId,
        options: DataStorageOptions,
    ) -> Result<Self> {
        let path = database_dir.as_ref().to_path_buf();
        let data_file = create_file(&path, FileType::DataFile, Some(file_id))?;
        debug!(
            "Create storage under path: {:?} with file id: {}",
            &path, file_id
        );
        let meta = data_file.metadata()?;
        DataStorage::open_by_file(&path, file_id, data_file, meta, options)
    }

    pub fn open<P: AsRef<Path>>(
        database_dir: P,
        file_id: FileId,
        options: DataStorageOptions,
    ) -> Result<Self> {
        let path = database_dir.as_ref().to_path_buf();
        let mut data_file = fs::open_file(&path, FileType::DataFile, Some(file_id))?;
        debug!(
            "Open storage under path: {:?} with file id: {}",
            &path, file_id
        );
        let meta = data_file.file.metadata()?;
        if !meta.permissions().readonly() {
            data_file.file.seek(SeekFrom::End(0))?;
        }

        DataStorage::open_by_file(&path, file_id, data_file.file, meta, options)
    }

    pub fn file_id(&self) -> FileId {
        self.file_id
    }

    pub fn is_empty(&self) -> bool {
        self.file_size() == 0
    }

    pub fn is_readonly(&self) -> Result<bool> {
        Ok(self.readonly)
    }

    pub fn iter(&self) -> Result<StorageIter> {
        let data_file = fs::open_file(&self.database_dir, FileType::DataFile, Some(self.file_id))?;
        debug!(
            "Create iterator under path: {:?} with file id: {}",
            &self.database_dir, self.file_id
        );
        let meta = data_file.file.metadata()?;
        Ok(StorageIter {
            storage: DataStorage::open_by_file(
                &self.database_dir,
                self.file_id,
                data_file.file,
                meta,
                self.options,
            )?,
        })
    }

    pub fn check_storage_overflow<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<V>) -> bool {
        row.size + self.file_size() as u64 > self.options.max_file_size
    }

    fn open_by_file(
        database_dir: &PathBuf,
        file_id: FileId,
        data_file: File,
        meta: Metadata,
        options: DataStorageOptions,
    ) -> Result<Self> {
        let file_size = meta.len();

        Ok(DataStorage {
            storage_impl: DataStorageImpl::FileStorage(FileDataStorage::new(
                database_dir,
                file_id,
                data_file,
                file_size,
                options,
            )?),
            file_id,
            database_dir: database_dir.clone(),
            readonly: meta.permissions().readonly(),
            options,
        })
    }
}

impl DataStorageWriter for DataStorage {
    fn write_row<V: Deref<Target = [u8]>>(&mut self, row: &RowToWrite<V>) -> Result<RowLocation> {
        if self.check_storage_overflow(row) {
            return Err(DataStorageError::StorageOverflow());
        }
        let r = match &mut self.storage_impl {
            DataStorageImpl::FileStorage(s) => s
                .write_row(row)
                .map_err(|e| DataStorageError::WriteRowFailed(s.file_id, e.to_string())),
        }?;
        Ok(r)
    }

    fn transit_to_readonly(self) -> Result<DataStorage> {
        match self.storage_impl {
            DataStorageImpl::FileStorage(s) => {
                let file_id = s.file_id;
                s.transit_to_readonly()
                    .map_err(|e| DataStorageError::TransitToReadOnlyFailed(file_id, e.to_string()))
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        match &mut self.storage_impl {
            DataStorageImpl::FileStorage(s) => s
                .flush()
                .map_err(|e| DataStorageError::FlushStorageFailed(s.file_id, e.to_string())),
        }
    }
}

impl DataStorageReader for DataStorage {
    fn file_size(&self) -> usize {
        match &self.storage_impl {
            DataStorageImpl::FileStorage(s) => s.file_size(),
        }
    }

    fn read_value(&mut self, row_offset: u64, row_size: u64) -> Result<TimedValue<Value>> {
        match &mut self.storage_impl {
            DataStorageImpl::FileStorage(s) => s
                .read_value(row_offset, row_size)
                .map_err(|e| DataStorageError::ReadRowFailed(s.file_id, e.to_string())),
        }
    }

    fn read_next_row(&mut self) -> Result<Option<RowToRead>> {
        match &mut self.storage_impl {
            DataStorageImpl::FileStorage(s) => s.read_next_row(),
        }
    }
}

#[derive(Debug)]
pub struct StorageIter {
    storage: DataStorage,
}

impl Iterator for StorageIter {
    type Item = Result<RowToRead>;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.storage.read_next_row();
        match ret {
            Ok(o) => o.map(Ok),
            Err(e) => {
                error!(target: "Storage", "Data file with file id {} was corrupted. Error: {}", 
                self.storage.file_id(), &e);
                None
            }
        }
    }
}

#[derive(Debug)]
pub struct FileDataStorage {
    database_dir: PathBuf,
    data_file: File,
    pub file_id: FileId,
    capacity: u64,
    options: DataStorageOptions,
}

impl FileDataStorage {
    pub fn new<P: AsRef<Path>>(
        database_dir: P,
        file_id: FileId,
        data_file: File,
        capacity: u64,
        options: DataStorageOptions,
    ) -> Result<Self> {
        Ok(FileDataStorage {
            database_dir: database_dir.as_ref().to_path_buf(),
            data_file,
            file_id,
            capacity,
            options,
        })
    }
}

impl DataStorageWriter for FileDataStorage {
    fn write_row<V: Deref<Target = [u8]>>(&mut self, row: &RowToWrite<V>) -> Result<RowLocation> {
        let value_offset = self.capacity;
        let data_to_write = row.to_bytes();
        self.data_file.write_all(&data_to_write)?;
        self.capacity += data_to_write.len() as u64;

        Ok(RowLocation {
            file_id: self.file_id,
            row_offset: value_offset,
            row_size: row.size,
        })
    }

    fn transit_to_readonly(mut self) -> Result<DataStorage> {
        self.data_file.flush()?;

        let path = FileType::DataFile.get_path(&self.database_dir, Some(self.file_id));
        let mut perms = std::fs::metadata(&path)?.permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(path, perms)?;

        let meta = self.data_file.metadata()?;
        DataStorage::open_by_file(
            &self.database_dir,
            self.file_id,
            self.data_file,
            meta,
            self.options,
        )
    }

    fn flush(&mut self) -> Result<()> {
        Ok(self.data_file.flush()?)
    }
}

impl DataStorageReader for FileDataStorage {
    fn file_size(&self) -> usize {
        self.capacity as usize
    }

    fn read_value(&mut self, row_offset: u64, row_size: u64) -> Result<TimedValue<Value>> {
        self.data_file.seek(SeekFrom::Start(row_offset))?;
        let mut buf = vec![0; row_size as usize];
        self.data_file.read_exact(&mut buf)?;

        let bs = Bytes::from(buf);
        let expected_crc = bs.slice(0..4).get_u32();

        let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut ck = crc32.digest();
        ck.update(&bs.slice(4..));
        let actual_crc = ck.finalize();
        if expected_crc != actual_crc {
            return Err(DataStorageError::CrcCheckFailed(
                self.file_id,
                row_offset,
                expected_crc,
                actual_crc,
            ));
        }
        let timestamp = bs
            .slice(DATA_FILE_TSTAMP_OFFSET..DATA_FILE_KEY_SIZE_OFFSET)
            .get_u64();

        let key_size = bs
            .slice(DATA_FILE_KEY_SIZE_OFFSET..(DATA_FILE_KEY_SIZE_OFFSET + KEY_SIZE_SIZE))
            .get_u64() as usize;
        let val_size = bs
            .slice(DATA_FILE_VALUE_SIZE_OFFSET..(DATA_FILE_VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE))
            .get_u64() as usize;
        let val_offset = DATA_FILE_KEY_OFFSET + key_size;
        let ret = bs.slice(val_offset..val_offset + val_size);

        Ok(TimedValue {
            value: Value::VectorBytes(ret.into()),
            timestamp,
        })
    }

    fn read_next_row(&mut self) -> Result<Option<RowToRead>> {
        let value_offset = self.data_file.stream_position()?;
        if value_offset >= self.capacity {
            return Ok(None);
        }

        let mut header_buf = vec![0; DATA_FILE_KEY_OFFSET];
        self.data_file.read_exact(&mut header_buf)?;

        let header_bs = Bytes::from(header_buf);
        let expected_crc = header_bs.slice(0..DATA_FILE_TSTAMP_OFFSET).get_u32();

        self.data_file.metadata().unwrap();

        let tstmp = header_bs
            .slice(DATA_FILE_TSTAMP_OFFSET..DATA_FILE_KEY_SIZE_OFFSET)
            .get_u64();
        let key_size = header_bs
            .slice(DATA_FILE_KEY_SIZE_OFFSET..(DATA_FILE_KEY_SIZE_OFFSET + KEY_SIZE_SIZE))
            .get_u64() as usize;
        let value_size = header_bs
            .slice(DATA_FILE_VALUE_SIZE_OFFSET..(DATA_FILE_VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE))
            .get_u64() as usize;

        let mut kv_buf = vec![0; key_size + value_size];
        self.data_file.read_exact(&mut kv_buf)?;

        let kv_bs = Bytes::from(kv_buf);
        let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut ck = crc32.digest();
        ck.update(&header_bs[DATA_FILE_TSTAMP_OFFSET..]);
        ck.update(&kv_bs);
        let actual_crc = ck.finalize();
        if expected_crc != actual_crc {
            return Err(DataStorageError::CrcCheckFailed(
                self.file_id,
                value_offset,
                expected_crc,
                actual_crc,
            ));
        }

        Ok(Some(RowToRead {
            key: kv_bs.slice(0..key_size).into(),
            value: kv_bs.slice(key_size..).into(),
            row_position: RowLocation {
                file_id: self.file_id,
                row_offset: value_offset,
                row_size: (DATA_FILE_KEY_OFFSET + key_size + value_size) as u64,
            },
            timestamp: tstmp,
        }))
    }
}

#[cfg(test)]
mod tests {

    use crate::database::database_tests_utils::{TestingRow, DEFAULT_OPTIONS};

    use super::*;

    use bitcask_tests::common::{get_temporary_directory_path, TestingKV};
    use test_log::test;

    fn get_file_storage() -> FileDataStorage {
        let dir = get_temporary_directory_path();
        let file_id = 1;
        let f = create_file(&dir, FileType::DataFile, Some(file_id)).unwrap();
        let meta = f.metadata().unwrap();
        let options = DataStorageOptions {
            max_file_size: 1024,
        };
        FileDataStorage::new(&dir, 1, f, meta.len(), options).unwrap()
    }

    fn get_row_to_write(kvs: &Vec<TestingKV>) -> Vec<RowToWrite<'_, Vec<u8>>> {
        kvs.iter()
            .map(|kv| RowToWrite::new(kv.key_ref(), kv.value()))
            .collect::<Vec<RowToWrite<'_, Vec<u8>>>>()
    }

    pub fn assert_rows_value<S: DataStorageReader>(db: &mut S, expect: &Vec<TestingRow>) {
        for row in expect {
            assert_row_value(db, row);
        }
    }

    pub fn assert_row_value<S: DataStorageReader>(db: &mut S, expect: &TestingRow) {
        let actual = db
            .read_value(expect.pos.row_offset, expect.pos.row_size)
            .unwrap();
        assert_eq!(*expect.kv.value(), *actual.value);
    }

    pub fn assert_database_rows<S: DataStorageReader>(db: &S, expect_rows: &Vec<TestingRow>) {
        let mut i = 0;
        // StorageIter { storage: db };
        // for actual_row in db.iter().unwrap().map(|r| r.unwrap()) {
        //     let expect_row = expect_rows.get(i).unwrap();
        //     assert_eq!(expect_row.kv.key(), actual_row.key);
        //     assert_eq!(expect_row.kv.value(), actual_row.value);
        //     assert_eq!(expect_row.pos, actual_row.row_position);
        //     i += 1;
        // }
        // assert_eq!(expect_rows.len(), i);
    }

    pub fn write_kvs_to_storage<S: DataStorageWriter>(
        db: &mut S,
        kvs: &Vec<TestingKV>,
    ) -> Vec<TestingRow> {
        kvs.into_iter()
            .map(|kv| {
                let pos = db
                    .write_row(&RowToWrite::new(kv.key_ref(), kv.value()))
                    .unwrap();
                TestingRow::new(
                    kv.clone(),
                    RowLocation {
                        file_id: pos.file_id,
                        row_offset: pos.row_offset,
                        row_size: pos.row_size,
                    },
                )
            })
            .collect::<Vec<TestingRow>>()
    }

    #[test]
    fn test_read_write_storage() {
        let a = "k1";
        let b = "k2";
        let c: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&a.into(), b.into());

        let mut storage = get_file_storage();
        let kvs = vec![
            TestingKV::new("k1", "value1"),
            TestingKV::new("k2", "value2"),
            TestingKV::new("k3", "value3"),
            TestingKV::new("k1", "value4"),
        ];

        let rows = write_kvs_to_storage(&mut storage, &kvs);
        assert_rows_value(&mut storage, &rows);
        // assert_database_rows(&db, &rows);
    }

    // #[test]
    // fn test_read_write_with_stable_files() {
    //     let dir = get_temporary_directory_path();
    //     let mut rows: Vec<TestingRow> = vec![];
    //     let file_id_generator = Arc::new(FileIdGenerator::new());
    //     let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
    //     let kvs = vec![
    //         TestingKV::new("k1", "value1"),
    //         TestingKV::new("k2", "value2"),
    //     ];
    //     rows.append(&mut write_kvs_to_db(&db, kvs));
    //     db.flush_writing_file().unwrap();

    //     let kvs = vec![
    //         TestingKV::new("k3", "hello world"),
    //         TestingKV::new("k1", "value4"),
    //     ];
    //     rows.append(&mut write_kvs_to_db(&db, kvs));
    //     db.flush_writing_file().unwrap();

    //     assert_eq!(3, file_id_generator.get_file_id());
    //     assert_eq!(2, db.stable_storages.len());
    //     assert_rows_value(&db, &rows);
    //     assert_database_rows(&db, &rows);
    // }

    // #[test]
    // fn test_recovery() {
    //     let dir = get_temporary_directory_path();
    //     let mut rows: Vec<TestingRow> = vec![];
    //     let file_id_generator = Arc::new(FileIdGenerator::new());
    //     {
    //         let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
    //         let kvs = vec![
    //             TestingKV::new("k1", "value1"),
    //             TestingKV::new("k2", "value2"),
    //         ];
    //         rows.append(&mut write_kvs_to_db(&db, kvs));
    //     }
    //     {
    //         let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
    //         let kvs = vec![
    //             TestingKV::new("k3", "hello world"),
    //             TestingKV::new("k1", "value4"),
    //         ];
    //         rows.append(&mut write_kvs_to_db(&db, kvs));
    //     }

    //     let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
    //     assert_eq!(1, file_id_generator.get_file_id());
    //     assert_eq!(0, db.stable_storages.len());
    //     assert_rows_value(&db, &rows);
    //     assert_database_rows(&db, &rows);
    // }

    // #[test]
    // fn test_wrap_file() {
    //     let file_id_generator = Arc::new(FileIdGenerator::new());
    //     let dir = get_temporary_directory_path();
    //     let db = Database::open(
    //         &dir,
    //         file_id_generator,
    //         DataBaseOptions {
    //             storage_options: DataStorageOptions { max_file_size: 100 },
    //         },
    //     )
    //     .unwrap();
    //     let kvs = vec![
    //         TestingKV::new("k1", "value1_value1_value1"),
    //         TestingKV::new("k2", "value2_value2_value2"),
    //         TestingKV::new("k3", "value3_value3_value3"),
    //         TestingKV::new("k1", "value4_value4_value4"),
    //     ];
    //     assert_eq!(0, db.stable_storages.len());
    //     let rows = write_kvs_to_db(&db, kvs);
    //     assert_rows_value(&db, &rows);
    //     assert_eq!(1, db.stable_storages.len());
    //     assert_database_rows(&db, &rows);
    // }
}
