mod file_storage;

pub use self::file_storage::FileDataStorage;

use log::{debug, error};
use std::{
    fs::{File, Metadata},
    io::{Seek, SeekFrom},
    ops::Deref,
    path::{Path, PathBuf},
};
use thiserror::Error;

use crate::{
    fs::{self, create_file, FileType},
    storage_id::StorageId,
};

use super::{
    common::{RowToRead, RowToWrite, Value},
    formatter::Formatter,
    formatter::FormatterError,
    RowLocation, TimedValue,
};

#[derive(Error, Debug)]
#[error("{}")]
pub enum DataStorageError {
    #[error("Write data file with id: {0} failed. error: {1}")]
    WriteRowFailed(StorageId, String),
    #[error("Read data file with id: {0} failed. error: {1}")]
    ReadRowFailed(StorageId, String),
    #[error("Flush writing storage with id: {0} failed. error: {1}")]
    FlushStorageFailed(StorageId, String),
    #[error("Transit writing storage with id: {0} to readonly failed. error: {1}")]
    TransitToReadOnlyFailed(StorageId, String),
    #[error("Storage with id: {0} overflow, need replace with a new one")]
    StorageOverflow(StorageId),
    #[error("No permission to write storage with id: {0}")]
    PermissionDenied(StorageId),
    #[error("Got IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Crc check failed on reading value with file id: {0}, offset: {1}. expect crc is: {2}, actual crc is: {3}")]
    CrcCheckFailed(u32, u64, u32, u32),
    #[error("Got formatter Error: {0}")]
    FormatterError(#[from] FormatterError),
}

pub type Result<T> = std::result::Result<T, DataStorageError>;

pub trait DataStorageWriter<F: Formatter> {
    fn write_row<V: Deref<Target = [u8]>>(&mut self, row: &RowToWrite<V>) -> Result<RowLocation>;

    fn transit_to_readonly(self) -> Result<DataStorage<F>>;

    fn flush(&mut self) -> Result<()>;
}

pub trait DataStorageReader {
    /// Total size in bytes of this storage
    fn storage_size(&self) -> usize;

    /// Read value from this storage at row_offset
    fn read_value(&mut self, row_offset: u64) -> Result<TimedValue<Value>>;

    fn read_next_row(&mut self) -> Result<Option<RowToRead>>;
}
#[derive(Debug)]
enum DataStorageImpl<F: Formatter> {
    FileStorage(FileDataStorage<F>),
}

#[derive(Debug, Clone, Copy)]
pub struct DataStorageOptions {
    pub max_file_size: u64,
}

#[derive(Debug)]
pub struct DataStorage<F: Formatter> {
    database_dir: PathBuf,
    storage_id: StorageId,
    storage_impl: DataStorageImpl<F>,
    readonly: bool,
    formatter: F,
    options: DataStorageOptions,
}

impl<F: Formatter> DataStorage<F> {
    pub fn new<P: AsRef<Path>>(
        database_dir: P,
        storage_id: StorageId,
        formatter: F,
        options: DataStorageOptions,
    ) -> Result<Self> {
        let path = database_dir.as_ref().to_path_buf();
        let data_file = create_file(&path, FileType::DataFile, Some(storage_id))?;
        debug!(
            "Create storage under path: {:?} with storage id: {}",
            &path, storage_id
        );
        let meta = data_file.metadata()?;
        DataStorage::open_by_file(&path, storage_id, data_file, meta, formatter, options)
    }

    pub fn open<P: AsRef<Path>>(
        database_dir: P,
        storage_id: StorageId,
        formatter: F,
        options: DataStorageOptions,
    ) -> Result<Self> {
        let path = database_dir.as_ref().to_path_buf();
        let mut data_file = fs::open_file(&path, FileType::DataFile, Some(storage_id))?;
        debug!(
            "Open storage under path: {:?} with storage id: {}",
            &path, storage_id
        );
        let meta = data_file.file.metadata()?;
        if !meta.permissions().readonly() {
            data_file.file.seek(SeekFrom::End(0))?;
        }

        DataStorage::open_by_file(&path, storage_id, data_file.file, meta, formatter, options)
    }

    pub fn storage_id(&self) -> StorageId {
        self.storage_id
    }

    pub fn is_empty(&self) -> bool {
        self.storage_size() == 0
    }

    pub fn is_readonly(&self) -> Result<bool> {
        Ok(self.readonly)
    }

    pub fn iter(&self) -> Result<StorageIter<F>> {
        let data_file = fs::open_file(
            &self.database_dir,
            FileType::DataFile,
            Some(self.storage_id),
        )?;
        debug!(
            "Create iterator under path: {:?} with storage id: {}",
            &self.database_dir, self.storage_id
        );
        let meta = data_file.file.metadata()?;
        Ok(StorageIter {
            storage: DataStorage::open_by_file(
                &self.database_dir,
                self.storage_id,
                data_file.file,
                meta,
                self.formatter,
                self.options,
            )?,
        })
    }

    pub fn check_storage_overflow<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<V>) -> bool {
        let row_size = self.formatter.row_size(row);
        (row_size + self.storage_size()) as u64 > self.options.max_file_size
    }

    fn open_by_file(
        database_dir: &PathBuf,
        storage_id: StorageId,
        data_file: File,
        meta: Metadata,
        formatter: F,
        options: DataStorageOptions,
    ) -> Result<Self> {
        let file_size = meta.len();

        Ok(DataStorage {
            storage_impl: DataStorageImpl::FileStorage(FileDataStorage::new(
                database_dir,
                storage_id,
                data_file,
                file_size,
                formatter,
                options,
            )?),
            storage_id,
            database_dir: database_dir.clone(),
            readonly: meta.permissions().readonly(),
            formatter,
            options,
        })
    }
}

impl<F: Formatter> DataStorageWriter<F> for DataStorage<F> {
    fn write_row<V: Deref<Target = [u8]>>(&mut self, row: &RowToWrite<V>) -> Result<RowLocation> {
        if self.check_storage_overflow(row) {
            return Err(DataStorageError::StorageOverflow(self.storage_id));
        }
        if self.readonly {
            return Err(DataStorageError::PermissionDenied(self.storage_id));
        }
        let r = match &mut self.storage_impl {
            DataStorageImpl::FileStorage(s) => s
                .write_row(row)
                .map_err(|e| DataStorageError::WriteRowFailed(s.storage_id, e.to_string())),
        }?;
        Ok(r)
    }

    fn transit_to_readonly(self) -> Result<DataStorage<F>> {
        match self.storage_impl {
            DataStorageImpl::FileStorage(s) => {
                let storage_id = s.storage_id;
                s.transit_to_readonly().map_err(|e| {
                    DataStorageError::TransitToReadOnlyFailed(storage_id, e.to_string())
                })
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        match &mut self.storage_impl {
            DataStorageImpl::FileStorage(s) => s
                .flush()
                .map_err(|e| DataStorageError::FlushStorageFailed(s.storage_id, e.to_string())),
        }
    }
}

impl<F: Formatter> DataStorageReader for DataStorage<F> {
    fn storage_size(&self) -> usize {
        match &self.storage_impl {
            DataStorageImpl::FileStorage(s) => s.storage_size(),
        }
    }

    fn read_value(&mut self, row_offset: u64) -> Result<TimedValue<Value>> {
        match &mut self.storage_impl {
            DataStorageImpl::FileStorage(s) => s
                .read_value(row_offset)
                .map_err(|e| DataStorageError::ReadRowFailed(s.storage_id, e.to_string())),
        }
    }

    fn read_next_row(&mut self) -> Result<Option<RowToRead>> {
        match &mut self.storage_impl {
            DataStorageImpl::FileStorage(s) => s.read_next_row(),
        }
    }
}

#[derive(Debug)]
pub struct StorageIter<F: Formatter> {
    storage: DataStorage<F>,
}

impl<F: Formatter> Iterator for StorageIter<F> {
    type Item = Result<RowToRead>;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.storage.read_next_row();
        match ret {
            Ok(o) => o.map(Ok),
            Err(e) => {
                error!(target: "Storage", "Data file with file id {} was corrupted. Error: {}", 
                self.storage.storage_id(), &e);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::database::formatter::FormatterV1;

    use super::*;

    use bitcask_tests::common::get_temporary_directory_path;
    use test_log::test;

    fn get_file_storage(max_size: u64) -> DataStorage<FormatterV1> {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        create_file(&dir, FileType::DataFile, Some(storage_id)).unwrap();
        let options = DataStorageOptions {
            max_file_size: max_size,
        };
        DataStorage::open(&dir, 1, FormatterV1::new(), options).unwrap()
    }

    #[test]
    fn test_read_write_file_storage() {
        let mut storage = get_file_storage(1024);

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
        let row_location1 = storage.write_row(&row_to_write).unwrap();

        let k2: Vec<u8> = "key2".into();
        let v2: Vec<u8> = "value2".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k2, v2.clone());
        let row_location2 = storage.write_row(&row_to_write).unwrap();

        assert_eq!(v1, *storage.read_value(row_location1.row_offset).unwrap());
        assert_eq!(v2, *storage.read_value(row_location2.row_offset).unwrap());
    }

    #[test]
    fn test_write_overflow() {
        let mut storage = get_file_storage(2);

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
        storage.write_row(&row_to_write).expect_err("overflow");
    }

    #[test]
    fn test_write_file_size() {
        let mut storage = get_file_storage(100);

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
        storage.write_row(&row_to_write).unwrap();

        assert_eq!(
            FormatterV1::new().row_size(&row_to_write),
            storage.storage_size()
        );
    }

    #[test]
    fn test_file_storage_read_next_row() {
        let mut storage = get_file_storage(1024);

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
        storage.write_row(&row_to_write).unwrap();

        let k2: Vec<u8> = "key2".into();
        let v2: Vec<u8> = "value2".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k2, v2.clone());
        storage.write_row(&row_to_write).unwrap();

        let mut storage = storage.transit_to_readonly().unwrap();
        let r = storage.read_next_row().unwrap().unwrap();
        assert_eq!(k1, r.key);
        assert_eq!(v1, r.value);
        let r = storage.read_next_row().unwrap().unwrap();
        assert_eq!(k2, r.key);
        assert_eq!(v2, r.value);
    }

    #[test]
    fn test_transit_storage_to_read_only() {
        let mut storage = get_file_storage(1024);

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
        storage.write_row(&row_to_write).unwrap();
        let mut storage = storage.transit_to_readonly().unwrap();

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
        storage
            .write_row(&row_to_write)
            .expect_err("no write permission");
    }
}
