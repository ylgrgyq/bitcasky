mod file_data_storage;

pub use self::file_data_storage::FileDataStorage;

use log::{debug, error};
use std::{
    fs::{File, Metadata},
    io::{Seek, SeekFrom},
    ops::Deref,
    path::{Path, PathBuf},
};
use thiserror::Error;

use crate::{
    formatter::{
        self, get_formatter_from_file, BitcaskFormatter, Formatter, FormatterError, RowToWrite,
    },
    fs::{self, create_file, FileType},
    storage_id::StorageId,
};

use super::{
    common::{RowToRead, Value},
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
    #[error("Got IO Error: {0}")]
    DataStorageFormatter(#[from] FormatterError),
    #[error("Crc check failed on reading value with file id: {0}, offset: {1}. expect crc is: {2}, actual crc is: {3}")]
    CrcCheckFailed(u32, u64, u32, u32),
    #[error("Failed to write file header for storage with id: {1}")]
    WriteFileHeaderError(#[source] FormatterError, StorageId),
    #[error("Failed to read file header for storage with id: {1}")]
    ReadFileHeaderError(#[source] FormatterError, StorageId),
}

pub type Result<T> = std::result::Result<T, DataStorageError>;

pub trait DataStorageWriter {
    fn write_row<V: Deref<Target = [u8]>>(&mut self, row: &RowToWrite<V>) -> Result<RowLocation>;

    fn transit_to_readonly(self) -> Result<DataStorage>;

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
    storage_id: StorageId,
    storage_impl: DataStorageImpl,
    readonly: bool,
    formatter: BitcaskFormatter,
    options: DataStorageOptions,
}

impl DataStorage {
    pub fn new<P: AsRef<Path>>(
        database_dir: P,
        storage_id: StorageId,
        options: DataStorageOptions,
    ) -> Result<Self> {
        let path = database_dir.as_ref().to_path_buf();
        let mut data_file = create_file(&path, FileType::DataFile, Some(storage_id))?;
        let formatter = formatter::initialize_new_file(&mut data_file)?;
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
        options: DataStorageOptions,
    ) -> Result<Self> {
        let path = database_dir.as_ref().to_path_buf();
        let mut data_file = fs::open_file(&path, FileType::DataFile, Some(storage_id))?;
        debug!(
            "Open storage under path: {:?} with storage id: {}",
            &path, storage_id
        );
        let meta = data_file.file.metadata()?;
        let formatter = get_formatter_from_file(&mut data_file.file)?;
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

    pub fn iter(&self) -> Result<StorageIter> {
        let mut data_file = fs::open_file(
            &self.database_dir,
            FileType::DataFile,
            Some(self.storage_id),
        )?;
        debug!(
            "Create iterator under path: {:?} with storage id: {}",
            &self.database_dir, self.storage_id
        );
        let formatter = formatter::get_formatter_from_file(&mut data_file.file)
            .map_err(|e| DataStorageError::ReadFileHeaderError(e, self.storage_id))?;
        let meta = data_file.file.metadata()?;
        Ok(StorageIter {
            storage: DataStorage::open_by_file(
                &self.database_dir,
                self.storage_id,
                data_file.file,
                meta,
                formatter,
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
        formatter: BitcaskFormatter,
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

impl DataStorageWriter for DataStorage {
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

    fn transit_to_readonly(self) -> Result<DataStorage> {
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

impl DataStorageReader for DataStorage {
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
                self.storage.storage_id(), &e);
                None
            }
        }
    }
}
