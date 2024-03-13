pub mod mmap_data_storage;

use log::{debug, error};
use std::{
    fs::{File, Metadata},
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;

use crate::common::create_file;
use crate::options::{BitcaskyOptions, SyncStrategy};
use crate::{
    formatter::{
        self, get_formatter_from_file, BitcaskyFormatter, FormatterError, RowToWrite,
        FILE_HEADER_SIZE,
    },
    fs::{self, FileType},
    storage_id::StorageId,
};

use self::mmap_data_storage::MmapDataStorage;

use super::{common::RowToRead, RowLocation, TimedValue};

#[derive(Error, Debug)]
#[error("{}")]
pub enum DataStorageError {
    #[error("Write data file with id: {0} failed. error: {1}")]
    WriteRowFailed(StorageId, String),
    #[error("Read data file with id: {0} failed. error: {1}")]
    ReadRowFailed(StorageId, String),
    #[error("Flush writing storage with id: {0} failed. error: {1}")]
    FlushStorageFailed(StorageId, String),
    #[error("Rewind storage with id: {0} failed. error: {1}")]
    RewindFailed(StorageId, String),
    #[error("Storage with id: {0} overflow, need replace with a new one")]
    StorageOverflow(StorageId),
    #[error("No permission to write storage with id: {0}")]
    PermissionDenied(StorageId),
    #[error("Got IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Got IO Error: {0}")]
    DataStorageFormatter(#[from] FormatterError),
    #[error("Failed to read file header for storage with id: {1}")]
    ReadFileHeaderError(#[source] FormatterError, StorageId),
    #[error("Read end of file")]
    EofError(),
}

pub type Result<T> = std::result::Result<T, DataStorageError>;

pub trait DataStorageWriter {
    fn write_row<K: AsRef<[u8]>, V: Deref<Target = [u8]>>(
        &mut self,
        row: &RowToWrite<K, V>,
    ) -> Result<RowLocation>;

    fn rewind(&mut self) -> Result<()>;

    fn flush(&mut self) -> Result<()>;
}

pub trait DataStorageReader {
    /// Read value from this storage at row_offset
    fn read_value(&mut self, row_offset: usize) -> Result<Option<TimedValue<Vec<u8>>>>;

    /// Read next value from this storage
    fn read_next_row(&mut self) -> Result<Option<RowToRead>>;

    fn seek_to_end(&mut self) -> Result<()>;

    fn offset(&self) -> usize;
}

#[derive(Debug)]
enum DataStorageImpl {
    MmapStorage(MmapDataStorage),
}

#[derive(Debug, Default, Clone)]
pub struct DataStorageTelemetry {
    pub storage_id: StorageId,
    pub formatter_version: u8,
    pub data_capacity: usize,
    pub data_size: usize,
    pub usage: f64,
    pub fragment: f64,
    pub read_value_times: u64,
    pub write_times: u64,
    pub dead_bytes: usize,
}

#[derive(Debug)]
pub struct DataStorage {
    database_dir: PathBuf,
    storage_id: StorageId,
    storage_impl: DataStorageImpl,
    options: Arc<BitcaskyOptions>,
    formatter: Arc<BitcaskyFormatter>,
    dirty: bool,
    dead_bytes: usize,
}

impl DataStorage {
    pub fn new<P: AsRef<Path>>(
        database_dir: P,
        storage_id: StorageId,
        formatter: Arc<BitcaskyFormatter>,
        options: Arc<BitcaskyOptions>,
    ) -> Result<Self> {
        let mut is_o_sync = false;
        #[cfg(unix)]
        if let SyncStrategy::OSync = options.database.sync_strategy {
            is_o_sync = true;
        }
        let path = database_dir.as_ref().to_path_buf();
        let data_file = create_file(
            &path,
            FileType::DataFile,
            Some(storage_id),
            &formatter,
            is_o_sync,
            options.database.storage.init_data_file_capacity,
        )?;

        debug!(
            "Create storage under path: {:?} with storage id: {}",
            &path, storage_id
        );
        let meta = data_file.metadata()?;
        DataStorage::open_by_file(
            &path,
            storage_id,
            data_file,
            meta,
            FILE_HEADER_SIZE,
            formatter,
            options,
        )
    }

    pub fn open<P: AsRef<Path>>(
        database_dir: P,
        storage_id: StorageId,
        options: Arc<BitcaskyOptions>,
    ) -> Result<Self> {
        let path = database_dir.as_ref().to_path_buf();
        let mut data_file = fs::open_file(&path, FileType::DataFile, Some(storage_id))?;
        debug!(
            "Open storage under path: {:?} with storage id: {}",
            &path, storage_id
        );
        let meta = data_file.file.metadata()?;
        let formatter = Arc::new(get_formatter_from_file(&mut data_file.file)?);

        DataStorage::open_by_file(
            &path,
            storage_id,
            data_file.file,
            meta,
            FILE_HEADER_SIZE,
            formatter,
            options,
        )
    }

    pub fn storage_id(&self) -> StorageId {
        self.storage_id
    }

    pub fn is_dirty(&mut self) -> bool {
        self.dirty
    }

    pub fn add_dead_bytes(&mut self, dead_bytes: usize) {
        self.dead_bytes += dead_bytes;
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
        let formatter = Arc::new(
            formatter::get_formatter_from_file(&mut data_file.file)
                .map_err(|e| DataStorageError::ReadFileHeaderError(e, self.storage_id))?,
        );
        let meta = data_file.file.metadata()?;
        Ok(StorageIter {
            storage: DataStorage::open_by_file(
                &self.database_dir,
                self.storage_id,
                data_file.file,
                meta,
                FILE_HEADER_SIZE,
                formatter,
                self.options.clone(),
            )?,
        })
    }

    pub fn get_telemetry_data(&self) -> DataStorageTelemetry {
        match &self.storage_impl {
            DataStorageImpl::MmapStorage(s) => {
                let data_size = s.offset - FILE_HEADER_SIZE;
                let data_capacity = s.capacity - FILE_HEADER_SIZE;
                let mut fragment = self.dead_bytes as f64 / data_size as f64;
                if fragment.is_nan() {
                    fragment = 0.0;
                }
                DataStorageTelemetry {
                    storage_id: self.storage_id,
                    formatter_version: self.formatter.version(),
                    data_capacity,
                    data_size,
                    usage: data_size as f64 / data_capacity as f64,
                    fragment,
                    read_value_times: s.read_value_times,
                    write_times: s.write_times,
                    dead_bytes: self.dead_bytes,
                }
            }
        }
    }

    fn open_by_file(
        database_dir: &Path,
        storage_id: StorageId,
        data_file: File,
        meta: Metadata,
        write_offset: usize,
        formatter: Arc<BitcaskyFormatter>,
        options: Arc<BitcaskyOptions>,
    ) -> Result<Self> {
        let capacity = meta.len() as usize;
        let storage_impl = DataStorageImpl::MmapStorage(MmapDataStorage::new(
            storage_id,
            data_file,
            write_offset,
            capacity,
            formatter.clone(),
            options.clone(),
        )?);
        Ok(DataStorage {
            storage_impl,
            storage_id,
            database_dir: database_dir.to_path_buf(),
            options,
            formatter,
            dirty: false,
            dead_bytes: 0,
        })
    }
}

impl DataStorageWriter for DataStorage {
    fn write_row<K: AsRef<[u8]>, V: Deref<Target = [u8]>>(
        &mut self,
        row: &RowToWrite<K, V>,
    ) -> Result<RowLocation> {
        let r = match &mut self.storage_impl {
            DataStorageImpl::MmapStorage(s) => s.write_row(row),
        }?;
        self.dirty = true;
        Ok(r)
    }

    fn rewind(&mut self) -> Result<()> {
        match &mut self.storage_impl {
            DataStorageImpl::MmapStorage(s) => {
                let storage_id = self.storage_id;
                s.rewind()
                    .map_err(|e| DataStorageError::RewindFailed(storage_id, e.to_string()))
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        match &mut self.storage_impl {
            DataStorageImpl::MmapStorage(s) => s
                .flush()
                .map_err(|e| DataStorageError::FlushStorageFailed(self.storage_id, e.to_string())),
        }
    }
}

impl DataStorageReader for DataStorage {
    fn read_value(&mut self, row_offset: usize) -> Result<Option<TimedValue<Vec<u8>>>> {
        match &mut self.storage_impl {
            DataStorageImpl::MmapStorage(s) => s
                .read_value(row_offset)
                .map_err(|e| DataStorageError::ReadRowFailed(self.storage_id, e.to_string())),
        }
    }

    fn read_next_row(&mut self) -> Result<Option<RowToRead>> {
        match &mut self.storage_impl {
            DataStorageImpl::MmapStorage(s) => s.read_next_row(),
        }
    }

    fn seek_to_end(&mut self) -> Result<()> {
        match &mut self.storage_impl {
            DataStorageImpl::MmapStorage(s) => s.seek_to_end(),
        }
    }

    fn offset(&self) -> usize {
        match &self.storage_impl {
            DataStorageImpl::MmapStorage(s) => s.offset(),
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
