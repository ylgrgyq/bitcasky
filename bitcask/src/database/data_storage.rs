use bytes::{Buf, Bytes};
use crc::{Crc, CRC_32_CKSUM};
use log::{debug, error};
use std::{
    fs::File,
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
    #[error("Got IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Crc check failed on reading value with file id: {0}, offset: {1}. expect crc is: {2}, actual crc is: {3}")]
    CrcCheckFailed(u32, u64, u32, u32),
}

pub type Result<T> = std::result::Result<T, DataStorageError>;

pub trait DataStorageWriter {
    fn write_row<V: Deref<Target = [u8]>>(&mut self, row: RowToWrite<V>) -> Result<RowLocation>;

    fn transit_to_readonly(self) -> Result<DataStorage>;

    fn flush(&mut self) -> Result<()>;
}

pub trait DataStorageReader {
    fn read_value(&mut self, row_offset: u64, row_size: u64) -> Result<TimedValue<Value>>;

    fn read_next_row(&mut self) -> Result<Option<RowToRead>>;
}

#[derive(Debug)]
enum DataStorageImpl {
    FileStorage(FileDataStorage),
}

#[derive(Debug)]
pub struct DataStorage {
    database_dir: PathBuf,
    file_id: FileId,
    storage_impl: DataStorageImpl,
    file_size: u64,
    readonly: bool,
}

impl DataStorage {
    pub fn new<P: AsRef<Path>>(database_dir: P, file_id: FileId) -> Result<Self> {
        let path = database_dir.as_ref().to_path_buf();
        let data_file = create_file(&path, FileType::DataFile, Some(file_id))?;
        debug!(
            "Create storage under path: {:?} with file id: {}",
            &path, file_id
        );
        Ok(DataStorage {
            storage_impl: DataStorageImpl::FileStorage(FileDataStorage::new(
                &path, file_id, data_file, 0,
            )?),
            file_id,
            database_dir: path,
            file_size: 0,
            readonly: false,
        })
    }

    pub fn open<P: AsRef<Path>>(database_dir: P, file_id: FileId) -> Result<Self> {
        let path = database_dir.as_ref().to_path_buf();
        let data_file = fs::open_file(&path, FileType::DataFile, Some(file_id))?;
        debug!(
            "Open storage under path: {:?} with file id: {}",
            &path, file_id
        );
        let meta = data_file.file.metadata()?;
        let file_size = meta.len();
        Ok(DataStorage {
            storage_impl: DataStorageImpl::FileStorage(FileDataStorage::new(
                &path,
                file_id,
                data_file.file,
                file_size,
            )?),
            file_id,
            database_dir: path,
            file_size,
            readonly: meta.permissions().readonly(),
        })
    }

    pub fn file_id(&self) -> FileId {
        self.file_id
    }

    pub fn file_size(&self) -> usize {
        self.file_size as usize
    }

    pub fn is_empty(&self) -> bool {
        self.file_size() == 0
    }

    pub fn is_readonly(&self) -> Result<bool> {
        Ok(self.readonly)
    }

    pub fn iter(&self) -> Result<StorageIter> {
        Ok(StorageIter {
            storage: DataStorage::open(&self.database_dir, self.file_id)?,
        })
    }
}

impl DataStorageWriter for DataStorage {
    fn write_row<V: Deref<Target = [u8]>>(&mut self, row: RowToWrite<V>) -> Result<RowLocation> {
        let r = match &mut self.storage_impl {
            DataStorageImpl::FileStorage(s) => s
                .write_row(row)
                .map_err(|e| DataStorageError::WriteRowFailed(s.file_id, e.to_string())),
        }?;
        self.file_size += r.row_size;
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
}

impl FileDataStorage {
    pub fn new<P: AsRef<Path>>(
        database_dir: P,
        file_id: FileId,
        data_file: File,
        capacity: u64,
    ) -> Result<Self> {
        Ok(FileDataStorage {
            database_dir: database_dir.as_ref().to_path_buf(),
            data_file,
            file_id,
            capacity,
        })
    }
}

impl DataStorageWriter for FileDataStorage {
    fn write_row<V: Deref<Target = [u8]>>(&mut self, row: RowToWrite<V>) -> Result<RowLocation> {
        let value_offset = self.data_file.seek(SeekFrom::End(0))?;
        let data_to_write = row.to_bytes();
        self.data_file.write_all(&data_to_write)?;

        Ok(RowLocation {
            file_id: self.file_id,
            row_offset: value_offset,
            row_size: row.size,
        })
    }

    fn transit_to_readonly(mut self) -> Result<DataStorage> {
        self.data_file.flush()?;
        let mut perms = self.data_file.metadata()?.permissions();
        perms.set_readonly(true);
        self.data_file.set_permissions(perms)?;
        let file_size = self.data_file.metadata()?.len();
        Ok(DataStorage {
            storage_impl: DataStorageImpl::FileStorage(FileDataStorage::new(
                &self.database_dir,
                self.file_id,
                self.data_file,
                file_size,
            )?),
            file_id: self.file_id,
            database_dir: self.database_dir,
            file_size,
            readonly: true,
        })
    }

    fn flush(&mut self) -> Result<()> {
        Ok(self.data_file.flush()?)
    }
}

impl DataStorageReader for FileDataStorage {
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
