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
pub enum StorageError {
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

pub type Result<T> = std::result::Result<T, StorageError>;

pub trait StorageWriter {
    fn write_row<V: Deref<Target = [u8]>>(&mut self, row: RowToWrite<V>) -> Result<RowLocation>;

    fn transit_to_readonly(self) -> Result<Storage>;

    fn flush(&mut self) -> Result<()>;
}

pub trait StorageReader {
    fn read_value(&mut self, row_offset: u64, row_size: u64) -> Result<TimedValue<Value>>;

    fn read_next_row(&mut self) -> Result<Option<RowToRead>>;
}

#[derive(Debug)]
pub enum StorageImpl {
    FileStorage(FileStorage),
}

#[derive(Debug)]
pub struct Storage {
    database_dir: PathBuf,
    file_id: FileId,
    storage_impl: StorageImpl,
    file_size: u64,
}

impl Storage {
    pub fn new<P: AsRef<Path>>(database_dir: P, file_id: FileId) -> Result<Self> {
        let data_file = create_file(database_dir.as_ref(), FileType::DataFile, Some(file_id))?;
        debug!(
            "Create row storage under path: {:?} with file id: {}",
            database_dir.as_ref(),
            file_id
        );
        Ok(Storage {
            storage_impl: StorageImpl::FileStorage(FileStorage::new(
                database_dir.as_ref(),
                file_id,
                data_file,
            )?),
            file_id,
            database_dir: database_dir.as_ref().to_path_buf(),
            file_size: 0,
        })
    }

    pub fn open<P: AsRef<Path>>(database_dir: P, file_id: FileId) -> Result<Self> {
        let data_file = fs::open_file(database_dir.as_ref(), FileType::DataFile, Some(file_id))?;
        let meta = data_file.file.metadata()?;

        Ok(Storage {
            storage_impl: StorageImpl::FileStorage(FileStorage::new(
                database_dir.as_ref(),
                file_id,
                data_file.file,
            )?),
            file_id,
            database_dir: database_dir.as_ref().to_path_buf(),
            file_size: meta.len(),
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

    pub fn iter(&self) -> Result<StorageIter> {
        Ok(StorageIter {
            storage: Storage::open(&self.database_dir, self.file_id)?,
        })
    }
}

impl StorageWriter for Storage {
    fn write_row<V: Deref<Target = [u8]>>(&mut self, row: RowToWrite<V>) -> Result<RowLocation> {
        let r = match &mut self.storage_impl {
            StorageImpl::FileStorage(s) => s
                .write_row(row)
                .map_err(|e| StorageError::WriteRowFailed(s.file_id, e.to_string())),
        }?;
        self.file_size += r.row_size;
        Ok(r)
    }

    fn transit_to_readonly(self) -> Result<Storage> {
        match self.storage_impl {
            StorageImpl::FileStorage(s) => {
                let file_id = s.file_id;
                s.transit_to_readonly()
                    .map_err(|e| StorageError::TransitToReadOnlyFailed(file_id, e.to_string()))
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        match &mut self.storage_impl {
            StorageImpl::FileStorage(s) => s
                .flush()
                .map_err(|e| StorageError::FlushStorageFailed(s.file_id, e.to_string())),
        }
    }
}

impl StorageReader for Storage {
    fn read_value(&mut self, row_offset: u64, row_size: u64) -> Result<TimedValue<Value>> {
        match &mut self.storage_impl {
            StorageImpl::FileStorage(s) => s
                .read_value(row_offset, row_size)
                .map_err(|e| StorageError::ReadRowFailed(s.file_id, e.to_string())),
        }
    }

    fn read_next_row(&mut self) -> Result<Option<RowToRead>> {
        match &mut self.storage_impl {
            StorageImpl::FileStorage(s) => s.read_next_row(),
        }
    }
}

#[derive(Debug)]
pub struct StorageIter {
    storage: Storage,
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
pub struct FileStorage {
    database_dir: PathBuf,
    data_file: File,
    pub file_id: FileId,
    capacity: u64,
}

impl FileStorage {
    pub fn new<P: AsRef<Path>>(database_dir: P, file_id: FileId, data_file: File) -> Result<Self> {
        let meta = data_file.metadata()?;

        Ok(FileStorage {
            database_dir: database_dir.as_ref().to_path_buf(),
            data_file,
            file_id,
            capacity: meta.len(),
        })
    }
}

impl StorageWriter for FileStorage {
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

    fn transit_to_readonly(mut self) -> Result<Storage> {
        self.data_file.flush()?;
        let mut perms = self.data_file.metadata()?.permissions();
        perms.set_readonly(true);
        self.data_file.set_permissions(perms)?;
        let file_size = self.data_file.metadata()?.len();
        Ok(Storage {
            storage_impl: StorageImpl::FileStorage(FileStorage::new(
                &self.database_dir,
                self.file_id,
                self.data_file,
            )?),
            file_id: self.file_id,
            database_dir: self.database_dir,
            file_size,
        })
    }

    fn flush(&mut self) -> Result<()> {
        Ok(self.data_file.flush()?)
    }
}

impl StorageReader for FileStorage {
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
            return Err(StorageError::CrcCheckFailed(
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
            return Err(StorageError::CrcCheckFailed(
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
