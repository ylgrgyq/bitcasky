use bytes::{Buf, Bytes};
use crc::{Crc, CRC_32_CKSUM};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    ops::Deref,
    path::Path,
};
use thiserror::Error;

use crate::{
    error::BitcaskResult,
    file_id::FileId,
    fs::{create_file, FileType},
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
    #[error("Index path type should be a non-negative integer number, but is: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Crc check failed on reading value with file id: {0}, offset: {1}. expect crc is: {2}, actual crc is: {3}")]
    CrcCheckFailed(u32, u64, u32, u32),
}

pub type Result<T> = std::result::Result<T, StorageError>;

pub struct WriteRowResult {
    pub file_id: FileId,
    pub row_offset: u64,
    pub row_size: u64,
}

pub trait RowStorage {
    fn get_file_id(&self) -> FileId;

    fn file_size(&self) -> usize;

    fn read_value(&mut self, row_offset: u64, row_size: u64) -> Result<TimedValue<Value>>;

    fn write_row<V: Deref<Target = [u8]>>(&mut self, row: RowToWrite<V>) -> Result<RowLocation>;

    fn read_next_row(&mut self) -> Result<Option<RowToRead>>;

    fn transit_to_readonly(&mut self) -> Result<()>;

    fn flush(&mut self) -> Result<()>;
}

#[derive(Debug)]
pub struct FileStorage {
    data_file: File,
    file_size: u64,
    pub file_id: FileId,
}

impl FileStorage {
    pub fn new(database_dir: &Path, file_id: FileId) -> Result<Self> {
        let data_file = create_file(database_dir, FileType::DataFile, Some(file_id))?;
        let meta = data_file.metadata()?;
        Ok(FileStorage {
            data_file,
            file_id,
            file_size: meta.len(),
        })
    }
}

impl RowStorage for FileStorage {
    fn get_file_id(&self) -> FileId {
        self.file_id
    }

    fn file_size(&self) -> usize {
        self.file_size as usize
    }

    fn write_row<V: Deref<Target = [u8]>>(&mut self, row: RowToWrite<V>) -> Result<RowLocation> {
        let value_offset = self.data_file.seek(SeekFrom::End(0))?;
        let data_to_write = row.to_bytes();
        self.data_file.write_all(&data_to_write)?;

        self.file_size += row.size;

        Ok(RowLocation {
            file_id: self.file_id,
            row_offset: value_offset,
            row_size: row.size,
        })
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
        if value_offset >= self.file_size {
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

    fn transit_to_readonly(&mut self) -> Result<()> {
        self.data_file.flush()?;
        let file_id = self.file_id;
        let mut perms = self.data_file.metadata()?.permissions();
        perms.set_readonly(true);
        self.data_file.set_permissions(perms)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(self.data_file.flush()?)
    }
}
