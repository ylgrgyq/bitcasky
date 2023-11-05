mod v1;
use std::{
    fs::File,
    io::{self, Read, Write},
    ops::Deref,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::storage_id::StorageId;

pub use self::v1::FormatterV1;

use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

#[derive(Debug)]
pub struct RowMeta {
    pub timestamp: u64,
    pub key_size: u64,
    pub value_size: u64,
}

pub struct RowHeader {
    pub crc: u32,
    pub meta: RowMeta,
}

#[derive(Debug)]
pub struct RowToWrite<'a, V: Deref<Target = [u8]>> {
    pub meta: RowMeta,
    pub key: &'a Vec<u8>,
    pub value: V,
}

impl<'a, V: Deref<Target = [u8]>> RowToWrite<'a, V> {
    pub fn new(key: &'a Vec<u8>, value: V) -> RowToWrite<'a, V> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        RowToWrite::new_with_timestamp(key, value, now)
    }

    pub fn new_with_timestamp(key: &'a Vec<u8>, value: V, timestamp: u64) -> RowToWrite<'a, V> {
        let key_size = key.len() as u64;
        let value_size = value.len() as u64;
        RowToWrite {
            meta: RowMeta {
                timestamp,
                key_size,
                value_size,
            },
            key,
            value,
        }
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct MergeMeta {
    pub known_max_storage_id: StorageId,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RowHintHeader {
    pub timestamp: u64,
    pub key_size: u64,
    pub row_offset: u64,
}

#[derive(Error, Debug)]
#[error("{}")]
pub enum FormatterError {
    #[error("Crc check failed. expect crc is: {expected_crc}, actual crc is: {actual_crc}")]
    CrcCheckFailed { expected_crc: u32, actual_crc: u32 },
    #[error("Got IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Invalid data file: {1}")]
    InvalidDataFile(#[source] io::Error, String),
    #[error("Magic string does not match")]
    MagicNotMatch(),
    #[error("Unknown formatter version: {0}")]
    UnknownFormatterVersion(u8),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct HintRow {
    pub timestamp: u64,
    pub key_size: u64,
    pub row_offset: u64,
    pub key: Vec<u8>,
}

pub type Result<T> = std::result::Result<T, FormatterError>;

pub trait Formatter: std::marker::Send + 'static + Copy {
    fn row_header_size(&self) -> usize;

    fn row_size<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'_, V>) -> usize;

    fn encode_row<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'_, V>) -> Bytes;

    fn decode_row_header(&self, bs: Bytes) -> Result<RowHeader>;

    fn validate_key_value(&self, header: &RowHeader, kv: &Bytes) -> Result<()>;

    fn encode_row_hint(&self, hint: &HintRow) -> Bytes;

    fn row_hint_header_size(&self) -> usize;

    fn decode_row_hint_header(&self, header_bs: Bytes) -> RowHintHeader;

    fn merge_meta_size(&self) -> usize;

    fn encode_merge_meta(&self, meta: MergeMeta) -> Bytes;

    fn decode_merge_meta(&self, meta: Bytes) -> MergeMeta;
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DataStorageFormatter {
    V1(FormatterV1),
}

impl Formatter for DataStorageFormatter {
    fn row_header_size(&self) -> usize {
        match self {
            DataStorageFormatter::V1(f) => f.row_header_size(),
        }
    }

    fn row_size<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'_, V>) -> usize {
        match self {
            DataStorageFormatter::V1(f) => f.row_size(row),
        }
    }

    fn encode_row<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'_, V>) -> Bytes {
        match self {
            DataStorageFormatter::V1(f) => f.encode_row(row),
        }
    }

    fn decode_row_header(&self, bs: Bytes) -> Result<RowHeader> {
        match self {
            DataStorageFormatter::V1(f) => f.decode_row_header(bs),
        }
    }

    fn validate_key_value(&self, header: &RowHeader, kv: &Bytes) -> Result<()> {
        match self {
            DataStorageFormatter::V1(f) => f.validate_key_value(header, kv),
        }
    }

    fn row_hint_header_size(&self) -> usize {
        match self {
            DataStorageFormatter::V1(f) => f.row_hint_header_size(),
        }
    }

    fn encode_row_hint(&self, hint: &HintRow) -> Bytes {
        match self {
            DataStorageFormatter::V1(f) => f.encode_row_hint(hint),
        }
    }

    fn decode_row_hint_header(&self, header_bs: Bytes) -> RowHintHeader {
        match self {
            DataStorageFormatter::V1(f) => f.decode_row_hint_header(header_bs),
        }
    }

    fn merge_meta_size(&self) -> usize {
        match self {
            DataStorageFormatter::V1(f) => f.merge_meta_size(),
        }
    }

    fn encode_merge_meta(&self, meta: MergeMeta) -> Bytes {
        match self {
            DataStorageFormatter::V1(f) => f.encode_merge_meta(meta),
        }
    }

    fn decode_merge_meta(&self, meta: Bytes) -> MergeMeta {
        match self {
            DataStorageFormatter::V1(f) => f.decode_merge_meta(meta),
        }
    }
}

const MAGIC: &[u8; 3] = b"btk";
const DEFAULT_FORMATTER_VERSION: u8 = 0;
pub const FILE_HEADER_SIZE: usize = 4;

pub fn initialize_new_file(file: &mut File) -> std::io::Result<DataStorageFormatter> {
    let mut bs = BytesMut::with_capacity(MAGIC.len() + 1);

    bs.extend_from_slice(MAGIC);
    bs.put_u8(DEFAULT_FORMATTER_VERSION);

    file.write_all(&bs.freeze())?;
    file.flush()?;

    Ok(DataStorageFormatter::V1(FormatterV1::new()))
}

pub fn get_formatter_from_data_file(file: &mut File) -> Result<DataStorageFormatter> {
    let mut file_header = vec![0; MAGIC.len() + 1];

    file.read_exact(&mut file_header)
        .map_err(|e| FormatterError::InvalidDataFile(e, "read file header failed".into()))?;

    if MAGIC != &file_header[0..3] {
        return Err(FormatterError::MagicNotMatch());
    }

    let formatter_version = file_header[3];
    if formatter_version == DEFAULT_FORMATTER_VERSION {
        return Ok(DataStorageFormatter::V1(FormatterV1::new()));
    }

    Err(FormatterError::UnknownFormatterVersion(formatter_version))
}

#[cfg(test)]
mod tests {
    use crate::fs::{create_file, open_file, FileType};

    use super::*;

    use bitcask_tests::common::get_temporary_directory_path;
    use test_log::test;

    #[test]
    fn test_formatter_v1_file() {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let mut file = create_file(&dir, FileType::DataFile, Some(storage_id)).unwrap();
        let init_formatter = initialize_new_file(&mut file).unwrap();

        let mut file = open_file(&dir, FileType::DataFile, Some(storage_id))
            .unwrap()
            .file;

        let read_formatter = get_formatter_from_data_file(&mut file).unwrap();
        assert_matches!(read_formatter, DataStorageFormatter::V1(_));
        assert_eq!(init_formatter, read_formatter);
    }
}
