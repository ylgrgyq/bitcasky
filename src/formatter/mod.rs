use std::{
    fs::File,
    io::{self, Read, Write},
    ops::Deref,
};

use crate::storage_id::StorageId;

use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

mod formatter_v1;
pub use self::formatter_v1::FormatterV1;

const MAGIC: &[u8; 3] = b"btk";
const FORMATTER_V1_VERSION: u8 = 1;
pub const FILE_HEADER_SIZE: usize = 8;

#[derive(Debug, PartialEq, Eq)]
pub struct RowMeta {
    pub expire_timestamp: u64,
    pub key_size: usize,
    pub value_size: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RowHeader {
    pub crc: u32,
    pub meta: RowMeta,
}

#[derive(Debug)]
pub struct RowToWrite<K: AsRef<[u8]>, V: Deref<Target = [u8]>> {
    pub meta: RowMeta,
    pub key: K,
    pub value: V,
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct MergeMeta {
    pub known_max_storage_id: StorageId,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RowHintHeader {
    pub expire_timestamp: u64,
    pub key_size: usize,
    pub row_offset: usize,
    pub row_size: usize,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RowHint {
    pub header: RowHintHeader,
    pub key: Vec<u8>,
}

impl<K: AsRef<[u8]>, V: Deref<Target = [u8]>> RowToWrite<K, V> {
    pub fn new(key: K, value: V) -> RowToWrite<K, V> {
        RowToWrite::new_with_timestamp(key, value, 0)
    }

    pub fn new_with_timestamp(key: K, value: V, expire_timestamp: u64) -> RowToWrite<K, V> {
        let key_size = key.as_ref().len();
        let value_size = value.len();
        RowToWrite {
            meta: RowMeta {
                expire_timestamp,
                key_size,
                value_size,
            },
            key,
            value,
        }
    }
}

#[derive(Error, Debug)]
#[error("{}")]
pub enum FormatterError {
    #[error("Crc check failed. expect crc is: {expected_crc}, actual crc is: {actual_crc}")]
    CrcCheckFailed { expected_crc: u32, actual_crc: u32 },
    #[error("Got IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Read file header failed: {1}")]
    ReadFileHeaderFailed(#[source] io::Error, String),
    #[error("Magic string does not match")]
    MagicNotMatch(),
    #[error("Unknown formatter version: {0}")]
    UnknownFormatterVersion(u8),
}

pub type Result<T> = std::result::Result<T, FormatterError>;

pub trait Formatter: std::marker::Send + 'static + Copy {
    fn row_header_size(&self) -> usize;

    fn net_row_size<K: AsRef<[u8]>, V: Deref<Target = [u8]>>(
        &self,
        row: &RowToWrite<K, V>,
    ) -> usize;

    fn encode_row<K: AsRef<[u8]>, V: Deref<Target = [u8]>>(
        &self,
        row: &RowToWrite<K, V>,
        output: &mut [u8],
    ) -> usize;

    fn decode_row_header(&self, bs: &[u8]) -> RowHeader;

    fn validate_key_value(&self, header: &RowHeader, kv: &[u8]) -> Result<()>;

    fn encode_row_hint(&self, hint: &RowHint, output: &mut [u8]) -> usize;

    fn row_hint_header_size(&self) -> usize;

    fn decode_row_hint_header(&self, header_bs: &[u8]) -> RowHintHeader;

    fn merge_meta_size(&self) -> usize;

    fn encode_merge_meta(&self, meta: &MergeMeta) -> Bytes;

    fn decode_merge_meta(&self, meta: Bytes) -> MergeMeta;
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BitcaskyFormatter {
    V1(FormatterV1),
}

impl BitcaskyFormatter {
    pub fn version(&self) -> u8 {
        match self {
            BitcaskyFormatter::V1(_) => FORMATTER_V1_VERSION,
        }
    }
}

impl Formatter for BitcaskyFormatter {
    fn row_header_size(&self) -> usize {
        match self {
            BitcaskyFormatter::V1(f) => f.row_header_size(),
        }
    }

    fn net_row_size<K: AsRef<[u8]>, V: Deref<Target = [u8]>>(
        &self,
        row: &RowToWrite<K, V>,
    ) -> usize {
        match self {
            BitcaskyFormatter::V1(f) => f.net_row_size(row),
        }
    }

    fn encode_row<K: AsRef<[u8]>, V: Deref<Target = [u8]>>(
        &self,
        row: &RowToWrite<K, V>,
        output: &mut [u8],
    ) -> usize {
        match self {
            BitcaskyFormatter::V1(f) => f.encode_row(row, output),
        }
    }

    fn decode_row_header(&self, bs: &[u8]) -> RowHeader {
        match self {
            BitcaskyFormatter::V1(f) => f.decode_row_header(bs),
        }
    }

    fn validate_key_value(&self, header: &RowHeader, kv: &[u8]) -> Result<()> {
        match self {
            BitcaskyFormatter::V1(f) => f.validate_key_value(header, kv),
        }
    }

    fn row_hint_header_size(&self) -> usize {
        match self {
            BitcaskyFormatter::V1(f) => f.row_hint_header_size(),
        }
    }

    fn encode_row_hint(&self, hint: &RowHint, output: &mut [u8]) -> usize {
        match self {
            BitcaskyFormatter::V1(f) => f.encode_row_hint(hint, output),
        }
    }

    fn decode_row_hint_header(&self, header_bs: &[u8]) -> RowHintHeader {
        match self {
            BitcaskyFormatter::V1(f) => f.decode_row_hint_header(header_bs),
        }
    }

    fn merge_meta_size(&self) -> usize {
        match self {
            BitcaskyFormatter::V1(f) => f.merge_meta_size(),
        }
    }

    fn encode_merge_meta(&self, meta: &MergeMeta) -> Bytes {
        match self {
            BitcaskyFormatter::V1(f) => f.encode_merge_meta(meta),
        }
    }

    fn decode_merge_meta(&self, meta: Bytes) -> MergeMeta {
        match self {
            BitcaskyFormatter::V1(f) => f.decode_merge_meta(meta),
        }
    }
}

impl Default for BitcaskyFormatter {
    fn default() -> Self {
        BitcaskyFormatter::V1(FormatterV1::default())
    }
}

pub fn initialize_new_file(file: &mut File, version: u8) -> std::io::Result<()> {
    let mut bs = BytesMut::with_capacity(FILE_HEADER_SIZE);

    bs.extend_from_slice(MAGIC);
    bs.put_u8(version);
    bs.put_u32(0);

    file.write_all(&bs.freeze())?;
    file.flush()?;
    Ok(())
}

pub fn get_formatter_from_file(file: &mut File) -> Result<BitcaskyFormatter> {
    let mut file_header = vec![0; FILE_HEADER_SIZE];

    file.read_exact(&mut file_header)
        .map_err(|e| FormatterError::ReadFileHeaderFailed(e, "read file header failed".into()))?;

    if MAGIC != &file_header[0..3] {
        return Err(FormatterError::MagicNotMatch());
    }

    let formatter_version = file_header[3];
    if formatter_version == FORMATTER_V1_VERSION {
        return Ok(BitcaskyFormatter::V1(FormatterV1::default()));
    }

    Err(FormatterError::UnknownFormatterVersion(formatter_version))
}

// Returns the number of padding bytes to add to a buffer to ensure 4-byte alignment.
pub fn padding(len: usize) -> usize {
    4usize.wrapping_sub(len) & 7
}

#[cfg(test)]
mod tests {
    use crate::fs::{create_file, open_file, FileType};

    use super::*;

    use crate::test_utils::get_temporary_directory_path;
    use test_log::test;

    #[test]
    fn test_formatter_v1_file() {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let mut file = create_file(&dir, FileType::DataFile, Some(storage_id)).unwrap();
        let init_formatter = BitcaskyFormatter::V1(FormatterV1::default());
        initialize_new_file(&mut file, init_formatter.version()).unwrap();

        let mut file = open_file(&dir, FileType::DataFile, Some(storage_id))
            .unwrap()
            .file;

        let read_formatter = get_formatter_from_file(&mut file).unwrap();
        assert_matches!(read_formatter, BitcaskyFormatter::V1(_));
        assert_eq!(init_formatter, read_formatter);
    }

    #[test]
    fn test_read_file_header_failed() {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        create_file(&dir, FileType::DataFile, Some(storage_id)).unwrap();

        let mut file = open_file(&dir, FileType::DataFile, Some(storage_id))
            .unwrap()
            .file;

        let read_formatter = get_formatter_from_file(&mut file).unwrap_err();
        assert_matches!(read_formatter, FormatterError::ReadFileHeaderFailed(_, _));
    }

    #[test]
    fn test_invalid_magic_word() {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let mut file = create_file(&dir, FileType::DataFile, Some(storage_id)).unwrap();
        file.write_all(b"bad magic word").unwrap();

        let mut file = open_file(&dir, FileType::DataFile, Some(storage_id))
            .unwrap()
            .file;

        let read_formatter = get_formatter_from_file(&mut file).unwrap_err();
        assert_matches!(read_formatter, FormatterError::MagicNotMatch());
    }

    #[test]
    fn test_unknown_formatter_version() {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let mut file = create_file(&dir, FileType::DataFile, Some(storage_id)).unwrap();
        file.write_all(MAGIC).unwrap();
        file.write_all(b"invalid data").unwrap();

        let mut file = open_file(&dir, FileType::DataFile, Some(storage_id))
            .unwrap()
            .file;

        let read_formatter = get_formatter_from_file(&mut file).unwrap_err();
        assert_matches!(read_formatter, FormatterError::UnknownFormatterVersion(_));
    }
}
