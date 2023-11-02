mod v1;
use std::{
    fs::File,
    io::{self, Read, Write},
    ops::Deref,
};

pub use self::v1::FormatterV1;

use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

use super::common::{RowHeader, RowToWrite};

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

pub type Result<T> = std::result::Result<T, FormatterError>;

pub trait Formatter: std::marker::Send + 'static + Copy {
    fn row_header_size(&self) -> usize;

    fn row_size<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'_, V>) -> usize;

    fn encode_row<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'_, V>) -> Bytes;

    fn decode_row_header(&self, bs: Bytes) -> Result<RowHeader>;

    fn validate_key_value(&self, header: &RowHeader, kv: &Bytes) -> Result<()>;
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
}

const MAGIC: &[u8; 3] = b"btk";
const DEFAULT_FORMATTER_VERSION: u8 = 0;
pub const FILE_HEADER_SIZE: usize = 4;

pub fn initialize_new_file(file: &mut File) -> Result<DataStorageFormatter> {
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
