mod v1;
use std::{
    fs::File,
    io::{Read, Write},
    ops::Deref,
};

pub use self::v1::FormatterV1;

use bytes::{BufMut, Bytes, BytesMut};
use log::info;
use thiserror::Error;

use super::common::{RowHeader, RowMeta, RowToWrite};

#[derive(Error, Debug)]
#[error("{}")]
pub enum FormatterError {
    #[error("Crc check failed. expect crc is: {expected_crc}, actual crc is: {actual_crc}")]
    CrcCheckFailed { expected_crc: u32, actual_crc: u32 },
    #[error("Got IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Invalid data file: {0}")]
    InvalidDataFile(String),
    #[error("Magic string does not match")]
    MagicNotMatch(),
    #[error("Unknown formatter version: {0}")]
    UnknownFormatterVersion(u8),
}

pub type Result<T> = std::result::Result<T, FormatterError>;

pub trait Formatter: std::marker::Send + 'static + Copy {
    fn header_size(&self) -> usize;

    fn row_size<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'_, V>) -> usize;

    fn encode_row<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'_, V>) -> Bytes;

    fn decode_row_meta(&self, buf: Bytes) -> Result<RowMeta>;

    fn decode_row_header(&self, bs: Bytes) -> Result<RowHeader>;

    fn validate_key_value(&self, header: &RowHeader, kv: &Bytes) -> Result<()>;
}

#[derive(Clone, Copy, Debug)]
pub enum DataStorageFormatter {
    V1(FormatterV1),
}

impl Formatter for DataStorageFormatter {
    fn header_size(&self) -> usize {
        match self {
            DataStorageFormatter::V1(f) => f.header_size(),
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

    fn decode_row_meta(&self, buf: Bytes) -> Result<RowMeta> {
        match self {
            DataStorageFormatter::V1(f) => f.decode_row_meta(buf),
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

pub fn initialize_new_file(mut file: File) -> Result<File> {
    let mut bs = BytesMut::with_capacity(MAGIC.len() + 1);

    bs.extend_from_slice(MAGIC);
    bs.put_u8(DEFAULT_FORMATTER_VERSION);

    let a = bs.freeze();

    file.write_all(&a)?;
    file.flush()?;

    Ok(file)
}

pub fn get_formatter_from_data_file(file: &mut File) -> Result<DataStorageFormatter> {
    let mut file_header = vec![0; MAGIC.len() + 1];

    info!("zzz {:?}", file_header);

    file.read_exact(&mut file_header).map_err(|e| );

    info!("zxcvxcvcv {:?}", file_header);

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
        let file = create_file(&dir, FileType::DataFile, Some(storage_id)).unwrap();
        initialize_new_file(file).unwrap();

        let mut file = open_file(&dir, FileType::DataFile, Some(storage_id))
            .unwrap()
            .file;

        let formatter = get_formatter_from_data_file(&mut file).unwrap();
        assert_matches!(formatter, DataStorageFormatter::V1(_));
    }

    #[test]
    fn test_formatter_v1_file2() {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let file = create_file(&dir, FileType::DataFile, Some(storage_id)).unwrap();

        let mut file = open_file(&dir, FileType::DataFile, Some(storage_id))
            .unwrap()
            .file;

        let formatter = get_formatter_from_data_file(&mut file).unwrap();
        assert_matches!(formatter, DataStorageFormatter::V1(_));
    }
}
