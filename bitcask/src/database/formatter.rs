use std::ops::Deref;

use bytes::{Buf, Bytes, BytesMut};
use crc::{Crc, CRC_32_CKSUM};

use super::{
    common::{RowHeader, RowMeta, RowToWrite},
    constants::{
        DATA_FILE_KEY_OFFSET, DATA_FILE_KEY_SIZE_OFFSET, DATA_FILE_TSTAMP_OFFSET,
        DATA_FILE_VALUE_SIZE_OFFSET, KEY_SIZE_SIZE, VALUE_SIZE_SIZE,
    },
};

use thiserror::Error;

#[derive(Error, Debug)]
#[error("{}")]
pub enum FormatterError {
    #[error("Crc check failed. expect crc is: {expected_crc}, actual crc is: {actual_crc}")]
    CrcCheckFailed { expected_crc: u32, actual_crc: u32 },
}

pub type Result<T> = std::result::Result<T, FormatterError>;

pub trait Formatter {
    fn header_size(&self) -> usize;

    fn row_size<'a, V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'a, V>) -> usize;

    fn encode_row<'a, V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'a, V>) -> Bytes;

    fn decode_row_meta(&self, buf: Bytes) -> Result<RowMeta>;

    fn decode_row_header(&self, bs: Bytes) -> Result<RowHeader>;
}

pub trait RowDataChecker {
    fn gen_crc<'a, V: Deref<Target = [u8]>>(
        &self,
        meta: &RowMeta,
        key: &'a Vec<u8>,
        value: &V,
    ) -> u32;

    fn gen_crc_by_kv_bytes(&self, meta: &RowMeta, kv: &Bytes) -> u32;

    fn check_crc(&self, header: &RowHeader, kv: &Bytes) -> Result<()> {
        let actual_crc = self.gen_crc_by_kv_bytes(&header.meta, kv);
        if header.crc != actual_crc {
            return Err(FormatterError::CrcCheckFailed {
                expected_crc: header.crc,
                actual_crc,
            });
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct DefaultCrcChecker {}

impl RowDataChecker for DefaultCrcChecker {
    fn gen_crc<'a, V: Deref<Target = [u8]>>(
        &self,
        meta: &RowMeta,
        key: &'a Vec<u8>,
        value: &V,
    ) -> u32 {
        let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut ck = crc32.digest();
        ck.update(&meta.timestamp.to_be_bytes());
        ck.update(&meta.key_size.to_be_bytes());
        ck.update(&value.len().to_be_bytes());
        ck.update(key);
        ck.update(&value);
        ck.finalize()
    }

    fn gen_crc_by_kv_bytes(&self, meta: &RowMeta, kv: &Bytes) -> u32 {
        let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut ck = crc32.digest();
        ck.update(&meta.timestamp.to_be_bytes());
        ck.update(&meta.key_size.to_be_bytes());
        ck.update(&meta.value_size.to_be_bytes());
        ck.update(kv);
        ck.finalize()
    }
}

#[derive(Debug)]
pub struct FormatterV1 {
    pub checker: DefaultCrcChecker,
}

impl FormatterV1 {
    pub fn new() -> FormatterV1 {
        FormatterV1 {
            checker: DefaultCrcChecker {},
        }
    }
}

impl Formatter for FormatterV1 {
    fn header_size(&self) -> usize {
        DATA_FILE_KEY_OFFSET
    }

    fn row_size<'a, V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'a, V>) -> usize {
        self.header_size() + row.key.len() + row.value.len()
    }

    fn encode_row<'a, V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'a, V>) -> Bytes {
        let mut bs = BytesMut::with_capacity(self.row_size(&row));

        let crc = self.checker.gen_crc(&row.meta, row.key, &row.value);

        bs.extend_from_slice(&crc.to_be_bytes());
        bs.extend_from_slice(&row.meta.timestamp.to_be_bytes());
        bs.extend_from_slice(&row.meta.key_size.to_be_bytes());
        bs.extend_from_slice(&row.meta.value_size.to_be_bytes());
        bs.extend_from_slice(row.key);
        bs.extend_from_slice(&row.value);
        bs.freeze()
    }

    fn decode_row_meta(&self, bs: Bytes) -> Result<RowMeta> {
        let timestamp = bs
            .slice(DATA_FILE_TSTAMP_OFFSET..DATA_FILE_KEY_SIZE_OFFSET)
            .get_u64();

        let key_size = bs
            .slice(DATA_FILE_KEY_SIZE_OFFSET..(DATA_FILE_KEY_SIZE_OFFSET + KEY_SIZE_SIZE))
            .get_u64();
        let val_size = bs
            .slice(DATA_FILE_VALUE_SIZE_OFFSET..(DATA_FILE_VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE))
            .get_u64();
        Ok(RowMeta {
            timestamp,
            key_size,
            value_size: val_size,
        })
    }

    fn decode_row_header(&self, bs: Bytes) -> Result<RowHeader> {
        let expected_crc = bs.slice(0..DATA_FILE_TSTAMP_OFFSET).get_u32();
        let timestamp = bs
            .slice(DATA_FILE_TSTAMP_OFFSET..DATA_FILE_KEY_SIZE_OFFSET)
            .get_u64();

        let key_size = bs
            .slice(DATA_FILE_KEY_SIZE_OFFSET..(DATA_FILE_KEY_SIZE_OFFSET + KEY_SIZE_SIZE))
            .get_u64();
        let val_size = bs
            .slice(DATA_FILE_VALUE_SIZE_OFFSET..(DATA_FILE_VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE))
            .get_u64();
        Ok(RowHeader {
            crc: expected_crc,
            meta: RowMeta {
                timestamp,
                key_size,
                value_size: val_size,
            },
        })
    }
}
