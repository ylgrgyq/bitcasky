use std::ops::Deref;

use bytes::{Buf, Bytes, BytesMut};
use crc::{Crc, CRC_32_CKSUM};

use super::{
    padding, Formatter, FormatterError, MergeMeta, Result, RowHeader, RowHintHeader, RowMeta,
    RowToWrite,
};

const CRC_SIZE: usize = 4;
const TSTAMP_SIZE: usize = 8;
const KEY_SIZE_SIZE: usize = 8;
const VALUE_SIZE_SIZE: usize = 8;
const DATA_FILE_TSTAMP_OFFSET: usize = CRC_SIZE;
const DATA_FILE_KEY_SIZE_OFFSET: usize = CRC_SIZE + TSTAMP_SIZE;
const DATA_FILE_VALUE_SIZE_OFFSET: usize = DATA_FILE_KEY_SIZE_OFFSET + KEY_SIZE_SIZE;
const DATA_FILE_KEY_OFFSET: usize = CRC_SIZE + TSTAMP_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE;

const ROW_OFFSET_SIZE: usize = 8;
const HINT_FILE_KEY_SIZE_OFFSET: usize = TSTAMP_SIZE;
const HINT_FILE_ROW_OFFSET_OFFSET: usize = HINT_FILE_KEY_SIZE_OFFSET + KEY_SIZE_SIZE;
const HINT_FILE_KEY_OFFSET: usize = HINT_FILE_ROW_OFFSET_OFFSET + ROW_OFFSET_SIZE;
const HINT_FILE_HEADER_SIZE: usize = TSTAMP_SIZE + KEY_SIZE_SIZE + ROW_OFFSET_SIZE;

const MERGE_META_FILE_SIZE: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct FormatterV1 {}

impl FormatterV1 {
    fn gen_crc<V: Deref<Target = [u8]>>(&self, meta: &RowMeta, key: &[u8], value: &V) -> u32 {
        let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut ck = crc32.digest();
        ck.update(&meta.timestamp.to_be_bytes());
        ck.update(&meta.key_size.to_be_bytes());
        ck.update(&value.len().to_be_bytes());
        ck.update(key);
        ck.update(value);
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

impl Formatter for FormatterV1 {
    fn row_header_size(&self) -> usize {
        DATA_FILE_KEY_OFFSET
    }

    fn net_row_size<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'_, V>) -> usize {
        self.row_header_size() + row.key.len() + row.value.len()
    }

    fn encode_row<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'_, V>) -> Bytes {
        let net_size = self.net_row_size(row);
        let mut bs = BytesMut::with_capacity(net_size);

        let crc = self.gen_crc(&row.meta, row.key, &row.value);

        bs.extend_from_slice(&crc.to_be_bytes());
        bs.extend_from_slice(&row.meta.timestamp.to_be_bytes());
        bs.extend_from_slice(&row.meta.key_size.to_be_bytes());
        bs.extend_from_slice(&row.meta.value_size.to_be_bytes());
        bs.extend_from_slice(row.key);
        bs.extend_from_slice(&row.value);
        bs.resize(net_size + padding(net_size), 0);
        bs.freeze()
    }

    fn decode_row_header(&self, bs: Bytes) -> RowHeader {
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
        RowHeader {
            crc: expected_crc,
            meta: RowMeta {
                timestamp,
                key_size,
                value_size: val_size,
            },
        }
    }

    fn validate_key_value(&self, header: &RowHeader, kv: &Bytes) -> Result<()> {
        let actual_crc = self.gen_crc_by_kv_bytes(&header.meta, kv);
        if header.crc != actual_crc {
            return Err(FormatterError::CrcCheckFailed {
                expected_crc: header.crc,
                actual_crc,
            });
        }
        Ok(())
    }

    fn encode_row_hint(&self, hint: &super::RowHint) -> Bytes {
        let mut bs = BytesMut::with_capacity(HINT_FILE_HEADER_SIZE + hint.key.len());
        let header = &hint.header;
        bs.extend_from_slice(&header.timestamp.to_be_bytes());
        bs.extend_from_slice(&header.key_size.to_be_bytes());
        bs.extend_from_slice(&header.row_offset.to_be_bytes());
        bs.extend_from_slice(&hint.key);
        bs.freeze()
    }

    fn row_hint_header_size(&self) -> usize {
        HINT_FILE_HEADER_SIZE
    }

    fn decode_row_hint_header(&self, header_bs: Bytes) -> RowHintHeader {
        let timestamp = header_bs.slice(0..TSTAMP_SIZE).get_u64();
        let key_size = header_bs
            .slice(HINT_FILE_KEY_SIZE_OFFSET..HINT_FILE_ROW_OFFSET_OFFSET)
            .get_u64();
        let row_offset = header_bs
            .slice(HINT_FILE_ROW_OFFSET_OFFSET..HINT_FILE_KEY_OFFSET)
            .get_u64();

        RowHintHeader {
            timestamp,
            key_size,
            row_offset,
        }
    }

    fn merge_meta_size(&self) -> usize {
        MERGE_META_FILE_SIZE
    }

    fn encode_merge_meta(&self, meta: &super::MergeMeta) -> Bytes {
        Bytes::copy_from_slice(&meta.known_max_storage_id.to_be_bytes())
    }

    fn decode_merge_meta(&self, mut meta: Bytes) -> MergeMeta {
        let known_max_storage_id = meta.get_u32();
        MergeMeta {
            known_max_storage_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::formatter::RowHint;

    use super::*;

    use test_log::test;

    #[test]
    fn test_encode_decode_merge_meta() {
        let merge_meta = MergeMeta {
            known_max_storage_id: 123,
        };

        let formatter = FormatterV1 {};
        let bytes = formatter.encode_merge_meta(&merge_meta);
        assert_eq!(formatter.merge_meta_size(), bytes.len());
        assert_eq!(merge_meta, formatter.decode_merge_meta(bytes));
    }

    #[test]
    fn test_encode_decode_row_hint() {
        let k = b"Hello".to_vec();
        let k_len = k.len();
        let hint = RowHint {
            header: RowHintHeader {
                timestamp: 12345,
                key_size: k.len() as u64,
                row_offset: 56789,
            },
            key: k,
        };

        let formatter = FormatterV1 {};
        let bytes = formatter.encode_row_hint(&hint);

        assert_eq!(k_len + formatter.row_hint_header_size(), bytes.len());
        assert_eq!(hint.header, formatter.decode_row_hint_header(bytes));
    }

    #[test]
    fn test_encode_decode_row() {
        let k = b"Hello".to_vec();
        let v = b"World".to_vec();
        let row = RowToWrite {
            meta: RowMeta {
                timestamp: 12345,
                key_size: k.len() as u64,
                value_size: v.len() as u64,
            },
            key: &k,
            value: v,
        };

        let formatter = FormatterV1 {};
        let bytes = formatter.encode_row(&row);

        assert_eq!(
            formatter.net_row_size(&row) + padding(formatter.net_row_size(&row)),
            bytes.len()
        );
        assert_eq!(row.meta, formatter.decode_row_header(bytes).meta);
    }
}
