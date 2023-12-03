use bytes::Bytes;
use log::info;

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    ops::Deref,
};

use common::{
    formatter::{padding, BitcaskFormatter, Formatter, RowMeta, RowToWrite, FILE_HEADER_SIZE},
    storage_id::StorageId,
};

use crate::{
    common::{RowToRead, Value},
    DataStorageError,
};

use super::{
    DataStorageOptions, DataStorageReader, DataStorageWriter, Result, RowLocation, TimedValue,
};

#[derive(Debug)]
pub struct FileDataStorage {
    data_file: File,
    pub storage_id: StorageId,
    offset: usize,
    capacity: usize,
    options: DataStorageOptions,
    formatter: BitcaskFormatter,
}

impl FileDataStorage {
    pub fn new(
        storage_id: StorageId,
        data_file: File,
        write_offset: usize,
        capacity: usize,
        formatter: BitcaskFormatter,
        options: DataStorageOptions,
    ) -> Result<Self> {
        Ok(FileDataStorage {
            data_file,
            storage_id,
            offset: write_offset,
            capacity,
            options,
            formatter,
        })
    }

    fn do_read_row(&mut self, offset: usize) -> Result<Option<(RowMeta, Bytes)>> {
        let header_size = self.formatter.row_header_size();
        if offset + header_size >= self.capacity {
            return Ok(None);
        }

        let mut header_buf = vec![0; self.formatter.row_header_size()];
        self.data_file.read_exact(&mut header_buf)?;
        let header_bs = Bytes::from(header_buf);

        let header = self.formatter.decode_row_header(header_bs);
        if header.meta.key_size == 0 {
            return Ok(None);
        }

        let net_size = header_size + header.meta.key_size + header.meta.value_size;
        let padded = padding(net_size) + net_size;
        if offset + padded > self.capacity {
            return Ok(None);
        }

        let mut kv_buf = vec![0; padded - header_size];
        self.data_file.read_exact(&mut kv_buf)?;
        let kv_bs = Bytes::from(kv_buf[0..header.meta.key_size + header.meta.value_size].to_vec());

        self.formatter.validate_key_value(&header, &kv_bs)?;
        Ok(Some((header.meta, kv_bs)))
    }

    fn check_storage_overflow<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<V>) -> bool {
        let row_size = self.formatter.net_row_size(row);
        row_size + padding(row_size) + self.offset > self.options.max_data_file_size
    }
}

impl DataStorageWriter for FileDataStorage {
    fn write_row<V: Deref<Target = [u8]>>(&mut self, row: &RowToWrite<V>) -> Result<RowLocation> {
        if self.check_storage_overflow(row) {
            return Err(DataStorageError::StorageOverflow(self.storage_id));
        }

        let data_to_write = self.formatter.encode_row(row);
        let value_offset = self.offset;
        self.data_file
            .write_all(&data_to_write)
            .map_err(|e| DataStorageError::WriteRowFailed(self.storage_id, e.to_string()))?;
        self.offset += data_to_write.len();
        if self.offset > self.capacity {
            self.capacity = self.offset;
        }

        Ok(RowLocation {
            storage_id: self.storage_id,
            row_offset: value_offset,
        })
    }

    fn rewind(&mut self) -> Result<()> {
        self.data_file.flush()?;

        self.data_file
            .seek(SeekFrom::Start(FILE_HEADER_SIZE as u64))?;

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(self.data_file.flush()?)
    }
}

impl DataStorageReader for FileDataStorage {
    fn read_value(&mut self, row_offset: usize) -> Result<TimedValue<Value>> {
        self.data_file
            .seek(SeekFrom::Start(row_offset as u64))
            .map_err(|e| {
                DataStorageError::ReadRowFailed(
                    self.storage_id,
                    format!("seek to: {} failed, error: {}", row_offset, e),
                )
            })?;

        if let Some((meta, kv_bs)) = self
            .do_read_row(row_offset)
            .map_err(|e| DataStorageError::ReadRowFailed(self.storage_id, e.to_string()))?
        {
            return Ok(TimedValue {
                value: Value::VectorBytes(kv_bs.slice(meta.key_size..).into()),
                expire_timestamp: meta.expire_timestamp,
            });
        }
        Err(DataStorageError::ReadRowFailed(
            self.storage_id,
            format!("no value found at offset: {}", row_offset),
        ))
    }

    fn read_next_row(&mut self) -> Result<Option<RowToRead>> {
        let value_offset = self.data_file.stream_position()? as usize;
        info!("read value {}", value_offset);

        if let Some((meta, kv_bs)) = self.do_read_row(value_offset)? {
            let net_size = self.formatter.row_header_size() + kv_bs.len();

            self.offset = value_offset + padding(net_size) + net_size;
            return Ok(Some(RowToRead {
                key: kv_bs.slice(0..meta.key_size).into(),
                value: TimedValue::expirable_value(
                    kv_bs
                        .slice(meta.key_size..(meta.key_size + meta.value_size))
                        .into(),
                    meta.expire_timestamp,
                ),
                row_location: RowLocation {
                    storage_id: self.storage_id,
                    row_offset: value_offset,
                },
            }));
        }
        Ok(None)
    }

    fn seek_to_end(&mut self) -> Result<()> {
        loop {
            let offset = self.data_file.stream_position().unwrap();
            if self.read_next_row()?.is_none() {
                self.data_file.seek(SeekFrom::Start(offset)).unwrap();
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common::{
        formatter::initialize_new_file,
        fs::{create_file, FileType},
    };

    use crate::data_storage::{DataSotrageType, DataStorage};

    use super::*;

    use bitcask_tests::common::get_temporary_directory_path;
    use test_log::test;

    fn get_file_storage(max_size: usize) -> DataStorage {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let mut file = create_file(&dir, FileType::DataFile, Some(storage_id)).unwrap();
        initialize_new_file(&mut file, BitcaskFormatter::default().version()).unwrap();
        let options = DataStorageOptions::default()
            .max_data_file_size(max_size)
            .init_data_file_capacity(max_size)
            .storage_type(DataSotrageType::File);
        DataStorage::open(&dir, 1, options).unwrap()
    }

    #[test]
    fn test_read_write_file_storage() {
        let mut storage = get_file_storage(1024);

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
        let row_location1 = storage.write_row(&row_to_write).unwrap();

        let k2: Vec<u8> = "key2".into();
        let v2: Vec<u8> = "value2".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k2, v2.clone());
        let row_location2 = storage.write_row(&row_to_write).unwrap();

        assert_eq!(v1, *storage.read_value(row_location1.row_offset).unwrap());
        assert_eq!(v2, *storage.read_value(row_location2.row_offset).unwrap());
    }

    #[test]
    fn test_write_overflow() {
        let mut storage = get_file_storage(2);

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
        storage.write_row(&row_to_write).expect_err("overflow");
    }

    #[test]
    fn test_file_storage_read_next_row() {
        let mut storage = get_file_storage(1024);

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
        storage.write_row(&row_to_write).unwrap();

        let k2: Vec<u8> = "key2".into();
        let v2: Vec<u8> = "value2".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k2, v2.clone());
        storage.write_row(&row_to_write).unwrap();

        storage.rewind().unwrap();

        let r = storage.read_next_row().unwrap().unwrap();

        assert_eq!(k1, r.key);
        assert_eq!(v1, r.value.value);
        let r = storage.read_next_row().unwrap().unwrap();
        assert_eq!(k2, r.key);
        assert_eq!(v2, r.value.value);
    }

    #[test]
    fn test_rewind() {
        let mut storage = get_file_storage(1024);

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
        let location = storage.write_row(&row_to_write).unwrap();
        storage.rewind().unwrap();

        if let Some(r) = storage.read_next_row().unwrap() {
            assert_eq!("key1".as_bytes().to_vec(), r.key);
            assert_eq!(location, r.row_location);
        } else {
            unreachable!();
        }
    }
}
