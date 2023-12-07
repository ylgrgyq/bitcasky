use std::{fs::File, io::Write, mem, ops::Deref, ptr};

use bytes::Bytes;
use common::{
    clock::Clock,
    formatter::{padding, BitcaskFormatter, Formatter, RowMeta, RowToWrite, FILE_HEADER_SIZE},
    options::BitcaskOptions,
    storage_id::StorageId,
};
use log::debug;
use memmap2::{MmapMut, MmapOptions};

use crate::{
    common::{RowToRead, Value},
    DataStorageError, RowLocation, TimedValue,
};

use super::{DataStorageReader, DataStorageWriter, Result};

#[derive(Debug)]
pub struct MmapDataStorage {
    data_file: File,
    pub storage_id: StorageId,
    write_offset: usize,
    capacity: usize,
    options: BitcaskOptions,
    formatter: BitcaskFormatter,
    map_view: MmapMut,
}

impl MmapDataStorage {
    pub fn new(
        storage_id: StorageId,
        data_file: File,
        write_offset: usize,
        capacity: usize,
        formatter: BitcaskFormatter,
        options: BitcaskOptions,
    ) -> Result<Self> {
        let mmap = unsafe {
            MmapOptions::new()
                .offset(0)
                .len(capacity)
                .map_mut(&data_file)?
        };

        Ok(MmapDataStorage {
            data_file,
            storage_id,
            write_offset,
            capacity,
            options,
            formatter,
            map_view: mmap,
        })
    }

    fn ensure_capacity<V: Deref<Target = [u8]>>(&mut self, row: &RowToWrite<V>) -> Result<()> {
        let mut row_size = self.formatter.net_row_size(row);
        row_size += padding(row_size);
        let required_capacity = row_size + self.write_offset;
        if required_capacity > self.options.database.storage.max_data_file_size {
            return Err(DataStorageError::StorageOverflow(self.storage_id));
        }

        if required_capacity > self.capacity {
            let mut new_capacity =
                std::cmp::max(required_capacity, self.capacity + self.capacity / 3);
            new_capacity = std::cmp::min(
                new_capacity,
                self.options.database.storage.max_data_file_size,
            );
            debug!(
                "data file with storage id: {:?}, resizing to {} bytes",
                self.storage_id, new_capacity
            );
            self.flush()?;

            common::resize_file(&self.data_file, new_capacity)?;
            let mut mmap = unsafe {
                MmapOptions::new()
                    .offset(0)
                    .len(new_capacity)
                    .map_mut(&self.data_file)?
            };
            mem::swap(&mut mmap, &mut self.map_view);
            self.capacity = new_capacity;
        }
        Ok(())
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.map_view[0..self.capacity]
    }

    fn as_slice(&self) -> &[u8] {
        &self.map_view[0..self.capacity]
    }

    pub fn copy_memory(src: &[u8], dst: &mut [u8]) {
        let len_src = src.len();
        assert!(dst.len() >= len_src);
        unsafe {
            ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), len_src);
        }
    }

    fn do_read_row(&mut self, offset: usize) -> Result<Option<(RowMeta, Bytes)>> {
        let header_size = self.formatter.row_header_size();
        if offset + header_size >= self.capacity {
            return Ok(None);
        }

        let header_bs = Bytes::copy_from_slice(
            &self.as_slice()[offset..(offset + self.formatter.row_header_size())],
        );

        let header = self.formatter.decode_row_header(header_bs);
        if header.meta.key_size == 0 {
            return Ok(None);
        }

        if offset + header_size + header.meta.key_size + header.meta.value_size > self.capacity {
            return Ok(None);
        }

        let net_size =
            self.formatter.row_header_size() + header.meta.key_size + header.meta.value_size;

        let kv_bs = Bytes::copy_from_slice(
            &self.as_slice()[offset + self.formatter.row_header_size()..offset + net_size],
        );

        self.formatter.validate_key_value(&header, &kv_bs)?;
        Ok(Some((header.meta, kv_bs)))
    }
}

impl DataStorageWriter for MmapDataStorage {
    fn write_row<V: std::ops::Deref<Target = [u8]>>(
        &mut self,
        row: &common::formatter::RowToWrite<V>,
    ) -> super::Result<crate::RowLocation> {
        self.ensure_capacity(row)?;

        let data_to_write = self.formatter.encode_row(row);

        let value_offset = self.write_offset;
        MmapDataStorage::copy_memory(&data_to_write, &mut self.as_mut_slice()[value_offset..]);
        self.write_offset += data_to_write.len();

        Ok(RowLocation {
            storage_id: self.storage_id,
            row_offset: value_offset,
        })
    }

    fn rewind(&mut self) -> super::Result<()> {
        self.data_file.flush()?;
        self.write_offset = FILE_HEADER_SIZE;
        Ok(())
    }

    fn flush(&mut self) -> super::Result<()> {
        Ok(self.map_view.flush_range(0, self.capacity)?)
    }
}

impl DataStorageReader for MmapDataStorage {
    fn read_value(&mut self, row_offset: usize) -> super::Result<Option<TimedValue<Value>>> {
        if let Some((meta, kv_bs)) = self
            .do_read_row(row_offset)
            .map_err(|e| DataStorageError::ReadRowFailed(self.storage_id, e.to_string()))?
        {
            if meta.expire_timestamp != 0 && meta.expire_timestamp <= self.options.clock.now() {
                return Ok(None);
            }

            return Ok(TimedValue {
                value: Value::VectorBytes(
                    kv_bs
                        .slice(meta.key_size..meta.key_size + meta.value_size)
                        .into(),
                ),
                expire_timestamp: meta.expire_timestamp,
            }
            .validate());
        }
        Err(DataStorageError::ReadRowFailed(
            self.storage_id,
            format!("no value found at offset: {}", row_offset),
        ))
    }

    fn read_next_row(&mut self) -> super::Result<Option<RowToRead>> {
        if let Some((meta, kv_bs)) = self.do_read_row(self.write_offset)? {
            let row_to_read = RowToRead {
                key: kv_bs.slice(0..meta.key_size).into(),
                value: TimedValue::expirable_value(
                    kv_bs.slice(meta.key_size..).into(),
                    meta.expire_timestamp,
                ),
                row_location: RowLocation {
                    storage_id: self.storage_id,
                    row_offset: self.write_offset,
                },
            };
            let net_size = self.formatter.row_header_size() + kv_bs.len();
            self.write_offset += net_size + padding(net_size);
            return Ok(Some(row_to_read));
        }
        Ok(None)
    }

    fn seek_to_end(&mut self) -> Result<()> {
        loop {
            if self.read_next_row()?.is_none() {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common::{
        clock::DebugClock, create_file, formatter::FILE_HEADER_SIZE, fs::FileType,
        options::DataSotrageType,
    };

    use super::*;

    use bitcask_tests::common::get_temporary_directory_path;
    use test_log::test;

    fn get_options(max_size: usize) -> BitcaskOptions {
        BitcaskOptions::default()
            .max_data_file_size(max_size)
            .init_data_file_capacity(max_size)
            .storage_type(DataSotrageType::Mmap)
    }

    fn get_file_storage(options: BitcaskOptions) -> MmapDataStorage {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let formatter = BitcaskFormatter::default();
        let file = create_file(dir, FileType::DataFile, Some(storage_id), &formatter, 512).unwrap();
        let meta = file.metadata().unwrap();
        MmapDataStorage::new(
            1,
            file,
            FILE_HEADER_SIZE,
            meta.len() as usize,
            formatter,
            options,
        )
        .unwrap()
    }

    #[test]
    fn test_read_write_immotal_value() {
        let mut storage = get_file_storage(get_options(1024));

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
        let row_location1 = storage.write_row(&row_to_write).unwrap();

        let k2: Vec<u8> = "key2".into();
        let v2: Vec<u8> = "value2".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k2, v2.clone());
        let row_location2 = storage.write_row(&row_to_write).unwrap();

        assert_eq!(
            v1,
            *storage
                .read_value(row_location1.row_offset)
                .unwrap()
                .unwrap()
        );
        assert_eq!(
            v2,
            *storage
                .read_value(row_location2.row_offset)
                .unwrap()
                .unwrap()
        );
    }

    #[test]
    fn test_read_write_expired_value() {
        let clock = DebugClock::new(1000);
        let mut storage = get_file_storage(get_options(1024).debug_clock(clock));

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> =
            RowToWrite::new_with_timestamp(&k1, v1.clone(), 100);
        let row_location1 = storage.write_row(&row_to_write).unwrap();

        let k2: Vec<u8> = "key2".into();
        let v2: Vec<u8> = "value2".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> =
            RowToWrite::new_with_timestamp(&k2, v2.clone(), 100);
        let row_location2 = storage.write_row(&row_to_write).unwrap();

        assert!(storage
            .read_value(row_location1.row_offset)
            .unwrap()
            .is_none());
        assert!(storage
            .read_value(row_location2.row_offset)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_read_write_not_expired_value() {
        let clock = DebugClock::new(1000);
        let mut storage = get_file_storage(get_options(1024).debug_clock(clock));

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> =
            RowToWrite::new_with_timestamp(&k1, v1.clone(), 2000);
        let row_location1 = storage.write_row(&row_to_write).unwrap();

        let k2: Vec<u8> = "key2".into();
        let v2: Vec<u8> = "value2".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> =
            RowToWrite::new_with_timestamp(&k2, v2.clone(), 2000);
        let row_location2 = storage.write_row(&row_to_write).unwrap();

        assert_eq!(
            v1,
            *storage
                .read_value(row_location1.row_offset)
                .unwrap()
                .unwrap()
        );
        assert_eq!(
            v2,
            *storage
                .read_value(row_location2.row_offset)
                .unwrap()
                .unwrap()
        );
    }

    #[test]
    fn test_write_overflow() {
        let mut storage = get_file_storage(get_options(2));

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
        storage.write_row(&row_to_write).expect_err("overflow");
    }

    #[test]
    fn test_expand_file_size() {
        let mut storage = get_file_storage(get_options(2048));
        let init_size = storage.data_file.metadata().unwrap().len();

        let mut size = 0;
        for i in 0..100 {
            if size >= 1800 {
                break;
            }

            let k1: Vec<u8> = format!("key{}", i).into();
            let v1: Vec<u8> = format!("value{}", i).into();
            let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
            storage.write_row(&row_to_write).unwrap();
            let net_size = storage.formatter.net_row_size(&row_to_write);
            size += net_size + padding(net_size);
        }

        assert!(storage.data_file.metadata().unwrap().len() > init_size);
    }

    #[test]
    fn test_read_next_immortal_row() {
        let mut storage = get_file_storage(get_options(1024));

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
    fn test_read_next_expired_row() {
        let clock = DebugClock::new(1000);
        let mut storage = get_file_storage(get_options(1024).debug_clock(clock));

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> =
            RowToWrite::new_with_timestamp(&k1, v1.clone(), 1001);
        storage.write_row(&row_to_write).unwrap();

        let k2: Vec<u8> = "key2".into();
        let v2: Vec<u8> = "value2".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> =
            RowToWrite::new_with_timestamp(&k2, v2.clone(), 100);
        storage.write_row(&row_to_write).unwrap();

        storage.rewind().unwrap();

        let r = storage.read_next_row().unwrap().unwrap();
        assert_eq!(k1, r.key);
        assert_eq!(v1, r.value.value);
        assert_eq!(1001, r.value.expire_timestamp);
        let r = storage.read_next_row().unwrap().unwrap();
        assert_eq!(k2, r.key);
        assert_eq!(v2, r.value.value);
        assert_eq!(100, r.value.expire_timestamp);
        assert!(storage.read_next_row().unwrap().is_none());
    }

    #[test]
    fn test_rewind() {
        let mut storage = get_file_storage(get_options(1024));

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
