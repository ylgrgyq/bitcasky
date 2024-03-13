use std::{fs::File, io::Write, mem, ops::Deref, sync::Arc, vec};

use crate::common::{
    clock::Clock,
    formatter::{padding, BitcaskyFormatter, Formatter, RowMeta, RowToWrite, FILE_HEADER_SIZE},
    storage_id::StorageId,
};
use crate::options::BitcaskyOptions;
use log::debug;
use memmap2::{MmapMut, MmapOptions};

use crate::database::{common::RowToRead, DataStorageError, RowLocation, TimedValue};

use super::{DataStorageReader, DataStorageWriter, Result};

type MetaAndKeyValue<'a> = (RowMeta, &'a [u8], Option<Vec<u8>>);

#[derive(Debug)]
pub struct MmapDataStorage {
    pub offset: usize,
    pub capacity: usize,
    pub read_value_times: u64,
    pub write_times: u64,
    data_file: File,
    storage_id: StorageId,
    options: Arc<BitcaskyOptions>,
    formatter: Arc<BitcaskyFormatter>,
    map_view: MmapMut,
}

impl MmapDataStorage {
    pub fn new(
        storage_id: StorageId,
        data_file: File,
        write_offset: usize,
        capacity: usize,
        formatter: Arc<BitcaskyFormatter>,
        options: Arc<BitcaskyOptions>,
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
            offset: write_offset,
            capacity,
            options,
            formatter,
            map_view: mmap,
            read_value_times: 0,
            write_times: 0,
        })
    }

    fn ensure_capacity<K: AsRef<[u8]>, V: Deref<Target = [u8]>>(
        &mut self,
        row: &RowToWrite<K, V>,
    ) -> Result<()> {
        let mut row_size = self.formatter.net_row_size(row);
        row_size += padding(row_size);
        let required_capacity = row_size + self.offset;
        if required_capacity > self.options.database.storage.max_data_file_size {
            return Err(DataStorageError::StorageOverflow(self.storage_id));
        }

        if required_capacity > self.capacity {
            let mut new_capacity =
                std::cmp::max(required_capacity + 8, self.capacity + self.capacity / 3);
            new_capacity = std::cmp::min(
                new_capacity,
                self.options.database.storage.max_data_file_size,
            );

            self.flush()?;

            new_capacity = crate::common::resize_file(&self.data_file, new_capacity)?;
            debug!(
                "data file with storage id: {:?}, require {} bytes, resizing from {} to {} bytes. ",
                self.storage_id, required_capacity, self.capacity, new_capacity
            );
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

    fn do_read_row(&mut self, offset: usize) -> Result<Option<MetaAndKeyValue>> {
        if offset > self.capacity {
            return Err(DataStorageError::EofError());
        }

        if offset == self.capacity {
            return Ok(None);
        }

        let header_size = self.formatter.row_header_size();
        if offset + header_size >= self.capacity {
            return Err(DataStorageError::EofError());
        }

        let header = self.formatter.decode_row_header(
            &self.as_slice()[offset..(offset + self.formatter.row_header_size())],
        );
        if header.meta.key_size == 0 {
            return Ok(None);
        }

        if offset + header_size + header.meta.key_size + header.meta.value_size > self.capacity {
            return Err(DataStorageError::EofError());
        }

        let net_size =
            self.formatter.row_header_size() + header.meta.key_size + header.meta.value_size;

        let kv_bs = &self.as_slice()[offset + self.formatter.row_header_size()..offset + net_size];

        self.formatter.validate_key_value(&header, kv_bs)?;

        let k = &kv_bs[0..header.meta.key_size];
        if header.meta.expire_timestamp != 0
            && header.meta.expire_timestamp <= self.options.clock.now()
        {
            Ok(Some((header.meta, k, None)))
        } else {
            let v = Some(kv_bs[header.meta.key_size..].into());
            Ok(Some((header.meta, k, v)))
        }
    }
}

impl DataStorageWriter for MmapDataStorage {
    fn write_row<K: AsRef<[u8]>, V: Deref<Target = [u8]>>(
        &mut self,
        row: &RowToWrite<K, V>,
    ) -> super::Result<RowLocation> {
        self.ensure_capacity(row)?;

        let value_offset = self.offset;
        let formatter = self.formatter.clone();
        let net_size = formatter.encode_row(row, &mut self.as_mut_slice()[value_offset..]);
        let row_size = net_size + padding(net_size);
        self.offset += row_size;
        self.write_times += 1;

        Ok(RowLocation {
            storage_id: self.storage_id,
            row_offset: value_offset,
            row_size,
        })
    }

    fn rewind(&mut self) -> super::Result<()> {
        self.data_file.flush()?;
        self.offset = FILE_HEADER_SIZE;
        Ok(())
    }

    fn flush(&mut self) -> super::Result<()> {
        Ok(self.map_view.flush_range(0, self.capacity)?)
    }
}

impl DataStorageReader for MmapDataStorage {
    fn read_value(&mut self, row_offset: usize) -> super::Result<Option<TimedValue<Vec<u8>>>> {
        let storage_id = self.storage_id;
        let row = self
            .do_read_row(row_offset)
            .map_err(|e| DataStorageError::ReadRowFailed(storage_id, e.to_string()))?;
        if row.is_none() {
            return Err(DataStorageError::ReadRowFailed(
                self.storage_id,
                format!("no value found at offset: {}", row_offset),
            ));
        }

        let ret = {
            let (meta, _, v_op) = row.unwrap();
            if let Some(v) = v_op {
                Ok(TimedValue {
                    value: v,
                    expire_timestamp: meta.expire_timestamp,
                }
                .validate())
            } else {
                Ok(None)
            }
        };
        self.read_value_times += 1;
        ret
    }

    fn read_next_row(&mut self) -> super::Result<Option<RowToRead>> {
        let row_offset = self.offset;
        let row = self.do_read_row(row_offset)?;
        if row.is_none() {
            return Ok(None);
        }

        let (meta, k, v) = row.unwrap();
        let key = k.into();
        let net_size: usize = self.formatter.row_header_size() + meta.key_size + meta.value_size;
        let row_size = net_size + padding(net_size);
        let row_to_read = RowToRead {
            key,
            value: TimedValue::expirable_value(v.unwrap_or(vec![]), meta.expire_timestamp),
            row_location: RowLocation {
                storage_id: self.storage_id,
                row_offset,
                row_size,
            },
        };

        self.offset += row_size;

        Ok(Some(row_to_read))
    }

    fn seek_to_end(&mut self) -> Result<()> {
        loop {
            if self.read_next_row()?.is_none() {
                break;
            }
        }
        Ok(())
    }

    fn offset(&self) -> usize {
        self.offset
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{
        clock::DebugClock, create_file, formatter::FILE_HEADER_SIZE, fs::FileType,
    };
    use crate::options::DataSotrageType;

    use super::*;

    use crate::utilities::common::get_temporary_directory_path;
    use test_log::test;

    fn get_options(max_size: usize) -> BitcaskyOptions {
        BitcaskyOptions::default()
            .max_data_file_size(max_size)
            .init_data_file_capacity(max_size)
            .storage_type(DataSotrageType::Mmap)
    }

    fn get_file_storage(options: BitcaskyOptions) -> MmapDataStorage {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let formatter = Arc::new(BitcaskyFormatter::default());
        let file = create_file(
            dir,
            FileType::DataFile,
            Some(storage_id),
            &formatter,
            false,
            512,
        )
        .unwrap();
        let meta = file.metadata().unwrap();
        MmapDataStorage::new(
            1,
            file,
            FILE_HEADER_SIZE,
            meta.len() as usize,
            formatter,
            Arc::new(options),
        )
        .unwrap()
    }

    #[test]
    fn test_read_write_immotal_value() {
        let mut storage = get_file_storage(get_options(1024));

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<Vec<u8>, Vec<u8>> = RowToWrite::new(k1, v1.clone());
        let row_location1 = storage.write_row(&row_to_write).unwrap();

        let k2: Vec<u8> = "key2".into();
        let v2: Vec<u8> = "value2".into();
        let row_to_write: RowToWrite<Vec<u8>, Vec<u8>> = RowToWrite::new(k2, v2.clone());
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
        let time = 1000;
        let clock = Arc::new(DebugClock::new(time));
        let mut storage = get_file_storage(get_options(1024).debug_clock(clock.clone()));

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<Vec<u8>, Vec<u8>> =
            RowToWrite::new_with_timestamp(k1, v1.clone(), time);
        let row_location1 = storage.write_row(&row_to_write).unwrap();

        let k2: Vec<u8> = "key2".into();
        let v2: Vec<u8> = "value2".into();
        let row_to_write: RowToWrite<Vec<u8>, Vec<u8>> =
            RowToWrite::new_with_timestamp(k2, v2.clone(), time + 1);
        let row_location2 = storage.write_row(&row_to_write).unwrap();

        assert!(storage
            .read_value(row_location1.row_offset)
            .unwrap()
            .is_none());
        // v2 still valid
        assert_eq!(
            v2,
            *storage
                .read_value(row_location2.row_offset)
                .unwrap()
                .unwrap()
        );

        // move clock, let v2 expire
        clock.set(time + 1);
        assert!(storage
            .read_value(row_location2.row_offset)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_write_overflow() {
        let mut storage = get_file_storage(get_options(2));

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<Vec<u8>, Vec<u8>> = RowToWrite::new(k1, v1.clone());
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
            let row_to_write: RowToWrite<Vec<u8>, Vec<u8>> = RowToWrite::new(k1, v1.clone());
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
        let row_to_write: RowToWrite<&[u8], Vec<u8>> = RowToWrite::new(&k1, v1.clone());
        storage.write_row(&row_to_write).unwrap();

        let k2: Vec<u8> = "key2".into();
        let v2: Vec<u8> = "value2".into();
        let row_to_write: RowToWrite<&[u8], Vec<u8>> = RowToWrite::new(&k2, v2.clone());
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
        let time = 1000;
        let clock = Arc::new(DebugClock::new(time));
        let mut storage = get_file_storage(get_options(1024).debug_clock(clock.clone()));

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<&[u8], Vec<u8>> =
            RowToWrite::new_with_timestamp(&k1, v1.clone(), time + 1);
        storage.write_row(&row_to_write).unwrap();

        let k2: Vec<u8> = "key2".into();
        let v2: Vec<u8> = "value2".into();
        let row_to_write: RowToWrite<&[u8], Vec<u8>> =
            RowToWrite::new_with_timestamp(&k2, v2.clone(), time);
        storage.write_row(&row_to_write).unwrap();

        storage.rewind().unwrap();

        let r = storage.read_next_row().unwrap().unwrap();
        assert_eq!(k1, r.key);
        assert_eq!(v1, r.value.value);
        assert_eq!(time + 1, r.value.expire_timestamp);
        let r = storage.read_next_row().unwrap().unwrap();
        assert_eq!(k2, r.key);
        assert!(r.value.value.is_empty());
        assert_eq!(time, r.value.expire_timestamp);
        assert!(storage.read_next_row().unwrap().is_none());

        // move clock, no valid value
        clock.set(time + 1);
        storage.rewind().unwrap();
        let r = storage.read_next_row().unwrap().unwrap();
        assert_eq!(k1, r.key);
        assert!(r.value.is_empty());
        assert_eq!(time + 1, r.value.expire_timestamp);
        let r = storage.read_next_row().unwrap().unwrap();
        assert_eq!(k2, r.key);
        assert!(r.value.value.is_empty());
        assert_eq!(time, r.value.expire_timestamp);
        assert!(storage.read_next_row().unwrap().is_none());
    }

    #[test]
    fn test_rewind() {
        let mut storage = get_file_storage(get_options(1024));

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<Vec<u8>, Vec<u8>> = RowToWrite::new(k1, v1.clone());
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
