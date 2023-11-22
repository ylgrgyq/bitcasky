use std::{
    fs::File,
    ops::Deref,
    path::{Path, PathBuf},
    ptr,
};

use bytes::Bytes;
use common::{
    formatter::{BitcaskFormatter, Formatter, RowMeta, RowToWrite},
    storage_id::StorageId,
};
use memmap2::{MmapMut, MmapOptions};

use crate::{common::Value, DataStorageError, DataStorageOptions, RowLocation, TimedValue};

use super::{DataStorageReader, DataStorageWriter, Result};

#[derive(Debug)]
pub struct MmapDataStorage {
    database_dir: PathBuf,
    data_file: File,
    pub storage_id: StorageId,
    write_offset: usize,
    capacity: usize,
    options: DataStorageOptions,
    formatter: BitcaskFormatter,
    map_view: MmapMut,
}

impl MmapDataStorage {
    pub fn new<P: AsRef<Path>>(
        database_dir: P,
        storage_id: StorageId,
        data_file: File,
        write_offset: usize,
        capacity: usize,
        formatter: BitcaskFormatter,
        options: DataStorageOptions,
    ) -> Result<Self> {
        let mmap = unsafe {
            MmapOptions::new()
                .offset(0)
                .len(capacity)
                .map_mut(&data_file)?
        };

        Ok(MmapDataStorage {
            database_dir: database_dir.as_ref().to_path_buf(),
            data_file,
            storage_id,
            write_offset,
            capacity,
            options,
            formatter,
            map_view: mmap,
        })
    }

    fn check_storage_overflow<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<V>) -> bool {
        let row_size = self.formatter.row_size(row);
        (row_size + self.write_offset) > self.capacity
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

    /// Returns the number of padding bytes to add to a buffer to ensure 8-byte alignment.
    fn padding(len: usize) -> usize {
        4usize.wrapping_sub(len) & 7
    }

    fn do_read_row(&mut self, row_offset: u64) -> Result<(RowMeta, Bytes)> {
        let header_bs = Bytes::copy_from_slice(
            &self.as_slice()
                [row_offset as usize..(row_offset as usize + self.formatter.row_header_size())],
        );

        let header = self.formatter.decode_row_header(header_bs);

        let kv_bs = Bytes::copy_from_slice(
            &self.as_slice()[row_offset as usize + self.formatter.row_header_size()
                ..row_offset as usize
                    + self.formatter.row_header_size()
                    + header.meta.key_size as usize
                    + header.meta.value_size as usize],
        );

        self.formatter.validate_key_value(&header, &kv_bs)?;
        Ok((header.meta, kv_bs))
    }
}

impl DataStorageWriter for MmapDataStorage {
    fn write_row<V: std::ops::Deref<Target = [u8]>>(
        &mut self,
        row: &common::formatter::RowToWrite<V>,
    ) -> super::Result<crate::RowLocation> {
        if self.check_storage_overflow(row) {
            return Err(DataStorageError::StorageOverflow(self.storage_id));
        }

        let data_to_write = self.formatter.encode_row(row);

        let value_offset = self.write_offset;
        MmapDataStorage::copy_memory(&data_to_write, &mut self.as_mut_slice()[value_offset..]);
        self.write_offset += data_to_write.len();

        Ok(RowLocation {
            storage_id: self.storage_id,
            row_offset: value_offset as u64,
        })
    }

    fn transit_to_readonly(self) -> super::Result<super::DataStorage> {
        todo!()
    }

    fn flush(&mut self) -> super::Result<()> {
        Ok(self.map_view.flush_range(0, self.capacity)?)
    }
}

impl DataStorageReader for MmapDataStorage {
    fn read_value(
        &mut self,
        row_offset: u64,
    ) -> super::Result<crate::TimedValue<crate::common::Value>> {
        let (meta, kv_bs) = self.do_read_row(row_offset)?;

        Ok(TimedValue {
            value: Value::VectorBytes(kv_bs.slice(meta.key_size as usize..).into()),
            timestamp: meta.timestamp,
        })
    }

    fn read_next_row(&mut self) -> super::Result<Option<crate::common::RowToRead>> {
        todo!()
    }

    fn seek_to_end(&mut self) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use common::{
        create_file,
        formatter::{initialize_new_file, FILE_HEADER_SIZE},
        fs::FileType,
    };

    use super::*;

    use bitcask_tests::common::get_temporary_directory_path;
    use test_log::test;

    fn get_file_storage(max_size: usize) -> MmapDataStorage {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let formatter = BitcaskFormatter::default();
        let file =
            create_file(&dir, FileType::DataFile, Some(storage_id), &formatter, 512).unwrap();
        let options = DataStorageOptions {
            max_data_file_size: max_size,
            init_data_file_capacity: max_size,
        };
        let meta = file.metadata().unwrap();
        MmapDataStorage::new(
            &dir,
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
    fn test_read_write_mmap_storage() {
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

    // #[test]
    // fn test_write_overflow() {
    //     let mut storage = get_file_storage(2);

    //     let k1: Vec<u8> = "key1".into();
    //     let v1: Vec<u8> = "value1".into();
    //     let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
    //     storage.write_row(&row_to_write).expect_err("overflow");
    // }

    // #[test]
    // fn test_file_storage_read_next_row() {
    //     let mut storage = get_file_storage(1024);

    //     let k1: Vec<u8> = "key1".into();
    //     let v1: Vec<u8> = "value1".into();
    //     let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
    //     storage.write_row(&row_to_write).unwrap();

    //     let k2: Vec<u8> = "key2".into();
    //     let v2: Vec<u8> = "value2".into();
    //     let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k2, v2.clone());
    //     storage.write_row(&row_to_write).unwrap();

    //     let mut storage = storage.transit_to_readonly().unwrap();

    //     let r = storage.read_next_row().unwrap().unwrap();

    //     assert_eq!(k1, r.key);
    //     assert_eq!(v1, r.value);
    //     let r = storage.read_next_row().unwrap().unwrap();
    //     assert_eq!(k2, r.key);
    //     assert_eq!(v2, r.value);
    // }

    // #[test]
    // fn test_transit_storage_to_read_only() {
    //     let mut storage = get_file_storage(1024);

    //     let k1: Vec<u8> = "key1".into();
    //     let v1: Vec<u8> = "value1".into();
    //     let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
    //     storage.write_row(&row_to_write).unwrap();
    //     let mut storage = storage.transit_to_readonly().unwrap();

    //     let k1: Vec<u8> = "key1".into();
    //     let v1: Vec<u8> = "value1".into();
    //     let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
    //     storage
    //         .write_row(&row_to_write)
    //         .expect_err("no write permission");
    // }
}
