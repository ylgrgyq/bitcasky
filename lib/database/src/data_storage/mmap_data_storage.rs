use std::{
    cell::UnsafeCell,
    fs::File,
    ops::Deref,
    path::{Path, PathBuf},
    ptr,
    sync::Arc,
};

use bytes::Bytes;
use common::{
    formatter::{BitcaskFormatter, Formatter, RowMeta, RowToWrite},
    storage_id::StorageId,
};
use memmap2::{MmapMut, MmapOptions};
use parking_lot::Mutex;

use crate::{common::Value, DataStorageError, DataStorageOptions, RowLocation, TimedValue};

use super::{file_data_storage::FileDataStorage, DataStorageReader, DataStorageWriter, Result};

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
        write_offset: u64,
        capacity: u64,
        formatter: BitcaskFormatter,
        options: DataStorageOptions,
    ) -> Result<Self> {
        let mmap = unsafe {
            MmapOptions::new()
                .offset(0)
                .len(capacity as usize)
                .map_mut(&data_file)?
        };
        // let map_view = Arc::new(Mutex::new(UnsafeCell::new(mmap)));

        Ok(MmapDataStorage {
            database_dir: database_dir.as_ref().to_path_buf(),
            data_file,
            storage_id,
            write_offset: write_offset as usize,
            capacity: capacity as usize,
            options,
            formatter,
            map_view: mmap,
        })
    }

    fn check_storage_overflow<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<V>) -> bool {
        let row_size = self.formatter.row_size(row);
        (row_size + self.write_offset as usize) > self.options.max_data_file_size
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { &mut self.map_view[0..self.capacity] }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { &self.map_view[0..self.capacity] }
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
        let mut header_buf = vec![0; self.formatter.row_header_size()];

        self.as_slice()[row_offset..row_offset + self.formatter.row_header_size()];
        self.data_file.read_exact(&mut header_buf)?;
        let header_bs = Bytes::from(header_buf);

        let header = self.formatter.decode_row_header(header_bs);

        let mut kv_buf = vec![0; (header.meta.key_size + header.meta.value_size) as usize];
        self.data_file.read_exact(&mut kv_buf)?;
        let kv_bs = Bytes::from(kv_buf);

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
        // self.data_file.seek(SeekFrom::Start(row_offset))?;

        let (meta, kv_bs) = self.do_read_row(row_offset)?;

        Ok(TimedValue {
            value: Value::VectorBytes(kv_bs.slice(meta.key_size as usize..).into()),
            timestamp: meta.timestamp,
        })
    }

    fn read_next_row(&mut self) -> super::Result<Option<crate::common::RowToRead>> {
        todo!()
    }
}
