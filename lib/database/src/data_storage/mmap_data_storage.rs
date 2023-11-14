use std::{
    fs::File,
    path::{Path, PathBuf},
};

use common::{formatter::BitcaskFormatter, storage_id::StorageId};

use crate::DataStorageOptions;

use super::{file_data_storage::FileDataStorage, DataStorageReader, DataStorageWriter, Result};

#[derive(Debug)]
pub struct MmapDataStorage {
    database_dir: PathBuf,
    data_file: File,
    pub storage_id: StorageId,
    capacity: u64,
    options: DataStorageOptions,
    formatter: BitcaskFormatter,
}

impl MmapDataStorage {
    pub fn new<P: AsRef<Path>>(
        database_dir: P,
        storage_id: StorageId,
        data_file: File,
        capacity: u64,
        formatter: BitcaskFormatter,
        options: DataStorageOptions,
    ) -> Result<Self> {
        todo!()
    }
}

impl DataStorageWriter for MmapDataStorage {
    fn write_row<V: std::ops::Deref<Target = [u8]>>(
        &mut self,
        row: &common::formatter::RowToWrite<V>,
    ) -> super::Result<crate::RowLocation> {
        todo!()
    }

    fn transit_to_readonly(self) -> super::Result<super::DataStorage> {
        todo!()
    }

    fn flush(&mut self) -> super::Result<()> {
        todo!()
    }
}

impl DataStorageReader for MmapDataStorage {
    fn storage_size(&self) -> usize {
        todo!()
    }

    fn read_value(
        &mut self,
        row_offset: u64,
    ) -> super::Result<crate::TimedValue<crate::common::Value>> {
        todo!()
    }

    fn read_next_row(&mut self) -> super::Result<Option<crate::common::RowToRead>> {
        todo!()
    }
}
