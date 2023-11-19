use bytes::Bytes;

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    ops::Deref,
    path::{Path, PathBuf},
};

use common::{
    formatter::{BitcaskFormatter, Formatter, RowMeta, RowToWrite, FILE_HEADER_SIZE},
    fs::FileType,
    storage_id::StorageId,
};

use crate::{
    common::{RowToRead, Value},
    DataStorageError,
};

use super::{
    DataStorage, DataStorageOptions, DataStorageReader, DataStorageWriter, Result, RowLocation,
    TimedValue,
};

#[derive(Debug)]
pub struct FileDataStorage {
    database_dir: PathBuf,
    data_file: File,
    pub storage_id: StorageId,
    write_offset: u64,
    capacity: u64,
    options: DataStorageOptions,
    formatter: BitcaskFormatter,
}

impl FileDataStorage {
    pub fn new<P: AsRef<Path>>(
        database_dir: P,
        storage_id: StorageId,
        data_file: File,
        write_offset: u64,
        capacity: u64,
        formatter: BitcaskFormatter,
        options: DataStorageOptions,
    ) -> Result<Self> {
        Ok(FileDataStorage {
            database_dir: database_dir.as_ref().to_path_buf(),
            data_file,
            storage_id,
            write_offset,
            capacity,
            options,
            formatter,
        })
    }

    fn do_read_row(&mut self) -> Result<(RowMeta, Bytes)> {
        let mut header_buf = vec![0; self.formatter.row_header_size()];
        self.data_file.read_exact(&mut header_buf)?;
        let header_bs = Bytes::from(header_buf);

        let header = self.formatter.decode_row_header(header_bs);

        let mut kv_buf = vec![0; (header.meta.key_size + header.meta.value_size) as usize];
        self.data_file.read_exact(&mut kv_buf)?;
        let kv_bs = Bytes::from(kv_buf);

        self.formatter.validate_key_value(&header, &kv_bs)?;
        Ok((header.meta, kv_bs))
    }

    pub fn check_storage_overflow<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<V>) -> bool {
        let row_size = self.formatter.row_size(row);
        (row_size + self.write_offset as usize) > self.options.max_data_file_size
    }
}

impl DataStorageWriter for FileDataStorage {
    fn write_row<V: Deref<Target = [u8]>>(&mut self, row: &RowToWrite<V>) -> Result<RowLocation> {
        if self.check_storage_overflow(row) {
            return Err(DataStorageError::StorageOverflow(self.storage_id));
        }

        let data_to_write = self.formatter.encode_row(row);
        let value_offset = self.write_offset;
        self.data_file
            .write_all(&data_to_write)
            .map_err(|e| DataStorageError::WriteRowFailed(self.storage_id, e.to_string()))?;
        self.write_offset += data_to_write.len() as u64;

        Ok(RowLocation {
            storage_id: self.storage_id,
            row_offset: value_offset,
        })
    }

    fn transit_to_readonly(mut self) -> Result<DataStorage> {
        self.data_file.flush()?;

        let path = FileType::DataFile.get_path(&self.database_dir, Some(self.storage_id));
        let mut perms = std::fs::metadata(&path)?.permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(path, perms)?;
        self.data_file
            .seek(SeekFrom::Start(FILE_HEADER_SIZE as u64))?;
        let meta = self.data_file.metadata()?;
        let file_size = meta.len();
        DataStorage::open_by_file(
            &self.database_dir,
            self.storage_id,
            self.data_file,
            meta,
            file_size,
            self.formatter,
            self.options,
        )
    }

    fn flush(&mut self) -> Result<()> {
        Ok(self.data_file.flush()?)
    }
}

impl DataStorageReader for FileDataStorage {
    fn read_value(&mut self, row_offset: u64) -> Result<TimedValue<Value>> {
        self.data_file.seek(SeekFrom::Start(row_offset))?;

        let (meta, kv_bs) = self.do_read_row()?;

        Ok(TimedValue {
            value: Value::VectorBytes(kv_bs.slice(meta.key_size as usize..).into()),
            timestamp: meta.timestamp,
        })
    }

    fn read_next_row(&mut self) -> Result<Option<RowToRead>> {
        let value_offset = self.data_file.stream_position()?;
        if value_offset >= self.capacity {
            return Ok(None);
        }

        let (meta, kv_bs) = self.do_read_row()?;

        Ok(Some(RowToRead {
            key: kv_bs.slice(0..meta.key_size as usize).into(),
            value: kv_bs.slice(meta.key_size as usize..).into(),
            row_location: RowLocation {
                storage_id: self.storage_id,
                row_offset: value_offset,
            },
            timestamp: meta.timestamp,
        }))
    }
}

#[cfg(test)]
mod tests {
    use common::{formatter::initialize_new_file, fs::create_file};

    use super::*;

    use bitcask_tests::common::get_temporary_directory_path;
    use test_log::test;

    fn get_file_storage(max_size: usize) -> DataStorage {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let mut file = create_file(&dir, FileType::DataFile, Some(storage_id)).unwrap();
        initialize_new_file(&mut file, BitcaskFormatter::default().version()).unwrap();
        let options = DataStorageOptions {
            max_data_file_size: max_size,
            init_data_file_capacity: max_size,
        };
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

        let mut storage = storage.transit_to_readonly().unwrap();

        let r = storage.read_next_row().unwrap().unwrap();

        assert_eq!(k1, r.key);
        assert_eq!(v1, r.value);
        let r = storage.read_next_row().unwrap().unwrap();
        assert_eq!(k2, r.key);
        assert_eq!(v2, r.value);
    }

    #[test]
    fn test_transit_storage_to_read_only() {
        let mut storage = get_file_storage(1024);

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
        storage.write_row(&row_to_write).unwrap();
        let mut storage = storage.transit_to_readonly().unwrap();

        let k1: Vec<u8> = "key1".into();
        let v1: Vec<u8> = "value1".into();
        let row_to_write: RowToWrite<'_, Vec<u8>> = RowToWrite::new(&k1, v1.clone());
        storage
            .write_row(&row_to_write)
            .expect_err("no write permission");
    }
}
