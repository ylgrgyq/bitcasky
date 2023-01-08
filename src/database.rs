use std::{
    collections::HashMap,
    error,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Buf, Bytes, BytesMut};
use crc::{Crc, CRC_32_CKSUM};

use crate::file_manager::{create_database_file, open_stable_database_files};

const CRC_SIZE: usize = 4;
const TSTAMP_SIZE: usize = 8;
const KEY_SIZE_SIZE: usize = 8;
const VALUE_SIZE_SIZE: usize = 8;
const KEY_SIZE_OFFSET: usize = CRC_SIZE + TSTAMP_SIZE;
const VALUE_SIZE_OFFSET: usize = KEY_SIZE_OFFSET + KEY_SIZE_SIZE;
const KEY_OFFSET: usize = CRC_SIZE + TSTAMP_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE;

pub struct Row<'a> {
    crc: u32,
    tstamp: u64,
    key_size: u64,
    value_size: u64,
    key: &'a Vec<u8>,
    value: &'a [u8],
    size: usize,
}

impl<'a> Row<'a> {
    pub fn new(key: &'a Vec<u8>, value: &'a [u8]) -> Row<'a> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        let key_size = key.len() as u64;
        let value_size = value.len() as u64;
        let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut ck = crc32.digest();
        ck.update(&now.to_be_bytes());
        ck.update(&key_size.to_be_bytes());
        ck.update(&value_size.to_be_bytes());
        ck.update(&key);
        ck.update(value);
        Row {
            crc: ck.finalize(),
            tstamp: now,
            key_size,
            value_size,
            key,
            value,
            size: KEY_OFFSET + key_size as usize + value_size as usize,
        }
    }

    fn to_bytes(&self) -> Bytes {
        let mut bs = BytesMut::with_capacity(self.size);
        bs.extend_from_slice(&self.crc.to_be_bytes());
        bs.extend_from_slice(&self.tstamp.to_be_bytes());
        bs.extend_from_slice(&self.key_size.to_be_bytes());
        bs.extend_from_slice(&self.value_size.to_be_bytes());
        bs.extend_from_slice(self.key);
        bs.extend_from_slice(self.value);
        bs.freeze()
    }
}

#[derive(Debug)]
pub struct ValueEntry {
    pub file_id: u32,
    pub value_offset: u64,
    pub value_size: usize,
    pub tstmp: u64,
}

#[derive(Debug)]
struct WritingFile {
    file_id: u32,
    data_file: File,
}

impl WritingFile {
    fn new(database_dir: &Path, file_id: u32) -> Result<WritingFile, Box<dyn error::Error>> {
        let data_file = create_database_file(&database_dir, file_id)?;
        Ok(WritingFile { file_id, data_file })
    }

    fn write_row(&mut self, row: Row) -> Result<ValueEntry, Box<dyn error::Error>> {
        let value_offset = self.data_file.seek(SeekFrom::End(0))?;
        self.data_file.write_all(&*row.to_bytes())?;
        Ok(ValueEntry {
            file_id: self.file_id,
            value_offset,
            value_size: row.size,
            tstmp: row.tstamp,
        })
    }
}

impl Drop for WritingFile {
    fn drop(&mut self) {
        self.data_file.flush();
    }
}

#[derive(Debug, Clone)]
pub struct DataBaseOptions {
    pub max_file_size: u32,
}

#[derive(Debug)]
pub struct Database {
    database_dir: PathBuf,
    writing_file: WritingFile,
    stable_files: HashMap<u32, File>,
    options: DataBaseOptions,
}

const MAX_DATABASE_FILE_SIZE: u32 = 100 * 1024;
const DATABASE_FILE_DIRECTORY: &str = "database";

impl Database {
    pub fn open(
        directory: &Path,
        options: DataBaseOptions,
    ) -> Result<Database, Box<dyn error::Error>> {
        let database_dir = directory.join(DATABASE_FILE_DIRECTORY);
        std::fs::create_dir_all(database_dir.clone())?;
        let stable_files = open_stable_database_files(&database_dir)?;
        let writing_file_id = stable_files.keys().max().unwrap_or(&0) + 1;
        let writing_file = WritingFile::new(&database_dir, writing_file_id)?;
        Ok(Database {
            writing_file,
            database_dir,
            stable_files,
            options,
        })
    }

    pub fn write_row(&mut self, row: Row) -> Result<ValueEntry, Box<dyn error::Error>> {
        self.writing_file.write_row(row)
    }

    pub fn read_value(
        &mut self,
        file_id: u32,
        value_offset: u64,
        size: usize,
    ) -> Result<Vec<u8>, Box<dyn error::Error>> {
        if file_id == self.writing_file.file_id {
            return read_value_from_file(&mut self.writing_file.data_file, value_offset, size);
        }
        let f = self.stable_files.get_mut(&file_id);
        if f.is_none() {
            return Err("file not found".into());
        }
        read_value_from_file(f.unwrap(), value_offset, size)
    }
}

fn read_value_from_file(
    data_file: &mut File,
    value_offset: u64,
    size: usize,
) -> Result<Vec<u8>, Box<dyn error::Error>> {
    data_file.seek(SeekFrom::Start(value_offset))?;
    let mut buf = vec![0; size];
    data_file.read_exact(&mut buf)?;

    let bs = Bytes::from(buf);
    let expected_crc = bs.slice(0..4).get_u32();

    let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
    let mut ck = crc32.digest();
    ck.update(&bs.slice(4..));
    if expected_crc != ck.finalize() {
        return Err("".into());
    }

    let key_size = bs
        .slice(KEY_SIZE_OFFSET..(KEY_SIZE_OFFSET + KEY_SIZE_SIZE))
        .get_u64() as usize;
    let val_size = bs
        .slice(VALUE_SIZE_OFFSET..(VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE))
        .get_u64() as usize;
    let val_offset = KEY_OFFSET + key_size;
    Ok(bs.slice(val_offset..val_offset + val_size).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    const DEFAULT_OPTIONS: DataBaseOptions = DataBaseOptions { max_file_size: 11 };

    #[test]
    fn test_read_write_writing_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Database::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
        let kvs = [
            ("k1", "value1奥森"),
            ("k2", "value2"),
            ("k3", "value3"),
            ("k1", "value4"),
        ];
        let offset_values = kvs
            .into_iter()
            .map(|(k, v)| (db.write_row(Row::new(&k.into(), v.as_bytes())).unwrap(), v))
            .collect::<Vec<(ValueEntry, &str)>>();

        offset_values.iter().for_each(|(ret, value)| {
            assert_eq!(
                db.read_value(ret.file_id, ret.value_offset, ret.value_size)
                    .unwrap(),
                *value.as_bytes()
            );
        });
    }

    #[test]
    fn test_read_write_with_stable_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut offset_values: Vec<(ValueEntry, &str)> = vec![];
        {
            let mut db = Database::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
            let kvs = [("k1", "value1"), ("k2", "value2")];
            offset_values.append(
                &mut kvs
                    .into_iter()
                    .map(|(k, v)| (db.write_row(Row::new(&k.into(), v.as_bytes())).unwrap(), v))
                    .collect::<Vec<(ValueEntry, &str)>>(),
            );
        }
        {
            let mut db = Database::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
            let kvs = [("k3", "hello world"), ("k1", "value4")];
            offset_values.append(
                &mut kvs
                    .into_iter()
                    .map(|(k, v)| (db.write_row(Row::new(&k.into(), v.as_bytes())).unwrap(), v))
                    .collect::<Vec<(ValueEntry, &str)>>(),
            );
        }

        let mut db = Database::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
        offset_values.iter().for_each(|(ret, value)| {
            assert_eq!(
                db.read_value(ret.file_id, ret.value_offset, ret.value_size)
                    .unwrap(),
                *value.as_bytes()
            );
        });
    }
}
