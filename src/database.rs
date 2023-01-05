use std::{
    arch::x86_64::__get_cpuid_max,
    collections::HashMap,
    error,
    fmt::format,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crc::{Crc, CRC_32_CKSUM};
use walkdir::{DirEntry, WalkDir};

use crate::file_manager::{self, create_database_file, open_database_files, DataBaseFile};

struct Item {
    crc32: u32,
}

pub struct Row {
    crc: u32,
    tstamp: u64,
    key_size: u64,
    value_size: u64,
    key: String,
    value: String,
}

impl Row {
    pub fn new(key: String, value: String) -> Row {
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
        ck.update(key.as_bytes());
        ck.update(value.as_bytes());
        Row {
            crc: ck.finalize(),
            tstamp: now,
            key_size,
            value_size,
            key,
            value,
        }
    }
}

pub struct Database {
    writing_file_id: u32,
    database_files: HashMap<u32, File>,
    database_dir: PathBuf,
    current_offset: u64,
    next_file_id: u32,
}

pub struct DataBaseOptions {
    max_file_size: u32,
}

const MAX_DATABASE_FILE_SIZE: u32 = 100 * 1024;

impl Database {
    pub fn open(
        directory: &Path,
        options: &DataBaseOptions,
    ) -> Result<Database, Box<dyn error::Error>> {
        let database_dir = directory.join("database");
        let database_files = open_database_files(&database_dir)?;
        let writing_file_id = database_files.keys().max().unwrap_or(&0) + 1;
        database_files.insert(
            writing_file_id,
            create_database_file(&database_dir, writing_file_id)?,
        );
        Ok(Database {
            writing_file_id,
            database_dir,
            database_files,
            current_offset: 0,
            next_file_id: 0,
        })
    }

    pub fn write_row(&mut self, row: Row) -> Result<u64, Box<dyn error::Error>> {
        let data_file = self.database_files.get(&self.writing_file_id).unwrap();

        data_file.write_all(&row.crc.to_be_bytes())?;
        data_file.write_all(&row.tstamp.to_be_bytes())?;
        data_file.write_all(&row.key_size.to_be_bytes()).unwrap();
        data_file.write_all(&row.value_size.to_be_bytes()).unwrap();
        self.current_offset += 28;

        let buf = row.key.as_bytes();
        data_file.write_all(&buf).zunwrap();
        self.current_offset += buf.len() as u64;
        let value_offset = self.current_offset;

        let buf = row.value.as_bytes();
        data_file.write_all(&buf).unwrap();
        self.current_offset += buf.len() as u64;

        data_file.flush().unwrap();
        Ok(value_offset)
    }

    pub fn read_value(
        &mut self,
        value_offset: u64,
        size: u64,
    ) -> Result<String, Box<dyn error::Error>> {
        self.data_file.seek(SeekFrom::Start(value_offset)).unwrap();
        let mut ret = String::new();
        let mut buf = vec![0; size as usize];
        let mut n = self.data_file.read(&mut buf).unwrap();
        while n > 0 {
            n = self.data_file.read(&mut buf).unwrap();
            ret.push_str(String::from_utf8(buf.to_vec()).unwrap().as_str());
        }
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_open_when_directory_not_exists() {
        let path = Path::new("/tmp/bitcask");
        if path.exists() {
            std::fs::remove_dir_all(path).unwrap();
        }
        // assert_eq!(Database::open(&path).is_err(), true);
    }

    #[test]
    fn test_race() {
        let path = Path::new("/tmp/bitcask");
        std::fs::create_dir_all(path).unwrap();
        // let mut db = Database::open(&path).unwrap();
        // let row = Row::new("Key".into(), "value".into());
        // let offset = db.write_row(row).unwrap();
        // assert_eq!(
        //     db.read_value(offset, "value".len() as u64).unwrap(),
        //     "value"
        // );
    }
}
