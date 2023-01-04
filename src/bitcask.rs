use std::{
    error,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::database::{Database, Row};
use crate::keydir::KeyDir;

struct Bitcask {
    keydir: KeyDir,
    database: Database,
}

impl Bitcask {
    fn open(&mut self, directory: &Path) -> Result<Bitcask, Box<dyn error::Error>> {
        let database = Database::open(directory).unwrap();
        Ok(Bitcask {
            keydir: KeyDir::new(),
            database,
        })
    }
    fn put(&mut self, key: String, value: String) {
        let row = Row::new(key, value);
        self.database.write_row(row);
    }

    fn get(&self, key: String) {}
    fn delete(&self, key: String) {}
    fn close(&self) {}
}
