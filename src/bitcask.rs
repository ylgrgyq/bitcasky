use std::{error, path::Path};

use crate::database::{DataBaseOptions, Database, Row};
use crate::keydir::KeyDir;

pub struct Bitcask {
    keydir: KeyDir,
    database: Database,
    options: BitcaskOptions,
}

#[derive(Debug, Clone)]
pub struct BitcaskOptions {
    database_options: DataBaseOptions,
}

impl Bitcask {
    pub fn open(
        &mut self,
        options: BitcaskOptions,
        directory: &Path,
    ) -> Result<Bitcask, Box<dyn error::Error>> {
        let database = Database::open(directory, options.database_options.clone()).unwrap();
        Ok(Bitcask {
            keydir: KeyDir::new(),
            database,
            options,
        })
    }
    pub fn put(&mut self, key: String, value: String) {
        let row = Row::new(key, value);
        self.database.write_row(row);
    }

    pub fn get(&self, key: String) {}
    pub fn delete(&self, key: String) {}
    pub fn close(&self) {}
}
