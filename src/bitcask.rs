use std::{error, path::Path};

use crate::database::{DataBaseOptions, Database, Row};
use crate::keydir::KeyDir;
use crate::utils::{is_tombstone, TOMBSTONE_VALUE};

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
        directory: &Path,
        options: BitcaskOptions,
    ) -> Result<Bitcask, Box<dyn error::Error>> {
        let database = Database::open(directory, options.database_options.clone()).unwrap();
        Ok(Bitcask {
            keydir: KeyDir::new(),
            database,
            options,
        })
    }
    pub fn put(&mut self, key: String, value: String) -> Result<(), Box<dyn error::Error>> {
        let row = Row::new(key.clone(), value.clone());
        let ret = self.database.write_row(row)?;
        if is_tombstone(&value) {
            self.keydir.delete(&key);
        } else {
            self.keydir.put(key, ret);
        }
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>, Box<dyn error::Error>> {
        match self.keydir.get(&key) {
            Some(e) => {
                let v = self
                    .database
                    .read_value(e.file_id, e.value_offset, e.value_size)?;
                if is_tombstone(&v) {
                    return Ok(None);
                }
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }

    pub fn delete(&mut self, key: String) -> Result<(), Box<dyn error::Error>> {
        self.put(key, TOMBSTONE_VALUE.into())
    }
    pub fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    const DEFAULT_OPTIONS: BitcaskOptions = BitcaskOptions {
        database_options: DataBaseOptions { max_file_size: 11 },
    };

    #[test]
    fn test_read_write_writing_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut bc = Bitcask::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
        bc.put("k1".into(), "value1".into()).unwrap();
        bc.put("k2".into(), "value2".into()).unwrap();
        bc.put("k3".into(), "value3".into()).unwrap();
        bc.put("k1".into(), "value4".into()).unwrap();

        assert_eq!(bc.get("k1".into()).unwrap().unwrap(), "value4");
        assert_eq!(bc.get("k2".into()).unwrap().unwrap(), "value2");
        assert_eq!(bc.get("k3".into()).unwrap().unwrap(), "value3");
    }

    #[test]
    fn test_delete() {
        let dir = tempfile::tempdir().unwrap();
        let mut bc = Bitcask::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
        bc.put("k1".into(), "value1".into()).unwrap();
        bc.put("k2".into(), "value2".into()).unwrap();
        bc.put("k3".into(), "value3".into()).unwrap();
        bc.put("k1".into(), "value4".into()).unwrap();

        bc.delete("k1".into()).unwrap();
        assert_eq!(bc.get("k1".into()).unwrap(), None);

        bc.delete("k2".into()).unwrap();
        assert_eq!(bc.get("k2".into()).unwrap(), None);

        bc.delete("k3".into()).unwrap();
        assert_eq!(bc.get("k3".into()).unwrap(), None);
    }
}
