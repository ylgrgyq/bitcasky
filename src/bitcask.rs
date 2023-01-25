use std::path::Path;

use crate::database::{DataBaseOptions, Database, Row};
use crate::error::BitcaskResult;
use crate::keydir::KeyDir;
use crate::utils::{is_tombstone, TOMBSTONE_VALUE};

pub struct Bitcask {
    keydir: KeyDir,
    database: Database,
    options: BitcaskOptions,
}

#[derive(Debug, Clone, Copy)]
pub struct BitcaskOptions {
    database_options: DataBaseOptions,
}

impl Bitcask {
    pub fn open(directory: &Path, options: BitcaskOptions) -> BitcaskResult<Bitcask> {
        let database = Database::open(directory, options.database_options)?;
        let keydir = KeyDir::new(&database)?;
        Ok(Bitcask {
            keydir,
            database,
            options,
        })
    }

    pub fn put(&self, key: Vec<u8>, value: &[u8]) -> BitcaskResult<()> {
        let row = Row::new(&key, value);
        let ret = self.database.write_row(row)?;
        self.keydir.put(key, ret);
        Ok(())
    }

    pub fn get(&self, key: &Vec<u8>) -> BitcaskResult<Option<Vec<u8>>> {
        match self.keydir.get(key) {
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

    pub fn delete(&self, key: &Vec<u8>) -> BitcaskResult<()> {
        let row = Row::new(&key, TOMBSTONE_VALUE.as_bytes());
        self.database.write_row(row)?;
        self.keydir.delete(&key);
        Ok(())
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
        let bc = Bitcask::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
        bc.put("k1".into(), "value1".as_bytes()).unwrap();
        bc.put("k2".into(), "value2".as_bytes()).unwrap();
        bc.put("k3".into(), "value3".as_bytes()).unwrap();
        bc.put("k1".into(), "value4".as_bytes()).unwrap();

        assert_eq!(bc.get(&"k1".into()).unwrap().unwrap(), "value4".as_bytes());
        assert_eq!(bc.get(&"k2".into()).unwrap().unwrap(), "value2".as_bytes());
        assert_eq!(bc.get(&"k3".into()).unwrap().unwrap(), "value3".as_bytes());
    }

    #[test]
    fn test_recovery() {
        let dir = tempfile::tempdir().unwrap();
        {
            let bc = Bitcask::open(
                &dir.path(),
                BitcaskOptions {
                    database_options: DataBaseOptions { max_file_size: 100 },
                },
            )
            .unwrap();
            bc.put("k1".into(), "value1_value1_value1".as_bytes())
                .unwrap();
            bc.put("k2".into(), "value2_value2_value2".as_bytes())
                .unwrap();
            bc.put("k3".into(), "value3_value3_value3".as_bytes())
                .unwrap();
            bc.put("k1".into(), "value4_value4_value4".as_bytes())
                .unwrap();
        }
        let bc = Bitcask::open(
            &dir.path(),
            BitcaskOptions {
                database_options: DataBaseOptions { max_file_size: 100 },
            },
        )
        .unwrap();
        assert_eq!(
            bc.get(&"k1".into()).unwrap().unwrap(),
            "value4_value4_value4".as_bytes()
        );
        assert_eq!(
            bc.get(&"k2".into()).unwrap().unwrap(),
            "value2_value2_value2".as_bytes()
        );
        assert_eq!(
            bc.get(&"k3".into()).unwrap().unwrap(),
            "value3_value3_value3".as_bytes()
        );
    }

    #[test]
    fn test_delete() {
        let dir = tempfile::tempdir().unwrap();
        let bc = Bitcask::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
        bc.put("k1".into(), "value1".as_bytes()).unwrap();
        bc.put("k2".into(), "value2".as_bytes()).unwrap();
        bc.put("k3".into(), "value3".as_bytes()).unwrap();
        bc.put("k1".into(), "value4".as_bytes()).unwrap();

        bc.delete(&"k1".into()).unwrap();
        assert_eq!(bc.get(&"k1".into()).unwrap(), None);

        bc.delete(&"k2".into()).unwrap();
        assert_eq!(bc.get(&"k2".into()).unwrap(), None);

        bc.delete(&"k3".into()).unwrap();
        assert_eq!(bc.get(&"k3".into()).unwrap(), None);
    }
}
