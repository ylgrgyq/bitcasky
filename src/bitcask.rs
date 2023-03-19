use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::database::{DataBaseOptions, Database};
use crate::error::{BitcaskError, BitcaskResult};
use crate::file_id::FileIdGenerator;
use crate::file_manager;
use crate::keydir::KeyDir;
use crate::utils::{is_tombstone, TOMBSTONE_VALUE};

#[derive(Debug, Clone, Copy)]
pub struct BitcaskOptions {
    database_options: DataBaseOptions,
    max_key_size: usize,
    max_value_size: usize,
}

#[derive(PartialEq)]
enum FoldStatus {
    Stopped,
    Continue,
}
pub struct FoldResult<T> {
    accumulator: T,
    status: FoldStatus,
}

pub struct Bitcask {
    keydir: RwLock<KeyDir>,
    file_id_generator: Arc<FileIdGenerator>,
    options: BitcaskOptions,
    database: Database,
}

impl Bitcask {
    pub fn open(directory: &Path, options: BitcaskOptions) -> BitcaskResult<Bitcask> {
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let database = Database::open(
            &directory,
            file_id_generator.clone(),
            options.database_options,
        )?;
        let keydir = KeyDir::new(&database)?;
        Ok(Bitcask {
            keydir: RwLock::new(keydir),
            file_id_generator,
            database,
            options,
        })
    }

    pub fn put(&self, key: Vec<u8>, value: &[u8]) -> BitcaskResult<()> {
        if key.len() > self.options.max_key_size {
            return Err(BitcaskError::InvalidParameter(
                "key".into(),
                "key size overflow".into(),
            ));
        }
        if value.len() > self.options.max_value_size {
            return Err(BitcaskError::InvalidParameter(
                "value".into(),
                "values size overflow".into(),
            ));
        }

        let kd = self.keydir.write().unwrap();
        let ret = self.database.write(&key, value)?;
        kd.put(key, ret);
        Ok(())
    }

    pub fn get(&self, key: &Vec<u8>) -> BitcaskResult<Option<Vec<u8>>> {
        let row_pos = {
            self.keydir
                .read()
                .unwrap()
                .get(key)
                .and_then(|r| Some(r.value().clone()))
        };

        match row_pos {
            Some(e) => {
                let v = self.database.read_value(&e)?;
                if is_tombstone(&v) {
                    return Ok(None);
                }
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }

    pub fn foreach_key<T>(&self, func: fn(key: &Vec<u8>) -> FoldResult<T>) {
        let kd = self.keydir.read().unwrap();
        for r in kd.iter() {
            if func(r.key()).status == FoldStatus::Stopped {
                break;
            }
        }
    }

    pub fn delete(&self, key: &Vec<u8>) -> BitcaskResult<()> {
        let kd = self.keydir.write().unwrap();
        self.database.write(key, TOMBSTONE_VALUE.as_bytes())?;
        kd.delete(&key);
        Ok(())
    }

    pub fn merge(&self) -> BitcaskResult<()> {
        let dir_path = file_manager::create_merge_file_dir(self.database.get_database_dir())?;
        let kd = self.flush_writing_file()?;
        let merge_db = Database::open(
            &dir_path,
            self.file_id_generator.clone(),
            self.options.database_options,
        )?;

        for r in kd.iter() {
            let k = r.key();
            let v = self.database.read_value(r.value())?;
            if !is_tombstone(&v) {
                merge_db.write_with_timestamp(k, &v, r.value().tstmp)?;
            }
        }

        Ok(())
    }

    fn flush_writing_file(&self) -> BitcaskResult<KeyDir> {
        // stop writing and switch the writing file to stable files
        let _kd = self.keydir.write().unwrap();
        self.database.flush_writing_file()?;
        Ok(_kd.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;
    const DEFAULT_OPTIONS: BitcaskOptions = BitcaskOptions {
        database_options: DataBaseOptions {
            max_file_size: Some(11),
        },
        max_key_size: 1024,
        max_value_size: 1024,
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
                    database_options: DataBaseOptions {
                        max_file_size: Some(100),
                    },
                    max_key_size: 1024,
                    max_value_size: 1024,
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
            bc.delete(&"k2".into()).unwrap();
        }
        let bc = Bitcask::open(
            &dir.path(),
            BitcaskOptions {
                database_options: DataBaseOptions {
                    max_file_size: Some(100),
                },
                max_key_size: 1024,
                max_value_size: 1024,
            },
        )
        .unwrap();
        assert_eq!(
            bc.get(&"k1".into()).unwrap().unwrap(),
            "value4_value4_value4".as_bytes()
        );
        assert_eq!(bc.get(&"k2".into()).unwrap(), None,);
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
