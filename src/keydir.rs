use std::time::{Duration, Instant};

use dashmap::{
    iter::{Iter, OwningIter},
    mapref::{multiple::RefMulti, one::Ref},
    DashMap,
};

use crate::error::BitcaskResult;
use bitcasky_database::{Database, RowLocation};

#[derive(Debug)]
pub struct KeyDirTelemetry {
    pub number_of_keys: usize,
    pub recovery_duration: Duration,
}

#[derive(Clone, Debug)]
pub struct KeyDir {
    index: DashMap<Vec<u8>, RowLocation>,
    recovery_duration: Duration,
}

impl KeyDir {
    pub fn new_empty_key_dir() -> KeyDir {
        let index = DashMap::new();
        KeyDir {
            index,
            recovery_duration: Duration::ZERO,
        }
    }

    pub fn new(database: &Database) -> BitcaskResult<KeyDir> {
        let index = DashMap::new();
        let start = Instant::now();
        for ret in database.recovery_iter()? {
            let item = ret?;
            if item.invalid {
                index.remove(&item.key);
                continue;
            }

            index.insert(item.key, item.row_location);
        }
        Ok(KeyDir {
            index,
            recovery_duration: start.elapsed(),
        })
    }

    pub fn put(&self, key: Vec<u8>, value: RowLocation) {
        self.index.insert(key, value);
    }

    pub fn checked_put(&self, key: Vec<u8>, value: RowLocation) {
        let r = self.index.get(&key);
        if let Some(pos) = r {
            let old_pos: RowLocation = *(pos);
            // key was written again during merge
            if value.storage_id < old_pos.storage_id {
                return;
            }
        }
        self.index.insert(key, value);
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<Ref<Vec<u8>, RowLocation>> {
        self.index.get(key)
    }

    pub fn contains_key(&self, key: &Vec<u8>) -> bool {
        self.index.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn iter(&self) -> KeyDirIterator {
        KeyDirIterator {
            iter: self.index.iter(),
        }
    }

    pub fn into_iter(self) -> IntoKeyDirIterator {
        IntoKeyDirIterator {
            iter: self.index.into_iter(),
        }
    }

    pub fn delete(&self, key: &Vec<u8>) -> Option<(Vec<u8>, RowLocation)> {
        self.index.remove(key)
    }

    pub fn clear(&self) {
        self.index.clear();
    }

    pub fn get_telemetry_data(&self) -> KeyDirTelemetry {
        KeyDirTelemetry {
            number_of_keys: self.len(),
            recovery_duration: self.recovery_duration,
        }
    }
}

pub struct KeyDirIterator<'a> {
    iter: Iter<'a, Vec<u8>, RowLocation>,
}

impl<'a> Iterator for KeyDirIterator<'a> {
    type Item = RefMulti<'a, Vec<u8>, RowLocation>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub struct IntoKeyDirIterator {
    iter: OwningIter<Vec<u8>, RowLocation>,
}

impl Iterator for IntoKeyDirIterator {
    type Item = (Vec<u8>, RowLocation);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}
