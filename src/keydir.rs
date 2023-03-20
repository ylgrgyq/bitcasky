use base64::{engine::general_purpose, Engine};
use dashmap::{
    iter::{Iter, OwningIter},
    mapref::{multiple::RefMulti, one::Ref},
    DashMap,
};
use log::{info, warn};

use crate::{
    database::{Database, RowPosition},
    error::BitcaskResult,
    utils::is_tombstone,
};

#[derive(Clone)]
pub struct KeyDir {
    index: DashMap<Vec<u8>, RowPosition>,
}

impl KeyDir {
    pub fn new_empty_key_dir() -> KeyDir {
        let index = DashMap::new();
        KeyDir { index }
    }

    pub fn new(database: &Database) -> BitcaskResult<KeyDir> {
        let index = DashMap::new();
        let kd = KeyDir { index };
        for ret in database.iter()? {
            let item = ret?;
            if is_tombstone(&item.value) {
                kd.delete(&item.key);
                continue;
            }
            kd.checked_put(item.key, item.row_position);
        }
        return Ok(kd);
    }

    pub fn put(&self, key: Vec<u8>, value: RowPosition) {
        self.index.insert(key, value);
    }

    pub fn checked_put(&self, key: Vec<u8>, value: RowPosition) {
        let r = self.index.get(&key);
        if r.is_some() {
            let pos: RowPosition = *(r.unwrap());
            if pos.tstmp > value.tstmp {
                let k = general_purpose::STANDARD.encode(&key);
                warn!(target: "KeyDir", "Found old version value for key in Base64: {} during recovery. Known version: {}, found version: {}", k, pos.tstmp, value.tstmp);
                return;
            } else if pos.tstmp == value.tstmp {
                let k = general_purpose::STANDARD.encode(&key);
                info!(target: "KeyDir", "Found known version value for key in Base64: {} during recovery. Known version: {}", k, pos.tstmp);
            }
        }
        self.index.insert(key, value);
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<Ref<Vec<u8>, RowPosition>> {
        self.index.get(key)
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

    pub fn delete(&self, key: &Vec<u8>) -> Option<(Vec<u8>, RowPosition)> {
        self.index.remove(key)
    }
}

pub struct KeyDirIterator<'a> {
    iter: Iter<'a, Vec<u8>, RowPosition>,
}

impl<'a> Iterator for KeyDirIterator<'a> {
    type Item = RefMulti<'a, Vec<u8>, RowPosition>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub struct IntoKeyDirIterator {
    iter: OwningIter<Vec<u8>, RowPosition>,
}

impl Iterator for IntoKeyDirIterator {
    type Item = (Vec<u8>, RowPosition);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}
