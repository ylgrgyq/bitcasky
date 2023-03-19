use base64::{engine::general_purpose, Engine};
use dashmap::{
    iter::Iter,
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
    pub fn new(database: &Database) -> BitcaskResult<KeyDir> {
        let index = DashMap::new();
        for ret in database.iter()? {
            let item = ret?;
            if is_tombstone(&item.value) {
                index.remove(&item.key);
                continue;
            }
            let r = index.get(&item.key);
            if r.is_some() {
                let pos: RowPosition = *(r.unwrap());
                if pos.tstmp > item.row_position.tstmp {
                    let k = general_purpose::STANDARD.encode(&item.key);
                    warn!(target: "KeyDir", "Found old version value for key in Base64: {} during recovery. Known version: {}, found version: {}", k, pos.tstmp, item.row_position.tstmp);
                    continue;
                } else if pos.tstmp == item.row_position.tstmp {
                    let k = general_purpose::STANDARD.encode(&item.key);
                    info!(target: "KeyDir", "Found known version value for key in Base64: {} during recovery. Known version: {}", k, pos.tstmp);
                }
            }
            index.insert(item.key, item.row_position);
        }
        return Ok(KeyDir { index });
    }

    pub fn put(&self, key: Vec<u8>, value: RowPosition) {
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
