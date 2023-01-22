use dashmap::{mapref::one::Ref, DashMap};

use crate::{
    database::{Database, ValueEntry},
    error::BitcaskResult,
};

pub struct KeyDir {
    index: DashMap<Vec<u8>, ValueEntry>,
}

impl KeyDir {
    pub fn new(database: &Database) -> BitcaskResult<KeyDir> {
        let index = DashMap::new();
        for (k, v) in database.iter()? {
            index.insert(k, v);
        }
        return Ok(KeyDir {
            index: DashMap::new(),
        });
    }

    pub fn put(&self, key: Vec<u8>, value: ValueEntry) {
        self.index.insert(key, value);
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<Ref<Vec<u8>, ValueEntry>> {
        self.index.get(key)
    }

    pub fn delete(&self, key: &Vec<u8>) -> Option<(Vec<u8>, ValueEntry)> {
        self.index.remove(key)
    }
}
