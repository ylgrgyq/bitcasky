use dashmap::{mapref::one::Ref, DashMap};

use crate::{
    database::{Database, ValuePosition},
    error::BitcaskResult,
};

pub struct KeyDir {
    index: DashMap<Vec<u8>, ValuePosition>,
}

impl KeyDir {
    pub fn new(database: &Database) -> BitcaskResult<KeyDir> {
        let index = DashMap::new();
        for ret in database.iter()? {
            let (k, v) = ret?;
            index.insert(k, v);
        }
        return Ok(KeyDir { index });
    }

    pub fn put(&self, key: Vec<u8>, value: ValuePosition) {
        self.index.insert(key, value);
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<Ref<Vec<u8>, ValuePosition>> {
        self.index.get(key)
    }

    pub fn delete(&self, key: &Vec<u8>) -> Option<(Vec<u8>, ValuePosition)> {
        self.index.remove(key)
    }
}
