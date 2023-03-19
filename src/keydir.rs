use dashmap::{
    iter::Iter,
    mapref::{multiple::RefMulti, one::Ref},
    DashMap,
};

use crate::{
    database::{Database, RowPosition},
    error::BitcaskResult,
    utils::is_tombstone,
};

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
