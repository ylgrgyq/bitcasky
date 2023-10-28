use dashmap::{
    iter::{Iter, OwningIter},
    mapref::{multiple::RefMulti, one::Ref},
    DashMap,
};

use crate::{
    database::{formatter::Formatter, Database, RowLocation},
    error::BitcaskResult,
};

#[derive(Clone, Debug)]
pub struct KeyDir {
    index: DashMap<Vec<u8>, RowLocation>,
}

impl KeyDir {
    pub fn new_empty_key_dir() -> KeyDir {
        let index = DashMap::new();
        KeyDir { index }
    }

    pub fn new<F: Formatter + Copy + 'static>(database: &Database<F>) -> BitcaskResult<KeyDir> {
        let index = DashMap::new();
        let kd = KeyDir { index };
        for ret in database.recovery_iter()? {
            let item = ret?;
            if item.is_tombstone {
                kd.delete(&item.key);
                continue;
            }

            kd.put(item.key, item.row_location);
        }
        Ok(kd)
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
