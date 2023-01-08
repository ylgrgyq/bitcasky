use std::collections::HashMap;

use crate::database::ValueEntry;

pub struct KeyDir {
    index: HashMap<Vec<u8>, ValueEntry>,
}

impl KeyDir {
    pub fn new() -> KeyDir {
        return KeyDir {
            index: HashMap::new(),
        };
    }

    pub fn put(&mut self, key: Vec<u8>, value: ValueEntry) {
        self.index.insert(key, value);
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<&ValueEntry> {
        self.index.get(key)
    }

    pub fn delete(&mut self, key: &Vec<u8>) -> Option<ValueEntry> {
        self.index.remove(key)
    }
}
