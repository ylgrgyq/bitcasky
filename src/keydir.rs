use std::collections::HashMap;

use crate::database::ValueEntry;

pub struct KeyDir {
    index: HashMap<String, ValueEntry>,
}

impl KeyDir {
    pub fn new() -> KeyDir {
        return KeyDir {
            index: HashMap::new(),
        };
    }

    pub fn put(&mut self, key: String, value: ValueEntry) {
        self.index.insert(key, value);
    }

    pub fn get(&self, key: &String) -> Option<&ValueEntry> {
        self.index.get(key)
    }

    pub fn delete(&mut self, key: &String) -> Option<ValueEntry> {
        self.index.remove(key)
    }
}
