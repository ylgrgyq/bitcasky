use std::collections::HashMap;

struct Index {
    file_id: String,
    value_size: u64,
    value_pos: u64,
    tstamp: u64,
}

pub struct KeyDir {
    index: HashMap<String, Index>,
}

impl KeyDir {
    pub fn new() -> KeyDir {
        return KeyDir {
            index: HashMap::new(),
        };
    }

    pub fn put(&mut self, key: String, value: Index) {
        self.index.insert(key, value);
    }

    pub fn get(&self, key: &String) -> Option<&Index> {
        self.index.get(key)
    }

    pub fn delete(&mut self, key: &String) -> Option<Index> {
        self.index.remove(key)
    }
}
