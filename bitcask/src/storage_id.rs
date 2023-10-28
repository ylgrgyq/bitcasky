use std::ops::Add;

use log::info;
use parking_lot::Mutex;

pub type StorageId = u32;

#[derive(Debug)]
pub struct StorageIdGenerator {
    id: Mutex<StorageId>,
}

impl StorageIdGenerator {
    pub fn new() -> StorageIdGenerator {
        StorageIdGenerator { id: Mutex::new(0) }
    }

    pub fn generate_next_id(&self) -> StorageId {
        let mut id = self.id.lock();
        let next_id = id.add(1);
        *id = next_id;
        next_id
    }

    pub fn update_id(&self, known_max_storage_id: StorageId) {
        let mut id = self.id.lock();
        if known_max_storage_id < *id {
            return;
        }
        *id = known_max_storage_id;
        info!(target: "StorageIdGenerator", "update storage id to {}", *id);
    }

    #[allow(dead_code)]
    pub fn get_id(&self) -> StorageId {
        *self.id.lock()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn test_generate_id() {
        let id_gen = StorageIdGenerator::new();
        assert_eq!(1, id_gen.generate_next_id());
        assert_eq!(2, id_gen.generate_next_id());
        assert_eq!(3, id_gen.generate_next_id());
        assert_eq!(3, id_gen.get_id());
    }

    #[test]
    fn test_update_storage_id() {
        let id_gen = StorageIdGenerator::new();
        assert_eq!(1, id_gen.generate_next_id());
        id_gen.update_id(10);
        assert_eq!(11, id_gen.generate_next_id());
        assert_eq!(12, id_gen.generate_next_id());
        assert_eq!(12, id_gen.get_id());
    }
}
