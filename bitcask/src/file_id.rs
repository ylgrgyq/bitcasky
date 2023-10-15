use std::ops::Add;

use log::info;
use parking_lot::Mutex;

pub type FileId = u32;

#[derive(Debug)]
pub struct FileIdGenerator {
    file_id: Mutex<FileId>,
}

impl FileIdGenerator {
    pub fn new() -> FileIdGenerator {
        FileIdGenerator {
            file_id: Mutex::new(0),
        }
    }

    pub fn generate_next_file_id(&self) -> FileId {
        let mut id = self.file_id.lock();
        let next_id = id.add(1);
        *id = next_id;
        next_id
    }

    pub fn update_file_id(&self, known_max_file_id: FileId) {
        let mut id = self.file_id.lock();
        if known_max_file_id < *id {
            return;
        }
        *id = known_max_file_id;
        info!(target: "FileIdGenerator", "update file id to {}", *id);
    }

    #[allow(dead_code)]
    pub fn get_file_id(&self) -> FileId {
        *self.file_id.lock()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn test_generate_id() {
        let id_gen = FileIdGenerator::new();
        assert_eq!(1, id_gen.generate_next_file_id());
        assert_eq!(2, id_gen.generate_next_file_id());
        assert_eq!(3, id_gen.generate_next_file_id());
        assert_eq!(3, id_gen.get_file_id());
    }

    #[test]
    fn test_update_file_id() {
        let id_gen = FileIdGenerator::new();
        assert_eq!(1, id_gen.generate_next_file_id());
        id_gen.update_file_id(10);
        assert_eq!(11, id_gen.generate_next_file_id());
        assert_eq!(12, id_gen.generate_next_file_id());
        assert_eq!(12, id_gen.get_file_id());
    }
}
