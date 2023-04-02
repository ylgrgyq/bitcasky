use std::path::PathBuf;

pub struct TestingKV {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl TestingKV {
    pub fn new(k: &str, v: &str) -> TestingKV {
        TestingKV {
            key: k.into(),
            value: v.into(),
        }
    }
    pub fn key(&self) -> Vec<u8> {
        self.key.clone()
    }
    pub fn value(&self) -> Vec<u8> {
        self.value.clone()
    }
}

pub fn get_temporary_directory_path() -> PathBuf {
    let dir = tempfile::tempdir().unwrap();
    dir.into_path()
}
