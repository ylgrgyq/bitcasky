use bytes::BytesMut;
use rand::RngCore;
use std::path::PathBuf;

#[derive(Clone)]
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

    pub fn from_bytes(k: &[u8], v: &[u8]) -> TestingKV {
        TestingKV {
            key: k.to_vec(),
            value: v.to_vec(),
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

pub fn generate_random_testing_kvs(
    key_size: usize,
    value_size: usize,
    kvs_len: usize,
) -> Vec<TestingKV> {
    let mut ret: Vec<TestingKV> = vec![generate_random_testing_kv(key_size, value_size); kvs_len];
    ret.fill_with(|| generate_random_testing_kv(key_size, value_size));
    ret
}

pub fn generate_random_testing_kv(key_size: usize, value_size: usize) -> TestingKV {
    let mut rng = rand::thread_rng();
    let mut k = BytesMut::with_capacity(key_size);
    rng.fill_bytes(k.as_mut());

    let mut v = BytesMut::with_capacity(value_size);
    rng.fill_bytes(v.as_mut());

    TestingKV::from_bytes(&k, &v)
}
