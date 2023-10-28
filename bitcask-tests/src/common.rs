use rand::{rngs::ThreadRng, RngCore};
use std::{collections::HashMap, path::PathBuf};

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

    pub fn key_ref(&self) -> &Vec<u8> {
        &self.key
    }
    pub fn value_ref(&self) -> &Vec<u8> {
        &self.value
    }

    pub fn key(&self) -> Vec<u8> {
        self.key.clone()
    }
    pub fn value(&self) -> Vec<u8> {
        self.value.clone()
    }
}

#[derive(Clone)]
pub enum TestingOperator {
    DELETE,
    PUT,
    MERGE,
}

#[derive(Clone)]
pub struct TestingOperation {
    testing_kv: TestingKV,
    operator: TestingOperator,
}

impl TestingOperation {
    pub fn operator(&self) -> &TestingOperator {
        &self.operator
    }

    pub fn key(&self) -> Vec<u8> {
        self.testing_kv.key()
    }

    pub fn value(&self) -> Vec<u8> {
        self.testing_kv.value()
    }
}

#[derive(Clone)]
pub struct TestingOperations {
    operations: Vec<TestingOperation>,
}

impl TestingOperations {
    pub fn operations(&self) -> &Vec<TestingOperation> {
        &self.operations
    }

    pub fn squash(&self) -> Vec<TestingOperation> {
        let mut ops: Vec<TestingOperation> = vec![];
        let mut map_index = HashMap::<Vec<u8>, usize>::new();
        for (pos, op) in self.operations.iter().enumerate() {
            match op.operator() {
                TestingOperator::PUT => {
                    if !map_index.is_empty() && map_index.get(&op.key()).is_some() {
                        ops.retain(|e| e.key() != op.key())
                    }
                    ops.push(op.clone());
                    map_index.insert(op.key(), pos);
                }
                TestingOperator::DELETE => {
                    if map_index.get(&op.key()).is_some() {
                        ops.retain(|e| e.key() != op.key())
                    }
                }
                _ => {}
            }
        }
        ops
    }
}

pub struct RandomTestingDataGenerator {
    rng: ThreadRng,
    key_size: usize,
    value_size: usize,
    candidate_operators: Vec<TestingOperator>,
}

impl RandomTestingDataGenerator {
    pub fn new(
        key_size: usize,
        value_size: usize,
        candidate_operations: Vec<TestingOperator>,
    ) -> RandomTestingDataGenerator {
        RandomTestingDataGenerator {
            rng: rand::thread_rng(),
            key_size,
            value_size,
            candidate_operators: candidate_operations,
        }
    }

    pub fn generate_testing_kvs(&mut self, kvs_len: usize) -> Vec<TestingKV> {
        let mut ret: Vec<TestingKV> = vec![self.generate_testing_kv(); kvs_len];
        ret.fill_with(|| self.generate_testing_kv());
        ret
    }

    pub fn generate_testing_operations(&mut self, operation_len: usize) -> TestingOperations {
        let mut ret: Vec<TestingOperation> = vec![self.generate_testing_operation(); operation_len];
        ret.fill_with(|| self.generate_testing_operation());
        TestingOperations { operations: ret }
    }

    pub fn generate_testing_kv(&mut self) -> TestingKV {
        let mut k = vec![0; self.key_size];
        self.rng.fill_bytes(&mut k);

        let mut v = vec![0; self.value_size];
        v.resize(self.key_size, 0);
        self.rng.fill_bytes(&mut v);

        TestingKV::from_bytes(&k, &v)
    }

    pub fn generate_testing_operation(&mut self) -> TestingOperation {
        let kv = self.generate_testing_kv();
        let operator = self.generate_write_operator();
        TestingOperation {
            testing_kv: kv,
            operator,
        }
    }

    pub fn generate_write_operator(&mut self) -> TestingOperator {
        let inx = self.rng.next_u64() % self.candidate_operators.len() as u64;
        self.candidate_operators[inx as usize].clone()
    }
}

pub fn get_temporary_directory_path() -> PathBuf {
    let dir = tempfile::tempdir().unwrap();
    dir.into_path()
}
