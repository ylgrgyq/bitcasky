use std::collections::HashSet;

use bitcask::{
    bitcask::{Bitcask, BitcaskOptions},
    error::BitcaskError,
};
use bitcask_tests::common::{
    get_temporary_directory_path, RandomTestingDataGenerator, TestingOperations, TestingOperator,
};
use test_log::test;
use walkdir::WalkDir;

const DEFAULT_OPTIONS: BitcaskOptions = BitcaskOptions {
    max_file_size: 10 * 1024,
    max_key_size: 64,
    max_value_size: 1024,
};

fn execute_testing_operations(bc: &Bitcask, ops: &TestingOperations) {
    for op in ops.operations() {
        match op.operator() {
            TestingOperator::PUT => bc.put(op.key(), op.value()).unwrap(),
            TestingOperator::DELETE => bc.delete(&op.key()).unwrap(),
            TestingOperator::MERGE => bc.merge().unwrap(),
        }
    }
}

#[test]
fn test_open_db_twice() {
    let dir = get_temporary_directory_path();
    let _bc = Bitcask::open(&dir, DEFAULT_OPTIONS).unwrap();
    let bc2 = Bitcask::open(&dir, DEFAULT_OPTIONS);
    assert!(bc2.is_err());
    assert!(matches!(
        bc2.err(),
        Some(BitcaskError::LockDirectoryFailed(_))
    ));
}

#[test]
fn test_read_write_writing_file() {
    let dir = get_temporary_directory_path();
    let bc = Bitcask::open(&dir, DEFAULT_OPTIONS).unwrap();
    bc.put("k1".into(), "value1".as_bytes()).unwrap();
    bc.put("k2".into(), "value2".as_bytes()).unwrap();
    bc.put("k3".into(), "value3".as_bytes()).unwrap();
    bc.put("k1".into(), "value4".as_bytes()).unwrap();

    assert_eq!(bc.get(&"k1".into()).unwrap().unwrap(), "value4".as_bytes());
    assert_eq!(bc.get(&"k2".into()).unwrap().unwrap(), "value2".as_bytes());
    assert_eq!(bc.get(&"k3".into()).unwrap().unwrap(), "value3".as_bytes());
}

#[test]
fn test_random_put_and_delete() {
    let mut gen = RandomTestingDataGenerator::new(
        64,
        512,
        vec![TestingOperator::PUT, TestingOperator::DELETE],
    );
    let ops = gen.generate_testing_operations(5000);
    let dir = get_temporary_directory_path();
    let bc = Bitcask::open(&dir, DEFAULT_OPTIONS).unwrap();
    execute_testing_operations(&bc, &ops);

    for op in ops.squash() {
        assert_eq!(bc.get(&op.key()).unwrap().unwrap(), op.value());
    }
}

#[test]
fn test_random_put_delete_merge() {
    let mut gen = RandomTestingDataGenerator::new(
        64,
        512,
        vec![
            TestingOperator::PUT,
            TestingOperator::DELETE,
            TestingOperator::MERGE,
        ],
    );
    let ops = gen.generate_testing_operations(3);
    let dir = get_temporary_directory_path();
    let bc = Bitcask::open(&dir, DEFAULT_OPTIONS).unwrap();
    execute_testing_operations(&bc, &ops);

    for op in ops.squash() {
        assert_eq!(bc.get(&op.key()).unwrap().unwrap(), op.value());
    }
}

#[test]
fn test_recovery() {
    let mut gen = RandomTestingDataGenerator::new(
        64,
        512,
        vec![TestingOperator::PUT, TestingOperator::DELETE],
    );
    let ops = gen.generate_testing_operations(5000);
    let dir = get_temporary_directory_path();
    {
        let bc = Bitcask::open(&dir, DEFAULT_OPTIONS).unwrap();
        execute_testing_operations(&bc, &ops);
    }
    let bc = Bitcask::open(&dir, DEFAULT_OPTIONS).unwrap();
    for op in ops.squash() {
        assert_eq!(bc.get(&op.key()).unwrap().unwrap(), op.value());
    }
}

#[test]
fn test_delete() {
    let dir = get_temporary_directory_path();
    let bc = Bitcask::open(&dir, DEFAULT_OPTIONS).unwrap();
    bc.put("k1".into(), "value1".as_bytes()).unwrap();
    bc.put("k2".into(), "value2".as_bytes()).unwrap();
    bc.put("k3".into(), "value3".as_bytes()).unwrap();
    bc.put("k1".into(), "value4".as_bytes()).unwrap();

    bc.delete(&"k1".into()).unwrap();
    assert_eq!(bc.get(&"k1".into()).unwrap(), None);

    bc.delete(&"k2".into()).unwrap();
    assert_eq!(bc.get(&"k2".into()).unwrap(), None);

    bc.delete(&"k3".into()).unwrap();
    assert_eq!(bc.get(&"k3".into()).unwrap(), None);
}

#[test]
fn test_delete_not_exists_key() {
    let dir = get_temporary_directory_path();
    let bc = Bitcask::open(&dir, DEFAULT_OPTIONS).unwrap();

    bc.delete(&"k1".into()).unwrap();
    assert_eq!(bc.get(&"k1".into()).unwrap(), None);

    bc.delete(&"k2".into()).unwrap();
    assert_eq!(bc.get(&"k2".into()).unwrap(), None);

    bc.delete(&"k3".into()).unwrap();
    assert_eq!(bc.get(&"k3".into()).unwrap(), None);

    assert!(WalkDir::new(&dir)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .map(|e| e.metadata().unwrap())
        .filter(|m| !m.is_dir())
        .all(|meta| { meta.len() == 0 }));
}

#[test]
fn test_foreach_keys() {
    let mut gen = RandomTestingDataGenerator::new(64, 512, vec![TestingOperator::PUT]);
    let ops = gen.generate_testing_operations(100);
    let dir = get_temporary_directory_path();
    let bc = Bitcask::open(&dir, DEFAULT_OPTIONS).unwrap();
    execute_testing_operations(&bc, &ops);

    let mut expected_set: HashSet<Vec<u8>> = HashSet::new();
    let mut actual_set: HashSet<Vec<u8>> = HashSet::new();
    for op in ops.squash() {
        expected_set.insert(op.key());
    }

    bc.foreach_key(|k| {
        actual_set.insert(k.clone());
    })
    .unwrap();
    assert_eq!(expected_set, actual_set);
}

#[test]
fn test_fold_keys() {
    let mut gen = RandomTestingDataGenerator::new(64, 512, vec![TestingOperator::PUT]);
    let ops = gen.generate_testing_operations(100);
    let dir = get_temporary_directory_path();
    let bc = Bitcask::open(&dir, DEFAULT_OPTIONS).unwrap();
    execute_testing_operations(&bc, &ops);

    let mut expected_set: HashSet<Vec<u8>> = HashSet::new();
    let mut actual_set: HashSet<Vec<u8>> = HashSet::new();
    for op in ops.squash() {
        expected_set.insert(op.key());
    }

    let ret = bc
        .fold_key(
            |k, acc| {
                actual_set.insert(k.clone());
                Ok(Some(acc.unwrap() + 1))
            },
            Some(0),
        )
        .unwrap();
    assert_eq!(expected_set.len(), ret.unwrap());
    assert_eq!(expected_set, actual_set);
}

#[test]
fn test_foreach() {
    let mut gen = RandomTestingDataGenerator::new(64, 512, vec![TestingOperator::PUT]);
    let ops = gen.generate_testing_operations(100);
    let dir = get_temporary_directory_path();
    let bc = Bitcask::open(&dir, DEFAULT_OPTIONS).unwrap();
    execute_testing_operations(&bc, &ops);

    let expected_pair = ops
        .squash()
        .iter()
        .map(|op| (op.key(), op.value()))
        .collect::<Vec<(Vec<u8>, Vec<u8>)>>();

    let mut actual_pair: Vec<(Vec<u8>, Vec<u8>)> = vec![];
    bc.foreach(|k, v| {
        actual_pair.push((k.clone(), v.clone()));
    })
    .unwrap();

    assert_eq!(expected_pair, actual_pair);
}

#[test]
fn test_fold() {
    let mut gen = RandomTestingDataGenerator::new(64, 512, vec![TestingOperator::PUT]);
    let ops = gen.generate_testing_operations(100);
    let dir = get_temporary_directory_path();
    let bc = Bitcask::open(&dir, DEFAULT_OPTIONS).unwrap();
    execute_testing_operations(&bc, &ops);

    let expected_pair = ops
        .squash()
        .iter()
        .map(|op| (op.key(), op.value()))
        .collect::<Vec<(Vec<u8>, Vec<u8>)>>();

    let mut actual_pair: Vec<(Vec<u8>, Vec<u8>)> = vec![];
    let ret = bc
        .fold(
            |k, v, acc| {
                actual_pair.push((k.clone(), v.clone()));
                Ok(Some(acc.unwrap() + 1))
            },
            Some(0),
        )
        .unwrap();
    assert_eq!(expected_pair.len(), ret.unwrap());
    assert_eq!(expected_pair, actual_pair);
}
