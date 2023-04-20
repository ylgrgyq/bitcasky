use std::path::Path;

use bitcask::{
    bitcask::{Bitcask, BitcaskOptions},
    error::BitcaskError,
};
use bitcask_tests::common::{
    get_temporary_directory_path, RandomTestingDataGenerator, TestingOperator,
};
use test_log::test;
use walkdir::WalkDir;

const DEFAULT_OPTIONS: BitcaskOptions = BitcaskOptions {
    max_file_size: 11,
    max_key_size: 1024,
    max_value_size: 1024,
    tolerate_data_file_corrption: true,
};

#[test]
fn test_open_db_twice() {
    let dir = get_temporary_directory_path();
    let _bc = Bitcask::open(&dir, DEFAULT_OPTIONS).unwrap();
    let bc2 = Bitcask::open(&dir, DEFAULT_OPTIONS);
    assert!(bc2.is_err());
    match bc2.err() {
        Some(BitcaskError::LockDirectoryFailed(_)) => assert!(true),
        _ => assert!(false),
    }
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
fn test_random_read_write() {
    let mut gen = RandomTestingDataGenerator::new(64, 64);
    let ops = gen.generate_testing_operations(10000);

    let dir = get_temporary_directory_path();
    let bc = Bitcask::open(&dir, BitcaskOptions::default()).unwrap();
    for op in ops.operations() {
        match op.operator() {
            TestingOperator::PUT => bc.put(op.key(), &op.value()).unwrap(),
            TestingOperator::DELETE => bc.delete(&op.key()).unwrap(),
        }
    }

    for op in ops.squash() {
        assert_eq!(bc.get(&op.key()).unwrap().unwrap(), op.value());
    }
}

#[test]
fn test_recovery() {
    let dir = get_temporary_directory_path();
    {
        let bc = Bitcask::open(
            &dir,
            BitcaskOptions {
                max_file_size: 100,
                max_key_size: 1024,
                max_value_size: 1024,
                tolerate_data_file_corrption: true,
            },
        )
        .unwrap();
        bc.put("k1".into(), "value1_value1_value1".as_bytes())
            .unwrap();
        bc.put("k2".into(), "value2_value2_value2".as_bytes())
            .unwrap();
        bc.put("k3".into(), "value3_value3_value3".as_bytes())
            .unwrap();
        bc.put("k1".into(), "value4_value4_value4".as_bytes())
            .unwrap();
        bc.delete(&"k2".into()).unwrap();
    }
    let bc = Bitcask::open(
        &dir,
        BitcaskOptions {
            max_file_size: 100,
            max_key_size: 1024,
            max_value_size: 1024,
            tolerate_data_file_corrption: true,
        },
    )
    .unwrap();
    assert_eq!(
        bc.get(&"k1".into()).unwrap().unwrap(),
        "value4_value4_value4".as_bytes()
    );
    assert_eq!(bc.get(&"k2".into()).unwrap(), None,);
    assert_eq!(
        bc.get(&"k3".into()).unwrap().unwrap(),
        "value3_value3_value3".as_bytes()
    );
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
