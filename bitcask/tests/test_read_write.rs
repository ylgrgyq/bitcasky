use std::{fs, path::Path};

use bitcask::bitcask::{Bitcask, BitcaskOptions};
use log::info;
use test_log::test;

const DEFAULT_OPTIONS: BitcaskOptions = BitcaskOptions {
    max_file_size: 11,
    max_key_size: 1024,
    max_value_size: 1024,
};

#[test]
fn test_read_write_writing_file() {
    let dir = tempfile::tempdir().unwrap();
    let bc = Bitcask::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
    bc.put("k1".into(), "value1".as_bytes()).unwrap();
    bc.put("k2".into(), "value2".as_bytes()).unwrap();
    bc.put("k3".into(), "value3".as_bytes()).unwrap();
    bc.put("k1".into(), "value4".as_bytes()).unwrap();

    assert_eq!(bc.get(&"k1".into()).unwrap().unwrap(), "value4".as_bytes());
    assert_eq!(bc.get(&"k2".into()).unwrap().unwrap(), "value2".as_bytes());
    assert_eq!(bc.get(&"k3".into()).unwrap().unwrap(), "value3".as_bytes());
}

#[test]
fn test_recovery() {
    let dir = tempfile::tempdir().unwrap();
    {
        let bc = Bitcask::open(
            &dir.path(),
            BitcaskOptions {
                max_file_size: 100,
                max_key_size: 1024,
                max_value_size: 1024,
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
        &dir.path(),
        BitcaskOptions {
            max_file_size: 100,
            max_key_size: 1024,
            max_value_size: 1024,
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
fn test_merge() {
    let dir = tempfile::tempdir().unwrap();
    {
        let bc = Bitcask::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
        bc.put("k1".into(), "value1".as_bytes()).unwrap();
    }
    {
        let bc = Bitcask::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
        bc.put("k2".into(), "value2".as_bytes()).unwrap();
        bc.put("k21".into(), "value2".as_bytes()).unwrap();
    }
    {
        let bc = Bitcask::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
        bc.put("k3".into(), "value3".as_bytes()).unwrap();
        bc.delete(&"k21".into()).unwrap();
    }
    {
        let bc = Bitcask::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
        bc.put("k1".into(), "value4".as_bytes()).unwrap();
    }

    let bc = Bitcask::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
    bc.merge().unwrap();

    for path in fs::read_dir(dir.path()).unwrap() {
        let file_path = path.unwrap();
        if file_path.path().is_dir() {
            continue;
        }

        // info!("asdfsf {:?}", file_path);
    }
    assert_eq!(bc.get(&"k1".into()).unwrap().unwrap(), "value4".as_bytes());
    assert_eq!(bc.get(&"k2".into()).unwrap().unwrap(), "value2".as_bytes());
    assert_eq!(bc.get(&"k3".into()).unwrap().unwrap(), "value3".as_bytes());
}

#[test]
fn test_delete() {
    let dir = tempfile::tempdir().unwrap();
    let bc = Bitcask::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
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
