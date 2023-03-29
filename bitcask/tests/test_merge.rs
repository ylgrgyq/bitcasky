use std::{fs, path::Path};

use bitcask::bitcask::{Bitcask, BitcaskOptions};
use log::info;
use test_log::test;
use walkdir::WalkDir;

const DEFAULT_OPTIONS: BitcaskOptions = BitcaskOptions {
    max_file_size: 11,
    max_key_size: 1024,
    max_value_size: 1024,
};

#[test]
fn test_merge() {
    let dir = tempfile::tempdir().unwrap();
    let bc = Bitcask::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
    bc.put("k1".into(), "value1".as_bytes()).unwrap();
    bc.put("k2".into(), "value2".as_bytes()).unwrap();
    bc.put("k3".into(), "value3".as_bytes()).unwrap();
    bc.delete(&"k1".into()).unwrap();
    bc.delete(&"k2".into()).unwrap();
    bc.delete(&"k3".into()).unwrap();

    bc.merge().unwrap();

    assert!(WalkDir::new(&dir)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .map(|e| e.metadata().unwrap())
        .filter(|m| !m.is_dir())
        .all(|meta| { meta.len() == 0 }));
}
