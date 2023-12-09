use std::time::Duration;

use bitcask::bitcask::Bitcask;
use bitcask_tests::common::get_temporary_directory_path;
use common::options::BitcaskOptions;
use test_log::test;

#[test]
fn test_merge_delete_no_remain() {
    let db_path = get_temporary_directory_path();
    let bc = Bitcask::open(&db_path, BitcaskOptions::default()).unwrap();
    bc.put("k1".into(), "value1".as_bytes()).unwrap();
    bc.put("k2".into(), "value2".as_bytes()).unwrap();
    bc.put("k3".into(), "value3".as_bytes()).unwrap();
    bc.delete(&"k1".into()).unwrap();
    bc.delete(&"k2".into()).unwrap();
    bc.delete(&"k3".into()).unwrap();

    bc.merge().unwrap();

    let stats = bc.stats().unwrap();
    assert_eq!(1, stats.number_of_data_files);
    assert_eq!(0, stats.number_of_keys);
}

#[test]
fn test_merge_has_remain() {
    let db_path = get_temporary_directory_path();
    let bc = Bitcask::open(&db_path, BitcaskOptions::default()).unwrap();
    bc.put("k1".into(), "value1".as_bytes()).unwrap();
    bc.put("k2".into(), "value2".as_bytes()).unwrap();
    bc.put("k3".into(), "value3".as_bytes()).unwrap();
    bc.put("k4".into(), "value4".as_bytes()).unwrap();
    bc.delete(&"k1".into()).unwrap();
    bc.delete(&"k2".into()).unwrap();
    bc.delete(&"k3".into()).unwrap();

    let before_merge_stats = bc.stats().unwrap();
    bc.merge().unwrap();
    let after_merge_stats = bc.stats().unwrap();
    assert_eq!(1, before_merge_stats.number_of_data_files);
    assert_eq!(2, after_merge_stats.number_of_data_files);
    assert_eq!(1, after_merge_stats.number_of_keys);
}

#[test]
fn test_merge_duplicate() {
    let db_path = get_temporary_directory_path();
    let bc = Bitcask::open(&db_path, BitcaskOptions::default()).unwrap();
    bc.put("k1".into(), "value1".as_bytes()).unwrap();
    bc.put("k1".into(), "value2".as_bytes()).unwrap();
    bc.put("k1".into(), "value3".as_bytes()).unwrap();

    let before_merge_stats = bc.stats().unwrap();
    bc.merge().unwrap();
    let after_merge_stats = bc.stats().unwrap();

    assert_eq!(1, before_merge_stats.number_of_data_files);

    assert_eq!(2, after_merge_stats.number_of_data_files);
    assert_eq!(1, after_merge_stats.number_of_keys);
}

#[test]
fn test_merge_recover_after_merge() {
    let db_path = get_temporary_directory_path();
    {
        let bc = Bitcask::open(&db_path, BitcaskOptions::default()).unwrap();
        bc.put("k2".into(), "value3value3".as_bytes()).unwrap();
        bc.put("k4".into(), "value4value4".as_bytes()).unwrap();
    }

    {
        let bc = Bitcask::open(&db_path, BitcaskOptions::default()).unwrap();
        // duplicate
        bc.put("k1".into(), "value1".as_bytes()).unwrap();
        bc.put("k1".into(), "value2".as_bytes()).unwrap();
        bc.put("k1".into(), "value3".as_bytes()).unwrap();

        // duplicate
        bc.put("k2".into(), "value2".as_bytes()).unwrap();
        bc.put("k2".into(), "value2value3".as_bytes()).unwrap();

        bc.put("k3".into(), "value3".as_bytes()).unwrap();
        bc.put("k4".into(), "value4value4".as_bytes()).unwrap();

        // delete duplicate
        bc.delete(&"k1".into()).unwrap();
        // delete plain key
        bc.delete(&"k3".into()).unwrap();

        bc.merge().unwrap();
    }

    let bc = Bitcask::open(&db_path, BitcaskOptions::default()).unwrap();
    assert_eq!(
        bc.get(&"k2".into()).unwrap().unwrap(),
        "value2value3".as_bytes()
    );
    assert_eq!(
        bc.get(&"k4".into()).unwrap().unwrap(),
        "value4value4".as_bytes()
    );
}

#[test]
fn test_recover_expirable_value() {
    let db_path = get_temporary_directory_path();
    {
        let bc = Bitcask::open(&db_path, BitcaskOptions::default()).unwrap();
        bc.put("importalK1".into(), "value1".as_bytes()).unwrap();
        bc.put_with_ttl(
            "expireToImortalK2".into(),
            "value2".as_bytes(),
            Duration::from_nanos(1),
        )
        .unwrap();
        bc.put("imortalToExpireK3".into(), "value3".as_bytes())
            .unwrap();
        bc.put_with_ttl(
            "expireK4".into(),
            "value4".as_bytes(),
            Duration::from_nanos(1),
        )
        .unwrap();
    }

    {
        let bc = Bitcask::open(&db_path, BitcaskOptions::default()).unwrap();
        bc.put("importalK1".into(), "value1value1".as_bytes())
            .unwrap();
        bc.put("expireToImortalK2".into(), "value2value2".as_bytes())
            .unwrap();
        bc.put_with_ttl(
            "imortalToExpireK3".into(),
            "value3".as_bytes(),
            Duration::from_nanos(1),
        )
        .unwrap();

        bc.merge().unwrap();
    }

    let bc = Bitcask::open(&db_path, BitcaskOptions::default()).unwrap();
    assert_eq!(
        bc.get(&"importalK1".into()).unwrap().unwrap(),
        "value1value1".as_bytes()
    );
    assert_eq!(
        bc.get(&"expireToImortalK2".into()).unwrap().unwrap(),
        "value2value2".as_bytes()
    );
    assert!(bc.get(&"imortalToExpireK3".into()).unwrap().is_none());
    assert!(bc.get(&"expireK4".into()).unwrap().is_none());
}
