use std::time::Duration;

use bitcasky::bitcasky::Bitcasky;
use bitcasky_common::options::BitcaskyOptions;
use test_log::test;
use utilities::common::get_temporary_directory_path;

#[test]
fn test_merge_delete_no_remain() {
    let db_path = get_temporary_directory_path();
    let bc = Bitcasky::open(&db_path, BitcaskyOptions::default()).unwrap();
    bc.put("k1", "value1".as_bytes()).unwrap();
    bc.put("k2", "value2".as_bytes()).unwrap();
    bc.put("k3", "value3".as_bytes()).unwrap();
    bc.delete(&"k1").unwrap();
    bc.delete(&"k2").unwrap();
    bc.delete(&"k3").unwrap();

    bc.merge().unwrap();

    let telemetry = bc.get_telemetry_data();
    assert_eq!(0, telemetry.database.stable_storages.len());
    assert_eq!(0, telemetry.keydir.number_of_keys);
}

#[test]
fn test_merge_has_remain() {
    let db_path = get_temporary_directory_path();
    let bc = Bitcasky::open(&db_path, BitcaskyOptions::default()).unwrap();
    bc.put("k1", "value1".as_bytes()).unwrap();
    bc.put("k2", "value2".as_bytes()).unwrap();
    bc.put("k3", "value3".as_bytes()).unwrap();
    bc.put("k4", "value4".as_bytes()).unwrap();
    bc.delete(&"k1").unwrap();
    bc.delete(&"k2").unwrap();
    bc.delete(&"k3").unwrap();

    let before_merge_telemetry = bc.get_telemetry_data();
    bc.merge().unwrap();
    let after_merge_telemetry = bc.get_telemetry_data();
    assert_eq!(0, before_merge_telemetry.database.stable_storages.len());
    assert_eq!(1, after_merge_telemetry.database.stable_storages.len());
    assert_eq!(1, after_merge_telemetry.keydir.number_of_keys);
}

#[test]
fn test_merge_duplicate() {
    let db_path = get_temporary_directory_path();
    let bc = Bitcasky::open(&db_path, BitcaskyOptions::default()).unwrap();
    bc.put("k1", "value1".as_bytes()).unwrap();
    bc.put("k1", "value2".as_bytes()).unwrap();
    bc.put("k1", "value3".as_bytes()).unwrap();

    let before_merge_telemetry = bc.get_telemetry_data();
    bc.merge().unwrap();
    let after_merge_telemetry = bc.get_telemetry_data();

    assert_eq!(0, before_merge_telemetry.database.stable_storages.len());

    assert_eq!(1, after_merge_telemetry.database.stable_storages.len());
    assert_eq!(1, after_merge_telemetry.keydir.number_of_keys);
}

#[test]
fn test_merge_recover_after_merge() {
    let db_path = get_temporary_directory_path();
    {
        let bc = Bitcasky::open(&db_path, BitcaskyOptions::default()).unwrap();
        bc.put("k2", "value3value3".as_bytes()).unwrap();
        bc.put("k4", "value4value4".as_bytes()).unwrap();
    }

    {
        let bc = Bitcasky::open(&db_path, BitcaskyOptions::default()).unwrap();
        // duplicate
        bc.put("k1", "value1".as_bytes()).unwrap();
        bc.put("k1", "value2".as_bytes()).unwrap();
        bc.put("k1", "value3".as_bytes()).unwrap();

        // duplicate
        bc.put("k2", "value2".as_bytes()).unwrap();
        bc.put("k2", "value2value3".as_bytes()).unwrap();

        bc.put("k3", "value3".as_bytes()).unwrap();
        bc.put("k4", "value4value4".as_bytes()).unwrap();

        // delete duplicate
        bc.delete(&"k1").unwrap();
        // delete plain key
        bc.delete(&"k3").unwrap();

        bc.merge().unwrap();
    }

    let bc = Bitcasky::open(&db_path, BitcaskyOptions::default()).unwrap();
    assert_eq!(bc.get(&"k2").unwrap().unwrap(), "value2value3".as_bytes());
    assert_eq!(bc.get(&"k4").unwrap().unwrap(), "value4value4".as_bytes());
}

#[test]
fn test_recover_expirable_value() {
    let db_path = get_temporary_directory_path();
    {
        let bc = Bitcasky::open(&db_path, BitcaskyOptions::default()).unwrap();
        bc.put("importalK1", "value1".as_bytes()).unwrap();
        bc.put_with_ttl(
            "expireToImortalK2",
            "value2".as_bytes(),
            Duration::from_nanos(1),
        )
        .unwrap();
        bc.put("imortalToExpireK3", "value3".as_bytes()).unwrap();
        bc.put_with_ttl("expireK4", "value4".as_bytes(), Duration::from_nanos(1))
            .unwrap();
        bc.put_with_ttl("notEpireK5", "value5".as_bytes(), Duration::from_secs(3600))
            .unwrap();
    }

    {
        let bc = Bitcasky::open(&db_path, BitcaskyOptions::default()).unwrap();
        bc.put("importalK1", "value1value1".as_bytes()).unwrap();
        bc.put("expireToImortalK2", "value2value2".as_bytes())
            .unwrap();
        bc.put_with_ttl(
            "imortalToExpireK3",
            "value3".as_bytes(),
            Duration::from_nanos(1),
        )
        .unwrap();

        bc.merge().unwrap();
    }

    let bc = Bitcasky::open(&db_path, BitcaskyOptions::default()).unwrap();
    assert_eq!(
        bc.get(&"importalK1").unwrap().unwrap(),
        "value1value1".as_bytes()
    );
    assert_eq!(
        bc.get(&"expireToImortalK2").unwrap().unwrap(),
        "value2value2".as_bytes()
    );
    assert!(bc.get(&"imortalToExpireK3").unwrap().is_none());
    assert!(bc.get(&"expireK4").unwrap().is_none());
    assert_eq!(bc.get(&"notEpireK5").unwrap().unwrap(), "value5".as_bytes());
}
