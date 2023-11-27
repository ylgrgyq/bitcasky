use std::vec;

use bitcask_tests::common::RandomTestingDataGenerator;
use common::formatter::RowToWrite;
use database::data_storage::{
    DataStorage, DataStorageOptions, DataStorageReader, DataStorageWriter,
};

use criterion::{criterion_group, criterion_main, Criterion};
use rand::{seq::SliceRandom, thread_rng};
use tempfile::{Builder, TempDir};

fn create_data_storage(dir: &TempDir) -> DataStorage {
    DataStorage::new(
        dir,
        100,
        DataStorageOptions::default()
            .max_data_file_size(usize::MAX)
            .init_data_file_capacity(100)
            .storage_type(database::data_storage::DataSotrageType::Mmap),
    )
    .unwrap()
}

fn format_bytes(n: usize) -> String {
    if n > 1_073_741_824 {
        format!("{:.2}GiB", n as f64 / 1_073_741_824f64)
    } else if n > 1_048_576 {
        format!("{:.2}MiB", n as f64 / 1_048_576f64)
    } else if n > 1_024 {
        format!("{:.2}KiB", n as f64 / 1_024f64)
    } else {
        format!("{n}B")
    }
}

fn write_row_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("write-row");
    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let mut data_storage = create_data_storage(&dir);

    let key_size = 100;
    let value_size = 100;
    let input = RandomTestingDataGenerator::new(key_size, value_size, vec![]).generate_testing_kv();

    let mut write_count = 0;
    group.bench_function("write-row", |b| {
        b.iter(|| {
            let row = RowToWrite::new(input.key_ref(), input.value());
            data_storage.write_row(&row).unwrap();
            write_count += 1;
        })
    });

    group.finish();

    println!(
        "Write {} times. Write {} data",
        write_count,
        format_bytes(write_count * (key_size + value_size)),
    );
}

fn sync_write_row_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("write-row");

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let mut data_storage = create_data_storage(&dir);

    let key_size = 100;
    let value_size = 100;
    let input = RandomTestingDataGenerator::new(key_size, value_size, vec![]).generate_testing_kv();

    let mut write_count = 0;
    group.bench_function("sync-write-row", |b| {
        b.iter(|| {
            let row = RowToWrite::new(input.key_ref(), input.value());
            data_storage.write_row(&row).unwrap();
            data_storage.flush().unwrap();
            write_count += 1;
        })
    });

    group.finish();

    println!(
        "Write {} times. Write {} data",
        write_count,
        format_bytes(write_count * (key_size + value_size)),
    );
}

fn rand_read_row_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("read-row");

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let mut data_storage = create_data_storage(&dir);

    let key_size = 100;
    let value_size = 100;
    let values = 10000;
    let input = RandomTestingDataGenerator::new(key_size, value_size, vec![]).generate_testing_kv();
    let mut offsets = vec![];
    for _ in 0..values {
        let row = RowToWrite::new(input.key_ref(), input.value());
        offsets.push(data_storage.write_row(&row).unwrap().row_offset);
    }
    data_storage.flush().unwrap();

    offsets.shuffle(&mut thread_rng());

    let mut counter = 0;
    group.bench_function("rand-read-row", |b| {
        b.iter(|| {
            let v = data_storage
                .read_value(*offsets.get(counter % offsets.len()).unwrap())
                .unwrap();
            assert_eq!(input.value(), *v.value);
            counter += 1;
        })
    });

    group.finish();

    println!(
        "Read {} times. Read {} data",
        counter,
        format_bytes(counter * value_size),
    );
}

fn sequential_read_row_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("read-row");

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let mut data_storage = create_data_storage(&dir);

    let key_size = 100;
    let value_size = 100;
    let values = 10000;
    let input = RandomTestingDataGenerator::new(key_size, value_size, vec![]).generate_testing_kv();
    let head_offset = data_storage
        .write_row(&RowToWrite::new(input.key_ref(), input.value()))
        .unwrap()
        .row_offset;
    for _ in 0..values {
        data_storage
            .write_row(&RowToWrite::new(input.key_ref(), input.value()))
            .unwrap();
    }

    data_storage.flush().unwrap();

    let mut counter = 0;
    group.bench_function("sequential-read-row", |b| {
        b.iter(|| {
            let v = data_storage.read_next_row().unwrap().unwrap();
            assert_eq!(input.value(), *v.value);
            if counter % values == 0 {
                data_storage.read_value(head_offset).unwrap();
            }
            counter += 1;
        })
    });

    group.finish();

    println!("Read {} times", counter,);
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = write_row_benchmark, sync_write_row_benchmark, rand_read_row_benchmark, sequential_read_row_benchmark
}

criterion_main!(benches);
