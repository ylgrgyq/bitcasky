use bitcask_tests::common::RandomTestingDataGenerator;
use common::formatter::RowToWrite;
use database::data_storage::{DataStorage, DataStorageOptions, DataStorageWriter};

use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::Builder;

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
    let mut data_storage = DataStorage::new(
        dir,
        100,
        DataStorageOptions {
            max_file_size: u64::MAX,
        },
    )
    .unwrap();

    let key_size = 100;
    let value_size = 100;
    let mut generator = RandomTestingDataGenerator::new(key_size, value_size, vec![]);
    let input = generator.generate_testing_kv();

    let mut query_count = 0;
    group.bench_function("write-small-row", |b| {
        b.iter(|| {
            let k = input.key();
            let row = RowToWrite::new(&k, input.value());
            data_storage.write_row(&row).unwrap();
            query_count += 1;
        })
    });

    group.finish();

    println!(
        "Execute {} times. Write {} data",
        query_count,
        format_bytes(query_count * (key_size + value_size)),
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = write_row_benchmark
}

criterion_main!(benches);
