use bitcask_tests::common::RandomTestingDataGenerator;
use common::formatter::RowToWrite;
use database::data_storage::{DataStorage, DataStorageOptions, DataStorageWriter};

use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::Builder;

fn write_row_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("write-row");

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
    let mut data_storage = DataStorage::new(dir, 100, DataStorageOptions::default()).unwrap();

    let key_size = 100;
    let value_size = 100;
    let mut generator = RandomTestingDataGenerator::new(key_size, value_size, vec![]);
    let input = generator.generate_testing_kv();

    group.bench_function("write-small-row", |b| {
        b.iter(|| {
            let k = input.key();
            let row = RowToWrite::new(&k, input.value());
            data_storage.write_row(&row).unwrap();
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = write_row_benchmark
}

criterion_main!(benches);
