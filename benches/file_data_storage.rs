extern crate bitcask;

use common::formatter::{
    get_formatter_from_file, BitcaskFormatter, Formatter, FormatterError, RowToWrite,
};
use database::data_storage::{DataStorage, DataStorageOptions, DataStorageWriter};

use criterion::{criterion_group, criterion_main, Criterion};
use log::info;
use rand::rngs::StdRng;
use rand::RngCore;
use rand::{Rng, SeedableRng};
use tempfile::Builder;

fn conditional_struct_search_benchmark(c: &mut Criterion) {
    info!("asdfasdf");

    let mut rng = StdRng::seed_from_u64(42);
    let mut group = c.benchmark_group("conditional-search-group");

    let seed = 42;

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    let mut data_storage = DataStorage::new(dir, 100, DataStorageOptions::default()).unwrap();

    // let struct_index = create_struct_payload_index(dir.path(), NUM_POINTS, seed);

    let mut result_size = 0;
    let mut query_count = 0;

    // let filter = random_must_filter(&mut rng, 2);
    // let cardinality = struct_index.estimate_cardinality(&filter);

    // let indexed_fields = struct_index.indexed_fields();

    // eprintln!("cardinality = {cardinality:#?}");
    // eprintln!("indexed_fields = {indexed_fields:#?}");

    group.bench_function("struct-conditional-search-query-points", |b| {
        b.iter(|| {
            let mut buf = vec![0; 199];
            let mut small_rng = StdRng::from_entropy();
            small_rng.fill_bytes(&mut buf);

            let mut buf2 = vec![0; 199];
            let mut small_rng = StdRng::from_entropy();
            small_rng.fill_bytes(&mut buf2);

            let row = RowToWrite::new(&buf, buf2);
            data_storage.write_row(&row).unwrap();

            // let filter = random_must_filter(&mut rng, 2);
            // result_size += struct_index.query_points(&filter).len();
            query_count += 1;
        })
    });
    eprintln!(
        "result_size / query_count = {:#?}",
        result_size / query_count
    );

    let mut result_size = 0;
    let mut query_count = 0;

    // group.bench_function("struct-conditional-search-context-check", |b| {
    //     b.iter(|| {
    //         let filter = random_must_filter(&mut rng, 2);
    //         let sample = (0..CHECK_SAMPLE_SIZE)
    //             .map(|_| rng.gen_range(0..NUM_POINTS) as PointOffsetType)
    //             .collect_vec();
    //         let context = struct_index.filter_context(&filter);

    //         let filtered_sample = sample
    //             .into_iter()
    //             .filter(|id| context.check(*id))
    //             .collect_vec();
    //         result_size += filtered_sample.len();
    //         query_count += 1;
    //     })
    // });

    // eprintln!(
    //     "result_size / query_count = {:#?}",
    //     result_size / query_count
    // );

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = conditional_struct_search_benchmark
}

criterion_main!(benches);
