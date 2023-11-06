use std::{
    io,
    sync::atomic::{AtomicUsize, Ordering},
};

use tokio::runtime::{self, Runtime};

pub fn create_optimization_runtime(max_optimization_threads: usize) -> io::Result<Runtime> {
    let mut optimize_runtime_builder = runtime::Builder::new_multi_thread();

    optimize_runtime_builder
        .enable_time()
        .thread_name_fn(move || {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let optimizer_id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("optimize-{optimizer_id}")
        });

    if max_optimization_threads > 0 {
        // panics if val is not larger than 0.
        optimize_runtime_builder.max_blocking_threads(max_optimization_threads);
    }
    optimize_runtime_builder.build()
}

struct OptimizerHolder {}
