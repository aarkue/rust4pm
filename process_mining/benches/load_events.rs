//! Benchmark the time taken to load from disk to EventLog
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use process_mining::{test_utils::get_test_data_path, EventLog, Importable};
use std::time::Duration;

fn bench_load_events(c: &mut Criterion) {
    let root = get_test_data_path();
    let repair = root.join("xes").join("RepairExample.xes");
    let traffic = root
        .join("xes")
        .join("Road_Traffic_Fine_Management_Process.xes.gz");

    let mut small = c.benchmark_group("load_events");
    small.sample_size(25);
    small.measurement_time(Duration::from_secs(10));
    small.bench_function("repair", |b| {
        b.iter(|| black_box(EventLog::import_from_path(&repair)).unwrap())
    });
    small.finish();

    let mut large = c.benchmark_group("load_events");
    large.sample_size(25);
    large.measurement_time(Duration::from_secs(45));
    large.bench_function("traffic", |b| {
        b.iter(|| black_box(EventLog::import_from_path(&traffic)).unwrap())
    });
    large.finish();
}

criterion_group!(benches, bench_load_events);
criterion_main!(benches);
