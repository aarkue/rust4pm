//! Benchmark the time taken to convert EventLog to polars::DataFrame
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use process_mining::core::event_data::case_centric::dataframe::convert_log_to_dataframe;
use process_mining::{test_utils::get_test_data_path, EventLog, Importable};
use std::time::Duration;

fn bench_dataframe_conversion(c: &mut Criterion) {
    let root = get_test_data_path();
    let repair = root.join("xes").join("RepairExample.xes");
    let traffic = root
        .join("xes")
        .join("Road_Traffic_Fine_Management_Process.xes.gz");

    let repair_log = EventLog::import_from_path(&repair).unwrap();
    let traffic_log = EventLog::import_from_path(&traffic).unwrap();

    // Small log (~5ms/conversion): fits the default window comfortably.
    let mut small = c.benchmark_group("log_to_dataframe_conversion");
    small.sample_size(25);
    small.bench_function("repair", |b| {
        b.iter(|| black_box(convert_log_to_dataframe(&repair_log, false)).unwrap())
    });
    small.finish();

    // Large log (~540ms/conversion): widen the window past sample_size * per-call
    // (~14s) to avoid the "unable to complete samples" warning.
    let mut large = c.benchmark_group("log_to_dataframe_conversion");
    large.sample_size(25);
    large.measurement_time(Duration::from_secs(15));
    large.bench_function("traffic", |b| {
        b.iter(|| black_box(convert_log_to_dataframe(&traffic_log, false)).unwrap())
    });
    large.finish();
}

criterion_group!(benches, bench_dataframe_conversion);
criterion_main!(benches);
