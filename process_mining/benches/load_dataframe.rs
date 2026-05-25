//! Benchmark the time taken to convert EventLog to polars::DataFrame
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use process_mining::core::event_data::case_centric::dataframe::convert_log_to_dataframe;

use process_mining::{test_utils::get_test_data_path, EventLog, Importable};

fn bench_dataframe_conversion(c: &mut Criterion) {
    let root = get_test_data_path();
    let datasets = vec![
        ("repair", "xes/RepairExample.xes"),
        ("traffic", "xes/Road_Traffic_Fine_Management_Process.xes.gz"),
    ];
    let mut group = c.benchmark_group("log_to_dataframe_conversion");
    group.sample_size(100);
    for (name, path_str) in datasets {
        let path = root.join(path_str);
        if let Ok(log) = EventLog::import_from_path(&path) {
            group.bench_function(name, |b| {
                b.iter(|| {
                    let _df = black_box(convert_log_to_dataframe(&log, false)).unwrap();
                })
            });
        } else {
            eprintln!(
                "Warning: Failed to load target bench log file at {:?}",
                path
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_dataframe_conversion);
criterion_main!(benches);
