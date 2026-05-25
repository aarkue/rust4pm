//! Benchmark the time taken to load from disk to EventLog
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use process_mining::{test_utils::get_test_data_path, EventLog, Importable};
use std::time::Duration;
fn bench_load_events(c: &mut Criterion) {
    let root = get_test_data_path();
    let datasets = vec![
        ("repair", "xes/RepairExample.xes"),
        ("traffic", "xes/Road_Traffic_Fine_Management_Process.xes.gz"),
    ];
    let mut group = c.benchmark_group("load_events");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(120));
    for (name, path_str) in datasets {
        let path = root.join(path_str);

        group.bench_function(name, |b| {
            b.iter(|| {
                let _df = black_box(EventLog::import_from_path(&path)).unwrap();
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_load_events);
criterion_main!(benches);
