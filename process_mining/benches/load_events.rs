//! Benchmark the time taken to load from disk to EventLog
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use process_mining::{test_utils::get_test_data_path, EventLog, Importable};
use std::time::Duration;
fn bench_load_events(c: &mut Criterion) {
    let root = get_test_data_path();
    let datasets = vec![
        ("repair", root.join("xes").join("RepairExample.xes")),
        (
            "traffic",
            root.join("xes")
                .join("Road_Traffic_Fine_Management_Process.xes.gz"),
        ),
    ];
    let mut group = c.benchmark_group("load_events");
    // Explicitly configure sample requirements for large datasets.
    // The large RTFM log takes significant time per iteration to decompress and parse;
    // a 120-second window ensures Criterion has enough runway to collect a
    // statistically valid sample distribution across 100 runs without timing out.
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(120));
    for (name, path) in datasets {
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
