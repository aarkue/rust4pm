//! Benchmark OCEL 2.0 import speed across importers, per source format:
//! - `ocel_direct`: `OCEL::import_from_path`, materializing the full log in memory
//! - `slim_streaming`: `SlimLinkedOCEL::import_from_path`
//! - `duckdb_*`: `stream_ocel_file_to_duckdb`, on-disk (`duckdb_raw` streaming-append,
//!   `duckdb_default` adding compression, cluster-by-key optimize and index building) and
//!   in-memory (`duckdb_mem_*`)
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use process_mining::{
    core::event_data::object_centric::{
        linked_ocel::SlimLinkedOCEL,
        ocel_sql::{stream_ocel_file_to_duckdb_with, DuckDbImportOptions},
    },
    Importable, OCEL,
};

mod common;

fn bench_format(c: &mut Criterion, ext: &str) {
    let src = common::order_management(ext);
    // The importer recreates its target, so one path serves every iteration.
    let tmp_dir = tempfile::tempdir().expect("create temp dir");
    let out = tmp_dir.path().join(format!("bench_import_{ext}.duckdb"));
    let opt_default = DuckDbImportOptions::default();
    let opt_raw = DuckDbImportOptions {
        compression: false,
        optimize_filesize: false,
    };

    let mut g = c.benchmark_group(format!("ocel_import_{ext}"));
    g.sample_size(10); // multi-MB fixtures: keep the sample count low

    g.bench_function("ocel_direct", |b| {
        b.iter(|| black_box(OCEL::import_from_path(black_box(&src)).unwrap()))
    });
    g.bench_function("slim_streaming", |b| {
        b.iter(|| black_box(SlimLinkedOCEL::import_from_path(black_box(&src)).unwrap()))
    });
    g.bench_function("duckdb_default", |b| {
        b.iter(|| stream_ocel_file_to_duckdb_with(&src, &out, &opt_default).unwrap())
    });
    g.bench_function("duckdb_raw", |b| {
        b.iter(|| stream_ocel_file_to_duckdb_with(&src, &out, &opt_raw).unwrap())
    });
    // In-memory DuckDB (":memory:") isolates engine cost from disk I/O; a fresh in-memory
    // database per iteration, discarded after.
    let mem = std::path::Path::new(":memory:");
    g.bench_function("duckdb_mem_default", |b| {
        b.iter(|| stream_ocel_file_to_duckdb_with(&src, mem, &opt_default).unwrap())
    });
    g.bench_function("duckdb_mem_raw", |b| {
        b.iter(|| stream_ocel_file_to_duckdb_with(&src, mem, &opt_raw).unwrap())
    });
    g.finish();
}

fn bench_import(c: &mut Criterion) {
    bench_format(c, "json");
    bench_format(c, "xml");
}

criterion_group!(benches, bench_import);
criterion_main!(benches);
