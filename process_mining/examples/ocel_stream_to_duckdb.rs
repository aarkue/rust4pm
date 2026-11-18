//! Stream an OCEL file in any OCEL format into a DuckDB database (consolidated schema).
//! JSON, XML and bundles stream without holding the whole log in memory, other formats are
//! imported first. An existing file at the output path is replaced.
//!
//! Run:
//!   cargo run --example ocel_stream_to_duckdb --features ocel-duckdb -- <src> <out.duckdb>
//!
use process_mining::core::event_data::object_centric::ocel_sql::{
    stream_ocel_file_to_duckdb, DuckDbWriteOptions,
};
use process_mining::OCELImportOptions;

fn main() {
    let mut args = std::env::args().skip(1);
    let src = args
        .next()
        .expect("usage: ocel_stream_to_duckdb <src> <out.duckdb>");
    let out = args
        .next()
        .expect("usage: ocel_stream_to_duckdb <src> <out.duckdb>");

    stream_ocel_file_to_duckdb(
        &src,
        &out,
        &DuckDbWriteOptions {
            overwrite: true,
            ..Default::default()
        },
        &OCELImportOptions::default(),
    )
    .expect("streaming import failed");
    println!("Wrote {out}");
}
