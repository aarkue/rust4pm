//! Stream an OCEL file directly into a DuckDB database (consolidated schema),
//! without materializing the whole log in memory.
//!
//! Run:
//!   cargo run --example ocel_stream_to_duckdb --features ocel-duckdb -- <src.json|.xml> <out.duckdb>
//!
use process_mining::core::event_data::object_centric::ocel_sql::{
    stream_ocel_file_to_duckdb_with, DuckDbImportOptions,
};

fn main() {
    let mut args = std::env::args().skip(1);
    let src = args
        .next()
        .expect("usage: ocel_stream_to_duckdb <src.json|.xml> <out.duckdb>");
    let out = args
        .next()
        .expect("usage: ocel_stream_to_duckdb <src.json|.xml> <out.duckdb>");

    let options = DuckDbImportOptions::default();
    stream_ocel_file_to_duckdb_with(&src, &out, &options).expect("streaming import failed");
    println!("Wrote {out}");
}
