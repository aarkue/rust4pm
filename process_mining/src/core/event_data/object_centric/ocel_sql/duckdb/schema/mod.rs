//! `DuckDB` OCEL schema (fixed tables; EAV object attributes): streaming import + on-demand wide views.
pub(crate) mod reader;
pub(crate) mod sink;
pub(crate) mod stream;
pub(crate) mod tables;
pub(crate) mod value;
pub(crate) mod views;

#[cfg(test)]
pub(crate) use reader::read_ocel_from_duckdb;
pub use stream::{stream_ocel_file_to_duckdb, write_ocel_to_duckdb, DuckDbWriteOptions};
pub use views::generate_type_views;
