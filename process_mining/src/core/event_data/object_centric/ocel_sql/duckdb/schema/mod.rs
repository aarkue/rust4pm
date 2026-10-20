//! `DuckDB` OCEL schema (fixed tables; EAV object attributes): streaming import + on-demand wide views.
pub(crate) mod reader;
pub(crate) mod sink;
pub(crate) mod stream;
pub(crate) mod tables;
pub(crate) mod value;
pub(crate) mod views;

pub use reader::{
    read_consolidated_ocel_from_duckdb_path, read_consolidated_slim_ocel_from_duckdb_path,
    read_ocel_from_duckdb,
};
pub use stream::{
    stream_ocel_file_to_duckdb, stream_ocel_file_to_duckdb_with, write_ocel_to_duckdb,
    write_ocel_to_duckdb_with, DuckDbImportOptions,
};
pub use views::generate_type_views;
