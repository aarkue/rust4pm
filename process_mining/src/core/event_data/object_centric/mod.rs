/// Convert an OCEL to a Polars `DataFrame`
///
/// See the [`dataframe::ocel_to_dataframes`] function.
///
#[cfg(feature = "dataframes")]
pub mod dataframe;
/// Linked OCEL 2.0, allowing convenient usage of object-centric data
pub mod linked_ocel;
/// Macros for the creation of [`ocel_struct::OCEL`]
pub mod macros;
pub mod ocel_json;
/// `SQL` OCEL 2.0 (`SQLite` and `DuckDB`)
///
#[cfg(not(all(not(feature = "ocel-duckdb"), not(feature = "ocel-sqlite"))))]
pub mod ocel_sql;
/// OCEL 2.0 struct and sub-structs
pub mod ocel_struct;
pub mod ocel_xml;
pub mod utils;

/// Graph Database OCEL Features (e.g., Export/Import)
///
#[cfg(feature = "kuzudb")]
pub mod graph_db;
