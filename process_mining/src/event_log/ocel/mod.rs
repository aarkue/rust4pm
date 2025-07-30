/// Convert an OCEL to a Polars `DataFrame`
///
/// See the [`dataframe::ocel_to_dataframes`] function.
///
#[cfg(feature = "dataframes")]
pub mod dataframe;
/// Functionality to flatten OCEL on an object type
pub mod flatten;
/// Linked OCEL 2.0, allowing convenient usage of object-centric data
pub mod linked_ocel;
/// Macros for the creation of [`crate::OCEL`]
pub mod macros;
/// OCEL 2.0 struct and sub-structs
pub mod ocel_struct;
/// `SQL` OCEL 2.0 (`SQLite` and `DuckDB`)
///
#[cfg(not(all(not(feature = "ocel-duckdb"), not(feature = "ocel-sqlite"))))]
pub mod sql;
/// XML Export for OCEL 2.0
pub mod xml_ocel_export;
#[allow(clippy::single_match)]
/// Parser for the OCEL 2.0 XML format
pub mod xml_ocel_import;

/// Graph Database OCEL Features (e.g., Export/Import)
pub mod graph_db;
