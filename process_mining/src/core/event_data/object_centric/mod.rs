//! Object-centric Event Data
//!

/// Convert an OCEL to a Polars `DataFrame`
///
/// See the [`dataframe::ocel_to_dataframes`] function.
///
#[cfg(feature = "dataframes")]
pub mod dataframe;
/// Graph Database OCEL Features (e.g., Export/Import)
///
#[cfg(feature = "kuzudb")]
pub mod graph_db;
pub mod linked_ocel;
pub mod macros;
pub mod ocel_json;
pub mod ocel_sql;
pub(crate) mod ocel_struct;
#[doc(inline)]
pub use ocel_struct::*;
pub mod ocel_xml;
pub mod utils;
