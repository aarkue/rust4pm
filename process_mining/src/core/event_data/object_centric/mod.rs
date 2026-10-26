//! Object-centric Event Data
//!

pub mod appendable;
/// Convert an OCEL to a Polars `DataFrame`
///
/// See the [`dataframe::ocel_to_dataframes`] function.
///
#[cfg(feature = "dataframes")]
pub mod dataframe;
/// Build an OCEL from relational data using a declarative blueprint.
// `ExtractionError` is deliberately descriptive (it carries the `MappingRef` a diagnostic points
// at), so it is well over clippy's `Err`-size threshold. Boxing it would shrink the per-row
// `Result`, at the cost of a `Box` in every construction site and match arm.
#[allow(clippy::result_large_err)]
#[cfg(feature = "extraction-blueprint")]
pub mod extraction;
pub mod io;
pub mod linked_ocel;
pub mod macros;
/// The OCEL 2.0 bundled CSV/Parquet format.
#[cfg(any(feature = "extraction-blueprint", feature = "ocel-bundle"))]
pub mod ocel_bundle;
pub mod ocel_csv;
pub mod ocel_json;
pub mod ocel_sql;
pub(crate) mod ocel_struct;
pub mod ocel_xml;
pub mod readable;
pub mod utils;
#[doc(inline)]
pub use ocel_struct::*;
