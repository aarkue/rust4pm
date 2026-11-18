use crate::core::event_data::object_centric::readable::ReadableOCEL;

use super::super::export::{export_ocel_to_sql_con, export_via_temp_file};
use super::super::*;
use ::duckdb::Connection;

/// Export an OCEL to a `DuckDB` file at `path`
///
/// A file already at `path` is replaced once the export succeeded, and kept if it fails.
pub(crate) fn export_ocel_duckdb_to_path<P, O>(ocel: &O, path: P) -> Result<(), OCELIOError>
where
    P: AsRef<std::path::Path>,
    O: ReadableOCEL + ?Sized,
{
    export_via_temp_file(path.as_ref(), &[".wal"], |tmp| {
        export_ocel_to_sql_con(&Connection::open(tmp)?, ocel)
    })
}
