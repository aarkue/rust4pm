use crate::core::event_data::object_centric::{ocel_struct::OCEL, readable::ReadableOCEL};

use super::super::export::export_ocel_to_sql_con;
use super::super::*;
use ::duckdb::Connection;
use macros_process_mining::register_binding;

///
/// Export an OCEL to a `DuckDB` file at the specified path
///
/// Note: This function is only available if the `ocel-duckdb` feature is enabled.
///
pub fn export_ocel_duckdb_to_path<P, O>(ocel: &O, path: P) -> Result<(), DatabaseError>
where
    P: AsRef<std::path::Path>,
    O: ReadableOCEL + ?Sized,
{
    if path.as_ref().exists() {
        let _ = std::fs::remove_file(&path);
    }
    let con = Connection::open(path)?;
    export_ocel_to_sql_con(&con, ocel)
}

#[register_binding(name = "export_ocel_duckdb_to_path", stringify_error)]
fn export_ocel_duckdb_to_path_binding(
    ocel: &OCEL,
    path: impl AsRef<std::path::Path>,
) -> Result<(), DatabaseError> {
    export_ocel_duckdb_to_path(ocel, path)
}
