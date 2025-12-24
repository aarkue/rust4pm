use crate::core::event_data::object_centric::ocel_struct::OCEL;

use super::super::export::export_ocel_to_sql_con;
use super::super::*;
use ::duckdb::Connection;

///
/// Export an [`OCEL`] to an `DuckDB` file at the specified path
///
/// Note: This function is only available if the `ocel-duckdb` feature is enabled.
///
pub fn export_ocel_duckdb_to_path<P: AsRef<std::path::Path>>(
    ocel: &OCEL,
    path: P,
) -> Result<(), DatabaseError> {
    let con = Connection::open(path)?;
    export_ocel_to_sql_con(&con, ocel)
}
