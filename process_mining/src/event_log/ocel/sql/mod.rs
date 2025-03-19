use crate::ocel::ocel_struct::OCELAttributeType;

pub(crate) const OCEL_ID_COLUMN: &str = "ocel_id";
pub(crate) const OCEL_TIME_COLUMN: &str = "ocel_time";
pub(crate) const OCEL_CHANGED_FIELD: &str = "ocel_changed_field";
pub(crate) const IGNORED_PRAGMA_COLUMNS: [&str; 3] =
    [OCEL_ID_COLUMN, OCEL_TIME_COLUMN, OCEL_CHANGED_FIELD];
pub(crate) const OCEL_TYPE_MAP_COLUMN: &str = "ocel_type_map";
pub(crate) const OCEL_TYPE_COLUMN: &str = "ocel_type";
pub(crate) const OCEL_O2O_SOURCE_ID_COLUMN: &str = "ocel_source_id";
pub(crate) const OCEL_O2O_TARGET_ID_COLUMN: &str = "ocel_target_id";
pub(crate) const OCEL_E2O_EVENT_ID_COLUMN: &str = "ocel_event_id";
pub(crate) const OCEL_E2O_OBJECT_ID_COLUMN: &str = "ocel_object_id";
pub(crate) const OCEL_REL_QUALIFIER_COLUMN: &str = "ocel_qualifier";

pub(crate) mod duckdb;
pub(crate) mod export;
pub(crate) mod sqlite;

pub(crate) fn sql_type_to_ocel(s: &str) -> OCELAttributeType {
    match s {
        "TEXT" => OCELAttributeType::String,
        "REAL" => OCELAttributeType::Float,
        "INTEGER" => OCELAttributeType::Integer,
        "BOOLEAN" => OCELAttributeType::Boolean,
        "TIMESTAMP" => OCELAttributeType::Time,
        _ => OCELAttributeType::String,
    }
}

pub(crate) fn ocel_type_to_sql(attr: &OCELAttributeType) -> &'static str {
    match attr {
        OCELAttributeType::String => "TEXT",
        OCELAttributeType::Float => "REAL",
        OCELAttributeType::Integer => "INTEGER",
        OCELAttributeType::Boolean => "BOOLEAN",
        OCELAttributeType::Time => "TIMESTAMP",
        _ => "TEXT",
    }
}

/// SQL Database Connection
///
/// Used to abstract away from actual implementation (currently SQLite or DuckDB)
#[derive(Debug)]
pub enum DatabaseConnection<'a> {
    #[cfg(feature = "ocel-sqlite")]
    /// SQLite Database Connection
    SQLITE(&'a rusqlite::Connection),
    #[cfg(feature = "ocel-duckdb")]
    /// DuckDB Database Connection
    DUCKDB(&'a ::duckdb::Connection),
}

#[cfg(feature = "ocel-sqlite")]
impl<'a> From<&'a rusqlite::Connection> for DatabaseConnection<'a> {
    fn from(value: &'a rusqlite::Connection) -> Self {
        Self::SQLITE(value)
    }
}
#[cfg(feature = "ocel-duckdb")]
impl<'a> From<&'a ::duckdb::Connection> for DatabaseConnection<'a> {
    fn from(value: &'a ::duckdb::Connection) -> Self {
        Self::DUCKDB(value)
    }
}

#[derive(Debug)]

/// SQL Database Error
///
/// Used to abstract away from actual implementation (currently SQLite or DuckDB)
pub enum DatabaseError {
    #[cfg(feature = "ocel-sqlite")]
    /// SQLite Database Error
    SQLITE(rusqlite::Error),
    #[cfg(feature = "ocel-duckdb")]
    /// DuckDB Database Error
    DUCKDB(::duckdb::Error),
}

#[cfg(feature = "ocel-sqlite")]
impl From<rusqlite::Error> for DatabaseError {
    fn from(value: rusqlite::Error) -> Self {
        Self::SQLITE(value)
    }
}
#[cfg(feature = "ocel-duckdb")]
impl From<::duckdb::Error> for DatabaseError {
    fn from(value: ::duckdb::Error) -> Self {
        Self::DUCKDB(value)
    }
}

#[cfg(all(not(feature = "ocel-duckdb"), not(feature = "ocel-sqlite")))]
/// SQL Query Parameter
///
/// Neither SQLite nor DuckDB is enabled!
/// No Params creation is possible.
pub trait Params {}
#[cfg(all(feature = "ocel-sqlite", not(feature = "ocel-duckdb")))]
/// SQL Query Parameter
///
/// See [`rusqlite::Params`]  
pub trait Params: rusqlite::Params {}
#[cfg(all(feature = "ocel-duckdb", not(feature = "ocel-sqlite")))]
/// SQL Query Parameter
///
/// See [`::duckdb::Params`]  
pub trait Params: ::duckdb::Params {}
#[cfg(all(feature = "ocel-duckdb", feature = "ocel-sqlite"))]
/// SQL Query Parameter
///
/// See [`rusqlite::Params`] and [`::duckdb::Params`]
pub trait Params: rusqlite::Params + ::duckdb::Params {}

#[cfg(all(feature = "ocel-duckdb", feature = "ocel-sqlite"))]
impl<P: rusqlite::Params + ::duckdb::Params> Params for P {}

impl<'a> DatabaseConnection<'a> {
    /// Execute a SQL statement with the given parameters
    pub fn execute<P: Params>(&self, query: &str, p: P) -> Result<usize, DatabaseError> {
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => Ok(connection.execute(&query, p)?),
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => Ok(connection.execute(&query, p)?),
        }
    }

    /// Execute a SQL statement without any parameters
    pub fn execute_no_params(&self, query: &str) -> Result<usize, DatabaseError> {
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => Ok(connection.execute(&query, [])?),
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => Ok(connection.execute(&query, [])?),
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs::remove_file;

    use crate::{
        export_ocel_to_sql_con, import_ocel_json_from_path, import_ocel_sqlite_from_path,
        utils::test_utils,
    };

    #[test]
    fn test_sqlite_ocel_round_trip_order() {
        let path: std::path::PathBuf = test_utils::get_test_data_path();
        let ocel =
            import_ocel_json_from_path(path.join("ocel").join("order-management.json")).unwrap();
        let export_path = path.join("export").join("roundtrip-sqlite-export.sqlite");
        let _ = remove_file(&export_path);
        let conn = rusqlite::Connection::open(&export_path).unwrap();
        export_ocel_to_sql_con(&conn, &ocel).unwrap();
        let ocel2 = import_ocel_sqlite_from_path(export_path).unwrap();
        assert_eq!(ocel.event_types.len(), ocel2.event_types.len());
        assert_eq!(ocel.object_types.len(), ocel2.object_types.len());

        assert_eq!(ocel.objects.len(), ocel2.objects.len());
        assert_eq!(ocel.events.len(), ocel2.events.len());
    }
}
