use chrono::DateTime;

use crate::ocel::ocel_struct::{OCELAttributeType, OCELType};

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

#[cfg(feature = "ocel-duckdb")]
pub(crate) mod duckdb;
pub(crate) mod export;
#[cfg(feature = "ocel-sqlite")]
pub(crate) mod sqlite;

#[cfg(feature = "ocel-duckdb")]
pub use duckdb::duckdb_ocel_export::export_ocel_duckdb_to_path;
#[cfg(feature = "ocel-duckdb")]
pub use duckdb::duckdb_ocel_import::import_ocel_duckdb_from_con;
#[cfg(feature = "ocel-duckdb")]
pub use duckdb::duckdb_ocel_import::import_ocel_duckdb_from_path;

#[cfg(feature = "ocel-sqlite")]
pub use sqlite::sqlite_ocel_export::export_ocel_sqlite_to_path;
#[cfg(feature = "ocel-sqlite")]
pub use sqlite::sqlite_ocel_export::export_ocel_sqlite_to_vec;

#[cfg(feature = "ocel-sqlite")]
pub use sqlite::sqlite_ocel_import::import_ocel_sqlite_from_con;
#[cfg(feature = "ocel-sqlite")]
pub use sqlite::sqlite_ocel_import::import_ocel_sqlite_from_path;
#[cfg(feature = "ocel-sqlite")]
pub use sqlite::sqlite_ocel_import::import_ocel_sqlite_from_slice;

pub(crate) fn sql_type_to_ocel(s: &str) -> OCELAttributeType {
    match s {
        "TEXT" => OCELAttributeType::String,
        "REAL" => OCELAttributeType::Float,
        // Used by duckdb
        "FLOAT" => OCELAttributeType::Float,
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
/// Used to abstract away from actual implementation (currently `SQLite` or `DuckDB`)
#[derive(Debug)]
pub enum DatabaseConnection<'a> {
    #[cfg(feature = "ocel-sqlite")]
    /// `SQLite` Database Connection
    SQLITE(&'a rusqlite::Connection),
    #[cfg(feature = "ocel-duckdb")]
    /// `DuckDB` Database Connection
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
/// Used to abstract away from actual implementation (currently `SQLite` or `DuckDB`)
pub enum DatabaseError {
    #[cfg(feature = "ocel-sqlite")]
    /// `SQLite` Database Error
    SQLITE(rusqlite::Error),
    #[cfg(feature = "ocel-duckdb")]
    /// `DuckDB` Database Error
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
#[cfg(all(feature = "ocel-sqlite", not(feature = "ocel-duckdb")))]
impl<P: rusqlite::Params> Params for P {}

#[cfg(all(feature = "ocel-duckdb", not(feature = "ocel-sqlite")))]
/// SQL Query Parameter
///
/// See [`::duckdb::Params`]  
pub trait Params: ::duckdb::Params {}
#[cfg(all(feature = "ocel-duckdb", not(feature = "ocel-sqlite")))]
impl<P: ::duckdb::Params> Params for P {}

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
            DatabaseConnection::SQLITE(connection) => Ok(connection.execute(query, p)?),
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => Ok(connection.execute(query, p)?),
        }
    }

    /// Execute a SQL statement without any parameters
    pub fn execute_no_params(&self, query: &str) -> Result<usize, DatabaseError> {
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => Ok(connection.execute(query, [])?),
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => Ok(connection.execute(query, [])?),
        }
    }

    /// Add rows for all OCEL objects to specified database table
    pub(crate) fn add_objects<I>(&self, table_name: &str, objects: I) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = &'a super::ocel_struct::OCELObject>,
    {
        let object_values = objects.into_iter().map(|o| [&o.id, &o.object_type]);
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => {
                for ov in object_values {
                    connection
                        .execute(&format!(r#"INSERT INTO "{table_name}" VALUES (?,?)"#), ov)?;
                }
                Ok(())
            }
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => {
                let mut ap = connection.appender(table_name)?;
                Ok(ap.append_rows(object_values)?)
            }
        }
    }
    /// Add rows for all OCEL objects to specified database table
    pub(crate) fn add_events<I>(&self, table_name: &str, events: I) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = &'a super::ocel_struct::OCELEvent>,
    {
        let event_values = events.into_iter().map(|o| [&o.id, &o.event_type]);
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => {
                for ov in event_values {
                    connection
                        .execute(&format!(r#"INSERT INTO "{table_name}" VALUES (?,?)"#), ov)?;
                }
                Ok(())
            }
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => {
                let mut ap = connection.appender(table_name)?;
                Ok(ap.append_rows(event_values)?)
            }
        }
    }

    /// Add rows for all object changes for _objects of one type_ to the specified database table (e.g., `objects_Orders`)
    pub(crate) fn add_object_changes_for_type<I>(
        &self,
        table_name: &str,
        object_type: &OCELType,
        objects: I,
    ) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = &'a super::ocel_struct::OCELObject>,
    {
        let object_values = objects.into_iter().flat_map(|o| {
            let initial_vals: Vec<_> = object_type
                .attributes
                .iter()
                .map(|a| {
                    let initial_val = o
                        .attributes
                        .iter()
                        .find(|oa| oa.name == a.name && oa.time == DateTime::UNIX_EPOCH);
                    initial_val.map(|v| v.value.to_string())
                })
                .collect();
            // let v = if initial_vals.is_empty() {
            //     Vec::default()
            // } else {
            let v = vec![(
                o.id.clone(),
                None,
                DateTime::UNIX_EPOCH.to_rfc3339(),
                initial_vals,
            )];
            // };
            v.into_iter().chain(
                o.attributes
                    .iter()
                    .filter(|a| a.time != DateTime::UNIX_EPOCH)
                    .map(|a| {
                        let vals: Vec<_> = object_type
                            .attributes
                            .iter()
                            .map(|ot_attr| {
                                if a.name == ot_attr.name {
                                    Some(a.value.to_string())
                                } else {
                                    None
                                }
                            })
                            .collect();
                        (
                            o.id.clone(),
                            Some(a.name.to_string()),
                            a.time.to_rfc3339(),
                            vals,
                        )
                    }),
            )
        });
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => {
                for (o_id, changed_field, time, values) in object_values {
                    let values: Vec<_> = values
                        .into_iter()
                        .map(|v| v.map(|v| format!("'{v}'")).unwrap_or("NULL".to_string()))
                        .collect();
                    let mut attr_vals = values.join(", ");
                    if !attr_vals.is_empty() {
                        attr_vals.insert_str(0, ", ");
                    }
                    connection.execute(
                        &format!(
                            r#"INSERT INTO "{table_name}" VALUES (?,?,{}{})"#,
                            &changed_field
                                .map(|f| format!("'{f}'"))
                                .unwrap_or("NULL".to_string()),
                            attr_vals
                        ),
                        [&o_id, &time],
                    )?;
                }
                Ok(())
            }
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => {
                let mut ap = connection.appender(table_name)?;
                let object_values: Vec<_> = object_values.collect();
                let x = object_values.iter().map(|ov| {
                    let chained: Vec<_> = vec![
                        &ov.0 as &dyn ::duckdb::ToSql,
                        &ov.2 as &dyn ::duckdb::ToSql,
                        &ov.1 as &dyn ::duckdb::ToSql,
                    ]
                    .into_iter()
                    .chain(ov.3.iter().map(|v| v as &dyn ::duckdb::ToSql))
                    .collect();

                    ::duckdb::appender_params_from_iter(chained)
                });
                ap.append_rows(x).unwrap();
                Ok(())
            }
        }
    }

    pub(crate) fn add_event_attributes_for_type<I>(
        &self,
        table_name: &str,
        event_type: &OCELType,
        events: I,
    ) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = &'a super::ocel_struct::OCELEvent>,
    {
        let event_values = events.into_iter().map(|o| {
            let values: Vec<_> = event_type
                .attributes
                .iter()
                .map(|a| {
                    let val = o.attributes.iter().find(|oa| oa.name == a.name);
                    val.map(|v| v.value.to_string())
                })
                .collect();
            // let v = if initial_vals.is_empty() {
            //     Vec::default()
            // } else {

            (o.id.clone(), o.time.to_rfc3339(), values)
        });
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => {
                for (e_id, time, values) in event_values {
                    let values: Vec<_> = values
                        .into_iter()
                        .map(|v| v.map(|v| format!("'{v}'")).unwrap_or("NULL".to_string()))
                        .collect();
                    let mut attr_vals = values.join(", ");
                    if !attr_vals.is_empty() {
                        attr_vals.insert_str(0, ", ");
                    }
                    connection.execute(
                        &format!(r#"INSERT INTO "{table_name}" VALUES (?,?{})"#, attr_vals),
                        [&e_id, &time],
                    )?;
                }
                Ok(())
            }

            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => {
                let mut ap = connection.appender(table_name)?;
                let event_values: Vec<_> = event_values.collect();
                let x = event_values.iter().map(|ov| {
                    let chained: Vec<_> =
                        vec![&ov.0 as &dyn ::duckdb::ToSql, &ov.1 as &dyn ::duckdb::ToSql]
                            .into_iter()
                            .chain(ov.2.iter().map(|v| v as &dyn ::duckdb::ToSql))
                            .collect();

                    ::duckdb::appender_params_from_iter(chained)
                });
                ap.append_rows(x).unwrap();
                Ok(())
            }
        }
    }

    /// Add rows for all OCEL objects to specified database table
    pub(crate) fn add_o2o_relationships<I>(
        &self,
        table_name: &str,
        objects: I,
    ) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = &'a super::ocel_struct::OCELObject>,
    {
        let object_values = objects.into_iter().flat_map(|o| {
            o.relationships
                .iter()
                .map(|r| [&o.id, &r.object_id, &r.qualifier])
        });
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => {
                for ov in object_values {
                    connection
                        .execute(&format!(r#"INSERT INTO "{table_name}" VALUES (?,?,?)"#), ov)?;
                }
                Ok(())
            }
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => {
                let mut ap = connection.appender(table_name)?;
                Ok(ap.append_rows(object_values)?)
            }
        }
    }

    /// Add rows for all OCEL objects to specified database table
    pub(crate) fn add_e2o_relationships<I>(
        &self,
        table_name: &str,
        events: I,
    ) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = &'a super::ocel_struct::OCELEvent>,
    {
        let event_values = events.into_iter().flat_map(|o| {
            o.relationships
                .iter()
                .map(|r| [&o.id, &r.object_id, &r.qualifier])
        });
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => {
                for ov in event_values {
                    connection
                        .execute(&format!(r#"INSERT INTO "{table_name}" VALUES (?,?,?)"#), ov)?;
                }
                Ok(())
            }
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => {
                let mut ap = connection.appender(table_name)?;
                Ok(ap.append_rows(event_values)?)
            }
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
