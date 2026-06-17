//! OCEL 2.0 SQL-based Import/Export (`SQLite` and `DuckDB`)
//!
//! 🔐 Requires the `ocel-sqlite` or `ocel-duckdb` feature to be enabled.
#![cfg(not(all(not(feature = "ocel-duckdb"), not(feature = "ocel-sqlite"))))]
use std::borrow::Cow;

use chrono::DateTime;

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

use crate::core::event_data::object_centric::ocel_struct::OCELAttributeType;
use crate::core::event_data::object_centric::ocel_struct::OCELType;

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
        // DOUBLE PRECISION instead of REAL for full f64 float support in both DuckDB and SQLite
        OCELAttributeType::Float => "DOUBLE PRECISION",
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

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseError::SQLITE(e) => write!(f, "SQLite error: {}", e),
            #[cfg(feature = "ocel-duckdb")]
            DatabaseError::DUCKDB(e) => write!(f, "DuckDB error: {}", e),
        }
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

    /// Insert one `(id, type)` row per item into `table_name`. Used by both
    /// [`add_objects`] (`(id, object_type)`) and [`add_events`] (`(id, event_type)`).
    fn add_id_type_rows<'b, T, I, F>(
        &self,
        table_name: &str,
        items: I,
        extract: F,
    ) -> Result<(), DatabaseError>
    where
        T: Clone + 'b,
        I: IntoIterator<Item = Cow<'b, T>>,
        F: for<'r> Fn(&'r T) -> [&'r String; 2],
    {
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => {
                for item in items {
                    connection.execute(
                        &format!(r#"INSERT INTO "{table_name}" VALUES (?,?)"#),
                        extract(&item),
                    )?;
                }
                Ok(())
            }
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => {
                let mut ap = connection.appender(table_name)?;
                for item in items {
                    ap.append_row(extract(&item))?;
                }
                Ok(())
            }
        }
    }

    pub(crate) fn add_objects<'b, I>(
        &self,
        table_name: &str,
        objects: I,
    ) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = Cow<'b, super::ocel_struct::OCELObject>>,
    {
        self.add_id_type_rows(table_name, objects, |o| [&o.id, &o.object_type])
    }

    pub(crate) fn add_events<'b, I>(&self, table_name: &str, events: I) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = Cow<'b, super::ocel_struct::OCELEvent>>,
    {
        self.add_id_type_rows(table_name, events, |e| [&e.id, &e.event_type])
    }

    /// Add rows for all object changes for _objects of one type_ to the specified database table (e.g., `objects_Orders`)
    pub(crate) fn add_object_changes_for_type<'b, I>(
        &self,
        table_name: &str,
        object_type: &OCELType,
        objects: I,
    ) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = Cow<'b, super::ocel_struct::OCELObject>>,
    {
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => {
                for o in objects {
                    write_object_changes_sqlite(connection, table_name, object_type, &o)?;
                }
                Ok(())
            }
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => {
                let mut ap = connection.appender(table_name)?;
                for o in objects {
                    write_object_changes_duckdb(&mut ap, object_type, &o)?;
                }
                Ok(())
            }
        }
    }

    pub(crate) fn add_event_attributes_for_type<'b, I>(
        &self,
        table_name: &str,
        event_type: &OCELType,
        events: I,
    ) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = Cow<'b, super::ocel_struct::OCELEvent>>,
    {
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => {
                for e in events {
                    write_event_attrs_sqlite(connection, table_name, event_type, &e)?;
                }
                Ok(())
            }
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => {
                let mut ap = connection.appender(table_name)?;
                for e in events {
                    write_event_attrs_duckdb(&mut ap, event_type, &e)?;
                }
                Ok(())
            }
        }
    }

    /// Insert one `(source_id, target_object_id, qualifier)` row per relationship.
    /// Used by both [`add_o2o_relationships`] and [`add_e2o_relationships`].
    fn add_relationship_rows<'b, T, I, F>(
        &self,
        table_name: &str,
        items: I,
        extract: F,
    ) -> Result<(), DatabaseError>
    where
        T: Clone + 'b,
        I: IntoIterator<Item = Cow<'b, T>>,
        F: for<'r> Fn(&'r T) -> (&'r String, &'r [super::ocel_struct::OCELRelationship]),
    {
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => {
                for item in items {
                    let (id, rels) = extract(&item);
                    for r in rels {
                        connection.execute(
                            &format!(r#"INSERT INTO "{table_name}" VALUES (?,?,?)"#),
                            [id, &r.object_id, &r.qualifier],
                        )?;
                    }
                }
                Ok(())
            }
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => {
                let mut ap = connection.appender(table_name)?;
                for item in items {
                    let (id, rels) = extract(&item);
                    for r in rels {
                        ap.append_row([id, &r.object_id, &r.qualifier])?;
                    }
                }
                Ok(())
            }
        }
    }

    pub(crate) fn add_o2o_relationships<'b, I>(
        &self,
        table_name: &str,
        objects: I,
    ) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = Cow<'b, super::ocel_struct::OCELObject>>,
    {
        self.add_relationship_rows(table_name, objects, |o| (&o.id, &o.relationships))
    }

    pub(crate) fn add_e2o_relationships<'b, I>(
        &self,
        table_name: &str,
        events: I,
    ) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = Cow<'b, super::ocel_struct::OCELEvent>>,
    {
        self.add_relationship_rows(table_name, events, |e| (&e.id, &e.relationships))
    }
}

#[cfg(feature = "ocel-sqlite")]
fn write_object_changes_sqlite(
    connection: &rusqlite::Connection,
    table_name: &str,
    object_type: &OCELType,
    o: &super::ocel_struct::OCELObject,
) -> Result<(), DatabaseError> {
    let initial_vals: Vec<_> = object_type
        .attributes
        .iter()
        .map(|a| {
            o.attributes
                .iter()
                .find(|oa| oa.name == a.name && oa.time == DateTime::UNIX_EPOCH)
                .map(|v| format!("'{}'", v.value))
                .unwrap_or_else(|| "NULL".to_string())
        })
        .collect();
    let mut attr_vals = initial_vals.join(", ");
    if !attr_vals.is_empty() {
        attr_vals.insert_str(0, ", ");
    }
    connection.execute(
        &format!(r#"INSERT INTO "{table_name}" VALUES (?,?,NULL{attr_vals})"#),
        [&o.id, &DateTime::UNIX_EPOCH.to_rfc3339()],
    )?;

    for a in o
        .attributes
        .iter()
        .filter(|a| a.time != DateTime::UNIX_EPOCH)
    {
        let vals: Vec<_> = object_type
            .attributes
            .iter()
            .map(|ot_attr| {
                if a.name == ot_attr.name {
                    format!("'{}'", a.value)
                } else {
                    "NULL".to_string()
                }
            })
            .collect();
        let mut attr_vals = vals.join(", ");
        if !attr_vals.is_empty() {
            attr_vals.insert_str(0, ", ");
        }
        connection.execute(
            &format!(
                r#"INSERT INTO "{table_name}" VALUES (?,?,'{}'{attr_vals})"#,
                a.name
            ),
            [&o.id, &a.time.to_rfc3339()],
        )?;
    }
    Ok(())
}

#[cfg(feature = "ocel-duckdb")]
fn write_object_changes_duckdb(
    ap: &mut ::duckdb::Appender<'_>,
    object_type: &OCELType,
    o: &super::ocel_struct::OCELObject,
) -> Result<(), DatabaseError> {
    let initial_vals: Vec<Option<String>> = object_type
        .attributes
        .iter()
        .map(|a| {
            o.attributes
                .iter()
                .find(|oa| oa.name == a.name && oa.time == DateTime::UNIX_EPOCH)
                .map(|v| v.value.to_string())
        })
        .collect();
    let unix = DateTime::UNIX_EPOCH.naive_utc();
    let no_field: Option<String> = None;
    let chained: Vec<&dyn ::duckdb::ToSql> = vec![
        &o.id as &dyn ::duckdb::ToSql,
        &unix as &dyn ::duckdb::ToSql,
        &no_field as &dyn ::duckdb::ToSql,
    ]
    .into_iter()
    .chain(initial_vals.iter().map(|v| v as &dyn ::duckdb::ToSql))
    .collect();
    ap.append_row(::duckdb::appender_params_from_iter(chained))?;

    for a in o
        .attributes
        .iter()
        .filter(|a| a.time != DateTime::UNIX_EPOCH)
    {
        let vals: Vec<Option<String>> = object_type
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
        let time_naive = a.time.naive_utc();
        let name_opt: Option<String> = Some(a.name.clone());
        let chained: Vec<&dyn ::duckdb::ToSql> = vec![
            &o.id as &dyn ::duckdb::ToSql,
            &time_naive as &dyn ::duckdb::ToSql,
            &name_opt as &dyn ::duckdb::ToSql,
        ]
        .into_iter()
        .chain(vals.iter().map(|v| v as &dyn ::duckdb::ToSql))
        .collect();
        ap.append_row(::duckdb::appender_params_from_iter(chained))?;
    }
    Ok(())
}

#[cfg(feature = "ocel-sqlite")]
fn write_event_attrs_sqlite(
    connection: &rusqlite::Connection,
    table_name: &str,
    event_type: &OCELType,
    e: &super::ocel_struct::OCELEvent,
) -> Result<(), DatabaseError> {
    let vals: Vec<_> = event_type
        .attributes
        .iter()
        .map(|a| {
            e.attributes
                .iter()
                .find(|ea| ea.name == a.name)
                .map(|v| format!("'{}'", v.value))
                .unwrap_or_else(|| "NULL".to_string())
        })
        .collect();
    let mut attr_vals = vals.join(", ");
    if !attr_vals.is_empty() {
        attr_vals.insert_str(0, ", ");
    }
    connection.execute(
        &format!(r#"INSERT INTO "{table_name}" VALUES (?,?{attr_vals})"#),
        [&e.id, &e.time.to_rfc3339()],
    )?;
    Ok(())
}

#[cfg(feature = "ocel-duckdb")]
fn write_event_attrs_duckdb(
    ap: &mut ::duckdb::Appender<'_>,
    event_type: &OCELType,
    e: &super::ocel_struct::OCELEvent,
) -> Result<(), DatabaseError> {
    let vals: Vec<Option<String>> = event_type
        .attributes
        .iter()
        .map(|a| {
            e.attributes
                .iter()
                .find(|ea| ea.name == a.name)
                .map(|v| v.value.to_string())
        })
        .collect();
    let time_naive = e.time.naive_utc();
    let chained: Vec<&dyn ::duckdb::ToSql> = vec![
        &e.id as &dyn ::duckdb::ToSql,
        &time_naive as &dyn ::duckdb::ToSql,
    ]
    .into_iter()
    .chain(vals.iter().map(|v| v as &dyn ::duckdb::ToSql))
    .collect();
    ap.append_row(::duckdb::appender_params_from_iter(chained))?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::fs::remove_file;

    use crate::{
        core::event_data::object_centric::{
            ocel_json::import_ocel_json_path,
            ocel_sql::{export::export_ocel_to_sql_con, import_ocel_sqlite_from_path},
        },
        test_utils,
    };

    #[test]
    fn test_sqlite_ocel_round_trip_order() {
        let path: std::path::PathBuf = test_utils::get_test_data_path();
        let ocel = import_ocel_json_path(path.join("ocel").join("order-management.json")).unwrap();
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
