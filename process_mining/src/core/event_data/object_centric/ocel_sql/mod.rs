//! OCEL 2.0 SQL-based Import/Export (`SQLite` and `DuckDB`)
//!
//! 🔐 Requires the `ocel-sqlite` or `ocel-duckdb` feature to be enabled.
#![cfg(not(all(not(feature = "ocel-duckdb"), not(feature = "ocel-sqlite"))))]
use std::borrow::Cow;
use std::collections::HashSet;

use chrono::DateTime;
#[cfg(feature = "ocel-duckdb")]
use duckdb::schema::value::ocel_value_to_duck;
#[cfg(feature = "ocel-sqlite")]
use rusqlite::types::Value as SqliteValue;

pub(crate) use super::sql_columns::T_OCEL_ATTR_COLUMNS;

pub(crate) const OCEL_ID_COLUMN: &str = "ocel_id";
pub(crate) const OCEL_TIME_COLUMN: &str = "ocel_time";
pub(crate) const OCEL_CHANGED_FIELD: &str = "ocel_changed_field";
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
pub(crate) mod import;
#[cfg(feature = "ocel-sqlite")]
pub(crate) mod sqlite;

#[cfg(feature = "ocel-duckdb")]
pub(crate) use duckdb::duckdb_ocel_export::export_ocel_duckdb_to_path;
#[cfg(feature = "ocel-duckdb")]
pub use duckdb::duckdb_ocel_import::import_ocel_duckdb;
#[cfg(all(test, feature = "ocel-duckdb"))]
pub(crate) use duckdb::read_ocel_from_duckdb;
#[cfg(feature = "ocel-duckdb")]
pub use duckdb::{
    generate_type_views, stream_ocel_file_to_duckdb, write_ocel_to_duckdb, DuckDbWriteOptions,
};
pub use export::export_ocel_to_sql_con;

#[cfg(feature = "ocel-sqlite")]
pub(crate) use sqlite::sqlite_ocel_export::{
    export_ocel_sqlite_to_path, export_ocel_sqlite_to_vec,
};
#[cfg(feature = "ocel-sqlite")]
pub use sqlite::sqlite_ocel_import::import_ocel_sqlite;

use crate::core::event_data::object_centric::io::OCELIOError;
use crate::core::event_data::object_centric::ocel_struct::{
    OCELAttributeType, OCELAttributeValue, OCELEvent, OCELObject, OCELObjectAttribute,
    OCELRelationship, OCELType, OCELTypeAttribute,
};

/// A double-quoted SQL identifier, embedded quotes doubled. For table/column names that come
/// from the file being read, which nothing has sanitized.
pub(crate) fn quoted_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A single-quoted SQL string literal, embedded quotes doubled. For the same untrusted names
/// where a literal is needed (`PRAGMA table_info('...')`).
#[cfg(feature = "ocel-sqlite")]
pub(crate) fn quoted_str(name: &str) -> String {
    format!("'{}'", name.replace('\'', "''"))
}

/// The attribute type of a column declared as `s`: `SQLite`'s declared type as written, or
/// `DuckDB`'s normalized one (`DECIMAL(18,3)`, `TIMESTAMP WITH TIME ZONE`, ...).
pub(crate) fn sql_type_to_ocel(s: &str) -> OCELAttributeType {
    let s = s.trim().to_ascii_uppercase();
    let base = s.split('(').next().unwrap_or_default().trim_end();
    match base {
        "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" | "DECIMAL"
        | "NUMERIC" => OCELAttributeType::Float,
        "INTEGER" | "INT" | "BIGINT" | "SMALLINT" | "TINYINT" | "MEDIUMINT" | "HUGEINT"
        | "UHUGEINT" | "UBIGINT" | "UINTEGER" | "USMALLINT" | "UTINYINT" | "INT1" | "INT2"
        | "INT4" | "INT8" | "INT64" | "UNSIGNED BIG INT" => OCELAttributeType::Integer,
        "BOOLEAN" | "BOOL" => OCELAttributeType::Boolean,
        "TIMESTAMP"
        | "TIMESTAMPTZ"
        | "TIMESTAMP WITH TIME ZONE"
        | "TIMESTAMP_S"
        | "TIMESTAMP_MS"
        | "TIMESTAMP_US"
        | "TIMESTAMP_NS"
        | "DATETIME"
        | "DATE" => OCELAttributeType::Time,
        _ => OCELAttributeType::String,
    }
}

pub(crate) fn ocel_type_to_sql(attr: &OCELAttributeType) -> &'static str {
    match attr {
        OCELAttributeType::String => "TEXT",
        // DOUBLE PRECISION instead of REAL for full f64 float support in both DuckDB and SQLite
        OCELAttributeType::Float => "DOUBLE PRECISION",
        // `INTEGER` is 32-bit in DuckDB. In SQLite, BIGINT has the same INTEGER affinity.
        OCELAttributeType::Integer => "BIGINT",
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

#[cfg(all(not(feature = "ocel-duckdb"), not(feature = "ocel-sqlite")))]
/// SQL Query Parameter
///
/// Neither SQLite nor DuckDB is enabled!
/// No Params creation is possible.
pub(crate) trait Params {}
#[cfg(all(feature = "ocel-sqlite", not(feature = "ocel-duckdb")))]
/// SQL Query Parameter
///
/// See [`rusqlite::Params`]  
pub(crate) trait Params: rusqlite::Params {}
#[cfg(all(feature = "ocel-sqlite", not(feature = "ocel-duckdb")))]
impl<P: rusqlite::Params> Params for P {}

#[cfg(all(feature = "ocel-duckdb", not(feature = "ocel-sqlite")))]
/// SQL Query Parameter
///
/// See [`::duckdb::Params`]  
pub(crate) trait Params: ::duckdb::Params {}
#[cfg(all(feature = "ocel-duckdb", not(feature = "ocel-sqlite")))]
impl<P: ::duckdb::Params> Params for P {}

#[cfg(all(feature = "ocel-duckdb", feature = "ocel-sqlite"))]
/// SQL Query Parameter
///
/// See [`rusqlite::Params`] and [`::duckdb::Params`]
pub(crate) trait Params: rusqlite::Params + ::duckdb::Params {}
#[cfg(all(feature = "ocel-duckdb", feature = "ocel-sqlite"))]
impl<P: rusqlite::Params + ::duckdb::Params> Params for P {}

impl<'a> DatabaseConnection<'a> {
    /// Execute a SQL statement with the given parameters
    pub(crate) fn execute<P: Params>(&self, query: &str, p: P) -> Result<usize, OCELIOError> {
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => Ok(connection.execute(query, p)?),
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => Ok(connection.execute(query, p)?),
        }
    }

    /// Execute a SQL statement without any parameters
    pub(crate) fn execute_no_params(&self, query: &str) -> Result<usize, OCELIOError> {
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => Ok(connection.execute(query, [])?),
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => Ok(connection.execute(query, [])?),
        }
    }

    /// Insert one `(id, type)` row per item into `table_name`.
    fn add_id_type_rows<'b, T, I, F>(
        &self,
        table_name: &str,
        items: I,
        extract: F,
    ) -> Result<(), OCELIOError>
    where
        T: Clone + 'b,
        I: IntoIterator<Item = Cow<'b, T>>,
        F: for<'r> Fn(&'r T) -> [&'r String; 2],
    {
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => {
                let mut insert =
                    connection.prepare(&format!(r#"INSERT INTO "{table_name}" VALUES (?,?)"#))?;
                for item in items {
                    insert.execute(extract(&item))?;
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

    pub(crate) fn add_objects<'b, I>(&self, table_name: &str, objects: I) -> Result<(), OCELIOError>
    where
        I: IntoIterator<Item = Cow<'b, OCELObject>>,
    {
        self.add_id_type_rows(table_name, objects, |o| [&o.id, &o.object_type])
    }

    pub(crate) fn add_events<'b, I>(&self, table_name: &str, events: I) -> Result<(), OCELIOError>
    where
        I: IntoIterator<Item = Cow<'b, OCELEvent>>,
    {
        self.add_id_type_rows(table_name, events, |e| [&e.id, &e.event_type])
    }

    /// Add the rows of `objects`, all of `object_type`, to its table `table_name`: one with the
    /// initial values, and one per later attribute change.
    pub(crate) fn add_object_changes_for_type<'b, I>(
        &self,
        table_name: &str,
        object_type: &OCELType,
        objects: I,
    ) -> Result<(), OCELIOError>
    where
        I: IntoIterator<Item = Cow<'b, OCELObject>>,
    {
        let columns = TypeColumns::new(object_type);
        let mut norm = ValueNormalizer::default();
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => {
                let placeholders = ",?".repeat(object_type.attributes.len());
                let mut insert = connection.prepare(&format!(
                    r#"INSERT INTO "{table_name}" VALUES (?,?,?{placeholders})"#
                ))?;
                for o in objects {
                    for row in object_rows(&columns, &o, &mut norm) {
                        insert_sqlite_row(&mut insert, row)?;
                    }
                }
                Ok(())
            }
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => {
                let mut ap = connection.appender(table_name)?;
                for o in objects {
                    for row in object_rows(&columns, &o, &mut norm) {
                        append_duckdb_row(&mut ap, row)?;
                    }
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
    ) -> Result<(), OCELIOError>
    where
        I: IntoIterator<Item = Cow<'b, OCELEvent>>,
    {
        let columns = TypeColumns::new(event_type);
        let mut norm = ValueNormalizer::default();
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => {
                let placeholders = ",?".repeat(event_type.attributes.len());
                let mut insert = connection.prepare(&format!(
                    r#"INSERT INTO "{table_name}" VALUES (?,?{placeholders})"#
                ))?;
                for e in events {
                    insert_sqlite_row(&mut insert, event_row(&columns, &e, &mut norm))?;
                }
                Ok(())
            }
            #[cfg(feature = "ocel-duckdb")]
            DatabaseConnection::DUCKDB(connection) => {
                let mut ap = connection.appender(table_name)?;
                for e in events {
                    append_duckdb_row(&mut ap, event_row(&columns, &e, &mut norm))?;
                }
                Ok(())
            }
        }
    }

    /// Insert one `(source_id, target_object_id, qualifier)` row per relationship.
    fn add_relationship_rows<'b, T, I, F>(
        &self,
        table_name: &str,
        items: I,
        extract: F,
    ) -> Result<(), OCELIOError>
    where
        T: Clone + 'b,
        I: IntoIterator<Item = Cow<'b, T>>,
        F: for<'r> Fn(&'r T) -> (&'r String, &'r [OCELRelationship]),
    {
        match self {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseConnection::SQLITE(connection) => {
                let mut insert =
                    connection.prepare(&format!(r#"INSERT INTO "{table_name}" VALUES (?,?,?)"#))?;
                for item in items {
                    let (id, rels) = extract(&item);
                    for r in rels {
                        insert.execute([id, &r.object_id, &r.qualifier])?;
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
    ) -> Result<(), OCELIOError>
    where
        I: IntoIterator<Item = Cow<'b, OCELObject>>,
    {
        self.add_relationship_rows(table_name, objects, |o| (&o.id, &o.relationships))
    }

    pub(crate) fn add_e2o_relationships<'b, I>(
        &self,
        table_name: &str,
        events: I,
    ) -> Result<(), OCELIOError>
    where
        I: IntoIterator<Item = Cow<'b, OCELEvent>>,
    {
        self.add_relationship_rows(table_name, events, |e| (&e.id, &e.relationships))
    }
}

/// Converts attribute values to the declared type of their column with
/// [`OCELAttributeValue::normalize_to`], so both SQL formats store the same values. A value that
/// does not convert is written as `NULL`, with one warning per type and attribute.
#[derive(Debug, Default)]
pub(crate) struct ValueNormalizer {
    warned: HashSet<(String, String)>,
}

impl ValueNormalizer {
    pub(crate) fn normalize(
        &mut self,
        v: &OCELAttributeValue,
        target: OCELAttributeType,
        type_name: &str,
        attr_name: &str,
    ) -> OCELAttributeValue {
        v.normalize_to(target).unwrap_or_else(|| {
            if self
                .warned
                .insert((type_name.to_string(), attr_name.to_string()))
            {
                log::warn!(
                    "attribute '{attr_name}' of type '{type_name}' has {} values that do not \
                     convert to its declared type {}. They are written as NULL.",
                    v.get_type().as_type_str(),
                    target.as_type_str(),
                );
            }
            OCELAttributeValue::Null
        })
    }
}

/// The attribute columns of one type's table, with the type each column holds.
struct TypeColumns<'t> {
    ty: &'t OCELType,
    targets: Vec<OCELAttributeType>,
}

impl<'t> TypeColumns<'t> {
    fn new(ty: &'t OCELType) -> Self {
        let targets = ty
            .attributes
            .iter()
            .map(|a| OCELAttributeType::from_type_str(&a.value_type))
            .collect();
        Self { ty, targets }
    }

    /// `v`, a value of the `i`th attribute, converted to the type of its column.
    fn value(
        &self,
        norm: &mut ValueNormalizer,
        i: usize,
        v: &OCELAttributeValue,
    ) -> OCELAttributeValue {
        norm.normalize(
            v,
            self.targets[i],
            &self.ty.name,
            &self.ty.attributes[i].name,
        )
    }
}

/// The `SQLite` value stored for a value already normalized to its column's type.
///
/// Typed columns cannot convert the `Display` forms of `Null` (`''`) and booleans (`'true'`), so
/// they are stored as SQL `NULL` and as `0`/`1`, `SQLite`'s own boolean representation.
#[cfg(feature = "ocel-sqlite")]
fn ocel_value_to_sqlite(v: OCELAttributeValue) -> SqliteValue {
    match v {
        OCELAttributeValue::Null => SqliteValue::Null,
        OCELAttributeValue::String(s) => SqliteValue::Text(s),
        OCELAttributeValue::Integer(i) => SqliteValue::Integer(i),
        OCELAttributeValue::Float(f) => SqliteValue::Real(f),
        OCELAttributeValue::Boolean(b) => SqliteValue::Integer(i64::from(b)),
        OCELAttributeValue::Time(t) => SqliteValue::Text(t.to_rfc3339()),
    }
}

/// The rows of `o` in its type's table, as `[id, time, changed field, attribute values...]`: one
/// with its values at the epoch and no changed field, then one per later change.
fn object_rows(
    columns: &TypeColumns<'_>,
    o: &OCELObject,
    norm: &mut ValueNormalizer,
) -> Vec<Vec<OCELAttributeValue>> {
    let id = || OCELAttributeValue::String(o.id.clone());
    let mut rows = Vec::new();
    let initial = columns.ty.attributes.iter().enumerate().map(|(i, a)| {
        o.attributes
            .iter()
            .find(|oa| oa.name == a.name && oa.time == DateTime::UNIX_EPOCH)
            .map_or(OCELAttributeValue::Null, |v| {
                columns.value(norm, i, &v.value)
            })
    });
    rows.push(
        [
            id(),
            OCELAttributeValue::Time(DateTime::UNIX_EPOCH.into()),
            OCELAttributeValue::Null,
        ]
        .into_iter()
        .chain(initial)
        .collect(),
    );
    for a in o
        .attributes
        .iter()
        .filter(|a| a.time != DateTime::UNIX_EPOCH)
    {
        // A `NULL` in the changed column is unambiguous here: `ocel_changed_field` says the
        // attribute changed, so the importer reads it back as `OCELAttributeValue::Null`.
        let values = columns
            .ty
            .attributes
            .iter()
            .enumerate()
            .map(|(i, ot_attr)| {
                if a.name == ot_attr.name {
                    columns.value(norm, i, &a.value)
                } else {
                    OCELAttributeValue::Null
                }
            });
        rows.push(
            [
                id(),
                OCELAttributeValue::Time(a.time),
                OCELAttributeValue::String(a.name.clone()),
            ]
            .into_iter()
            .chain(values)
            .collect(),
        );
    }
    rows
}

/// The row of `e` in its type's table, as `[id, time, attribute values...]`.
fn event_row(
    columns: &TypeColumns<'_>,
    e: &OCELEvent,
    norm: &mut ValueNormalizer,
) -> Vec<OCELAttributeValue> {
    let values = columns.ty.attributes.iter().enumerate().map(|(i, a)| {
        e.attributes
            .iter()
            .find(|ea| ea.name == a.name)
            .map_or(OCELAttributeValue::Null, |v| {
                columns.value(norm, i, &v.value)
            })
    });
    [
        OCELAttributeValue::String(e.id.clone()),
        OCELAttributeValue::Time(e.time),
    ]
    .into_iter()
    .chain(values)
    .collect()
}

#[cfg(feature = "ocel-sqlite")]
fn insert_sqlite_row(
    insert: &mut rusqlite::Statement<'_>,
    row: Vec<OCELAttributeValue>,
) -> Result<(), OCELIOError> {
    insert.execute(rusqlite::params_from_iter(
        row.into_iter().map(ocel_value_to_sqlite),
    ))?;
    Ok(())
}

#[cfg(feature = "ocel-duckdb")]
fn append_duckdb_row(
    ap: &mut ::duckdb::Appender<'_>,
    row: Vec<OCELAttributeValue>,
) -> Result<(), OCELIOError> {
    ap.append_row(::duckdb::appender_params_from_iter(
        row.into_iter().map(ocel_value_to_duck),
    ))?;
    Ok(())
}

#[cfg(all(test, feature = "ocel-sqlite"))]
mod test {
    use std::fs::remove_file;

    use crate::{
        core::event_data::object_centric::ocel_sql::{export_ocel_to_sql_con, import_ocel_sqlite},
        test_utils, Importable, OCEL,
    };

    #[test]
    fn test_sqlite_ocel_round_trip_order() {
        let path: std::path::PathBuf = test_utils::get_test_data_path();
        let ocel = OCEL::import_from_path(path.join("ocel").join("order-management.json")).unwrap();
        let export_path = path.join("export").join("roundtrip-sqlite-export.sqlite");
        let _ = remove_file(&export_path);
        let conn = rusqlite::Connection::open(&export_path).unwrap();
        export_ocel_to_sql_con(&conn, &ocel).unwrap();
        let ocel2 = import_ocel_sqlite(&conn, &Default::default()).unwrap();
        assert_eq!(ocel.event_types.len(), ocel2.event_types.len());
        assert_eq!(ocel.object_types.len(), ocel2.object_types.len());

        assert_eq!(ocel.objects.len(), ocel2.objects.len());
        assert_eq!(ocel.events.len(), ocel2.events.len());
    }
}

/// A file in which the `object` table lists objects that no type table has: `Truck` has no
/// attributes and an empty table, `Crate` is not in `object_map_type`. `b1` has two initial
/// rows, and `e2` has a type without a table.
#[cfg(test)]
pub(crate) mod object_table_fixture {
    use crate::core::event_data::object_centric::ocel_struct::{
        OCELAttributeValue, OCELRelationship, OCEL,
    };

    pub(crate) const SQL: &str = "
        CREATE TABLE event_map_type (ocel_type TEXT, ocel_type_map TEXT);
        CREATE TABLE object_map_type (ocel_type TEXT, ocel_type_map TEXT);
        CREATE TABLE event (ocel_id TEXT, ocel_type TEXT);
        CREATE TABLE object (ocel_id TEXT, ocel_type TEXT);
        CREATE TABLE event_object (ocel_event_id TEXT, ocel_object_id TEXT, ocel_qualifier TEXT);
        CREATE TABLE object_object (ocel_source_id TEXT, ocel_target_id TEXT, ocel_qualifier TEXT);
        CREATE TABLE event_Ev (ocel_id TEXT, ocel_time TIMESTAMP);
        CREATE TABLE object_Truck (ocel_id TEXT, ocel_time TIMESTAMP);
        CREATE TABLE object_Box (
            ocel_id TEXT, ocel_time TIMESTAMP, ocel_changed_field TEXT, weight REAL, color TEXT
        );
        INSERT INTO event_map_type VALUES ('Ev', 'Ev');
        INSERT INTO object_map_type VALUES ('Truck', 'Truck'), ('Box', 'Box');
        INSERT INTO event VALUES ('e1', 'Ev'), ('e2', 'Ghost');
        INSERT INTO event_Ev VALUES ('e1', '2020-01-01T00:00:00+00:00');
        INSERT INTO object VALUES
            ('t1', 'Truck'), ('t2', 'Truck'), ('b1', 'Box'), ('b2', 'Box'), ('x1', 'Crate');
        INSERT INTO object_Box VALUES ('b1', '1970-01-01T00:00:00+00:00', NULL, 1.5, NULL);
        INSERT INTO object_Box VALUES ('b1', '1970-01-01T00:00:00+00:00', NULL, 9.0, 'red');
        INSERT INTO object_object VALUES ('t1', 'b1', 'carries');
        INSERT INTO event_object VALUES ('e1', 't1', 'uses');
    ";

    pub(crate) fn check(ocel: &OCEL) {
        let mut objects: Vec<_> = ocel
            .objects
            .iter()
            .map(|o| (o.id.as_str(), o.object_type.as_str()))
            .collect();
        objects.sort_unstable();
        assert_eq!(
            objects,
            [
                ("b1", "Box"),
                ("b2", "Box"),
                ("t1", "Truck"),
                ("t2", "Truck"),
                ("x1", "Crate")
            ]
        );
        let mut types: Vec<_> = ocel.object_types.iter().map(|t| t.name.as_str()).collect();
        types.sort_unstable();
        assert_eq!(types, ["Box", "Crate", "Truck"]);

        let ob = |id: &str| ocel.objects.iter().find(|o| o.id == id).unwrap();
        assert!(ob("t2").attributes.is_empty());
        assert_eq!(
            ob("t1").relationships,
            [OCELRelationship {
                object_id: "b1".into(),
                qualifier: "carries".into()
            }]
        );
        let mut b1: Vec<_> = ob("b1")
            .attributes
            .iter()
            .map(|a| (a.name.as_str(), a.value.clone()))
            .collect();
        b1.sort_by(|a, b| a.0.cmp(b.0));
        assert_eq!(
            b1,
            [
                ("color", OCELAttributeValue::String("red".into())),
                ("weight", OCELAttributeValue::Float(1.5)),
            ],
            "the first initial row wins, later ones only fill in its NULLs"
        );

        assert_eq!(ocel.events.len(), 1);
        assert_eq!(ocel.events[0].relationships.len(), 1);
    }
}
