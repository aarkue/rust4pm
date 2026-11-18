//! Table/index schema (`CREATE` statements) and shared table/column name constants.
use std::borrow::Cow;
use std::collections::HashMap;

use duckdb::Connection;

use crate::core::event_data::object_centric::ocel_sql::quoted_ident;
use crate::core::event_data::object_centric::ocel_struct::OCELAttributeType;
use crate::core::event_data::object_centric::sql_columns::{
    collides_with_base_col, suffixed_column,
};
pub(crate) use crate::core::event_data::object_centric::sql_columns::{
    event_attr_column, ColumnNames, EVENT_BASE_COLS, T_EVENT_ATTR_COLUMNS,
};

pub(crate) const T_EVENTS: &str = "events";
pub(crate) const T_OBJECTS: &str = "objects";
pub(crate) const T_EVENT_ATTR_META: &str = "event_attr_meta";
pub(crate) const T_OBJECT_ATTR_META: &str = "object_attr_meta";
pub(crate) const T_OBJECT_ATTR_CHANGES: &str = "object_attribute_changes";
pub(crate) const T_E2O: &str = "e2o";
pub(crate) const T_O2O: &str = "o2o";

// Everything except `events`, which `create_events_table` creates once its attribute columns are
// known.
const CREATE_TABLES: &str = r#"
CREATE TABLE objects (id TEXT PRIMARY KEY, ocel_type TEXT);
CREATE TABLE object_attribute_changes (id TEXT, name TEXT, "time" TIMESTAMPTZ, value VARCHAR, value_type TEXT);
CREATE TABLE event_attr_meta (event_type TEXT, attr_name TEXT, attr_type TEXT);
CREATE TABLE object_attr_meta (object_type TEXT, attr_name TEXT, attr_type TEXT);
CREATE TABLE e2o (event_id TEXT, object_id TEXT, qualifier TEXT);
CREATE TABLE o2o (source_id TEXT, target_id TEXT, qualifier TEXT);
"#;

const INDEXES: &str = r#"
CREATE INDEX object_attribute_changes_id ON object_attribute_changes(id);
CREATE INDEX e2o_event ON e2o(event_id);
CREATE INDEX e2o_object ON e2o(object_id);
CREATE INDEX o2o_source ON o2o(source_id);
CREATE INDEX o2o_target ON o2o(target_id);
"#;

pub(crate) fn create_schema(con: &Connection) -> Result<(), duckdb::Error> {
    con.execute_batch(CREATE_TABLES)
}

pub(crate) fn create_indexes(con: &Connection) -> Result<(), duckdb::Error> {
    con.execute_batch(INDEXES)
}

/// `DuckDB` column type for an event-attribute [`OCELAttributeType`].
pub(crate) fn event_attr_sql_type(t: OCELAttributeType) -> &'static str {
    match t {
        OCELAttributeType::Integer => "BIGINT",
        OCELAttributeType::Float => "DOUBLE",
        OCELAttributeType::Boolean => "BOOLEAN",
        OCELAttributeType::Time => "TIMESTAMPTZ",
        OCELAttributeType::String | OCELAttributeType::Null => "VARCHAR",
    }
}

/// Inverse of [`event_attr_column`]: the attribute name an `events` column holds.
pub(crate) fn event_attr_name(column: &str) -> Cow<'_, str> {
    match column.strip_suffix("_attr") {
        Some(name) if collides_with_base_col(name, &EVENT_BASE_COLS) => Cow::Borrowed(name),
        _ => Cow::Borrowed(column),
    }
}

/// The fixed columns of an `object_<type>` wide view.
pub(crate) const OBJECT_VIEW_BASE_COLS: [&str; 1] = ["id"];

/// The alias an object attribute is pivoted into in an `object_<type>` view. Mirrors
/// [`event_attr_column`]'s collision handling, but against [`OBJECT_VIEW_BASE_COLS`].
pub(crate) fn object_attr_column(name: &str) -> Cow<'_, str> {
    suffixed_column(name, &OBJECT_VIEW_BASE_COLS)
}

/// Record `(attribute, column)` pairs in [`T_EVENT_ATTR_COLUMNS`], created on first use: only
/// a file with attribute names that differ in case alone needs it.
pub(crate) fn record_event_column_renames(
    con: &Connection,
    renames: &[(String, String)],
) -> Result<(), duckdb::Error> {
    if renames.is_empty() {
        return Ok(());
    }
    con.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {T_EVENT_ATTR_COLUMNS} (attr_name TEXT, column_name TEXT)"
    ))?;
    let mut ap = con.appender(T_EVENT_ATTR_COLUMNS)?;
    for (attr, column) in renames {
        ap.append_row([attr, column])?;
    }
    ap.flush()
}

/// The `column -> attribute` pairs [`record_event_column_renames`] recorded, if any.
pub(crate) fn event_column_renames(
    con: &Connection,
) -> Result<HashMap<String, String>, duckdb::Error> {
    let exists: bool = con.query_row(
        "SELECT count(*) > 0 FROM information_schema.tables WHERE table_name = ?",
        [T_EVENT_ATTR_COLUMNS],
        |r| r.get(0),
    )?;
    if !exists {
        return Ok(HashMap::new());
    }
    let mut stmt = con.prepare(&format!(
        "SELECT column_name, attr_name FROM {T_EVENT_ATTR_COLUMNS}"
    ))?;
    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
    rows.collect()
}

/// The `(attribute, type)` of each `events` attribute column, in column order, and the position
/// of each attribute in it.
pub(crate) type EventColumns = (Vec<(String, OCELAttributeType)>, HashMap<String, usize>);

/// Create the `events` table with one column per attribute of `attr_types`, sorted by name for
/// determinism, claiming the column names from `names`.
pub(crate) fn create_events_table(
    con: &Connection,
    attr_types: &HashMap<String, OCELAttributeType>,
    names: &mut ColumnNames,
) -> Result<EventColumns, duckdb::Error> {
    let mut attrs: Vec<(String, OCELAttributeType)> = attr_types
        .iter()
        .map(|(name, ty)| (name.clone(), *ty))
        .collect();
    attrs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let index = attrs
        .iter()
        .enumerate()
        .map(|(i, (name, _))| (name.clone(), i))
        .collect();
    let mut renames = Vec::new();
    let columns: Vec<(String, OCELAttributeType)> = attrs
        .iter()
        .map(|(name, ty)| {
            let (column, renamed) = names.claim_event_column(name);
            renames.extend(renamed);
            (column, *ty)
        })
        .collect();
    con.execute_batch(&build_events_table(&columns))?;
    record_event_column_renames(con, &renames)?;
    Ok((attrs, index))
}

/// Add an `events` column of type `ty` for attribute `name`, which arrived after
/// [`create_events_table`], claiming the column name from `names`. `ADD COLUMN` appends it after
/// all others.
pub(crate) fn add_events_column(
    con: &Connection,
    name: &str,
    ty: OCELAttributeType,
    names: &mut ColumnNames,
) -> Result<(), duckdb::Error> {
    let (column, renamed) = names.claim_event_column(name);
    con.execute_batch(&format!(
        "ALTER TABLE {T_EVENTS} ADD COLUMN {} {}",
        quoted_ident(&column),
        event_attr_sql_type(ty)
    ))?;
    record_event_column_renames(con, renamed.as_slice())
}

/// Build the `CREATE TABLE events` statement: fixed columns plus one typed column per
/// `(column, type)` in `cols`.
pub(crate) fn build_events_table(cols: &[(String, OCELAttributeType)]) -> String {
    let mut sql = String::from(
        r#"CREATE TABLE events (id TEXT PRIMARY KEY, ocel_type TEXT, "time" TIMESTAMPTZ"#,
    );
    for (column, ty) in cols {
        sql.push_str(", ");
        sql.push_str(&quoted_ident(column));
        sql.push(' ');
        sql.push_str(event_attr_sql_type(*ty));
    }
    sql.push(')');
    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_schema_makes_all_static_tables() {
        // `events` is absent here: the sink creates it once the event-attribute schema is known.
        let con = duckdb::Connection::open_in_memory().unwrap();
        create_schema(&con).unwrap();
        let mut stmt = con
            .prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name")
            .unwrap();
        let names: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            names,
            vec![
                "e2o",
                "event_attr_meta",
                "o2o",
                "object_attr_meta",
                "object_attribute_changes",
                "objects",
            ]
        );
    }

    #[test]
    fn attribute_columns_never_collide_with_base_columns() {
        let names = [
            "time",
            "id",
            "ocel_type",
            "time_attr",
            "Time",
            "ID",
            "Time_attr",
        ];
        let mut columns = ColumnNames::new(&EVENT_BASE_COLS);
        let cols: Vec<_> = names
            .iter()
            .map(|n| {
                (
                    columns.claim(&event_attr_column(n)),
                    OCELAttributeType::String,
                )
            })
            .collect();
        let con = duckdb::Connection::open_in_memory().unwrap();
        con.execute_batch(&build_events_table(&cols)).unwrap();
        for name in ["time", "id", "ocel_type", "time_attr", "Time", "ID"] {
            assert_eq!(event_attr_name(&event_attr_column(name)), name);
        }
    }

    #[test]
    fn attribute_names_differing_in_case_get_distinct_columns() {
        let con = duckdb::Connection::open_in_memory().unwrap();
        let mut columns = ColumnNames::new(&EVENT_BASE_COLS);
        let mut cols = Vec::new();
        let mut renames = Vec::new();
        for name in ["amount", "Amount", "AMOUNT", "amount_2"] {
            let (column, renamed) = columns.claim_event_column(name);
            cols.push((column, OCELAttributeType::Integer));
            renames.extend(renamed);
        }
        con.execute_batch(&build_events_table(&cols)).unwrap();
        record_event_column_renames(&con, &renames).unwrap();
        let back = event_column_renames(&con).unwrap();
        assert_eq!(back.len(), 3);
        assert_eq!(back["Amount_2"], "Amount");
        assert_eq!(back["AMOUNT_3"], "AMOUNT");
        assert_eq!(back["amount_2_2"], "amount_2");
    }

    #[test]
    fn build_events_table_quotes_and_types_columns() {
        let sql = build_events_table(&[
            ("amount".to_string(), OCELAttributeType::Float),
            ("weird \"name\"".to_string(), OCELAttributeType::String),
        ]);
        assert!(sql.contains(r#"id TEXT PRIMARY KEY, ocel_type TEXT, "time" TIMESTAMPTZ"#));
        assert!(sql.contains(r#""amount" DOUBLE"#));
        assert!(sql.contains(r#""weird ""name""" VARCHAR"#));
        let con = duckdb::Connection::open_in_memory().unwrap();
        con.execute_batch(&sql).unwrap();
    }

    #[test]
    fn timestamps_are_utc_anchored() {
        // TIMESTAMPTZ, not a bare TIMESTAMP, so stored instants are unambiguous.
        let con = duckdb::Connection::open_in_memory().unwrap();
        create_schema(&con).unwrap();
        con.execute_batch(&build_events_table(&[(
            "at".to_string(),
            OCELAttributeType::Time,
        )]))
        .unwrap();
        for (table, column) in [
            ("events", "time"),
            ("events", "at"),
            ("object_attribute_changes", "time"),
        ] {
            let ty: String = con
                .query_row(
                    "SELECT data_type FROM information_schema.columns \
                     WHERE table_name = ? AND column_name = ?",
                    [table, column],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(ty, "TIMESTAMP WITH TIME ZONE", "{table}.{column}");
        }
    }
}
