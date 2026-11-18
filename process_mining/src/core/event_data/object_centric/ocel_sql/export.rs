use std::collections::BTreeSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::core::event_data::object_centric::readable::ReadableOCEL;
use crate::core::event_data::object_centric::sql_columns::{
    ocel_sql_attr_columns, ColumnNames, T_OCEL_ATTR_COLUMNS,
};

use super::*;
/// Export an OCEL log to an SQL Database connection.
///
/// Accepts any [`ReadableOCEL`] backend.
///
/// Available with the `ocel-sqlite` or `ocel-duckdb` feature.
pub fn export_ocel_to_sql_con<'a, DC, O>(con: DC, ocel: &O) -> Result<(), OCELIOError>
where
    DC: Into<DatabaseConnection<'a>>,
    O: ReadableOCEL + ?Sized,
{
    let con = con.into();
    con.execute_no_params("BEGIN TRANSACTION")?;
    // event map type
    con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "event_map_type" ("{OCEL_TYPE_COLUMN}" TEXT, "{OCEL_TYPE_MAP_COLUMN}"	TEXT, PRIMARY KEY("{OCEL_TYPE_COLUMN}"))"#))?;
    // object map type
    con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "object_map_type" ("{OCEL_TYPE_COLUMN}" TEXT, "{OCEL_TYPE_MAP_COLUMN}"	TEXT, PRIMARY KEY("{OCEL_TYPE_COLUMN}"))"#))?;

    // event
    con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "event" ("{OCEL_ID_COLUMN}"	TEXT, "{OCEL_TYPE_COLUMN}"	TEXT, PRIMARY KEY("{OCEL_ID_COLUMN}"), FOREIGN KEY("{OCEL_TYPE_COLUMN}") REFERENCES "event_map_type" ("{OCEL_TYPE_COLUMN}"))"#))?;
    // object
    con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "object" ("{OCEL_ID_COLUMN}"	TEXT, "{OCEL_TYPE_COLUMN}"	TEXT, PRIMARY KEY("{OCEL_ID_COLUMN}"), FOREIGN KEY("{OCEL_TYPE_COLUMN}") REFERENCES "object_map_type" ("{OCEL_TYPE_COLUMN}"))"#))?;

    // O2O (object_object)
    // , FOREIGN KEY("{OCEL_O2O_SOURCE_ID_COLUMN}") REFERENCES "object"("{OCEL_ID_COLUMN}"), FOREIGN KEY("{OCEL_O2O_TARGET_ID_COLUMN}") REFERENCES "object"("{OCEL_ID_COLUMN}")
    con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "object_object" ("{OCEL_O2O_SOURCE_ID_COLUMN}" TEXT, "{OCEL_O2O_TARGET_ID_COLUMN}" TEXT, "{OCEL_REL_QUALIFIER_COLUMN}" TEXT, PRIMARY KEY("{OCEL_O2O_SOURCE_ID_COLUMN}", "{OCEL_O2O_TARGET_ID_COLUMN}", "{OCEL_REL_QUALIFIER_COLUMN}"))"#))?;
    // E2O (event_object)
    con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "event_object" ("{OCEL_E2O_EVENT_ID_COLUMN}" TEXT, "{OCEL_E2O_OBJECT_ID_COLUMN}" TEXT, "{OCEL_REL_QUALIFIER_COLUMN}" TEXT, PRIMARY KEY("{OCEL_E2O_EVENT_ID_COLUMN}", "{OCEL_E2O_OBJECT_ID_COLUMN}", "{OCEL_REL_QUALIFIER_COLUMN}"), FOREIGN KEY("{OCEL_E2O_EVENT_ID_COLUMN}") REFERENCES "event"("{OCEL_ID_COLUMN}"), FOREIGN KEY("{OCEL_E2O_OBJECT_ID_COLUMN}") REFERENCES "object"("{OCEL_ID_COLUMN}"))"#))?;

    con.execute_no_params(&format!(
        r#"CREATE INDEX IF NOT EXISTS "event_id" ON "event" ("{OCEL_ID_COLUMN}" ASC)"#
    ))?;
    con.execute_no_params(&format!(
        r#"CREATE INDEX IF NOT EXISTS "object_id" ON "object" ("{OCEL_ID_COLUMN}" ASC)"#
    ))?;

    con.execute_no_params(&format!(r#"CREATE INDEX IF NOT EXISTS "event_object_source" ON "event_object" ("{OCEL_E2O_EVENT_ID_COLUMN}" ASC)"#))?;
    con.execute_no_params(&format!(r#"CREATE INDEX IF NOT EXISTS "event_object_target" ON "event_object" ("{OCEL_E2O_OBJECT_ID_COLUMN}" ASC)"#))?;
    con.execute_no_params(&format!(r#"CREATE INDEX IF NOT EXISTS "event_object_both" ON "event_object" ("{OCEL_E2O_EVENT_ID_COLUMN}","{OCEL_E2O_OBJECT_ID_COLUMN}" ASC)"#))?;

    con.execute_no_params(&format!(r#"CREATE INDEX IF NOT EXISTS "object_object_source" ON "object_object" ("{OCEL_O2O_SOURCE_ID_COLUMN}" ASC)"#))?;
    con.execute_no_params(&format!(r#"CREATE INDEX IF NOT EXISTS "object_object_target" ON "object_object" ("{OCEL_O2O_TARGET_ID_COLUMN}" ASC)"#))?;
    con.execute_no_params(&format!(r#"CREATE INDEX IF NOT EXISTS "object_object_both" ON "object_object" ("{OCEL_O2O_SOURCE_ID_COLUMN}","{OCEL_O2O_TARGET_ID_COLUMN}" ASC)"#))?;

    con.execute_no_params("COMMIT")?;
    con.execute_no_params("BEGIN TRANSACTION")?;
    let event_types = with_undeclared(ocel.event_types(), ocel.undeclared_event_types());
    let object_types = with_undeclared(ocel.object_types(), ocel.undeclared_object_types());
    let (event_tables, object_tables) = type_tables(&event_types, &object_types);

    let mut renamed_columns: Vec<[String; 3]> = Vec::new();

    for (et, t) in event_types.iter().zip(&event_tables) {
        let table = &t.table;
        let mut attr_cols = column_definitions(et, table, &mut renamed_columns);
        if !attr_cols.is_empty() {
            attr_cols.push(',');
        }
        con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "{table}" ("{OCEL_ID_COLUMN}"	TEXT, "{OCEL_TIME_COLUMN}"	TIMESTAMP,{attr_cols} PRIMARY KEY("{OCEL_ID_COLUMN}"))"#))?;

        con.execute(
            r#"INSERT INTO "event_map_type" VALUES (?, ?)"#,
            [&et.name, &t.map_name],
        )?;
    }

    for (ot, t) in object_types.iter().zip(&object_tables) {
        let table = &t.table;
        let mut attr_cols = column_definitions(ot, table, &mut renamed_columns);
        if !attr_cols.is_empty() {
            attr_cols.insert_str(0, ", ");
        }
        con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "{table}" ("{OCEL_ID_COLUMN}"	TEXT, "{OCEL_TIME_COLUMN}" TIMESTAMP, {OCEL_CHANGED_FIELD} TEXT{attr_cols})"#))?;

        con.execute(
            r#"INSERT INTO "object_map_type" VALUES (?, ?)"#,
            [&ot.name, &t.map_name],
        )?;
    }

    if !renamed_columns.is_empty() {
        con.execute_no_params(&format!(
            r#"CREATE TABLE IF NOT EXISTS "{T_OCEL_ATTR_COLUMNS}" ("table_name" TEXT, "attr_name" TEXT, "column_name" TEXT)"#
        ))?;
        for [table, attr, column] in &renamed_columns {
            con.execute(
                &format!(r#"INSERT INTO "{T_OCEL_ATTR_COLUMNS}" VALUES (?, ?, ?)"#),
                [table, attr, column],
            )?;
        }
    }

    con.add_objects("object", ocel.iter_objects())?;

    for (ot, t) in object_types.iter().zip(&object_tables) {
        con.add_object_changes_for_type(&t.table, ot, ocel.iter_objects_of_type(&ot.name))?;
    }

    con.execute_no_params("COMMIT")?;
    con.execute_no_params("BEGIN TRANSACTION")?;
    con.add_o2o_relationships("object_object", ocel.iter_objects())?;
    con.execute_no_params("COMMIT")?;
    con.execute_no_params("BEGIN TRANSACTION")?;

    con.add_events("event", ocel.iter_events())?;

    for (et, t) in event_types.iter().zip(&event_tables) {
        con.add_event_attributes_for_type(&t.table, et, ocel.iter_events_of_type(&et.name))?;
    }
    con.execute_no_params("COMMIT")?;
    con.execute_no_params("BEGIN TRANSACTION")?;
    con.add_e2o_relationships("event_object", ocel.iter_events())?;

    con.execute_no_params("COMMIT")?;
    con.execute_no_params("BEGIN TRANSACTION")?;

    for TypeTable { table, index, .. } in object_tables.iter().chain(&event_tables) {
        con.execute_no_params(&format!(
            r#"CREATE INDEX IF NOT EXISTS "{index}" ON "{table}" ("{OCEL_ID_COLUMN}" ASC)"#
        ))?;
    }
    con.execute_no_params("COMMIT")?;

    Ok(())
}

/// Run `export` on a new file next to `path`, then move that file to `path`, so a failed export
/// leaves a file already at `path` as it was. `sidecars` are the suffixes of the files the
/// database keeps next to its own (`.wal`): a stale one at `path` would be read with the new file.
pub(crate) fn export_via_temp_file(
    path: &Path,
    sidecars: &[&str],
    export: impl FnOnce(&Path) -> Result<(), OCELIOError>,
) -> Result<(), OCELIOError> {
    let tmp = unused_temp_path(path, sidecars);
    let remove_tmp = || {
        let _ = std::fs::remove_file(&tmp);
        for s in sidecars {
            let _ = std::fs::remove_file(with_suffix(&tmp, s));
        }
    };
    if let Err(e) = export(&tmp) {
        remove_tmp();
        return Err(e);
    }
    let mut moved = std::fs::rename(&tmp, path);
    for s in sidecars {
        if moved.is_err() {
            break;
        }
        let (from, to) = (with_suffix(&tmp, s), with_suffix(path, s));
        moved = if from.exists() {
            std::fs::rename(from, to)
        } else {
            match std::fs::remove_file(to) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
                _ => Ok(()),
            }
        };
    }
    moved.map_err(|e| {
        remove_tmp();
        OCELIOError::Io(e)
    })
}

/// A path next to `path` that neither exists nor has any of `sidecars`.
fn unused_temp_path(path: &Path, sidecars: &[&str]) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    loop {
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp = path.with_file_name(format!(".{name}.{}-{n}.tmp", std::process::id()));
        if !tmp.exists() && sidecars.iter().all(|s| !with_suffix(&tmp, s).exists()) {
            return tmp;
        }
    }
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut p = OsString::from(path);
    p.push(suffix);
    p.into()
}

/// `declared`, followed by a type without attributes for each of `undeclared`, so every type
/// in the `event`/`object` table gets a `*_map_type` row (which its foreign key, and pm4py,
/// require) and a table.
fn with_undeclared(declared: &[OCELType], undeclared: BTreeSet<String>) -> Vec<OCELType> {
    let mut types = declared.to_vec();
    types.extend(undeclared.into_iter().map(|name| OCELType {
        name,
        attributes: Vec::new(),
    }));
    types
}

/// The `<name> <type>` column definitions for the attributes of `t`, stored in `table`.
///
/// Column names are quoted rather than cleaned, so a column is named like its attribute unless
/// that name is reserved (`ocel_id`) or taken case-insensitively (`Price` next to `price`). Such
/// a column is renamed and pushed to `renamed` as a [`T_OCEL_ATTR_COLUMNS`] row, from which the
/// importers restore the attribute name. `ocel_changed_field` always holds the attribute name.
fn column_definitions(t: &OCELType, table: &str, renamed: &mut Vec<[String; 3]>) -> String {
    let columns = ocel_sql_attr_columns(t.attributes.iter().map(|a| a.name.as_str()));
    t.attributes
        .iter()
        .zip(columns)
        .map(|(att, column)| {
            let def = format!(
                "{} {}",
                quoted_ident(&column),
                ocel_type_to_sql(&OCELAttributeType::from_type_str(&att.value_type))
            );
            if column != att.name {
                renamed.push([table.to_string(), att.name.clone(), column]);
            }
            def
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The fixed tables and indexes. `SQLite` keeps table and index names in one namespace.
const FIXED_NAMES: [&str; 15] = [
    "event_map_type",
    "object_map_type",
    "event",
    "object",
    "event_object",
    "object_object",
    "event_id",
    "object_id",
    "event_object_source",
    "event_object_target",
    "event_object_both",
    "object_object_source",
    "object_object_target",
    "object_object_both",
    T_OCEL_ATTR_COLUMNS,
];

/// The table of one event or object type, and the index on its `ocel_id` column.
struct TypeTable {
    /// The table name without its `event_`/`object_` prefix, as `ocel_type_map` stores it.
    map_name: String,
    table: String,
    index: String,
}

/// The [`TypeTable`] of each of `event_types` and `object_types`, in order.
///
/// Both databases compare identifiers case-insensitively, and cleaning can make two names equal,
/// so a name that is already taken gets a counter: types `Order` and `order` must not share
/// a table. The importers find the table through `ocel_type_map`, so the name is free to pick.
fn type_tables(
    event_types: &[OCELType],
    object_types: &[OCELType],
) -> (Vec<TypeTable>, Vec<TypeTable>) {
    let mut names = ColumnNames::new(&FIXED_NAMES);
    let mut tables = |prefix: &str, types: &[OCELType]| -> Vec<(String, String)> {
        types
            .iter()
            .map(|t| {
                let table = names.claim(&format!("{prefix}_{}", clean_sql_name(&t.name)));
                (table[prefix.len() + 1..].to_string(), table)
            })
            .collect()
    };
    let event_tables = tables("event", event_types);
    let object_tables = tables("object", object_types);
    // Indexes are named once all tables are, so that no index takes the name of a table.
    let mut with_index = |tables: Vec<(String, String)>| -> Vec<TypeTable> {
        tables
            .into_iter()
            .map(|(map_name, table)| TypeTable {
                index: names.claim(&format!("{table}_{OCEL_ID_COLUMN}")),
                map_name,
                table,
            })
            .collect()
    };
    (with_index(event_tables), with_index(object_tables))
}

fn clean_sql_name(type_name: &str) -> String {
    type_name
        .chars()
        .map(|c| {
            if c != '\'' && c != '\\' && c != '"' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod test {
    use std::fs::remove_file;

    use super::export_ocel_to_sql_con;

    #[test]
    #[cfg(feature = "ocel-duckdb")]
    fn test_duckdb_ocel_export_order() {
        use crate::{test_utils, Importable, OCEL};

        let path = test_utils::get_test_data_path();
        let ocel = OCEL::import_from_path(path.join("ocel").join("order-management.json")).unwrap();
        let export_path = path.join("export").join("duckdb-export.db");
        let _ = remove_file(&export_path);
        let conn = ::duckdb::Connection::open(&export_path).unwrap();
        export_ocel_to_sql_con(&conn, &ocel).unwrap();
    }

    #[test]
    #[cfg(feature = "ocel-sqlite")]
    fn test_sqlite_ocel_export_order() {
        use crate::{test_utils, Importable, OCEL};

        let path: std::path::PathBuf = test_utils::get_test_data_path();
        let ocel = OCEL::import_from_path(path.join("ocel").join("order-management.json")).unwrap();
        let export_path = path.join("export").join("sqlite-export.sqlite");
        let _ = remove_file(&export_path);
        let conn = rusqlite::Connection::open(&export_path).unwrap();
        export_ocel_to_sql_con(&conn, &ocel).unwrap();
    }
}
