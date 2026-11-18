//! On-demand generation of per-type wide views.
use std::collections::HashMap;

use duckdb::Connection;

use crate::core::event_data::object_centric::io::OCELIOError;
use crate::core::event_data::object_centric::ocel_sql::quoted_ident;

use super::tables::{
    event_attr_column, event_column_renames, object_attr_column, ColumnNames,
    OBJECT_VIEW_BASE_COLS, T_E2O, T_EVENTS, T_EVENT_ATTR_COLUMNS, T_EVENT_ATTR_META, T_O2O,
    T_OBJECTS, T_OBJECT_ATTR_CHANGES, T_OBJECT_ATTR_META,
};
use super::value::NULL_VALUE_TYPE;

/// `DuckDB` cast target for an OCEL `value_type` string.
fn cast_type(value_type: &str) -> &'static str {
    match value_type {
        "integer" => "BIGINT",
        "float" => "DOUBLE",
        "boolean" => "BOOLEAN",
        "time" => "TIMESTAMPTZ",
        _ => "VARCHAR",
    }
}

fn distinct_types(con: &Connection, entity_table: &str) -> Result<Vec<String>, duckdb::Error> {
    let mut stmt = con.prepare(&format!(
        "SELECT DISTINCT ocel_type FROM {entity_table} ORDER BY ocel_type"
    ))?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    rows.collect()
}

/// (`attr_name`, `cast_type`) for one object type. If a name has >1 `value_type`, coerce to VARCHAR.
///
/// `Null` values (`NULL_VALUE_TYPE`) have no type of their own and are not counted.
fn object_attrs_for_type(
    con: &Connection,
    ocel_type: &str,
) -> Result<Vec<(String, &'static str)>, duckdb::Error> {
    let typed = format!("a.value_type <> '{NULL_VALUE_TYPE}'");
    let sql = format!(
        "SELECT a.name, count(DISTINCT a.value_type) FILTER (WHERE {typed}) AS n, \
         any_value(a.value_type) FILTER (WHERE {typed}) AS vt \
         FROM {T_OBJECT_ATTR_CHANGES} a JOIN {T_OBJECTS} e ON a.id = e.id \
         WHERE e.ocel_type = ? GROUP BY a.name ORDER BY a.name"
    );
    let mut stmt = con.prepare(&sql)?;
    let rows = stmt.query_map([ocel_type], |r| {
        let name: String = r.get(0)?;
        let n: i64 = r.get(1)?;
        let vt: Option<String> = r.get(2)?;
        Ok((name, n, vt))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (name, n, vt) = row?;
        let cast = match vt {
            Some(vt) if n == 1 => cast_type(&vt),
            _ => "VARCHAR",
        };
        out.push((name, cast));
    }
    Ok(out)
}

/// Declared attribute column names for one event type (from `event_attr_meta`).
fn event_attrs_for_type(con: &Connection, ocel_type: &str) -> Result<Vec<String>, duckdb::Error> {
    let mut stmt = con.prepare(&format!(
        "SELECT DISTINCT attr_name FROM {T_EVENT_ATTR_META} WHERE event_type = ? ORDER BY attr_name"
    ))?;
    let rows = stmt.query_map([ocel_type], |r| r.get::<_, String>(0))?;
    rows.collect()
}

/// Build `event_<type>` and `object_<type>` wide views.
///
/// An attribute aliased to its bare name would collide with the view's base columns
/// (`id`/`time` for events, `id` for objects), so such a name takes an `_attr` suffix instead.
/// Aliases are compared case-insensitively, like all `DuckDB` identifiers, so of two attributes
/// whose names differ only in case, the second takes a `_<n>` suffix too.
///
/// View names take a `_<n>` suffix the same way, if they would reuse a schema table's name (type
/// `attr_meta`) or another view's (types `Pay` and `pay`).
pub fn generate_type_views(con: &Connection) -> Result<(), OCELIOError> {
    const BASE_COLS: &[&str] = &["id", "time"];

    let mut view_names = ColumnNames::new(&[
        T_EVENTS,
        T_OBJECTS,
        T_EVENT_ATTR_META,
        T_OBJECT_ATTR_META,
        T_OBJECT_ATTR_CHANGES,
        T_E2O,
        T_O2O,
        T_EVENT_ATTR_COLUMNS,
    ]);

    let renamed: HashMap<String, String> = event_column_renames(con)?
        .into_iter()
        .map(|(column, attr)| (attr, column))
        .collect();
    // Events: project the type's typed columns straight off the wide `events` table.
    for ty in distinct_types(con, T_EVENTS)? {
        let attrs = event_attrs_for_type(con, &ty)?;
        let mut aliases = ColumnNames::new(BASE_COLS);
        let cols: String = attrs
            .iter()
            .map(|name| {
                let alias = if BASE_COLS.iter().any(|b| b.eq_ignore_ascii_case(name)) {
                    aliases.claim(&format!("{name}_attr"))
                } else {
                    aliases.claim(name)
                };
                let column = renamed
                    .get(name)
                    .map_or_else(|| event_attr_column(name), |c| c.as_str().into());
                format!(", {} AS {}", quoted_ident(&column), quoted_ident(&alias))
            })
            .collect();
        let view = quoted_ident(&view_names.claim(&format!("event_{ty}")));
        let ty_lit = ty.replace('\'', "''");
        let sql = format!(
            "CREATE OR REPLACE VIEW {view} AS \
             SELECT id, \"time\"{cols} FROM {T_EVENTS} WHERE ocel_type = '{ty_lit}'"
        );
        con.execute_batch(&sql)?;
    }

    // Objects: pivot the EAV `object_attribute_changes` table into wide columns.
    for ty in distinct_types(con, T_OBJECTS)? {
        let attrs = object_attrs_for_type(con, &ty)?;
        let mut aliases = ColumnNames::new(&OBJECT_VIEW_BASE_COLS);
        let cols: String = attrs
            .iter()
            .map(|(name, cast)| {
                let n = name.replace('\'', "''");
                let alias = quoted_ident(&aliases.claim(&object_attr_column(name)));
                format!(
                    ", CAST(any_value(a.value) FILTER (WHERE a.name = '{n}' \
                     AND a.value_type <> '{NULL_VALUE_TYPE}') AS {cast}) AS {alias}"
                )
            })
            .collect();
        let view = quoted_ident(&view_names.claim(&format!("object_{ty}")));
        let ty_lit = ty.replace('\'', "''");
        let sql = format!(
            "CREATE OR REPLACE VIEW {view} AS \
             SELECT e.id{cols} \
             FROM {T_OBJECTS} e LEFT JOIN {T_OBJECT_ATTR_CHANGES} a ON a.id = e.id \
             WHERE e.ocel_type = '{ty_lit}' \
             GROUP BY e.id"
        );
        con.execute_batch(&sql)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::stream::DuckDbWriteOptions;
    use super::*;
    use crate::core::event_data::object_centric::ocel_sql::duckdb::schema::stream::stream_ocel_file_to_duckdb;
    use crate::test_utils::get_test_data_path;

    #[test]
    fn generates_queryable_event_views() {
        let src = get_test_data_path()
            .join("ocel")
            .join("order-management.json");
        let out = get_test_data_path()
            .join("export")
            .join("stream-views.duckdb");
        let _ = std::fs::remove_file(&out);
        stream_ocel_file_to_duckdb(
            &src,
            &out,
            &DuckDbWriteOptions {
                overwrite: true,
                ..Default::default()
            },
            &Default::default(),
        )
        .unwrap();
        let con = Connection::open(&out).unwrap();
        generate_type_views(&con).unwrap();

        let n_views: i64 = con
            .query_row(
                "SELECT count(*) FROM information_schema.tables WHERE table_type = 'VIEW' AND table_name LIKE 'event_%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(n_views >= 1, "expected at least one event_<type> view");

        // The view is actually queryable, not just present in the catalog.
        let view_name: String = con
            .query_row(
                "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW' AND table_name LIKE 'event_%' ORDER BY table_name LIMIT 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        con.query_row(&format!("SELECT count(*) FROM \"{view_name}\""), [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap();

        // order-management objects have attributes, so this exercises a non-trivial pivot.
        let object_view_name: String = con
            .query_row(
                "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW' AND table_name LIKE 'object_%' ORDER BY table_name LIMIT 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let object_row_count: i64 = con
            .query_row(
                &format!("SELECT count(*) FROM \"{object_view_name}\""),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            object_row_count > 0,
            "expected {object_view_name} to return rows"
        );
    }

    /// Returns (`column_name`, `data_type`) pairs for a view/table, ordered by position.
    fn columns_of(con: &Connection, table_name: &str) -> Vec<(String, String)> {
        let mut stmt = con
            .prepare(
                "SELECT column_name, data_type FROM information_schema.columns \
                 WHERE table_name = ? ORDER BY ordinal_position",
            )
            .unwrap();
        stmt.query_map([table_name], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    #[test]
    fn event_view_exposes_typed_attribute_column() {
        let con = Connection::open_in_memory().unwrap();
        super::super::tables::create_schema(&con).unwrap();
        con.execute_batch(&super::super::tables::build_events_table(&[(
            "amount".to_string(),
            crate::core::event_data::object_centric::ocel_struct::OCELAttributeType::Integer,
        )]))
        .unwrap();
        con.execute_batch(
            "INSERT INTO events VALUES ('e1', 't', TIMESTAMP '2024-01-01 00:00:00', 42);
             INSERT INTO event_attr_meta VALUES ('t', 'amount', 'integer');",
        )
        .unwrap();

        generate_type_views(&con).unwrap();

        let names: Vec<String> = columns_of(&con, "event_t")
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert!(names.contains(&"id".to_string()), "cols: {names:?}");
        assert!(names.contains(&"time".to_string()), "cols: {names:?}");
        assert!(names.contains(&"amount".to_string()), "cols: {names:?}");

        let amount: i64 = con
            .query_row("SELECT amount FROM event_t WHERE id = 'e1'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(amount, 42);
    }

    #[test]
    fn object_view_coerces_heterogeneous_types_to_varchar() {
        let con = Connection::open_in_memory().unwrap();
        super::super::tables::create_schema(&con).unwrap();
        con.execute_batch(&super::super::tables::build_events_table(&[]))
            .unwrap();

        // Two rows give the same attribute name two different value_types.
        con.execute_batch(
            "INSERT INTO objects VALUES ('o1', 'o');
             INSERT INTO objects VALUES ('o2', 'o');
             INSERT INTO object_attribute_changes VALUES \
                ('o1', 'attr', TIMESTAMP '2024-01-01 00:00:00', '1', 'integer');
             INSERT INTO object_attribute_changes VALUES \
                ('o2', 'attr', TIMESTAMP '2024-01-01 00:00:00', 'foo', 'string');",
        )
        .unwrap();

        generate_type_views(&con).unwrap();

        let object_cols = columns_of(&con, "object_o");
        let attr_type = object_cols
            .iter()
            .find(|(n, _)| n == "attr")
            .map(|(_, t)| t.as_str())
            .unwrap_or_else(|| panic!("expected 'attr' column in object_o: {object_cols:?}"));
        assert_eq!(
            attr_type, "VARCHAR",
            "heterogeneous value_types must coerce to VARCHAR"
        );

        let object_row_count: i64 = con
            .query_row("SELECT count(*) FROM object_o", [], |r| r.get(0))
            .unwrap();
        assert_eq!(object_row_count, 2);
    }

    #[test]
    fn object_view_handles_id_and_id_attr_attribute_collision() {
        let con = Connection::open_in_memory().unwrap();
        super::super::tables::create_schema(&con).unwrap();
        con.execute_batch(&super::super::tables::build_events_table(&[]))
            .unwrap();

        // An object with both an `id` attribute and an `id_attr` attribute must not produce
        // two columns aliased to the same name.
        con.execute_batch(
            "INSERT INTO objects VALUES ('o1', 'o');
             INSERT INTO object_attribute_changes VALUES \
                ('o1', 'id', TIMESTAMP '2024-01-01 00:00:00', 'v1', 'string');
             INSERT INTO object_attribute_changes VALUES \
                ('o1', 'id_attr', TIMESTAMP '2024-01-01 00:00:00', 'v2', 'string');",
        )
        .unwrap();

        generate_type_views(&con).unwrap();

        let names: Vec<String> = columns_of(&con, "object_o")
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert!(names.contains(&"id".to_string()), "cols: {names:?}");
        assert!(names.contains(&"id_attr".to_string()), "cols: {names:?}");
        assert!(
            names.contains(&"id_attr_attr".to_string()),
            "cols: {names:?}"
        );

        let (id_attr, id_attr_attr): (String, String) = con
            .query_row(
                "SELECT id_attr, id_attr_attr FROM object_o WHERE id = 'o1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(id_attr, "v1", "the `id` attribute value");
        assert_eq!(id_attr_attr, "v2", "the `id_attr` attribute value");
    }

    #[test]
    fn type_views_never_replace_each_other_or_a_schema_table() {
        let con = Connection::open_in_memory().unwrap();
        super::super::tables::create_schema(&con).unwrap();
        con.execute_batch(&super::super::tables::build_events_table(&[]))
            .unwrap();
        con.execute_batch(
            "INSERT INTO events VALUES ('e1', 'Pay', TIMESTAMP '2024-01-01 00:00:00');
             INSERT INTO events VALUES ('e2', 'pay', TIMESTAMP '2024-01-01 00:00:00');
             INSERT INTO events VALUES ('e3', 'attr_meta', TIMESTAMP '2024-01-01 00:00:00');
             INSERT INTO objects VALUES ('o1', 'attribute_changes');",
        )
        .unwrap();

        generate_type_views(&con).unwrap();

        for (view, id) in [
            ("event_Pay", "e1"),
            ("event_pay_2", "e2"),
            ("event_attr_meta_2", "e3"),
            ("object_attribute_changes_2", "o1"),
        ] {
            let ids: Vec<String> = con
                .prepare(&format!("SELECT id FROM \"{view}\""))
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            assert_eq!(ids, [id], "{view}");
        }
    }
}
