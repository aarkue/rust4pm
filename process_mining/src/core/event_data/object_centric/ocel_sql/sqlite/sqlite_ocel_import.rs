use crate::core::event_data::object_centric::io::OCELImportOptions;
use crate::core::event_data::object_centric::ocel_struct::{OCELAttributeValue, OCEL};
use crate::core::event_data::timestamp_utils::parse_timestamp;

use super::super::import::{import_ocel, resolve_columns, Col, SqlRow, SqlSource};
use super::super::*;
use crate::utils::control::Control;
use chrono::{DateTime, FixedOffset};
use rusqlite::types::{FromSqlError, ValueRef};
use rusqlite::{Connection, OpenFlags, Row};

impl SqlSource for Connection {
    type Row<'r> = Row<'r>;

    fn db_path(&self) -> Option<&str> {
        self.path()
    }

    fn has_table(&self, name: &str) -> Result<bool, OCELIOError> {
        Ok(self.query_row(
            "SELECT count(*) > 0 FROM sqlite_master WHERE type = 'table' AND name = ?",
            [name],
            |r| r.get(0),
        )?)
    }

    fn table_columns(&self, table: &str) -> Result<Vec<(String, String)>, OCELIOError> {
        let mut s = self.prepare(&format!("PRAGMA table_info({})", quoted_str(table)))?;
        let columns = s
            .query([])?
            .and_then(|x| Ok::<_, rusqlite::Error>((x.get("name")?, x.get("type")?)))
            .flatten()
            .collect();
        Ok(columns)
    }

    fn select_list(_: &[(String, String)]) -> String {
        "*".to_string()
    }

    fn as_text(column: &str) -> String {
        column.to_string()
    }

    fn for_each_row<'w>(
        &self,
        sql: &str,
        wanted: &[&'w str],
        mut each: impl FnMut(&Row<'_>, &[Col<'w>]) -> Result<(), OCELIOError>,
    ) -> Result<(), OCELIOError> {
        let mut s = self.prepare(sql)?;
        let cols = resolve_columns(&s.column_names().collect::<Vec<_>>(), wanted);
        let mut rows = s.query([])?;
        while let Some(r) = rows.next()? {
            each(r, &cols)?;
        }
        Ok(())
    }

    fn query_count(&self, sql: &str) -> Result<i64, OCELIOError> {
        Ok(self.query_row(sql, [], |r| r.get(0))?)
    }
}

impl SqlRow for Row<'_> {
    fn text(&self, col: Col<'_>) -> Result<String, OCELIOError> {
        Ok(self.get(col.index()?)?)
    }

    fn opt_text(&self, col: Col<'_>) -> Result<Option<String>, OCELIOError> {
        Ok(self.get(col.index()?)?)
    }

    fn time(&self, col: Col<'_>) -> Result<DateTime<FixedOffset>, OCELIOError> {
        let i = col.index()?;
        if let Ok(time) = self.get::<_, DateTime<FixedOffset>>(i) {
            return Ok(time);
        }
        let text: String = self.get(i)?;
        parse_timestamp(&text, None).map_err(|_| col.invalid_time(&text))
    }

    /// Errors only on a cell that has no attribute value at all (a blob, or text that is not
    /// UTF-8), which is never dropped silently.
    fn attribute(
        &self,
        col: Col<'_>,
        declared: OCELAttributeType,
        name: &str,
    ) -> Result<Option<OCELAttributeValue>, OCELIOError> {
        let cell = self.get_ref(col.index()?)?;
        sqlite_cell_to_ocel(cell, declared)
            .map_err(|e| OCELIOError::Invalid(format!("attribute '{name}': {e}")))
    }
}

/// Interpret one `SQLite` cell as a value of the `declared` attribute type.
///
/// `SQLite` columns are dynamically typed, so the cell's own storage class decides, not the
/// column's: a value that does not parse as the declared type is kept as what it is (usually a
/// string) rather than lost. In a non-text column, empty text and `null` are a `Null` value:
/// rust4pm up to 0.6.2 wrote `Null` as `''`, and some published logs write it as `'null'`.
fn sqlite_cell_to_ocel(
    cell: ValueRef<'_>,
    declared: OCELAttributeType,
) -> Result<Option<OCELAttributeValue>, FromSqlError> {
    let text = match cell {
        ValueRef::Null => return Ok(None),
        ValueRef::Integer(i) => {
            return Ok(Some(match declared {
                OCELAttributeType::Boolean if i == 0 || i == 1 => {
                    OCELAttributeValue::Boolean(i == 1)
                }
                OCELAttributeType::Float => OCELAttributeValue::Float(i as f64),
                _ => OCELAttributeValue::Integer(i),
            }))
        }
        ValueRef::Real(f) => return Ok(Some(OCELAttributeValue::Float(f))),
        ValueRef::Text(t) => std::str::from_utf8(t).map_err(|e| FromSqlError::Other(e.into()))?,
        ValueRef::Blob(_) => return Err(FromSqlError::InvalidType),
    };
    if matches!(
        declared,
        OCELAttributeType::String | OCELAttributeType::Null
    ) {
        return Ok(Some(OCELAttributeValue::String(text.to_string())));
    }
    if text.is_empty() || text.eq_ignore_ascii_case("null") {
        return Ok(Some(OCELAttributeValue::Null));
    }
    let parsed = match declared {
        OCELAttributeType::Integer => text.parse().ok().map(OCELAttributeValue::Integer),
        OCELAttributeType::Float => text.parse().ok().map(OCELAttributeValue::Float),
        OCELAttributeType::Boolean => {
            if text.eq_ignore_ascii_case("true") {
                Some(OCELAttributeValue::Boolean(true))
            } else if text.eq_ignore_ascii_case("false") {
                Some(OCELAttributeValue::Boolean(false))
            } else {
                None
            }
        }
        OCELAttributeType::Time => parse_timestamp(text, None)
            .ok()
            .map(OCELAttributeValue::Time),
        OCELAttributeType::String | OCELAttributeType::Null => unreachable!("handled above"),
    };
    Ok(Some(parsed.unwrap_or_else(|| {
        OCELAttributeValue::String(text.to_string())
    })))
}

/// Import an [`OCEL`] from the OCEL 2.0 `SQLite` database `con`.
pub fn import_ocel_sqlite(
    con: &Connection,
    options: &OCELImportOptions,
) -> Result<OCEL, OCELIOError> {
    import_ocel(con, options, &Control::new())
}

/// [`import_ocel_sqlite`] of the file at `path`, reporting to `ctl`.
///
/// The file is opened read-only, so a missing `path` is an [`OCELIOError::Io`] error and no
/// empty database is left behind.
pub(crate) fn import_sqlite_path(
    path: &std::path::Path,
    options: &OCELImportOptions,
    ctl: &Control<'_>,
) -> Result<OCEL, OCELIOError> {
    let con = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| match std::fs::metadata(path) {
        Err(io) => OCELIOError::Io(std::io::Error::new(
            io.kind(),
            format!("{}: {io}", path.display()),
        )),
        Ok(_) => e.into(),
    })?;
    import_ocel(&con, options, ctl)
}

/// [`import_ocel_sqlite`] of the database file `bytes`, reporting to `ctl`.
pub(crate) fn import_sqlite_slice(
    bytes: &[u8],
    options: &OCELImportOptions,
    ctl: &Control<'_>,
) -> Result<OCEL, OCELIOError> {
    let mut con = Connection::open_in_memory()?;
    // SQLite gets its own copy, as it may write to and resize the buffer. A file left in WAL mode
    // has no `-wal` file here, so its header is switched to rollback mode, as the
    // `sqlite3_deserialize` docs advise, or every read fails with `SQLITE_CANTOPEN`.
    let mut header = bytes[..bytes.len().min(20)].to_vec();
    for version in header.iter_mut().skip(18) {
        if *version == 2 {
            *version = 1;
        }
    }
    let rest = &bytes[header.len()..];
    // `sqlite3_malloc64(0)` gives no buffer, which would report empty input as out of memory
    if !bytes.is_empty() {
        con.deserialize_read_exact(
            rusqlite::MAIN_DB,
            std::io::Read::chain(header.as_slice(), rest),
            bytes.len(),
            false,
        )?;
    }
    import_ocel(&con, options, ctl)
}

#[cfg(test)]
mod sqlite_tests {
    use std::collections::HashSet;

    use chrono::DateTime;
    use rusqlite::Connection;

    use crate::{
        core::event_data::object_centric::{
            io::OCELIOError,
            ocel_sql::{import_ocel_sqlite, object_table_fixture},
            ocel_struct::{OCELAttributeValue, OCELObjectAttribute, OCELRelationship},
        },
        test_utils::get_test_data_path,
    };

    #[test]
    fn test_sqlite_ocel() -> Result<(), OCELIOError> {
        let path = get_test_data_path()
            .join("ocel")
            .join("order-management.sqlite");

        let con = Connection::open(path).unwrap();
        let ocel = import_ocel_sqlite(&con, &Default::default())?;

        assert_eq!(ocel.objects.len(), 10840);
        assert_eq!(ocel.events.len(), 21008);

        assert_eq!(ocel.event_types.len(), 11);
        assert_eq!(ocel.object_types.len(), 6);

        let po_1337 = ocel.events.iter().find(|e| e.id == "pay_o-991337").unwrap();
        assert_eq!(
            po_1337.time,
            DateTime::parse_from_rfc3339("2023-12-13T10:31:50+00:00").unwrap()
        );
        assert_eq!(
            po_1337
                .relationships
                .clone()
                .into_iter()
                .collect::<HashSet<_>>(),
            vec![
                ("Echo", "product"),
                ("iPad", "product"),
                ("iPad Pro", "product"),
                ("o-991337", "order"),
                ("i-885283", "item"),
                ("i-885284", "item"),
                ("i-885285", "item"),
            ]
            .into_iter()
            .map(|(o_id, q)| OCELRelationship {
                object_id: o_id.to_string(),
                qualifier: q.to_string()
            })
            .collect::<HashSet<_>>()
        );

        let o_1337 = ocel.objects.iter().find(|o| o.id == "o-991337").unwrap();
        assert_eq!(
            o_1337.attributes,
            vec![OCELObjectAttribute {
                name: "price".to_string(),
                value: OCELAttributeValue::Float(1909.04),
                time: DateTime::UNIX_EPOCH.into()
            }]
        );
        assert_eq!(
            o_1337
                .relationships
                .clone()
                .into_iter()
                .collect::<HashSet<_>>(),
            vec![
                ("i-885283", "comprises"),
                ("i-885284", "comprises"),
                ("i-885285", "comprises"),
            ]
            .into_iter()
            .map(|(o_id, q)| OCELRelationship {
                object_id: o_id.to_string(),
                qualifier: q.to_string()
            })
            .collect::<HashSet<_>>()
        );

        Ok(())
    }

    /// Truck, Handling Unit and Forklift have no attributes, so only the `object` table lists
    /// their objects.
    #[test]
    fn container_logistics_imports_every_object() {
        let path = get_test_data_path()
            .join("ocel")
            .join("ContainerLogistics.sqlite");
        if !path.exists() {
            eprintln!("Skipping: {} not found", path.display());
            return;
        }
        let ocel =
            import_ocel_sqlite(&Connection::open(path).unwrap(), &Default::default()).unwrap();
        assert_eq!(ocel.objects.len(), 13910);
        assert_eq!(ocel.events.len(), 35413);
        let mut per_type = std::collections::BTreeMap::new();
        for o in &ocel.objects {
            *per_type.entry(o.object_type.as_str()).or_insert(0) += 1;
        }
        assert_eq!(
            per_type.into_iter().collect::<Vec<_>>(),
            [
                ("Container", 1999),
                ("Customer Order", 600),
                ("Forklift", 3),
                ("Handling Unit", 10579),
                ("Transport Document", 596),
                ("Truck", 6),
                ("Vehicle", 127),
            ]
        );
        assert_eq!(ocel.object_types.len(), 7);
        let o2o: usize = ocel.objects.iter().map(|o| o.relationships.len()).sum();
        assert_eq!(o2o, 15953);
    }

    #[test]
    fn objects_listed_only_in_the_object_table_are_imported() {
        let con = Connection::open_in_memory().unwrap();
        con.execute_batch(object_table_fixture::SQL).unwrap();
        let ocel = import_ocel_sqlite(&con, &Default::default()).unwrap();
        object_table_fixture::check(&ocel);
    }
}

#[cfg(test)]
mod missing_changed_field_tests {
    use chrono::DateTime;
    use rusqlite::Connection;

    use crate::core::event_data::object_centric::{
        io::OCELImportOptions,
        ocel_sql::import_ocel_sqlite,
        ocel_struct::{OCELAttributeValue, OCELObjectAttribute},
    };

    /// A non-conforming `SQLite` file: object type `Truck` has no `ocel_changed_field` column,
    /// but does carry a misspelled `ocel_change_field` one whose value names a real attribute.
    fn build_fixture(path: &std::path::Path) {
        let con = Connection::open(path).unwrap();
        con.execute_batch(
            "
            CREATE TABLE event_map_type (ocel_type_map TEXT, ocel_type TEXT);
            CREATE TABLE object_map_type (ocel_type_map TEXT, ocel_type TEXT);
            CREATE TABLE event_object (ocel_event_id TEXT, ocel_object_id TEXT, ocel_qualifier TEXT);
            CREATE TABLE object_object (ocel_source_id TEXT, ocel_target_id TEXT, ocel_qualifier TEXT);
            CREATE TABLE event_Ev (ocel_id TEXT PRIMARY KEY, ocel_time TIMESTAMP);
            CREATE TABLE object_Truck (
                ocel_id TEXT PRIMARY KEY,
                ocel_time TIMESTAMP,
                driver TEXT,
                ocel_change_field TEXT
            );
            INSERT INTO object_map_type VALUES ('Truck', 'Truck');
            INSERT INTO event_map_type VALUES ('Ev', 'Ev');
            INSERT INTO event_Ev VALUES ('e1', '2020-01-01T00:00:00+00:00');
            INSERT INTO object_Truck VALUES ('t1', '2020-01-01T00:00:00+00:00', 'Alice', NULL);
            INSERT INTO object_Truck VALUES ('t2', '2020-01-02T00:00:00+00:00', 'Bob', 'driver');
            ",
        )
        .unwrap();
    }

    /// The column is only missing where there is no attribute change to record, so a file
    /// without it is read rather than refused.
    #[test]
    fn default_import_reads_a_file_without_the_changed_field_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no-changed-field.sqlite");
        build_fixture(&path);

        let con = Connection::open(&path).unwrap();
        let ocel = import_ocel_sqlite(&con, &Default::default())
            .expect("a file without the column must still be read");
        assert_eq!(ocel.objects.len(), 2, "both Truck objects must be present");
    }

    /// Holding the file to the specification is opt-in, and names what is wrong with it.
    #[test]
    fn strict_import_rejects_missing_changed_field_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no-changed-field.sqlite");
        build_fixture(&path);

        let con = Connection::open(&path).unwrap();
        let err = import_ocel_sqlite(
            &con,
            &OCELImportOptions {
                strict: true,
                ..Default::default()
            },
        )
        .unwrap_err();
        let msg = err.to_string();

        assert!(
            msg.contains("Truck"),
            "message should name the object type: {msg}"
        );
        assert!(
            msg.contains("object_Truck"),
            "message should name the table: {msg}"
        );
        assert!(
            msg.contains("ocel_changed_field"),
            "message should name the missing column: {msg}"
        );
        assert!(
            msg.contains("does not conform"),
            "message should say the file is non-conforming: {msg}"
        );
        assert!(
            msg.contains("ocel_change_field"),
            "message should mention the misspelled column it found: {msg}"
        );
    }

    #[test]
    fn a_file_without_the_changed_field_column_reads_as_initial_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no-changed-field.sqlite");
        build_fixture(&path);

        let con = Connection::open(&path).unwrap();
        let ocel = import_ocel_sqlite(
            &con,
            &OCELImportOptions {
                strict: false,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(ocel.objects.len(), 2, "both Truck objects must be present");

        let t1 = ocel.objects.iter().find(|o| o.id == "t1").unwrap();
        assert_eq!(t1.object_type, "Truck");
        assert_eq!(
            t1.attributes,
            vec![OCELObjectAttribute {
                name: "driver".to_string(),
                value: OCELAttributeValue::String("Alice".to_string()),
                time: DateTime::UNIX_EPOCH.into(),
            }],
            "t1's ocel_change_field cell is NULL, so it contributes no attribute; \
             it must not be invented as a change record"
        );

        let t2 = ocel.objects.iter().find(|o| o.id == "t2").unwrap();
        assert_eq!(t2.object_type, "Truck");
        let mut t2_attrs = t2.attributes.clone();
        t2_attrs.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(
            t2_attrs,
            vec![
                OCELObjectAttribute {
                    name: "driver".to_string(),
                    value: OCELAttributeValue::String("Bob".to_string()),
                    time: DateTime::UNIX_EPOCH.into(),
                },
                OCELObjectAttribute {
                    name: "ocel_change_field".to_string(),
                    value: OCELAttributeValue::String("driver".to_string()),
                    time: DateTime::UNIX_EPOCH.into(),
                },
            ],
            "no attribute-change row must be invented from the misspelled column"
        );

        let truck_type = ocel
            .object_types
            .iter()
            .find(|t| t.name == "Truck")
            .unwrap();
        let mut attr_names: Vec<_> = truck_type
            .attributes
            .iter()
            .map(|a| a.name.clone())
            .collect();
        attr_names.sort();
        assert_eq!(attr_names, vec!["driver", "ocel_change_field"]);
    }
}

#[cfg(test)]
mod attribute_value_tests {
    use chrono::{DateTime, FixedOffset};
    use rusqlite::Connection;

    use crate::core::event_data::object_centric::{
        io::OCELIOError,
        ocel_sql::{export_ocel_sqlite_to_path, export_ocel_sqlite_to_vec, import_ocel_sqlite},
        ocel_struct::{
            OCELAttributeValue, OCELEvent, OCELEventAttribute, OCELObject, OCELObjectAttribute,
            OCELType, OCELTypeAttribute, OCEL,
        },
    };

    fn time(s: &str) -> DateTime<FixedOffset> {
        DateTime::parse_from_rfc3339(s).unwrap()
    }

    fn type_attr(name: &str, value_type: &str) -> OCELTypeAttribute {
        OCELTypeAttribute {
            name: name.to_string(),
            value_type: value_type.to_string(),
        }
    }

    fn ob_attr(name: &str, value: OCELAttributeValue, t: &str) -> OCELObjectAttribute {
        OCELObjectAttribute {
            name: name.to_string(),
            value,
            time: time(t),
        }
    }

    fn event(id: &str, note: OCELAttributeValue) -> OCELEvent {
        OCELEvent {
            id: id.to_string(),
            event_type: "ev".to_string(),
            time: time("2024-01-01T00:00:00+00:00"),
            attributes: vec![OCELEventAttribute {
                name: "note".to_string(),
                value: note,
            }],
            relationships: Vec::new(),
        }
    }

    fn sorted_events(ocel: &OCEL) -> Vec<OCELEvent> {
        let mut events = ocel.events.clone();
        events.sort_by(|a, b| a.id.cmp(&b.id));
        events
    }

    /// Every value kind that `SQLite` can hold survives a round trip with its type, including
    /// booleans and a `Null` entry in an object's attribute history.
    #[test]
    fn values_survive_a_sqlite_round_trip() {
        const EPOCH: &str = "1970-01-01T00:00:00+00:00";
        let ocel = OCEL {
            event_types: vec![OCELType {
                name: "ev".to_string(),
                attributes: vec![type_attr("note", "string")],
            }],
            object_types: vec![OCELType {
                name: "item".to_string(),
                attributes: vec![
                    type_attr("flag", "boolean"),
                    type_attr("amount", "float"),
                    type_attr("count", "integer"),
                    type_attr("due", "time"),
                    type_attr("label", "string"),
                ],
            }],
            events: vec![
                event("e1", OCELAttributeValue::String(String::new())),
                event("e2", OCELAttributeValue::String("text".to_string())),
            ],
            objects: vec![OCELObject {
                id: "i1".to_string(),
                object_type: "item".to_string(),
                attributes: vec![
                    ob_attr("flag", OCELAttributeValue::Boolean(true), EPOCH),
                    ob_attr("amount", OCELAttributeValue::Float(10.5), EPOCH),
                    ob_attr("count", OCELAttributeValue::Integer(-3), EPOCH),
                    ob_attr(
                        "due",
                        OCELAttributeValue::Time(time("2024-02-01T12:00:00+00:00")),
                        EPOCH,
                    ),
                    ob_attr("label", OCELAttributeValue::String(String::new()), EPOCH),
                    ob_attr(
                        "flag",
                        OCELAttributeValue::Boolean(false),
                        "2024-01-02T00:00:00+00:00",
                    ),
                    ob_attr(
                        "amount",
                        OCELAttributeValue::Null,
                        "2024-01-03T00:00:00+00:00",
                    ),
                    ob_attr(
                        "label",
                        OCELAttributeValue::Null,
                        "2024-01-04T00:00:00+00:00",
                    ),
                ],
                relationships: Vec::new(),
            }],
        };

        let back = super::import_sqlite_slice(
            &export_ocel_sqlite_to_vec(&ocel).unwrap(),
            &Default::default(),
            &crate::Control::new(),
        )
        .unwrap();

        assert_eq!(sorted_events(&back), ocel.events);
        assert_eq!(back.objects, ocel.objects);
    }

    /// SQL has a single `NULL` for both "no value" and "the value `Null`". Outside an attribute
    /// change it is read as no value, as every published log uses it, so a `Null` event attribute
    /// comes back absent rather than as a `String("")` indistinguishable from a real empty one.
    #[test]
    fn a_null_event_attribute_is_written_as_sql_null() {
        let ocel = OCEL {
            event_types: vec![OCELType {
                name: "ev".to_string(),
                attributes: vec![type_attr("note", "string")],
            }],
            object_types: Vec::new(),
            events: vec![
                event("e1", OCELAttributeValue::Null),
                event("e2", OCELAttributeValue::String(String::new())),
            ],
            objects: Vec::new(),
        };
        let back = super::import_sqlite_slice(
            &export_ocel_sqlite_to_vec(&ocel).unwrap(),
            &Default::default(),
            &crate::Control::new(),
        )
        .unwrap();
        let events = sorted_events(&back);
        assert!(events[0].attributes.is_empty(), "{:?}", events[0]);
        assert_eq!(
            events[1].attributes[0].value,
            OCELAttributeValue::String(String::new())
        );
    }

    #[test]
    fn a_wal_mode_file_imports_from_a_slice() {
        let ocel = OCEL {
            event_types: vec![OCELType {
                name: "ev".to_string(),
                attributes: vec![type_attr("note", "string")],
            }],
            object_types: Vec::new(),
            events: vec![event("e1", OCELAttributeValue::String("x".to_string()))],
            objects: Vec::new(),
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.sqlite");
        crate::core::event_data::object_centric::ocel_sql::export_ocel_sqlite_to_path(&ocel, &path)
            .unwrap();
        Connection::open(&path)
            .unwrap()
            .pragma_update(None, "journal_mode", "WAL")
            .unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes[18], 2, "the fixture must be in WAL mode");

        let back = super::import_sqlite_slice(&bytes, &Default::default(), &crate::Control::new())
            .unwrap();
        assert_eq!(back.events, ocel.events);
        assert_eq!(bytes[18], 2);

        let back =
            super::import_sqlite_path(&path, &Default::default(), &crate::Control::new()).unwrap();
        assert_eq!(back.events, ocel.events);
    }

    #[test]
    fn path_import_opens_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.sqlite");
        let err = <OCEL as crate::Importable>::import_from_path(&missing).unwrap_err();
        assert!(
            matches!(&err, OCELIOError::Io(e) if e.kind() == std::io::ErrorKind::NotFound),
            "{err}"
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

        for name in [
            "order-management.sqlite",
            "ContainerLogistics.sqlite",
            "ocel2-p2p.sqlite",
        ] {
            let path = crate::test_utils::get_test_data_path()
                .join("ocel")
                .join(name);
            let ocel = <OCEL as crate::Importable>::import_from_path(&path).unwrap();
            assert!(!ocel.events.is_empty(), "{name}");
        }
    }

    /// Every type an item has gets a `*_map_type` row and a table, declared or not.
    #[test]
    fn undeclared_types_are_exported() {
        let mut ghost = event("e2", OCELAttributeValue::String("x".to_string()));
        ghost.event_type = "ghost".to_string();
        let ocel = OCEL {
            event_types: vec![OCELType {
                name: "ev".to_string(),
                attributes: vec![type_attr("note", "string")],
            }],
            object_types: Vec::new(),
            events: vec![
                event("e1", OCELAttributeValue::String("x".to_string())),
                ghost,
            ],
            objects: vec![OCELObject {
                id: "o1".to_string(),
                object_type: "thing".to_string(),
                attributes: Vec::new(),
                relationships: Vec::new(),
            }],
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("undeclared.sqlite");
        export_ocel_sqlite_to_path(&ocel, &path).unwrap();
        let con = Connection::open(&path).unwrap();
        let map_types = |table: &str| -> Vec<String> {
            let mut s = con
                .prepare(&format!("SELECT ocel_type FROM {table} ORDER BY ocel_type"))
                .unwrap();
            s.query_map([], |r| r.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
        };
        assert_eq!(map_types("event_map_type"), ["ev", "ghost"]);
        assert_eq!(map_types("object_map_type"), ["thing"]);

        let back = import_ocel_sqlite(&con, &Default::default()).unwrap();
        let mut events: Vec<_> = back
            .events
            .iter()
            .map(|e| (e.id.as_str(), e.event_type.as_str()))
            .collect();
        events.sort_unstable();
        assert_eq!(events, [("e1", "ev"), ("e2", "ghost")]);
        assert_eq!(back.objects.len(), 1);
        assert_eq!(back.objects[0].object_type, "thing");
    }

    fn file_with_rows(rows: &str) -> Connection {
        let con = Connection::open_in_memory().unwrap();
        con.execute_batch(&format!(
            "
            CREATE TABLE event_map_type (ocel_type_map TEXT, ocel_type TEXT);
            CREATE TABLE object_map_type (ocel_type_map TEXT, ocel_type TEXT);
            CREATE TABLE event_object (ocel_event_id TEXT, ocel_object_id TEXT, ocel_qualifier TEXT);
            CREATE TABLE object_object (ocel_source_id TEXT, ocel_target_id TEXT, ocel_qualifier TEXT);
            CREATE TABLE object_item (
                ocel_id TEXT,
                ocel_time TIMESTAMP,
                ocel_changed_field TEXT,
                flag BOOLEAN,
                amount DOUBLE PRECISION,
                due TIMESTAMP
            );
            INSERT INTO object_map_type VALUES ('item', 'item');
            {rows}
            "
        ))
        .unwrap();
        con
    }

    fn attrs_of(ocel: &OCEL, id: &str) -> Vec<(String, OCELAttributeValue, String)> {
        let mut attrs: Vec<_> = ocel
            .objects
            .iter()
            .find(|o| o.id == id)
            .unwrap()
            .attributes
            .iter()
            .map(|a| (a.name.clone(), a.value.clone(), a.time.to_rfc3339()))
            .collect();
        attrs.sort_by(|a, b| (&a.2, &a.0).cmp(&(&b.2, &b.0)));
        attrs
    }

    /// Files written by rust4pm up to 0.6.2 store `Null` as `''` and booleans as
    /// `'true'`/`'false'` text, and some published logs store `Null` as `'null'`.
    #[test]
    fn text_written_into_typed_columns_is_still_read() {
        let con = file_with_rows(
            "
            INSERT INTO object_item VALUES ('i1', '1970-01-01T00:00:00+00:00', NULL, 'true', '', '');
            INSERT INTO object_item VALUES ('i1', '2024-01-02T00:00:00+00:00', 'flag', 'false', NULL, NULL);
            INSERT INTO object_item VALUES ('i1', '2024-01-03T00:00:00+00:00', 'amount', NULL, '', NULL);
            INSERT INTO object_item VALUES ('i1', '2024-01-04T00:00:00+00:00', 'amount', NULL, 'null', NULL);
            INSERT INTO object_item VALUES ('i1', '2024-01-05T00:00:00+00:00', 'amount', NULL, 'n/a', NULL);
            ",
        );
        let ocel = import_ocel_sqlite(&con, &Default::default()).unwrap();
        let epoch = "1970-01-01T00:00:00+00:00".to_string();
        assert_eq!(
            attrs_of(&ocel, "i1"),
            vec![
                ("amount".into(), OCELAttributeValue::Null, epoch.clone()),
                ("due".into(), OCELAttributeValue::Null, epoch.clone()),
                ("flag".into(), OCELAttributeValue::Boolean(true), epoch),
                (
                    "flag".into(),
                    OCELAttributeValue::Boolean(false),
                    "2024-01-02T00:00:00+00:00".into()
                ),
                (
                    "amount".into(),
                    OCELAttributeValue::Null,
                    "2024-01-03T00:00:00+00:00".into()
                ),
                (
                    "amount".into(),
                    OCELAttributeValue::Null,
                    "2024-01-04T00:00:00+00:00".into()
                ),
                // Not a float, but kept as what it is instead of being dropped.
                (
                    "amount".into(),
                    OCELAttributeValue::String("n/a".into()),
                    "2024-01-05T00:00:00+00:00".into()
                ),
            ]
        );
    }

    /// A cell that holds no attribute value at all fails the import instead of vanishing.
    #[test]
    fn an_unreadable_cell_is_reported() {
        let con = file_with_rows(
            "INSERT INTO object_item VALUES ('i1', '1970-01-01T00:00:00+00:00', NULL, NULL, x'00ff', NULL);",
        );
        let err = import_ocel_sqlite(&con, &Default::default()).unwrap_err();
        assert!(err.to_string().contains("amount"), "{err}");
    }
}
