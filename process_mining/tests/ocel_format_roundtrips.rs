//! One log that uses every OCEL 2.0 feature, written and read back through every format.
//!
//! The log has every attribute type on events and objects, object attribute histories (including
//! a change to `Null` and to the empty string), `Null` and empty-string values, attributes left
//! out, E2O and O2O relationships with qualifiers, types without attributes, and times with
//! fractional seconds.
//!
//! Each format must give the log back exactly, except for the differences listed for it. Those
//! lists are exhaustive in both directions: a difference that is not listed fails the test, and so
//! does a listed one that no longer occurs, so fixing a gap means deleting its entry here.

use process_mining::core::event_data::object_centric::{
    OCELAttributeValue, OCELEvent, OCELEventAttribute, OCELObject, OCELObjectAttribute,
    OCELRelationship, OCELType, OCELTypeAttribute, OCEL,
};

use process_mining::core::io::{Exportable, Importable};
use OCELAttributeValue as V;

const EPOCH: &str = "1970-01-01T00:00:00+00:00";

fn time(s: &str) -> chrono::DateTime<chrono::FixedOffset> {
    chrono::DateTime::parse_from_rfc3339(s).unwrap()
}

fn all_attribute_types() -> Vec<OCELTypeAttribute> {
    [
        ("s", "string"),
        ("i", "integer"),
        ("f", "float"),
        ("b", "boolean"),
        ("t", "time"),
    ]
    .into_iter()
    .map(|(name, value_type)| OCELTypeAttribute {
        name: name.to_string(),
        value_type: value_type.to_string(),
    })
    .collect()
}

fn ev_attr(name: &str, value: OCELAttributeValue) -> OCELEventAttribute {
    OCELEventAttribute {
        name: name.to_string(),
        value,
    }
}

fn ob_attr(name: &str, value: OCELAttributeValue, at: &str) -> OCELObjectAttribute {
    OCELObjectAttribute {
        name: name.to_string(),
        value,
        time: time(at),
    }
}

fn rel(object_id: &str, qualifier: &str) -> OCELRelationship {
    OCELRelationship {
        object_id: object_id.to_string(),
        qualifier: qualifier.to_string(),
    }
}

fn full_log() -> OCEL {
    OCEL {
        event_types: vec![
            OCELType {
                name: "place order".to_string(),
                attributes: all_attribute_types(),
            },
            OCELType {
                name: "ping".to_string(),
                attributes: Vec::new(),
            },
        ],
        object_types: vec![
            OCELType {
                name: "item".to_string(),
                attributes: all_attribute_types(),
            },
            OCELType {
                name: "customer".to_string(),
                attributes: Vec::new(),
            },
        ],
        events: vec![
            OCELEvent {
                id: "e1".to_string(),
                event_type: "place order".to_string(),
                time: time("2024-01-01T10:00:00+00:00"),
                attributes: vec![
                    ev_attr("s", V::String("a, \"b\"".to_string())),
                    ev_attr("i", V::Integer(-42)),
                    ev_attr("f", V::Float(1.5)),
                    ev_attr("b", V::Boolean(true)),
                    ev_attr("t", V::Time(time("2024-02-03T04:05:06.789+00:00"))),
                ],
                relationships: vec![rel("o1", "creates"), rel("o2", "contains"), rel("c1", "by")],
            },
            OCELEvent {
                id: "e2".to_string(),
                event_type: "place order".to_string(),
                time: time("2024-01-02T10:00:00+00:00"),
                // `t` is left out.
                attributes: vec![
                    ev_attr("s", V::String(String::new())),
                    ev_attr("i", V::Integer(0)),
                    ev_attr("f", V::Null),
                    ev_attr("b", V::Boolean(false)),
                ],
                relationships: Vec::new(),
            },
            OCELEvent {
                id: "e3".to_string(),
                event_type: "ping".to_string(),
                time: time("2024-01-03T10:00:00.250+00:00"),
                attributes: Vec::new(),
                relationships: vec![rel("c1", "about")],
            },
        ],
        objects: vec![
            OCELObject {
                id: "o1".to_string(),
                object_type: "item".to_string(),
                attributes: vec![
                    ob_attr("s", V::String("x".to_string()), EPOCH),
                    ob_attr("i", V::Integer(7), EPOCH),
                    ob_attr("f", V::Float(2.25), EPOCH),
                    ob_attr("b", V::Boolean(true), EPOCH),
                    ob_attr("t", V::Time(time("2024-05-06T07:08:09+00:00")), EPOCH),
                    ob_attr("f", V::Float(3.5), "2024-01-05T00:00:00+00:00"),
                    ob_attr("f", V::Null, "2024-01-06T00:00:00+00:00"),
                    ob_attr("b", V::Boolean(false), "2024-01-07T00:00:00+00:00"),
                    ob_attr("s", V::String(String::new()), "2024-01-08T00:00:00+00:00"),
                    ob_attr("i", V::Integer(8), "2024-01-09T00:00:00.5+00:00"),
                    ob_attr("s", V::Null, "2024-01-10T00:00:00+00:00"),
                ],
                relationships: vec![rel("c1", "owned by")],
            },
            OCELObject {
                id: "o2".to_string(),
                object_type: "item".to_string(),
                attributes: vec![ob_attr("s", V::Null, EPOCH)],
                relationships: vec![rel("o1", "part of")],
            },
            OCELObject {
                id: "c1".to_string(),
                object_type: "customer".to_string(),
                attributes: Vec::new(),
                relationships: Vec::new(),
            },
        ],
    }
}

/// Everything the log says, one fact per line, so two logs are compared exactly and
/// independently of order.
fn facts(ocel: &OCEL) -> Vec<String> {
    let mut out = Vec::new();
    let type_line = |kind: &str, t: &OCELType| {
        let mut attrs: Vec<_> = t
            .attributes
            .iter()
            .map(|a| format!("{}:{}", a.name, a.value_type))
            .collect();
        attrs.sort();
        format!("{kind} {:?} {attrs:?}", t.name)
    };
    out.extend(ocel.event_types.iter().map(|t| type_line("etype", t)));
    out.extend(ocel.object_types.iter().map(|t| type_line("otype", t)));
    for e in &ocel.events {
        out.push(format!(
            "event {} {:?} {}",
            e.id,
            e.event_type,
            e.time.to_rfc3339()
        ));
        for a in &e.attributes {
            out.push(format!("event {} {} = {:?}", e.id, a.name, a.value));
        }
        for r in &e.relationships {
            out.push(format!("e2o {} {} {:?}", e.id, r.object_id, r.qualifier));
        }
    }
    for o in &ocel.objects {
        out.push(format!("object {} {:?}", o.id, o.object_type));
        for a in &o.attributes {
            out.push(format!(
                "object {} {} @{} = {:?}",
                o.id,
                a.name,
                a.time.to_rfc3339(),
                a.value
            ));
        }
        for r in &o.relationships {
            out.push(format!("o2o {} {} {:?}", o.id, r.object_id, r.qualifier));
        }
    }
    out.sort();
    out
}

/// A fact the format does not give back as written: `lost` is missing after the round trip and,
/// if `Some`, `got` is there instead.
#[derive(Clone, Copy)]
struct Gap {
    lost: &'static str,
    got: Option<&'static str>,
}

const fn lost(lost: &'static str) -> Gap {
    Gap { lost, got: None }
}

const fn changed(lost: &'static str, got: &'static str) -> Gap {
    Gap {
        lost,
        got: Some(got),
    }
}

/// SQL `NULL` and an empty cell mean "no value", so a `Null` that is not an attribute change
/// comes back as an attribute that is absent.
const NULL_IS_NO_VALUE: [Gap; 2] = [
    lost(r#"event e2 f = Null"#),
    lost(r#"object o2 s @1970-01-01T00:00:00+00:00 = Null"#),
];

fn assert_round_trip(format: &str, back: &OCEL, gaps: &[Gap]) {
    let src = facts(&full_log());
    let back = facts(back);
    let mut expected: Vec<String> = src
        .iter()
        .filter(|f| !gaps.iter().any(|g| g.lost == f.as_str()))
        .cloned()
        .chain(gaps.iter().filter_map(|g| g.got.map(str::to_string)))
        .collect();
    expected.sort();
    for g in gaps {
        assert!(
            src.iter().any(|f| f == g.lost),
            "{format}: listed gap {:?} is not a fact of the source log",
            g.lost
        );
    }
    let missing: Vec<_> = expected.iter().filter(|f| !back.contains(f)).collect();
    let unexpected: Vec<_> = back.iter().filter(|f| !expected.contains(f)).collect();
    assert!(
        missing.is_empty() && unexpected.is_empty(),
        "{format}: round trip differs from what is expected of the format\n\
         expected but missing:\n  {}\nnot expected:\n  {}",
        missing
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n  "),
        unexpected
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n  "),
    );
}

#[test]
fn json_round_trip_is_exact() {
    let bytes = full_log().export_to_bytes("json").unwrap();
    assert_round_trip(
        "JSON",
        &OCEL::import_from_bytes(&bytes, "json").unwrap(),
        &[],
    );
}

#[test]
fn xml_round_trip() {
    let bytes = full_log().export_to_bytes("xml").unwrap();
    assert_round_trip(
        "XML",
        &OCEL::import_from_bytes(&bytes, "xml").unwrap(),
        &[
            NULL_IS_NO_VALUE[0],
            NULL_IS_NO_VALUE[1],
            // Format limitation: XML has no null, so a change to `Null` is written as an empty
            // element, which a string attribute reads back as the empty string.
            changed(
                r#"object o1 s @2024-01-10T00:00:00+00:00 = Null"#,
                r#"object o1 s @2024-01-10T00:00:00+00:00 = String("")"#,
            ),
        ],
    );
}

#[test]
fn csv_round_trip() {
    let bytes = full_log().export_to_bytes("ocel.csv").unwrap();
    assert_round_trip(
        "CSV",
        &OCEL::import_from_bytes(&bytes, "ocel.csv").unwrap(),
        &[
            // Format limitation: an empty cell is a missing value.
            lost(r#"event e2 f = Null"#),
            lost(r#"event e2 s = String("")"#),
        ],
    );
}

#[cfg(feature = "ocel-sqlite")]
#[test]
fn sqlite_round_trip() {
    let bytes = full_log().export_to_bytes("sqlite").unwrap();
    assert_round_trip(
        "SQLite",
        &OCEL::import_from_bytes(&bytes, "sqlite").unwrap(),
        &NULL_IS_NO_VALUE,
    );
}

#[cfg(feature = "ocel-duckdb")]
#[test]
fn duckdb_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.duckdb");
    full_log().export_to_path(&path).unwrap();
    assert_round_trip(
        "DuckDB",
        &OCEL::import_from_path(&path).unwrap(),
        &NULL_IS_NO_VALUE,
    );
}

#[cfg(feature = "ocel-duckdb")]
#[test]
fn duckdb_consolidated_round_trip() {
    use process_mining::core::event_data::object_centric::ocel_sql::write_ocel_to_duckdb;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.duckdb");
    write_ocel_to_duckdb(&full_log(), &path, &Default::default()).unwrap();
    assert_round_trip(
        "DuckDB (consolidated)",
        &OCEL::import_from_path(&path).unwrap(),
        // Event attributes are typed columns, where `NULL` is no value. Object attribute values
        // keep `Null`.
        &[NULL_IS_NO_VALUE[0]],
    );
}

#[cfg(feature = "ocel-bundle")]
fn through_bundle(format: &str) -> OCEL {
    use process_mining::core::event_data::object_centric::ocel_bundle::Container;
    let dir = tempfile::tempdir().unwrap();
    full_log()
        .export_to_path_as(dir.path(), format, Default::default())
        .unwrap();
    Container::open(dir.path()).and_then(|c| c.read()).unwrap()
}

#[cfg(feature = "ocel-bundle")]
#[test]
fn bundle_csv_round_trip() {
    assert_round_trip(
        "bundle (CSV)",
        &through_bundle("ocel.zip"),
        &[
            NULL_IS_NO_VALUE[0],
            NULL_IS_NO_VALUE[1],
            // Format limitation: "missing values are represented by empty cells", so the
            // empty string is no value, and a change to it a change to `Null`.
            lost(r#"event e2 s = String("")"#),
            changed(
                r#"object o1 s @2024-01-08T00:00:00+00:00 = String("")"#,
                r#"object o1 s @2024-01-08T00:00:00+00:00 = Null"#,
            ),
        ],
    );
}

#[cfg(feature = "ocel-bundle-parquet")]
#[test]
fn bundle_parquet_round_trip() {
    assert_round_trip(
        "bundle (Parquet)",
        &through_bundle("ocel-parquet.zip"),
        &NULL_IS_NO_VALUE,
    );
}

/// Values and names the SQL formats got wrong: an integer beyond 32 bits, names with a quote or
/// differing only in case, and values whose variant is not the attribute's declared type.
#[cfg(any(feature = "ocel-sqlite", feature = "ocel-duckdb"))]
fn sql_edge_log() -> OCEL {
    let attr = |name: &str, value_type: &str| OCELTypeAttribute {
        name: name.to_string(),
        value_type: value_type.to_string(),
    };
    let typ = |name: &str, attributes| OCELType {
        name: name.to_string(),
        attributes,
    };
    let event = |id: &str, event_type: &str, attributes| OCELEvent {
        id: id.to_string(),
        event_type: event_type.to_string(),
        time: time("2024-01-01T10:00:00+00:00"),
        attributes,
        relationships: Vec::new(),
    };
    let object = |id: &str, object_type: &str, attributes| OCELObject {
        id: id.to_string(),
        object_type: object_type.to_string(),
        attributes,
        relationships: Vec::new(),
    };
    OCEL {
        event_types: vec![
            typ(
                "order",
                vec![
                    attr("big", "integer"),
                    attr("customer's", "string"),
                    attr("txt", "string"),
                    attr("flag", "boolean"),
                    attr("n", "integer"),
                ],
            ),
            typ("Order", Vec::new()),
        ],
        object_types: vec![
            typ(
                "a'b",
                vec![attr("customer's", "string"), attr("Big", "integer")],
            ),
            typ("a_b", Vec::new()),
        ],
        events: vec![
            event(
                "e1",
                "order",
                vec![
                    ev_attr("big", V::Integer(3_000_000_000)),
                    ev_attr("customer's", V::String("x".to_string())),
                    ev_attr("txt", V::Boolean(true)),
                    ev_attr("flag", V::Integer(1)),
                    ev_attr("n", V::Float(2.5)),
                ],
            ),
            event("e2", "order", vec![ev_attr("txt", V::Float(1e20))]),
            event("e3", "Order", Vec::new()),
        ],
        objects: vec![
            object(
                "o1",
                "a'b",
                vec![
                    ob_attr("customer's", V::String("y".to_string()), EPOCH),
                    ob_attr(
                        "customer's",
                        V::String("z".to_string()),
                        "2024-01-02T00:00:00+00:00",
                    ),
                    ob_attr("Big", V::Integer(-5_000_000_000), EPOCH),
                ],
            ),
            object("o2", "a_b", Vec::new()),
        ],
    }
}

/// What `sql_edge_log` reads back as: a mismatched value is converted to the declared type.
#[cfg(any(feature = "ocel-sqlite", feature = "ocel-duckdb"))]
fn assert_sql_edge_round_trip(format: &str, back: &OCEL) {
    let mut expected = facts(&sql_edge_log());
    for (from, to) in [
        (
            "e1 txt = Boolean(true)",
            r#"e1 txt = String("true")"#.to_string(),
        ),
        (
            "e2 txt = Float(1e20)",
            r#"e2 txt = String("100000000000000000000")"#.to_string(),
        ),
        (
            "e1 flag = Integer(1)",
            "e1 flag = Boolean(true)".to_string(),
        ),
        ("e1 n = Float(2.5)", "e1 n = Integer(3)".to_string()),
    ] {
        let fact = expected
            .iter_mut()
            .find(|f| f.ends_with(from))
            .unwrap_or_else(|| panic!("no fact {from:?}"));
        *fact = format!("event {to}");
    }
    expected.sort();
    assert_eq!(facts(back), expected, "{format}");
}

#[cfg(feature = "ocel-sqlite")]
#[test]
fn sqlite_round_trip_of_sql_edge_cases() {
    let bytes = sql_edge_log().export_to_bytes("sqlite").unwrap();
    assert_sql_edge_round_trip(
        "SQLite",
        &OCEL::import_from_bytes(&bytes, "sqlite").unwrap(),
    );
}

#[cfg(feature = "ocel-duckdb")]
#[test]
fn duckdb_round_trip_of_sql_edge_cases() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.duckdb");
    sql_edge_log().export_to_path(&path).unwrap();
    assert_sql_edge_round_trip("DuckDB", &OCEL::import_from_path(&path).unwrap());
}

/// Type names whose tables would be named like `SQLite`'s fixed indexes (`event_id`,
/// `event_object_source`) or like the index of another type's table (object type `event` and
/// event type `obid`), or that `DuckDB` would read as a qualified name (`v1.2`).
#[cfg(any(feature = "ocel-sqlite", feature = "ocel-duckdb"))]
fn sql_type_name_log() -> OCEL {
    let typ = |name: &str| OCELType {
        name: name.to_string(),
        attributes: vec![OCELTypeAttribute {
            name: "a".to_string(),
            value_type: "string".to_string(),
        }],
    };
    let names = [
        "id",
        "object_source",
        "v1.2",
        "event",
        "obid",
        "event_ocel_id",
    ];
    OCEL {
        event_types: names.map(typ).to_vec(),
        object_types: names.map(typ).to_vec(),
        events: names
            .iter()
            .map(|n| OCELEvent {
                id: format!("e-{n}"),
                event_type: n.to_string(),
                time: time("2024-01-01T10:00:00+00:00"),
                attributes: vec![ev_attr("a", V::String(n.to_string()))],
                relationships: Vec::new(),
            })
            .collect(),
        objects: names
            .iter()
            .map(|n| OCELObject {
                id: format!("o-{n}"),
                object_type: n.to_string(),
                attributes: vec![ob_attr("a", V::String(n.to_string()), EPOCH)],
                relationships: Vec::new(),
            })
            .collect(),
    }
}

#[cfg(feature = "ocel-sqlite")]
#[test]
fn sqlite_round_trip_of_type_names_like_fixed_names() {
    let bytes = sql_type_name_log().export_to_bytes("sqlite").unwrap();
    let back = OCEL::import_from_bytes(&bytes, "sqlite").unwrap();
    assert_eq!(facts(&back), facts(&sql_type_name_log()));
}

#[cfg(feature = "ocel-duckdb")]
#[test]
fn duckdb_round_trip_of_type_names_like_fixed_names() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.duckdb");
    sql_type_name_log().export_to_path(&path).unwrap();
    let back = OCEL::import_from_path(&path).unwrap();
    assert_eq!(facts(&back), facts(&sql_type_name_log()));
}

/// `sql_edge_log` with text that is not a number in the integer attribute `big`.
#[cfg(feature = "ocel-duckdb")]
fn sql_log_with_text_in_an_integer_attribute() -> OCEL {
    let mut log = sql_edge_log();
    log.events[0].attributes[0].value = V::String("n/a".to_string());
    log
}

/// Text that is not a number cannot fill an integer column. It is written as `NULL` instead of
/// failing the export.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn duckdb_export_writes_text_in_an_integer_attribute_as_null() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.duckdb");
    sql_log_with_text_in_an_integer_attribute()
        .export_to_path(&path)
        .unwrap();
    let back = OCEL::import_from_path(&path).unwrap();
    let e1 = back.events.iter().find(|e| e.id == "e1").unwrap();
    assert!(e1.attributes.iter().all(|a| a.name != "big"), "{e1:?}");
}

/// Both SQL formats convert mismatched values by the same rules, so they read back alike.
#[cfg(all(feature = "ocel-sqlite", feature = "ocel-duckdb"))]
#[test]
fn sqlite_and_duckdb_export_read_back_alike() {
    let log = sql_log_with_text_in_an_integer_attribute();
    let sqlite =
        OCEL::import_from_bytes(&log.export_to_bytes("sqlite").unwrap(), "sqlite").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.duckdb");
    log.export_to_path(&path).unwrap();
    let duckdb = OCEL::import_from_path(&path).unwrap();
    assert_eq!(facts(&sqlite), facts(&duckdb));
    for fact in [
        "event e1 n = Integer(3)",
        "event e1 flag = Boolean(true)",
        r#"event e1 txt = String("true")"#,
    ] {
        assert!(facts(&sqlite).iter().any(|f| f == fact), "{fact}");
    }
    assert!(!facts(&sqlite).iter().any(|f| f.starts_with("event e1 big")));
}

/// The consolidated schema keeps all event attributes in one wide table next to its own `id`,
/// `ocel_type` and `time` columns, and `DuckDB` compares column names case-insensitively.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn duckdb_consolidated_round_trip_of_colliding_attribute_names() {
    use process_mining::core::event_data::object_centric::ocel_sql::{
        generate_type_views, write_ocel_to_duckdb,
    };
    let names = ["Time", "ID", "amount", "Amount", "ocel_TYPE"];
    let log = OCEL {
        event_types: vec![OCELType {
            name: "x".to_string(),
            attributes: names
                .iter()
                .map(|n| OCELTypeAttribute {
                    name: n.to_string(),
                    value_type: "integer".to_string(),
                })
                .chain([OCELTypeAttribute {
                    name: "flag".to_string(),
                    value_type: "boolean".to_string(),
                }])
                .collect(),
        }],
        object_types: Vec::new(),
        events: vec![OCELEvent {
            id: "e1".to_string(),
            event_type: "x".to_string(),
            time: time("2024-01-01T10:00:00+00:00"),
            attributes: names
                .iter()
                .zip(1..)
                .map(|(n, i)| ev_attr(n, V::Integer(i)))
                .chain([ev_attr("flag", V::Integer(0))])
                .collect(),
            relationships: Vec::new(),
        }],
        objects: Vec::new(),
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.duckdb");
    write_ocel_to_duckdb(&log, &path, &Default::default()).unwrap();
    let back = OCEL::import_from_path(&path).unwrap();
    let mut expected = facts(&log);
    for f in &mut expected {
        if f.ends_with("flag = Integer(0)") {
            *f = "event e1 flag = Boolean(false)".to_string();
        }
    }
    expected.sort();
    assert_eq!(facts(&back), expected);
    generate_type_views(&duckdb::Connection::open(&path).unwrap()).unwrap();
}

/// Attribute names that are fixed columns of an OCEL 2.0 SQL type table, or that differ only in
/// case, on events and on objects with a history.
#[cfg(any(feature = "ocel-sqlite", feature = "ocel-duckdb"))]
fn sql_reserved_attribute_log() -> OCEL {
    let names = [
        "ocel_id",
        "ocel_time",
        "ocel_changed_field",
        "OCEL_ID",
        "Price",
        "price",
        "price_2",
    ];
    let typ = |name: &str| OCELType {
        name: name.to_string(),
        attributes: names
            .iter()
            .map(|n| OCELTypeAttribute {
                name: n.to_string(),
                value_type: "integer".to_string(),
            })
            .collect(),
    };
    OCEL {
        event_types: vec![typ("x")],
        object_types: vec![typ("y")],
        events: vec![OCELEvent {
            id: "e1".to_string(),
            event_type: "x".to_string(),
            time: time("2024-01-01T10:00:00+00:00"),
            attributes: names
                .iter()
                .zip(1..)
                .map(|(n, i)| ev_attr(n, V::Integer(i)))
                .collect(),
            relationships: Vec::new(),
        }],
        objects: vec![OCELObject {
            id: "o1".to_string(),
            object_type: "y".to_string(),
            attributes: names
                .iter()
                .zip(1..)
                .map(|(n, i)| ob_attr(n, V::Integer(i), EPOCH))
                .chain(
                    names
                        .iter()
                        .zip(11..)
                        .map(|(n, i)| ob_attr(n, V::Integer(i), "2024-01-02T10:00:00+00:00")),
                )
                .collect(),
            relationships: Vec::new(),
        }],
    }
}

#[cfg(feature = "ocel-sqlite")]
#[test]
fn sqlite_round_trip_of_reserved_attribute_names() {
    let bytes = sql_reserved_attribute_log()
        .export_to_bytes("sqlite")
        .unwrap();
    let back = OCEL::import_from_bytes(&bytes, "sqlite").unwrap();
    assert_eq!(facts(&back), facts(&sql_reserved_attribute_log()));
}

#[cfg(feature = "ocel-duckdb")]
#[test]
fn duckdb_round_trip_of_reserved_attribute_names() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.duckdb");
    sql_reserved_attribute_log().export_to_path(&path).unwrap();
    let back = OCEL::import_from_path(&path).unwrap();
    assert_eq!(facts(&back), facts(&sql_reserved_attribute_log()));
}

/// Two object types of one name cannot both be in `object_map_type`, so the export fails, and
/// leaves no partial file behind.
#[cfg(any(feature = "ocel-sqlite", feature = "ocel-duckdb"))]
fn sql_unexportable_log() -> OCEL {
    let mut log = sql_reserved_attribute_log();
    log.object_types.push(log.object_types[0].clone());
    log
}

#[cfg(feature = "ocel-sqlite")]
#[test]
fn sqlite_export_failure_leaves_no_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.sqlite");
    assert!(sql_unexportable_log().export_to_path(&path).is_err());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[cfg(feature = "ocel-duckdb")]
#[test]
fn duckdb_export_failure_leaves_no_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.duckdb");
    assert!(sql_unexportable_log().export_to_path(&path).is_err());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[cfg(feature = "ocel-sqlite")]
#[test]
fn sqlite_export_failure_keeps_the_existing_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.sqlite");
    std::fs::write(&path, "kept").unwrap();
    assert!(sql_unexportable_log().export_to_path(&path).is_err());
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "kept");
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[cfg(feature = "ocel-duckdb")]
#[test]
fn duckdb_export_failure_keeps_the_existing_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.duckdb");
    std::fs::write(&path, "kept").unwrap();
    assert!(sql_unexportable_log().export_to_path(&path).is_err());
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "kept");
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

/// Exporting over an existing log replaces it, and leaves no other file.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn duckdb_export_replaces_an_existing_log() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.duckdb");
    sql_type_name_log().export_to_path(&path).unwrap();
    sql_reserved_attribute_log().export_to_path(&path).unwrap();
    let back = OCEL::import_from_path(&path).unwrap();
    assert_eq!(facts(&back), facts(&sql_reserved_attribute_log()));
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}
