//! End-to-end tests: `RowProvider`, node graph execution, `ExtractionSink`, mapping execution
//! and `ExtractionReport`.
#![cfg(all(test, feature = "ocel-sqlite"))]

use std::collections::HashMap;

use rusqlite::{params, Connection};
use tempfile::tempdir;

use super::blueprint::{
    Blueprint, DuplicateObjectPolicy, EventEndpoint, IdRendering, InlineObjectRef, Mapping,
    MappingEntry, MissingEndpointPolicy, Node, NodeOp, ObjectEndpoint, Target,
};
use super::case_centric::FlatEventTable;
use super::catalog::{ExtractionCatalog, TableSchema};
use super::expr::{AttributeMapping, SplitKind, SplitSpec, TimestampSource, ValueExpression};
use super::extract::extract;
use super::predicate::{CompareOp, Literal, Operand, Predicate};
use super::provider::RowProvider;
use super::report::{DropReason, ExtractionError};
use super::slim_sink::SlimOcelSink;
use super::sqlite_provider::SqliteRowProvider;
use super::validate::validate;
use crate::core::event_data::object_centric::linked_ocel::LinkedOCELAccess;
use crate::core::event_data::object_centric::utils::flatten::flatten_ocel_on;
use crate::core::event_data::object_centric::OCELAttributeType;
use crate::core::event_data::object_centric::OCELAttributeValue;

/// A fresh `SQLite` file in its own temp directory, and the still-open build connection used to
/// populate it. Every test gets its own directory, since a shared fixture path corrupts under
/// concurrent test runs.
struct Fixture {
    _dir: tempfile::TempDir,
    path: std::path::PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("fixture.sqlite");
        Self { _dir: dir, path }
    }

    fn build(&self) -> Connection {
        Connection::open(&self.path).expect("open sqlite file")
    }

    fn provider(&self) -> SqliteRowProvider {
        SqliteRowProvider::open(&self.path).expect("reopen sqlite file")
    }
}

fn providers_of<'a>(
    source_id: &str,
    provider: &'a SqliteRowProvider,
) -> HashMap<String, &'a dyn RowProvider> {
    let mut m: HashMap<String, &dyn RowProvider> = HashMap::new();
    m.insert(source_id.to_string(), provider);
    m
}

fn source_node(id: &str, source_id: &str, table: &str) -> Node {
    Node {
        id: id.to_string(),
        label: None,
        op: NodeOp::Source {
            source_id: source_id.to_string(),
            table: table.to_string(),
        },
    }
}

fn col(name: &str) -> ValueExpression {
    ValueExpression::Column {
        column: name.to_string(),
    }
}

fn constant(value: &str) -> ValueExpression {
    ValueExpression::Constant {
        value: value.to_string(),
    }
}

fn blank_blueprint(nodes: Vec<Node>, mappings: Vec<MappingEntry>) -> Blueprint {
    Blueprint {
        version: super::MODEL_VERSION,
        id_rendering: IdRendering::Raw,
        nodes,
        mappings,
        on_missing_endpoint: MissingEndpointPolicy::Drop,
        on_duplicate_object: DuplicateObjectPolicy::FirstWins,
    }
}

// flat event table, case-centric.

/// Fixture + blueprint + catalog for case 1: flat, case-centric event table. Factored out so
/// the differential test can run the identical blueprint against the identical data
/// through both sinks.
fn case1_fixture_and_blueprint() -> (Fixture, Blueprint, ExtractionCatalog) {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE events (case_id TEXT, activity TEXT, ts TEXT, region TEXT);",
        )
        .unwrap();
        let rows = [
            ("A", "create", "2020-01-01T00:00:00Z", "EU"),
            ("A", "approve", "2020-01-02T00:00:00Z", "EU"),
            ("A", "close", "2020-01-03T00:00:00Z", "EU"),
            ("B", "create", "2020-01-01T00:00:00Z", "US"),
            ("B", "close", "2020-01-02T00:00:00Z", "US"),
            ("C", "create", "2020-01-01T00:00:00Z", "US"),
        ];
        for (case_id, activity, ts, region) in rows {
            con.execute(
                "INSERT INTO events (case_id, activity, ts, region) VALUES (?1, ?2, ?3, ?4)",
                params![case_id, activity, ts, region],
            )
            .unwrap();
        }
    }

    let bp = Blueprint::from_flat_event_table(FlatEventTable {
        source_id: "db".into(),
        table: "events".into(),
        case_id: "case_id".into(),
        activity: "activity".into(),
        timestamp: "ts".into(),
        case_object_type: "Case".into(),
        case_attributes: vec![],
        event_attributes: vec![],
    });
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "events",
            [
                ("case_id", "TEXT", false),
                ("activity", "TEXT", false),
                ("ts", "TEXT", false),
                ("region", "TEXT", true),
            ],
        ),
    );
    (fx, bp, catalog)
}

#[test]
fn case_1_flat_event_table_case_centric() {
    let (fx, bp, catalog) = case1_fixture_and_blueprint();
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");

    let ocel = sink.ocel();
    assert_eq!(
        ocel.get_obs_of_type("Case").count(),
        3,
        "one object per distinct case"
    );
    assert_eq!(ocel.get_all_evs().count(), 6, "one event per row");
    let e2o_total: usize = ocel.get_all_evs().map(|e| ocel.get_e2o(e).count()).sum();
    assert_eq!(e2o_total, 6, "one E2O per row");

    assert_eq!(report.per_mapping.len(), 1);
    let stats = &report.per_mapping[0];
    // Zero, not `rows - distinct cases`. This blueprint's single mapping creates its case objects
    // through an inline object reference, and resolving a relation endpoint is not a
    // deduplication (see `MappingStats::deduplicated`). Counting only the repeats among those
    // resolutions is what the per-mapping id set used to buy, and a deferring sink could never
    // have agreed with it. The three repeated case ids still produce three objects, not six; only
    // the counter changed.
    assert_eq!(
        stats.deduplicated, 0,
        "an inline object reference resolves an endpoint; it does not deduplicate an entity"
    );
    assert!(stats.dropped.is_empty(), "zero drops: {:?}", stats.dropped);

    let log = flatten_ocel_on(ocel, "Case");
    assert_eq!(log.traces.len(), 3);
    let trace_a = log
        .traces
        .iter()
        .find(|t| t.events.len() == 3)
        .expect("case A's trace");
    assert_eq!(trace_a.events.len(), 3);
}

// discriminated table (zero-match type still declared).

#[test]
fn case_2_discriminated_table_declares_zero_match_type() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE docs (id INTEGER, kind TEXT);")
            .unwrap();
        con.execute("INSERT INTO docs (id, kind) VALUES (1, 'invoice')", [])
            .unwrap();
        con.execute("INSERT INTO docs (id, kind) VALUES (2, 'invoice')", [])
            .unwrap();
        con.execute("INSERT INTO docs (id, kind) VALUES (3, 'bill')", [])
            .unwrap();
    }

    let node = source_node("docs", "db", "docs");
    let object_mapping = |label: &str, kind: &str, object_type: &str| {
        MappingEntry::Single(Mapping {
            node: "docs".into(),
            label: Some(label.into()),
            when: Some(Predicate::Compare {
                left: Operand::Column {
                    column: "kind".into(),
                },
                op: CompareOp::Eq,
                right: Operand::Literal {
                    value: Literal::Text(kind.into()),
                },
            }),
            target: Target::Object {
                object_type: constant(object_type),
                id: col("id"),
                timestamp: None,
                attributes: vec![],
            },
        })
    };
    let bp = blank_blueprint(
        vec![node],
        vec![
            object_mapping("invoices", "invoice", "Invoice"),
            object_mapping("bills", "bill", "Bill"),
            object_mapping("credit_notes", "credit_note", "CreditNote"),
        ],
    );
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new("docs", [("id", "INTEGER", false), ("kind", "TEXT", false)]),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");

    let ocel = sink.ocel();
    let mut types: Vec<&str> = ocel.get_ob_types().collect();
    types.sort_unstable();
    assert_eq!(
        types,
        vec!["Bill", "CreditNote", "Invoice"],
        "all three declared, even CreditNote"
    );
    assert_eq!(ocel.get_obs_of_type("Invoice").count(), 2);
    assert_eq!(ocel.get_obs_of_type("Bill").count(), 1);
    assert_eq!(ocel.get_obs_of_type("CreditNote").count(), 0);

    let credit_notes = report
        .per_mapping
        .iter()
        .find(|m| m.mapping.label.as_deref() == Some("credit_notes"))
        .expect("credit_notes stats");
    assert_eq!(credit_notes.rows_read, 3);
    assert_eq!(credit_notes.entities_emitted, 0);
    assert_eq!(
        credit_notes.dropped.get(&DropReason::PredicateExcluded),
        Some(&3)
    );
}

// ordered group, first-match-wins, equals hand-desugared.

#[test]
fn case_3_ordered_group_first_match_wins() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE changes (id INTEGER, new_status TEXT);")
            .unwrap();
        for (id, status) in [(1, "C"), (2, "P"), (3, "C"), (4, "X")] {
            con.execute(
                "INSERT INTO changes (id, new_status) VALUES (?1, ?2)",
                params![id, status],
            )
            .unwrap();
        }
    }

    let node = source_node("changes", "db", "changes");
    let event_target = |event_type: &str| Target::Event {
        event_type: constant(event_type),
        id: Some(col("id")),
        timestamp: TimestampSource::constant("2020-01-01T00:00:00Z"),
        attributes: vec![],
        objects: vec![],
    };
    let completed = Mapping {
        node: "changes".into(),
        label: Some("completed".into()),
        when: Some(Predicate::Compare {
            left: Operand::Column {
                column: "new_status".into(),
            },
            op: CompareOp::Eq,
            right: Operand::Literal {
                value: Literal::Text("C".into()),
            },
        }),
        target: event_target("Completed"),
    };
    let changed = Mapping {
        node: "changes".into(),
        label: Some("changed".into()),
        when: None,
        target: event_target("Changed"),
    };
    let ordered_bp = blank_blueprint(
        vec![node.clone()],
        vec![MappingEntry::Ordered {
            mappings: vec![completed, changed],
        }],
    );
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "changes",
            [("id", "INTEGER", false), ("new_status", "TEXT", false)],
        ),
    );
    assert_eq!(validate(&ordered_bp, &catalog), vec![]);

    let desugared_mappings = super::desugar::desugar(&ordered_bp);
    let hand_bp = blank_blueprint(
        vec![node],
        desugared_mappings
            .into_iter()
            .map(MappingEntry::Single)
            .collect(),
    );

    let run = |bp: &Blueprint| {
        let provider = fx.provider();
        let providers = providers_of("db", &provider);
        let mut sink = SlimOcelSink::new();
        extract(bp, &catalog, &providers, &mut sink).expect("extract");
        let ocel = sink.into_ocel();
        let mut ids_by_type: Vec<(String, String)> = ocel
            .get_all_evs()
            .map(|e| (e.get_ev_type(&ocel).clone(), e.get_ev(&ocel).id.clone()))
            .collect();
        ids_by_type.sort_unstable();
        ids_by_type
    };

    let ordered_result = run(&ordered_bp);
    let hand_result = run(&hand_bp);
    assert_eq!(
        ordered_result, hand_result,
        "ordered sugar must equal hand-desugared"
    );
    assert_eq!(
        ordered_result,
        vec![
            ("Changed".to_string(), "2".to_string()),
            ("Changed".to_string(), "4".to_string()),
            ("Completed".to_string(), "1".to_string()),
            ("Completed".to_string(), "3".to_string()),
        ],
        "first-match-wins: only rows without new_status='C' fall through to 'changed'"
    );
}

// join, right_<name> rule agrees with `validate`.

/// Fixture + blueprint + catalog for case 4: two tables joined on a shared column name. Factored
/// out so the differential test can reuse it. See [`case1_fixture_and_blueprint`].
fn case4_fixture_and_blueprint() -> (Fixture, Blueprint, ExtractionCatalog) {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE orders (id INTEGER, amount INTEGER);
             CREATE TABLE meta (id INTEGER, region TEXT);",
        )
        .unwrap();
        for (id, amount) in [(1, 100), (2, 200), (3, 300)] {
            con.execute(
                "INSERT INTO orders (id, amount) VALUES (?1, ?2)",
                params![id, amount],
            )
            .unwrap();
        }
        for (id, region) in [(1, "EU"), (2, "US")] {
            con.execute(
                "INSERT INTO meta (id, region) VALUES (?1, ?2)",
                params![id, region],
            )
            .unwrap();
        }
    }

    let left = source_node("orders", "db", "orders");
    let right = source_node("meta", "db", "meta");
    let join = Node {
        id: "joined".into(),
        label: None,
        op: NodeOp::Join {
            left: "orders".into(),
            right: "meta".into(),
            on: vec![("id".into(), "id".into())],
        },
    };
    let mapping = MappingEntry::Single(Mapping {
        node: "joined".into(),
        label: None,
        when: None,
        target: Target::Object {
            object_type: constant("Order"),
            id: col("right_id"),
            timestamp: None,
            attributes: vec![
                AttributeMapping {
                    source_column: "amount".into(),
                    name: "amount".into(),
                    value_type: None,
                },
                AttributeMapping {
                    source_column: "region".into(),
                    name: "region".into(),
                    value_type: None,
                },
            ],
        },
    });
    let bp = blank_blueprint(vec![left, right, join], vec![mapping]);
    let catalog = ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new(
                "orders",
                [("id", "INTEGER", false), ("amount", "INTEGER", false)],
            ),
        )
        .with_table(
            "db",
            TableSchema::new(
                "meta",
                [("id", "INTEGER", false), ("region", "TEXT", false)],
            ),
        );
    (fx, bp, catalog)
}

#[test]
fn case_4_join_right_prefix_matches_validate_prediction() {
    let (fx, bp, catalog) = case4_fixture_and_blueprint();
    // If graph.rs's runtime column resolution disagreed with validate's node_columns
    // prediction, `right_id`/`region` would be reported UnknownColumn here.
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    let ocel = sink.ocel();
    assert_eq!(
        ocel.get_obs_of_type("Order").count(),
        2,
        "inner join drops unmatched id=3"
    );

    for (id, amount, region) in [("1", 100i64, "EU"), ("2", 200i64, "US")] {
        let ob = ocel.get_ob_by_id(id).expect("object by right_id");
        let amount_val = &ob.get_attribute_value("amount", ocel).expect("amount attr")[0].1;
        assert_eq!(amount_val, &OCELAttributeValue::Integer(amount));
        let region_val = &ob.get_attribute_value("region", ocel).expect("region attr")[0].1;
        assert_eq!(region_val, &OCELAttributeValue::String(region.to_string()));
    }
}

/// C1: a `Filter` hands its consumers the input's row verbatim, so its projected schema must be
/// the input's, not a narrower one. `demanded_columns` used to propagate demand downward only,
/// so a source read by both a mapping and a filter produced rows wider than the filter's own
/// schema, and every mapping reading the filter then silently indexed the wrong slot.
///
/// The dropped column is deliberately not the last one: `b` sits between `a` and `c`, so the
/// filter's narrower schema `{a, c}` puts `c` at slot 1, where the row actually carries `b`.
#[test]
fn c1_a_filter_reads_the_same_row_layout_its_consumers_index_it_with() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE t (a INTEGER, b TEXT, c TEXT);")
            .unwrap();
        con.execute("INSERT INTO t VALUES (1, 'X', 'Y')", [])
            .unwrap();
    }

    let source = source_node("s", "db", "t");
    let filter = Node {
        id: "f".into(),
        label: None,
        op: NodeOp::Filter {
            input: "s".into(),
            condition: Predicate::Compare {
                left: Operand::Column { column: "a".into() },
                op: CompareOp::Eq,
                right: Operand::Literal {
                    value: Literal::Integer(1),
                },
            },
        },
    };
    // Two consumers of `s`: this mapping (which needs `b`) and the filter (which needs `a`).
    let via_source = MappingEntry::Single(Mapping {
        node: "s".into(),
        label: Some("via_source".into()),
        when: None,
        target: Target::Object {
            object_type: constant("FromSource"),
            id: col("b"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let via_filter = MappingEntry::Single(Mapping {
        node: "f".into(),
        label: Some("via_filter".into()),
        when: None,
        target: Target::Object {
            object_type: constant("FromFilter"),
            id: col("c"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let bp = blank_blueprint(vec![source, filter], vec![via_source, via_filter]);
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "t",
            [
                ("a", "INTEGER", false),
                ("b", "TEXT", false),
                ("c", "TEXT", false),
            ],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    let ocel = sink.ocel();

    let from_filter: Vec<String> = ocel
        .get_obs_of_type("FromFilter")
        .map(|o| o.get_ob(ocel).id.clone())
        .collect();
    assert_eq!(
        from_filter,
        vec!["Y".to_string()],
        "the filter's mapping must read column c, not whatever sits at c's position in a \
         narrower projection"
    );
    let from_source: Vec<String> = ocel
        .get_obs_of_type("FromSource")
        .map(|o| o.get_ob(ocel).id.clone())
        .collect();
    assert_eq!(from_source, vec!["X".to_string()]);
}

/// The `right_<name>` rule is one rule, shared by demand routing and the runtime row assembly.
/// A left table with a real column literally named `right_foo` used to route demand to the right
/// input, so it was never fetched from the left and materialised as `Null`, while the runtime
/// rule resolved it from the left.
#[test]
fn a_left_column_literally_named_right_foo_resolves_from_the_left() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE l (id INTEGER, right_foo TEXT);
             CREATE TABLE r (id INTEGER, other TEXT);",
        )
        .unwrap();
        con.execute("INSERT INTO l VALUES (1, 'from-left')", [])
            .unwrap();
        con.execute("INSERT INTO r VALUES (1, 'x')", []).unwrap();
    }

    let join = Node {
        id: "joined".into(),
        label: None,
        op: NodeOp::Join {
            left: "l".into(),
            right: "r".into(),
            on: vec![("id".into(), "id".into())],
        },
    };
    let mapping = MappingEntry::Single(Mapping {
        node: "joined".into(),
        label: None,
        when: None,
        target: Target::Object {
            object_type: constant("Thing"),
            id: col("right_foo"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let bp = blank_blueprint(
        vec![
            source_node("l", "db", "l"),
            source_node("r", "db", "r"),
            join,
        ],
        vec![mapping],
    );
    let catalog = ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new(
                "l",
                [("id", "INTEGER", false), ("right_foo", "TEXT", false)],
            ),
        )
        .with_table(
            "db",
            TableSchema::new("r", [("id", "INTEGER", false), ("other", "TEXT", false)]),
        );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert!(
        sink.ocel().get_ob_by_id("from-left").is_some(),
        "left's own right_foo column must be fetched from the left, not materialised as Null; \
         dropped: {:?}",
        report.per_mapping[0].dropped
    );
}

/// The `right_<name>` rule's second disagreement, which the test above cannot reach because
/// there `right_id`'s unprefixed twin is the join key and so is demanded from the left anyway.
///
/// Here `foo` exists on both sides but is neither a join key nor read by any mapping, so nothing
/// demands it from the left. The runtime rule tested "does the left have `foo`?" against the
/// projected left schema, which by then did not, so `right_foo` materialised as `Null` and the
/// row was dropped for an unusable id. Both rules now ask both sides' full schemas.
#[test]
fn a_right_prefixed_column_resolves_when_its_unprefixed_twin_is_not_a_join_key() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE l (id INTEGER, foo TEXT);
             CREATE TABLE r (id INTEGER, foo TEXT);",
        )
        .unwrap();
        con.execute("INSERT INTO l VALUES (1, 'left-foo')", [])
            .unwrap();
        con.execute("INSERT INTO r VALUES (1, 'right-foo')", [])
            .unwrap();
    }

    let join = Node {
        id: "joined".into(),
        label: None,
        op: NodeOp::Join {
            left: "l".into(),
            right: "r".into(),
            on: vec![("id".into(), "id".into())],
        },
    };
    let mapping = MappingEntry::Single(Mapping {
        node: "joined".into(),
        label: None,
        when: None,
        target: Target::Object {
            object_type: constant("Thing"),
            // The only column any mapping reads: `foo` itself is never demanded from the left.
            id: col("right_foo"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let bp = blank_blueprint(
        vec![
            source_node("l", "db", "l"),
            source_node("r", "db", "r"),
            join,
        ],
        vec![mapping],
    );
    let catalog = ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new("l", [("id", "INTEGER", false), ("foo", "TEXT", false)]),
        )
        .with_table(
            "db",
            TableSchema::new("r", [("id", "INTEGER", false), ("foo", "TEXT", false)]),
        );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert!(
        sink.ocel().get_ob_by_id("right-foo").is_some(),
        "right_foo must be read from the right input, not materialised as Null; dropped: {:?}",
        report.per_mapping[0].dropped
    );
}

/// I-e: a join key column that is a `Float` used to render through `Value::canonical_string`,
/// which is `None` for floats, so the join silently produced zero rows where SQL joins them.
#[test]
fn a_join_on_a_float_key_matches_rows_instead_of_silently_producing_none() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE l (k REAL, name TEXT);
             CREATE TABLE r (k REAL, note TEXT);",
        )
        .unwrap();
        con.execute("INSERT INTO l VALUES (1.5, 'a')", []).unwrap();
        con.execute("INSERT INTO r VALUES (1.5, 'b')", []).unwrap();
    }

    let join = Node {
        id: "joined".into(),
        label: None,
        op: NodeOp::Join {
            left: "l".into(),
            right: "r".into(),
            on: vec![("k".into(), "k".into())],
        },
    };
    let mapping = MappingEntry::Single(Mapping {
        node: "joined".into(),
        label: None,
        when: None,
        target: Target::Object {
            object_type: constant("Pair"),
            id: col("name"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let bp = blank_blueprint(
        vec![
            source_node("l", "db", "l"),
            source_node("r", "db", "r"),
            join,
        ],
        vec![mapping],
    );
    let catalog = ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new("l", [("k", "REAL", false), ("name", "TEXT", false)]),
        )
        .with_table(
            "db",
            TableSchema::new("r", [("k", "REAL", false), ("note", "TEXT", false)]),
        );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert_eq!(
        sink.ocel().get_obs_of_type("Pair").count(),
        1,
        "a float join key must join, as it does in SQL"
    );
}

/// The projection contract: an empty `columns` means "no columns, one callback per row", not
/// "every column". Falling back to `SELECT *` handed the caller rows wider than the index it
/// reads them with.
#[test]
fn an_empty_projection_yields_empty_rows_not_every_column() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE t (a INTEGER, b TEXT, c TEXT);")
            .unwrap();
        for i in 0..3 {
            con.execute("INSERT INTO t VALUES (?1, 'x', 'y')", params![i])
                .unwrap();
        }
    }
    let provider = fx.provider();
    let mut widths = Vec::new();
    provider
        .scan("t", &[], &mut |vals| {
            widths.push(vals.len());
            std::ops::ControlFlow::Continue(())
        })
        .expect("scan");
    assert_eq!(widths, vec![0, 0, 0], "one empty row per table row");
}

// C3: endpoint resolution is staged, so mapping order cannot change the result.

/// Fixture, catalog and the three mappings of the C3 test, so the test can assemble them in
/// either order.
fn c3_fixture() -> (Fixture, ExtractionCatalog, Vec<MappingEntry>, Vec<Node>) {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE objs (id TEXT, kind TEXT);
             CREATE TABLE rels (src TEXT, dst TEXT);",
        )
        .unwrap();
        con.execute("INSERT INTO objs VALUES ('a', 'A')", [])
            .unwrap();
        con.execute("INSERT INTO objs VALUES ('b', 'B')", [])
            .unwrap();
        con.execute("INSERT INTO rels VALUES ('a', 'b')", [])
            .unwrap();
    }
    let nodes = vec![
        source_node("objs", "db", "objs"),
        source_node("rels", "db", "rels"),
    ];
    let of_kind = |kind: &str, object_type: &str| {
        MappingEntry::Single(Mapping {
            node: "objs".into(),
            label: Some(object_type.to_string()),
            when: Some(Predicate::Compare {
                left: Operand::Column {
                    column: "kind".into(),
                },
                op: CompareOp::Eq,
                right: Operand::Literal {
                    value: Literal::Text(kind.into()),
                },
            }),
            target: Target::Object {
                object_type: constant(object_type),
                id: col("id"),
                timestamp: None,
                attributes: vec![],
            },
        })
    };
    let o2o = MappingEntry::Single(Mapping {
        node: "rels".into(),
        label: Some("o2o".into()),
        when: None,
        target: Target::O2O {
            source: ObjectEndpoint {
                id: col("src"),
                object_type: Some(constant("A")),
                split: None,
            },
            target: ObjectEndpoint {
                id: col("dst"),
                object_type: Some(constant("B")),
                split: None,
            },
            qualifier: Some(constant("links")),
        },
    });
    let catalog = ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new("objs", [("id", "TEXT", false), ("kind", "TEXT", false)]),
        )
        .with_table(
            "db",
            TableSchema::new("rels", [("src", "TEXT", false), ("dst", "TEXT", false)]),
        );
    (
        fx,
        catalog,
        vec![o2o, of_kind("A", "A"), of_kind("B", "B")],
        nodes,
    )
}

/// C3: a blueprint whose `O2O` mapping is written before the mappings creating its endpoints
/// must produce the same log as the reverse order.
///
/// Resolution used to see only what had been emitted so far, under a first-seen-node execution
/// order the author never wrote, so this legal blueprint silently dropped its one relation. SQL
/// cannot reproduce that either: a compiled relation view joins against all objects.
#[test]
fn c3_mapping_order_does_not_change_the_result() {
    use super::differential::snapshot;

    let run = |mappings: Vec<MappingEntry>| {
        let (fx, catalog, _, nodes) = c3_fixture();
        let bp = blank_blueprint(nodes, mappings);
        assert_eq!(validate(&bp, &catalog), vec![]);
        let provider = fx.provider();
        let providers = providers_of("db", &provider);
        let mut sink = SlimOcelSink::new();
        let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
        (snapshot(sink.ocel()), report)
    };

    let (_, _, mappings, _) = c3_fixture();
    let relation_first = mappings.clone();
    let mut endpoints_first = mappings;
    endpoints_first.rotate_left(1); // [A, B, o2o]

    let (relation_first_snapshot, report) = run(relation_first);
    let (endpoints_first_snapshot, _) = run(endpoints_first);

    assert_eq!(
        relation_first_snapshot, endpoints_first_snapshot,
        "the order the mappings are written in must not change the log"
    );
    assert_eq!(
        relation_first_snapshot
            .objects
            .get("a")
            .expect("object a")
            .o2o,
        vec![("links".to_string(), "b".to_string())],
        "the relation must be emitted, not dropped: {:?}",
        report.per_mapping
    );
}

/// C3, the other half: an event's inline object reference must also resolve against every
/// object the blueprint produces, not merely those emitted before it.
///
/// A `Target::Event` with `id: None` cannot emit its references in the relations pass, since a
/// run-minted UUID cannot be re-derived there, so they are emitted in the events pass. That used
/// to be the same pass as objects, where a `Target::Object` mapping written after the event
/// mapping had not run yet. With
/// `on_missing_endpoint: Drop` the first row of every case then lost its relation, and writing
/// the two mappings the other way round kept it. `Blueprint::from_flat_event_table` generates
/// precisely this shape, and escapes only because it hardcodes `Create`.
///
/// Event ids are minted per run, so the two runs are compared on everything except them.
#[test]
fn c3_an_inline_object_reference_resolves_against_every_object_whatever_the_order() {
    use super::differential::{snapshot, OcelSnapshot};

    let build = || {
        let fx = Fixture::new();
        {
            let con = fx.build();
            con.execute_batch(
                "CREATE TABLE events (case_id TEXT, activity TEXT, ts TEXT, region TEXT);",
            )
            .unwrap();
            for (case_id, activity, ts, region) in [
                ("A", "create", "2020-01-01T00:00:00Z", "EU"),
                ("A", "approve", "2020-01-02T00:00:00Z", "EU"),
                ("A", "close", "2020-01-03T00:00:00Z", "EU"),
                ("B", "create", "2020-01-01T00:00:00Z", "US"),
                ("B", "close", "2020-01-02T00:00:00Z", "US"),
                ("C", "create", "2020-01-01T00:00:00Z", "US"),
            ] {
                con.execute(
                    "INSERT INTO events (case_id, activity, ts, region) VALUES (?1, ?2, ?3, ?4)",
                    params![case_id, activity, ts, region],
                )
                .unwrap();
            }
        }
        let mut bp = Blueprint::from_flat_event_table(FlatEventTable {
            source_id: "db".into(),
            table: "events".into(),
            case_id: "case_id".into(),
            activity: "activity".into(),
            timestamp: "ts".into(),
            case_object_type: "Case".into(),
            case_attributes: vec![AttributeMapping {
                source_column: "region".into(),
                name: "region".into(),
                value_type: None,
            }],
            event_attributes: vec![],
        });
        // The generator hardcodes `Create`, which hides the ordering question by synthesising
        // whatever has not been emitted yet. `Drop` exposes it, and is a setting an author may
        // legitimately choose for the same blueprint.
        bp.on_missing_endpoint = MissingEndpointPolicy::Drop;
        let catalog = ExtractionCatalog::new().with_table(
            "db",
            TableSchema::new(
                "events",
                [
                    ("case_id", "TEXT", false),
                    ("activity", "TEXT", false),
                    ("ts", "TEXT", false),
                    ("region", "TEXT", false),
                ],
            ),
        );
        (fx, bp, catalog)
    };

    // Everything a minted event id does not make incomparable between two runs.
    let comparable = |s: &OcelSnapshot| {
        let mut events: Vec<_> = s
            .events
            .values()
            .map(|e| (e.event_type.clone(), e.time, e.e2o.clone()))
            .collect();
        events.sort();
        (events, s.objects.clone())
    };

    let run = |event_mapping_first: bool| {
        let (fx, mut bp, catalog) = build();
        assert_eq!(bp.mappings.len(), 2, "one event mapping, one case mapping");
        if !event_mapping_first {
            bp.mappings.reverse();
        }
        assert_eq!(validate(&bp, &catalog), vec![]);
        let provider = fx.provider();
        let providers = providers_of("db", &provider);
        let mut sink = SlimOcelSink::new();
        extract(&bp, &catalog, &providers, &mut sink).expect("extract");
        comparable(&snapshot(sink.ocel()))
    };

    let event_first = run(true);
    let objects_first = run(false);
    assert_eq!(
        event_first, objects_first,
        "the order the mappings are written in must not change the log"
    );
    assert_eq!(
        event_first
            .0
            .iter()
            .filter(|(_, _, e2o)| !e2o.is_empty())
            .count(),
        6,
        "every row's case object exists, so every row keeps its relation"
    );
}

/// `add_event` deduplicates on id, and the dropped one must not be counted as emitted. (The
/// invariant is named after a bug in the OCPQ extractor, which discarded `add_event`'s return
/// value while checking `add_object`'s, so `total_events` overreported.)
#[test]
fn i4_a_duplicate_event_id_is_counted_as_deduplicated_not_as_emitted() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE events (id TEXT, activity TEXT, ts TEXT);")
            .unwrap();
        con.execute(
            "INSERT INTO events VALUES ('e1', 'A', '2020-01-01T00:00:00Z')",
            [],
        )
        .unwrap();
        con.execute(
            "INSERT INTO events VALUES ('e1', 'A', '2020-01-02T00:00:00Z')",
            [],
        )
        .unwrap();
    }
    let mapping = MappingEntry::Single(Mapping {
        node: "events".into(),
        label: None,
        when: None,
        target: Target::Event {
            event_type: col("activity"),
            id: Some(col("id")),
            timestamp: TimestampSource::column("ts"),
            attributes: vec![],
            objects: vec![],
        },
    });
    let bp = blank_blueprint(vec![source_node("events", "db", "events")], vec![mapping]);
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "events",
            [
                ("id", "TEXT", false),
                ("activity", "TEXT", false),
                ("ts", "TEXT", false),
            ],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");

    assert_eq!(sink.ocel().get_all_evs().count(), 1, "one event survives");
    let stats = &report.per_mapping[0];
    assert_eq!(stats.rows_read, 2);
    assert_eq!(
        stats.entities_emitted, 1,
        "the deduplicated row must not be counted as emitted"
    );
    assert_eq!(stats.deduplicated, 1);
    assert!(stats.dropped.is_empty(), "a repeat is not a drop");
}

/// I-d: a row whose event id repeats an earlier row's used to return before its inline object
/// references were processed, so those relations vanished with no `DropReason`. They belong to
/// the row, not to the event insertion, and a compiled relation view emits one per row regardless
/// of how many distinct events the id column names.
#[test]
fn i_d_a_duplicate_event_id_keeps_that_row_s_inline_object_relations() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE events (id TEXT, activity TEXT, ts TEXT, obj TEXT);")
            .unwrap();
        con.execute(
            "INSERT INTO events VALUES ('e1', 'A', '2020-01-01T00:00:00Z', 'o1')",
            [],
        )
        .unwrap();
        con.execute(
            "INSERT INTO events VALUES ('e1', 'A', '2020-01-02T00:00:00Z', 'o2')",
            [],
        )
        .unwrap();
    }
    let mapping = MappingEntry::Single(Mapping {
        node: "events".into(),
        label: None,
        when: None,
        target: Target::Event {
            event_type: col("activity"),
            id: Some(col("id")),
            timestamp: TimestampSource::column("ts"),
            attributes: vec![],
            objects: vec![InlineObjectRef {
                object: ObjectEndpoint {
                    id: col("obj"),
                    object_type: Some(constant("Thing")),
                    split: None,
                },
                qualifier: Some(constant("uses")),
            }],
        },
    });
    let mut bp = blank_blueprint(vec![source_node("events", "db", "events")], vec![mapping]);
    bp.on_missing_endpoint = MissingEndpointPolicy::Create;
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "events",
            [
                ("id", "TEXT", false),
                ("activity", "TEXT", false),
                ("ts", "TEXT", false),
                ("obj", "TEXT", false),
            ],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");

    let ocel = sink.ocel();
    assert_eq!(ocel.get_all_evs().count(), 1);
    let e2o_total: usize = ocel.get_all_evs().map(|e| ocel.get_e2o(e).count()).sum();
    assert_eq!(
        e2o_total, 2,
        "both rows' inline relations survive; dropped: {:?}",
        report.per_mapping[0].dropped
    );
    assert!(ocel.get_ob_by_id("o2").is_some(), "the second row's object");
}

/// I-c: `deduplicated` used to count every relation endpoint that resolved, so an `E2O` mapping
/// over n rows with every endpoint present and every id distinct reported n deduplications while
/// nothing had been deduplicated.
#[test]
fn i_c_resolving_an_existing_endpoint_is_not_a_deduplication() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE objs (id TEXT);
             CREATE TABLE evs (id TEXT, activity TEXT, ts TEXT);
             CREATE TABLE rels (ev TEXT, ob TEXT);",
        )
        .unwrap();
        for i in 1..=3 {
            con.execute("INSERT INTO objs VALUES (?1)", params![format!("o{i}")])
                .unwrap();
            con.execute(
                "INSERT INTO evs VALUES (?1, 'A', '2020-01-01T00:00:00Z')",
                params![format!("e{i}")],
            )
            .unwrap();
            con.execute(
                "INSERT INTO rels VALUES (?1, ?2)",
                params![format!("e{i}"), format!("o{i}")],
            )
            .unwrap();
        }
    }
    let objects = MappingEntry::Single(Mapping {
        node: "objs".into(),
        label: Some("objects".into()),
        when: None,
        target: Target::Object {
            object_type: constant("Thing"),
            id: col("id"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let events = MappingEntry::Single(Mapping {
        node: "evs".into(),
        label: Some("events".into()),
        when: None,
        target: Target::Event {
            event_type: col("activity"),
            id: Some(col("id")),
            timestamp: TimestampSource::column("ts"),
            attributes: vec![],
            objects: vec![],
        },
    });
    let e2o = MappingEntry::Single(Mapping {
        node: "rels".into(),
        label: Some("e2o".into()),
        when: None,
        target: Target::E2O {
            event: EventEndpoint {
                id: col("ev"),
                event_type: None,
            },
            object: ObjectEndpoint {
                id: col("ob"),
                object_type: Some(constant("Thing")),
                split: None,
            },
            qualifier: Some(constant("uses")),
        },
    });
    let bp = blank_blueprint(
        vec![
            source_node("objs", "db", "objs"),
            source_node("evs", "db", "evs"),
            source_node("rels", "db", "rels"),
        ],
        vec![objects, events, e2o],
    );
    let catalog = ExtractionCatalog::new()
        .with_table("db", TableSchema::new("objs", [("id", "TEXT", false)]))
        .with_table(
            "db",
            TableSchema::new(
                "evs",
                [
                    ("id", "TEXT", false),
                    ("activity", "TEXT", false),
                    ("ts", "TEXT", false),
                ],
            ),
        )
        .with_table(
            "db",
            TableSchema::new("rels", [("ev", "TEXT", false), ("ob", "TEXT", false)]),
        );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");

    let stats = report
        .per_mapping
        .iter()
        .find(|m| m.mapping.label.as_deref() == Some("e2o"))
        .expect("e2o stats");
    assert_eq!(stats.entities_emitted, 3, "three relations");
    assert!(stats.dropped.is_empty(), "no drops: {:?}", stats.dropped);
    assert_eq!(
        stats.deduplicated, 0,
        "nothing was deduplicated: three distinct endpoints, each resolved once"
    );
}

/// I-a: two object types sharing one rendered id under `IdRendering::Raw` (the default). The
/// lookup ignored the type, so the second type's object silently merged into the first's, and
/// writing the second's attributes onto it failed as soon as the first type had not declared that
/// attribute name, aborting the whole extraction with `?`.
#[test]
fn i_a_a_cross_type_id_collision_is_reported_not_merged_and_does_not_abort() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE things (id TEXT, kind TEXT, note TEXT);")
            .unwrap();
        con.execute("INSERT INTO things VALUES ('1', 'order', NULL)", [])
            .unwrap();
        con.execute("INSERT INTO things VALUES ('1', 'item', 'n')", [])
            .unwrap();
    }
    let of_kind = |kind: &str, object_type: &str, attributes: Vec<AttributeMapping>| {
        MappingEntry::Single(Mapping {
            node: "things".into(),
            label: Some(object_type.to_string()),
            when: Some(Predicate::Compare {
                left: Operand::Column {
                    column: "kind".into(),
                },
                op: CompareOp::Eq,
                right: Operand::Literal {
                    value: Literal::Text(kind.into()),
                },
            }),
            target: Target::Object {
                object_type: constant(object_type),
                id: col("id"),
                timestamp: None,
                attributes,
            },
        })
    };
    let bp = blank_blueprint(
        vec![source_node("things", "db", "things")],
        vec![
            of_kind("order", "Order", vec![]),
            of_kind(
                "item",
                "Item",
                vec![AttributeMapping {
                    source_column: "note".into(),
                    name: "note".into(),
                    value_type: None,
                }],
            ),
        ],
    );
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "things",
            [
                ("id", "TEXT", false),
                ("kind", "TEXT", false),
                ("note", "TEXT", true),
            ],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("must not abort the run");

    let ocel = sink.ocel();
    assert_eq!(ocel.get_obs_of_type("Order").count(), 1);
    assert_eq!(
        ocel.get_obs_of_type("Item").count(),
        0,
        "the colliding Item is not merged into the Order"
    );
    let item = report
        .per_mapping
        .iter()
        .find(|m| m.mapping.label.as_deref() == Some("Item"))
        .expect("Item stats");
    assert_eq!(
        item.dropped.get(&DropReason::IdTypeCollision),
        Some(&1),
        "a collision has its own reason: {:?}",
        item.dropped
    );
    assert_eq!(item.deduplicated, 0, "a collision is not a deduplication");
    assert!(
        report.errors.iter().any(|e| matches!(
            e,
            ExtractionError::IdTypeCollision { id, requested_type, .. }
                if id == "1" && requested_type == "Item"
        )),
        "collision reported: {:?}",
        report.errors
    );
}

/// Fixture, blueprint and catalog for the lazy-declaration case: two object mappings over one row, naming the same
/// object id, where only the second carries an attribute. Factored out so case 11 can run the
/// identical blueprint through a deferring sink, the shape whose two sinks disagreed.
fn dynamic_type_fixture_and_blueprint() -> (Fixture, Blueprint, ExtractionCatalog) {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE rows_ (id TEXT, kind TEXT, note TEXT);")
            .unwrap();
        con.execute("INSERT INTO rows_ VALUES ('x', 'T', 'hello')", [])
            .unwrap();
    }
    // Both mappings name the object type dynamically, so neither is declared up front by the
    // static pass. The first declares the type with no attributes and adds the object; the
    // second then grows the type and writes to the object that already exists.
    let bare = MappingEntry::Single(Mapping {
        node: "rows_".into(),
        label: Some("bare".into()),
        when: None,
        target: Target::Object {
            object_type: col("kind"),
            id: col("id"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let with_attr = MappingEntry::Single(Mapping {
        node: "rows_".into(),
        label: Some("with_attr".into()),
        when: None,
        target: Target::Object {
            object_type: col("kind"),
            id: col("id"),
            timestamp: None,
            attributes: vec![AttributeMapping {
                source_column: "note".into(),
                name: "note".into(),
                value_type: None,
            }],
        },
    });
    let bp = blank_blueprint(
        vec![source_node("rows_", "db", "rows_")],
        vec![bare, with_attr],
    );
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "rows_",
            [
                ("id", "TEXT", false),
                ("kind", "TEXT", false),
                ("note", "TEXT", true),
            ],
        ),
    );
    (fx, bp, catalog)
}

/// A type that gains an attribute after an object of it was added must not make the next
/// attribute write fail. Reachable through any dynamic object type, which declares lazily, one row
/// at a time.
#[test]
fn dynamic_type_a_type_gaining_an_attribute_after_an_object_exists_does_not_abort() {
    let (fx, bp, catalog) = dynamic_type_fixture_and_blueprint();
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    extract(&bp, &catalog, &providers, &mut sink).expect("must not abort the run");

    let ocel = sink.ocel();
    let obj = ocel.get_ob_by_id("x").expect("object x");
    let note = obj
        .get_attribute_value("note", ocel)
        .expect("note history on the grown type");
    assert_eq!(note[0].1, OCELAttributeValue::String("hello".into()));
}

/// I-f: an empty id cell is not an identity. `''` is how an ERP export routinely writes "no id";
/// accepting it collapsed every such row into one entity whose id is the empty string, counted as
/// deduplication rather than as the loss it is.
#[test]
fn i_f_an_empty_id_is_dropped_not_treated_as_an_identity() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE objs (id TEXT);").unwrap();
        for id in ["", "", "real"] {
            con.execute("INSERT INTO objs VALUES (?1)", params![id])
                .unwrap();
        }
    }
    let mapping = MappingEntry::Single(Mapping {
        node: "objs".into(),
        label: None,
        when: None,
        target: Target::Object {
            object_type: constant("Thing"),
            id: col("id"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let bp = blank_blueprint(vec![source_node("objs", "db", "objs")], vec![mapping]);
    let catalog =
        ExtractionCatalog::new().with_table("db", TableSchema::new("objs", [("id", "TEXT", true)]));
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");

    assert_eq!(
        sink.ocel().get_obs_of_type("Thing").count(),
        1,
        "only the row with a real id produces an object"
    );
    assert!(sink.ocel().get_ob_by_id("").is_none());
    let stats = &report.per_mapping[0];
    assert_eq!(
        stats.dropped.get(&DropReason::NullOrUnrenderableId),
        Some(&2),
        "both empty ids are counted as unusable ids: {:?}",
        stats.dropped
    );
    assert_eq!(stats.deduplicated, 0, "they are not one entity seen twice");
}

/// I-g: two mappings declaring one attribute of one type under different value types. The sink
/// merges last-wins while each mapping converts its own rows against the type it declared, so the
/// attribute ends up holding values of two types under one declaration and nothing noticed.
#[test]
fn i_g_conflicting_attribute_declarations_are_reported_and_widened() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE rows_ (id TEXT, kind TEXT, amount TEXT);")
            .unwrap();
        con.execute("INSERT INTO rows_ VALUES ('a', 'x', '1')", [])
            .unwrap();
        con.execute("INSERT INTO rows_ VALUES ('b', 'y', 'text')", [])
            .unwrap();
    }
    let typed = |label: &str, kind: &str, value_type: OCELAttributeType| {
        MappingEntry::Single(Mapping {
            node: "rows_".into(),
            label: Some(label.into()),
            when: Some(Predicate::Compare {
                left: Operand::Column {
                    column: "kind".into(),
                },
                op: CompareOp::Eq,
                right: Operand::Literal {
                    value: Literal::Text(kind.into()),
                },
            }),
            target: Target::Object {
                object_type: constant("Thing"),
                id: col("id"),
                timestamp: None,
                attributes: vec![AttributeMapping {
                    source_column: "amount".into(),
                    name: "amount".into(),
                    value_type: Some(value_type),
                }],
            },
        })
    };
    let bp = blank_blueprint(
        vec![source_node("rows_", "db", "rows_")],
        vec![
            typed("as_integer", "x", OCELAttributeType::Integer),
            typed("as_string", "y", OCELAttributeType::String),
        ],
    );
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "rows_",
            [
                ("id", "TEXT", false),
                ("kind", "TEXT", false),
                ("amount", "TEXT", false),
            ],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");

    assert!(
        report.errors.iter().any(|e| matches!(
            e,
            ExtractionError::ConflictingAttributeType { type_name, attribute, .. }
                if type_name == "Thing" && attribute == "amount"
        )),
        "the conflict must be reported: {:?}",
        report.errors
    );
    use crate::core::event_data::object_centric::readable::ReadableOCEL;
    let declared = sink
        .ocel()
        .object_types()
        .iter()
        .find(|t| t.name == "Thing")
        .expect("Thing declared")
        .clone();
    let amount = declared
        .attributes
        .iter()
        .find(|a| a.name == "amount")
        .expect("amount declared");
    assert_eq!(
        amount.value_type,
        OCELAttributeType::String.to_type_string(),
        "the declaration is widened to a type covering both, not whichever ran last"
    );

    // Widening the declaration is only half of it: the values stored under it must be of the
    // widened type as well, or the attribute still holds two types at once, which was the
    // problem. `a` is the row written by the mapping that declared `integer`.
    let ocel = sink.ocel();
    for (id, expected) in [("a", "1"), ("b", "text")] {
        let obj = ocel.get_ob_by_id(id).expect("object");
        let value = &obj
            .get_attribute_value("amount", ocel)
            .expect("amount history")[0]
            .1;
        assert_eq!(
            value,
            &OCELAttributeValue::String(expected.to_string()),
            "object '{id}' must hold a value of the type 'amount' is declared with"
        );
    }
}

// `on_missing_endpoint` at every endpoint kind, not just an O2O's target.

/// One event, one existing object, and a relation row naming a missing object plus a relation
/// row naming a missing event. `kind` selects which mapping the blueprint carries.
///
/// The two rows name different missing objects on purpose. Sharing one id hid a divergence:
/// `run_e2o` gives up before it ever looks at the object endpoint of the row whose event is
/// missing, so under `Create` the eager sink never creates that object, while a deferring sink,
/// which cannot fail an event endpoint at all, staged it and synthesised it at finalize.
fn i5_fixture(target: Target, node: &str) -> (Fixture, Blueprint, ExtractionCatalog) {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE evs (id TEXT, activity TEXT, ts TEXT);
             CREATE TABLE rels (ev TEXT, ob TEXT);",
        )
        .unwrap();
        con.execute(
            "INSERT INTO evs VALUES ('e1', 'A', '2020-01-01T00:00:00Z')",
            [],
        )
        .unwrap();
        con.execute("INSERT INTO rels VALUES ('e1', 'ghost')", [])
            .unwrap();
        con.execute("INSERT INTO rels VALUES ('no-such-event', 'ghost2')", [])
            .unwrap();
    }
    let events = MappingEntry::Single(Mapping {
        node: "evs".into(),
        label: Some("events".into()),
        when: None,
        target: Target::Event {
            event_type: col("activity"),
            id: Some(col("id")),
            timestamp: TimestampSource::column("ts"),
            attributes: vec![],
            objects: vec![],
        },
    });
    let under_test = MappingEntry::Single(Mapping {
        node: node.into(),
        label: Some("under_test".into()),
        when: None,
        target,
    });
    let bp = blank_blueprint(
        vec![
            source_node("evs", "db", "evs"),
            source_node("rels", "db", "rels"),
        ],
        vec![events, under_test],
    );
    let catalog = ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new(
                "evs",
                [
                    ("id", "TEXT", false),
                    ("activity", "TEXT", false),
                    ("ts", "TEXT", false),
                ],
            ),
        )
        .with_table(
            "db",
            TableSchema::new("rels", [("ev", "TEXT", false), ("ob", "TEXT", false)]),
        );
    (fx, bp, catalog)
}

fn e2o_target() -> Target {
    Target::E2O {
        event: EventEndpoint {
            id: col("ev"),
            event_type: None,
        },
        object: ObjectEndpoint {
            id: col("ob"),
            object_type: Some(constant("Thing")),
            split: None,
        },
        qualifier: Some(constant("uses")),
    }
}

fn i5_run(
    bp: &Blueprint,
    catalog: &ExtractionCatalog,
    fx: &Fixture,
) -> (SlimOcelSink, super::report::ExtractionReport) {
    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(bp, catalog, &providers, &mut sink).expect("extract");
    (sink, report)
}

/// `E2O`'s object side: the policy applies exactly as it does to an `O2O`'s target.
#[test]
fn i5_e2o_object_side_honours_every_missing_endpoint_policy() {
    for (policy, expect_objects, expect_drops, expect_error) in [
        (MissingEndpointPolicy::Drop, 0, 2u64, false),
        // One relation is still dropped: its event endpoint is missing, and `Create` cannot
        // synthesise an event.
        (MissingEndpointPolicy::Create, 1, 1, false),
        (MissingEndpointPolicy::Error, 0, 2, true),
    ] {
        let (fx, mut bp, catalog) = i5_fixture(e2o_target(), "rels");
        bp.on_missing_endpoint = policy;
        assert_eq!(validate(&bp, &catalog), vec![]);
        let (sink, report) = i5_run(&bp, &catalog, &fx);
        let stats = report
            .per_mapping
            .iter()
            .find(|m| m.mapping.label.as_deref() == Some("under_test"))
            .expect("stats");
        assert_eq!(
            sink.ocel().get_obs_of_type("Thing").count(),
            expect_objects,
            "{policy:?}: object synthesis"
        );
        assert_eq!(
            stats.dropped.get(&DropReason::UnresolvedEndpoint).copied(),
            Some(expect_drops),
            "{policy:?}: drops {:?}",
            stats.dropped
        );
        assert_eq!(
            report
                .errors
                .iter()
                .any(|e| matches!(e, ExtractionError::MissingEndpoint { .. })),
            expect_error,
            "{policy:?}: errors {:?}",
            report.errors
        );
    }
}

/// `E2O`'s event side, which behaves differently on purpose: `Create` cannot synthesise an
/// event, since there is no timestamp to give it, so it degrades to `Drop` there while still
/// creating objects.
#[test]
fn i5_create_cannot_synthesise_an_event_endpoint_and_degrades_to_drop() {
    let (fx, mut bp, catalog) = i5_fixture(e2o_target(), "rels");
    bp.on_missing_endpoint = MissingEndpointPolicy::Create;
    assert_eq!(validate(&bp, &catalog), vec![]);
    let (sink, report) = i5_run(&bp, &catalog, &fx);

    assert_eq!(
        sink.ocel().get_all_evs().count(),
        1,
        "'no-such-event' is not synthesised"
    );
    assert_eq!(
        sink.ocel().get_obs_of_type("Thing").count(),
        1,
        "the object endpoint, by contrast, is created"
    );
    let stats = report
        .per_mapping
        .iter()
        .find(|m| m.mapping.label.as_deref() == Some("under_test"))
        .expect("stats");
    assert_eq!(
        stats.dropped.get(&DropReason::UnresolvedEndpoint),
        Some(&1),
        "the row with the missing event endpoint is dropped and counted"
    );
}

/// an event's inline object reference: same policy, same code path, same counts as the `E2O`
/// object side above.
#[test]
fn i5_inline_object_references_honour_every_missing_endpoint_policy() {
    for (policy, expect_objects, expect_drops, expect_error) in [
        (MissingEndpointPolicy::Drop, 0, 1u64, false),
        (MissingEndpointPolicy::Create, 1, 0, false),
        (MissingEndpointPolicy::Error, 0, 1, true),
    ] {
        let fx = Fixture::new();
        {
            let con = fx.build();
            con.execute_batch("CREATE TABLE evs (id TEXT, activity TEXT, ts TEXT, ob TEXT);")
                .unwrap();
            con.execute(
                "INSERT INTO evs VALUES ('e1', 'A', '2020-01-01T00:00:00Z', 'ghost')",
                [],
            )
            .unwrap();
        }
        let mapping = MappingEntry::Single(Mapping {
            node: "evs".into(),
            label: Some("under_test".into()),
            when: None,
            target: Target::Event {
                event_type: col("activity"),
                id: Some(col("id")),
                timestamp: TimestampSource::column("ts"),
                attributes: vec![],
                objects: vec![InlineObjectRef {
                    object: ObjectEndpoint {
                        id: col("ob"),
                        object_type: Some(constant("Thing")),
                        split: None,
                    },
                    qualifier: Some(constant("uses")),
                }],
            },
        });
        let mut bp = blank_blueprint(vec![source_node("evs", "db", "evs")], vec![mapping]);
        bp.on_missing_endpoint = policy;
        let catalog = ExtractionCatalog::new().with_table(
            "db",
            TableSchema::new(
                "evs",
                [
                    ("id", "TEXT", false),
                    ("activity", "TEXT", false),
                    ("ts", "TEXT", false),
                    ("ob", "TEXT", false),
                ],
            ),
        );
        assert_eq!(validate(&bp, &catalog), vec![]);
        let (sink, report) = i5_run(&bp, &catalog, &fx);
        let stats = report
            .per_mapping
            .iter()
            .find(|m| m.mapping.label.as_deref() == Some("under_test"))
            .expect("stats");
        assert_eq!(
            sink.ocel().get_obs_of_type("Thing").count(),
            expect_objects,
            "{policy:?}: object synthesis"
        );
        assert_eq!(
            stats.dropped.get(&DropReason::UnresolvedEndpoint).copied(),
            Some(expect_drops).filter(|d| *d > 0),
            "{policy:?}: drops {:?}",
            stats.dropped
        );
        assert_eq!(
            report
                .errors
                .iter()
                .any(|e| matches!(e, ExtractionError::MissingEndpoint { .. })),
            expect_error,
            "{policy:?}: errors {:?}",
            report.errors
        );
    }
}

// multi-value split: delimiter and regex.

#[test]
fn case_5_multi_value_split_delimiter() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE events (id INTEGER, activity TEXT, tags TEXT);")
            .unwrap();
        con.execute("INSERT INTO events VALUES (1, 'A', 'a,b,c')", [])
            .unwrap();
        con.execute("INSERT INTO events VALUES (2, 'B', 'b,d')", [])
            .unwrap();
    }

    let node = source_node("events", "db", "events");
    let mapping = MappingEntry::Single(Mapping {
        node: "events".into(),
        label: None,
        when: None,
        target: Target::Event {
            event_type: col("activity"),
            id: Some(col("id")),
            timestamp: TimestampSource::constant("2020-01-01T00:00:00Z"),
            attributes: vec![],
            objects: vec![InlineObjectRef {
                object: ObjectEndpoint {
                    id: col("tags"),
                    object_type: Some(constant("Tag")),
                    split: Some(SplitSpec {
                        kind: SplitKind::Delimiter {
                            delimiter: ",".into(),
                        },
                        trim: true,
                    }),
                },
                qualifier: Some(constant("tag")),
            }],
        },
    });
    let mut bp = blank_blueprint(vec![node], vec![mapping]);
    bp.on_missing_endpoint = MissingEndpointPolicy::Create;
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "events",
            [
                ("id", "INTEGER", false),
                ("activity", "TEXT", false),
                ("tags", "TEXT", false),
            ],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    let ocel = sink.ocel();

    assert_eq!(
        ocel.get_obs_of_type("Tag").count(),
        4,
        "distinct tags a,b,c,d"
    );
    let e2o_total: usize = ocel.get_all_evs().map(|e| ocel.get_e2o(e).count()).sum();
    assert_eq!(e2o_total, 5, "3 tags for event 1 + 2 tags for event 2");
}

#[test]
fn case_5_multi_value_split_regex() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE events (id INTEGER, activity TEXT, tags TEXT);")
            .unwrap();
        con.execute("INSERT INTO events VALUES (1, 'A', 'x1;y22')", [])
            .unwrap();
    }

    let node = source_node("events", "db", "events");
    let mapping = MappingEntry::Single(Mapping {
        node: "events".into(),
        label: None,
        when: None,
        target: Target::Event {
            event_type: col("activity"),
            id: Some(col("id")),
            timestamp: TimestampSource::constant("2020-01-01T00:00:00Z"),
            attributes: vec![],
            objects: vec![InlineObjectRef {
                object: ObjectEndpoint {
                    id: col("tags"),
                    object_type: Some(constant("Field")),
                    split: Some(SplitSpec {
                        kind: SplitKind::Regex {
                            pattern: "[a-z][0-9]+".into(),
                        },
                        trim: true,
                    }),
                },
                qualifier: Some(constant("field")),
            }],
        },
    });
    let mut bp = blank_blueprint(vec![node], vec![mapping]);
    bp.on_missing_endpoint = MissingEndpointPolicy::Create;
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "events",
            [
                ("id", "INTEGER", false),
                ("activity", "TEXT", false),
                ("tags", "TEXT", false),
            ],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    let ocel = sink.ocel();
    assert_eq!(ocel.get_obs_of_type("Field").count(), 2, "x1 and y22");
}

// object attributes: static (single value) and change-tracked (timed history).

#[test]
fn case_6_static_object_attribute_is_single_valued_not_per_row() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE events (case_id TEXT, activity TEXT, ts TEXT, region TEXT);",
        )
        .unwrap();
        for (i, activity) in ["create", "approve", "close"].iter().enumerate() {
            con.execute(
                "INSERT INTO events VALUES ('A', ?1, ?2, 'EU')",
                params![activity, format!("2020-01-0{}T00:00:00Z", i + 1)],
            )
            .unwrap();
        }
    }

    let bp = Blueprint::from_flat_event_table(FlatEventTable {
        source_id: "db".into(),
        table: "events".into(),
        case_id: "case_id".into(),
        activity: "activity".into(),
        timestamp: "ts".into(),
        case_object_type: "Case".into(),
        case_attributes: vec![AttributeMapping {
            source_column: "region".into(),
            name: "region".into(),
            value_type: None,
        }],
        event_attributes: vec![],
    });
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "events",
            [
                ("case_id", "TEXT", false),
                ("activity", "TEXT", false),
                ("ts", "TEXT", false),
                ("region", "TEXT", true),
            ],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    let ocel = sink.ocel();
    let case = ocel.get_ob_by_id("A").expect("case A");
    let history = case
        .get_attribute_value("region", ocel)
        .expect("region history");
    assert_eq!(history.len(), 1, "one value, not one per row");
    assert_eq!(history[0].1, OCELAttributeValue::String("EU".into()));
}

#[test]
fn case_6_change_tracked_object_attribute_accumulates_a_timed_history() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE status_changes (doc_id TEXT, ts TEXT, status TEXT);")
            .unwrap();
        for (ts, status) in [
            ("2020-01-01T00:00:00Z", "draft"),
            ("2020-01-02T00:00:00Z", "open"),
            ("2020-01-03T00:00:00Z", "closed"),
        ] {
            con.execute(
                "INSERT INTO status_changes VALUES ('D1', ?1, ?2)",
                params![ts, status],
            )
            .unwrap();
        }
    }

    let node = source_node("status_changes", "db", "status_changes");
    let mapping = MappingEntry::Single(Mapping {
        node: "status_changes".into(),
        label: None,
        when: None,
        target: Target::Object {
            object_type: constant("Doc"),
            id: col("doc_id"),
            timestamp: Some(TimestampSource::column("ts")),
            attributes: vec![AttributeMapping {
                source_column: "status".into(),
                name: "status".into(),
                value_type: None,
            }],
        },
    });
    let bp = blank_blueprint(vec![node], vec![mapping]);
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "status_changes",
            [
                ("doc_id", "TEXT", false),
                ("ts", "TEXT", false),
                ("status", "TEXT", false),
            ],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    let ocel = sink.ocel();

    assert_eq!(ocel.get_obs_of_type("Doc").count(), 1);
    let doc = ocel.get_ob_by_id("D1").expect("doc D1");
    let mut history: Vec<String> = doc
        .get_attribute_value("status", ocel)
        .expect("status history")
        .iter()
        .map(|(_, v)| v.to_string())
        .collect();
    history.sort_unstable();
    assert_eq!(history, vec!["closed", "draft", "open"]);
    assert_eq!(
        report.per_mapping[0].deduplicated, 2,
        "2nd and 3rd rows repeat the id"
    );
}

// event attributes, typed from the catalog and explicitly overridden.

#[test]
fn case_7_event_attributes_typed_and_overridden() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE events (id INTEGER, activity TEXT, ts TEXT, amount TEXT);")
            .unwrap();
        con.execute(
            "INSERT INTO events VALUES (1, 'A', '2020-01-01T00:00:00Z', '100.5')",
            [],
        )
        .unwrap();
    }

    let node = source_node("events", "db", "events");
    let mapping = MappingEntry::Single(Mapping {
        node: "events".into(),
        label: None,
        when: None,
        target: Target::Event {
            event_type: col("activity"),
            id: Some(col("id")),
            timestamp: TimestampSource::column("ts"),
            attributes: vec![
                AttributeMapping {
                    source_column: "amount".into(),
                    name: "amount_num".into(),
                    value_type: None,
                },
                AttributeMapping {
                    source_column: "amount".into(),
                    name: "amount_raw".into(),
                    value_type: Some(OCELAttributeType::String),
                },
            ],
            objects: vec![],
        },
    });
    let bp = blank_blueprint(vec![node], vec![mapping]);
    // amount declared NUMERIC in the catalog, so amount_num infers Float; amount_raw overrides
    // to String explicitly.
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "events",
            [
                ("id", "INTEGER", false),
                ("activity", "TEXT", false),
                ("ts", "TEXT", false),
                ("amount", "NUMERIC", false),
            ],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    let ocel = sink.ocel();
    let ev = ocel.get_ev_by_id("1").expect("event 1");
    assert_eq!(
        ev.get_attribute_value("amount_num", ocel),
        Some(&OCELAttributeValue::Float(100.5)),
        "inferred from the catalog's NUMERIC column type"
    );
    assert_eq!(
        ev.get_attribute_value("amount_raw", ocel),
        Some(&OCELAttributeValue::String("100.5".into())),
        "explicit String override"
    );
}

// every DropReason, one fixture each.

#[test]
fn case_8_unresolved_endpoint_is_dropped_and_counted() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE rels (event_id TEXT, object_id TEXT);")
            .unwrap();
        con.execute("INSERT INTO rels VALUES ('nope', 'thing-1')", [])
            .unwrap();
    }
    let node = source_node("rels", "db", "rels");
    let mapping = MappingEntry::Single(Mapping {
        node: "rels".into(),
        label: None,
        when: None,
        target: Target::E2O {
            event: EventEndpoint {
                id: col("event_id"),
                event_type: None,
            },
            object: ObjectEndpoint {
                id: col("object_id"),
                object_type: Some(constant("Thing")),
                split: None,
            },
            qualifier: None,
        },
    });
    let bp = blank_blueprint(vec![node], vec![mapping]);
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "rels",
            [("event_id", "TEXT", false), ("object_id", "TEXT", false)],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert_eq!(
        report.per_mapping[0]
            .dropped
            .get(&DropReason::UnresolvedEndpoint),
        Some(&1)
    );
}

#[test]
fn case_8_unparseable_timestamp_is_dropped_and_counted() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE events (id INTEGER, activity TEXT, ts TEXT);")
            .unwrap();
        con.execute("INSERT INTO events VALUES (1, 'A', 'not-a-date')", [])
            .unwrap();
    }
    let node = source_node("events", "db", "events");
    let mapping = MappingEntry::Single(Mapping {
        node: "events".into(),
        label: None,
        when: None,
        target: Target::Event {
            event_type: col("activity"),
            id: Some(col("id")),
            timestamp: TimestampSource::column("ts"),
            attributes: vec![],
            objects: vec![],
        },
    });
    let bp = blank_blueprint(vec![node], vec![mapping]);
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "events",
            [
                ("id", "INTEGER", false),
                ("activity", "TEXT", false),
                ("ts", "TEXT", false),
            ],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert_eq!(
        report.per_mapping[0]
            .dropped
            .get(&DropReason::UnparseableTimestamp),
        Some(&1)
    );
    assert_eq!(sink.ocel().get_all_evs().count(), 0);
}

#[test]
fn case_8_null_id_is_dropped_and_counted() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE objs (id TEXT);").unwrap();
        con.execute("INSERT INTO objs (id) VALUES (NULL)", [])
            .unwrap();
    }
    let node = source_node("objs", "db", "objs");
    let mapping = MappingEntry::Single(Mapping {
        node: "objs".into(),
        label: None,
        when: None,
        target: Target::Object {
            object_type: constant("Thing"),
            id: col("id"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let bp = blank_blueprint(vec![node], vec![mapping]);
    let catalog =
        ExtractionCatalog::new().with_table("db", TableSchema::new("objs", [("id", "TEXT", true)]));
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert_eq!(
        report.per_mapping[0]
            .dropped
            .get(&DropReason::NullOrUnrenderableId),
        Some(&1)
    );
    assert_eq!(sink.ocel().get_obs_of_type("Thing").count(), 0);
}

#[test]
fn case_8_predicate_excluded_is_dropped_and_counted() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE objs (id INTEGER, kind TEXT);")
            .unwrap();
        con.execute("INSERT INTO objs VALUES (1, 'no')", [])
            .unwrap();
    }
    let node = source_node("objs", "db", "objs");
    let mapping = MappingEntry::Single(Mapping {
        node: "objs".into(),
        label: None,
        when: Some(Predicate::Compare {
            left: Operand::Column {
                column: "kind".into(),
            },
            op: CompareOp::Eq,
            right: Operand::Literal {
                value: Literal::Text("yes".into()),
            },
        }),
        target: Target::Object {
            object_type: constant("Thing"),
            id: col("id"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let bp = blank_blueprint(vec![node], vec![mapping]);
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new("objs", [("id", "INTEGER", false), ("kind", "TEXT", false)]),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert_eq!(
        report.per_mapping[0]
            .dropped
            .get(&DropReason::PredicateExcluded),
        Some(&1)
    );
}

// policy matrix.

fn o2o_policy_fixture() -> (Fixture, Blueprint, ExtractionCatalog) {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE sources (id TEXT);
             CREATE TABLE rels (src TEXT, dst TEXT);",
        )
        .unwrap();
        con.execute("INSERT INTO sources VALUES ('s1')", [])
            .unwrap();
        con.execute("INSERT INTO rels VALUES ('s1', 'd1')", [])
            .unwrap();
    }
    let sources_node = source_node("sources", "db", "sources");
    let rels_node = source_node("rels", "db", "rels");
    let source_mapping = MappingEntry::Single(Mapping {
        node: "sources".into(),
        label: None,
        when: None,
        target: Target::Object {
            object_type: constant("A"),
            id: col("id"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let o2o_mapping = MappingEntry::Single(Mapping {
        node: "rels".into(),
        label: Some("o2o".into()),
        when: None,
        target: Target::O2O {
            source: ObjectEndpoint {
                id: col("src"),
                object_type: Some(constant("A")),
                split: None,
            },
            target: ObjectEndpoint {
                id: col("dst"),
                object_type: Some(constant("B")),
                split: None,
            },
            qualifier: None,
        },
    });
    let bp = blank_blueprint(
        vec![sources_node, rels_node],
        vec![source_mapping, o2o_mapping],
    );
    let catalog = ExtractionCatalog::new()
        .with_table("db", TableSchema::new("sources", [("id", "TEXT", false)]))
        .with_table(
            "db",
            TableSchema::new("rels", [("src", "TEXT", false), ("dst", "TEXT", false)]),
        );
    (fx, bp, catalog)
}

#[test]
fn case_9_missing_endpoint_drop() {
    let (fx, mut bp, catalog) = o2o_policy_fixture();
    bp.on_missing_endpoint = MissingEndpointPolicy::Drop;
    assert_eq!(validate(&bp, &catalog), vec![]);
    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert_eq!(
        sink.ocel().get_obs_of_type("B").count(),
        0,
        "target never created"
    );
    let o2o_stats = report
        .per_mapping
        .iter()
        .find(|m| m.mapping.label.as_deref() == Some("o2o"))
        .unwrap();
    assert_eq!(
        o2o_stats.dropped.get(&DropReason::UnresolvedEndpoint),
        Some(&1)
    );
    assert!(report.errors.is_empty());
}

#[test]
fn case_9_missing_endpoint_create() {
    let (fx, mut bp, catalog) = o2o_policy_fixture();
    bp.on_missing_endpoint = MissingEndpointPolicy::Create;
    assert_eq!(validate(&bp, &catalog), vec![]);
    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert_eq!(
        sink.ocel().get_obs_of_type("B").count(),
        1,
        "target synthesised"
    );
    assert!(sink.ocel().get_ob_by_id("d1").is_some());
    let o2o_stats = report
        .per_mapping
        .iter()
        .find(|m| m.mapping.label.as_deref() == Some("o2o"))
        .unwrap();
    assert!(o2o_stats.dropped.is_empty());
}

#[test]
fn case_9_missing_endpoint_error() {
    let (fx, mut bp, catalog) = o2o_policy_fixture();
    bp.on_missing_endpoint = MissingEndpointPolicy::Error;
    assert_eq!(validate(&bp, &catalog), vec![]);
    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert_eq!(sink.ocel().get_obs_of_type("B").count(), 0);
    assert!(report
        .errors
        .iter()
        .any(|e| matches!(e, ExtractionError::MissingEndpoint { id, .. } if id == "d1")));
}

fn duplicate_object_fixture() -> (Fixture, Blueprint, ExtractionCatalog) {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE dups (id TEXT);").unwrap();
        con.execute("INSERT INTO dups VALUES ('x')", []).unwrap();
        con.execute("INSERT INTO dups VALUES ('x')", []).unwrap();
    }
    let node = source_node("dups", "db", "dups");
    let mapping = MappingEntry::Single(Mapping {
        node: "dups".into(),
        label: None,
        when: None,
        target: Target::Object {
            object_type: constant("Dup"),
            id: col("id"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let bp = blank_blueprint(vec![node], vec![mapping]);
    let catalog = ExtractionCatalog::new()
        .with_table("db", TableSchema::new("dups", [("id", "TEXT", false)]));
    (fx, bp, catalog)
}

#[test]
fn case_9_duplicate_object_first_wins() {
    let (fx, mut bp, catalog) = duplicate_object_fixture();
    bp.on_duplicate_object = DuplicateObjectPolicy::FirstWins;
    assert_eq!(validate(&bp, &catalog), vec![]);
    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert_eq!(sink.ocel().get_obs_of_type("Dup").count(), 1);
    assert_eq!(report.per_mapping[0].deduplicated, 1);
    assert!(report.errors.is_empty());
}

#[test]
fn case_9_duplicate_object_error() {
    let (fx, mut bp, catalog) = duplicate_object_fixture();
    bp.on_duplicate_object = DuplicateObjectPolicy::Error;
    assert_eq!(validate(&bp, &catalog), vec![]);
    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert_eq!(
        sink.ocel().get_obs_of_type("Dup").count(),
        1,
        "the first still succeeds"
    );
    assert!(report
        .errors
        .iter()
        .any(|e| matches!(e, ExtractionError::DuplicateObject { id, .. } if id == "x")));
}

// Literal coercion actually runs.

#[test]
fn case_10_text_literal_coerces_against_integer_column() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE docs (id INTEGER, docstatus INTEGER);")
            .unwrap();
        con.execute("INSERT INTO docs VALUES (1, 1)", []).unwrap();
        con.execute("INSERT INTO docs VALUES (2, 0)", []).unwrap();
    }
    let source = source_node("docs", "db", "docs");
    let filter = Node {
        id: "filtered".into(),
        label: None,
        op: NodeOp::Filter {
            input: "docs".into(),
            // Authored the way an editor's text input would: docstatus = "1", a text literal,
            // against an INTEGER column. Without coercion this matches nothing.
            condition: Predicate::Compare {
                left: Operand::Column {
                    column: "docstatus".into(),
                },
                op: CompareOp::Eq,
                right: Operand::Literal {
                    value: Literal::Text("1".into()),
                },
            },
        },
    };
    let mapping = MappingEntry::Single(Mapping {
        node: "filtered".into(),
        label: None,
        when: None,
        target: Target::Object {
            object_type: constant("Doc"),
            id: col("id"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let bp = blank_blueprint(vec![source, filter], vec![mapping]);
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "docs",
            [("id", "INTEGER", false), ("docstatus", "INTEGER", false)],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert_eq!(
        sink.ocel().get_obs_of_type("Doc").count(),
        1,
        "coercion must make docstatus=1 match"
    );
    assert!(sink.ocel().get_ob_by_id("1").is_some());
}

// both sinks agree, on the case 1 and case 4 fixtures.

/// Run `bp`/`catalog` against `fx`'s data through both `SlimOcelSink` and `DuckDbSink` --
/// simultaneously, via [`differential::run_against_both`], so an auto-generated event id (as
/// `Blueprint::from_flat_event_table`'s event mapping produces, having no `id` expression) is
/// minted once and shared rather than independently randomized per sink, and assert their
/// [`differential::snapshot`]s are equal. Reused by both case 11 tests rather than
/// inlined twice; step 3's compiler differential test is expected to build its own `OcelSnapshot`
/// and compare it the same way.
#[cfg(feature = "ocel-duckdb")]
fn assert_both_sinks_agree(fx: &Fixture, bp: &Blueprint, catalog: &ExtractionCatalog) {
    use super::differential::{run_against_both, snapshot};
    use super::duckdb_sink::DuckDbSink;

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut slim_sink = SlimOcelSink::new();

    let db_dir = tempdir().expect("tempdir");
    let db_path = db_dir.path().join("out.duckdb");
    let mut duck_sink = DuckDbSink::new(&db_path).expect("open duckdb sink");

    run_against_both(&mut slim_sink, &mut duck_sink, |sink| {
        extract(bp, catalog, &providers, sink).expect("extract")
    });

    let slim_snapshot = snapshot(slim_sink.ocel());
    let con = duckdb::Connection::open(&db_path).expect("reopen duckdb file");
    let duck_ocel = crate::core::event_data::object_centric::ocel_sql::read_ocel_from_duckdb(&con)
        .expect("read duckdb back");
    let duck_snapshot = snapshot(&duck_ocel);

    assert_eq!(
        slim_snapshot, duck_snapshot,
        "SlimOcelSink and DuckDbSink must produce the same events, objects and relations"
    );
}

/// Run `bp`/`catalog` against `fx`'s data through both sinks in separate `extract` calls, so
/// `DuckDbSink` answers every resolution [`Resolution::Deferred`] and the extractor takes its
/// deferral branches, the half of the sink-agreement invariant a fan-out run cannot reach (see
/// [`differential`]'s module docs). Returns both reports, eager first.
#[cfg(feature = "ocel-duckdb")]
fn assert_both_sinks_agree_on_separate_runs(
    fx: &Fixture,
    bp: &Blueprint,
    catalog: &ExtractionCatalog,
) -> (
    super::report::ExtractionReport,
    super::report::ExtractionReport,
) {
    use super::differential::{extract_separately, snapshot};
    use super::duckdb_sink::DuckDbSink;

    let provider = fx.provider();
    let providers = providers_of("db", &provider);

    let db_dir = tempdir().expect("tempdir");
    let db_path = db_dir.path().join("out.duckdb");
    let mut slim = SlimOcelSink::new();
    let mut duck = DuckDbSink::new(&db_path).expect("open duckdb sink");
    let (slim_report, duck_report) =
        extract_separately(bp, catalog, &providers, &mut slim, &mut duck).expect("extract");
    assert_eq!(
        slim_report.finalize,
        super::sink::FinalizeReport::default(),
        "an eager sink defers nothing"
    );

    let con = duckdb::Connection::open(&db_path).expect("reopen duckdb file");
    let duck_ocel = crate::core::event_data::object_centric::ocel_sql::read_ocel_from_duckdb(&con)
        .expect("read duckdb back");
    assert_eq!(
        snapshot(slim.ocel()),
        snapshot(&duck_ocel),
        "deferred resolution must produce the same log as eager resolution"
    );

    // The log snapshot is content, not counters, so it cannot catch two sinks agreeing on what
    // they wrote while disagreeing on how they characterise it. `deduplicated` is "the sink
    // already had this entity", which an eager sink answers at `resolve_object` and a
    // deferring one at `add_object`, so it must be identical per mapping regardless of which sink
    // produced it.
    assert_eq!(
        slim_report
            .per_mapping
            .iter()
            .map(|m| (m.mapping.index, m.deduplicated))
            .collect::<Vec<_>>(),
        duck_report
            .per_mapping
            .iter()
            .map(|m| (m.mapping.index, m.deduplicated))
            .collect::<Vec<_>>(),
        "both sinks must agree on each mapping's deduplicated count"
    );

    (slim_report, duck_report)
}

#[cfg(feature = "ocel-duckdb")]
#[test]
fn case_11_both_sinks_agree_on_flat_event_table() {
    let (fx, bp, catalog) = case1_fixture_and_blueprint();
    assert_eq!(validate(&bp, &catalog), vec![]);
    assert_both_sinks_agree(&fx, &bp, &catalog);
}

/// exercising the path the fan-out harness cannot: two separate `extract` calls, one per
/// sink, so `DuckDbSink` answers every endpoint [`Resolution::Deferred`] and resolves it by join
/// at finalize while `SlimOcelSink` resolves eagerly. Only a blueprint whose ids are all
/// author-given is comparable this way, since a minted event id differs between runs by design,
/// which is why this uses the C3 fixture rather than case 1's.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn case_11_a_deferring_sink_agrees_with_an_eager_one_on_separate_runs() {
    let (fx, catalog, mappings, nodes) = c3_fixture();
    let bp = blank_blueprint(nodes, mappings);
    assert_eq!(validate(&bp, &catalog), vec![]);

    let (_, duck_report) = assert_both_sinks_agree_on_separate_runs(&fx, &bp, &catalog);
    assert_eq!(
        duck_report.finalize.resolved_relations, 1,
        "the O2O relation's deferred endpoints both resolved at finalize"
    );
    assert_eq!(duck_report.finalize.unresolved_endpoints, 0);
}

/// `on_missing_endpoint: Error` must produce errors under a deferring sink too. It produced
/// none at all: the extractor pushes [`ExtractionError::MissingEndpoint`] where it resolves an
/// endpoint, and a deferring sink resolves none: it counts and deletes them at finalize, where
/// the mapping and the row are long gone. The policy silently degraded to `Drop`.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn i6_the_error_policy_reports_unresolved_endpoints_under_a_deferring_sink() {
    use super::duckdb_sink::DuckDbSink;

    let (fx, mut bp, catalog) = i5_fixture(e2o_target(), "rels");
    bp.on_missing_endpoint = MissingEndpointPolicy::Error;
    assert_eq!(validate(&bp, &catalog), vec![]);

    let (eager_sink, eager) = i5_run(&bp, &catalog, &fx);
    drop(eager_sink);
    let eager_errors = eager
        .errors
        .iter()
        .filter(|e| matches!(e, ExtractionError::MissingEndpoint { .. }))
        .count();
    assert_eq!(
        eager_errors, 2,
        "one per unresolved endpoint: {:?}",
        eager.errors
    );

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let db_dir = tempdir().expect("tempdir");
    let mut duck = DuckDbSink::new(db_dir.path().join("out.duckdb")).expect("open duckdb sink");
    let deferred = extract(&bp, &catalog, &providers, &mut duck).expect("extract to duckdb");

    assert_eq!(deferred.finalize.unresolved_endpoints, 2);
    assert!(
        deferred
            .errors
            .contains(&ExtractionError::MissingEndpointsAtFinalize { count: 2 }),
        "a deferring sink reports the same policy violation in bulk: {:?}",
        deferred.errors
    );
}

/// `on_missing_endpoint: Create` must synthesise the same objects under both sinks. `run_e2o`
/// returns before it looks at the object endpoint when the event endpoint does not resolve, so
/// the eager sink never creates that row's object; a deferring sink resolves no endpoint eagerly,
/// staged it, and created it at finalize even though the relation was deleted immediately after.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn case_11_create_synthesises_the_same_objects_under_both_sinks() {
    let (fx, mut bp, catalog) = i5_fixture(e2o_target(), "rels");
    bp.on_missing_endpoint = MissingEndpointPolicy::Create;
    assert_eq!(validate(&bp, &catalog), vec![]);

    let (_, duck_report) = assert_both_sinks_agree_on_separate_runs(&fx, &bp, &catalog);
    assert_eq!(
        duck_report.finalize.objects_created, 1,
        "only the endpoint of the row whose event resolves is created"
    );
}

/// `Create`'s `O2O` half, and the case the endpoint gate regressed: a relation row whose source
/// exists nowhere but that row.
///
/// The eager path creates the source, then resolves the target against a log that now contains
/// it, then creates the target too. A deferring sink cannot: it gates the target's ask on the
/// source id, and evaluated that gate against `objects` from inside the very `INSERT INTO objects`
/// that creates the source. The source was therefore never there, the target ask was ruled
/// unreachable, the target was never created, and the relation was deleted for want of it. The
/// gate has to test the set the statement is about to produce, not the one it reads.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn case_11_create_synthesises_both_ends_of_a_relation_only_o2o() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE links (src TEXT, tgt TEXT);")
            .unwrap();
        con.execute("INSERT INTO links VALUES ('s1', 't1')", [])
            .unwrap();
    }
    let o2o = MappingEntry::Single(Mapping {
        node: "links".into(),
        label: Some("o2o".into()),
        when: None,
        target: Target::O2O {
            source: ObjectEndpoint {
                id: col("src"),
                object_type: Some(constant("Src")),
                split: None,
            },
            target: ObjectEndpoint {
                id: col("tgt"),
                object_type: Some(constant("Tgt")),
                split: None,
            },
            qualifier: Some(constant("q")),
        },
    });
    let mut bp = blank_blueprint(vec![source_node("links", "db", "links")], vec![o2o]);
    bp.on_missing_endpoint = MissingEndpointPolicy::Create;
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new("links", [("src", "TEXT", false), ("tgt", "TEXT", false)]),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let (_, duck_report) = assert_both_sinks_agree_on_separate_runs(&fx, &bp, &catalog);
    assert_eq!(
        duck_report.finalize.objects_created, 2,
        "both endpoints are synthesised, the target no less than the source"
    );
    assert_eq!(duck_report.finalize.resolved_relations, 1);
    assert_eq!(duck_report.finalize.unresolved_endpoints, 0);
}

/// The `O2O` half, and the reason the endpoint gate exists at all. Moved here from
/// `duckdb_sink`'s own test module, where it drove `DuckDbSink` alone, pinning the sink under test
/// rather than the reference semantics, and gave its "unresolvable" source a type that `Create`
/// can create, so the eager path did reach the target ask and the asserted type was wrong.
///
/// A source endpoint whose type expression evaluates to `NULL` is genuinely unresolvable under
/// `Create`: there is nothing to create it as, so `run_o2o` gives up before it looks at any
/// target. The deferring sink still staged that target ask, naming `shared`, an id a real `E2O`
/// also names later and under a different type. Ungated, the earlier, unreachable ask wins
/// `arg_min` and the object is created as `Ghost` where the eager sink makes it an `Order`.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn case_11_an_o2o_target_ask_for_an_unresolvable_source_does_not_win_the_type() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE evs (id TEXT, activity TEXT, ts TEXT);
             CREATE TABLE links (src TEXT, src_type TEXT, tgt TEXT, tgt_type TEXT);
             CREATE TABLE e2orels (ev TEXT, ob TEXT);",
        )
        .unwrap();
        con.execute(
            "INSERT INTO evs VALUES ('e1', 'A', '2020-01-01T00:00:00Z')",
            [],
        )
        .unwrap();
        // `tgt_type` is read from the row rather than written as a constant so that no mapping
        // declares `Ghost` statically: an object type declared but left without a single object
        // does not survive the `DuckDB` round trip, which would fail the snapshot for a reason
        // that has nothing to do with the gate under test.
        con.execute(
            "INSERT INTO links VALUES ('no-type', NULL, 'shared', 'Ghost')",
            [],
        )
        .unwrap();
        con.execute("INSERT INTO e2orels VALUES ('e1', 'shared')", [])
            .unwrap();
    }
    // Mapping order fixes the ask order: `Phase::Relations` runs its nodes in first-seen order, so
    // the `O2O`'s target ask is staged before the `E2O`'s and would win an ungated `arg_min`.
    let o2o = MappingEntry::Single(Mapping {
        node: "links".into(),
        label: Some("o2o".into()),
        when: None,
        target: Target::O2O {
            source: ObjectEndpoint {
                id: col("src"),
                object_type: Some(col("src_type")),
                split: None,
            },
            target: ObjectEndpoint {
                id: col("tgt"),
                object_type: Some(col("tgt_type")),
                split: None,
            },
            qualifier: Some(constant("q")),
        },
    });
    let e2o = MappingEntry::Single(Mapping {
        node: "e2orels".into(),
        label: Some("e2o".into()),
        when: None,
        target: Target::E2O {
            event: EventEndpoint {
                id: col("ev"),
                event_type: None,
            },
            object: ObjectEndpoint {
                id: col("ob"),
                object_type: Some(constant("Order")),
                split: None,
            },
            qualifier: Some(constant("uses")),
        },
    });
    let events = MappingEntry::Single(Mapping {
        node: "evs".into(),
        label: Some("events".into()),
        when: None,
        target: Target::Event {
            event_type: col("activity"),
            id: Some(col("id")),
            timestamp: TimestampSource::column("ts"),
            attributes: vec![],
            objects: vec![],
        },
    });
    let mut bp = blank_blueprint(
        vec![
            source_node("links", "db", "links"),
            source_node("e2orels", "db", "e2orels"),
            source_node("evs", "db", "evs"),
        ],
        vec![o2o, e2o, events],
    );
    bp.on_missing_endpoint = MissingEndpointPolicy::Create;
    let catalog = ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new(
                "links",
                [
                    ("src", "TEXT", false),
                    ("src_type", "TEXT", true),
                    ("tgt", "TEXT", false),
                    ("tgt_type", "TEXT", false),
                ],
            ),
        )
        .with_table(
            "db",
            TableSchema::new("e2orels", [("ev", "TEXT", false), ("ob", "TEXT", false)]),
        )
        .with_table(
            "db",
            TableSchema::new(
                "evs",
                [
                    ("id", "TEXT", false),
                    ("activity", "TEXT", false),
                    ("ts", "TEXT", false),
                ],
            ),
        );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let (slim_report, duck_report) = assert_both_sinks_agree_on_separate_runs(&fx, &bp, &catalog);
    assert_eq!(
        duck_report.finalize.objects_created, 1,
        "only `shared`, and only through the E2O ask"
    );
    let slim_sink = {
        let provider = fx.provider();
        let providers = providers_of("db", &provider);
        let mut sink = SlimOcelSink::new();
        extract(&bp, &catalog, &providers, &mut sink).expect("extract");
        sink
    };
    assert_eq!(
        slim_sink
            .ocel()
            .get_ob_by_id("shared")
            .map(|o| o.get_ob_type(slim_sink.ocel()).to_string()),
        Some("Order".to_string()),
        "the eager oracle: the O2O's target ask is never made, so `Ghost` never applies: {:?}",
        slim_report.errors
    );
}

/// Two object mappings naming the same id, the second carrying an attribute (the lazy-declaration shape, and
/// an entirely ordinary blueprint). The second mapping's `resolve_object` answers `Exists` to an
/// eager sink and `Deferred` to a deferring one; the deferring path used to treat the ensuing
/// `add_object` rejection as a deduplication and throw the attributes away, so the same blueprint
/// produced an object with a `note` under one sink and without it under the other.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn case_11_a_deferring_sink_writes_a_second_mapping_s_attributes_too() {
    let (fx, bp, catalog) = dynamic_type_fixture_and_blueprint();
    assert_eq!(validate(&bp, &catalog), vec![]);

    // That the second mapping's `note` survives is what the helper's snapshot comparison pins: the
    // eager run writes it (`dynamic_type_a_type_gaining_an_attribute_after_an_object_exists_does_not_abort`)
    // and the deferring run must produce the same log. The counter is asserted for the shape of
    // the answer: the `add_object` rejection is a deduplication under both sinks, and taking that
    // path must not cost the attributes.
    let (_, duck_report) = assert_both_sinks_agree_on_separate_runs(&fx, &bp, &catalog);
    let with_attr = duck_report
        .per_mapping
        .iter()
        .find(|m| m.mapping.label.as_deref() == Some("with_attr"))
        .expect("stats");
    assert_eq!(
        with_attr.deduplicated, 1,
        "the sink already had the id, and the attributes are written anyway"
    );
}

#[cfg(feature = "ocel-duckdb")]
#[test]
fn case_11_both_sinks_agree_on_join() {
    let (fx, bp, catalog) = case4_fixture_and_blueprint();
    assert_eq!(validate(&bp, &catalog), vec![]);
    assert_both_sinks_agree(&fx, &bp, &catalog);
}

// a Source -> Filter chain streams and never materialises; a Join does.
//
// Asserted on `ExtractionReport::rows_materialized` (see graph.rs's
// `GraphExecutor::rows_materialized`) rather than on process-wide memory, which is not
// attributable to one `extract` call when tests run concurrently.

#[test]
fn case_12_source_filter_chain_does_not_materialize_rows() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE small_src (id INTEGER, val INTEGER);")
            .unwrap();
        for i in 0..10i64 {
            con.execute(
                "INSERT INTO small_src (id, val) VALUES (?1, ?2)",
                params![i, i],
            )
            .unwrap();
        }
    }

    let source = source_node("small_src", "db", "small_src");
    let filter = Node {
        id: "small".into(),
        label: None,
        op: NodeOp::Filter {
            input: "small_src".into(),
            condition: Predicate::Compare {
                left: Operand::Column {
                    column: "val".into(),
                },
                op: CompareOp::Lt,
                right: Operand::Literal {
                    value: Literal::Integer(5),
                },
            },
        },
    };
    let mapping = MappingEntry::Single(Mapping {
        node: "small".into(),
        label: None,
        when: None,
        target: Target::Object {
            object_type: constant("Row"),
            id: col("id"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let bp = blank_blueprint(vec![source, filter], vec![mapping]);
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "small_src",
            [("id", "INTEGER", false), ("val", "INTEGER", false)],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();

    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");

    assert_eq!(
        sink.ocel().get_obs_of_type("Row").count(),
        5,
        "correctness: val < 5"
    );
    assert_eq!(
        report.rows_materialized, 0,
        "a Source -> Filter chain must stream: it should never call \
         GraphExecutor::materialize, so rows_materialized stays zero regardless of table size"
    );
}

/// A [`RowProvider`] that generates `rows` rows on the fly and counts how many it has handed out
/// so far, so a test can ask "how much of the table had been read when the first entity came out
/// the other end?".
#[derive(Debug)]
struct CountingProvider {
    rows: i64,
    emitted: std::rc::Rc<std::cell::Cell<u64>>,
}

impl RowProvider for CountingProvider {
    fn scan(
        &self,
        _table: &str,
        columns: &[&str],
        f: &mut dyn FnMut(&[super::value::Value]) -> std::ops::ControlFlow<()>,
    ) -> Result<(), super::provider::ProviderError> {
        for i in 0..self.rows {
            let vals: Vec<super::value::Value> = columns
                .iter()
                .map(|_| super::value::Value::Integer(i))
                .collect();
            self.emitted.set(self.emitted.get() + 1);
            if f(&vals).is_break() {
                break;
            }
        }
        Ok(())
    }
}

/// Wraps [`SlimOcelSink`] and records the provider's row counter the first time an object
/// reaches the sink. Every other call is passed straight through.
#[derive(Debug)]
struct FirstEntityWitness {
    inner: SlimOcelSink,
    emitted: std::rc::Rc<std::cell::Cell<u64>>,
    rows_read_at_first_object: Option<u64>,
}

impl super::sink::ExtractionSink for FirstEntityWitness {
    fn declare_event_type(
        &mut self,
        name: &str,
        attrs: &[crate::core::event_data::object_centric::OCELTypeAttribute],
    ) -> Result<(), super::sink::SinkError> {
        self.inner.declare_event_type(name, attrs)
    }
    fn declare_object_type(
        &mut self,
        name: &str,
        attrs: &[crate::core::event_data::object_centric::OCELTypeAttribute],
    ) -> Result<(), super::sink::SinkError> {
        self.inner.declare_object_type(name, attrs)
    }
    fn add_event(
        &mut self,
        event_type: &str,
        time: chrono::DateTime<chrono::FixedOffset>,
        id: &str,
        attributes: &[(String, OCELAttributeValue)],
    ) -> Result<super::sink::EventRef, super::sink::SinkError> {
        self.inner.add_event(event_type, time, id, attributes)
    }
    fn add_object(
        &mut self,
        object_type: &str,
        id: &str,
        attributes: &[(
            String,
            chrono::DateTime<chrono::FixedOffset>,
            OCELAttributeValue,
        )],
    ) -> Result<super::sink::ObjectRef, super::sink::SinkError> {
        if self.rows_read_at_first_object.is_none() {
            self.rows_read_at_first_object = Some(self.emitted.get());
        }
        self.inner.add_object(object_type, id, attributes)
    }
    fn add_object_attribute(
        &mut self,
        object: &super::sink::ObjectRef,
        name: &str,
        time: chrono::DateTime<chrono::FixedOffset>,
        value: OCELAttributeValue,
    ) -> Result<(), super::sink::SinkError> {
        self.inner.add_object_attribute(object, name, time, value)
    }
    fn resolve_event(
        &mut self,
        id: &str,
        event_type: Option<&str>,
    ) -> super::sink::Resolution<super::sink::EventRef> {
        self.inner.resolve_event(id, event_type)
    }
    fn resolve_object(
        &mut self,
        id: &str,
        object_type: Option<&str>,
    ) -> super::sink::Resolution<super::sink::ObjectRef> {
        self.inner.resolve_object(id, object_type)
    }
    fn finalize(&mut self) -> Result<super::sink::FinalizeReport, super::sink::SinkError> {
        self.inner.finalize()
    }
    fn add_e2o(
        &mut self,
        event: &super::sink::EventRef,
        object: &super::sink::ObjectRef,
        qualifier: &str,
    ) -> Result<(), super::sink::SinkError> {
        self.inner.add_e2o(event, object, qualifier)
    }
    fn add_o2o(
        &mut self,
        source: &super::sink::ObjectRef,
        target: &super::sink::ObjectRef,
        qualifier: &str,
    ) -> Result<(), super::sink::SinkError> {
        self.inner.add_o2o(source, target, qualifier)
    }
}

/// witnessed from outside the code under test.
///
/// `rows_materialized == 0` (asserted by the test above) is a counter the executor maintains
/// about itself: it says "materialize was never called", which is true and useful but is the
/// implementation grading its own homework. This asserts the observable consequence instead, and
/// nothing in the extractor participates in the measurement: a provider counts the rows it has
/// handed out, a sink records that counter when the first object reaches it, and if the chain
/// streams the first object must arrive after exactly one row. A buffering implementation would
/// have had to pull all ten thousand first.
#[test]
fn case_12_the_first_entity_arrives_after_one_row_not_after_the_whole_table() {
    const ROWS: i64 = 10_000;
    let emitted = std::rc::Rc::new(std::cell::Cell::new(0u64));
    let provider = CountingProvider {
        rows: ROWS,
        emitted: emitted.clone(),
    };
    let mut providers: HashMap<String, &dyn RowProvider> = HashMap::new();
    providers.insert("db".to_string(), &provider);

    let source = source_node("big", "db", "big");
    let filter = Node {
        id: "kept".into(),
        label: None,
        op: NodeOp::Filter {
            input: "big".into(),
            condition: Predicate::Compare {
                left: Operand::Column {
                    column: "val".into(),
                },
                op: CompareOp::Ge,
                right: Operand::Literal {
                    value: Literal::Integer(0),
                },
            },
        },
    };
    let mapping = MappingEntry::Single(Mapping {
        node: "kept".into(),
        label: None,
        when: None,
        target: Target::Object {
            object_type: constant("Row"),
            id: col("id"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let bp = blank_blueprint(vec![source, filter], vec![mapping]);
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new("big", [("id", "INTEGER", false), ("val", "INTEGER", false)]),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let mut sink = FirstEntityWitness {
        inner: SlimOcelSink::new(),
        emitted: emitted.clone(),
        rows_read_at_first_object: None,
    };
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");

    assert_eq!(
        sink.rows_read_at_first_object,
        Some(1),
        "the first object must reach the sink after the provider has handed out exactly one \
         row; a buffering chain would have read all {ROWS} first"
    );
    assert_eq!(
        emitted.get(),
        ROWS as u64,
        "and the whole table is still read"
    );
    assert_eq!(report.rows_materialized, 0);
    assert_eq!(
        sink.inner.ocel().get_obs_of_type("Row").count(),
        ROWS as usize
    );
}

#[test]
fn case_12_join_reports_materialized_rows() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE orders (id INTEGER, amount INTEGER);
             CREATE TABLE meta (id INTEGER, region TEXT);",
        )
        .unwrap();
        for (id, amount) in [(1, 100), (2, 200), (3, 300)] {
            con.execute(
                "INSERT INTO orders (id, amount) VALUES (?1, ?2)",
                params![id, amount],
            )
            .unwrap();
        }
        for (id, region) in [(1, "EU"), (2, "US")] {
            con.execute(
                "INSERT INTO meta (id, region) VALUES (?1, ?2)",
                params![id, region],
            )
            .unwrap();
        }
    }

    let left = source_node("orders", "db", "orders");
    let right = source_node("meta", "db", "meta");
    let join = Node {
        id: "joined".into(),
        label: None,
        op: NodeOp::Join {
            left: "orders".into(),
            right: "meta".into(),
            on: vec![("id".into(), "id".into())],
        },
    };
    let mapping = MappingEntry::Single(Mapping {
        node: "joined".into(),
        label: None,
        when: None,
        target: Target::Object {
            object_type: constant("Order"),
            id: col("right_id"),
            timestamp: None,
            attributes: vec![],
        },
    });
    let bp = blank_blueprint(vec![left, right, join], vec![mapping]);
    let catalog = ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new(
                "orders",
                [("id", "INTEGER", false), ("amount", "INTEGER", false)],
            ),
        )
        .with_table(
            "db",
            TableSchema::new(
                "meta",
                [("id", "INTEGER", false), ("region", "TEXT", false)],
            ),
        );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();

    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");

    assert_eq!(
        sink.ocel().get_obs_of_type("Order").count(),
        2,
        "inner join drops unmatched id=3"
    );
    assert!(
        report.rows_materialized > 0,
        "a Join executed here holds its right input, so rows_materialized should be nonzero \
         (metric is not trivially always zero)"
    );
}

// Code review closure: sink-caveats findings.

/// Fixture for finding 1: an `E2O` whose object type is read from a column, not a constant, so
/// two rows can stage the same object id under two different types. Row 1 names a missing event;
/// row 2 names the same object under a real one. Only row 2's ask is one the eager path ever
/// makes: `run_e2o` gives up on row 1 before it looks at the object endpoint, because the event
/// does not resolve.
#[cfg(feature = "ocel-duckdb")]
fn finding1_fixture() -> (Fixture, Blueprint, ExtractionCatalog) {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE evs (id TEXT, activity TEXT, ts TEXT);
             CREATE TABLE rels (ev TEXT, ob TEXT, obtype TEXT);",
        )
        .unwrap();
        con.execute(
            "INSERT INTO evs VALUES ('e1', 'A', '2020-01-01T00:00:00Z')",
            [],
        )
        .unwrap();
        con.execute("INSERT INTO rels VALUES ('missing', 'x', 'T1')", [])
            .unwrap();
        con.execute("INSERT INTO rels VALUES ('e1', 'x', 'T2')", [])
            .unwrap();
    }
    let events = MappingEntry::Single(Mapping {
        node: "evs".into(),
        label: Some("events".into()),
        when: None,
        target: Target::Event {
            event_type: col("activity"),
            id: Some(col("id")),
            timestamp: TimestampSource::column("ts"),
            attributes: vec![],
            objects: vec![],
        },
    });
    let rels = MappingEntry::Single(Mapping {
        node: "rels".into(),
        label: Some("rels".into()),
        when: None,
        target: Target::E2O {
            event: EventEndpoint {
                id: col("ev"),
                event_type: None,
            },
            object: ObjectEndpoint {
                id: col("ob"),
                object_type: Some(col("obtype")),
                split: None,
            },
            qualifier: Some(constant("uses")),
        },
    });
    let mut bp = blank_blueprint(
        vec![
            source_node("evs", "db", "evs"),
            source_node("rels", "db", "rels"),
        ],
        vec![events, rels],
    );
    bp.on_missing_endpoint = MissingEndpointPolicy::Create;
    let catalog = ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new(
                "evs",
                [
                    ("id", "TEXT", false),
                    ("activity", "TEXT", false),
                    ("ts", "TEXT", false),
                ],
            ),
        )
        .with_table(
            "db",
            TableSchema::new(
                "rels",
                [
                    ("ev", "TEXT", false),
                    ("ob", "TEXT", false),
                    ("obtype", "TEXT", false),
                ],
            ),
        );
    (fx, bp, catalog)
}

/// Finding 1: `DuckDbSink::resolve_deferred`'s `arg_min` used to range over every staged ask for
/// an id, including the ones `finding1_fixture` sets up on purpose, so it picked `T1` (staged
/// first, from the row whose event never resolves) for an id the eager path only ever asks about
/// under `T2`.
///
/// This runs `DuckDbSink` alone, pinning the type `arg_min` picks. The second divergence this
/// fixture used to trigger, the `rels` mapping's own `deduplicated` count differing between the
/// sinks because `resolve_object_endpoint` recorded the ghost ask at row-processing time and
/// nothing at finalize could undo it, is gone with the per-mapping id set, and
/// `both_sinks_agree_on_deduplicated_where_a_ghost_ask_used_to_split_them` runs this
/// fixture through the full agreement helper to keep it that way.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn finding1_arg_min_ignores_asks_the_eager_path_never_makes() {
    use super::duckdb_sink::DuckDbSink;

    let (fx, bp, catalog) = finding1_fixture();
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let db_dir = tempdir().expect("tempdir");
    let db_path = db_dir.path().join("out.duckdb");
    let mut duck = DuckDbSink::new(&db_path).expect("open duckdb sink");
    let report = extract(&bp, &catalog, &providers, &mut duck).expect("extract to duckdb");
    assert_eq!(
        report.finalize.objects_created, 1,
        "only the id reachable through the surviving (e1, x) relation is created"
    );

    let con = duckdb::Connection::open(&db_path).expect("reopen duckdb file");
    let ocel = crate::core::event_data::object_centric::ocel_sql::read_ocel_from_duckdb(&con)
        .expect("read duckdb back");
    let x = ocel.objects.iter().find(|o| o.id == "x").expect("object x");
    assert_eq!(
        x.object_type, "T2",
        "the type must come from the ask reachable through the surviving (e1, x) relation, not \
         the one staged for the row whose event ('missing') never resolves"
    );
}

/// Finding 2: two `Target::Object` mappings on the same node name the same id under two
/// different types (only possible under `IdRendering::Raw`). An eager sink's `resolve_object`
/// answers `Missing` for the second mapping (the id is taken by another type), so its `add_object`
/// failure is unconditionally an `IdTypeCollision`. A deferring sink cannot answer that at
/// `resolve_object` time, so it always takes the "maybe I already own this id" branch, and used to
/// treat the `add_object` rejection that follows as "the id already exists, append to it",
/// writing the second mapping's attribute onto the first mapping's object instead of reporting
/// a collision.
///
/// Runs `DuckDbSink` alone rather than through `assert_both_sinks_agree_on_separate_runs`: its
/// full snapshot comparison also flags that a type declared with zero objects (`Item` here, once
/// its one candidate object collides and is dropped) does not round-trip through `DuckDB`, which
/// derives `object_types()` from `DISTINCT ocel_type` over the `objects` table rather than a
/// persisted declaration. That is a separate, pre-existing limitation this fixture happens to
/// trigger, not the content-merge bug this test pins.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn finding2_a_deferring_sink_does_not_merge_a_type_collision() {
    use super::duckdb_sink::DuckDbSink;

    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE things (id TEXT, kind TEXT, note TEXT);")
            .unwrap();
        con.execute("INSERT INTO things VALUES ('1', 'order', NULL)", [])
            .unwrap();
        con.execute("INSERT INTO things VALUES ('1', 'item', 'n')", [])
            .unwrap();
    }
    let of_kind = |kind: &str, object_type: &str, attributes: Vec<AttributeMapping>| {
        MappingEntry::Single(Mapping {
            node: "things".into(),
            label: Some(object_type.to_string()),
            when: Some(Predicate::Compare {
                left: Operand::Column {
                    column: "kind".into(),
                },
                op: CompareOp::Eq,
                right: Operand::Literal {
                    value: Literal::Text(kind.into()),
                },
            }),
            target: Target::Object {
                object_type: constant(object_type),
                id: col("id"),
                timestamp: None,
                attributes,
            },
        })
    };
    let bp = blank_blueprint(
        vec![source_node("things", "db", "things")],
        vec![
            of_kind("order", "Order", vec![]),
            of_kind(
                "item",
                "Item",
                vec![AttributeMapping {
                    source_column: "note".into(),
                    name: "note".into(),
                    value_type: None,
                }],
            ),
        ],
    );
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "things",
            [
                ("id", "TEXT", false),
                ("kind", "TEXT", false),
                ("note", "TEXT", true),
            ],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let db_dir = tempdir().expect("tempdir");
    let db_path = db_dir.path().join("out.duckdb");
    let mut duck = DuckDbSink::new(&db_path).expect("open duckdb sink");
    let report = extract(&bp, &catalog, &providers, &mut duck).expect("extract to duckdb");

    let item = report
        .per_mapping
        .iter()
        .find(|m| m.mapping.label.as_deref() == Some("Item"))
        .expect("Item stats");
    assert_eq!(
        item.dropped.get(&DropReason::IdTypeCollision),
        Some(&1),
        "a deferring sink must report the same collision an eager one does: {:?}",
        item.dropped
    );

    let con = duckdb::Connection::open(&db_path).expect("reopen duckdb file");
    let ocel = crate::core::event_data::object_centric::ocel_sql::read_ocel_from_duckdb(&con)
        .expect("read duckdb back");
    let order = ocel.objects.iter().find(|o| o.id == "1").expect("object 1");
    assert_eq!(order.object_type, "Order");
    assert!(
        order.attributes.is_empty(),
        "the colliding Item mapping's 'note' attribute must not be merged onto the surviving \
         Order object: {:?}",
        order.attributes
    );
}

/// Finding 8: `Blueprint::from_flat_event_table`'s case-object mapping now creates the case
/// objects (the objects pass runs before the events pass), rather than merely finding them
/// already created by the event mapping's inline `Create` reference. Pins the counter the
/// three-phase split corrected: before it, this mapping's `entities_emitted` was 0, because the
/// event mapping's own inline reference (also `Create`) always ran first and created the object,
/// leaving the case mapping nothing to do but append.
#[test]
fn finding8_case_object_mapping_creates_the_case_objects_not_the_event_mapping() {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE events (case_id TEXT, activity TEXT, ts TEXT, region TEXT);",
        )
        .unwrap();
        con.execute(
            "INSERT INTO events VALUES ('c1', 'A', '2020-01-01T00:00:00Z', 'east')",
            [],
        )
        .unwrap();
        con.execute(
            "INSERT INTO events VALUES ('c1', 'B', '2020-01-01T01:00:00Z', 'east')",
            [],
        )
        .unwrap();
        con.execute(
            "INSERT INTO events VALUES ('c2', 'A', '2020-01-01T02:00:00Z', 'west')",
            [],
        )
        .unwrap();
    }
    let bp = Blueprint::from_flat_event_table(FlatEventTable {
        source_id: "db".into(),
        table: "events".into(),
        case_id: "case_id".into(),
        activity: "activity".into(),
        timestamp: "ts".into(),
        case_object_type: "Case".into(),
        case_attributes: vec![AttributeMapping {
            source_column: "region".into(),
            name: "region".into(),
            value_type: None,
        }],
        event_attributes: vec![],
    });
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "events",
            [
                ("case_id", "TEXT", false),
                ("activity", "TEXT", false),
                ("ts", "TEXT", false),
                ("region", "TEXT", false),
            ],
        ),
    );
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");

    let cases = report
        .per_mapping
        .iter()
        .find(|m| m.mapping.label.as_deref() == Some("cases"))
        .expect("cases stats");
    assert_eq!(
        cases.entities_emitted, 2,
        "the objects pass creates the 2 distinct cases; the event mapping only finds them"
    );
    assert_eq!(sink.ocel().get_obs_of_type("Case").count(), 2);
}

// `compile` refuses an invalid blueprint, exactly as `extract` does.

/// A one-node, one-mapping blueprint that compiles clean, plus the catalog it needs. The base
/// every case below perturbs into something `validate` rejects.
fn compilable_blueprint() -> (Blueprint, ExtractionCatalog) {
    let bp = blank_blueprint(
        vec![source_node("docs", "db", "docs")],
        vec![MappingEntry::Single(Mapping {
            node: "docs".into(),
            label: Some("orders".into()),
            when: None,
            target: Target::Object {
                object_type: constant("Order"),
                id: col("id"),
                timestamp: None,
                attributes: vec![],
            },
        })],
    );
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new("docs", [("id", "TEXT", false), ("kind", "TEXT", false)]),
    );
    (bp, catalog)
}

fn compile_default(bp: &Blueprint, catalog: &ExtractionCatalog) -> super::compile::CompiledOcel {
    super::compile::compile(
        bp,
        catalog,
        super::compile::SqlDialect::default(),
        super::compile::EmissionShape::PerType,
    )
}

/// `extraction_compile`'s binding docs promise it "refuses to run an invalid blueprint", and
/// `extract` does exactly that. `compile` did not: the bindings deserialize a `Blueprint` with
/// plain serde rather than `Blueprint::from_json`, so the version check lives only in
/// `validate`, and a `{"version": 2, ...}` blueprint whose new constructs are additive fields
/// was silently compiled under a v1 reading with zero entries in `errors`.
#[test]
fn i1_compile_refuses_a_blueprint_from_a_future_model_version() {
    let (mut bp, catalog) = compilable_blueprint();
    bp.version = super::MODEL_VERSION + 1;
    assert!(!validate(&bp, &catalog).is_empty());

    let compiled = compile_default(&bp, &catalog);
    assert!(
        !compiled.errors().is_empty(),
        "a future-version blueprint must be reported, not compiled under a v1 reading"
    );
    assert!(
        compiled.relations().is_empty(),
        "nothing may be emitted for a blueprint this build cannot read: {:?}",
        compiled.relations()
    );
}

/// Duplicate node ids used to read differently in two places: `Blueprint::node` takes the first
/// match, `full_node_schemas` lets the last win. `node_sql_inner` then took the op from one and the
/// columns from another and emitted wrong SQL rather than none. `validate` rejects the duplicate;
/// `compile` has to act on that.
#[test]
fn i1_compile_refuses_duplicate_node_ids() {
    let (mut bp, catalog) = compilable_blueprint();
    bp.nodes.push(Node {
        id: "docs".into(),
        label: None,
        op: NodeOp::Source {
            source_id: "db".into(),
            table: "other".into(),
        },
    });
    assert!(!validate(&bp, &catalog).is_empty());

    let compiled = compile_default(&bp, &catalog);
    assert!(
        !compiled.errors().is_empty(),
        "a duplicate node id must be reported rather than compiled from two different nodes"
    );
    assert!(
        compiled.relations().is_empty(),
        "{:?}",
        compiled.relations()
    );
}

/// A blueprint naming a table the catalog does not have degrades to a complete but permanently
/// empty view set. A caller that does not read `errors()` gets a silent empty log; the failure
/// has to be in `errors`.
#[test]
fn i1_compile_refuses_an_unknown_table() {
    let (bp, _) = compilable_blueprint();
    let catalog = ExtractionCatalog::new();
    assert!(!validate(&bp, &catalog).is_empty());

    let compiled = compile_default(&bp, &catalog);
    assert!(
        !compiled.errors().is_empty(),
        "an unknown table must be reported, not compiled into an empty view set"
    );
    assert!(
        compiled.relations().is_empty(),
        "{:?}",
        compiled.relations()
    );
}

/// The complement: a blueprint that does validate still compiles exactly as before, so adding
/// the precondition check did not turn a working compile into a rejection.
#[test]
fn i1_compile_still_emits_views_for_a_valid_blueprint() {
    let (bp, catalog) = compilable_blueprint();
    assert_eq!(validate(&bp, &catalog), vec![]);

    let compiled = compile_default(&bp, &catalog);
    assert!(compiled.errors().is_empty(), "{:?}", compiled.errors());
    assert!(!compiled.relations().is_empty());
}

// the two sinks must agree on declared types and attribute values, not only on entities.

/// A declared type with zero matching rows is part of the log: `extract` declares every
/// statically-named type up front so the declared type set is a function of the blueprint alone,
/// not of which rows happen to match. `case_2`'s `CreditNote` is such a type, and the fixture used
/// to run through `SlimOcelSink` only.
///
/// This is a known divergence, not closable from this module. The consolidated `DuckDB` layout has
/// nowhere to record a declared object type: `declare_object_type` has no table to write to, and
/// `read_ocel_from_duckdb` rebuilds the type list from `SELECT DISTINCT ocel_type FROM objects`.
/// A type with no rows therefore cannot survive the round trip. Closing it needs an
/// `object_attr_meta(object_type, attr_name, attr_type)` table in
/// `ocel_sql::duckdb::schema::tables` and a declared-types union in that module's reader, i.e. a
/// change to the crate's on-disk OCEL 2.0 `DuckDB` format, which the SQL compiler's
/// `EmissionShape::Consolidated` output would have to emit as well.
///
/// This test pins the divergence exactly: everything except the declared type set agrees, and
/// the only difference is the zero-entity type. It fails the moment either half changes.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn i3_a_declared_type_with_zero_entities_is_lost_by_the_duckdb_layout() {
    let (fx, bp, catalog) = case2_fixture_and_blueprint();
    assert_eq!(validate(&bp, &catalog), vec![]);
    let (slim, duck) = snapshot_both_sinks(&fx, &bp, &catalog);

    assert_eq!(slim.events, duck.events, "events agree");
    assert_eq!(slim.objects, duck.objects, "objects agree");
    assert_eq!(slim.event_types, duck.event_types);
    assert_eq!(
        slim.object_types.keys().collect::<Vec<_>>(),
        vec!["Bill", "CreditNote", "Invoice"],
        "the extractor declares every statically-named type, matched or not"
    );
    assert_eq!(
        duck.object_types.keys().collect::<Vec<_>>(),
        vec!["Bill", "Invoice"],
        "the DuckDB reader rebuilds types from the rows that exist, so CreditNote is lost"
    );
}

/// One attribute name declared under two different types by two different event types.
/// The extractor reconciles by `(kind, type_name, attr_name)`, so `A.n` stays an integer.
///
/// Known divergence, not closable from this module. The consolidated layout stores event
/// attributes as *one wide column per attribute name*, shared by every event type, so `A.n` and
/// `B.n` are one `VARCHAR` column and the integer is read back as `String("5")`. The declared
/// types themselves round-trip correctly (`event_attr_meta` is keyed per type); only the value
/// does not. Closing it needs `read_ocel_from_duckdb` to narrow each wide-column value back to
/// its `(event_type, attr_name)` declared type, again a change to the shared reader.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn i4_an_attribute_declared_under_two_event_types_is_widened_by_the_duckdb_layout() {
    let (fx, bp, catalog) = retyped_event_attribute_fixture();
    assert_eq!(validate(&bp, &catalog), vec![]);
    let (slim, duck) = snapshot_both_sinks(&fx, &bp, &catalog);

    assert_eq!(
        slim.event_types, duck.event_types,
        "the per-type declarations do round-trip"
    );
    assert_eq!(slim.objects, duck.objects);
    assert_eq!(
        slim.events["e1"].attributes,
        vec![("n".to_string(), OCELAttributeValue::Integer(5))],
        "the extractor keeps A.n an integer"
    );
    assert_eq!(
        duck.events["e1"].attributes,
        vec![("n".to_string(), OCELAttributeValue::String("5".into()))],
        "the shared wide column for 'n' is VARCHAR, because B.n is a string"
    );
    assert_eq!(
        slim.events["e2"].attributes, duck.events["e2"].attributes,
        "the string-typed event type is unaffected"
    );
}

/// Both sinks' logs, for a test that needs to inspect a specific divergence rather than assert
/// wholesale equality.
#[cfg(feature = "ocel-duckdb")]
fn snapshot_both_sinks(
    fx: &Fixture,
    bp: &Blueprint,
    catalog: &ExtractionCatalog,
) -> (
    super::differential::OcelSnapshot,
    super::differential::OcelSnapshot,
) {
    use super::differential::{run_against_both, snapshot};
    use super::duckdb_sink::DuckDbSink;

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut slim_sink = SlimOcelSink::new();
    let db_dir = tempdir().expect("tempdir");
    let db_path = db_dir.path().join("out.duckdb");
    let mut duck_sink = DuckDbSink::new(&db_path).expect("open duckdb sink");
    run_against_both(&mut slim_sink, &mut duck_sink, |sink| {
        extract(bp, catalog, &providers, sink).expect("extract")
    });
    let slim = snapshot(slim_sink.ocel());
    let con = duckdb::Connection::open(&db_path).expect("reopen duckdb file");
    let duck_ocel = crate::core::event_data::object_centric::ocel_sql::read_ocel_from_duckdb(&con)
        .expect("read duckdb back");
    (slim, snapshot(&duck_ocel))
}

/// The same type declared by two mappings, so `declare_event_type` arrives twice for it.
/// `SlimOcelSink` keeps the last declaration; the `DuckDB` reader keeps the first row per
/// `(type, name)`, from a query with no `ORDER BY`.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn i5_both_sinks_agree_when_one_type_is_declared_by_two_mappings() {
    let (fx, bp, catalog) = twice_declared_type_fixture();
    assert_eq!(validate(&bp, &catalog), vec![]);
    assert_both_sinks_agree(&fx, &bp, &catalog);
}

/// A `NULL` cell written as an object attribute. `to_sql_value` renders `Null` as `("",
/// "string")`, which `from_sql_value` reads back as `String("")` where the eager sink holds
/// `Null`; `DuckDbSink` now stores it under the `"null"` type string instead, so the value
/// round-trips exactly.
///
/// The declared-type half remains a known divergence. `read_ocel_from_duckdb` derives an
/// object type's declared attributes from the `value_type` of the change rows it observes, not
/// from any declaration, so a name whose values span two types yields two entries for it. Same
/// shared-reader change.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn i6_a_null_object_attribute_round_trips_as_null() {
    let (fx, bp, catalog) = null_object_attribute_fixture();
    assert_eq!(validate(&bp, &catalog), vec![]);
    let (slim, duck) = snapshot_both_sinks(&fx, &bp, &catalog);

    assert_eq!(
        slim.objects, duck.objects,
        "every object attribute value, Null included, must survive the round trip"
    );
    assert_eq!(
        slim.objects["o1"].attributes[0].2,
        OCELAttributeValue::Null,
        "the NULL cell is a Null attribute, not String(\"\")"
    );
    assert_eq!(slim.events, duck.events);
    assert_eq!(
        slim.object_types["Order"],
        vec![("note".to_string(), "string".to_string())],
        "the extractor declares note once, as the string the blueprint asked for"
    );
    assert_eq!(
        duck.object_types["Order"],
        vec![
            ("note".to_string(), "null".to_string()),
            ("note".to_string(), "string".to_string())
        ],
        "the reader derives object attribute types from observed values, so a name whose \
         values span two types yields two entries"
    );
}

/// `case_2`'s fixture and blueprint, factored out so the differential test above can reuse the
/// exact shape `case_2_discriminated_table_declares_zero_match_type` pins.
#[cfg(feature = "ocel-duckdb")]
fn case2_fixture_and_blueprint() -> (Fixture, Blueprint, ExtractionCatalog) {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE docs (id INTEGER, kind TEXT);")
            .unwrap();
        for (id, kind) in [(1, "invoice"), (2, "invoice"), (3, "bill")] {
            con.execute(
                "INSERT INTO docs (id, kind) VALUES (?1, ?2)",
                params![id, kind],
            )
            .unwrap();
        }
    }
    let object_mapping = |label: &str, kind: &str, object_type: &str| {
        MappingEntry::Single(Mapping {
            node: "docs".into(),
            label: Some(label.into()),
            when: Some(Predicate::Compare {
                left: Operand::Column {
                    column: "kind".into(),
                },
                op: CompareOp::Eq,
                right: Operand::Literal {
                    value: Literal::Text(kind.into()),
                },
            }),
            target: Target::Object {
                object_type: constant(object_type),
                id: col("id"),
                timestamp: None,
                attributes: vec![],
            },
        })
    };
    let bp = blank_blueprint(
        vec![source_node("docs", "db", "docs")],
        vec![
            object_mapping("invoices", "invoice", "Invoice"),
            object_mapping("bills", "bill", "Bill"),
            object_mapping("credit_notes", "credit_note", "CreditNote"),
        ],
    );
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new("docs", [("id", "INTEGER", false), ("kind", "TEXT", false)]),
    );
    (fx, bp, catalog)
}

/// Two event types, `A` and `B`, both declaring an attribute `n`, `A` as an integer and `B` as a
/// string, with one `A` row carrying an integer value for it.
#[cfg(feature = "ocel-duckdb")]
fn retyped_event_attribute_fixture() -> (Fixture, Blueprint, ExtractionCatalog) {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE evs (id TEXT, kind TEXT, n INTEGER, s TEXT, ts TEXT);")
            .unwrap();
        con.execute(
            "INSERT INTO evs VALUES ('e1', 'A', 5, 'five', '2020-01-01T00:00:00Z')",
            [],
        )
        .unwrap();
        con.execute(
            "INSERT INTO evs VALUES ('e2', 'B', 7, 'seven', '2020-01-02T00:00:00Z')",
            [],
        )
        .unwrap();
    }
    let event_mapping = |label: &str, kind: &str, column: &str, value_type: OCELAttributeType| {
        MappingEntry::Single(Mapping {
            node: "evs".into(),
            label: Some(label.into()),
            when: Some(Predicate::Compare {
                left: Operand::Column {
                    column: "kind".into(),
                },
                op: CompareOp::Eq,
                right: Operand::Literal {
                    value: Literal::Text(kind.into()),
                },
            }),
            target: Target::Event {
                event_type: constant(kind),
                id: Some(col("id")),
                timestamp: TimestampSource::column("ts"),
                attributes: vec![AttributeMapping {
                    source_column: column.into(),
                    name: "n".into(),
                    value_type: Some(value_type),
                }],
                objects: vec![],
            },
        })
    };
    let bp = blank_blueprint(
        vec![source_node("evs", "db", "evs")],
        vec![
            event_mapping("a_events", "A", "n", OCELAttributeType::Integer),
            event_mapping("b_events", "B", "s", OCELAttributeType::String),
        ],
    );
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "evs",
            [
                ("id", "TEXT", false),
                ("kind", "TEXT", false),
                ("n", "INTEGER", false),
                ("s", "TEXT", false),
                ("ts", "TEXT", false),
            ],
        ),
    );
    (fx, bp, catalog)
}

/// One object type `Order` declared by two mappings on two nodes, each naming attribute `amount`,
/// so `declare_object_type` arrives twice for it, as it does for any type several mappings
/// produce.
#[cfg(feature = "ocel-duckdb")]
fn twice_declared_type_fixture() -> (Fixture, Blueprint, ExtractionCatalog) {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE a (id TEXT, amount INTEGER);
             CREATE TABLE b (id TEXT, amount INTEGER);",
        )
        .unwrap();
        con.execute("INSERT INTO a VALUES ('o1', 10)", []).unwrap();
        con.execute("INSERT INTO b VALUES ('o2', 20)", []).unwrap();
    }
    let object_mapping = |label: &str, node: &str, value_type: OCELAttributeType| {
        MappingEntry::Single(Mapping {
            node: node.into(),
            label: Some(label.into()),
            when: None,
            target: Target::Object {
                object_type: constant("Order"),
                id: col("id"),
                timestamp: None,
                attributes: vec![AttributeMapping {
                    source_column: "amount".into(),
                    name: "amount".into(),
                    value_type: Some(value_type),
                }],
            },
        })
    };
    let bp = blank_blueprint(
        vec![source_node("a", "db", "a"), source_node("b", "db", "b")],
        vec![
            object_mapping("from_a", "a", OCELAttributeType::Integer),
            object_mapping("from_b", "b", OCELAttributeType::Integer),
        ],
    );
    let catalog = ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new("a", [("id", "TEXT", false), ("amount", "INTEGER", false)]),
        )
        .with_table(
            "db",
            TableSchema::new("b", [("id", "TEXT", false), ("amount", "INTEGER", false)]),
        );
    (fx, bp, catalog)
}

/// One object with a `NULL` attribute cell.
#[cfg(feature = "ocel-duckdb")]
fn null_object_attribute_fixture() -> (Fixture, Blueprint, ExtractionCatalog) {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE obs (id TEXT, note TEXT);")
            .unwrap();
        con.execute("INSERT INTO obs VALUES ('o1', NULL)", [])
            .unwrap();
        con.execute("INSERT INTO obs VALUES ('o2', 'hello')", [])
            .unwrap();
    }
    let bp = blank_blueprint(
        vec![source_node("obs", "db", "obs")],
        vec![MappingEntry::Single(Mapping {
            node: "obs".into(),
            label: Some("orders".into()),
            when: None,
            target: Target::Object {
                object_type: constant("Order"),
                id: col("id"),
                timestamp: None,
                attributes: vec![AttributeMapping {
                    source_column: "note".into(),
                    name: "note".into(),
                    value_type: Some(OCELAttributeType::String),
                }],
            },
        })],
    );
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new("obs", [("id", "TEXT", true), ("note", "TEXT", true)]),
    );
    (fx, bp, catalog)
}

// `entities_emitted` counts hand-offs to the sink, not survivors.

/// The counter divergence `assert_both_sinks_agree_on_separate_runs`'s snapshot cannot see: both
/// sinks write the same log, but an eager sink refuses a dangling relation at the call site while
/// a deferring one writes it, counts it, and deletes it at finalize.
///
/// Pins the exact numbers documented on [`MappingStats::entities_emitted`], and the identity that
/// makes them reconcilable: `emitted - unresolved_endpoints` is the same on both sides.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn i7_entities_emitted_counts_hand_offs_not_survivors_for_a_deferring_sink() {
    let (fx, bp, catalog) = dangling_e2o_fixture();
    assert_eq!(validate(&bp, &catalog), vec![]);

    let (slim_report, duck_report) = assert_both_sinks_agree_on_separate_runs(&fx, &bp, &catalog);

    let emitted = |r: &super::report::ExtractionReport| {
        r.per_mapping
            .iter()
            .find(|m| m.mapping.label.as_deref() == Some("rels"))
            .expect("rels stats")
            .clone()
    };
    let eager = emitted(&slim_report);
    let deferring = emitted(&duck_report);

    assert_eq!(
        eager.entities_emitted, 0,
        "an eager sink refuses the relation at the call site"
    );
    assert_eq!(
        eager.dropped.get(&DropReason::UnresolvedEndpoint),
        Some(&1),
        "and reports the loss against the mapping that caused it"
    );
    assert_eq!(slim_report.finalize.unresolved_endpoints, 0);

    assert_eq!(
        deferring.entities_emitted, 1,
        "a deferring sink cannot refuse, so it counts the hand-off"
    );
    assert_eq!(
        deferring.dropped.get(&DropReason::UnresolvedEndpoint),
        None,
        "the loss is not attributable to a mapping once it is settled at finalize"
    );
    assert_eq!(
        duck_report.finalize.unresolved_endpoints, 1,
        "it is reported in bulk here instead"
    );

    // The identity that makes the two reconcilable, and the reason the divergence is bounded.
    let total = |r: &super::report::ExtractionReport| {
        r.per_mapping
            .iter()
            .map(|m| m.entities_emitted)
            .sum::<u64>()
            - r.finalize.unresolved_endpoints
    };
    assert_eq!(
        total(&slim_report),
        total(&duck_report),
        "emitted minus unresolved must agree across sinks"
    );
}

/// One real event, one real object, and one `E2O` row naming an event that does not exist.
#[cfg(feature = "ocel-duckdb")]
fn dangling_e2o_fixture() -> (Fixture, Blueprint, ExtractionCatalog) {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch(
            "CREATE TABLE evs (id TEXT, ts TEXT);
             CREATE TABLE obs (id TEXT);
             CREATE TABLE rels (ev TEXT, ob TEXT);",
        )
        .unwrap();
        con.execute("INSERT INTO evs VALUES ('e1', '2020-01-01T00:00:00Z')", [])
            .unwrap();
        con.execute("INSERT INTO obs VALUES ('o1')", []).unwrap();
        con.execute("INSERT INTO rels VALUES ('no-such-event', 'o1')", [])
            .unwrap();
    }
    let bp = blank_blueprint(
        vec![
            source_node("evs", "db", "evs"),
            source_node("obs", "db", "obs"),
            source_node("rels", "db", "rels"),
        ],
        vec![
            MappingEntry::Single(Mapping {
                node: "evs".into(),
                label: Some("events".into()),
                when: None,
                target: Target::Event {
                    event_type: constant("Pay"),
                    id: Some(col("id")),
                    timestamp: TimestampSource::column("ts"),
                    attributes: vec![],
                    objects: vec![],
                },
            }),
            MappingEntry::Single(Mapping {
                node: "obs".into(),
                label: Some("objects".into()),
                when: None,
                target: Target::Object {
                    object_type: constant("Order"),
                    id: col("id"),
                    timestamp: None,
                    attributes: vec![],
                },
            }),
            MappingEntry::Single(Mapping {
                node: "rels".into(),
                label: Some("rels".into()),
                when: None,
                target: Target::E2O {
                    event: EventEndpoint {
                        id: col("ev"),
                        event_type: None,
                    },
                    object: ObjectEndpoint {
                        id: col("ob"),
                        object_type: Some(constant("Order")),
                        split: None,
                    },
                    qualifier: None,
                },
            }),
        ],
    );
    let catalog = ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new("evs", [("id", "TEXT", false), ("ts", "TEXT", false)]),
        )
        .with_table("db", TableSchema::new("obs", [("id", "TEXT", false)]))
        .with_table(
            "db",
            TableSchema::new("rels", [("ev", "TEXT", false), ("ob", "TEXT", false)]),
        );
    (fx, bp, catalog)
}

// The extractor holds no per-mapping id set: `deduplicated` is answered by the sink, and so is
// the first-wins rule for a repeated `(id, attribute, time)`.

/// A [`RowProvider`] generating `rows` rows of a single `id` column, cycling through `distinct`
/// ids. Holds nothing itself.
#[derive(Debug)]
struct GeneratedIds {
    rows: usize,
    distinct: usize,
}

impl RowProvider for GeneratedIds {
    fn scan(
        &self,
        table: &str,
        columns: &[&str],
        f: &mut dyn FnMut(&[super::value::Value]) -> std::ops::ControlFlow<()>,
    ) -> Result<(), super::provider::ProviderError> {
        if table != "gen" {
            return Err(super::provider::ProviderError::UnknownTable {
                table: table.to_string(),
            });
        }
        for c in columns {
            if *c != "id" {
                return Err(super::provider::ProviderError::UnknownColumn {
                    table: table.to_string(),
                    column: (*c).to_string(),
                });
            }
        }
        for i in 0..self.rows {
            let cell = super::value::Value::Text(format!("id-{}", i % self.distinct));
            let row: Vec<super::value::Value> =
                columns.iter().map(|_| cell.clone()).collect::<Vec<_>>();
            if f(&row).is_break() {
                return Ok(());
            }
        }
        Ok(())
    }
}

/// One object id on 100k rows extracts cleanly, and every repeat after the first is counted as a
/// deduplication rather than lost.
#[test]
fn one_object_id_on_a_hundred_thousand_rows_extracts_cleanly() {
    let bp = blank_blueprint(
        vec![source_node("gen", "db", "gen")],
        vec![MappingEntry::Single(Mapping {
            node: "gen".into(),
            label: Some("obs".into()),
            when: None,
            target: Target::Object {
                object_type: constant("Order"),
                id: col("id"),
                timestamp: None,
                attributes: vec![],
            },
        })],
    );
    let catalog =
        ExtractionCatalog::new().with_table("db", TableSchema::new("gen", [("id", "TEXT", false)]));
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = GeneratedIds {
        rows: 100_000,
        distinct: 1,
    };
    let mut providers: HashMap<String, &dyn RowProvider> = HashMap::new();
    providers.insert("db".to_string(), &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");

    assert_eq!(sink.ocel().get_obs_of_type("Order").count(), 1);
    let stats = &report.per_mapping[0];
    assert_eq!(stats.entities_emitted, 1);
    assert_eq!(
        stats.deduplicated, 99_999,
        "every row after the first named an object the sink already had"
    );
    assert!(stats.dropped.is_empty(), "{:?}", stats.dropped);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}

/// Two static object mappings on one row, writing the same `(id, attribute, time)` with the same
/// value. First-wins now spans every mapping, because it lives in the sink rather than in one
/// mapping's own bookkeeping, so this stores one value, not two, and is not an error.
fn repeated_attribute_fixture(second_value: &str) -> (Fixture, Blueprint, ExtractionCatalog) {
    let fx = Fixture::new();
    {
        let con = fx.build();
        con.execute_batch("CREATE TABLE rows_ (id TEXT, a TEXT, b TEXT);")
            .unwrap();
        con.execute(
            "INSERT INTO rows_ VALUES ('x', 'first', ?1)",
            params![second_value],
        )
        .unwrap();
    }
    let mapping = |label: &str, column: &str| {
        MappingEntry::Single(Mapping {
            node: "rows_".into(),
            label: Some(label.to_string()),
            when: None,
            target: Target::Object {
                object_type: constant("Order"),
                id: col("id"),
                timestamp: None,
                attributes: vec![AttributeMapping {
                    source_column: column.to_string(),
                    name: "note".into(),
                    value_type: None,
                }],
            },
        })
    };
    let bp = blank_blueprint(
        vec![source_node("rows_", "db", "rows_")],
        vec![mapping("first", "a"), mapping("second", "b")],
    );
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "rows_",
            [
                ("id", "TEXT", false),
                ("a", "TEXT", false),
                ("b", "TEXT", false),
            ],
        ),
    );
    (fx, bp, catalog)
}

/// a repeated `(id, attribute, time)` carrying an identical value is not an error,
/// and stores one value.
#[test]
fn a_repeated_attribute_with_the_same_value_stores_one_value() {
    let (fx, bp, catalog) = repeated_attribute_fixture("first");
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert!(report.errors.is_empty(), "{:?}", report.errors);

    let ocel = sink.ocel();
    let obj = ocel.get_ob_by_id("x").expect("object x");
    let note = obj.get_attribute_value("note", ocel).expect("note history");
    assert_eq!(note.len(), 1, "one value per (id, attribute, time)");
    assert_eq!(note[0].1, OCELAttributeValue::String("first".into()));
}

/// the same `(id, attribute, time)` written twice with different values. First
/// wins, and it is not an error.
///
/// Which write is first is scan order, not a promise: nothing issues an `ORDER BY`, and mapping
/// execution order is `(phase, first-seen node position, mapping index)`. The rule is that exactly
/// one value survives and that a repeat costs neither an error nor a second entry, not that a
/// particular one of two conflicting values is chosen. Here both mappings sit on one node, which
/// is the one shape mapping order does decide (see `extract`'s own docs).
#[test]
fn a_repeated_attribute_with_a_different_value_is_first_wins() {
    let (fx, bp, catalog) = repeated_attribute_fixture("second");
    assert_eq!(validate(&bp, &catalog), vec![]);

    let provider = fx.provider();
    let providers = providers_of("db", &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert!(report.errors.is_empty(), "{:?}", report.errors);

    let ocel = sink.ocel();
    let obj = ocel.get_ob_by_id("x").expect("object x");
    let note = obj.get_attribute_value("note", ocel).expect("note history");
    assert_eq!(note.len(), 1, "one value per (id, attribute, time)");
    assert_eq!(
        note[0].1,
        OCELAttributeValue::String("first".into()),
        "the first write wins"
    );
}

/// The first-wins rule has to hold in both sinks or the same blueprint produces two different
/// logs: `SlimOcelSink` scans the attribute's history, `DuckDbSink` lets a unique index reject
/// the insert, and only a differential run proves the two agree. Both the identical-value and the
/// conflicting-value fixtures, since they take different paths through `DuckDB`'s constraint
/// handling only in what the discarded row held.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn both_sinks_keep_exactly_one_value_per_id_attribute_time() {
    for second_value in ["first", "second"] {
        let (fx, bp, catalog) = repeated_attribute_fixture(second_value);
        assert_eq!(validate(&bp, &catalog), vec![]);
        assert_both_sinks_agree_on_separate_runs(&fx, &bp, &catalog);
    }
}

/// The two sinks must agree on `deduplicated` on the
/// fixture that used to make them disagree.
///
/// `finding1_fixture` interleaves a repeated object id with a row whose event never resolves. A
/// deferring sink cannot fail `resolve_event` at the call site, so it reached
/// `resolve_object_endpoint` on a row the eager path abandons earlier, and the per-mapping id set
/// that function touched had already recorded the ghost ask by the time `finalize` could tell the
/// relation would not survive. Nothing at finalize could undo it, so the `rels` mapping's
/// `deduplicated` differed between the sinks. With no such set, endpoint resolution counts nothing
/// on either side and the divergence is closed by construction.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn both_sinks_agree_on_deduplicated_where_a_ghost_ask_used_to_split_them() {
    let (fx, bp, catalog) = finding1_fixture();
    assert_eq!(validate(&bp, &catalog), vec![]);

    let (slim_report, _) = assert_both_sinks_agree_on_separate_runs(&fx, &bp, &catalog);
    let rels = slim_report
        .per_mapping
        .iter()
        .find(|m| m.mapping.label.as_deref() == Some("rels"))
        .expect("rels stats");
    assert_eq!(
        rels.deduplicated, 0,
        "a relation mapping deduplicates nothing: it emits one relation per row"
    );
}

/// The other half: a *cross-mapping* repeat, which the old per-mapping set could not see at all.
/// Two object mappings name one id; the second one finds an object the sink already has, which is
/// now a deduplication under both sinks.
#[cfg(feature = "ocel-duckdb")]
#[test]
fn a_second_mapping_finding_the_first_s_object_is_a_deduplication() {
    let (fx, bp, catalog) = dynamic_type_fixture_and_blueprint();
    assert_eq!(validate(&bp, &catalog), vec![]);

    let (slim_report, duck_report) = assert_both_sinks_agree_on_separate_runs(&fx, &bp, &catalog);
    for report in [&slim_report, &duck_report] {
        let with_attr = report
            .per_mapping
            .iter()
            .find(|m| m.mapping.label.as_deref() == Some("with_attr"))
            .expect("stats");
        assert_eq!(
            with_attr.deduplicated, 1,
            "the sink already had this id, which is what `deduplicated` now counts"
        );
    }
}
