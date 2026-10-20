//! Unit tests for the dialect layer and the assembly path.
//!
//! Nothing here executes SQL: these check the shape of what is emitted and which mappings are
//! refused. That the SQL agrees with the extractor is checked in `compile::differential`.
#![cfg(test)]

use std::collections::{BTreeMap, BTreeSet};

use super::{compile, EmissionShape, ProbeKind, RejectReason, SqlDialect};
use crate::core::event_data::object_centric::extraction::blueprint::{
    Blueprint, DuplicateObjectPolicy, EventEndpoint, IdRendering, Mapping, MappingEntry,
    MissingEndpointPolicy, Node, NodeOp, ObjectEndpoint, Target,
};
use crate::core::event_data::object_centric::extraction::catalog::{
    Catalog, ExtractionCatalog, TableSchema,
};
use crate::core::event_data::object_centric::extraction::expr::{
    AttributeMapping, TimestampSource, ValueExpression,
};

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

fn source(id: &str, table: &str) -> Node {
    Node {
        id: id.to_string(),
        label: None,
        op: NodeOp::Source {
            source_id: "db".to_string(),
            table: table.to_string(),
        },
    }
}

fn blueprint(nodes: Vec<Node>, mappings: Vec<MappingEntry>) -> Blueprint {
    Blueprint {
        version: crate::core::event_data::object_centric::extraction::MODEL_VERSION,
        id_rendering: IdRendering::Raw,
        nodes,
        mappings,
        on_missing_endpoint: MissingEndpointPolicy::Drop,
        on_duplicate_object: DuplicateObjectPolicy::FirstWins,
    }
}

fn orders_catalog() -> ExtractionCatalog {
    ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "orders",
            [
                ("id", "INTEGER", false),
                ("kind", "TEXT", true),
                ("at", "TIMESTAMP", true),
            ],
        ),
    )
}

fn object_mapping() -> MappingEntry {
    MappingEntry::Single(Mapping {
        node: "orders".into(),
        label: Some("orders".into()),
        when: None,
        target: Target::Object {
            object_type: constant("Order"),
            id: col("id"),
            timestamp: None,
            attributes: vec![],
        },
    })
}

fn event_mapping() -> MappingEntry {
    MappingEntry::Single(Mapping {
        node: "orders".into(),
        label: Some("placed".into()),
        when: None,
        target: Target::Event {
            event_type: constant("Placed"),
            id: Some(col("id")),
            timestamp: TimestampSource::column("at"),
            attributes: vec![],
            objects: vec![],
        },
    })
}

fn view_bodies(c: &super::CompiledOcel) -> BTreeMap<String, String> {
    c.relations()
        .iter()
        .map(|v| (v.name.clone(), v.body.clone()))
        .collect()
}

#[test]
fn identifiers_and_literals_are_quoted_with_embedded_delimiters_doubled() {
    let d = SqlDialect::DuckDb;
    assert_eq!(d.quote_ident(r#"we"ird"#), r#""we""ird""#);
    assert_eq!(d.string_literal("it's"), "'it''s'");
}

#[test]
fn the_six_relations_are_always_emitted_even_for_an_empty_blueprint() {
    let bp = blueprint(vec![], vec![]);
    let c = compile(
        &bp,
        &ExtractionCatalog::new(),
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    let names: Vec<&str> = c.relations().iter().map(|v| v.name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "object",
            "event",
            "event_map_type",
            "object_map_type",
            "event_object",
            "object_object"
        ]
    );
    assert!(c.errors().is_empty(), "{:?}", c.errors());
}

#[test]
fn a_per_type_view_is_emitted_for_each_declared_type() {
    let bp = blueprint(
        vec![source("orders", "orders")],
        vec![object_mapping(), event_mapping()],
    );
    let c = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    assert!(c.errors().is_empty(), "{:?}", c.errors());
    let views = view_bodies(&c);
    assert!(views.contains_key("object_Order"), "{:?}", views.keys());
    assert!(views.contains_key("event_Placed"), "{:?}", views.keys());
}

#[test]
fn the_three_emission_paths_share_one_relation_body_set() {
    let bp = blueprint(
        vec![source("orders", "orders")],
        vec![object_mapping(), event_mapping()],
    );
    let c = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    let ddl = c.ddl();
    let materialize = c.materialize_ddl();
    let prelude = c.with_prelude("SELECT 1");
    for v in c.relations() {
        assert!(ddl.contains(&v.body), "ddl is missing {}", v.name);
        assert!(
            materialize.contains(&v.body),
            "materialize_ddl is missing {}",
            v.name
        );
        assert!(
            prelude.contains(&v.body),
            "with_prelude is missing {}",
            v.name
        );
    }
    assert!(ddl.contains("CREATE VIEW \"object\" AS"));
    assert!(materialize.contains("CREATE TABLE \"object\" AS"));
    assert!(prelude.starts_with("WITH \"object\" AS ("));
    assert!(prelude.ends_with("SELECT 1"));
}

/// An `E2O` mapping whose object endpoint is created rather than dropped
/// ([`MissingEndpointPolicy::Create`]) inherits the event endpoint's own "does this event
/// actually exist" check, so the row `assemble` adds to `object` semi-joins `event` and `object`
/// must be emitted after `event`.
fn object_created_by_e2o_depends_on_event() -> Blueprint {
    let mut bp = blueprint(
        vec![source("orders", "orders")],
        vec![
            event_mapping(),
            MappingEntry::Single(Mapping {
                node: "orders".into(),
                label: Some("tag".into()),
                when: None,
                target: Target::E2O {
                    event: EventEndpoint {
                        id: col("id"),
                        event_type: Some(constant("Placed")),
                    },
                    object: ObjectEndpoint {
                        id: col("kind"),
                        object_type: Some(constant("Tag")),
                        split: None,
                    },
                    qualifier: None,
                },
            }),
        ],
    );
    bp.on_missing_endpoint = MissingEndpointPolicy::Create;
    bp
}

#[test]
fn every_emission_path_emits_the_event_relation_before_the_object_one_that_reads_it() {
    let bp = object_created_by_e2o_depends_on_event();
    let c = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    assert!(c.errors().is_empty(), "{:?}", c.errors());
    // Confirm this blueprint actually exercises the dependency the test is pinning, not just
    // that some unrelated ordering happens to work out.
    let object_body = &view_bodies(&c)["object"];
    assert!(
        object_body.contains("FROM \"event\" AS e"),
        "fixture no longer creates the object-depends-on-event edge this test pins: {object_body}"
    );

    let paths = [
        (
            "ddl",
            c.ddl(),
            "CREATE VIEW \"event\" AS",
            "CREATE VIEW \"object\" AS",
        ),
        (
            "materialize_ddl",
            c.materialize_ddl(),
            "CREATE TABLE \"event\" AS",
            "CREATE TABLE \"object\" AS",
        ),
        (
            "with_prelude",
            c.with_prelude("SELECT 1"),
            "\"event\" AS (",
            "\"object\" AS (",
        ),
    ];
    for (path, sql, event_marker, object_marker) in paths {
        let event_pos = sql
            .find(event_marker)
            .unwrap_or_else(|| panic!("{path}: '{event_marker}' is missing:\n{sql}"));
        let object_pos = sql
            .find(object_marker)
            .unwrap_or_else(|| panic!("{path}: '{object_marker}' is missing:\n{sql}"));
        assert!(
            event_pos < object_pos,
            "{path}: 'object' semi-joins 'event' and must come after it:\n{sql}"
        );
    }
}

#[test]
fn with_prelude_returns_the_analysis_query_untouched_when_there_is_nothing_to_bind() {
    let c = super::CompiledOcel {
        dialect: SqlDialect::DuckDb,
        shape: EmissionShape::PerType,
        views: Vec::new(),
        probes: Vec::new(),
        errors: Vec::new(),
    };
    assert_eq!(c.with_prelude("SELECT 1"), "SELECT 1");
}

#[test]
fn probe_statements_carry_the_relation_ctes_so_they_run_view_free() {
    let bp = blueprint(vec![source("orders", "orders")], vec![object_mapping()]);
    let c = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    let statements = c.probe_statements();
    assert_eq!(statements.len(), c.probes().len());
    assert!(
        statements.iter().all(|s| s.starts_with("WITH ")),
        "every probe must be self-contained: {statements:?}"
    );
    assert!(c
        .probes()
        .iter()
        .any(|p| p.kind == ProbeKind::AmbiguousObjectIdentity));
}

#[test]
fn a_union_is_union_all_with_the_absent_column_null_filled() {
    let catalog = ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new("a", [("id", "INTEGER", false), ("extra", "TEXT", true)]),
        )
        .with_table("db", TableSchema::new("b", [("id", "INTEGER", false)]));
    let bp = blueprint(
        vec![
            source("a", "a"),
            source("b", "b"),
            Node {
                id: "u".into(),
                label: None,
                op: NodeOp::Union {
                    inputs: vec!["a".into(), "b".into()],
                },
            },
        ],
        vec![MappingEntry::Single(Mapping {
            node: "u".into(),
            label: None,
            when: None,
            target: Target::Object {
                object_type: constant("Order"),
                id: col("id"),
                timestamp: None,
                attributes: vec![],
            },
        })],
    );
    let c = compile(&bp, &catalog, SqlDialect::DuckDb, EmissionShape::PerType);
    assert!(c.errors().is_empty(), "{:?}", c.errors());
    let body = &view_bodies(&c)["object"];
    assert!(body.contains("UNION ALL"), "{body}");
    assert!(
        !body.replace("UNION ALL", "").contains("UNION"),
        "plain UNION would drop rows the extractor keeps: {body}"
    );
    assert!(
        body.contains("CAST(NULL AS VARCHAR) AS \"extra\""),
        "the branch without 'extra' must null-fill it: {body}"
    );
}

#[test]
fn a_cross_kind_join_key_compiles_to_a_join_that_matches_nothing() {
    // `join_key_part` tags each key with the runtime value's kind, so a Text "1" never matches
    // an Integer 1, where DuckDB would implicit-cast and join them.
    let catalog = ExtractionCatalog::new()
        .with_table("db", TableSchema::new("l", [("k", "TEXT", false)]))
        .with_table("db", TableSchema::new("r", [("k", "INTEGER", false)]));
    let bp = blueprint(
        vec![
            source("l", "l"),
            source("r", "r"),
            Node {
                id: "j".into(),
                label: None,
                op: NodeOp::Join {
                    left: "l".into(),
                    right: "r".into(),
                    on: vec![("k".into(), "k".into())],
                },
            },
        ],
        vec![MappingEntry::Single(Mapping {
            node: "j".into(),
            label: None,
            when: None,
            target: Target::Object {
                object_type: constant("Order"),
                id: col("k"),
                timestamp: None,
                attributes: vec![],
            },
        })],
    );
    let c = compile(&bp, &catalog, SqlDialect::DuckDb, EmissionShape::PerType);
    assert!(c.errors().is_empty(), "{:?}", c.errors());
    let body = &view_bodies(&c)["object"];
    assert!(
        body.contains("INNER JOIN") && body.contains("ON FALSE"),
        "a cross-kind key must be said to match nothing, not left to the engine: {body}"
    );
}

#[test]
fn a_join_key_whose_kind_the_catalog_does_not_declare_is_refused() {
    let catalog = ExtractionCatalog::new()
        .with_table("db", TableSchema::new("l", [("k", "GEOMETRY", false)]))
        .with_table("db", TableSchema::new("r", [("k", "GEOMETRY", false)]));
    let bp = blueprint(
        vec![
            source("l", "l"),
            source("r", "r"),
            Node {
                id: "j".into(),
                label: None,
                op: NodeOp::Join {
                    left: "l".into(),
                    right: "r".into(),
                    on: vec![("k".into(), "k".into())],
                },
            },
        ],
        vec![MappingEntry::Single(Mapping {
            node: "j".into(),
            label: None,
            when: None,
            target: Target::Object {
                object_type: constant("Order"),
                id: col("k"),
                timestamp: None,
                attributes: vec![],
            },
        })],
    );
    let c = compile(&bp, &catalog, SqlDialect::DuckDb, EmissionShape::PerType);
    assert!(
        matches!(
            c.errors().first().map(|e| &e.reason),
            Some(RejectReason::UndecidableJoinKey { .. })
        ),
        "{:?}",
        c.errors()
    );
}

#[test]
fn an_event_without_an_id_expression_is_reported_and_the_rest_still_compiles() {
    let bp = blueprint(
        vec![source("orders", "orders")],
        vec![
            object_mapping(),
            MappingEntry::Single(Mapping {
                node: "orders".into(),
                label: Some("minted".into()),
                when: None,
                target: Target::Event {
                    event_type: constant("Placed"),
                    id: None,
                    timestamp: TimestampSource::column("at"),
                    attributes: vec![],
                    objects: vec![],
                },
            }),
        ],
    );
    let c = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    assert_eq!(c.errors().len(), 1, "{:?}", c.errors());
    let err = &c.errors()[0];
    assert!(matches!(
        err.reason,
        RejectReason::SynthesizedId { field: "id" }
    ));
    assert_eq!(
        err.mapping.as_ref().map(|m| m.path.as_str()),
        Some("mappings[1]")
    );
    // The object mapping is untouched.
    assert!(view_bodies(&c).contains_key("object_Order"));
}

#[test]
fn a_type_read_from_a_column_without_a_domain_is_a_reject_not_a_wrong_view() {
    let bp = dynamic_type_blueprint();
    let c = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    assert!(
        matches!(
            c.errors().first().map(|e| &e.reason),
            Some(RejectReason::DynamicTypeName { .. })
        ),
        "{:?}",
        c.errors()
    );
    assert!(!view_bodies(&c)
        .keys()
        .any(|k| k.starts_with("object_") && k != "object_map_type" && k != "object_object"));
}

#[test]
fn a_supplied_domain_names_one_view_per_value_and_gets_a_staleness_probe() {
    let catalog = orders_catalog().with_domain(
        "db",
        "orders",
        "kind",
        ["retail".to_string(), "wholesale".to_string()],
    );
    let c = compile(
        &dynamic_type_blueprint(),
        &catalog,
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    assert!(c.errors().is_empty(), "{:?}", c.errors());
    let views = view_bodies(&c);
    assert!(views.contains_key("object_retail"), "{:?}", views.keys());
    assert!(views.contains_key("object_wholesale"), "{:?}", views.keys());
    let stale = c
        .probes()
        .iter()
        .find(|p| matches!(&p.kind, ProbeKind::StaleTypeDomain { column } if column == "kind"))
        .expect("a domain-derived type set must get a staleness probe");
    assert!(
        stale.sql.contains("NOT IN ('retail', 'wholesale')"),
        "{}",
        stale.sql
    );
}

#[test]
fn a_domain_above_the_cardinality_cap_is_an_error_naming_the_column() {
    let domain: Vec<String> = (0..=super::MAX_TYPE_DOMAIN)
        .map(|i| format!("t{i}"))
        .collect();
    let catalog = orders_catalog().with_domain("db", "orders", "kind", domain);
    let c = compile(
        &dynamic_type_blueprint(),
        &catalog,
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    assert!(
        matches!(
            c.errors().first().map(|e| &e.reason),
            Some(RejectReason::TypeDomainTooLarge { column, .. }) if column == "kind"
        ),
        "{:?}",
        c.errors()
    );
}

#[test]
fn a_recorded_but_empty_domain_is_a_reject_rather_than_a_view_set_with_nothing_in_it() {
    // Distinct from no domain at all, which `Catalog::column_domain` reports as `None`. With no
    // names there is no per-type view to emit and the probe would read `NOT IN ()`.
    let catalog = orders_catalog().with_domain("db", "orders", "kind", Vec::<String>::new());
    let c = compile(
        &dynamic_type_blueprint(),
        &catalog,
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    assert!(
        matches!(
            c.errors().first().map(|e| &e.reason),
            Some(RejectReason::DynamicTypeName { detail, .. }) if detail.contains("empty")
        ),
        "{:?}",
        c.errors()
    );
    assert!(c.probes().is_empty(), "{:?}", c.probes());
}

#[test]
fn a_dynamically_typed_events_attributes_are_columns_of_the_wide_events_table() {
    // The attribute plan keys a declaration by its type name, and a `Consolidated` mapping whose
    // type is read from a column has none. `events` still needs the column, or every attribute of
    // every dynamically-typed event is dropped from the table.
    let bp = blueprint(
        vec![source("orders", "orders")],
        vec![MappingEntry::Single(Mapping {
            node: "orders".into(),
            label: Some("dynamic".into()),
            when: None,
            target: Target::Event {
                event_type: col("kind"),
                id: Some(col("id")),
                timestamp: TimestampSource::column("at"),
                attributes: vec![AttributeMapping {
                    source_column: "kind".into(),
                    name: "kind".into(),
                    value_type: None,
                }],
                objects: vec![],
            },
        })],
    );
    let c = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::DuckDb,
        EmissionShape::Consolidated,
    );
    assert!(c.errors().is_empty(), "{:?}", c.errors());
    let events = &view_bodies(&c)["events"];
    assert!(
        events.starts_with("SELECT DISTINCT id, ocel_type, \"time\", \"kind\" FROM"),
        "the attribute must have a column of its own: {events}"
    );
    assert!(
        events.contains("src.\"kind\" AS \"kind\""),
        "and the branch must project the value into it: {events}"
    );
}

#[test]
fn an_o2o_creates_its_source_object_only_where_the_target_id_is_there_too() {
    // `run_o2o` renders both endpoint ids before it resolves either and returns as soon as one is
    // absent, so a row with no target creates no source object either.
    let mut bp = blueprint(
        vec![source("orders", "orders")],
        vec![MappingEntry::Single(Mapping {
            node: "orders".into(),
            label: Some("order-customer".into()),
            when: None,
            target: Target::O2O {
                source: ObjectEndpoint {
                    id: col("id"),
                    object_type: Some(constant("Order")),
                    split: None,
                },
                target: ObjectEndpoint {
                    id: col("kind"),
                    object_type: Some(constant("Customer")),
                    split: None,
                },
                qualifier: None,
            },
        })],
    );
    bp.on_missing_endpoint = MissingEndpointPolicy::Create;
    let c = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    assert!(c.errors().is_empty(), "{:?}", c.errors());
    let body = &view_bodies(&c)["object"];
    let created_order = body
        .split("UNION ALL")
        .find(|branch| branch.contains("'Order' AS ocel_type"))
        .unwrap_or_else(|| panic!("no branch creates the source object: {body}"));
    assert!(
        created_order.contains("src.\"kind\" IS NOT NULL"),
        "the created source object must inherit the target's id guard: {created_order}"
    );
}

#[test]
fn a_created_endpoint_whose_type_is_read_from_the_data_is_refused_by_a_per_type_shape() {
    // `validate` asks only that the endpoint declares a type, so a `Column` passes it. Emitting
    // the branch anyway puts objects in `object` that no `object_<T>` view and no
    // `object_map_type` row names.
    let mut bp = blueprint(
        vec![source("orders", "orders")],
        vec![
            event_mapping(),
            MappingEntry::Single(Mapping {
                node: "orders".into(),
                label: Some("tag".into()),
                when: None,
                target: Target::E2O {
                    event: EventEndpoint {
                        id: col("id"),
                        event_type: Some(constant("Placed")),
                    },
                    object: ObjectEndpoint {
                        id: col("id"),
                        object_type: Some(col("kind")),
                        split: None,
                    },
                    qualifier: None,
                },
            }),
        ],
    );
    bp.on_missing_endpoint = MissingEndpointPolicy::Create;
    let c = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    assert!(
        matches!(
            c.errors().first().map(|e| &e.reason),
            Some(RejectReason::DynamicTypeName { field, .. }) if *field == "object"
        ),
        "{:?}",
        c.errors()
    );
    // And nothing of that mapping reached the relations.
    let object_body = &view_bodies(&c)["object"];
    assert!(!object_body.contains("\"kind\""), "{object_body}");

    // The same blueprint is fine under `Consolidated`, where the type is a column value.
    let consolidated = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::DuckDb,
        EmissionShape::Consolidated,
    );
    assert!(
        consolidated.errors().is_empty(),
        "{:?}",
        consolidated.errors()
    );
}

#[test]
fn a_type_named_after_a_relation_the_compiler_defines_is_refused() {
    let bp = blueprint(
        vec![source("orders", "orders")],
        vec![MappingEntry::Single(Mapping {
            node: "orders".into(),
            label: None,
            when: None,
            target: Target::Object {
                object_type: constant("map_type"),
                id: col("id"),
                timestamp: None,
                attributes: vec![],
            },
        })],
    );
    let c = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    assert!(
        matches!(
            c.errors().first().map(|e| &e.reason),
            Some(RejectReason::ReservedTypeName { name }) if name == "map_type"
        ),
        "{:?}",
        c.errors()
    );
}

#[test]
fn a_negated_guard_is_forced_false_rather_than_left_null() {
    use crate::core::event_data::object_centric::extraction::predicate::{
        CompareOp, Literal, Operand, Predicate,
    };
    let guard = Predicate::Compare {
        left: Operand::Column {
            column: "kind".into(),
        },
        op: CompareOp::Eq,
        right: Operand::Literal {
            value: Literal::Text("retail".into()),
        },
    };
    let bp = blueprint(
        vec![source("orders", "orders")],
        vec![MappingEntry::Ordered {
            mappings: vec![
                Mapping {
                    node: "orders".into(),
                    label: Some("retail".into()),
                    when: Some(guard),
                    target: Target::Object {
                        object_type: constant("Retail"),
                        id: col("id"),
                        timestamp: None,
                        attributes: vec![],
                    },
                },
                Mapping {
                    node: "orders".into(),
                    label: Some("other".into()),
                    when: None,
                    target: Target::Object {
                        object_type: constant("Other"),
                        id: col("id"),
                        timestamp: None,
                        attributes: vec![],
                    },
                },
            ],
        }],
    );
    let c = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    assert!(c.errors().is_empty(), "{:?}", c.errors());
    let body = &view_bodies(&c)["object"];
    assert!(
        body.contains("(NOT COALESCE("),
        "the catch-all's negated guard must be total before NOT sees it: {body}"
    );
}

#[test]
fn the_consolidated_shape_emits_its_six_fixed_relations_even_for_an_empty_blueprint() {
    let bp = blueprint(vec![], vec![]);
    let c = compile(
        &bp,
        &ExtractionCatalog::new(),
        SqlDialect::DuckDb,
        EmissionShape::Consolidated,
    );
    let mut names: Vec<&str> = c.relations().iter().map(|v| v.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        vec![
            "e2o",
            "event_attr_meta",
            "events",
            "o2o",
            "object_attribute_changes",
            "objects",
        ]
    );
    assert!(c.errors().is_empty(), "{:?}", c.errors());
}

/// A [`Catalog`] that panics if [`Catalog::column_domain`] is ever called, so a test compiling
/// under it is itself the proof that [`EmissionShape::Consolidated`] never consults a domain --
/// not just that it tolerates a missing one.
#[derive(Debug)]
struct PanicOnDomainCatalog(ExtractionCatalog);

impl Catalog for PanicOnDomainCatalog {
    fn has_source(&self, source_id: &str) -> bool {
        self.0.has_source(source_id)
    }

    fn table(&self, source_id: &str, table: &str) -> Option<&TableSchema> {
        self.0.table(source_id, table)
    }

    fn column_domain(
        &self,
        _source_id: &str,
        _table: &str,
        _column: &str,
    ) -> Option<&BTreeSet<String>> {
        panic!(
            "EmissionShape::Consolidated must never call Catalog::column_domain: the type is a \
             column value, not a view name"
        );
    }
}

fn dynamic_type_blueprint() -> Blueprint {
    blueprint(
        vec![source("orders", "orders")],
        vec![MappingEntry::Single(Mapping {
            node: "orders".into(),
            label: Some("dynamic".into()),
            when: None,
            target: Target::Object {
                object_type: col("kind"),
                id: col("id"),
                timestamp: None,
                attributes: vec![],
            },
        })],
    )
}

#[test]
fn consolidated_compiles_a_dynamic_type_with_no_domain_without_ever_consulting_it() {
    let bp = dynamic_type_blueprint();
    let catalog = PanicOnDomainCatalog(orders_catalog());
    let c = compile(
        &bp,
        &catalog,
        SqlDialect::DuckDb,
        EmissionShape::Consolidated,
    );
    assert!(c.errors().is_empty(), "{:?}", c.errors());
    assert!(
        !c.probes()
            .iter()
            .any(|p| matches!(p.kind, ProbeKind::StaleTypeDomain { .. })),
        "no domain means nothing can go stale: {:?}",
        c.probes()
    );
    let objects_body = &view_bodies(&c)["objects"];
    assert!(
        objects_body.contains("src.\"kind\" AS ocel_type"),
        "the type must still be read straight off the column: {objects_body}"
    );
}

fn id_equals_text(
    value: &str,
) -> crate::core::event_data::object_centric::extraction::predicate::Predicate {
    use crate::core::event_data::object_centric::extraction::predicate::{
        CompareOp, Literal, Operand, Predicate,
    };
    Predicate::Compare {
        left: Operand::Column {
            column: "id".into(),
        },
        op: CompareOp::Eq,
        right: Operand::Literal {
            value: Literal::Text(value.into()),
        },
    }
}

#[test]
fn a_literal_is_coerced_to_the_operand_columns_declared_type() {
    // `id = "1"` against an INTEGER column: `prepare` coerces the text literal, so the emitted
    // comparison must be against an integer, not a string.
    let bp = blueprint(
        vec![source("orders", "orders")],
        vec![MappingEntry::Single(Mapping {
            node: "orders".into(),
            label: None,
            when: Some(id_equals_text("1")),
            target: Target::Object {
                object_type: constant("Order"),
                id: col("id"),
                timestamp: None,
                attributes: vec![],
            },
        })],
    );
    let c = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    assert!(c.errors().is_empty(), "{:?}", c.errors());
    let body = &view_bodies(&c)["object"];
    assert!(
        body.contains("CAST(1 AS BIGINT)"),
        "the literal must be coerced to the column's kind: {body}"
    );
    assert!(!body.contains("= '1'"), "{body}");
}

#[test]
fn an_uncoercible_literal_compiles_to_a_predicate_that_matches_nothing() {
    // Against the emitter rather than through `compile`: `validate` refuses a blueprint whose
    // literal cannot be read as its column's type, so the whole compile stops before this
    // predicate is reached. The emitter still has to answer for it, since a `Filter` node's
    // condition reaches the same code from the push-down path.
    let catalog = orders_catalog();
    let schema = catalog.table("db", "orders").expect("the fixture table");
    let sql = super::emit::predicate_sql(
        SqlDialect::DuckDb,
        &id_equals_text("abc"),
        schema,
        super::emit::ROW_ALIAS,
    )
    .expect("the predicate itself compiles");
    assert!(
        sql.contains("FALSE"),
        "an uncoercible literal must match nothing, exactly as `prepare` leaves it: {sql}"
    );
    // And it must not have been handed to the engine to implicit-cast instead, which is the one
    // way this could quietly select rows the extractor refuses.
    assert!(
        !sql.contains("'abc'"),
        "the literal must not reach the emitted SQL at all: {sql}"
    );
}

#[test]
fn a_timestamp_parsed_by_the_chrono_cascade_is_reported_as_residual() {
    let catalog = ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new("orders", [("id", "INTEGER", false), ("at", "TEXT", true)]),
    );
    let bp = blueprint(
        vec![source("orders", "orders")],
        vec![MappingEntry::Single(Mapping {
            node: "orders".into(),
            label: None,
            when: None,
            target: Target::Event {
                event_type: constant("Placed"),
                id: Some(col("id")),
                timestamp: TimestampSource::column("at"),
                attributes: vec![],
                objects: vec![],
            },
        })],
    );
    let c = compile(&bp, &catalog, SqlDialect::DuckDb, EmissionShape::PerType);
    assert!(
        matches!(
            c.errors().first().map(|e| &e.reason),
            Some(RejectReason::ResidualTimestamp { .. })
        ),
        "{:?}",
        c.errors()
    );
}

#[test]
fn a_constant_timestamp_folds_at_compile_time() {
    let bp = blueprint(
        vec![source("orders", "orders")],
        vec![MappingEntry::Single(Mapping {
            node: "orders".into(),
            label: None,
            when: None,
            target: Target::Event {
                event_type: constant("Placed"),
                id: Some(col("id")),
                timestamp: TimestampSource::constant("2020-01-01T00:00:00Z"),
                attributes: vec![],
                objects: vec![],
            },
        })],
    );
    let c = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    assert!(c.errors().is_empty(), "{:?}", c.errors());
    let body = &view_bodies(&c)["event_Placed"];
    assert!(
        body.contains("CAST('2020-01-01T00:00:00+00:00' AS TIMESTAMPTZ)"),
        "the fold must produce the constant's own instant: {body}"
    );
}

#[test]
fn every_reject_reason_renders_a_message_naming_what_it_is_about() {
    // Each reason is paired with something out of its own payload, so an arm that renders a
    // fixed sentence and drops what it is about fails here.
    let reasons = [
        (RejectReason::SynthesizedId { field: "id" }, "id"),
        (
            RejectReason::DynamicTypeName {
                field: "type",
                detail: "no domain".into(),
            },
            "no domain",
        ),
        (
            RejectReason::TypeDomainTooLarge {
                column: "kind".into(),
                size: 9,
                cap: 8,
            },
            "kind",
        ),
        (
            RejectReason::ReservedTypeName {
                name: "reserved".into(),
            },
            "reserved",
        ),
        (RejectReason::UnknownNode { node: "n".into() }, "n"),
        (RejectReason::UnresolvedNodeSchema { node: "n".into() }, "n"),
        (RejectReason::NodeCycle { node: "n".into() }, "n"),
        (RejectReason::EmptyProjection { node: "n".into() }, "n"),
        (RejectReason::EmptyUnion { node: "n".into() }, "n"),
        (
            RejectReason::UnknownColumn {
                column: "c".into(),
                field: "id",
            },
            "c",
        ),
        (
            RejectReason::UndeclaredColumnKind {
                column: "c".into(),
                col_type: "GEOMETRY".into(),
                field: "id",
            },
            "GEOMETRY",
        ),
        (
            RejectReason::UnstableIdentityRendering {
                column: "c".into(),
                col_type: "DOUBLE".into(),
                field: "id",
            },
            "DOUBLE",
        ),
        (
            RejectReason::UnstableDisplayRendering {
                column: "c".into(),
                col_type: "DOUBLE".into(),
                field: "matches",
            },
            "matches",
        ),
        (
            RejectReason::ResidualTimestamp {
                detail: "cascade".into(),
            },
            "cascade",
        ),
        (
            RejectReason::UndecidableJoinKey {
                node: "j".into(),
                side: "left",
                column: "k".into(),
                col_type: "GEOMETRY".into(),
            },
            "GEOMETRY",
        ),
        (
            RejectReason::InvalidRegex {
                pattern: "(".into(),
                message: "unclosed".into(),
            },
            "unclosed",
        ),
        (
            RejectReason::InvalidTemplate {
                template: "a{".into(),
                reason: "unterminated".into(),
            },
            "unterminated",
        ),
        (
            RejectReason::AttributeCoercion {
                attribute: "a".into(),
                column: "c".into(),
                col_type: "TEXT".into(),
                declared: "integer",
            },
            "integer",
        ),
        (
            RejectReason::DynamicTypeAttributeConflict {
                attribute: "conflicted".into(),
            },
            "conflicted",
        ),
        (
            RejectReason::ViewCycle {
                view: "object".into(),
            },
            "object",
        ),
    ];
    for (r, payload) in reasons {
        let message = r.to_string();
        assert!(
            message.contains(payload),
            "{r:?} rendered {message:?}, which does not name {payload:?}"
        );
    }
}

// The differential suite runs against DuckDB only, so nothing executes the PostgreSQL output. These
// pin the spellings that actually differ between the two, which is what a differential run would
// otherwise have caught, plus the one construct the dialect deliberately refuses.

#[test]
fn postgres_emits_the_same_relations_as_duckdb() {
    let bp = blueprint(
        vec![source("orders", "orders")],
        vec![object_mapping(), event_mapping()],
    );
    let pg = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::Postgres,
        EmissionShape::PerType,
    );
    let duck = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    assert!(pg.errors().is_empty(), "{:?}", pg.errors());
    let pg_names: Vec<&str> = pg.relations().iter().map(|v| v.name.as_str()).collect();
    let duck_names: Vec<&str> = duck.relations().iter().map(|v| v.name.as_str()).collect();
    assert_eq!(
        pg_names, duck_names,
        "the two dialects must describe the same OCEL surface"
    );
}

/// The type names, which are the most pervasive difference: `VARCHAR`/`DOUBLE` are `DuckDB`
/// spellings, and a `CAST(.. AS DOUBLE)` is a syntax error in `PostgreSQL`.
#[test]
fn postgres_uses_its_own_type_names() {
    use crate::core::event_data::object_centric::extraction::value::ValueKind;
    let pg = SqlDialect::Postgres;
    assert_eq!(pg.cast_to_text("x"), "CAST(x AS TEXT)");
    assert_eq!(pg.kind_sql_type(ValueKind::Text), "TEXT");
    assert_eq!(pg.kind_sql_type(ValueKind::Float), "DOUBLE PRECISION");
    assert_eq!(
        pg.attribute_sql_type(crate::core::event_data::object_centric::OCELAttributeType::Float),
        "DOUBLE PRECISION"
    );
    assert_eq!(
        pg.attribute_sql_type(crate::core::event_data::object_centric::OCELAttributeType::String),
        "TEXT"
    );

    let bp = blueprint(
        vec![source("orders", "orders")],
        vec![object_mapping(), event_mapping()],
    );
    let sql = compile(
        &bp,
        &orders_catalog(),
        SqlDialect::Postgres,
        EmissionShape::PerType,
    )
    .ddl();
    assert!(
        !sql.contains("AS VARCHAR"),
        "PostgreSQL output must not carry DuckDB's VARCHAR spelling:\n{sql}"
    );
    assert!(
        !sql.contains("AS DOUBLE)"),
        "PostgreSQL output must not carry DuckDB's DOUBLE spelling:\n{sql}"
    );
}

/// The three function names with no shared spelling.
#[test]
fn postgres_uses_its_own_function_names() {
    let pg = SqlDialect::Postgres;
    // `string_split` is DuckDB-only.
    assert!(pg
        .split_to_rows("x", ",")
        .starts_with("unnest(string_to_array(x, "));
    // `strftime` is DuckDB-only, and `%f` is not a PostgreSQL pattern.
    let iso = pg.timestamptz_to_iso_text("x");
    assert!(iso.starts_with("to_char(timezone('UTC', x), "), "{iso}");
    assert!(iso.contains("US"), "microseconds, six digits: {iso}");
    // In PostgreSQL `regexp_matches` is set-returning, so a predicate has to use `~`.
    assert_eq!(pg.regex_match("x", "a.c"), "(x ~ 'a.c')");
}

/// A split goes into a `SELECT` list, where a bare `(SELECT .. FROM regexp_matches(..))` is a
/// scalar subquery: any value with more than one match raises "more than one row returned by a
/// subquery used as an expression". The array constructor is what keeps it set-returning.
#[test]
fn postgres_regex_splitting_stays_set_returning() {
    let pg = SqlDialect::Postgres;
    for groups in [0, 1, 2] {
        let sql = pg.regex_split_to_rows("x", "a(b)(c)", groups);
        assert!(sql.starts_with("unnest(ARRAY("), "{groups} groups: {sql}");
    }
}

/// `list_concat` takes exactly two lists, so the number of capture groups decides how many calls
/// there are, not how many arguments one call has.
#[test]
fn duckdb_regex_splitting_never_passes_list_concat_a_third_list() {
    let duck = SqlDialect::DuckDb;
    let calls = |groups| {
        duck.regex_split_to_rows("x", "(a)(b)(c)", groups)
            .matches("list_concat")
            .count()
    };
    assert_eq!(calls(1), 0);
    assert_eq!(calls(2), 1);
    assert_eq!(calls(3), 2);
}
