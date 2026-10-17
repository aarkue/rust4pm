//! The differential harness: extract, compile, run the SQL, and compare the two logs.
//!
//! `compile::tests` checks the shape of emitted SQL; these tests check that the SQL means the same
//! thing the extractor does.
//!
//! Both sides are reduced to the same [`LogSets`] and compared field by field. Values are rendered
//! through one kind-tagged function used on both sides, so a `Text` `"1"` cannot pass for an
//! `Integer` `1`. The comparison is on sets, not multisets: every relation view is `DISTINCT`,
//! since the extractor's identity rules make a repeated `(event, object, qualifier)`
//! indistinguishable from a single one.
//!
//! The extractor runs against the fixture through `DuckDbRowProvider`, a test-only [`RowProvider`]
//! over the same connection the compiled SQL is executed on, so a divergence can only come from
//! the compiler. This also satisfies the compiler's catalog precondition, since `DuckDB` types its
//! columns rather than its cells.
//!
//! Under `IdRendering::Raw` two entities of different types can claim one id, where the extractor
//! reports `IdTypeCollision` and drops the loser while the compiled views keep both.
//! `assert_agrees` asserts the report contains no `IdTypeCollision` before comparing, so `Raw`
//! blueprints stay testable and a fixture that grows a collision fails with a named reason.
#![cfg(all(test, feature = "ocel-duckdb"))]

use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;

use chrono::{DateTime, FixedOffset};
use duckdb::types::ValueRef;
use duckdb::Connection;

use super::{compile, CompiledOcel, EmissionShape, ProbeKind, RejectReason, SqlDialect};
use crate::core::event_data::object_centric::extraction::blueprint::{
    Blueprint, DuplicateObjectPolicy, EventEndpoint, IdRendering, InlineObjectRef, Mapping,
    MappingEntry, MissingEndpointPolicy, Node, NodeOp, ObjectEndpoint, Target,
};
use crate::core::event_data::object_centric::extraction::catalog::{
    ExtractionCatalog, TableSchema,
};
use crate::core::event_data::object_centric::extraction::expr::{
    AttributeMapping, SplitKind, SplitSpec, TimestampSource, ValueExpression,
};
use crate::core::event_data::object_centric::extraction::extract::extract;
use crate::core::event_data::object_centric::extraction::predicate::{
    CompareOp, Literal, Operand, Predicate,
};
use crate::core::event_data::object_centric::extraction::provider::{ProviderError, RowProvider};
use crate::core::event_data::object_centric::extraction::report::{
    ExtractionError, ExtractionReport,
};
use crate::core::event_data::object_centric::extraction::slim_sink::SlimOcelSink;
use crate::core::event_data::object_centric::extraction::validate::validate;
use crate::core::event_data::object_centric::extraction::value::Value;
use crate::core::event_data::object_centric::ocel_sql::duckdb::schema::value::from_sql_value;
use crate::core::event_data::object_centric::readable::ReadableOCEL;
use crate::core::event_data::object_centric::{OCELAttributeType, OCELAttributeValue};

/// A `DuckDB` database in its own temporary directory. Every test gets its own, so a shared path
/// cannot corrupt under a concurrent run.
struct Fixture {
    _dir: tempfile::TempDir,
    con: Connection,
}

impl Fixture {
    /// Create the database and run `setup` (a `CREATE TABLE`/`INSERT` script) against it, with
    /// the session time zone pinned to UTC.
    ///
    /// Pinning it makes every other test here deterministic, but also blind to a fragment that
    /// reads a naive `TIMESTAMP`/`DATE` in the session's zone rather than anchoring it, since
    /// under UTC the two readings coincide. Hence [`Self::with_time_zone`].
    fn new(setup: &str) -> Self {
        Self::with_time_zone(setup, "UTC")
    }

    /// [`Self::new`] under an explicit session time zone.
    fn with_time_zone(setup: &str, zone: &str) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("fixture.duckdb");
        let con = Connection::open(&path).expect("open duckdb");
        con.execute_batch(&format!("SET TimeZone='{zone}';"))
            .expect("set tz");
        con.execute_batch(setup).expect("fixture setup");
        Self { _dir: dir, con }
    }
}

/// A test-only [`RowProvider`] over a `DuckDB` connection, so the extractor and the compiled SQL
/// read the very same rows.
#[derive(Debug)]
struct DuckDbRowProvider<'a> {
    con: &'a Connection,
}

fn backend(table: &str, e: &duckdb::Error) -> ProviderError {
    ProviderError::Backend {
        table: table.to_string(),
        message: e.to_string(),
    }
}

impl RowProvider for DuckDbRowProvider<'_> {
    fn scan(
        &self,
        table: &str,
        columns: &[&str],
        f: &mut dyn FnMut(&[Value]) -> ControlFlow<()>,
    ) -> Result<(), ProviderError> {
        let list = if columns.is_empty() {
            "1".to_string()
        } else {
            columns
                .iter()
                .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let sql = format!("SELECT {list} FROM \"{}\"", table.replace('"', "\"\""));
        let mut stmt = self.con.prepare(&sql).map_err(|e| backend(table, &e))?;
        let mut rows = stmt.query([]).map_err(|e| backend(table, &e))?;
        let mut buf = vec![Value::Null; columns.len()];
        while let Some(row) = rows.next().map_err(|e| backend(table, &e))? {
            for (i, slot) in buf.iter_mut().enumerate() {
                *slot = value_of(row.get_ref(i).map_err(|e| backend(table, &e))?);
            }
            if f(&buf).is_break() {
                return Ok(());
            }
        }
        Ok(())
    }
}

/// One `DuckDB` cell as the extractor's own [`Value`].
fn value_of(v: ValueRef<'_>) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Boolean(b) => Value::Boolean(b),
        ValueRef::TinyInt(i) => Value::Integer(i64::from(i)),
        ValueRef::SmallInt(i) => Value::Integer(i64::from(i)),
        ValueRef::Int(i) => Value::Integer(i64::from(i)),
        ValueRef::BigInt(i) => Value::Integer(i),
        ValueRef::UTinyInt(i) => Value::Integer(i64::from(i)),
        ValueRef::USmallInt(i) => Value::Integer(i64::from(i)),
        ValueRef::UInt(i) => Value::Integer(i64::from(i)),
        ValueRef::Float(f) => Value::Float(f64::from(f)),
        ValueRef::Double(f) => Value::Float(f),
        ValueRef::Text(t) => Value::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Timestamp(unit, raw) => Value::Timestamp(
            DateTime::from_timestamp_micros(unit.to_micros(raw))
                .expect("a timestamp DuckDB produced is in range")
                .fixed_offset(),
        ),
        ValueRef::Date32(d) => Value::Timestamp(
            DateTime::from_timestamp(i64::from(d) * 86_400, 0)
                .expect("a date DuckDB produced is in range")
                .fixed_offset(),
        ),
        other => panic!("fixture used a column type the harness does not convert: {other:?}"),
    }
}

/// One log reduced to comparable sets. See the module docs for why sets.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct LogSets {
    /// `(event id, event type, instant)`.
    events: BTreeSet<(String, String, String)>,
    /// `(event id, attribute name, rendered value)`.
    event_attributes: BTreeSet<(String, String, String)>,
    /// `(object id, object type)`.
    objects: BTreeSet<(String, String)>,
    /// `(object id, attribute name, instant, rendered value)`.
    object_attributes: BTreeSet<(String, String, String, String)>,
    /// `(event id, object id, qualifier)`.
    e2o: BTreeSet<(String, String, String)>,
    /// `(source object id, target object id, qualifier)`.
    o2o: BTreeSet<(String, String, String)>,
}

/// A value rendered with its kind, so a `Text` `"1"` cannot compare equal to an `Integer` `1`.
fn render(v: &Value) -> String {
    match v {
        Value::Null => "<null>".to_string(),
        Value::Text(s) => format!("s:{s}"),
        Value::Integer(i) => format!("i:{i}"),
        Value::Float(f) => format!("f:{f:?}"),
        Value::Boolean(b) => format!("b:{b}"),
        Value::Timestamp(t) => format!("t:{}", t.to_utc().to_rfc3339()),
    }
}

/// The same rendering for the extractor's own attribute values.
fn render_attr(v: &OCELAttributeValue) -> String {
    match v {
        OCELAttributeValue::Null => "<null>".to_string(),
        OCELAttributeValue::String(s) => format!("s:{s}"),
        OCELAttributeValue::Integer(i) => format!("i:{i}"),
        OCELAttributeValue::Float(f) => format!("f:{f:?}"),
        OCELAttributeValue::Boolean(b) => format!("b:{b}"),
        OCELAttributeValue::Time(t) => format!("t:{}", t.to_utc().to_rfc3339()),
    }
}

fn instant(t: DateTime<FixedOffset>) -> String {
    t.to_utc().to_rfc3339()
}

/// Reduce an extraction result to [`LogSets`].
fn from_extractor<O: ReadableOCEL + ?Sized>(ocel: &O) -> LogSets {
    let mut out = LogSets::default();
    for e in ocel.iter_events() {
        out.events
            .insert((e.id.clone(), e.event_type.clone(), instant(e.time)));
        for a in &e.attributes {
            out.event_attributes
                .insert((e.id.clone(), a.name.clone(), render_attr(&a.value)));
        }
        for r in &e.relationships {
            out.e2o
                .insert((e.id.clone(), r.object_id.clone(), r.qualifier.clone()));
        }
    }
    for o in ocel.iter_objects() {
        out.objects.insert((o.id.clone(), o.object_type.clone()));
        for a in &o.attributes {
            out.object_attributes.insert((
                o.id.clone(),
                a.name.clone(),
                instant(a.time),
                render_attr(&a.value),
            ));
        }
        for r in &o.relationships {
            out.o2o
                .insert((o.id.clone(), r.object_id.clone(), r.qualifier.clone()));
        }
    }
    out
}

/// Run `sql` and return `(column names, rows)` with every cell as a [`Value`].
fn query(con: &Connection, sql: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    let mut stmt = con
        .prepare(sql)
        .unwrap_or_else(|e| panic!("preparing\n{sql}\nfailed: {e}"));
    let mut rows = stmt
        .query([])
        .unwrap_or_else(|e| panic!("running\n{sql}\nfailed: {e}"));
    let mut names: Vec<String> = Vec::new();
    let mut out = Vec::new();
    while let Some(row) = rows.next().expect("fetch row") {
        if names.is_empty() {
            names = row.as_ref().column_names();
        }
        out.push(
            (0..names.len())
                .map(|i| value_of(row.get_ref(i).expect("read cell")))
                .collect(),
        );
    }
    (names, out)
}

fn text_of(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        other => panic!("expected text, got {other:?}"),
    }
}

/// Reduce the compiled views to [`LogSets`], reading the same OCEL 2.0 relations an external
/// tool would.
fn from_sql(con: &Connection) -> LogSets {
    let mut out = LogSets::default();

    let (_, event_types) = query(
        con,
        "SELECT ocel_type, ocel_type_map FROM \"event_map_type\"",
    );
    for row in &event_types {
        let (ocel_type, suffix) = (text_of(&row[0]), text_of(&row[1]));
        let (names, rows) = query(con, &format!("SELECT * FROM \"event_{suffix}\""));
        for r in rows {
            let id = text_of(&r[0]);
            let Value::Timestamp(t) = &r[1] else {
                panic!("event_{suffix}.ocel_time is not a timestamp: {:?}", r[1]);
            };
            out.events
                .insert((id.clone(), ocel_type.clone(), instant(*t)));
            for (i, name) in names.iter().enumerate().skip(2) {
                out.event_attributes
                    .insert((id.clone(), name.clone(), render(&r[i])));
            }
        }
    }

    let (_, objects) = query(con, "SELECT ocel_id, ocel_type FROM \"object\"");
    for r in &objects {
        out.objects.insert((text_of(&r[0]), text_of(&r[1])));
    }

    let (_, object_types) = query(
        con,
        "SELECT ocel_type, ocel_type_map FROM \"object_map_type\"",
    );
    for row in &object_types {
        let suffix = text_of(&row[1]);
        let (names, rows) = query(con, &format!("SELECT * FROM \"object_{suffix}\""));
        for r in rows {
            // A row with no `ocel_changed_field` is the object's existence row and carries no
            // observation. See `object_type_view`'s docs.
            let Value::Text(changed) = &r[2] else {
                continue;
            };
            let id = text_of(&r[0]);
            let Value::Timestamp(t) = &r[1] else {
                panic!("object_{suffix}.ocel_time is not a timestamp: {:?}", r[1]);
            };
            let column = names
                .iter()
                .position(|n| n == changed)
                .unwrap_or_else(|| panic!("object_{suffix} has no column '{changed}'"));
            out.object_attributes
                .insert((id, changed.clone(), instant(*t), render(&r[column])));
        }
    }

    let (_, e2o) = query(
        con,
        "SELECT ocel_event_id, ocel_object_id, ocel_qualifier FROM \"event_object\"",
    );
    for r in &e2o {
        out.e2o
            .insert((text_of(&r[0]), text_of(&r[1]), text_of(&r[2])));
    }
    let (_, o2o) = query(
        con,
        "SELECT ocel_source_id, ocel_target_id, ocel_qualifier FROM \"object_object\"",
    );
    for r in &o2o {
        out.o2o
            .insert((text_of(&r[0]), text_of(&r[1]), text_of(&r[2])));
    }
    out
}

/// Reduce [`EmissionShape::Consolidated`]'s relations to [`LogSets`], reading them the way an
/// external reader would: `events` is a plain wide `SELECT *`, and
/// `object_attribute_changes.value`/`value_type` round-trip through [`from_sql_value`], the same
/// function
/// `DuckDbLinkedOCEL` uses.
fn from_sql_consolidated(con: &Connection) -> LogSets {
    let mut out = LogSets::default();

    let (names, events) = query(con, "SELECT * FROM \"events\"");
    for r in &events {
        let id = text_of(&r[0]);
        let ocel_type = text_of(&r[1]);
        let Value::Timestamp(t) = &r[2] else {
            panic!("events.time is not a timestamp: {:?}", r[2]);
        };
        out.events.insert((id.clone(), ocel_type, instant(*t)));
        for (i, name) in names.iter().enumerate().skip(3) {
            // A wide column an event's own type never declared reads back as `NULL`, not an
            // observation.
            if matches!(r[i], Value::Null) {
                continue;
            }
            out.event_attributes
                .insert((id.clone(), name.clone(), render(&r[i])));
        }
    }

    let (_, objects) = query(con, "SELECT id, ocel_type FROM \"objects\"");
    for r in &objects {
        out.objects.insert((text_of(&r[0]), text_of(&r[1])));
    }

    let (_, attrs) = query(
        con,
        "SELECT id, name, \"time\", value, value_type FROM \"object_attribute_changes\"",
    );
    for r in &attrs {
        let id = text_of(&r[0]);
        let name = text_of(&r[1]);
        let Value::Timestamp(t) = &r[2] else {
            panic!(
                "object_attribute_changes.time is not a timestamp: {:?}",
                r[2]
            );
        };
        let value = from_sql_value(&text_of(&r[3]), &text_of(&r[4]));
        out.object_attributes
            .insert((id, name, instant(*t), render_attr(&value)));
    }

    let (_, e2o) = query(con, "SELECT event_id, object_id, qualifier FROM \"e2o\"");
    for r in &e2o {
        out.e2o
            .insert((text_of(&r[0]), text_of(&r[1]), text_of(&r[2])));
    }
    let (_, o2o) = query(con, "SELECT source_id, target_id, qualifier FROM \"o2o\"");
    for r in &o2o {
        out.o2o
            .insert((text_of(&r[0]), text_of(&r[1]), text_of(&r[2])));
    }
    out
}

/// What one differential run produced, for tests that want to assert more than agreement.
/// `compiled`/`sql` are `PerType`'s, `consolidated`/`consolidated_sql` are `Consolidated`'s.
struct Run {
    report: ExtractionReport,
    compiled: CompiledOcel,
    extractor: LogSets,
    sql: LogSets,
    consolidated: CompiledOcel,
    consolidated_sql: LogSets,
}

/// Every entry of an `event_attributes`-shaped set except the `Null`-valued ones. See
/// [`assert_consolidated_agrees`] for why the carve-out exists.
fn drop_null_event_attrs(
    set: &BTreeSet<(String, String, String)>,
) -> BTreeSet<(String, String, String)> {
    set.iter()
        .filter(|(_, _, v)| v != "<null>")
        .cloned()
        .collect()
}

/// The six [`LogSets`] fields, field by field, prefixing every failure with `shape` so a
/// disagreement names which emission surface produced it. `drop_null_event_attributes` is
/// documented on its one caller, [`assert_consolidated_agrees`].
fn assert_log_sets_agree(
    extractor: &LogSets,
    sql: &LogSets,
    shape: &str,
    drop_null_event_attributes: bool,
) {
    assert_eq!(
        extractor.events, sql.events,
        "[{shape}] events disagree (extractor left, SQL right)"
    );
    if drop_null_event_attributes {
        assert_eq!(
            drop_null_event_attrs(&extractor.event_attributes),
            drop_null_event_attrs(&sql.event_attributes),
            "[{shape}] event attributes disagree"
        );
    } else {
        assert_eq!(
            extractor.event_attributes, sql.event_attributes,
            "[{shape}] event attributes disagree"
        );
    }
    assert_eq!(extractor.objects, sql.objects, "[{shape}] objects disagree");
    assert_eq!(
        extractor.object_attributes, sql.object_attributes,
        "[{shape}] object attribute observations disagree"
    );
    assert_eq!(extractor.e2o, sql.e2o, "[{shape}] E2O relations disagree");
    assert_eq!(extractor.o2o, sql.o2o, "[{shape}] O2O relations disagree");
}

/// [`assert_log_sets_agree`] against `PerType`'s own output, with nothing dropped: `PerType` has
/// one view per event type, so a `NULL` cell there is unambiguously an observation of `Null`.
fn assert_per_type_agrees(extractor: &LogSets, sql: &LogSets) {
    assert_log_sets_agree(extractor, sql, "PerType", false);
}

/// [`assert_log_sets_agree`] against `Consolidated`'s own output, with every `Null`-valued event
/// attribute observation dropped from both sides first.
///
/// A `NULL` cell in that shape's wide `events` table is ambiguous between "declared, with value
/// `Null`" and "belongs to a different event type", a limit of the schema that every reader
/// shares. Object attributes have no such gap, since `object_attribute_changes` is EAV.
fn assert_consolidated_agrees(extractor: &LogSets, sql: &LogSets) {
    assert_log_sets_agree(extractor, sql, "Consolidated", true);
}

/// Whether a differential run tolerates the compiler refusing a mapping.
///
/// A mapping the compiler skipped is a relation neither side carries, so an unasserted
/// `RejectReason` turns a would-be divergence into a silent pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompileErrors {
    /// Neither shape may refuse anything: [`run_both`] asserts both error lists are empty.
    None,
    /// The test asserts the refusals itself.
    Expected,
}

/// Extract, compile both emission shapes, execute and compare.
///
/// Asserts, in order, that the blueprint validates, that the extraction reports no
/// `IdTypeCollision` (see the module docs), that neither shape refused a mapping, that every
/// probe of both shapes returns zero rows, and that each shape's [`LogSets`] equals the
/// extractor's.
fn assert_agrees(fx: &Fixture, blueprint: &Blueprint, catalog: &ExtractionCatalog) -> Run {
    let run = run_both(fx, blueprint, catalog);
    assert_per_type_agrees(&run.extractor, &run.sql);
    assert_consolidated_agrees(&run.extractor, &run.consolidated_sql);
    run
}

/// `probes()` paired with `probe_statements()`, checking the two lists have the same length so a
/// `zip` cannot silently drop the tail.
fn probes_with_sql(compiled: &CompiledOcel) -> Vec<(ProbeKind, String)> {
    let statements = compiled.probe_statements();
    assert_eq!(
        compiled.probes().len(),
        statements.len(),
        "every probe must have a statement, or zipping the two silently checks a prefix"
    );
    compiled
        .probes()
        .iter()
        .map(|p| p.kind.clone())
        .zip(statements)
        .collect()
}

/// Every probe a compile produced, run against `con` and asserted empty.
/// [`every_probe_kind_is_shown_to_fire`] separately drives each [`ProbeKind`] into returning rows,
/// since emptiness is satisfied vacuously by a probe that selects nothing.
fn assert_probes_hold(con: &Connection, compiled: &CompiledOcel, shape: &str) {
    for (kind, sql) in probes_with_sql(compiled) {
        let (_, rows) = query(con, &sql);
        assert!(
            rows.is_empty(),
            "[{shape}] probe {kind:?} must hold before the views may be compared: {} rows\n{sql}",
            rows.len()
        );
    }
}

/// Which [`ProbeKind`]s actually return rows against `con`, as their `Debug` spellings.
fn firing_probe_kinds(con: &Connection, compiled: &CompiledOcel) -> BTreeSet<String> {
    probes_with_sql(compiled)
        .into_iter()
        .filter(|(_, sql)| !query(con, sql).1.is_empty())
        .map(|(kind, _)| format!("{kind:?}"))
        .collect()
}

/// Unless the caller said it expects one, a [`CompileError`](super::CompileError) fails the run.
fn assert_no_unexpected_errors(compiled: &CompiledOcel, shape: &str, errors: CompileErrors) {
    if errors == CompileErrors::None {
        assert!(
            compiled.errors().is_empty(),
            "[{shape}] the compiler refused a mapping, so the comparison below would be between \
             an extraction and a partial compile: {:?}",
            compiled.errors()
        );
    }
}

/// Whether every `Target::Event` in `blueprint` names its type with a `Constant`.
///
/// Only a statically known event type contributes rows to `event_attr_meta`, so comparing that
/// relation against the extractor's declarations is exact only for such a blueprint.
fn every_event_type_is_static(blueprint: &Blueprint) -> bool {
    fn is_static(m: &Mapping) -> bool {
        match &m.target {
            Target::Event { event_type, .. } => {
                matches!(event_type, ValueExpression::Constant { .. })
            }
            _ => true,
        }
    }
    blueprint.mappings.iter().all(|entry| match entry {
        MappingEntry::Single(m) => is_static(m),
        MappingEntry::Ordered { mappings } => mappings.iter().all(is_static),
    })
}

/// `event_attr_meta`, the one `Consolidated` relation [`from_sql_consolidated`] does not read, so
/// no [`LogSets`] comparison can catch it being wrong. Compared against the extractor's own
/// [`ReadableOCEL::event_types`].
fn assert_event_attr_meta_agrees<O: ReadableOCEL + ?Sized>(con: &Connection, ocel: &O) {
    let expected: BTreeSet<(String, String, String)> = ocel
        .event_types()
        .iter()
        .flat_map(|t| {
            t.attributes
                .iter()
                .map(|a| (t.name.clone(), a.name.clone(), a.value_type.clone()))
        })
        .collect();
    let (_, rows) = query(
        con,
        "SELECT event_type, attr_name, attr_type FROM \"event_attr_meta\"",
    );
    let actual: BTreeSet<(String, String, String)> = rows
        .iter()
        .map(|r| (text_of(&r[0]), text_of(&r[1]), text_of(&r[2])))
        .collect();
    assert_eq!(
        expected, actual,
        "[Consolidated] event_attr_meta disagrees with the extractor's event-type declarations \
         (extractor left, SQL right)"
    );
}

/// Extract, compile [`EmissionShape::Consolidated`] alone and execute it, for the blueprints
/// [`run_both`] cannot take because `PerType` refuses them. Returns the compile, the extractor's
/// log and the SQL's, uncompared.
fn run_consolidated_only(
    fx: &Fixture,
    blueprint: &Blueprint,
    catalog: &ExtractionCatalog,
) -> (CompiledOcel, LogSets, LogSets) {
    assert_eq!(
        validate(blueprint, catalog),
        vec![],
        "blueprint must validate"
    );
    let provider = DuckDbRowProvider { con: &fx.con };
    let mut providers: HashMap<String, &dyn RowProvider> = HashMap::new();
    providers.insert("db".to_string(), &provider);
    let mut sink = SlimOcelSink::new();
    extract(blueprint, catalog, &providers, &mut sink).expect("extract");
    let extractor = from_extractor(sink.ocel());

    let compiled = compile(
        blueprint,
        catalog,
        SqlDialect::DuckDb,
        EmissionShape::Consolidated,
    );
    assert!(compiled.errors().is_empty(), "{:?}", compiled.errors());
    let ddl = compiled.ddl();
    fx.con.execute_batch(&ddl).unwrap_or_else(|e| {
        panic!("executing the emitted Consolidated DDL failed: {e}\n---\n{ddl}")
    });
    assert_probes_hold(&fx.con, &compiled, "Consolidated");
    let sql = from_sql_consolidated(&fx.con);
    (compiled, extractor, sql)
}

/// The half of [`assert_agrees`] that produces every log without comparing them, with both
/// shapes required to compile whole.
fn run_both(fx: &Fixture, blueprint: &Blueprint, catalog: &ExtractionCatalog) -> Run {
    run_both_with(fx, blueprint, catalog, CompileErrors::None)
}

/// [`run_both`] for the tests that deliberately provoke a [`RejectReason`] and assert it
/// themselves.
fn run_both_expecting_errors(
    fx: &Fixture,
    blueprint: &Blueprint,
    catalog: &ExtractionCatalog,
) -> Run {
    run_both_with(fx, blueprint, catalog, CompileErrors::Expected)
}

/// Produce every log without comparing them, for the tests that have to demonstrate an expected
/// difference (a skipped mapping).
///
/// Both shapes' `CREATE VIEW`s go on the same connection: `PerType` and `Consolidated` never
/// share a relation name, so nothing here needs two databases.
fn run_both_with(
    fx: &Fixture,
    blueprint: &Blueprint,
    catalog: &ExtractionCatalog,
    errors: CompileErrors,
) -> Run {
    assert_eq!(
        validate(blueprint, catalog),
        vec![],
        "blueprint must validate"
    );

    let provider = DuckDbRowProvider { con: &fx.con };
    let mut providers: HashMap<String, &dyn RowProvider> = HashMap::new();
    providers.insert("db".to_string(), &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(blueprint, catalog, &providers, &mut sink).expect("extract");
    assert!(
        !report
            .errors
            .iter()
            .any(|e| matches!(e, ExtractionError::IdTypeCollision { .. })),
        "harness precondition: a cross-type id collision makes the extractor drop an entity the \
         views keep, which is not the compiler's disagreement to answer for: {:?}",
        report.errors
    );
    let extractor = from_extractor(sink.ocel());

    let compiled = compile(
        blueprint,
        catalog,
        SqlDialect::DuckDb,
        EmissionShape::PerType,
    );
    assert_no_unexpected_errors(&compiled, "PerType", errors);
    let ddl = compiled.ddl();
    fx.con
        .execute_batch(&ddl)
        .unwrap_or_else(|e| panic!("executing the emitted PerType DDL failed: {e}\n---\n{ddl}"));
    assert_probes_hold(&fx.con, &compiled, "PerType");
    let sql = from_sql(&fx.con);

    let consolidated = compile(
        blueprint,
        catalog,
        SqlDialect::DuckDb,
        EmissionShape::Consolidated,
    );
    assert_no_unexpected_errors(&consolidated, "Consolidated", errors);
    let consolidated_ddl = consolidated.ddl();
    fx.con.execute_batch(&consolidated_ddl).unwrap_or_else(|e| {
        panic!("executing the emitted Consolidated DDL failed: {e}\n---\n{consolidated_ddl}")
    });
    assert_probes_hold(&fx.con, &consolidated, "Consolidated");
    let consolidated_sql = from_sql_consolidated(&fx.con);

    if errors == CompileErrors::None && every_event_type_is_static(blueprint) {
        assert_event_attr_meta_agrees(&fx.con, sink.ocel());
    }

    Run {
        report,
        compiled,
        extractor,
        sql,
        consolidated,
        consolidated_sql,
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

fn blueprint(rendering: IdRendering, nodes: Vec<Node>, mappings: Vec<MappingEntry>) -> Blueprint {
    Blueprint {
        version: crate::core::event_data::object_centric::extraction::MODEL_VERSION,
        id_rendering: rendering,
        nodes,
        mappings,
        on_missing_endpoint: MissingEndpointPolicy::Drop,
        on_duplicate_object: DuplicateObjectPolicy::FirstWins,
    }
}

fn single(node: &str, label: &str, when: Option<Predicate>, target: Target) -> MappingEntry {
    MappingEntry::Single(Mapping {
        node: node.to_string(),
        label: Some(label.to_string()),
        when,
        target,
    })
}

fn ts(column: &str) -> TimestampSource {
    TimestampSource::column(column.to_string())
}

fn endpoint(id: &str, object_type: &str) -> ObjectEndpoint {
    ObjectEndpoint {
        id: col(id),
        object_type: Some(constant(object_type)),
        split: None,
    }
}

fn eq_text(column: &str, value: &str) -> Predicate {
    Predicate::Compare {
        left: Operand::Column {
            column: column.to_string(),
        },
        op: CompareOp::Eq,
        right: Operand::Literal {
            value: Literal::Text(value.to_string()),
        },
    }
}

const ORDERS: &str = "
CREATE TABLE orders (id BIGINT, cust VARCHAR, ts TIMESTAMP);
INSERT INTO orders VALUES
  (1, 'ACME',   TIMESTAMP '2020-01-01 08:00:00'),
  (2, 'ACME',   TIMESTAMP '2020-01-02 09:30:00'),
  (3, 'GLOBEX', TIMESTAMP '2020-01-03 10:15:00'),
  (4, NULL,     TIMESTAMP '2020-01-04 11:00:00'),
  (5, 'GLOBEX', NULL);
";

fn orders_catalog() -> ExtractionCatalog {
    ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "orders",
            [
                ("id", "BIGINT", false),
                ("cust", "VARCHAR", true),
                ("ts", "TIMESTAMP", true),
            ],
        ),
    )
}

fn order_object() -> MappingEntry {
    single(
        "orders",
        "order",
        None,
        Target::Object {
            object_type: constant("Order"),
            id: col("id"),
            timestamp: None,
            attributes: vec![],
        },
    )
}

fn customer_object() -> MappingEntry {
    single(
        "orders",
        "customer",
        None,
        Target::Object {
            object_type: constant("Customer"),
            id: col("cust"),
            timestamp: None,
            attributes: vec![],
        },
    )
}

fn placed_event() -> MappingEntry {
    single(
        "orders",
        "placed",
        None,
        Target::Event {
            event_type: constant("Placed"),
            id: Some(col("id")),
            timestamp: ts("ts"),
            attributes: vec![],
            objects: vec![],
        },
    )
}

#[test]
fn case_1_event_object_e2o_and_o2o_targets_each_agree() {
    let fx = Fixture::new(ORDERS);
    let bp = blueprint(
        IdRendering::TypePrefixed,
        vec![source("orders", "orders")],
        vec![
            order_object(),
            customer_object(),
            placed_event(),
            single(
                "orders",
                "placed-order",
                None,
                Target::E2O {
                    event: EventEndpoint {
                        id: col("id"),
                        event_type: Some(constant("Placed")),
                    },
                    object: endpoint("id", "Order"),
                    qualifier: Some(constant("order")),
                },
            ),
            single(
                "orders",
                "order-customer",
                None,
                Target::O2O {
                    source: endpoint("id", "Order"),
                    target: endpoint("cust", "Customer"),
                    qualifier: Some(constant("buyer")),
                },
            ),
        ],
    );
    let run = assert_agrees(&fx, &bp, &orders_catalog());
    // The comparison is only worth something if it compared something.
    assert!(!run.extractor.objects.is_empty());
    assert!(!run.extractor.e2o.is_empty());
    assert!(!run.extractor.o2o.is_empty());
    // Row 5 has no timestamp, so its event is dropped on both sides.
    assert_eq!(run.extractor.events.len(), 4, "{:?}", run.extractor.events);
    assert!(
        run.compiled.errors().is_empty(),
        "{:?}",
        run.compiled.errors()
    );
}

#[test]
fn case_1_an_inline_object_reference_agrees() {
    let fx = Fixture::new(ORDERS);
    let bp = blueprint(
        IdRendering::TypePrefixed,
        vec![source("orders", "orders")],
        vec![
            customer_object(),
            single(
                "orders",
                "placed",
                None,
                Target::Event {
                    event_type: constant("Placed"),
                    id: Some(col("id")),
                    timestamp: ts("ts"),
                    attributes: vec![],
                    objects: vec![InlineObjectRef {
                        object: endpoint("cust", "Customer"),
                        qualifier: Some(constant("buyer")),
                    }],
                },
            ),
        ],
    );
    let run = assert_agrees(&fx, &bp, &orders_catalog());
    assert!(!run.extractor.e2o.is_empty());
    let _ = run.report;
}

#[test]
fn case_1_an_o2o_creates_no_source_object_for_a_row_whose_target_id_is_absent() {
    // `run_o2o` renders both endpoint ids before it resolves either, and returns as soon as one
    // is absent, so row 4 (`cust` NULL) creates neither `Order-4` nor a customer. A compiled
    // source branch built from the filters as they stood before the target's guards were added
    // creates `Order-4` regardless, an object no extraction produces.
    let fx = Fixture::new(ORDERS);
    let mut bp = blueprint(
        IdRendering::TypePrefixed,
        vec![source("orders", "orders")],
        vec![single(
            "orders",
            "order-customer",
            None,
            Target::O2O {
                source: endpoint("id", "Order"),
                target: endpoint("cust", "Customer"),
                qualifier: Some(constant("buyer")),
            },
        )],
    );
    // Both endpoints exist only as relation ends, so they have to be created rather than dropped.
    bp.on_missing_endpoint = MissingEndpointPolicy::Create;
    let run = assert_agrees(&fx, &bp, &orders_catalog());
    assert!(
        !run.extractor
            .objects
            .contains(&("Order-4".to_string(), "Order".to_string())),
        "the row with no customer must create no order either: {:?}",
        run.extractor.objects
    );
    assert_eq!(run.extractor.o2o.len(), 4, "{:?}", run.extractor.o2o);
}

const DOCS: &str = "
CREATE TABLE docs (id BIGINT, kind VARCHAR, ts TIMESTAMP);
INSERT INTO docs VALUES
  (1, 'invoice', TIMESTAMP '2021-01-01 00:00:00'),
  (2, 'credit',  TIMESTAMP '2021-01-02 00:00:00'),
  (3, 'invoice', TIMESTAMP '2021-01-03 00:00:00'),
  (4, NULL,      TIMESTAMP '2021-01-04 00:00:00');
";

fn docs_catalog() -> ExtractionCatalog {
    ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "docs",
            [
                ("id", "BIGINT", false),
                ("kind", "VARCHAR", true),
                ("ts", "TIMESTAMP", false),
            ],
        ),
    )
}

#[test]
fn case_2_when_discriminated_mappings_over_one_node_agree() {
    let fx = Fixture::new(DOCS);
    let bp = blueprint(
        IdRendering::TypePrefixed,
        vec![source("docs", "docs")],
        vec![
            single(
                "docs",
                "invoices",
                Some(eq_text("kind", "invoice")),
                Target::Object {
                    object_type: constant("Invoice"),
                    id: col("id"),
                    timestamp: None,
                    attributes: vec![],
                },
            ),
            single(
                "docs",
                "credits",
                Some(eq_text("kind", "credit")),
                Target::Object {
                    object_type: constant("CreditNote"),
                    id: col("id"),
                    timestamp: None,
                    attributes: vec![],
                },
            ),
        ],
    );
    let run = assert_agrees(&fx, &bp, &docs_catalog());
    assert_eq!(
        run.extractor.objects.len(),
        3,
        "{:?}",
        run.extractor.objects
    );
}

#[test]
fn case_3_an_ordered_group_keeps_the_rows_whose_guard_column_is_null() {
    // `desugar` rewrites the catch-all's guard to `Not(kind = 'invoice') AND Not(kind =
    // 'credit')`. In the extractor's two-valued evaluation that is true for row 4, whose `kind`
    // is NULL. A naive SQL compile makes `NOT (kind = 'invoice')` NULL there and drops exactly
    // that row, which is why this fixture has a NULL in the guard column.
    let fx = Fixture::new(DOCS);
    let bp = blueprint(
        IdRendering::TypePrefixed,
        vec![source("docs", "docs")],
        vec![MappingEntry::Ordered {
            mappings: vec![
                Mapping {
                    node: "docs".into(),
                    label: Some("invoice".into()),
                    when: Some(eq_text("kind", "invoice")),
                    target: Target::Object {
                        object_type: constant("Invoice"),
                        id: col("id"),
                        timestamp: None,
                        attributes: vec![],
                    },
                },
                Mapping {
                    node: "docs".into(),
                    label: Some("credit".into()),
                    when: Some(eq_text("kind", "credit")),
                    target: Target::Object {
                        object_type: constant("CreditNote"),
                        id: col("id"),
                        timestamp: None,
                        attributes: vec![],
                    },
                },
                Mapping {
                    node: "docs".into(),
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
    let run = assert_agrees(&fx, &bp, &docs_catalog());
    assert!(
        run.extractor
            .objects
            .contains(&("Other-4".to_string(), "Other".to_string())),
        "the NULL-guard row must reach the catch-all: {:?}",
        run.extractor.objects
    );
    assert!(
        run.sql
            .objects
            .contains(&("Other-4".to_string(), "Other".to_string())),
        "and the compiled view must keep it too: {:?}",
        run.sql.objects
    );
}

const JOINED: &str = "
CREATE TABLE headers (id BIGINT, ref VARCHAR, ts TIMESTAMP);
CREATE TABLE lines (id BIGINT, ref VARCHAR, sku VARCHAR);
CREATE TABLE more_lines (id BIGINT, ref VARCHAR);
INSERT INTO headers VALUES
  (1, 'R1', TIMESTAMP '2022-01-01 00:00:00'),
  (2, 'R2', TIMESTAMP '2022-01-02 00:00:00'),
  (3, 'R3', TIMESTAMP '2022-01-03 00:00:00');
INSERT INTO lines VALUES (10, 'R1', 'A'), (11, 'R1', 'B'), (12, 'R2', 'C');
INSERT INTO more_lines VALUES (20, 'R3'), (21, 'R9');
";

fn joined_catalog() -> ExtractionCatalog {
    ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new(
                "headers",
                [
                    ("id", "BIGINT", false),
                    ("ref", "VARCHAR", false),
                    ("ts", "TIMESTAMP", false),
                ],
            ),
        )
        .with_table(
            "db",
            TableSchema::new(
                "lines",
                [
                    ("id", "BIGINT", false),
                    ("ref", "VARCHAR", false),
                    ("sku", "VARCHAR", false),
                ],
            ),
        )
        .with_table(
            "db",
            TableSchema::new(
                "more_lines",
                [("id", "BIGINT", false), ("ref", "VARCHAR", false)],
            ),
        )
}

#[test]
fn case_4_a_join_agrees_including_the_right_prefixed_collision_column() {
    let fx = Fixture::new(JOINED);
    // `headers` and `lines` both have `id` and `ref`, so the join's output carries the left's
    // under their own names and the right's as `right_id` / `right_ref`.
    let bp = blueprint(
        IdRendering::TypePrefixed,
        vec![
            source("h", "headers"),
            source("l", "lines"),
            Node {
                id: "j".into(),
                label: None,
                op: NodeOp::Join {
                    left: "h".into(),
                    right: "l".into(),
                    on: vec![("ref".into(), "ref".into())],
                },
            },
        ],
        vec![
            single(
                "j",
                "header",
                None,
                Target::Object {
                    object_type: constant("Header"),
                    id: col("id"),
                    timestamp: None,
                    attributes: vec![],
                },
            ),
            single(
                "j",
                "line",
                None,
                Target::Object {
                    object_type: constant("Line"),
                    id: col("right_id"),
                    timestamp: None,
                    attributes: vec![],
                },
            ),
            single(
                "j",
                "header-line",
                None,
                Target::O2O {
                    source: endpoint("id", "Header"),
                    target: endpoint("right_id", "Line"),
                    qualifier: Some(col("sku")),
                },
            ),
        ],
    );
    let run = assert_agrees(&fx, &bp, &joined_catalog());
    assert_eq!(run.extractor.o2o.len(), 3, "{:?}", run.extractor.o2o);
    assert!(run
        .extractor
        .objects
        .contains(&("Line-10".to_string(), "Line".to_string())));
}

#[test]
fn case_4_a_union_agrees_with_the_absent_column_null_filled() {
    let fx = Fixture::new(JOINED);
    // `lines` has `sku`, `more_lines` does not, so the union null-fills it for the second
    // branch. The object id reads `sku` through a Coalesce so the null-fill is observable.
    let bp = blueprint(
        IdRendering::TypePrefixed,
        vec![
            source("l", "lines"),
            source("m", "more_lines"),
            Node {
                id: "u".into(),
                label: None,
                op: NodeOp::Union {
                    inputs: vec!["l".into(), "m".into()],
                },
            },
        ],
        vec![single(
            "u",
            "line",
            None,
            Target::Object {
                object_type: constant("Line"),
                id: ValueExpression::Coalesce {
                    parts: vec![col("sku"), col("ref")],
                },
                timestamp: None,
                attributes: vec![],
            },
        )],
    );
    let run = assert_agrees(&fx, &bp, &joined_catalog());
    assert!(
        run.extractor
            .objects
            .contains(&("Line-R3".to_string(), "Line".to_string())),
        "the branch without `sku` must fall through to `ref`: {:?}",
        run.extractor.objects
    );
    assert_eq!(
        run.extractor.objects.len(),
        5,
        "{:?}",
        run.extractor.objects
    );
}

const SPLITS: &str = "
CREATE TABLE tickets (id BIGINT, parts VARCHAR, pairs VARCHAR, ts TIMESTAMP);
INSERT INTO tickets VALUES
  (1, 'a, b ,,c', 'x=1;y=22', TIMESTAMP '2023-01-01 00:00:00'),
  (2, 'd',        'z=3',      TIMESTAMP '2023-01-02 00:00:00'),
  (3, '',         '',         TIMESTAMP '2023-01-03 00:00:00'),
  (4, NULL,       NULL,       TIMESTAMP '2023-01-04 00:00:00');
";

fn tickets_catalog() -> ExtractionCatalog {
    ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "tickets",
            [
                ("id", "BIGINT", false),
                ("parts", "VARCHAR", true),
                ("pairs", "VARCHAR", true),
                ("ts", "TIMESTAMP", false),
            ],
        ),
    )
}

fn split_blueprint(column: &str, kind: SplitKind) -> Blueprint {
    let mut bp = blueprint(
        IdRendering::TypePrefixed,
        vec![source("t", "tickets")],
        vec![
            single(
                "t",
                "ticket",
                None,
                Target::Event {
                    event_type: constant("Ticket"),
                    id: Some(col("id")),
                    timestamp: ts("ts"),
                    attributes: vec![],
                    objects: vec![],
                },
            ),
            single(
                "t",
                "tags",
                None,
                Target::E2O {
                    event: EventEndpoint {
                        id: col("id"),
                        event_type: Some(constant("Ticket")),
                    },
                    object: ObjectEndpoint {
                        id: col(column),
                        object_type: Some(constant("Tag")),
                        split: Some(SplitSpec { kind, trim: true }),
                    },
                    qualifier: Some(constant("tag")),
                },
            ),
        ],
    );
    // Tags exist only as relation endpoints, so they have to be created rather than dropped.
    bp.on_missing_endpoint = MissingEndpointPolicy::Create;
    bp
}

#[test]
fn case_5_a_delimiter_split_agrees() {
    let fx = Fixture::new(SPLITS);
    let bp = split_blueprint(
        "parts",
        SplitKind::Delimiter {
            delimiter: ",".into(),
        },
    );
    let run = assert_agrees(&fx, &bp, &tickets_catalog());
    assert!(
        run.extractor
            .objects
            .contains(&("Tag-b".to_string(), "Tag".to_string())),
        "trimmed parts must survive: {:?}",
        run.extractor.objects
    );
    // 'a, b ,,c' yields a, b, c (the empty part is dropped) and 'd' yields d.
    assert_eq!(run.extractor.e2o.len(), 4, "{:?}", run.extractor.e2o);
}

#[test]
fn case_5_a_regex_split_agrees() {
    let fx = Fixture::new(SPLITS);
    let bp = split_blueprint(
        "pairs",
        SplitKind::Regex {
            pattern: "([a-z])=([0-9]+)".into(),
        },
    );
    let run = assert_agrees(&fx, &bp, &tickets_catalog());
    // 'x=1;y=22' yields x, 1, y, 22 and 'z=3' yields z, 3.
    assert_eq!(run.extractor.e2o.len(), 6, "{:?}", run.extractor.e2o);
}

#[test]
fn case_5_a_single_group_regex_split_agrees() {
    // One group is its own emission path: the parts come from a single `regexp_extract_all` call
    // rather than a concatenation of one per group.
    let fx = Fixture::new(SPLITS);
    let bp = split_blueprint(
        "pairs",
        SplitKind::Regex {
            pattern: "=([0-9]+)".into(),
        },
    );
    let run = assert_agrees(&fx, &bp, &tickets_catalog());
    // 'x=1;y=22' yields 1, 22 and 'z=3' yields 3.
    assert_eq!(run.extractor.e2o.len(), 3, "{:?}", run.extractor.e2o);
}

#[test]
fn case_5_a_group_free_regex_split_agrees() {
    // No group at all is a third path: every whole match is a part.
    let fx = Fixture::new(SPLITS);
    let bp = split_blueprint(
        "pairs",
        SplitKind::Regex {
            pattern: "[a-z]=[0-9]+".into(),
        },
    );
    let run = assert_agrees(&fx, &bp, &tickets_catalog());
    // 'x=1;y=22' yields x=1, y=22 and 'z=3' yields z=3.
    assert_eq!(run.extractor.e2o.len(), 3, "{:?}", run.extractor.e2o);
}

const ATTRS: &str = "
CREATE TABLE items (id BIGINT, name VARCHAR, price DOUBLE, active BOOLEAN, ts TIMESTAMP);
INSERT INTO items VALUES
  (1, 'widget', 9.5,  true,  TIMESTAMP '2024-01-01 00:00:00'),
  (2, 'gadget', 12.0, false, TIMESTAMP '2024-01-02 00:00:00'),
  (3, NULL,     NULL, NULL,  TIMESTAMP '2024-01-03 00:00:00');
CREATE TABLE price_changes (item BIGINT, price DOUBLE, ts TIMESTAMP);
INSERT INTO price_changes VALUES
  (1, 9.5,  TIMESTAMP '2024-01-01 00:00:00'),
  (1, 10.5, TIMESTAMP '2024-02-01 00:00:00'),
  (2, 12.0, TIMESTAMP '2024-01-02 00:00:00');
";

fn attrs_catalog() -> ExtractionCatalog {
    ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new(
                "items",
                [
                    ("id", "BIGINT", false),
                    ("name", "VARCHAR", true),
                    ("price", "DOUBLE", true),
                    ("active", "BOOLEAN", true),
                    ("ts", "TIMESTAMP", false),
                ],
            ),
        )
        .with_table(
            "db",
            TableSchema::new(
                "price_changes",
                [
                    ("item", "BIGINT", false),
                    ("price", "DOUBLE", false),
                    ("ts", "TIMESTAMP", false),
                ],
            ),
        )
}

#[test]
fn case_6_static_object_attributes_and_typed_event_attributes_agree() {
    let fx = Fixture::new(ATTRS);
    let bp = blueprint(
        IdRendering::TypePrefixed,
        vec![source("items", "items")],
        vec![
            single(
                "items",
                "item",
                None,
                Target::Object {
                    object_type: constant("Item"),
                    id: col("id"),
                    timestamp: None,
                    attributes: vec![
                        AttributeMapping {
                            source_column: "name".into(),
                            name: "name".into(),
                            value_type: None,
                        },
                        AttributeMapping {
                            source_column: "active".into(),
                            name: "active".into(),
                            value_type: Some(OCELAttributeType::Boolean),
                        },
                    ],
                },
            ),
            single(
                "items",
                "listed",
                None,
                Target::Event {
                    event_type: constant("Listed"),
                    id: Some(col("id")),
                    timestamp: ts("ts"),
                    attributes: vec![
                        AttributeMapping {
                            source_column: "price".into(),
                            name: "price".into(),
                            value_type: Some(OCELAttributeType::Float),
                        },
                        AttributeMapping {
                            source_column: "name".into(),
                            name: "name".into(),
                            value_type: None,
                        },
                    ],
                    objects: vec![],
                },
            ),
        ],
    );
    let run = assert_agrees(&fx, &bp, &attrs_catalog());
    assert!(
        run.extractor
            .object_attributes
            .iter()
            .any(|(id, name, _, v)| id == "Item-1" && name == "name" && v == "s:widget"),
        "{:?}",
        run.extractor.object_attributes
    );
    assert!(
        run.extractor
            .event_attributes
            .iter()
            .any(|(id, name, v)| id == "Listed-1" && name == "price" && v == "f:9.5"),
        "{:?}",
        run.extractor.event_attributes
    );
    // A NULL cell is a recorded observation of Null, not an absent attribute.
    assert!(run.extractor.object_attributes.contains(&(
        "Item-3".to_string(),
        "name".to_string(),
        "1970-01-01T00:00:00+00:00".to_string(),
        "<null>".to_string()
    )));
}

#[test]
fn case_6_change_tracked_object_attributes_agree() {
    let fx = Fixture::new(ATTRS);
    let bp = blueprint(
        IdRendering::TypePrefixed,
        vec![source("pc", "price_changes")],
        vec![single(
            "pc",
            "prices",
            None,
            Target::Object {
                object_type: constant("Item"),
                id: col("item"),
                timestamp: Some(ts("ts")),
                attributes: vec![AttributeMapping {
                    source_column: "price".into(),
                    name: "price".into(),
                    value_type: None,
                }],
            },
        )],
    );
    let run = assert_agrees(&fx, &bp, &attrs_catalog());
    assert_eq!(
        run.extractor.object_attributes.len(),
        3,
        "every row is an observation: {:?}",
        run.extractor.object_attributes
    );
    assert!(run.extractor.object_attributes.contains(&(
        "Item-1".to_string(),
        "price".to_string(),
        "2024-02-01T00:00:00+00:00".to_string(),
        "f:10.5".to_string()
    )));
}

#[test]
fn case_7_a_text_literal_against_an_integer_column_selects_the_same_rows() {
    // An editor with a text input emits Literal::Text("2"), and `prepare` coerces it to the
    // column's declared kind, so `id <= "2"` is a numeric comparison in the extractor. An
    // uncoercible literal never reaches either side, since `validate` refuses the blueprint
    // outright, as the assertion at the end of this test pins.
    let fx = Fixture::new(ORDERS);
    let bp = blueprint(
        IdRendering::TypePrefixed,
        vec![source("orders", "orders")],
        vec![
            single(
                "orders",
                "coercible",
                Some(Predicate::Compare {
                    left: Operand::Column {
                        column: "id".into(),
                    },
                    op: CompareOp::Le,
                    right: Operand::Literal {
                        value: Literal::Text("2".into()),
                    },
                }),
                Target::Object {
                    object_type: constant("Small"),
                    id: col("id"),
                    timestamp: None,
                    attributes: vec![],
                },
            ),
            single(
                "orders",
                "uncoercible",
                Some(Predicate::In {
                    column: "id".into(),
                    values: vec![Literal::Text("3".into())],
                }),
                Target::Object {
                    object_type: constant("Listed"),
                    id: col("id"),
                    timestamp: None,
                    attributes: vec![],
                },
            ),
        ],
    );
    let run = assert_agrees(&fx, &bp, &orders_catalog());
    assert_eq!(
        run.extractor
            .objects
            .iter()
            .filter(|(_, t)| t == "Small")
            .count(),
        2,
        "id <= 2 must be a numeric comparison: {:?}",
        run.extractor.objects
    );
    assert_eq!(
        run.extractor
            .objects
            .iter()
            .filter(|(_, t)| t == "Listed")
            .count(),
        1,
        "the text member of the IN list must match the integer row: {:?}",
        run.extractor.objects
    );

    let mut uncoercible = bp;
    uncoercible.mappings.push(single(
        "orders",
        "uncoercible",
        Some(Predicate::In {
            column: "id".into(),
            values: vec![Literal::Text("abc".into())],
        }),
        Target::Object {
            object_type: constant("Other"),
            id: col("id"),
            timestamp: None,
            attributes: vec![],
        },
    ));
    assert!(
        !validate(&uncoercible, &orders_catalog()).is_empty(),
        "a literal no cell of the column can equal is a validation error, not a compile one"
    );
}

const ACTIVITIES: &str = "
CREATE TABLE log (id BIGINT, activity VARCHAR, ts TIMESTAMP);
INSERT INTO log VALUES
  (1, 'Create',  TIMESTAMP '2025-01-01 00:00:00'),
  (2, 'Approve', TIMESTAMP '2025-01-02 00:00:00'),
  (3, 'Create',  TIMESTAMP '2025-01-03 00:00:00');
";

fn log_catalog() -> ExtractionCatalog {
    ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "log",
            [
                ("id", "BIGINT", false),
                ("activity", "VARCHAR", false),
                ("ts", "TIMESTAMP", false),
            ],
        ),
    )
}

fn dynamic_type_blueprint() -> Blueprint {
    blueprint(
        IdRendering::TypePrefixed,
        vec![source("log", "log")],
        vec![single(
            "log",
            "activities",
            None,
            Target::Event {
                event_type: col("activity"),
                id: Some(col("id")),
                timestamp: ts("ts"),
                attributes: vec![],
                objects: vec![],
            },
        )],
    )
}

#[test]
fn case_8_a_dynamic_event_type_with_a_supplied_domain_agrees() {
    let fx = Fixture::new(ACTIVITIES);
    let catalog = log_catalog().with_domain(
        "db",
        "log",
        "activity",
        ["Create".to_string(), "Approve".to_string()],
    );
    let run = assert_agrees(&fx, &dynamic_type_blueprint(), &catalog);
    assert!(
        run.compiled.errors().is_empty(),
        "{:?}",
        run.compiled.errors()
    );
    assert_eq!(run.extractor.events.len(), 3);
    assert!(run
        .compiled
        .relations()
        .iter()
        .any(|v| v.name == "event_Create"));
    assert!(run
        .compiled
        .relations()
        .iter()
        .any(|v| v.name == "event_Approve"));
}

#[test]
fn case_8_the_staleness_probe_fires_once_a_value_outside_the_domain_is_inserted() {
    let fx = Fixture::new(ACTIVITIES);
    let catalog = log_catalog().with_domain(
        "db",
        "log",
        "activity",
        ["Create".to_string(), "Approve".to_string()],
    );
    let bp = dynamic_type_blueprint();
    // Holds before, views included.
    assert_agrees(&fx, &bp, &catalog);

    let compiled = compile(&bp, &catalog, SqlDialect::DuckDb, EmissionShape::PerType);
    let stale = probes_with_sql(&compiled)
        .into_iter()
        .find(|(kind, _)| matches!(kind, ProbeKind::StaleTypeDomain { .. }))
        .map(|(_, sql)| sql)
        .expect("a domain-derived type set must carry a staleness probe");

    fx.con
        .execute_batch("INSERT INTO log VALUES (4, 'Reject', TIMESTAMP '2025-01-04 00:00:00');")
        .expect("insert an out-of-domain value");

    let (_, rows) = query(&fx.con, &stale);
    assert_eq!(
        rows.len(),
        1,
        "the probe must report exactly the value that appeared after compilation"
    );
    assert_eq!(text_of(&rows[0][0]), "Reject");

    // And the views really are missing those events now, which is what the probe guards.
    let sql = from_sql(&fx.con);
    assert_eq!(sql.events.len(), 3, "{:?}", sql.events);
}

// The capability `PerType` cannot have at all: a type read from a column with no domain.
//
// `PerType` needs a domain to name each per-type view; with none supplied it is a
// `RejectReason::DynamicTypeName`. Under `Consolidated` the type is a column value, so there is
// no domain to need.

#[test]
fn a_dynamic_type_with_no_domain_is_a_per_type_reject_but_a_consolidated_pass() {
    let fx = Fixture::new(ACTIVITIES);
    // Unlike case 8's, this catalog never gets a `.with_domain` call.
    let catalog = log_catalog();
    let bp = dynamic_type_blueprint();
    assert_eq!(validate(&bp, &catalog), vec![], "blueprint must validate");

    let per_type = compile(&bp, &catalog, SqlDialect::DuckDb, EmissionShape::PerType);
    assert_eq!(per_type.errors().len(), 1, "{:?}", per_type.errors());
    assert!(
        matches!(
            per_type.errors()[0].reason,
            RejectReason::DynamicTypeName { .. }
        ),
        "{:?}",
        per_type.errors()[0]
    );

    let (consolidated, extractor, sql) = run_consolidated_only(&fx, &bp, &catalog);
    assert_eq!(extractor.events.len(), 3, "{:?}", extractor.events);
    assert!(extractor.events.iter().any(|(_, t, _)| t == "Create"));
    assert!(extractor.events.iter().any(|(_, t, _)| t == "Approve"));
    assert!(
        !consolidated
            .probes()
            .iter()
            .any(|p| matches!(p.kind, ProbeKind::StaleTypeDomain { .. })),
        "no domain means nothing can go stale: {:?}",
        consolidated.probes()
    );
    assert_consolidated_agrees(&extractor, &sql);
}

#[test]
fn a_dynamically_typed_events_attributes_reach_the_wide_events_table() {
    // The attribute plan keys a declaration by its type name, which a `Consolidated` mapping
    // reading its type from a column does not have, so `AttributePlan::dynamic_event_attrs` has
    // to name those columns instead.
    let fx = Fixture::new(ACTIVITIES);
    let catalog = log_catalog();
    let bp = blueprint(
        IdRendering::TypePrefixed,
        vec![source("log", "log")],
        vec![single(
            "log",
            "activities",
            None,
            Target::Event {
                event_type: col("activity"),
                id: Some(col("id")),
                timestamp: ts("ts"),
                attributes: vec![AttributeMapping {
                    source_column: "activity".into(),
                    name: "activity".into(),
                    value_type: None,
                }],
                objects: vec![],
            },
        )],
    );
    let (_, extractor, sql) = run_consolidated_only(&fx, &bp, &catalog);
    assert!(
        extractor
            .event_attributes
            .iter()
            .any(|(_, name, _)| name == "activity"),
        "the fixture must actually declare an event attribute: {:?}",
        extractor.event_attributes
    );
    assert_consolidated_agrees(&extractor, &sql);
}

#[test]
fn case_9_the_same_blueprint_agrees_under_both_emission_shapes() {
    // Reuses case 6's mix of static object attributes, a typed event attribute and a `NULL`
    // observation, where a shape-specific bug is most likely to show.
    let fx = Fixture::new(ATTRS);
    let bp = blueprint(
        IdRendering::TypePrefixed,
        vec![source("items", "items")],
        vec![
            single(
                "items",
                "item",
                None,
                Target::Object {
                    object_type: constant("Item"),
                    id: col("id"),
                    timestamp: None,
                    attributes: vec![AttributeMapping {
                        source_column: "name".into(),
                        name: "name".into(),
                        value_type: None,
                    }],
                },
            ),
            single(
                "items",
                "listed",
                None,
                Target::Event {
                    event_type: constant("Listed"),
                    id: Some(col("id")),
                    timestamp: ts("ts"),
                    attributes: vec![AttributeMapping {
                        source_column: "price".into(),
                        name: "price".into(),
                        value_type: Some(OCELAttributeType::Float),
                    }],
                    objects: vec![],
                },
            ),
        ],
    );
    let run = assert_agrees(&fx, &bp, &attrs_catalog());
    assert!(
        run.compiled.errors().is_empty(),
        "{:?}",
        run.compiled.errors()
    );
    assert!(
        run.consolidated.errors().is_empty(),
        "{:?}",
        run.consolidated.errors()
    );
    // `Null`-valued event attributes are dropped from both sides for the same reason
    // `assert_consolidated_agrees` drops them.
    assert_eq!(run.sql.events, run.consolidated_sql.events);
    assert_eq!(
        drop_null_event_attrs(&run.sql.event_attributes),
        drop_null_event_attrs(&run.consolidated_sql.event_attributes),
        "the two shapes must agree with each other, not merely with the extractor separately"
    );
    assert_eq!(run.sql.objects, run.consolidated_sql.objects);
    assert_eq!(
        run.sql.object_attributes,
        run.consolidated_sql.object_attributes
    );
    assert_eq!(run.sql.e2o, run.consolidated_sql.e2o);
    assert_eq!(run.sql.o2o, run.consolidated_sql.o2o);
    // The relation names genuinely differ, so this is not two views of the same SQL.
    let per_type_names: BTreeSet<&str> = run
        .compiled
        .relations()
        .iter()
        .map(|v| v.name.as_str())
        .collect();
    let consolidated_names: BTreeSet<&str> = run
        .consolidated
        .relations()
        .iter()
        .map(|v| v.name.as_str())
        .collect();
    assert!(
        per_type_names.is_disjoint(&consolidated_names),
        "PerType: {per_type_names:?}, Consolidated: {consolidated_names:?}"
    );
}

fn case_11_blueprint() -> Blueprint {
    blueprint(
        IdRendering::TypePrefixed,
        vec![source("items", "items")],
        vec![
            single(
                "items",
                "item",
                None,
                Target::Object {
                    object_type: constant("Item"),
                    id: col("id"),
                    timestamp: None,
                    attributes: vec![
                        AttributeMapping {
                            source_column: "name".into(),
                            name: "name".into(),
                            value_type: None,
                        },
                        AttributeMapping {
                            source_column: "active".into(),
                            name: "active".into(),
                            value_type: Some(OCELAttributeType::Boolean),
                        },
                    ],
                },
            ),
            single(
                "items",
                "listed",
                None,
                Target::Event {
                    event_type: constant("Listed"),
                    id: Some(col("id")),
                    timestamp: ts("ts"),
                    attributes: vec![AttributeMapping {
                        source_column: "price".into(),
                        name: "price".into(),
                        value_type: Some(OCELAttributeType::Float),
                    }],
                    objects: vec![],
                },
            ),
            single(
                "items",
                "listed-item",
                None,
                Target::E2O {
                    event: EventEndpoint {
                        id: col("id"),
                        event_type: Some(constant("Listed")),
                    },
                    object: endpoint("id", "Item"),
                    qualifier: Some(constant("subject")),
                },
            ),
        ],
    )
}

#[test]
fn case_11_all_four_consolidated_emission_paths_agree() {
    let bp = case_11_blueprint();
    let catalog = attrs_catalog();

    let fx_extract = Fixture::new(ATTRS);
    assert_eq!(validate(&bp, &catalog), vec![], "blueprint must validate");
    let provider = DuckDbRowProvider {
        con: &fx_extract.con,
    };
    let mut providers: HashMap<String, &dyn RowProvider> = HashMap::new();
    providers.insert("db".to_string(), &provider);
    let mut sink = SlimOcelSink::new();
    extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    let extractor = from_extractor(sink.ocel());
    // Sanity: every relation this blueprint touches actually has rows, or the four-way
    // comparison below would be vacuous.
    assert!(!extractor.events.is_empty());
    assert!(!extractor.objects.is_empty());
    assert!(!extractor.e2o.is_empty());
    assert!(!extractor.object_attributes.is_empty());

    let compiled = compile(
        &bp,
        &catalog,
        SqlDialect::DuckDb,
        EmissionShape::Consolidated,
    );
    assert!(compiled.errors().is_empty(), "{:?}", compiled.errors());

    // Path 1: `ddl` (`CREATE VIEW`).
    let fx_ddl = Fixture::new(ATTRS);
    let ddl = compiled.ddl();
    fx_ddl
        .con
        .execute_batch(&ddl)
        .unwrap_or_else(|e| panic!("ddl failed: {e}\n---\n{ddl}"));
    assert_probes_hold(&fx_ddl.con, &compiled, "Consolidated ddl");
    assert_consolidated_agrees(&extractor, &from_sql_consolidated(&fx_ddl.con));

    // Path 2: `materialize_ddl` (`CREATE TABLE ... AS`), against a fresh database with write
    // rights, so a relation referenced more than once is computed once instead of re-inlined.
    let fx_mat = Fixture::new(ATTRS);
    let materialize = compiled.materialize_ddl();
    fx_mat
        .con
        .execute_batch(&materialize)
        .unwrap_or_else(|e| panic!("materialize_ddl failed: {e}\n---\n{materialize}"));
    assert_consolidated_agrees(&extractor, &from_sql_consolidated(&fx_mat.con));

    // Path 3: `with_prelude`, every relation bound as a CTE in front of a read-only analysis
    // query, so it runs with no `CREATE` right at all.
    let fx_pre = Fixture::new(ATTRS);
    let analysis = "SELECT \
         (SELECT count(*) FROM events) AS n_events, \
         (SELECT count(*) FROM objects) AS n_objects, \
         (SELECT count(*) FROM e2o) AS n_e2o, \
         (SELECT count(*) FROM object_attribute_changes) AS n_object_attrs";
    let (_, rows) = query(&fx_pre.con, &compiled.with_prelude(analysis));
    assert_eq!(rows.len(), 1);
    let as_i64 = |v: &Value| match v {
        Value::Integer(i) => *i,
        other => panic!("expected an integer count, got {other:?}"),
    };
    assert_eq!(
        as_i64(&rows[0][0]),
        extractor.events.len() as i64,
        "with_prelude: n_events"
    );
    assert_eq!(
        as_i64(&rows[0][1]),
        extractor.objects.len() as i64,
        "with_prelude: n_objects"
    );
    assert_eq!(
        as_i64(&rows[0][2]),
        extractor.e2o.len() as i64,
        "with_prelude: n_e2o"
    );
    assert_eq!(
        as_i64(&rows[0][3]),
        extractor.object_attributes.len() as i64,
        "with_prelude: n_object_attrs"
    );

    // Path 4: the probes, run view-free (`probe_statements`) against yet another fresh
    // connection, so an already-materialized relation cannot be hiding a probe bug.
    let fx_probes = Fixture::new(ATTRS);
    for (kind, sql) in probes_with_sql(&compiled) {
        let (_, rows) = query(&fx_probes.con, &sql);
        assert!(
            rows.is_empty(),
            "probe {kind:?} must hold view-free too: {} rows\n{sql}",
            rows.len()
        );
    }
}

#[test]
fn case_10_a_skipped_mapping_is_reported_and_its_entities_are_the_only_difference() {
    let fx = Fixture::new(ORDERS);
    let bp = blueprint(
        IdRendering::TypePrefixed,
        vec![source("orders", "orders")],
        vec![
            order_object(),
            // No `id`, so the extractor mints a UUID per row: nondeterministic, and refused.
            single(
                "orders",
                "minted",
                None,
                Target::Event {
                    event_type: constant("Placed"),
                    id: None,
                    timestamp: ts("ts"),
                    attributes: vec![],
                    objects: vec![],
                },
            ),
        ],
    );
    let run = run_both_expecting_errors(&fx, &bp, &orders_catalog());

    assert_eq!(
        run.compiled.errors().len(),
        1,
        "{:?}",
        run.compiled.errors()
    );
    let err = &run.compiled.errors()[0];
    assert!(matches!(
        err.reason,
        RejectReason::SynthesizedId { field: "id" }
    ));
    assert_eq!(
        err.mapping.as_ref().and_then(|m| m.label.clone()),
        Some("minted".to_string())
    );

    // The difference is exactly the skipped mapping's events, demonstrated rather than hidden.
    assert_eq!(run.extractor.events.len(), 4, "{:?}", run.extractor.events);
    assert!(run.extractor.events.iter().all(|(_, t, _)| t == "Placed"));
    assert!(
        run.sql.events.is_empty(),
        "the skipped mapping's events must be absent from the SQL: {:?}",
        run.sql.events
    );
    // Everything else still agrees.
    assert_eq!(run.extractor.objects, run.sql.objects);
    assert_eq!(run.extractor.e2o, run.sql.e2o);
    assert_eq!(run.extractor.o2o, run.sql.o2o);

    // `Consolidated` skips the same mapping for the same reason: `SynthesizedId` is decided
    // before any shape-specific logic runs, so it shows the identical difference.
    assert_eq!(
        run.consolidated.errors().len(),
        1,
        "{:?}",
        run.consolidated.errors()
    );
    assert!(matches!(
        run.consolidated.errors()[0].reason,
        RejectReason::SynthesizedId { field: "id" }
    ));
    assert!(
        run.consolidated_sql.events.is_empty(),
        "{:?}",
        run.consolidated_sql.events
    );
    assert_eq!(run.extractor.objects, run.consolidated_sql.objects);
    assert_eq!(run.extractor.e2o, run.consolidated_sql.e2o);
    assert_eq!(run.extractor.o2o, run.consolidated_sql.o2o);
}

const CROSS_KIND: &str = "
CREATE TABLE left_side (k VARCHAR);
CREATE TABLE right_side (k BIGINT);
CREATE TABLE right_text (k VARCHAR);
INSERT INTO left_side VALUES ('1'), ('2');
INSERT INTO right_side VALUES (1), (2);
INSERT INTO right_text VALUES ('1'), ('2');
";

fn cross_kind_catalog() -> ExtractionCatalog {
    ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new("left_side", [("k", "VARCHAR", false)]),
        )
        .with_table(
            "db",
            TableSchema::new("right_side", [("k", "BIGINT", false)]),
        )
        .with_table(
            "db",
            TableSchema::new("right_text", [("k", "VARCHAR", false)]),
        )
}

/// `left_side` joined to `right` on `k`, with every matched row becoming a `Joined` object.
fn join_blueprint(right: &str) -> Blueprint {
    blueprint(
        IdRendering::TypePrefixed,
        vec![
            source("l", "left_side"),
            source("r", right),
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
        vec![single(
            "j",
            "joined",
            None,
            Target::Object {
                object_type: constant("Joined"),
                id: col("k"),
                timestamp: None,
                attributes: vec![],
            },
        )],
    )
}

#[test]
fn a_same_kind_join_key_does_produce_rows() {
    // The control for `a_text_versus_number_join_key_produces_no_rows_on_both_sides`, whose
    // emptiness assertions would also hold against a compiler emitting `ON FALSE` for every join.
    let fx = Fixture::new(CROSS_KIND);
    let run = assert_agrees(&fx, &join_blueprint("right_text"), &cross_kind_catalog());
    assert_eq!(
        run.extractor.objects.len(),
        2,
        "{:?}",
        run.extractor.objects
    );
    assert_eq!(run.sql.objects.len(), 2, "{:?}", run.sql.objects);
}

#[test]
fn a_text_versus_number_join_key_produces_no_rows_on_both_sides() {
    // `graph.rs` keys each join column through `Value::join_key_part`, which tags the runtime
    // value's kind: `s:1` never matches `n:1`. DuckDB would implicit-cast and join them, so a
    // bare `l.k = r.k` would make the view keep two rows the extractor refuses.
    let fx = Fixture::new(CROSS_KIND);
    let run = assert_agrees(&fx, &join_blueprint("right_side"), &cross_kind_catalog());
    assert!(
        run.extractor.objects.is_empty(),
        "the extractor's kind-tagged keys must not match: {:?}",
        run.extractor.objects
    );
    assert!(
        run.sql.objects.is_empty(),
        "and the compiled join must say so rather than inherit DuckDB's implicit cast: {:?}",
        run.sql.objects
    );
}

/// Serialises the panic-hook swap [`comparison_rejects`] performs, so two of these running
/// concurrently cannot leave the suppressing hook installed for a genuine failure elsewhere.
static HOOK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Whether `compare`, one of the real comparison entry points, rejects a deliberately tampered
/// log.
///
/// The default panic hook is suppressed for the duration: these panics are the expected result,
/// and printing six backtraces during a passing run would make a green suite look broken.
fn comparison_rejects(compare: impl FnOnce() + std::panic::UnwindSafe) -> bool {
    let _guard = HOOK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = std::panic::catch_unwind(compare);
    std::panic::set_hook(previous);
    outcome.is_err()
}

/// One deliberate corruption of a compiled log, applied to a single [`LogSets`] field.
type Perturbation = fn(&mut LogSets);

/// A blueprint filling all six [`LogSets`] fields at once, so a tampering test can perturb each
/// one in turn against a log that genuinely carries it.
fn every_relation_blueprint() -> Blueprint {
    blueprint(
        IdRendering::TypePrefixed,
        vec![source("items", "items")],
        vec![
            single(
                "items",
                "item",
                None,
                Target::Object {
                    object_type: constant("Item"),
                    id: col("id"),
                    timestamp: None,
                    attributes: vec![AttributeMapping {
                        source_column: "name".into(),
                        name: "name".into(),
                        value_type: None,
                    }],
                },
            ),
            single(
                "items",
                "label",
                None,
                Target::Object {
                    object_type: constant("Label"),
                    id: col("name"),
                    timestamp: None,
                    attributes: vec![],
                },
            ),
            single(
                "items",
                "listed",
                None,
                Target::Event {
                    event_type: constant("Listed"),
                    id: Some(col("id")),
                    timestamp: ts("ts"),
                    attributes: vec![AttributeMapping {
                        source_column: "price".into(),
                        name: "price".into(),
                        value_type: Some(OCELAttributeType::Float),
                    }],
                    objects: vec![],
                },
            ),
            single(
                "items",
                "listed-item",
                None,
                Target::E2O {
                    event: EventEndpoint {
                        id: col("id"),
                        event_type: Some(constant("Listed")),
                    },
                    object: endpoint("id", "Item"),
                    qualifier: Some(constant("subject")),
                },
            ),
            single(
                "items",
                "item-label",
                None,
                Target::O2O {
                    source: endpoint("id", "Item"),
                    target: endpoint("name", "Label"),
                    qualifier: Some(constant("named")),
                },
            ),
        ],
    )
}

#[test]
fn the_harness_notices_when_the_two_logs_disagree() {
    // Drives the real comparison entry points against a log tampered in one field at a time, so
    // every `assert_eq!` inside `assert_log_sets_agree` is individually load-bearing.
    let fx = Fixture::new(ATTRS);
    let run = assert_agrees(&fx, &every_relation_blueprint(), &attrs_catalog());

    // Tampering only means something against a field that actually carries rows.
    assert!(!run.extractor.events.is_empty());
    assert!(!run.extractor.event_attributes.is_empty());
    assert!(!run.extractor.objects.is_empty());
    assert!(!run.extractor.object_attributes.is_empty());
    assert!(!run.extractor.e2o.is_empty());
    assert!(!run.extractor.o2o.is_empty());

    let extractor = &run.extractor;
    let perturbations: Vec<(&str, Perturbation)> = vec![
        ("events", |s| {
            s.events.insert((
                "Listed-999".into(),
                "Listed".into(),
                "2024-09-09T00:00:00+00:00".into(),
            ));
        }),
        ("event attributes", |s| {
            s.event_attributes
                .insert(("Listed-1".into(), "price".into(), "f:999.0".into()));
        }),
        ("objects", |s| {
            s.objects.insert(("Item-999".into(), "Item".into()));
        }),
        ("object attributes", |s| {
            s.object_attributes.insert((
                "Item-1".into(),
                "name".into(),
                "1970-01-01T00:00:00+00:00".into(),
                "s:tampered".into(),
            ));
        }),
        ("E2O", |s| {
            s.e2o
                .insert(("Listed-1".into(), "Item-999".into(), "subject".into()));
        }),
        ("O2O", |s| {
            s.o2o
                .insert(("Item-1".into(), "Label-999".into(), "named".into()));
        }),
    ];
    for (field, perturb) in perturbations {
        let mut tampered = run.sql.clone();
        perturb(&mut tampered);
        assert!(
            comparison_rejects(|| assert_per_type_agrees(extractor, &tampered)),
            "assert_per_type_agrees accepted a log whose {field} were tampered with"
        );
    }

    // `assert_consolidated_agrees` compares event attributes through a different `assert_eq!`,
    // the `drop_null_event_attributes` branch, so it needs its own case.
    let mut tampered = run.consolidated_sql.clone();
    tampered
        .event_attributes
        .insert(("Listed-1".into(), "price".into(), "f:999.0".into()));
    assert!(
        comparison_rejects(|| assert_consolidated_agrees(extractor, &tampered)),
        "assert_consolidated_agrees accepted a tampered non-null event attribute"
    );

    // And the carve-out really is a carve-out, not a hole: a `Null`-valued extra observation is
    // rejected by `PerType`'s comparison and deliberately tolerated by `Consolidated`'s.
    let mut null_valued = run.sql.clone();
    null_valued
        .event_attributes
        .insert(("Listed-1".into(), "invented".into(), "<null>".into()));
    assert!(
        comparison_rejects(|| assert_per_type_agrees(extractor, &null_valued)),
        "PerType compares event attributes in full, `Null` observations included"
    );
}

const AMBIGUOUS: &str = "
CREATE TABLE dup (id BIGINT, ts TIMESTAMP, note VARCHAR);
INSERT INTO dup VALUES
  (1, TIMESTAMP '2020-01-01 00:00:00', 'first'),
  (1, TIMESTAMP '2020-01-02 00:00:00', 'second');
";

fn ambiguous_catalog() -> ExtractionCatalog {
    ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "dup",
            [
                ("id", "BIGINT", false),
                ("ts", "TIMESTAMP", false),
                ("note", "VARCHAR", true),
            ],
        ),
    )
}

#[test]
fn every_probe_kind_is_shown_to_fire() {
    // `assert_probes_hold` only asserts a probe returns nothing, which a probe selecting nothing
    // at all satisfies vacuously. This drives each `ProbeKind` into returning rows on a fixture
    // that genuinely violates what it guards.
    //
    // Not `run_both`: the extractor answers these inputs with an `IdTypeCollision` or a
    // first-row-wins choice, which `run_both` refuses to compare across.
    let fx = Fixture::new(AMBIGUOUS);
    let bp = blueprint(
        // `Raw`, so the two object mappings really do claim the same id under two types.
        IdRendering::Raw,
        vec![source("dup", "dup")],
        vec![
            single(
                "dup",
                "as-a",
                None,
                Target::Object {
                    object_type: constant("A"),
                    id: col("id"),
                    timestamp: None,
                    // Two rows, one id, different values: `AmbiguousStaticObjectAttributes`.
                    attributes: vec![AttributeMapping {
                        source_column: "note".into(),
                        name: "note".into(),
                        value_type: Some(OCELAttributeType::String),
                    }],
                },
            ),
            single(
                "dup",
                "as-b",
                None,
                Target::Object {
                    object_type: constant("B"),
                    id: col("id"),
                    timestamp: None,
                    attributes: vec![],
                },
            ),
            single(
                "dup",
                "e1",
                None,
                Target::Event {
                    event_type: constant("E1"),
                    id: Some(col("id")),
                    timestamp: ts("ts"),
                    attributes: vec![],
                    objects: vec![],
                },
            ),
            single(
                "dup",
                "e2",
                None,
                Target::Event {
                    event_type: constant("E2"),
                    id: Some(col("id")),
                    timestamp: ts("ts"),
                    attributes: vec![],
                    objects: vec![],
                },
            ),
        ],
    );
    let catalog = ambiguous_catalog();
    assert_eq!(validate(&bp, &catalog), vec![], "blueprint must validate");

    let compiled = compile(&bp, &catalog, SqlDialect::DuckDb, EmissionShape::PerType);
    assert!(compiled.errors().is_empty(), "{:?}", compiled.errors());
    let ddl = compiled.ddl();
    fx.con
        .execute_batch(&ddl)
        .unwrap_or_else(|e| panic!("ddl failed: {e}\n---\n{ddl}"));

    // The fourth `ProbeKind`, `StaleTypeDomain`, is driven into firing by
    // `case_8_the_staleness_probe_fires_once_a_value_outside_the_domain_is_inserted`.
    let firing = firing_probe_kinds(&fx.con, &compiled);
    for expected in [
        "AmbiguousObjectIdentity",
        "AmbiguousEventIdentity",
        "AmbiguousStaticObjectAttributes",
    ] {
        assert!(
            firing.contains(expected),
            "probe {expected} never fires, so asserting it returns zero rows proves nothing: \
             firing = {firing:?}"
        );
    }

    // The extractor really does disagree here: the probes announce a genuine divergence.
    let provider = DuckDbRowProvider { con: &fx.con };
    let mut providers: HashMap<String, &dyn RowProvider> = HashMap::new();
    providers.insert("db".to_string(), &provider);
    let mut sink = SlimOcelSink::new();
    let report = extract(&bp, &catalog, &providers, &mut sink).expect("extract");
    assert!(
        report
            .errors
            .iter()
            .any(|e| matches!(e, ExtractionError::IdTypeCollision { .. })),
        "{:?}",
        report.errors
    );
    assert_ne!(
        from_extractor(sink.ocel()).objects,
        from_sql(&fx.con).objects,
        "the extractor drops the losing object and the views keep it, which is what \
         AmbiguousObjectIdentity announces"
    );
}

const BOOL_FLAGS: &str = "
CREATE TABLE flags (order_id INTEGER, is_cancelled BOOLEAN);
INSERT INTO flags VALUES
  (5, NULL),
  (6, false),
  (7, true);
";

fn bool_flags_catalog() -> ExtractionCatalog {
    ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "flags",
            [
                ("order_id", "INTEGER", false),
                ("is_cancelled", "BOOLEAN", true),
            ],
        ),
    )
}

#[test]
fn a_null_boolean_in_a_template_id_drops_the_row_rather_than_rendering_false() {
    // `render_template` returns `None` as soon as a placeholder has no `Value::canonical_string`,
    // and `Value::Null` has none, so row 5 has no id and the extractor drops it. A
    // `CASE WHEN <col> THEN 'true' ELSE 'false' END` would take the ELSE branch for `NULL` and
    // mint the id `5-false`, past the caller's `IS NOT NULL` and `<> ''` guards.
    let fx = Fixture::new(BOOL_FLAGS);
    let bp = blueprint(
        IdRendering::TypePrefixed,
        vec![source("flags", "flags")],
        vec![single(
            "flags",
            "order",
            None,
            Target::Object {
                object_type: constant("Order"),
                id: ValueExpression::Template {
                    template: "{order_id}-{is_cancelled}".to_string(),
                },
                timestamp: None,
                attributes: vec![],
            },
        )],
    );
    let run = assert_agrees(&fx, &bp, &bool_flags_catalog());
    let ids: BTreeSet<&str> = run
        .extractor
        .objects
        .iter()
        .map(|(id, _)| id.as_str())
        .collect();
    assert_eq!(
        ids,
        BTreeSet::from(["Order-6-false", "Order-7-true"]),
        "row 5's NULL flag must render no id at all"
    );
}

#[test]
fn matches_against_a_null_boolean_column_is_false_rather_than_matching_false() {
    // `Predicate::Matches` reads the cell through `Value::display_string().is_some_and(..)`, and
    // `Value::Null`'s is `None`, so row 5 does not match `^false$`. Rendering the column as the
    // literal text `false` first would make it match, keeping a row the extractor drops.
    let fx = Fixture::new(BOOL_FLAGS);
    let bp = blueprint(
        IdRendering::TypePrefixed,
        vec![source("flags", "flags")],
        vec![single(
            "flags",
            "cancelled",
            Some(Predicate::Matches {
                column: "is_cancelled".to_string(),
                regex: "^false$".to_string(),
            }),
            Target::Object {
                object_type: constant("NotCancelled"),
                id: col("order_id"),
                timestamp: None,
                attributes: vec![],
            },
        )],
    );
    let run = assert_agrees(&fx, &bp, &bool_flags_catalog());
    assert_eq!(
        run.extractor
            .objects
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["NotCancelled-6"]),
        "only the genuine `false` row matches"
    );
}

// C2: a naive TIMESTAMP/DATE column read in a predicate or a join key, under a session zone
// that is not UTC.
//
// Every other fixture pins `TimeZone='UTC'`, under which an unanchored `src."ts"` and an anchored
// `timezone('UTC', CAST(src."ts" AS TIMESTAMP))` agree by accident. These run at
// `America/New_York` so that accident is removed.

const NAIVE_TIMES: &str = "
CREATE TABLE evs (id BIGINT, ts TIMESTAMP, d DATE);
INSERT INTO evs VALUES
  (1, TIMESTAMP '2020-01-01 12:00:00', DATE '2020-01-01'),
  (2, TIMESTAMP '2020-01-02 12:00:00', DATE '2020-01-02');
";

fn naive_times_catalog() -> ExtractionCatalog {
    ExtractionCatalog::new().with_table(
        "db",
        TableSchema::new(
            "evs",
            [
                ("id", "BIGINT", false),
                ("ts", "TIMESTAMP", false),
                ("d", "DATE", false),
            ],
        ),
    )
}

/// One object per row of `evs` surviving `when`.
fn naive_times_blueprint(when: Predicate) -> Blueprint {
    blueprint(
        IdRendering::TypePrefixed,
        vec![source("evs", "evs")],
        vec![single(
            "evs",
            "kept",
            Some(when),
            Target::Object {
                object_type: constant("Kept"),
                id: col("id"),
                timestamp: None,
                attributes: vec![],
            },
        )],
    )
}

#[test]
fn a_naive_timestamp_compared_in_a_predicate_is_read_as_utc_not_in_the_session_zone() {
    // The extractor's providers report a naive column as a `Value::Timestamp` at UTC, so
    // `2020-01-01 12:00:00` is 12:00Z and is not after 12:30Z. Emitting a bare `src."ts"`
    // hands DuckDB a naive TIMESTAMP next to a TIMESTAMPTZ, which it promotes using the session
    // zone: at America/New_York that reads 17:00Z, which is after 12:30Z, and the view keeps a
    // row the extractor drops.
    let fx = Fixture::with_time_zone(NAIVE_TIMES, "America/New_York");
    let bp = naive_times_blueprint(Predicate::Compare {
        left: Operand::Column {
            column: "ts".to_string(),
        },
        op: CompareOp::Gt,
        right: Operand::Literal {
            value: Literal::Text("2020-01-01T12:30:00Z".to_string()),
        },
    });
    let run = assert_agrees(&fx, &bp, &naive_times_catalog());
    assert_eq!(
        run.extractor
            .objects
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["Kept-2"]),
        "row 1 is 12:00Z, which is not after 12:30Z"
    );
}

#[test]
fn a_naive_timestamp_in_an_in_list_is_read_as_utc_not_in_the_session_zone() {
    // `Predicate::In` has its own emission path (`in_sql`), which read the column just as bare as
    // `operand_sql` did.
    let fx = Fixture::with_time_zone(NAIVE_TIMES, "America/New_York");
    let bp = naive_times_blueprint(Predicate::In {
        column: "ts".to_string(),
        values: vec![Literal::Text("2020-01-01T12:00:00Z".to_string())],
    });
    let run = assert_agrees(&fx, &bp, &naive_times_catalog());
    assert_eq!(
        run.extractor
            .objects
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["Kept-1"]),
        "row 1's instant is exactly the literal"
    );
}

#[test]
fn a_naive_date_compared_in_a_predicate_is_read_as_utc_not_in_the_session_zone() {
    // `ColumnSchema::declared_kind` maps DATE to `ValueKind::Timestamp` too, so a DATE column
    // reaches the identical emission path and shifts the identical way.
    let fx = Fixture::with_time_zone(NAIVE_TIMES, "America/New_York");
    let bp = naive_times_blueprint(Predicate::Compare {
        left: Operand::Column {
            column: "d".to_string(),
        },
        op: CompareOp::Lt,
        right: Operand::Literal {
            value: Literal::Text("2020-01-01T02:00:00Z".to_string()),
        },
    });
    let run = assert_agrees(&fx, &bp, &naive_times_catalog());
    assert_eq!(
        run.extractor
            .objects
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["Kept-1"]),
        "row 1's date is UTC midnight, which is before 02:00Z. Read in the session zone it \
         would be 05:00Z and the view would drop the row. The boundary sits deliberately \
         between the two readings"
    );
}

const MIXED_TZ_JOIN: &str = "
CREATE TABLE naive_side (k TIMESTAMP, tag VARCHAR);
CREATE TABLE tz_side (k TIMESTAMPTZ, label VARCHAR);
INSERT INTO naive_side VALUES (TIMESTAMP '2020-06-01 12:00:00', 'n');
INSERT INTO tz_side VALUES (TIMESTAMPTZ '2020-06-01 12:00:00+00', 'utc-noon');
";

#[test]
fn a_naive_to_tz_join_key_matches_on_the_instant_not_on_the_session_reading() {
    // `graph.rs` keys both sides through `Value::join_key_part`, which sees two `Value::Timestamp`
    // at the same instant and pairs the rows. A bare `l.k = r.k` promotes the naive side using
    // the session zone (16:00Z at America/New_York in June) and the join finds nothing.
    let fx = Fixture::with_time_zone(MIXED_TZ_JOIN, "America/New_York");
    let catalog = ExtractionCatalog::new()
        .with_table(
            "db",
            TableSchema::new(
                "naive_side",
                [("k", "TIMESTAMP", false), ("tag", "VARCHAR", false)],
            ),
        )
        .with_table(
            "db",
            TableSchema::new(
                "tz_side",
                [("k", "TIMESTAMPTZ", false), ("label", "VARCHAR", false)],
            ),
        );
    let bp = blueprint(
        IdRendering::TypePrefixed,
        vec![
            source("n", "naive_side"),
            source("t", "tz_side"),
            Node {
                id: "j".into(),
                label: None,
                op: NodeOp::Join {
                    left: "n".into(),
                    right: "t".into(),
                    on: vec![("k".into(), "k".into())],
                },
            },
        ],
        vec![single(
            "j",
            "paired",
            None,
            Target::Object {
                object_type: constant("Paired"),
                id: col("label"),
                timestamp: None,
                attributes: vec![],
            },
        )],
    );
    let run = assert_agrees(&fx, &bp, &catalog);
    assert_eq!(
        run.extractor
            .objects
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["Paired-utc-noon"]),
        "the two keys are the same instant, so the join must pair them"
    );
}
