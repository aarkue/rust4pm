//! Executes the flat node graph, holding as little of it in memory as each node allows.
//!
//! `Source`, `Filter` and `Union` nodes hold a single row at a time. A `Join` has to hold one of
//! its inputs in full while the other is scanned past it; it holds the right input and streams
//! the left, so put the smaller table on the right. If both join inputs read the same source,
//! [`super::pushdown`] lets that source perform the join and neither side is held.
//!
//! Nothing is cached between nodes: a node read by two consumers is executed twice. See
//! [`GraphExecutor::stream`].
//!
//! A join resolves both key column sets and all output columns before reading a row; anything that
//! does not resolve is an error rather than a skipped key or a null fill.
//!
//! A [`Union`](super::blueprint::NodeOp::Union) concatenates its inputs' rows keeping duplicates
//! (`UNION ALL`). Its output columns are the union of its inputs' column names, and an input
//! lacking one contributes `Null` for it.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fmt::Write;
use std::ops::ControlFlow;

use super::blueprint::{Blueprint, NodeOp};
use super::catalog::{Catalog, TableSchema};
use super::compile::RejectReason;
use super::predicate::PreparedPredicate;
use super::provider::{ProviderError, RowProvider};
use super::pushdown;
use super::report::ExtractionError;
use super::row::{build_column_index, Row};
use super::schema::{
    demanded_columns, full_node_schemas, join_column_source, projected_schema, JoinSide,
};
use super::validate::ValidationError;
use super::value::Value;
use super::Mapping;

/// A hash join's build side: its right input, held in full while the left streams past it.
///
/// Two things are cut before a row lands here: its columns are narrowed to what the join's output
/// reads from the right (key columns are already in the key), and a row whose key is `Null` is
/// dropped, since no left row can match it.
#[derive(Debug, Default)]
struct BuildSide {
    /// Right rows grouped by join key (see [`JoinKeyBuf::render`]). A key with several rows is a
    /// many-to-many join, and each is paired with every matching left row.
    by_key: HashMap<String, Vec<Vec<Value>>>,
    /// How many rows this table holds, for [`GraphExecutor::rows_materialized`].
    rows: u64,
}

/// Where one output column of a `Join` comes from.
enum ColSource {
    /// A position in the left input's row, which is read directly as it streams past.
    Left(usize),
    /// A position in a [`BuildSide`] row, not in the right input's row, which is wider.
    Right(usize),
}

/// Runs a blueprint's node graph against a set of providers.
///
/// Owns each node's execution-time schema (see [`super::schema`]) and every `Filter`'s prepared
/// predicate, resolved once up front so row-by-row evaluation re-parses nothing. No node's rows
/// are cached, so this struct's size is a function of the blueprint, not of the data.
pub(crate) struct GraphExecutor<'a> {
    blueprint: &'a Blueprint,
    providers: &'a HashMap<String, &'a dyn RowProvider>,
    schemas: HashMap<&'a str, TableSchema>,
    /// Every node's full column shape, before projection. A `Join` needs both its inputs' full
    /// schemas to route an output column back to the side it came from, not their projected ones.
    /// See [`join_column_source`].
    full: HashMap<&'a str, TableSchema>,
    prepared_filters: HashMap<&'a str, PreparedPredicate>,
    /// See [`Self::rows_materialized`].
    rows_materialized: Cell<u64>,
    /// See [`Self::take_pushdown_rejections`].
    pushdown_rejections: RefCell<Vec<(String, RejectReason)>>,
}

impl<'a> GraphExecutor<'a> {
    /// Resolve every node's execution schema and prepare every `Filter`'s predicate.
    pub(crate) fn new(
        blueprint: &'a Blueprint,
        catalog: &dyn Catalog,
        providers: &'a HashMap<String, &'a dyn RowProvider>,
        mappings: &[(String, Mapping)],
    ) -> Result<Self, ExtractionError> {
        let full = full_node_schemas(blueprint, catalog);
        let demand = demanded_columns(blueprint, &full, mappings);
        let mut schemas: HashMap<&str, TableSchema> = HashMap::new();
        for node in &blueprint.nodes {
            let full_schema = full
                .get(node.id.as_str())
                .cloned()
                .unwrap_or_else(|| TableSchema {
                    name: node.id.clone(),
                    columns: std::collections::BTreeMap::new(),
                });
            let want = demand.get(node.id.as_str()).cloned().unwrap_or_default();
            schemas.insert(
                node.id.as_str(),
                projected_schema(&full_schema, &want, &node.id),
            );
        }

        let mut prepared_filters = HashMap::new();
        for node in &blueprint.nodes {
            if let NodeOp::Filter { input, condition } = &node.op {
                let input_schema = schemas.get(input.as_str());
                let prepared =
                    condition
                        .prepare(input_schema)
                        .map_err(|e| ExtractionError::InvalidRegex {
                            pattern: format!("filter '{}'", node.id),
                            message: e.to_string(),
                        })?;
                prepared_filters.insert(node.id.as_str(), prepared);
            }
        }

        Ok(Self {
            blueprint,
            providers,
            schemas,
            full,
            prepared_filters,
            rows_materialized: Cell::new(0),
            pushdown_rejections: RefCell::new(Vec::new()),
        })
    }

    /// Total rows this executor has put in memory, summed over every hash join's [`BuildSide`].
    ///
    /// Zero for a graph of `Source`, `Filter` and `Union` nodes, and for a `Join` pushed down to
    /// its source. A running total, not a peak. See
    /// [`ExtractionReport::rows_materialized`](super::report::ExtractionReport::rows_materialized).
    pub(crate) fn rows_materialized(&self) -> u64 {
        self.rows_materialized.get()
    }

    /// Every node the emitter refused to push down, paired with the reason it gave, leaving the
    /// executor's own list empty.
    pub(crate) fn take_pushdown_rejections(&self) -> Vec<(String, RejectReason)> {
        std::mem::take(&mut self.pushdown_rejections.borrow_mut())
    }

    /// The execution-time (projected) schema of `node_id`'s output rows.
    pub(crate) fn schema_of(&self, node_id: &str) -> Option<&TableSchema> {
        self.schemas.get(node_id)
    }

    fn column_names(&self, node_id: &str) -> Vec<String> {
        self.schemas
            .get(node_id)
            .map(|s| s.columns.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Call `on_row` once per row of `node_id`'s output.
    ///
    /// Every node but a locally executed `Join` passes its rows straight through, one at a time,
    /// with no intermediate buffer. A `Join` holds its right input and streams its left past it
    /// (see [`Self::hash_join`]), unless [`Self::push_down`] can hand the whole thing to the
    /// source.
    ///
    /// Nothing here is cached, so a `Union` feeding two mappings scans everything beneath it once
    /// per consumer. Caching would make memory a function of the data, where rescanning makes time
    /// a function of the consumer count, which is a property of the blueprint.
    pub(crate) fn stream(
        &self,
        node_id: &str,
        on_row: &mut dyn FnMut(&[Value]) -> Result<(), ExtractionError>,
    ) -> Result<(), ExtractionError> {
        let node = self
            .blueprint
            .node(node_id)
            .expect("node id resolved from a validated blueprint");
        match &node.op {
            NodeOp::Source { source_id, table } => {
                let provider = self.providers.get(source_id).copied().ok_or_else(|| {
                    ExtractionError::MissingProvider {
                        source_id: source_id.clone(),
                    }
                })?;
                let names = self.column_names(node_id);
                let refs: Vec<&str> = names.iter().map(String::as_str).collect();
                let mut first_err: Option<ExtractionError> = None;
                provider
                    .scan(table, &refs, &mut |vals| match on_row(vals) {
                        Ok(()) => ControlFlow::Continue(()),
                        Err(e) => {
                            // Abandon the scan: the error is fatal, so reading the rest of the
                            // table would be pure waste.
                            first_err = Some(e);
                            ControlFlow::Break(())
                        }
                    })
                    .map_err(|e| ExtractionError::Provider {
                        node: node_id.to_string(),
                        source: e,
                    })?;
                match first_err {
                    Some(e) => Err(e),
                    None => Ok(()),
                }
            }
            NodeOp::Filter { input, .. } => {
                // Safe only because a `Filter`'s demand equals its input's, so the row forwarded
                // verbatim below is as wide as the schema its consumers index it with. See
                // `schema::demanded_columns`.
                let names = self.column_names(input);
                let refs: Vec<&str> = names.iter().map(String::as_str).collect();
                let index = build_column_index(&refs);
                let prepared = self
                    .prepared_filters
                    .get(node_id)
                    .expect("every Filter node has a prepared predicate");
                self.stream(input, &mut |vals| {
                    let row = Row {
                        values: vals,
                        index: &index,
                    };
                    if prepared.evaluate(&row) {
                        on_row(vals)
                    } else {
                        Ok(())
                    }
                })
            }
            NodeOp::Union { inputs } => {
                let out_cols = self.column_names(node_id);
                for input in inputs {
                    let in_cols = self.column_names(input);
                    // Resolved once per input, not per row. `None` is a column this input does
                    // not have, and gets the null fill.
                    let positions: Vec<Option<usize>> = out_cols
                        .iter()
                        .map(|c| in_cols.iter().position(|ic| ic == c))
                        .collect();
                    let mut buf = vec![Value::Null; out_cols.len()];
                    self.stream(input, &mut |vals| {
                        for (slot, position) in buf.iter_mut().zip(&positions) {
                            *slot = match position {
                                Some(i) => vals[*i].clone(),
                                None => Value::Null,
                            };
                        }
                        on_row(&buf)
                    })?;
                }
                Ok(())
            }
            NodeOp::Join { left, right, on } => {
                if self.push_down(node_id, on_row)? {
                    return Ok(());
                }
                self.hash_join(node_id, left, right, on, on_row)
            }
        }
    }

    /// Ask `node_id`'s source to execute the whole node, join and filters included, and stream
    /// the result back, holding nothing here.
    ///
    /// Returns `false` without emitting a row when that is not possible, and then the caller
    /// executes the node itself. Every step can decline: the leaves may not share one source
    /// ([`pushdown::single_source`]), the provider may not run SQL
    /// ([`RowProvider::query_dialect`]), the emitter may refuse the node
    /// ([`pushdown::node_query_or_reason`], whose reason is kept for
    /// [`Self::take_pushdown_rejections`]), or the provider may reject the query.
    ///
    /// Only the last can happen after rows have already been emitted, so falling back is allowed
    /// only while nothing has been emitted yet.
    fn push_down(
        &self,
        node_id: &str,
        on_row: &mut dyn FnMut(&[Value]) -> Result<(), ExtractionError>,
    ) -> Result<bool, ExtractionError> {
        let Some(source_id) = pushdown::single_source(self.blueprint, node_id) else {
            return Ok(false);
        };
        let Some(provider) = self.providers.get(source_id).copied() else {
            return Ok(false);
        };
        let Some(dialect) = provider.query_dialect() else {
            return Ok(false);
        };
        let names = self.column_names(node_id);
        let sql = match pushdown::node_query_or_reason(
            self.blueprint,
            &self.full,
            node_id,
            &names,
            dialect,
        ) {
            Ok(sql) => sql,
            Err(reason) => {
                // A node read by two consumers gets here twice, with the same reason.
                let mut declined = self.pushdown_rejections.borrow_mut();
                if !declined.iter().any(|(n, _)| n == node_id) {
                    declined.push((node_id.to_string(), reason));
                }
                return Ok(false);
            }
        };

        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let mut first_err: Option<ExtractionError> = None;
        let mut emitted = false;
        let result = provider.scan_query(&sql, &refs, &mut |vals| {
            emitted = true;
            match on_row(vals) {
                Ok(()) => ControlFlow::Continue(()),
                Err(e) => {
                    first_err = Some(e);
                    ControlFlow::Break(())
                }
            }
        });
        match result {
            Ok(()) => match first_err {
                Some(e) => Err(e),
                None => Ok(true),
            },
            Err(ProviderError::QueryUnsupported) if !emitted => Ok(false),
            Err(e) => Err(ExtractionError::Provider {
                node: node_id.to_string(),
                source: e,
            }),
        }
    }

    /// A hash join that holds one side, not two: the right input becomes a [`BuildSide`] and
    /// the left is streamed past it.
    ///
    /// Which side is held, and why every column is resolved before a row is read, are this
    /// module's docs.
    fn hash_join(
        &self,
        node_id: &str,
        left: &str,
        right: &str,
        on: &[(String, String)],
        on_row: &mut dyn FnMut(&[Value]) -> Result<(), ExtractionError>,
    ) -> Result<(), ExtractionError> {
        let out_cols = self.column_names(node_id);
        let l_cols = self.column_names(left);
        let r_cols = self.column_names(right);

        let key_position = |columns: &[String], column: &str, side: &'static str| {
            columns.iter().position(|c| c == column).ok_or_else(|| {
                ExtractionError::JoinKeyColumnMissing {
                    node: node_id.to_string(),
                    side,
                    column: column.to_string(),
                }
            })
        };
        let left_pos: Vec<usize> = on
            .iter()
            .map(|(l, _)| key_position(&l_cols, l, "left"))
            .collect::<Result<_, _>>()?;
        let right_pos: Vec<usize> = on
            .iter()
            .map(|(_, r)| key_position(&r_cols, r, "right"))
            .collect::<Result<_, _>>()?;

        let empty = TableSchema {
            name: String::new(),
            columns: std::collections::BTreeMap::new(),
        };
        let l_full = self.full.get(left).unwrap_or(&empty);
        let r_full = self.full.get(right).unwrap_or(&empty);
        // `kept` lists positions in a right input row, and a `ColSource::Right` indexes into that
        // narrowed tuple, so a right column the output never mentions is never stored.
        let mut kept: Vec<usize> = Vec::new();
        let sources: Vec<ColSource> = out_cols
            .iter()
            .map(|name| {
                let unresolved = || {
                    ExtractionError::Invalid(vec![ValidationError::UnknownColumn {
                        node: node_id.to_string(),
                        column: name.clone(),
                    }])
                };
                match join_column_source(name, l_full, r_full) {
                    Some((JoinSide::Left, source_column)) => l_cols
                        .iter()
                        .position(|c| c == source_column)
                        .map(ColSource::Left)
                        .ok_or_else(unresolved),
                    Some((JoinSide::Right, source_column)) => {
                        let p = r_cols
                            .iter()
                            .position(|c| c == source_column)
                            .ok_or_else(unresolved)?;
                        let slot = kept.iter().position(|k| *k == p).unwrap_or_else(|| {
                            kept.push(p);
                            kept.len() - 1
                        });
                        Ok(ColSource::Right(slot))
                    }
                    None => Err(unresolved()),
                }
            })
            .collect::<Result<_, _>>()?;

        let mut keys = JoinKeyBuf::default();
        let mut build = BuildSide::default();
        self.stream(right, &mut |vals| {
            // A `Null` key matches nothing, so the row is dropped here rather than stored and
            // skipped later.
            if let Some(key) = keys.render(vals, &right_pos) {
                let row: Vec<Value> = kept.iter().map(|&i| vals[i].clone()).collect();
                match build.by_key.get_mut(key) {
                    Some(rows) => rows.push(row),
                    None => {
                        build.by_key.insert(key.to_string(), vec![row]);
                    }
                }
                build.rows += 1;
            }
            Ok(())
        })?;
        self.rows_materialized
            .set(self.rows_materialized.get() + build.rows);

        let mut out = vec![Value::Null; out_cols.len()];
        self.stream(left, &mut |lrow| {
            let Some(key) = keys.render(lrow, &left_pos) else {
                return Ok(());
            };
            let Some(matches) = build.by_key.get(key) else {
                return Ok(());
            };
            for rrow in matches {
                for (slot, source) in out.iter_mut().zip(&sources) {
                    *slot = match source {
                        ColSource::Left(p) => lrow[*p].clone(),
                        ColSource::Right(p) => rrow[*p].clone(),
                    };
                }
                on_row(&out)?;
            }
            Ok(())
        })
    }
}

/// Scratch buffers for rendering join keys, reused across every row of both a join's inputs.
///
/// One key is one string, not a `Vec` of them: the streamed side discards its key right after the
/// probe, so a per-column allocation there would be pure waste.
#[derive(Debug, Default)]
struct JoinKeyBuf {
    key: String,
    part: String,
}

impl JoinKeyBuf {
    /// One row's join key, or `None` if any key column is missing or is `Null`, which excludes the
    /// row from the join (SQL inner-join semantics: `NULL` never joins).
    ///
    /// Each part carries its length, so two columns cannot run together into a key another pair of
    /// values also renders. Rendered through [`Value::write_join_key_part`] rather than
    /// [`Value::canonical_string`], which is `None` for `Float` and `Timestamp`.
    fn render(&mut self, row: &[Value], positions: &[usize]) -> Option<&str> {
        self.key.clear();
        for &i in positions {
            self.part.clear();
            if !row.get(i)?.write_join_key_part(&mut self.part) {
                return None;
            }
            let _ = write!(self.key, "{}:{}", self.part.len(), self.part);
        }
        Some(&self.key)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::core::event_data::object_centric::extraction::blueprint::Node;
    use crate::core::event_data::object_centric::extraction::catalog::{
        ExtractionCatalog, TableSchema,
    };
    use crate::core::event_data::object_centric::extraction::provider::ProviderError;

    /// A [`RowProvider`] over literal rows, so these tests can drive the executor without a
    /// database, and without `validate` first rejecting the deliberately malformed graphs below.
    #[derive(Debug)]
    struct VecProvider(BTreeMap<String, (Vec<String>, Vec<Vec<Value>>)>);

    impl RowProvider for VecProvider {
        fn scan(
            &self,
            table: &str,
            columns: &[&str],
            f: &mut dyn FnMut(&[Value]) -> ControlFlow<()>,
        ) -> Result<(), ProviderError> {
            let (names, rows) = self
                .0
                .get(table)
                .ok_or_else(|| ProviderError::UnknownTable {
                    table: table.to_string(),
                })?;
            let positions: Vec<usize> = columns
                .iter()
                .map(|c| {
                    names
                        .iter()
                        .position(|n| n == c)
                        .ok_or_else(|| ProviderError::UnknownColumn {
                            table: table.to_string(),
                            column: (*c).to_string(),
                        })
                })
                .collect::<Result<_, _>>()?;
            for row in rows {
                let projected: Vec<Value> = positions.iter().map(|&i| row[i].clone()).collect();
                if f(&projected).is_break() {
                    break;
                }
            }
            Ok(())
        }
    }

    fn source(id: &str, table: &str) -> Node {
        Node {
            id: id.to_string(),
            label: None,
            op: NodeOp::Source {
                source_id: "db".into(),
                table: table.to_string(),
            },
        }
    }

    /// A `Join` whose `on` names a key column its input's rows do not carry must be an error.
    /// Silently dropping that pair from the key shortens the key, so rows agreeing on only the
    /// remaining columns get paired: a partial cross product reported as a successful run.
    #[test]
    fn a_join_key_column_missing_from_an_input_is_an_error_not_a_shorter_key() {
        let provider = VecProvider(
            [
                (
                    "l".to_string(),
                    (
                        vec!["id".to_string()],
                        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]],
                    ),
                ),
                (
                    "r".to_string(),
                    (
                        vec!["id".to_string()],
                        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]],
                    ),
                ),
            ]
            .into_iter()
            .collect(),
        );
        let mut providers: HashMap<String, &dyn RowProvider> = HashMap::new();
        providers.insert("db".to_string(), &provider);

        let blueprint = Blueprint {
            version: crate::core::event_data::object_centric::extraction::MODEL_VERSION,
            id_rendering: crate::core::event_data::object_centric::extraction::IdRendering::Raw,
            nodes: vec![
                source("l", "l"),
                source("r", "r"),
                Node {
                    id: "j".into(),
                    label: None,
                    op: NodeOp::Join {
                        left: "l".into(),
                        right: "r".into(),
                        // "missing" is on neither side; `validate` would reject this blueprint,
                        // which is why this test drives the executor directly.
                        on: vec![("missing".into(), "id".into())],
                    },
                },
            ],
            mappings: vec![],
            on_missing_endpoint: Default::default(),
            on_duplicate_object: Default::default(),
        };
        let catalog = ExtractionCatalog::new()
            .with_table("db", TableSchema::new("l", [("id", "INTEGER", false)]))
            .with_table("db", TableSchema::new("r", [("id", "INTEGER", false)]));

        let exec = GraphExecutor::new(&blueprint, &catalog, &providers, &[]).expect("executor");
        let err = exec
            .stream("j", &mut |_| Ok(()))
            .expect_err("must not silently cross-join");
        assert!(
            matches!(
                &err,
                ExtractionError::JoinKeyColumnMissing { node, side, column }
                    if node == "j" && *side == "left" && column == "missing"
            ),
            "got {err:?}"
        );
    }

    /// A left column literally named `right_<name>` and the rename of the right's `<name>` want
    /// the same output name.
    #[test]
    fn a_join_output_column_claimed_by_both_sides_is_an_error_not_a_silent_choice() {
        let provider = VecProvider(
            [
                (
                    "l".to_string(),
                    (
                        vec!["id".to_string(), "right_id".to_string()],
                        vec![vec![Value::Integer(1), Value::Text("left".into())]],
                    ),
                ),
                (
                    "r".to_string(),
                    (vec!["id".to_string()], vec![vec![Value::Integer(1)]]),
                ),
            ]
            .into_iter()
            .collect(),
        );
        let mut providers: HashMap<String, &dyn RowProvider> = HashMap::new();
        providers.insert("db".to_string(), &provider);

        let blueprint = Blueprint {
            version: crate::core::event_data::object_centric::extraction::MODEL_VERSION,
            id_rendering: crate::core::event_data::object_centric::extraction::IdRendering::Raw,
            nodes: vec![
                source("l", "l"),
                source("r", "r"),
                Node {
                    id: "j".into(),
                    label: None,
                    op: NodeOp::Join {
                        left: "l".into(),
                        right: "r".into(),
                        on: vec![("id".into(), "id".into())],
                    },
                },
            ],
            mappings: vec![],
            on_missing_endpoint: Default::default(),
            on_duplicate_object: Default::default(),
        };
        let catalog = ExtractionCatalog::new()
            .with_table(
                "db",
                TableSchema::new("l", [("id", "INTEGER", false), ("right_id", "TEXT", false)]),
            )
            .with_table("db", TableSchema::new("r", [("id", "INTEGER", false)]));

        let mapping = Mapping {
            node: "j".into(),
            label: None,
            when: None,
            target: crate::core::event_data::object_centric::extraction::Target::Object {
                object_type: crate::core::event_data::object_centric::extraction::expr::ValueExpression::Constant {
                    value: "t".into(),
                },
                id: crate::core::event_data::object_centric::extraction::expr::ValueExpression::Column {
                    column: "right_id".into(),
                },
                timestamp: None,
                attributes: vec![],
            },
        };
        let mappings = vec![("mappings[0]".to_string(), mapping)];

        let exec =
            GraphExecutor::new(&blueprint, &catalog, &providers, &mappings).expect("executor");
        let err = exec
            .stream("j", &mut |_| Ok(()))
            .expect_err("must not pick a side");
        assert!(
            matches!(
                &err,
                ExtractionError::Invalid(errors)
                    if matches!(
                        errors.as_slice(),
                        [ValidationError::UnknownColumn { node, column }]
                            if node == "j" && column == "right_id"
                    )
            ),
            "got {err:?}"
        );
    }
}
