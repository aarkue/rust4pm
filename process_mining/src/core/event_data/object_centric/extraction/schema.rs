//! Per-node column resolution: what a node's rows look like, and which of its columns are actually
//! read anywhere downstream.
//!
//! Internal to the executor. One traversal produces both the full column shape of every node
//! (needed for [`Predicate::prepare`](super::predicate::Predicate::prepare)'s literal coercion and
//! to disambiguate a `Join`'s `right_<name>` columns) and the subset of each node's columns
//! anything downstream reads, so a `Source` node only asks its
//! [`RowProvider`](super::provider::RowProvider) for what a mapping or `Filter` will use.
//!
//! Demand flows downward everywhere except a `Filter`, whose demand must equal its input's:
//! [`GraphExecutor::stream`](super::graph::GraphExecutor::stream) hands a filter's consumers the
//! input's row verbatim, so a narrower demand would shift every position after a dropped column.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};

use super::blueprint::{Blueprint, Mapping, Node, NodeOp};
use super::catalog::{Catalog, ColumnSchema, TableSchema, UNTYPED_COL_TYPE};

/// The full, statically known column shape of every node, keyed by node id.
///
/// The single resolution of a node's columns: `validate` derives the names it checks against from
/// this, so a blueprint cannot pass validation against one set of columns and run against another.
pub(crate) fn full_node_schemas<'a>(
    blueprint: &'a Blueprint,
    catalog: &dyn Catalog,
) -> HashMap<&'a str, TableSchema> {
    let mut out: HashMap<&str, TableSchema> = HashMap::new();
    // Nodes may appear in any order, so a node whose inputs are not resolved yet is retried on a
    // later pass.
    let mut changed = true;
    while changed {
        changed = false;
        for node in &blueprint.nodes {
            if out.contains_key(node.id.as_str()) {
                continue;
            }
            if let Some(schema) = resolve_one(node, &out, catalog) {
                out.insert(node.id.as_str(), schema);
                changed = true;
            }
        }
    }
    out
}

fn resolve_one(
    node: &Node,
    out: &HashMap<&str, TableSchema>,
    catalog: &dyn Catalog,
) -> Option<TableSchema> {
    match &node.op {
        NodeOp::Source { source_id, table } => catalog.table(source_id, table).cloned(),
        NodeOp::Filter { input, .. } => out.get(input.as_str()).cloned(),
        NodeOp::Union { inputs } => {
            let mut columns: BTreeMap<String, ColumnSchema> = BTreeMap::new();
            let mut declared_by: BTreeMap<String, usize> = BTreeMap::new();
            for input in inputs {
                let s = out.get(input.as_str())?;
                for (name, col) in &s.columns {
                    *declared_by.entry(name.clone()).or_insert(0) += 1;
                    match columns.entry(name.clone()) {
                        Entry::Vacant(slot) => {
                            slot.insert(col.clone());
                        }
                        Entry::Occupied(mut slot) => reconcile_union_column(slot.get_mut(), col),
                    }
                }
            }
            // A column only some inputs declare is Null on every row a non-declaring input
            // contributes, regardless of what each declaring input says about nullability.
            for (name, col) in &mut columns {
                if declared_by[name] < inputs.len() {
                    col.nullable = true;
                }
            }
            Some(TableSchema {
                name: node.id.clone(),
                columns,
            })
        }
        NodeOp::Join { left, right, .. } => {
            let l = out.get(left.as_str())?;
            let r = out.get(right.as_str())?;
            let mut columns = l.columns.clone();
            for (name, col) in &r.columns {
                if l.columns.contains_key(name) {
                    let renamed = format!("right_{name}");
                    columns.insert(
                        renamed.clone(),
                        ColumnSchema {
                            name: renamed,
                            col_type: col.col_type.clone(),
                            nullable: col.nullable,
                        },
                    );
                } else {
                    columns.insert(name.clone(), col.clone());
                }
            }
            Some(TableSchema {
                name: node.id.clone(),
                columns,
            })
        }
    }
}

/// Fold a second input's declaration of one `Union` column into the first input's.
///
/// Inputs disagreeing about a column's kind leave it [`UNTYPED_COL_TYPE`] rather than picking a
/// side, which would coerce a guard literal to a kind half the rows do not have.
fn reconcile_union_column(into: &mut ColumnSchema, other: &ColumnSchema) {
    if into.declared_kind() != other.declared_kind() {
        into.col_type = UNTYPED_COL_TYPE.to_string();
    }
    into.nullable |= other.nullable;
}

/// Which side of a `Join` one of its output columns comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinSide {
    /// The join's left input.
    Left,
    /// The join's right input.
    Right,
}

/// Resolve one `Join` output column name to the input it comes from and the name it has there.
///
/// The single rule both this module (deciding which input to demand a column from) and
/// [`GraphExecutor`](super::graph::GraphExecutor) (deciding which input row to read it out of)
/// use, so the two cannot disagree. The precedence mirrors how [`full_node_schemas`] builds a
/// `Join`'s schema: left columns first, then each right column either under its own name or,
/// where the left already has that name, under `right_<name>`.
///
/// `None` means the name resolves to no single column: neither side has one, or both a left
/// column literally called `right_<name>` and the rename of the right's `<name>` claim it.
pub(crate) fn join_column_source<'a>(
    name: &'a str,
    left: &TableSchema,
    right: &TableSchema,
) -> Option<(JoinSide, &'a str)> {
    if let Some(stripped) = name.strip_prefix("right_") {
        if left.columns.contains_key(stripped) && right.columns.contains_key(stripped) {
            if left.columns.contains_key(name) {
                return None;
            }
            return Some((JoinSide::Right, stripped));
        }
    }
    if left.columns.contains_key(name) {
        return Some((JoinSide::Left, name));
    }
    if right.columns.contains_key(name) {
        return Some((JoinSide::Right, name));
    }
    None
}

/// The columns each node must produce: the union of what a mapping reading it needs, what a
/// downstream `Filter`/`Join` condition needs, and (recursively) what a downstream node's own
/// demand requires of it. A `Source` node's entry becomes the `columns` argument to
/// [`RowProvider::scan`](super::provider::RowProvider::scan).
///
/// `full` is [`full_node_schemas`]'s result, needed to resolve which side of a `Join` a demanded
/// column (possibly `right_`-prefixed) actually belongs to, see [`join_column_source`]. A
/// `Filter`'s demand equals its input's rather than being narrower, see this module's docs.
pub(crate) fn demanded_columns<'a>(
    blueprint: &'a Blueprint,
    full: &HashMap<&str, TableSchema>,
    mappings: &[(String, Mapping)],
) -> HashMap<&'a str, HashSet<String>> {
    let mut demand: HashMap<&str, HashSet<String>> = blueprint
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), HashSet::new()))
        .collect();

    for (_, m) in mappings {
        let Some(entry) = demand.get_mut(m.node.as_str()) else {
            continue;
        };
        let mut cols: HashSet<&str> = HashSet::new();
        if let Some(when) = &m.when {
            when.referenced_columns(&mut cols);
        }
        super::validate::collect_target_columns(&m.target, &mut cols);
        entry.extend(cols.into_iter().map(str::to_string));
    }

    for node in &blueprint.nodes {
        if let NodeOp::Filter { condition, .. } = &node.op {
            let mut cols: HashSet<&str> = HashSet::new();
            condition.referenced_columns(&mut cols);
            if let Some(entry) = demand.get_mut(node.id.as_str()) {
                entry.extend(cols.into_iter().map(str::to_string));
            }
        }
    }

    // Demand only grows and is bounded by each node's full column set, so this terminates. A
    // bounded number of passes would not do: a `Filter` moves demand both down and back up, so one
    // demand can travel arbitrarily many edges in either direction before settling.
    let mut changed = true;
    while changed {
        changed = false;
        for node in &blueprint.nodes {
            let here = demand.get(node.id.as_str()).cloned().unwrap_or_default();
            match &node.op {
                NodeOp::Source { .. } => {}
                NodeOp::Filter { input, .. } => {
                    // Equal, not narrower. See this function's docs.
                    let mut from_input = HashSet::new();
                    if let Some(entry) = demand.get_mut(input.as_str()) {
                        changed |= extend_demand(entry, here.iter().cloned());
                        from_input.clone_from(entry);
                    }
                    if let Some(entry) = demand.get_mut(node.id.as_str()) {
                        changed |= extend_demand(entry, from_input);
                    }
                }
                NodeOp::Union { inputs } => {
                    for input in inputs {
                        if let Some(entry) = demand.get_mut(input.as_str()) {
                            changed |= extend_demand(entry, here.iter().cloned());
                        }
                    }
                }
                NodeOp::Join { left, right, on } => {
                    for (l, r) in on {
                        if let Some(entry) = demand.get_mut(left.as_str()) {
                            changed |= entry.insert(l.clone());
                        }
                        if let Some(entry) = demand.get_mut(right.as_str()) {
                            changed |= entry.insert(r.clone());
                        }
                    }
                    let (Some(l_full), Some(r_full)) =
                        (full.get(left.as_str()), full.get(right.as_str()))
                    else {
                        continue;
                    };
                    for col in &here {
                        let Some((side, source_column)) = join_column_source(col, l_full, r_full)
                        else {
                            continue;
                        };
                        let input = match side {
                            JoinSide::Left => left,
                            JoinSide::Right => right,
                        };
                        if let Some(entry) = demand.get_mut(input.as_str()) {
                            changed |= entry.insert(source_column.to_string());
                        }
                    }
                }
            }
        }
    }

    demand
}

/// Add every name in `more` to `into`, reporting whether anything was actually new.
fn extend_demand(into: &mut HashSet<String>, more: impl IntoIterator<Item = String>) -> bool {
    let mut changed = false;
    for name in more {
        changed |= into.insert(name);
    }
    changed
}

/// A node's execution-time schema: [`full_node_schemas`]'s entry for that node, restricted to
/// [`demanded_columns`]'s entry, i.e. exactly the columns anything downstream reads, with their
/// declared types for [`Predicate::prepare`](super::predicate::Predicate::prepare).
pub(crate) fn projected_schema(
    full: &TableSchema,
    demanded: &HashSet<String>,
    node_id: &str,
) -> TableSchema {
    TableSchema {
        name: node_id.to_string(),
        columns: full
            .columns
            .iter()
            .filter(|(name, _)| demanded.contains(name.as_str()))
            .map(|(n, c)| (n.clone(), c.clone()))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::extraction::blueprint::{IdRendering, Node};
    use crate::core::event_data::object_centric::extraction::catalog::ExtractionCatalog;
    use crate::core::event_data::object_centric::extraction::value::ValueKind;
    use crate::core::event_data::object_centric::extraction::MODEL_VERSION;

    fn blueprint(nodes: Vec<Node>) -> Blueprint {
        Blueprint {
            version: MODEL_VERSION,
            id_rendering: IdRendering::Raw,
            nodes,
            mappings: vec![],
            on_missing_endpoint: super::super::MissingEndpointPolicy::default(),
            on_duplicate_object: super::super::DuplicateObjectPolicy::default(),
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

    #[test]
    fn inputs_disagreeing_about_a_union_column_declare_no_kind() {
        let bp = blueprint(vec![
            source("new", "orders"),
            source("old", "legacy_orders"),
            Node {
                id: "all".into(),
                label: None,
                op: NodeOp::Union {
                    inputs: vec!["new".into(), "old".into()],
                },
            },
        ]);
        let catalog = ExtractionCatalog::new()
            .with_table(
                "db",
                TableSchema::new(
                    "orders",
                    [("id", "INTEGER", false), ("state", "TEXT", false)],
                ),
            )
            .with_table(
                "db",
                TableSchema::new(
                    "legacy_orders",
                    [("id", "VARCHAR", true), ("state", "TEXT", false)],
                ),
            );

        let full = full_node_schemas(&bp, &catalog);
        let all = full.get("all").expect("union resolves");
        assert_eq!(all.columns["id"].declared_kind(), None);
        assert!(all.columns["id"].nullable, "either input may be null here");
        assert_eq!(all.columns["state"].declared_kind(), Some(ValueKind::Text));
    }

    #[test]
    fn a_union_column_declared_by_only_one_input_is_nullable() {
        let bp = blueprint(vec![
            source("new", "orders"),
            source("old", "legacy_orders"),
            Node {
                id: "all".into(),
                label: None,
                op: NodeOp::Union {
                    inputs: vec!["new".into(), "old".into()],
                },
            },
        ]);
        let catalog = ExtractionCatalog::new()
            .with_table(
                "db",
                TableSchema::new(
                    "orders",
                    [("id", "INTEGER", false), ("discount", "INTEGER", false)],
                ),
            )
            .with_table(
                "db",
                TableSchema::new("legacy_orders", [("id", "INTEGER", false)]),
            );

        let full = full_node_schemas(&bp, &catalog);
        let all = full.get("all").expect("union resolves");
        // `legacy_orders` has no `discount` column, so a row from it contributes Null there --
        // nullable even though the sole declaring input marks it non-null.
        assert!(
            all.columns["discount"].nullable,
            "column missing from one union input must be nullable"
        );
        assert!(
            !all.columns["id"].nullable,
            "column declared non-null by every input stays non-null"
        );
    }

    #[test]
    fn a_join_column_name_claimed_by_both_sides_resolves_to_neither() {
        let left = TableSchema::new("l", [("id", "INTEGER", false), ("right_id", "TEXT", false)]);
        let right = TableSchema::new("r", [("id", "INTEGER", false)]);
        assert_eq!(join_column_source("right_id", &left, &right), None);
        assert_eq!(
            join_column_source("id", &left, &right),
            Some((JoinSide::Left, "id"))
        );

        // Without a left column of that name the rename is unambiguous.
        let plain = TableSchema::new("l", [("id", "INTEGER", false)]);
        assert_eq!(
            join_column_source("right_id", &plain, &right),
            Some((JoinSide::Right, "id"))
        );
    }
}
