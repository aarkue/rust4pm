//! Deciding when a node can be executed by its source instead of row by row, and asking the
//! step-3 compiler to write the SQL that does it.
//!
//! A [`Join`](super::blueprint::NodeOp::Join) is the one node
//! [`GraphExecutor`](super::graph::GraphExecutor) cannot execute in constant memory. When both
//! inputs read the same source, the engine already has both tables and can do the join itself.
//!
//! The SQL comes from [`Emitter`], the compiler's own emitter, so that a memory optimisation
//! cannot change results by drifting from it. [`Emitter::node_sql`] projects a node's full column
//! set while the executor's rows carry only the demanded subset (see [`super::schema`]), so the
//! query is wrapped in a projection naming exactly the executor's columns in its order.
//!
//! The emitter decides everything from declared column types, so its SQL matches the row-level
//! path only when the source's runtime values have the kinds the catalog declares. A provider
//! asserts that by returning a dialect from
//! [`RowProvider::query_dialect`](super::provider::RowProvider::query_dialect); a dynamically
//! typed source keeps the default `None` and is never pushed down to.

use std::collections::HashMap;

use super::blueprint::{Blueprint, NodeOp};
use super::catalog::TableSchema;
use super::compile::emit::{Emitter, ROW_ALIAS};
use super::compile::{RejectReason, SqlDialect};

/// The one `source_id` every [`Source`](super::blueprint::NodeOp::Source) leaf under `node_id`
/// reads, or `None` when the leaves disagree, a node is missing, or the graph cycles.
///
/// `Filter`, `Union` and `Join` are all transparent here: a filter inherits its input's answer,
/// and a union or join inherits its inputs' only when they agree. So a join over two filtered
/// tables of one database is pushable, and the same join over a database and a CSV file is not.
pub(crate) fn single_source<'a>(blueprint: &'a Blueprint, node_id: &str) -> Option<&'a str> {
    walk(blueprint, node_id, 0)
}

fn walk<'a>(blueprint: &'a Blueprint, node_id: &str, depth: usize) -> Option<&'a str> {
    // A validated blueprint is acyclic, but this runs on unvalidated ones too (the executor's
    // own tests build them deliberately); no path can visit more nodes than exist.
    if depth > blueprint.nodes.len() {
        return None;
    }
    match &blueprint.node(node_id)?.op {
        NodeOp::Source { source_id, .. } => Some(source_id.as_str()),
        NodeOp::Filter { input, .. } => walk(blueprint, input, depth + 1),
        NodeOp::Union { inputs } => {
            let mut it = inputs.iter();
            let first = walk(blueprint, it.next()?, depth + 1)?;
            it.all(|i| walk(blueprint, i, depth + 1) == Some(first))
                .then_some(first)
        }
        NodeOp::Join { left, right, .. } => {
            let l = walk(blueprint, left, depth + 1)?;
            (walk(blueprint, right, depth + 1) == Some(l)).then_some(l)
        }
    }
}

/// A `SELECT` producing `node_id`'s rows as the executor expects them: exactly `columns`, in
/// that order, under those names.
///
/// `full` is [`full_node_schemas`](super::schema::full_node_schemas)' result, which the executor
/// already holds. Passing it avoids resolving every node against the catalog a second time.
///
/// `Err` when the emitter declines the node (an unsupported predicate, a join key whose declared
/// type does not decide comparability, an unresolved table) or when `columns` is empty. Declining
/// is safe, since the caller then executes the node row by row, but the fall-back is `hash_join`,
/// whose memory grows with the data, so the reason is carried out rather than discarded.
pub(crate) fn node_query_or_reason<'a>(
    blueprint: &'a Blueprint,
    full: &HashMap<&'a str, TableSchema>,
    node_id: &str,
    columns: &[String],
    dialect: SqlDialect,
) -> Result<String, RejectReason> {
    if columns.is_empty() {
        return Err(RejectReason::EmptyProjection {
            node: node_id.to_string(),
        });
    }
    let emitter = Emitter::from_schemas(blueprint, full, dialect);
    let inner = emitter.node_sql(node_id)?;
    let list: Vec<String> = columns
        .iter()
        .map(|c| {
            let quoted = dialect.quote_ident(c);
            format!("{ROW_ALIAS}.{quoted} AS {quoted}")
        })
        .collect();
    Ok(format!(
        "SELECT {} FROM {}",
        list.join(", "),
        dialect.derived_table(&inner, ROW_ALIAS)
    ))
}
