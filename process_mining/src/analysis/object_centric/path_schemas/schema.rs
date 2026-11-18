use super::graph::{TypeEdge, TypeGraph, TypeRef};
use crate::utils::control::{Cancelled, Control};
use macros_process_mining::controlled;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// A self-contained, serializable path schema: edges are embedded, so no [`TypeGraph`]
/// is needed to interpret or traverse it. Produced via [`PathSchema::resolve`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct ResolvedPathSchema {
    /// The starting type.
    pub source: TypeRef,
    /// Ordered traversal steps with embedded typed edges.
    pub steps: Vec<ResolvedStep>,
    /// The ending type.
    pub target: TypeRef,
}

/// One step of a [`ResolvedPathSchema`]: a typed edge plus traversal direction.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct ResolvedStep {
    /// The typed edge traversed in this step.
    pub edge: TypeEdge,
    /// Whether the edge is traversed in reverse direction.
    pub reverse: bool,
}

impl ResolvedPathSchema {
    /// Number of steps (edges) in the schema.
    pub fn len(&self) -> usize {
        self.steps.len()
    }

    /// Whether the schema has no steps.
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// A human-readable string representation.
    pub fn display(&self) -> String {
        let mut parts = vec![self.source.name().to_string()];
        for step in &self.steps {
            let dir = if step.reverse { "<" } else { "" };
            parts.push(format!("-[{}{}]->", dir, step.edge.qualifier));
            let next = if step.reverse {
                step.edge.source.name()
            } else {
                step.edge.target.name()
            };
            parts.push(next.to_string());
        }
        parts.join(" ")
    }
}

/// A step in a [`PathSchema`]: an edge index into [`TypeGraph::edges`] plus whether the
/// edge is traversed in reverse. Index-based to keep enumeration cheap (no per-step
/// type clones). For a self-contained / serializable form use [`PathSchema::resolve`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaStep {
    /// Index into [`TypeGraph::edges`].
    pub edge_idx: usize,
    /// Whether this edge is traversed in reverse direction.
    pub reverse: bool,
}

/// A type-level path schema: a sequence of type graph edges forming a path from a source
/// type to a target type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PathSchema {
    /// The starting type.
    pub source: TypeRef,
    /// Ordered traversal steps.
    pub steps: Vec<SchemaStep>,
    /// The ending type.
    pub target: TypeRef,
}

impl PathSchema {
    /// Number of steps (edges) in the schema.
    pub fn len(&self) -> usize {
        self.steps.len()
    }

    /// Whether the schema has no steps.
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// Produce a self-contained, serializable form (edges embedded, no graph needed).
    pub fn resolve(&self, type_graph: &TypeGraph) -> ResolvedPathSchema {
        ResolvedPathSchema {
            source: self.source.clone(),
            steps: self
                .steps
                .iter()
                .map(|s| ResolvedStep {
                    edge: type_graph.edges[s.edge_idx].clone(),
                    reverse: s.reverse,
                })
                .collect(),
            target: self.target.clone(),
        }
    }

    /// A human-readable string representation.
    pub fn display(&self, type_graph: &TypeGraph) -> String {
        self.resolve(type_graph).display()
    }
}

/// Default cap on the number of schemas [`enumerate_schemas`] returns.
pub const DEFAULT_MAX_SCHEMAS: usize = 10_000;

/// Cap on the number of partial paths one enumeration explores. Bounds the run time when few
/// paths match `target` (e.g., with cycles and a large `max_length`).
pub const MAX_EXPLORED_PATHS: usize = 1_000_000;

/// Longer `max_length`s are clamped to this. Paths are explored depth-first, so the stack
/// holds `O(max_length^2)` steps.
pub const MAX_SCHEMA_LENGTH: usize = 64;

/// Result of [`enumerate_schemas`].
#[derive(Debug, Clone)]
pub struct SchemaEnumeration {
    /// Enumerated schemas, in enumeration order.
    pub schemas: Vec<PathSchema>,
    /// Whether enumeration stopped at `max_schemas`, [`MAX_EXPLORED_PATHS`] or
    /// [`MAX_SCHEMA_LENGTH`], so `schemas` may be incomplete.
    pub limit_reached: bool,
}

/// Explored paths between two progress reports (and cancel checks) of an enumeration.
const EXPLORED_PER_REPORT: usize = 4096;

/// Progress stage of [`enumerate_schemas`], counting explored partial paths.
pub(super) const ENUMERATE_STAGE: &str = "Enumerating path schemas";

/// Enumerate type-level path schemas between two types (or from one type) by exhaustively
/// traversing the type graph.
///
/// - `max_length`: maximum number of steps (edges) in a schema.
/// - `target`: if `Some`, only return schemas ending at this type.
/// - `allow_cycles`: if `false`, never visit the same type twice within a schema.
/// - `allowed_types`: if `Some`, every intermediate step must land in this set. Enumeration
///   still starts from `source`, and the `target` type (when one is given) is always
///   permitted, so a schema can always begin at the source and reach the target; only the
///   steps in between are constrained.
///
/// Returns at most `max_schemas` schemas (e.g., [`DEFAULT_MAX_SCHEMAS`]) of at most
/// [`MAX_SCHEMA_LENGTH`] steps, explores at most [`MAX_EXPLORED_PATHS`] paths, and tells whether
/// a limit was hit. Below the limits, the schemas and their order are the same as without one.
///
/// Reports progress with stage `"Enumerating path schemas"`, where `done` counts the partial paths
/// explored so far, with no total. Streams each schema as soon as it is found: one partial value
/// is one [`ResolvedPathSchema`], in enumeration order.
#[controlled]
#[allow(clippy::too_many_arguments)]
pub fn enumerate_schemas(
    type_graph: &TypeGraph,
    source: &TypeRef,
    target: Option<&TypeRef>,
    max_length: usize,
    allow_cycles: bool,
    allowed_types: Option<&HashSet<TypeRef>>,
    max_schemas: usize,
    ctl: &Control<'_>,
) -> Result<SchemaEnumeration, Cancelled> {
    enumerate_bounded(
        type_graph,
        source,
        target,
        max_length,
        allow_cycles,
        allowed_types,
        max_schemas,
        ctl,
        true,
    )
}

/// Enumerate without streaming schemas as partials when `stream_schemas` is unset, for a caller
/// whose partial values are something else.
#[allow(clippy::too_many_arguments)]
pub(super) fn enumerate_bounded(
    type_graph: &TypeGraph,
    source: &TypeRef,
    target: Option<&TypeRef>,
    max_length: usize,
    allow_cycles: bool,
    allowed_types: Option<&HashSet<TypeRef>>,
    max_schemas: usize,
    ctl: &Control<'_>,
    stream_schemas: bool,
) -> Result<SchemaEnumeration, Cancelled> {
    let requested_length = max_length;
    let max_length = max_length.min(MAX_SCHEMA_LENGTH);
    let can_step = |neighbor: &TypeRef, visited: &[TypeRef]| {
        (allow_cycles || !visited.contains(neighbor))
            && !allowed_types
                .is_some_and(|allowed| !allowed.contains(neighbor) && target != Some(neighbor))
    };
    let mut results = Vec::new();
    let mut limit_reached = false;
    let mut explored = 0usize;
    let mut queue: Vec<(TypeRef, Vec<SchemaStep>, Vec<TypeRef>)> =
        vec![(source.clone(), Vec::new(), vec![source.clone()])];

    ctl.progress(ENUMERATE_STAGE, 0, None);
    while let Some((current, steps, visited)) = queue.pop() {
        if explored == MAX_EXPLORED_PATHS {
            limit_reached = true;
            break;
        }
        explored += 1;
        if explored.is_multiple_of(EXPLORED_PER_REPORT) {
            ctl.check()?;
            ctl.progress(ENUMERATE_STAGE, explored as u64, None);
        }
        let matches_target = match target {
            Some(tt) => &current == tt,
            None => true,
        };
        if !steps.is_empty() && matches_target {
            if results.len() == max_schemas {
                limit_reached = true;
                break;
            }
            let schema = PathSchema {
                source: source.clone(),
                steps: steps.clone(),
                target: current.clone(),
            };
            if stream_schemas && ctl.wants_partials() {
                ctl.partial(&schema.resolve(type_graph));
            }
            results.push(schema);
        }

        if steps.len() >= max_length {
            if max_length < requested_length
                && !limit_reached
                && type_graph
                    .neighbors_undirected(&current)
                    .iter()
                    .any(|(_, neighbor, _)| can_step(neighbor, &visited))
            {
                limit_reached = true;
            }
            continue;
        }

        for (edge_idx, neighbor, reverse) in type_graph.neighbors_undirected(&current) {
            if !can_step(neighbor, &visited) {
                continue;
            }
            let mut new_steps = steps.clone();
            new_steps.push(SchemaStep { edge_idx, reverse });
            let mut new_visited = visited.clone();
            // Only read without cycles, where it is at most one entry per type.
            if !allow_cycles {
                new_visited.push(neighbor.clone());
            }
            queue.push((neighbor.clone(), new_steps, new_visited));
        }
    }

    ctl.check()?;
    ctl.progress(ENUMERATE_STAGE, explored as u64, None);
    Ok(SchemaEnumeration {
        schemas: results,
        limit_reached,
    })
}
