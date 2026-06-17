use super::graph::{TypeEdge, TypeGraph, TypeRef};
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
pub fn enumerate_schemas(
    type_graph: &TypeGraph,
    source: &TypeRef,
    target: Option<&TypeRef>,
    max_length: usize,
    allow_cycles: bool,
    allowed_types: Option<&HashSet<TypeRef>>,
) -> Vec<PathSchema> {
    let mut results = Vec::new();
    let mut queue: Vec<(TypeRef, Vec<SchemaStep>, Vec<TypeRef>)> =
        vec![(source.clone(), Vec::new(), vec![source.clone()])];

    while let Some((current, steps, visited)) = queue.pop() {
        let matches_target = match target {
            Some(tt) => &current == tt,
            None => true,
        };
        if !steps.is_empty() && matches_target {
            results.push(PathSchema {
                source: source.clone(),
                steps: steps.clone(),
                target: current.clone(),
            });
        }

        if steps.len() >= max_length {
            continue;
        }

        for (edge_idx, neighbor, reverse) in type_graph.neighbors_undirected(&current) {
            if !allow_cycles && visited.contains(neighbor) {
                continue;
            }
            if allowed_types
                .is_some_and(|allowed| !allowed.contains(neighbor) && target != Some(neighbor))
            {
                continue;
            }
            let mut new_steps = steps.clone();
            new_steps.push(SchemaStep { edge_idx, reverse });
            let mut new_visited = visited.clone();
            new_visited.push(neighbor.clone());
            queue.push((neighbor.clone(), new_steps, new_visited));
        }
    }

    results
}
