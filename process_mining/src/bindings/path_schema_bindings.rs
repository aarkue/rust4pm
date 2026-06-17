//! Binding wrappers for path-schema discovery over a [`SlimLinkedOCEL`].
//!
//! See [`crate::analysis::object_centric::path_schemas`] for the underlying algorithm.

use macros_process_mining::register_binding;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::analysis::object_centric::path_schemas::{
    discover_path_schemas, enumerate_schemas, find_connections_with_sources, get_entities_of_type,
    schema_stats, Connection, PathConnectionParams, PathSchemaDiscovery, PathSchemaQuery,
    ResolvedPathSchema, SchemaStats, TypeEdge, TypeGraph, TypeRef,
};
use crate::core::event_data::object_centric::linked_ocel::{LinkedOCELAccess, SlimLinkedOCEL};

/// A node (event or object type) of the OCEL type graph, with its entity count.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PathSchemaTypeNode {
    /// Type name (activity / object class).
    pub name: String,
    /// Whether this is an event type (`true`) or object type (`false`).
    pub is_event: bool,
    /// Number of entities of this type.
    pub count: usize,
}

/// The OCEL type graph: typed nodes and qualified relationship edges.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PathSchemaTypeGraph {
    /// Event and object type nodes.
    pub nodes: Vec<PathSchemaTypeNode>,
    /// Qualified E2O / O2O relationship edges.
    pub edges: Vec<TypeEdge>,
}

/// Connections of a single schema, with metrics and throughput.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PathSchemaConnections {
    /// Human-readable schema string.
    pub schema: String,
    /// Metrics and throughput for the connections.
    pub stats: SchemaStats,
    /// The connections, with entities referenced by their OCEL index.
    pub connections: Vec<Connection>,
    /// Whether the connection limit was reached (results may be incomplete).
    pub limit_reached: bool,
    /// Whether selectivity-based early termination was triggered.
    pub selectivity_pruned: bool,
}

/// Build the OCEL type graph (typed nodes with entity counts, plus relationship edges).
#[register_binding]
fn path_schema_type_graph(ocel: &SlimLinkedOCEL) -> PathSchemaTypeGraph {
    let type_graph = TypeGraph::from_linked_ocel(ocel);
    let mut nodes =
        Vec::with_capacity(type_graph.event_types.len() + type_graph.object_types.len());
    for name in &type_graph.event_types {
        nodes.push(PathSchemaTypeNode {
            count: ocel.get_evs_of_type(name).count(),
            name: name.clone(),
            is_event: true,
        });
    }
    for name in &type_graph.object_types {
        nodes.push(PathSchemaTypeNode {
            count: ocel.get_obs_of_type(name).count(),
            name: name.clone(),
            is_event: false,
        });
    }
    PathSchemaTypeGraph {
        nodes,
        edges: type_graph.edges,
    }
}

/// Enumerate type-level path schemas between two types (or from one type), without any
/// instance traversal. Each schema is self-contained ([`ResolvedPathSchema`]).
#[register_binding]
fn path_schema_enumerate(
    ocel: &SlimLinkedOCEL,
    source: TypeRef,
    #[bind(default)] target: Option<TypeRef>,
    max_length: usize,
    #[bind(default)] allow_cycles: bool,
    #[bind(default)] allowed_types: Option<Vec<TypeRef>>,
) -> Vec<ResolvedPathSchema> {
    let type_graph = TypeGraph::from_linked_ocel(ocel);
    let allowed: Option<HashSet<TypeRef>> = allowed_types.map(|types| types.into_iter().collect());
    enumerate_schemas(
        &type_graph,
        &source,
        target.as_ref(),
        max_length,
        allow_cycles,
        allowed.as_ref(),
    )
    .iter()
    .map(|sch| sch.resolve(&type_graph))
    .collect()
}

/// Discover path schemas between the query types and score each by support / coverage /
/// selectivity / reach / exclusivity and throughput.
#[register_binding]
fn path_schema_discover(ocel: &SlimLinkedOCEL, query: PathSchemaQuery) -> PathSchemaDiscovery {
    discover_path_schemas(ocel, &query)
}

/// Find the concrete instance connections of a single (resolved) schema, with metrics and
/// throughput. Entities are referenced by their compact OCEL index.
#[register_binding]
fn path_schema_connections(
    ocel: &SlimLinkedOCEL,
    schema: ResolvedPathSchema,
    #[bind(default)] params: PathConnectionParams,
) -> PathSchemaConnections {
    let sources = get_entities_of_type(ocel, &schema.source);
    let total_sources = sources.len();
    let total_targets = get_entities_of_type(ocel, &schema.target).len();
    let result = find_connections_with_sources(ocel, &schema, &sources, &params);
    let stats = schema_stats(&result.connections, total_sources, total_targets);
    PathSchemaConnections {
        schema: schema.display(),
        stats,
        connections: result.connections,
        limit_reached: result.limit_reached,
        selectivity_pruned: result.selectivity_pruned,
    }
}
