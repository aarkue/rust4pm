//! Binding wrappers for path-schema discovery over a [`SlimLinkedOCEL`].
//!
//! See [`crate::analysis::object_centric::path_schemas`] for the underlying algorithm.

use macros_process_mining::register_binding;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::analysis::object_centric::path_schemas::connections_from;
use crate::analysis::object_centric::path_schemas::{
    get_entities_of_type, schema_stats, Connection, PathConnectionParams, ResolvedPathSchema,
    SchemaStats, TypeGraph, TypeRef, DEFAULT_MAX_SCHEMAS,
};
use crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;
use crate::utils::control::{Cancelled, Control};

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

/// Result of [`path_schema_enumerate`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PathSchemaEnumeration {
    /// The enumerated schemas.
    pub schemas: Vec<ResolvedPathSchema>,
    /// Whether enumeration hit a limit (results may be incomplete).
    pub limit_reached: bool,
}

/// Enumerate type-level path schemas between two types (or from one type), without any
/// instance traversal. Each schema is self-contained ([`ResolvedPathSchema`]). Returns at
/// most `max_schemas` schemas, see [`enumerate_schemas`] for the other limits and the progress
/// reported.
///
/// One partial value is one [`ResolvedPathSchema`], as in [`PathSchemaEnumeration::schemas`],
/// streamed as soon as it is found.
///
/// [`enumerate_schemas`]: crate::analysis::object_centric::path_schemas::enumerate_schemas
#[register_binding(stringify_error)]
#[allow(clippy::too_many_arguments)]
fn path_schema_enumerate(
    ocel: &SlimLinkedOCEL,
    source: TypeRef,
    #[bind(default)] target: Option<TypeRef>,
    max_length: usize,
    #[bind(default)] allow_cycles: bool,
    #[bind(default)] allowed_types: Option<Vec<TypeRef>>,
    #[bind(default = DEFAULT_MAX_SCHEMAS)] max_schemas: usize,
    #[bind(ctx)] ctl: &Control<'_>,
) -> Result<PathSchemaEnumeration, Cancelled> {
    let type_graph = TypeGraph::from_linked_ocel(ocel);
    let allowed: Option<HashSet<TypeRef>> = allowed_types.map(|types| types.into_iter().collect());
    let result = ctl.enumerate_schemas(
        &type_graph,
        &source,
        target.as_ref(),
        max_length,
        allow_cycles,
        allowed.as_ref(),
        max_schemas,
    )?;
    Ok(PathSchemaEnumeration {
        schemas: result
            .schemas
            .iter()
            .map(|sch| sch.resolve(&type_graph))
            .collect(),
        limit_reached: result.limit_reached,
    })
}

/// Find the concrete instance connections of a single (resolved) schema, with metrics and
/// throughput. Entities are referenced by their compact OCEL index.
///
/// See [`Control::find_connections`] for the progress reported.
#[register_binding(stringify_error)]
fn path_schema_connections(
    ocel: &SlimLinkedOCEL,
    schema: ResolvedPathSchema,
    #[bind(default)] params: PathConnectionParams,
    #[bind(ctx)] ctl: &Control<'_>,
) -> Result<PathSchemaConnections, Cancelled> {
    let sources = get_entities_of_type(ocel, &schema.source);
    let total_sources = sources.len();
    let total_targets = get_entities_of_type(ocel, &schema.target).len();
    let result = connections_from(ocel, &schema, &sources, &params, ctl)?;
    let stats = schema_stats(&result.connections, total_sources, total_targets);
    Ok(PathSchemaConnections {
        schema: schema.display(),
        stats,
        connections: result.connections,
        limit_reached: result.limit_reached,
        selectivity_pruned: result.selectivity_pruned,
    })
}
