//! Path-based entity linking over OCEL type graphs ("path schemas").
//!
//! Discovers how events/objects of different types are connected through chains of
//! intermediate entities, ranking type-level path schemas by support / coverage /
//! selectivity / reach / exclusivity and event-to-event throughput.
//!
//! Main entry points:
//! - [`TypeGraph::from_linked_ocel`] builds the type graph.
//! - [`enumerate_schemas`] lists candidate path schemas between given types.
//! - [`find_connections`] returns all concrete instance connections for one schema.
//! - [`schema_stats`] computes metrics + throughput for one schema's connections.
//! - [`discover_path_schemas`] runs the full pipeline: enumerate, connect, score, and
//!   group connection-equivalent schemas.

/// Connection-equivalence classes.
mod equivalence;
/// Type graph construction.
mod graph;
/// Schema metric computation.
mod metrics;
/// Instance-level path search.
mod paths;
/// Type-level schema enumeration.
mod schema;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::core::event_data::object_centric::linked_ocel::slim_linked_ocel::SlimLinkedOCEL;

pub use equivalence::{group_by_hash, ConnectionEquivalenceClass};
pub use graph::{
    entity_id, entity_type, get_neighbors_via_edge, EntityKind, EntityRef, Qualifier, TypeEdge,
    TypeGraph, TypeName, TypeRef,
};
pub use metrics::{compute_metrics, compute_throughput_times, throughput_durations, SchemaMetrics};
pub use paths::{
    find_connections, find_connections_with_sources, get_entities_of_type, summarize_connections,
    Connection, ConnectionResult, ConnectionSummary, EventSelection, PathConnectionParams,
    TemporalConstraint,
};
pub use schema::{enumerate_schemas, PathSchema, ResolvedPathSchema, ResolvedStep, SchemaStep};

/// Throughput time statistics (seconds) over event-to-event connections.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ThroughputStats {
    /// Minimum duration in seconds.
    pub min: f64,
    /// Maximum duration in seconds.
    pub max: f64,
    /// Mean duration in seconds.
    pub mean: f64,
    /// Median duration in seconds.
    pub median: f64,
}

impl ThroughputStats {
    fn from_parts((min, max, mean, median): (f64, f64, f64, f64)) -> Self {
        Self {
            min,
            max,
            mean,
            median,
        }
    }
}

/// Metrics plus optional throughput for a single schema.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SchemaStats {
    /// Schema quality metrics.
    pub metrics: SchemaMetrics,
    /// Event-to-event throughput times, if both endpoints are events.
    pub throughput: Option<ThroughputStats>,
}

/// A discovery query: source/target types, max schema length, and connection params.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PathSchemaQuery {
    /// Source type to start schemas from.
    pub source: TypeRef,
    /// Optional target type; if `None`, schemas to any type are enumerated.
    pub target: Option<TypeRef>,
    /// Maximum number of steps per schema.
    pub max_length: usize,
    /// Whether a schema may revisit the same type.
    pub allow_cycles: bool,
    /// Optional set of types the intermediate steps may pass through; `None` allows all. The
    /// source (the start) and the target (when one is given) are always permitted, so only the
    /// steps in between are constrained.
    pub allowed_types: Option<Vec<TypeRef>>,
    /// Connection-finding parameters.
    pub params: PathConnectionParams,
}

/// One enumerated schema with its computed stats and equivalence class.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DiscoveredSchema {
    /// Enumeration index (stable for a given `source`/`target`/`max_length`/`allowed_types`).
    pub index: usize,
    /// Human-readable schema string.
    pub schema: String,
    /// Source type.
    pub source: TypeRef,
    /// Target type.
    pub target: TypeRef,
    /// Number of steps in the schema.
    pub length: usize,
    /// Computed metrics and throughput.
    pub stats: SchemaStats,
    /// Whether the schema has zero connections.
    pub is_dead: bool,
    /// Whether selectivity-based early termination was triggered.
    pub selectivity_pruned: bool,
    /// Whether the connection limit was reached (results may be incomplete).
    pub limit_reached: bool,
    /// Index into [`PathSchemaDiscovery::equivalence_classes`].
    pub equivalence_class: usize,
}

/// Result of [`discover_path_schemas`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PathSchemaDiscovery {
    /// Source entity type the query started from.
    pub source_type: String,
    /// Total number of source-type entities.
    pub total_sources: usize,
    /// Enumerated schemas with their stats.
    pub schemas: Vec<DiscoveredSchema>,
    /// Connection-equivalence classes over the enumerated schemas.
    pub equivalence_classes: Vec<ConnectionEquivalenceClass>,
}

/// Compute metrics + throughput for one schema's connections.
pub fn schema_stats(
    connections: &[Connection],
    total_sources: usize,
    total_targets: usize,
) -> SchemaStats {
    SchemaStats {
        metrics: metrics::compute_metrics(connections, total_sources, total_targets),
        throughput: metrics::compute_throughput_times(connections).map(ThroughputStats::from_parts),
    }
}

fn stats_from_summary(
    summary: ConnectionSummary,
    total_sources: usize,
    total_targets: usize,
) -> SchemaStats {
    SchemaStats {
        metrics: metrics::metrics_from_counts(
            summary.support,
            summary.sources_with_paths,
            total_sources,
            summary.distinct_targets,
            total_targets,
        ),
        throughput: metrics::throughput_from_durations(summary.durations)
            .map(ThroughputStats::from_parts),
    }
}

/// Run the full discovery pipeline: enumerate schemas, summarize each schema's connections,
/// score them, and group connection-equivalent schemas.
///
/// The source entities (shared by every enumerated schema) are collected once. Each schema
/// is summarized in a single streaming pass (see [`summarize_connections`]) that never
/// materializes the connection list, and connection equivalence is decided by a 64-bit
/// hash of each schema's (source, target) set rather than by retaining the sets.
pub fn discover_path_schemas(
    ocel: &SlimLinkedOCEL,
    query: &PathSchemaQuery,
) -> PathSchemaDiscovery {
    let type_graph = TypeGraph::from_linked_ocel(ocel);
    let allowed: Option<HashSet<TypeRef>> = query
        .allowed_types
        .as_ref()
        .map(|types| types.iter().cloned().collect());
    let schemas = enumerate_schemas(
        &type_graph,
        &query.source,
        query.target.as_ref(),
        query.max_length,
        query.allow_cycles,
        allowed.as_ref(),
    );

    let source_entities = get_entities_of_type(ocel, &query.source);
    let total_sources = source_entities.len();

    // Per-target-type entity counts (reused across schemas sharing a target type).
    let mut target_totals: HashMap<&TypeRef, usize> = HashMap::new();
    for sch in &schemas {
        target_totals
            .entry(&sch.target)
            .or_insert_with(|| get_entities_of_type(ocel, &sch.target).len());
    }

    let resolved: Vec<ResolvedPathSchema> =
        schemas.iter().map(|s| s.resolve(&type_graph)).collect();
    let displays: Vec<String> = resolved.iter().map(ResolvedPathSchema::display).collect();
    let summaries: Vec<ConnectionSummary> = resolved
        .iter()
        .map(|r| summarize_connections(ocel, r, &source_entities, &query.params))
        .collect();

    let hashes: Vec<(String, usize, u64)> = displays
        .iter()
        .zip(&summaries)
        .map(|(display, d)| (display.clone(), d.support, d.hash))
        .collect();
    let (equivalence_classes, class_of) = group_by_hash(&hashes);

    let schemas_out: Vec<DiscoveredSchema> = resolved
        .into_iter()
        .zip(displays)
        .zip(summaries)
        .enumerate()
        .map(|(i, ((r, display), summary))| {
            let total_targets = target_totals[&r.target];
            let is_dead = summary.support == 0;
            let limit_reached = summary.limit_reached;
            let selectivity_pruned = summary.selectivity_pruned;
            DiscoveredSchema {
                index: i,
                schema: display,
                source: r.source,
                target: r.target,
                length: r.steps.len(),
                stats: stats_from_summary(summary, total_sources, total_targets),
                is_dead,
                selectivity_pruned,
                limit_reached,
                equivalence_class: class_of[i],
            }
        })
        .collect();

    PathSchemaDiscovery {
        source_type: query.source.name().to_string(),
        total_sources,
        schemas: schemas_out,
        equivalence_classes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::io::Importable;
    use crate::test_utils::get_test_data_path;

    fn load_ocel() -> SlimLinkedOCEL {
        let path = get_test_data_path()
            .join("ocel")
            .join("order-management.json");
        SlimLinkedOCEL::import_from_path(&path).expect("import order-management.json")
    }

    fn first_event_type(graph: &TypeGraph) -> TypeRef {
        TypeRef::Event(graph.event_types[0].clone())
    }

    #[test]
    fn enumerate_resolve_roundtrip() {
        let ocel = load_ocel();
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let src = first_event_type(&graph);
        let schemas = enumerate_schemas(&graph, &src, None, 2, false, None);
        assert!(!schemas.is_empty());
        for sch in &schemas {
            assert!(sch.len() <= 2);
            let resolved = sch.resolve(&graph);
            assert_eq!(resolved.steps.len(), sch.len());
            assert_eq!(resolved.source, src);
            assert!(!resolved.display().is_empty());
            // The index-based and resolved displays agree.
            assert_eq!(sch.display(&graph), resolved.display());
        }
    }

    #[test]
    fn forward_temporal_is_subset_of_unconstrained() {
        let ocel = load_ocel();
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let src = first_event_type(&graph);
        // An event-to-event schema (length 2) so timestamps actually constrain the path.
        let schema = enumerate_schemas(&graph, &src, None, 2, false, None)
            .into_iter()
            .map(|s| s.resolve(&graph))
            .find(|s| s.len() == 2 && s.target.is_event())
            .expect("an event-to-event schema");

        let unconstrained = find_connections(
            &ocel,
            &schema,
            &PathConnectionParams {
                temporal: TemporalConstraint::None,
                ..Default::default()
            },
        );
        let forward = find_connections(
            &ocel,
            &schema,
            &PathConnectionParams {
                temporal: TemporalConstraint::Forward,
                ..Default::default()
            },
        );
        assert!(forward.connections.len() <= unconstrained.connections.len());
        // Event-to-event connections carry throughput.
        let stats = schema_stats(&forward.connections, 0, 0);
        if !forward.connections.is_empty() {
            assert!(stats.throughput.is_some());
        }
    }

    #[test]
    fn last_selection_keeps_one_target_per_source() {
        let ocel = load_ocel();
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let src = first_event_type(&graph);
        let schema = enumerate_schemas(&graph, &src, None, 2, false, None)
            .into_iter()
            .map(|s| s.resolve(&graph))
            .find(|s| s.target.is_event())
            .expect("a schema ending at an event type");

        let last = find_connections(
            &ocel,
            &schema,
            &PathConnectionParams {
                selection: EventSelection::Last,
                ..Default::default()
            },
        );
        let mut sources: Vec<_> = last.connections.iter().map(|c| c.source).collect();
        sources.sort();
        let unique = sources.len();
        sources.dedup();
        assert_eq!(unique, sources.len(), "at most one connection per source");
    }

    #[test]
    fn enumeration_order_is_deterministic() {
        let ocel = load_ocel();
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let src = first_event_type(&graph);
        // Discovery indices are stable because the type-graph edges (and thus enumeration
        // order) are deterministic; verifying that here is cheap and sufficient.
        let displays = |g: &TypeGraph| {
            enumerate_schemas(g, &src, None, 3, false, None)
                .iter()
                .map(|s| s.display(g))
                .collect::<Vec<_>>()
        };
        let regraph = TypeGraph::from_linked_ocel(&ocel);
        assert_eq!(displays(&graph), displays(&regraph));
    }

    #[test]
    fn discovery_rows_are_consistent() {
        let ocel = load_ocel();
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let query = PathSchemaQuery {
            source: first_event_type(&graph),
            target: None,
            max_length: 1,
            allow_cycles: false,
            allowed_types: None,
            params: PathConnectionParams::default(),
        };
        let discovery = discover_path_schemas(&ocel, &query);

        assert!(!discovery.schemas.is_empty());
        for (i, row) in discovery.schemas.iter().enumerate() {
            assert_eq!(row.index, i, "indices are dense and ordered");
            assert!(row.equivalence_class < discovery.equivalence_classes.len());
            assert_eq!(row.is_dead, row.stats.metrics.path_count == 0);
            // Metrics computed with real source/target totals stay normalized.
            let m = &row.stats.metrics;
            assert_eq!(m.total_sources, discovery.total_sources);
            for fraction in [m.coverage, m.selectivity, m.reach, m.exclusivity] {
                assert!(
                    (0.0..=1.0).contains(&fraction),
                    "metric out of range: {fraction}"
                );
            }
        }
    }

    #[test]
    fn hash_equivalence_matches_exact_sets() {
        use std::collections::HashSet;
        let ocel = load_ocel();
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let src = first_event_type(&graph);
        let target = TypeRef::Object(graph.object_types[0].clone());
        let query = PathSchemaQuery {
            source: src.clone(),
            target: Some(target.clone()),
            max_length: 2,
            allow_cycles: false,
            allowed_types: None,
            params: PathConnectionParams::default(),
        };
        let discovery = discover_path_schemas(&ocel, &query);
        let enumerated = enumerate_schemas(&graph, &src, Some(&target), 2, false, None);

        // Exact (source, target) set per discovered schema, via the materializing path.
        let exact: Vec<HashSet<(EntityRef, EntityRef)>> = discovery
            .schemas
            .iter()
            .map(|row| {
                let resolved = enumerated[row.index].resolve(&graph);
                find_connections(&ocel, &resolved, &PathConnectionParams::default())
                    .connections
                    .iter()
                    .map(|c| (c.source, c.target))
                    .collect()
            })
            .collect();

        for i in 0..discovery.schemas.len() {
            for j in (i + 1)..discovery.schemas.len() {
                let same_class = discovery.schemas[i].equivalence_class
                    == discovery.schemas[j].equivalence_class;
                let same_set = exact[i] == exact[j];
                assert_eq!(same_class, same_set, "schemas {i} and {j}");
            }
        }
    }

    #[test]
    fn bfs_fast_path_matches_dfs() {
        use std::collections::HashSet;
        let ocel = load_ocel();
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let src = first_event_type(&graph);
        let schema = enumerate_schemas(&graph, &src, None, 2, false, None)
            .into_iter()
            .map(|s| s.resolve(&graph))
            .find(|s| s.len() == 2 && s.target.is_event())
            .expect("an event-to-event schema");

        let pair_set = |r: &ConnectionResult| {
            r.connections
                .iter()
                .map(|c| (c.source, c.target))
                .collect::<HashSet<_>>()
        };
        let week = 7 * 24 * 3600;
        for temporal in [
            TemporalConstraint::None,
            TemporalConstraint::Forward,
            TemporalConstraint::Bounded(week),
        ] {
            // Default params take the frontier-BFS fast path; disabling target dedup forces
            // the path-enumerating DFS. Both must reach the same set of (source, target) pairs.
            let bfs = find_connections(
                &ocel,
                &schema,
                &PathConnectionParams {
                    temporal,
                    ..Default::default()
                },
            );
            let dfs = find_connections(
                &ocel,
                &schema,
                &PathConnectionParams {
                    temporal,
                    dedup_targets: false,
                    ..Default::default()
                },
            );
            assert_eq!(pair_set(&bfs), pair_set(&dfs), "temporal = {temporal:?}");
        }
    }

    #[test]
    fn same_name_event_and_object_types_stay_distinct() {
        use crate::core::event_data::object_centric::ocel_struct::{
            OCELEvent, OCELObject, OCELRelationship, OCELType, OCEL,
        };
        use chrono::DateTime;

        let ty = |name: &str| OCELType {
            name: name.to_string(),
            attributes: vec![],
        };
        let rel = |object_id: &str, qualifier: &str| OCELRelationship {
            object_id: object_id.to_string(),
            qualifier: qualifier.to_string(),
        };
        let time = DateTime::parse_from_rfc3339("2020-01-01T00:00:00+00:00").unwrap();

        // An event type "order" and an object type "order" share a name (allowed by OCEL).
        let ocel = OCEL {
            event_types: vec![ty("order")],
            object_types: vec![ty("order"), ty("item")],
            events: vec![OCELEvent {
                id: "e1".to_string(),
                event_type: "order".to_string(),
                time,
                attributes: vec![],
                relationships: vec![rel("i1", "handles")],
            }],
            objects: vec![
                OCELObject {
                    id: "i1".to_string(),
                    object_type: "item".to_string(),
                    attributes: vec![],
                    relationships: vec![],
                },
                OCELObject {
                    id: "o1".to_string(),
                    object_type: "order".to_string(),
                    attributes: vec![],
                    relationships: vec![rel("i1", "contains")],
                },
            ],
        };

        let slim = SlimLinkedOCEL::from_ocel(ocel);
        let graph = TypeGraph::from_linked_ocel(&slim);
        let event_order = TypeRef::Event("order".to_string());
        let object_order = TypeRef::Object("order".to_string());
        let item = TypeRef::Object("item".to_string());

        // The two same-name types resolve to disjoint instance sets.
        assert_eq!(get_entities_of_type(&slim, &event_order).len(), 1);
        assert_eq!(get_entities_of_type(&slim, &object_order).len(), 1);

        // Each reaches "item", but via a kind-distinct first edge (E2O vs O2O).
        let from_event = enumerate_schemas(&graph, &event_order, Some(&item), 1, false, None);
        let from_object = enumerate_schemas(&graph, &object_order, Some(&item), 1, false, None);
        assert!(!from_event.is_empty());
        assert!(!from_object.is_empty());
        assert!(from_event[0].resolve(&graph).steps[0]
            .edge
            .source
            .is_event());
        assert!(!from_object[0].resolve(&graph).steps[0]
            .edge
            .source
            .is_event());
    }
}
