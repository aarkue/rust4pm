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

use macros_process_mining::{controlled, register_binding};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::core::event_data::object_centric::linked_ocel::slim_linked_ocel::SlimLinkedOCEL;
use crate::utils::control::{Cancelled, Control};

pub use equivalence::{group_by_hash, ConnectionEquivalenceClass};
pub use graph::{
    entity_id, entity_type, get_neighbors_via_edge, EntityKind, EntityRef, Qualifier, TypeEdge,
    TypeGraph, TypeName, TypeRef,
};
pub use metrics::{compute_metrics, compute_throughput_times, throughput_durations, SchemaMetrics};
#[cfg(feature = "bindings")]
pub(crate) use paths::connections_from;
pub use paths::{
    find_connections, get_entities_of_type, summarize_connections, Connection, ConnectionResult,
    ConnectionSummary, EventSelection, PathConnectionParams, TemporalConstraint,
};
pub use schema::{
    enumerate_schemas, PathSchema, ResolvedPathSchema, ResolvedStep, SchemaEnumeration, SchemaStep,
    DEFAULT_MAX_SCHEMAS, MAX_EXPLORED_PATHS, MAX_SCHEMA_LENGTH,
};

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
    /// Maximum number of schemas to enumerate (see [`enumerate_schemas`]).
    #[serde(default = "default_max_schemas")]
    pub max_schemas: usize,
    /// Connection-finding parameters.
    pub params: PathConnectionParams,
}

fn default_max_schemas() -> usize {
    DEFAULT_MAX_SCHEMAS
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
    /// Whether schema enumeration hit a limit, so `schemas` may be incomplete.
    pub schema_limit_reached: bool,
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

/// Discover path schemas between the query types: enumerate schemas, summarize each schema's
/// connections, score it by support, coverage, selectivity, reach, exclusivity and throughput,
/// and group connection-equivalent schemas.
///
/// The source entities (shared by every enumerated schema) are collected once. Each schema
/// is summarized in a single streaming pass (see [`summarize_connections`]) that never
/// materializes the connection list, and connection equivalence is decided by a 64-bit
/// hash of each schema's (source, target) set rather than by retaining the sets.
///
/// Reports progress to a [`Control`] in two stages: `"Enumerating path schemas"` (see
/// [`Control::enumerate_schemas`]), then `"Summarizing path schemas"`, where `done` counts
/// the source entities scanned over all schemas, out of the number of schemas times the number
/// of source entities.
///
/// Streams each schema as soon as it is summarized: one partial value is one
/// [`DiscoveredSchema`], in `index` order, as in [`PathSchemaDiscovery::schemas`]. Only its
/// `equivalence_class` is provisional: the `index` of the first schema with the same
/// connection set so far (its own `index` if none), which the result replaces with an index
/// into [`PathSchemaDiscovery::equivalence_classes`].
#[controlled]
#[register_binding]
pub fn discover_path_schemas(
    ocel: &SlimLinkedOCEL,
    query: &PathSchemaQuery,
    ctl: &Control<'_>,
) -> Result<PathSchemaDiscovery, Cancelled> {
    let type_graph = TypeGraph::from_linked_ocel(ocel);
    let allowed: Option<HashSet<TypeRef>> = query
        .allowed_types
        .as_ref()
        .map(|types| types.iter().cloned().collect());
    let SchemaEnumeration {
        schemas,
        limit_reached: schema_limit_reached,
    } = schema::enumerate_bounded(
        &type_graph,
        &query.source,
        query.target.as_ref(),
        query.max_length,
        query.allow_cycles,
        allowed.as_ref(),
        query.max_schemas,
        ctl,
        false,
    )?;

    let source_entities = get_entities_of_type(ocel, &query.source);
    let total_sources = source_entities.len();

    // Per-target-type entity counts (reused across schemas sharing a target type).
    let mut target_totals: HashMap<&TypeRef, usize> = HashMap::new();
    for sch in &schemas {
        target_totals
            .entry(&sch.target)
            .or_insert_with(|| get_entities_of_type(ocel, &sch.target).len());
    }

    let progress = ctl.counter(
        DISCOVER_STAGE,
        Some((schemas.len() as u64) * (total_sources as u64)),
    );
    let mut rows: Vec<DiscoveredSchema> = Vec::with_capacity(schemas.len());
    let mut keys: Vec<(String, usize, u64)> = Vec::with_capacity(schemas.len());
    let mut first_with_key: HashMap<(usize, u64), usize> = HashMap::new();
    for (i, sch) in schemas.iter().enumerate() {
        let r = sch.resolve(&type_graph);
        let summary =
            paths::summarize_scan(ocel, &r, &source_entities, &query.params, ctl, &progress)?;
        let display = r.display();
        keys.push((display.clone(), summary.support, summary.hash));
        let provisional_class = *first_with_key
            .entry((summary.support, summary.hash))
            .or_insert(i);
        let total_targets = target_totals[&r.target];
        let row = DiscoveredSchema {
            index: i,
            schema: display,
            length: r.steps.len(),
            source: r.source,
            target: r.target,
            is_dead: summary.support == 0,
            selectivity_pruned: summary.selectivity_pruned,
            limit_reached: summary.limit_reached,
            stats: stats_from_summary(summary, total_sources, total_targets),
            equivalence_class: provisional_class,
        };
        ctl.partial(&row);
        rows.push(row);
    }

    let (equivalence_classes, class_of) = group_by_hash(&keys);
    for (row, class) in rows.iter_mut().zip(class_of) {
        row.equivalence_class = class;
    }

    Ok(PathSchemaDiscovery {
        source_type: query.source.name().to_string(),
        total_sources,
        schemas: rows,
        equivalence_classes,
        schema_limit_reached,
    })
}

/// Progress stage of [`Control::discover_path_schemas`] after enumeration, counting source
/// entities over all schemas.
const DISCOVER_STAGE: &str = "Summarizing path schemas";

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
        let schemas =
            enumerate_schemas(&graph, &src, None, 2, false, None, DEFAULT_MAX_SCHEMAS).schemas;
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
        let schema = enumerate_schemas(&graph, &src, None, 2, false, None, DEFAULT_MAX_SCHEMAS)
            .schemas
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
        let schema = enumerate_schemas(&graph, &src, None, 2, false, None, DEFAULT_MAX_SCHEMAS)
            .schemas
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
            enumerate_schemas(g, &src, None, 3, false, None, DEFAULT_MAX_SCHEMAS)
                .schemas
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
            max_schemas: DEFAULT_MAX_SCHEMAS,
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
            max_schemas: DEFAULT_MAX_SCHEMAS,
            params: PathConnectionParams::default(),
        };
        let discovery = discover_path_schemas(&ocel, &query);
        let enumerated = enumerate_schemas(
            &graph,
            &src,
            Some(&target),
            2,
            false,
            None,
            DEFAULT_MAX_SCHEMAS,
        )
        .schemas;

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
        let schema = enumerate_schemas(&graph, &src, None, 2, false, None, DEFAULT_MAX_SCHEMAS)
            .schemas
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
        let from_event = enumerate_schemas(
            &graph,
            &event_order,
            Some(&item),
            1,
            false,
            None,
            DEFAULT_MAX_SCHEMAS,
        )
        .schemas;
        let from_object = enumerate_schemas(
            &graph,
            &object_order,
            Some(&item),
            1,
            false,
            None,
            DEFAULT_MAX_SCHEMAS,
        )
        .schemas;
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

    #[test]
    fn a_connection_cap_keeps_the_same_sources_on_every_run() {
        use std::collections::BTreeSet;
        let ocel = load_ocel();
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let src = first_event_type(&graph);
        let schema = enumerate_schemas(&graph, &src, None, 2, false, None, DEFAULT_MAX_SCHEMAS)
            .schemas
            .into_iter()
            .map(|s| s.resolve(&graph))
            .find(|s| s.len() == 2 && s.target.is_event())
            .expect("an event-to-event schema");
        let sources = get_entities_of_type(&ocel, &schema.source);
        let full = find_connections(&ocel, &schema, &PathConnectionParams::default());
        let max = full.connections.len() / 3;
        assert!(max > 0);

        for dedup_targets in [true, false] {
            let params = PathConnectionParams {
                max_connections: Some(max),
                dedup_targets,
                ..Default::default()
            };
            let pairs = || {
                let result = find_connections(&ocel, &schema, &params);
                assert!(result.limit_reached);
                let mut pairs: Vec<_> = result
                    .connections
                    .iter()
                    .map(|c| (c.source, c.target))
                    .collect();
                pairs.sort();
                pairs
            };
            let summary = || {
                let s = summarize_connections(&ocel, &schema, &sources, &params);
                (s.support, s.sources_with_paths, s.distinct_targets, s.hash)
            };
            let first_pairs = pairs();
            let first_summary = summary();
            for _ in 0..10 {
                assert_eq!(pairs(), first_pairs);
                assert_eq!(summary(), first_summary);
            }

            // The kept sources are the first ones in source order that have connections.
            let kept: BTreeSet<EntityRef> = first_pairs.iter().map(|(s, _)| *s).collect();
            let with_paths: BTreeSet<EntityRef> =
                full.connections.iter().map(|c| c.source).collect();
            let expected: BTreeSet<EntityRef> = sources
                .iter()
                .filter(|s| with_paths.contains(s))
                .take(kept.len())
                .copied()
                .collect();
            assert_eq!(kept, expected);
        }
    }

    #[test]
    fn a_dfs_connection_cap_keeps_the_in_order_prefix() {
        let ocel = load_ocel();
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let src = first_event_type(&graph);
        let schema = enumerate_schemas(&graph, &src, None, 2, false, None, DEFAULT_MAX_SCHEMAS)
            .schemas
            .into_iter()
            .map(|s| s.resolve(&graph))
            .find(|s| s.len() == 2 && s.target.is_event())
            .expect("an event-to-event schema");
        let pairs = |r: &ConnectionResult| -> Vec<_> {
            r.connections.iter().map(|c| (c.source, c.target)).collect()
        };
        for dedup_targets in [true, false] {
            let params = PathConnectionParams {
                dedup_targets,
                ..Default::default()
            };
            let full = pairs(&find_connections(&ocel, &schema, &params));
            assert!(full.len() > 7);
            for max in [1, full.len() / 7, full.len() / 3, full.len() - 1] {
                let capped = find_connections(
                    &ocel,
                    &schema,
                    &PathConnectionParams {
                        max_connections: Some(max),
                        ..params.clone()
                    },
                );
                // Deduplicated targets over distinct types take the set-based scan, which may
                // overshoot the cap.
                if !dedup_targets || schema.source == schema.target {
                    assert_eq!(pairs(&capped), full[..max], "max {}", max);
                }
                assert!(capped.limit_reached);
            }
        }
    }

    #[test]
    fn a_zero_connection_cap_keeps_nothing() {
        let ocel = load_ocel();
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let src = first_event_type(&graph);
        // Distinct types with deduplicated targets take the set-based scan, without
        // deduplication the path-enumerating one.
        let schema = enumerate_schemas(&graph, &src, None, 2, false, None, DEFAULT_MAX_SCHEMAS)
            .schemas
            .into_iter()
            .map(|s| s.resolve(&graph))
            .find(|s| s.len() == 2 && s.target.is_event() && s.source != s.target)
            .expect("an event-to-event schema over distinct types");
        let sources = get_entities_of_type(&ocel, &schema.source);
        for dedup_targets in [true, false] {
            let params = PathConnectionParams {
                max_connections: Some(0),
                dedup_targets,
                ..Default::default()
            };
            let result = find_connections(&ocel, &schema, &params);
            assert!(result.connections.is_empty());
            assert!(result.limit_reached);
            let summary = summarize_connections(&ocel, &schema, &sources, &params);
            assert_eq!(summary.support, 0);
            assert!(summary.limit_reached);
        }
    }

    #[test]
    fn controlled_connection_scan_stops_once_cancelled() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let ocel = load_ocel();
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let src = first_event_type(&graph);
        let schema = enumerate_schemas(&graph, &src, None, 2, false, None, DEFAULT_MAX_SCHEMAS)
            .schemas
            .into_iter()
            .map(|s| s.resolve(&graph))
            .find(|s| s.len() == 2)
            .expect("a schema of length 2");
        let flag = AtomicBool::new(false);
        let ctl = Control::new()
            .on_progress(|_| flag.store(true, Ordering::Relaxed))
            .cancel_flag(&flag);
        for dedup_targets in [true, false] {
            let params = PathConnectionParams {
                dedup_targets,
                ..Default::default()
            };
            flag.store(false, Ordering::Relaxed);
            assert_eq!(
                ctl.find_connections(&ocel, &schema, &params).unwrap_err(),
                Cancelled
            );
        }
    }

    /// One event of type `a` related to one object of type `o` once per qualifier, so the
    /// type graph has one `a`-`o` edge per qualifier and cyclic paths of any length.
    fn cyclic_ocel(qualifiers: &[&str]) -> SlimLinkedOCEL {
        use crate::core::event_data::object_centric::ocel_struct::{
            OCELEvent, OCELObject, OCELRelationship, OCELType, OCEL,
        };
        use chrono::DateTime;

        let ty = |name: &str| OCELType {
            name: name.to_string(),
            attributes: vec![],
        };
        SlimLinkedOCEL::from_ocel(OCEL {
            event_types: vec![ty("a")],
            object_types: vec![ty("o"), ty("unrelated")],
            events: vec![OCELEvent {
                id: "e1".to_string(),
                event_type: "a".to_string(),
                time: DateTime::parse_from_rfc3339("2020-01-01T00:00:00+00:00").unwrap(),
                attributes: vec![],
                relationships: qualifiers
                    .iter()
                    .map(|q| OCELRelationship {
                        object_id: "o1".to_string(),
                        qualifier: q.to_string(),
                    })
                    .collect(),
            }],
            objects: vec![OCELObject {
                id: "o1".to_string(),
                object_type: "o".to_string(),
                attributes: vec![],
                relationships: vec![],
            }],
        })
    }

    #[test]
    fn cyclic_enumeration_stops_at_the_schema_cap() {
        let ocel = cyclic_ocel(&["q1", "q2"]);
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let src = TypeRef::Event("a".to_string());

        // 2 + 4 + 8 schemas, below every limit.
        let full = enumerate_schemas(&graph, &src, None, 3, true, None, 14);
        assert_eq!(full.schemas.len(), 14);
        assert!(!full.limit_reached);
        assert_eq!(
            full.schemas,
            enumerate_schemas(&graph, &src, None, 3, true, None, DEFAULT_MAX_SCHEMAS).schemas
        );

        let capped = enumerate_schemas(&graph, &src, None, 3, true, None, 5);
        assert!(capped.limit_reached);
        assert_eq!(capped.schemas, full.schemas[..5]);

        // 2^40 paths without a cap.
        assert_eq!(
            enumerate_schemas(&graph, &src, None, 40, true, None, DEFAULT_MAX_SCHEMAS)
                .schemas
                .len(),
            DEFAULT_MAX_SCHEMAS
        );
        let discovery = discover_path_schemas(
            &ocel,
            &PathSchemaQuery {
                source: src,
                target: None,
                max_length: 40,
                allow_cycles: true,
                allowed_types: None,
                max_schemas: 50,
                params: PathConnectionParams::default(),
            },
        );
        assert_eq!(discovery.schemas.len(), 50);
        assert!(discovery.schema_limit_reached);
    }

    #[test]
    fn cyclic_enumeration_stops_at_the_exploration_cap() {
        let ocel = cyclic_ocel(&["q1", "q2"]);
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let src = TypeRef::Event("a".to_string());
        let unreachable = TypeRef::Object("unrelated".to_string());
        let result =
            enumerate_schemas(&graph, &src, Some(&unreachable), 40, true, None, usize::MAX);
        assert!(result.schemas.is_empty());
        assert!(result.limit_reached);
    }

    #[test]
    fn max_length_is_clamped() {
        let ocel = cyclic_ocel(&["q"]);
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let src = TypeRef::Event("a".to_string());
        let at_cap = enumerate_schemas(&graph, &src, None, MAX_SCHEMA_LENGTH, true, None, 1000);
        assert_eq!(at_cap.schemas.len(), MAX_SCHEMA_LENGTH);
        assert!(!at_cap.limit_reached);
        let above = enumerate_schemas(&graph, &src, None, usize::MAX, true, None, 1000);
        assert_eq!(above.schemas, at_cap.schemas);
        assert!(above.limit_reached);
        // Without cycles, nothing is cut off at the clamped length.
        let acyclic = enumerate_schemas(&graph, &src, None, usize::MAX, false, None, 1000);
        assert_eq!(acyclic.schemas.len(), 1);
        assert!(!acyclic.limit_reached);
    }

    fn discovery_query(graph: &TypeGraph) -> PathSchemaQuery {
        PathSchemaQuery {
            source: first_event_type(graph),
            target: None,
            max_length: 2,
            allow_cycles: false,
            allowed_types: None,
            max_schemas: DEFAULT_MAX_SCHEMAS,
            params: PathConnectionParams::default(),
        }
    }

    #[test]
    fn controlled_discovery_streams_each_schema_and_reports_sources() {
        use std::sync::Mutex;
        let ocel = load_ocel();
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let query = discovery_query(&graph);
        let progress = Mutex::new(Vec::new());
        let partials = Mutex::new(Vec::new());
        let ctl = Control::new()
            .on_progress(|p| progress.lock().unwrap().push(p))
            .on_partial(|v| partials.lock().unwrap().push(v));
        let discovery = ctl.discover_path_schemas(&ocel, &query).unwrap();
        drop(ctl);

        let plain = discover_path_schemas(&ocel, &query);
        assert_eq!(
            serde_json::to_value(&discovery).unwrap(),
            serde_json::to_value(&plain).unwrap()
        );

        let partials = partials.into_inner().unwrap();
        assert_eq!(partials.len(), discovery.schemas.len());
        for (partial, row) in partials.iter().zip(&discovery.schemas) {
            let partial: DiscoveredSchema = serde_json::from_value(partial.clone()).unwrap();
            let first_same = discovery
                .schemas
                .iter()
                .position(|r| r.equivalence_class == row.equivalence_class)
                .unwrap();
            assert_eq!(partial.equivalence_class, first_same);
            let mut expected = serde_json::to_value(row).unwrap();
            expected["equivalence_class"] = serde_json::json!(first_same);
            assert_eq!(serde_json::to_value(&partial).unwrap(), expected);
        }

        let progress = progress.into_inner().unwrap();
        let summarize: Vec<_> = progress
            .iter()
            .filter(|p| p.stage == DISCOVER_STAGE)
            .collect();
        let total = (discovery.schemas.len() * discovery.total_sources) as u64;
        assert!(summarize.iter().all(|p| p.total == Some(total)));
        assert!(summarize.windows(2).all(|w| w[0].done <= w[1].done));
        assert_eq!(summarize.last().unwrap().done, total);
        assert!(progress.iter().any(|p| p.stage == schema::ENUMERATE_STAGE));
    }

    #[test]
    fn controlled_discovery_stops_once_cancelled() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let ocel = load_ocel();
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let flag = AtomicBool::new(false);
        let ctl = Control::new()
            .on_progress(|p| {
                if p.stage == DISCOVER_STAGE && p.done > 0 {
                    flag.store(true, Ordering::Relaxed);
                }
            })
            .cancel_flag(&flag);
        assert_eq!(
            ctl.discover_path_schemas(&ocel, &discovery_query(&graph))
                .unwrap_err(),
            Cancelled
        );
    }

    #[test]
    fn controlled_enumeration_streams_resolved_schemas() {
        use std::sync::Mutex;
        let ocel = cyclic_ocel(&["q1", "q2"]);
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let src = TypeRef::Event("a".to_string());
        let partials = Mutex::new(Vec::new());
        let ctl = Control::new().on_partial(|v| partials.lock().unwrap().push(v));
        let result = ctl
            .enumerate_schemas(&graph, &src, None, 3, true, None, 14)
            .unwrap();
        drop(ctl);
        let expected: Vec<_> = result
            .schemas
            .iter()
            .map(|s| serde_json::to_value(s.resolve(&graph)).unwrap())
            .collect();
        assert_eq!(partials.into_inner().unwrap(), expected);
    }
}
