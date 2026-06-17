use super::graph::{get_neighbors_via_edge, EntityRef, TypeRef};
use super::schema::{ResolvedPathSchema, ResolvedStep};
use crate::core::event_data::object_centric::linked_ocel::{
    slim_linked_ocel::SlimLinkedOCEL, LinkedOCELAccess,
};
use chrono::{DateTime, FixedOffset, TimeDelta};
use hashbrown::{HashMap, HashSet};
use rayon::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// A discovered connection between two entities, with timestamps.
///
/// Only source and target are materialized (not the full intermediate path).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Connection {
    /// Source entity of the connection.
    pub source: EntityRef,
    /// Target entity of the connection.
    pub target: EntityRef,
    /// Timestamp of the source (only present if the source is an event).
    pub source_time: Option<DateTime<FixedOffset>>,
    /// Timestamp of the target (only present if the target is an event).
    pub target_time: Option<DateTime<FixedOffset>>,
}

/// Temporal constraint applied along a path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum TemporalConstraint {
    /// No temporal constraint.
    None,
    /// Event timestamps must be non-decreasing along the path.
    Forward,
    /// Every event must lie within an absolute window (in seconds) around the source event.
    Bounded(u64),
}

/// Strategy for choosing target events when several are reachable from one source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum EventSelection {
    /// Keep all reachable targets.
    All,
    /// Keep only the earliest target event per source.
    First,
    /// Keep only the latest target event per source.
    Last,
    /// Keep the target event closest in time to the source event.
    Closest,
}

/// Parameters for connection finding.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PathConnectionParams {
    /// Temporal constraint applied along each path.
    pub temporal: TemporalConstraint,
    /// Which target event(s) to keep per source.
    pub selection: EventSelection,
    /// Global cap on the number of connections: a coarse safety limit, checked between
    /// sources, so a single high-fan-out source can overshoot it.
    pub max_connections: Option<usize>,
    /// Store only one connection per (source, target) pair.
    pub dedup_targets: bool,
    /// Terminate early once selectivity is provably below this threshold.
    pub selectivity_threshold: Option<f64>,
}

impl Default for PathConnectionParams {
    fn default() -> Self {
        Self {
            temporal: TemporalConstraint::None,
            selection: EventSelection::All,
            max_connections: None,
            dedup_targets: true,
            selectivity_threshold: None,
        }
    }
}

/// Result of a connection-finding run.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConnectionResult {
    /// The discovered connections.
    pub connections: Vec<Connection>,
    /// Whether the global `max_connections` limit was hit (results may be incomplete).
    pub limit_reached: bool,
    /// Whether the selectivity threshold triggered early termination. Best-effort under
    /// parallel evaluation: it never affects the returned connection set, only whether the
    /// scan stopped early, so this flag can vary run to run near the threshold.
    pub selectivity_pruned: bool,
}

/// Selectivity-based early termination shared by the connection scans.
///
/// Selectivity is `active / support`: distinct connected sources over distinct (source,
/// target) pairs. After each processed source, the highest selectivity the rest of the scan
/// could still reach is `(active + remaining) / (support + remaining)`, since at best every
/// unprocessed source adds one new target and lifts both counts by one. Once that bound falls
/// below the threshold it can never recover, so the scan stops.
///
/// Best-effort under parallel evaluation: termination only stops the scan early, it never
/// changes which connections are returned.
struct SelectivityGuard {
    threshold: f64,
    total_sources: usize,
    processed: AtomicUsize,
    active: AtomicUsize,
    support: AtomicUsize,
    terminated: AtomicBool,
}

impl SelectivityGuard {
    fn new(threshold: f64, total_sources: usize) -> Self {
        Self {
            threshold,
            total_sources,
            processed: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            support: AtomicUsize::new(0),
            terminated: AtomicBool::new(false),
        }
    }

    fn terminated(&self) -> bool {
        self.terminated.load(Ordering::Relaxed)
    }

    /// Record one processed source contributing `num_targets`, updating the termination flag.
    fn record(&self, num_targets: usize) {
        let processed = self.processed.fetch_add(1, Ordering::Relaxed) + 1;
        if num_targets > 0 {
            self.active.fetch_add(1, Ordering::Relaxed);
            self.support.fetch_add(num_targets, Ordering::Relaxed);
        }
        let active = self.active.load(Ordering::Relaxed);
        let support = self.support.load(Ordering::Relaxed);
        let remaining = self.total_sources.saturating_sub(processed);
        let denominator = support + remaining;
        if denominator > 0 && ((active + remaining) as f64 / denominator as f64) < self.threshold {
            self.terminated.store(true, Ordering::Relaxed);
        }
    }
}

/// All entity references of a given type.
pub fn get_entities_of_type(ocel: &SlimLinkedOCEL, type_ref: &TypeRef) -> Vec<EntityRef> {
    match type_ref {
        TypeRef::Event(name) => ocel
            .get_evs_of_type(name)
            .map(|e| EntityRef::Event(*e))
            .collect(),
        TypeRef::Object(name) => ocel
            .get_obs_of_type(name)
            .map(|o| EntityRef::Object(*o))
            .collect(),
    }
}

/// Find all connections for a single (resolved) schema across every source-type entity.
///
/// Memory-efficient depth-first traversal: only source-target pairs (with timestamps) are
/// materialized, using O(schema length) stack per source. See [`PathConnectionParams`].
pub fn find_connections(
    ocel: &SlimLinkedOCEL,
    schema: &ResolvedPathSchema,
    params: &PathConnectionParams,
) -> ConnectionResult {
    let sources = get_entities_of_type(ocel, &schema.source);
    find_connections_with_sources(ocel, schema, &sources, params)
}

/// Like [`find_connections`] but with the source entities supplied by the caller.
///
/// Lets a batch caller (e.g. discovery over many schemas of the same source type) collect
/// the source entities once and reuse them.
pub fn find_connections_with_sources(
    ocel: &SlimLinkedOCEL,
    schema: &ResolvedPathSchema,
    source_entities: &[EntityRef],
    params: &PathConnectionParams,
) -> ConnectionResult {
    let total_sources = source_entities.len();
    if total_sources == 0 {
        return ConnectionResult {
            connections: Vec::new(),
            limit_reached: false,
            selectivity_pruned: false,
        };
    }

    let bound = temporal_bound(params.temporal);
    let connection_count = AtomicUsize::new(0);
    let selectivity = params
        .selectivity_threshold
        .map(|theta| SelectivityGuard::new(theta, total_sources));

    // Prefer the set-based frontier (collapses the path explosion on high-degree hubs) when
    // it stays exact: deduplicated targets and no repeated types (so no entity recurs within
    // a path). Otherwise fall back to the path-enumerating DFS.
    let use_bfs = params.dedup_targets && has_distinct_types(schema);

    let walk = Walk {
        ocel,
        steps: &schema.steps,
        temporal: params.temporal,
        bound,
        max_connections: params.max_connections,
        connection_count: &connection_count,
    };

    let connections: Vec<Connection> = source_entities
        .par_iter()
        .flat_map(|&source| {
            if selectivity.as_ref().is_some_and(|g| g.terminated()) || walk.limit_reached() {
                return Vec::new();
            }
            let source_time = event_time(ocel, &source);

            let (conns, num_targets) = if use_bfs {
                let targets = reachable_targets(
                    ocel,
                    &schema.steps,
                    params.temporal,
                    bound,
                    source,
                    source_time,
                );
                connection_count.fetch_add(targets.len(), Ordering::Relaxed);
                let num_targets = targets.len();
                let conns = targets
                    .into_iter()
                    .map(|target| Connection {
                        source,
                        target,
                        source_time,
                        target_time: event_time(ocel, &target),
                    })
                    .collect();
                (conns, num_targets)
            } else {
                let mut state = SourceState {
                    source,
                    source_time,
                    visited: vec![source],
                    seen_targets: params.dedup_targets.then(HashSet::new),
                    connections: Vec::new(),
                };
                walk.dfs(&mut state, 0, source, source_time);
                // Only the selectivity guard reads this; skip the work when it is absent.
                let num_targets = match (&selectivity, &state.seen_targets) {
                    (None, _) => 0,
                    (Some(_), Some(seen)) => seen.len(),
                    (Some(_), None) => state
                        .connections
                        .iter()
                        .map(|c| c.target)
                        .collect::<HashSet<_>>()
                        .len(),
                };
                (state.connections, num_targets)
            };

            if let Some(guard) = &selectivity {
                guard.record(num_targets);
            }

            conns
        })
        .collect();

    let limit_reached = walk.limit_reached();
    let connections = apply_event_selection(connections, params.selection);
    ConnectionResult {
        connections,
        limit_reached,
        selectivity_pruned: selectivity.is_some_and(|g| g.terminated()),
    }
}

/// Compact, non-materializing summary of one schema's connections for batch discovery.
///
/// Folds the metric inputs, throughput durations and an order-independent 64-bit
/// hash of the (source, target) set in a single pass, without retaining the
/// connections themselves.
#[derive(Debug, Clone, Default)]
pub struct ConnectionSummary {
    /// Number of distinct (source, target) pairs.
    pub support: usize,
    /// Number of distinct source entities with at least one connection.
    pub sources_with_paths: usize,
    /// Number of distinct target entities reached.
    pub distinct_targets: usize,
    /// Event-to-event throughput durations in seconds.
    pub durations: Vec<f64>,
    /// Order-independent hash of the (source, target) set (equal sets, equal value).
    pub hash: u64,
    /// Whether the global connection limit was hit.
    pub limit_reached: bool,
    /// Whether the selectivity threshold triggered early termination.
    pub selectivity_pruned: bool,
}

#[derive(Default)]
struct SummaryAcc {
    support: usize,
    sources_with_paths: usize,
    targets: HashSet<EntityRef>,
    durations: Vec<f64>,
    hash: u64,
}

/// Summarize a schema's connections without materializing them (see [`ConnectionSummary`]).
///
/// Uses the set-based frontier traversal for the common case; falls back to materializing
/// via [`find_connections_with_sources`] when repeated types or non-deduped targets require
/// the path-enumerating DFS.
pub fn summarize_connections(
    ocel: &SlimLinkedOCEL,
    schema: &ResolvedPathSchema,
    source_entities: &[EntityRef],
    params: &PathConnectionParams,
) -> ConnectionSummary {
    let total_sources = source_entities.len();
    if total_sources == 0 {
        return ConnectionSummary::default();
    }

    let use_bfs = params.dedup_targets && has_distinct_types(schema);
    if !use_bfs {
        return summary_from_connections(&find_connections_with_sources(
            ocel,
            schema,
            source_entities,
            params,
        ));
    }

    let bound = temporal_bound(params.temporal);
    let connection_count = AtomicUsize::new(0);
    let selectivity = params
        .selectivity_threshold
        .map(|theta| SelectivityGuard::new(theta, total_sources));

    let acc = source_entities
        .par_iter()
        .fold(SummaryAcc::default, |mut acc, &source| {
            if selectivity.as_ref().is_some_and(|g| g.terminated()) {
                return acc;
            }
            if let Some(max) = params.max_connections {
                if connection_count.load(Ordering::Relaxed) >= max {
                    return acc;
                }
            }
            let source_time = event_time(ocel, &source);
            let targets = reachable_targets(
                ocel,
                &schema.steps,
                params.temporal,
                bound,
                source,
                source_time,
            );
            let num_targets = targets.len();
            if num_targets > 0 {
                acc.sources_with_paths += 1;
                acc.support += num_targets;
                connection_count.fetch_add(num_targets, Ordering::Relaxed);
                for &target in &targets {
                    acc.hash ^= pair_hash(source, target);
                    acc.targets.insert(target);
                    if let (Some(s), Some(t)) = (source_time, event_time(ocel, &target)) {
                        acc.durations
                            .push(t.signed_duration_since(s).num_milliseconds() as f64 / 1000.0);
                    }
                }
            }
            if let Some(guard) = &selectivity {
                guard.record(num_targets);
            }
            acc
        })
        .reduce(SummaryAcc::default, |mut a, b| {
            a.support += b.support;
            a.sources_with_paths += b.sources_with_paths;
            a.hash ^= b.hash;
            a.durations.extend(b.durations);
            a.targets.extend(b.targets);
            a
        });

    ConnectionSummary {
        support: acc.support,
        sources_with_paths: acc.sources_with_paths,
        distinct_targets: acc.targets.len(),
        durations: acc.durations,
        hash: acc.hash,
        limit_reached: params
            .max_connections
            .is_some_and(|max| connection_count.load(Ordering::Relaxed) >= max),
        selectivity_pruned: selectivity.is_some_and(|g| g.terminated()),
    }
}

/// Summary from already-materialized connections (the non-BFS fallback path).
fn summary_from_connections(result: &ConnectionResult) -> ConnectionSummary {
    let mut sources: HashSet<EntityRef> = HashSet::new();
    let mut targets: HashSet<EntityRef> = HashSet::new();
    let mut pairs: HashSet<(EntityRef, EntityRef)> = HashSet::new();
    let mut durations = Vec::new();
    let mut hash = 0u64;
    for c in &result.connections {
        sources.insert(c.source);
        targets.insert(c.target);
        if pairs.insert((c.source, c.target)) {
            hash ^= pair_hash(c.source, c.target);
        }
        if let (Some(s), Some(t)) = (c.source_time, c.target_time) {
            durations.push(t.signed_duration_since(s).num_milliseconds() as f64 / 1000.0);
        }
    }
    ConnectionSummary {
        support: pairs.len(),
        sources_with_paths: sources.len(),
        distinct_targets: targets.len(),
        durations,
        hash,
        limit_reached: result.limit_reached,
        selectivity_pruned: result.selectivity_pruned,
    }
}

/// Deterministic 64-bit hash of a directed (source, target) pair. XOR-folded across a
/// connection set (each pair contributing once), it identifies that set order-independently
/// (equal sets, equal value).
fn pair_hash(source: EntityRef, target: EntityRef) -> u64 {
    let mut hasher = DefaultHasher::new();
    (source, target).hash(&mut hasher);
    hasher.finish()
}

/// Immutable context shared across the whole traversal of one schema.
struct Walk<'a> {
    ocel: &'a SlimLinkedOCEL,
    steps: &'a [ResolvedStep],
    temporal: TemporalConstraint,
    bound: Option<TimeDelta>,
    max_connections: Option<usize>,
    connection_count: &'a AtomicUsize,
}

/// Per-source mutable traversal state.
struct SourceState {
    source: EntityRef,
    source_time: Option<DateTime<FixedOffset>>,
    visited: Vec<EntityRef>,
    seen_targets: Option<HashSet<EntityRef>>,
    connections: Vec<Connection>,
}

impl Walk<'_> {
    fn limit_reached(&self) -> bool {
        self.max_connections
            .is_some_and(|max| self.connection_count.load(Ordering::Relaxed) >= max)
    }

    fn passes_temporal(
        &self,
        neighbor: &EntityRef,
        last_event_time: Option<DateTime<FixedOffset>>,
        source_time: Option<DateTime<FixedOffset>>,
    ) -> bool {
        temporal_ok(
            self.temporal,
            self.bound,
            event_time(self.ocel, neighbor),
            last_event_time,
            source_time,
        )
    }

    fn dfs(
        &self,
        state: &mut SourceState,
        step_idx: usize,
        current: EntityRef,
        last_event_time: Option<DateTime<FixedOffset>>,
    ) {
        if self.limit_reached() {
            return;
        }

        if step_idx >= self.steps.len() {
            if let Some(seen) = state.seen_targets.as_mut() {
                if !seen.insert(current) {
                    return;
                }
            }
            state.connections.push(Connection {
                source: state.source,
                target: current,
                source_time: state.source_time,
                target_time: event_time(self.ocel, &current),
            });
            self.connection_count.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let step = &self.steps[step_idx];
        let mut neighbors = get_neighbors_via_edge(self.ocel, &current, &step.edge, step.reverse);
        neighbors.retain(|n| {
            !state.visited.contains(n)
                && self.passes_temporal(n, last_event_time, state.source_time)
        });
        if neighbors.is_empty() {
            return;
        }

        for neighbor in neighbors {
            let neighbor_time = event_time(self.ocel, &neighbor).or(last_event_time);
            state.visited.push(neighbor);
            self.dfs(state, step_idx + 1, neighbor, neighbor_time);
            state.visited.pop();
            if self.limit_reached() {
                return;
            }
        }
    }
}

/// The absolute time window for a bounded temporal constraint, else `None`.
fn temporal_bound(temporal: TemporalConstraint) -> Option<TimeDelta> {
    match temporal {
        TemporalConstraint::Bounded(secs) => TimeDelta::try_seconds(secs as i64),
        _ => None,
    }
}

/// Timestamp of an entity (only events carry timestamps).
fn event_time(ocel: &SlimLinkedOCEL, entity: &EntityRef) -> Option<DateTime<FixedOffset>> {
    match entity {
        EntityRef::Event(ev) => Some(*ocel.get_ev_time(ev)),
        EntityRef::Object(_) => None,
    }
}

/// Whether reaching `new_time` (the candidate entity's timestamp) keeps a path valid under
/// the temporal constraint. `last_event_time` is the most recent event time on the path so
/// far; `source_time` is the timestamp of the path's source.
fn temporal_ok(
    temporal: TemporalConstraint,
    bound: Option<TimeDelta>,
    new_time: Option<DateTime<FixedOffset>>,
    last_event_time: Option<DateTime<FixedOffset>>,
    source_time: Option<DateTime<FixedOffset>>,
) -> bool {
    let Some(new_t) = new_time else {
        return true;
    };
    match temporal {
        TemporalConstraint::None => true,
        TemporalConstraint::Forward => match last_event_time {
            Some(prev) => new_t >= prev,
            None => true,
        },
        TemporalConstraint::Bounded(_) => match (source_time, bound) {
            (Some(st), Some(b)) => (new_t - st).abs() <= b,
            _ => true,
        },
    }
}

/// Whether the schema's type sequence has no repeated types.
///
/// With distinct types per step an entity can never appear twice within a path, so the
/// per-path cycle check is unnecessary and set-based frontier traversal stays exact.
fn has_distinct_types(schema: &ResolvedPathSchema) -> bool {
    let mut seen: HashSet<&TypeRef> = HashSet::with_capacity(schema.steps.len() + 1);
    if !seen.insert(&schema.source) {
        return false;
    }
    schema.steps.iter().all(|step| {
        let next = if step.reverse {
            &step.edge.source
        } else {
            &step.edge.target
        };
        seen.insert(next)
    })
}

/// Distinct targets reachable from one source via a set-based level-by-level frontier.
///
/// Each frontier level keeps, per entity, the earliest "last event time" seen so far
/// (`None` = no event yet, the most permissive), which makes the forward-temporal check
/// exact while collapsing the path explosion that the DFS would otherwise enumerate.
fn reachable_targets(
    ocel: &SlimLinkedOCEL,
    steps: &[ResolvedStep],
    temporal: TemporalConstraint,
    bound: Option<TimeDelta>,
    source: EntityRef,
    source_time: Option<DateTime<FixedOffset>>,
) -> Vec<EntityRef> {
    let mut frontier: HashMap<EntityRef, Option<DateTime<FixedOffset>>> = HashMap::new();
    frontier.insert(source, source_time);

    for step in steps {
        let mut next: HashMap<EntityRef, Option<DateTime<FixedOffset>>> =
            HashMap::with_capacity(frontier.len());
        for (&entity, &last_time) in &frontier {
            for neighbor in get_neighbors_via_edge(ocel, &entity, &step.edge, step.reverse) {
                // The frontier time is only consulted by the forward/bounded checks; under
                // `None` it is never read, so skip the per-edge timestamp lookup entirely.
                let neighbor_time = match temporal {
                    TemporalConstraint::None => None,
                    _ => event_time(ocel, &neighbor),
                };
                if !temporal_ok(temporal, bound, neighbor_time, last_time, source_time) {
                    continue;
                }
                let new_last = neighbor_time.or(last_time);
                next.entry(neighbor)
                    .and_modify(|t| {
                        if new_last < *t {
                            *t = new_last;
                        }
                    })
                    .or_insert(new_last);
            }
        }
        if next.is_empty() {
            return Vec::new();
        }
        frontier = next;
    }

    frontier.into_keys().collect()
}

/// Keep one target event per source according to the selection strategy.
fn apply_event_selection(
    connections: Vec<Connection>,
    selection: EventSelection,
) -> Vec<Connection> {
    if matches!(selection, EventSelection::All) {
        return connections;
    }

    // Single pass keeping the best connection per source (no per-group sort).
    let mut best: HashMap<EntityRef, Connection> = HashMap::new();
    for conn in connections {
        let keep = match best.get(&conn.source) {
            Some(current) => is_preferred(&conn, current, selection),
            None => true,
        };
        if keep {
            best.insert(conn.source, conn);
        }
    }
    best.into_values().collect()
}

/// Whether `candidate` should replace `current` as the kept target for their shared source.
///
/// The `target` secondary key makes the choice deterministic when targets tie on time.
fn is_preferred(candidate: &Connection, current: &Connection, selection: EventSelection) -> bool {
    match selection {
        EventSelection::First => {
            (candidate.target_time, candidate.target) < (current.target_time, current.target)
        }
        EventSelection::Last => {
            (Reverse(candidate.target_time), candidate.target)
                < (Reverse(current.target_time), current.target)
        }
        EventSelection::Closest => closeness(candidate) < closeness(current),
        EventSelection::All => unreachable!(),
    }
}

/// Sort key for [`EventSelection::Closest`]: absolute source-to-target gap, then target.
fn closeness(c: &Connection) -> (TimeDelta, EntityRef) {
    let gap = match (c.source_time, c.target_time) {
        (Some(s), Some(t)) => (t - s).abs(),
        _ => TimeDelta::MAX,
    };
    (gap, c.target)
}
