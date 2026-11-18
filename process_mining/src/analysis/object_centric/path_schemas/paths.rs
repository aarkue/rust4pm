use super::graph::{get_neighbors_via_edge, EntityRef, TypeRef};
use super::schema::{ResolvedPathSchema, ResolvedStep};
use crate::core::event_data::object_centric::linked_ocel::{
    slim_linked_ocel::SlimLinkedOCEL, LinkedOCELAccess,
};
use crate::utils::control::{Cancelled, Control, Counter};
use chrono::{DateTime, FixedOffset, TimeDelta};
use hashbrown::{HashMap, HashSet};
use macros_process_mining::controlled;
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
    /// Global cap on the number of connections, taken from sources in source order. The
    /// path-enumerating scan stops exactly at the cap. The set-based scan (used with
    /// `dedup_targets` when no type repeats along the schema) checks it between sources, so a
    /// single high-fan-out source can overshoot it.
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
    /// Whether the selectivity threshold triggered early termination. Sources after the one
    /// that triggered it are not scanned.
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
/// Sources are recorded in source order, so where the scan stops does not depend on thread
/// scheduling.
struct SelectivityGuard {
    threshold: f64,
    total_sources: usize,
    processed: usize,
    active: usize,
    support: usize,
    terminated: bool,
}

impl SelectivityGuard {
    fn new(threshold: f64, total_sources: usize) -> Self {
        Self {
            threshold,
            total_sources,
            processed: 0,
            active: 0,
            support: 0,
            terminated: false,
        }
    }

    /// Record one processed source contributing `num_targets`, updating the termination flag.
    fn record(&mut self, num_targets: usize) {
        self.processed += 1;
        if num_targets > 0 {
            self.active += 1;
            self.support += num_targets;
        }
        let remaining = self.total_sources.saturating_sub(self.processed);
        let denominator = self.support + remaining;
        if denominator > 0
            && ((self.active + remaining) as f64 / denominator as f64) < self.threshold
        {
            self.terminated = true;
        }
    }
}

/// The scans evaluate one chunk of sources in parallel, then merge its results in source order
/// and apply the cutoffs there. That keeps the result independent of thread scheduling, and
/// bounds the work done past a cutoff to one chunk.
fn source_chunks(sources: &[EntityRef]) -> std::slice::Chunks<'_, EntityRef> {
    sources.chunks((rayon::current_num_threads() * 64).max(256))
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

/// Progress stage of [`find_connections`], counting source entities.
const FIND_STAGE: &str = "Finding connections";

/// Progress stage of [`summarize_connections`], counting source entities.
const SUMMARIZE_STAGE: &str = "Summarizing connections";

/// Find all connections for a single (resolved) schema across every source-type entity.
///
/// Memory-efficient depth-first traversal: only source-target pairs (with timestamps) are
/// materialized, using O(schema length) stack per source. See [`PathConnectionParams`].
///
/// Reports progress with stage `"Finding connections"`, where `done` counts the source entities
/// scanned out of all of them.
#[controlled]
pub fn find_connections(
    ocel: &SlimLinkedOCEL,
    schema: &ResolvedPathSchema,
    params: &PathConnectionParams,
    ctl: &Control<'_>,
) -> Result<ConnectionResult, Cancelled> {
    let sources = get_entities_of_type(ocel, &schema.source);
    connections_from(ocel, schema, &sources, params, ctl)
}

/// [`find_connections`] from source entities the caller already collected.
pub(crate) fn connections_from(
    ocel: &SlimLinkedOCEL,
    schema: &ResolvedPathSchema,
    source_entities: &[EntityRef],
    params: &PathConnectionParams,
    ctl: &Control<'_>,
) -> Result<ConnectionResult, Cancelled> {
    let progress = ctl.counter(FIND_STAGE, Some(source_entities.len() as u64));
    scan_connections(ocel, schema, source_entities, params, ctl, &progress)
}

/// Count the sources a scan skipped after a cutoff as done, so a stage shared by several scans
/// still ends at its total.
fn finish_scan(progress: &Counter<'_, '_>, total: usize, scanned: usize) {
    if total > scanned {
        progress.add((total - scanned) as u64);
    }
}

/// [`connections_from`], adding each scanned source chunk to `progress` and
/// checking `ctl` between chunks.
pub(super) fn scan_connections(
    ocel: &SlimLinkedOCEL,
    schema: &ResolvedPathSchema,
    source_entities: &[EntityRef],
    params: &PathConnectionParams,
    ctl: &Control<'_>,
    progress: &Counter<'_, '_>,
) -> Result<ConnectionResult, Cancelled> {
    let total_sources = source_entities.len();
    if total_sources == 0 || params.max_connections == Some(0) {
        finish_scan(progress, total_sources, 0);
        return Ok(ConnectionResult {
            connections: Vec::new(),
            limit_reached: total_sources > 0,
            selectivity_pruned: false,
        });
    }

    let bound = temporal_bound(params.temporal);
    let mut selectivity = params
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
        cancel: ctl.cancel_flag_ref(),
    };

    let max = params.max_connections;
    let mut connections: Vec<Connection> = Vec::new();
    let mut limit_reached = false;
    let mut scanned = 0;
    'scan: for chunk in source_chunks(source_entities) {
        ctl.check()?;
        let cap = max.map(|max| max.saturating_sub(connections.len()));
        // Connections kept per DFS source of the chunk. A source caps itself at what the sources
        // before it left of `cap`. Their counts only grow, so it never keeps less than the
        // in-order merge below takes from it.
        let kept: Vec<AtomicUsize> = match (cap, use_bfs) {
            (Some(_), false) => chunk.iter().map(|_| AtomicUsize::new(0)).collect(),
            _ => Vec::new(),
        };
        let per_source: Vec<Vec<Connection>> = chunk
            .par_iter()
            .enumerate()
            .map(|(i, &source)| {
                let source_time = event_time(ocel, &source);
                if use_bfs {
                    return reachable_targets(
                        ocel,
                        &schema.steps,
                        params.temporal,
                        bound,
                        source,
                        source_time,
                    )
                    .into_iter()
                    .map(|target| Connection {
                        source,
                        target,
                        source_time,
                        target_time: event_time(ocel, &target),
                    })
                    .collect();
                }
                let cap = cap.map(|cap| {
                    let before: usize = kept[..i].iter().map(|k| k.load(Ordering::Relaxed)).sum();
                    cap.saturating_sub(before)
                });
                let mut state = SourceState {
                    source,
                    source_time,
                    visited: vec![source],
                    seen_targets: params.dedup_targets.then(HashSet::new),
                    connections: Vec::new(),
                    cap,
                };
                walk.dfs(&mut state, 0, source, source_time);
                if let Some(k) = kept.get(i) {
                    k.store(state.connections.len(), Ordering::Relaxed);
                }
                state.connections
            })
            .collect();
        // A DFS cut short by the cancel flag holds partial results.
        ctl.check()?;

        for mut conns in per_source {
            if let (Some(max), false) = (max, use_bfs) {
                conns.truncate(max - connections.len());
            }
            let num_targets = match &selectivity {
                None => 0,
                Some(_) if params.dedup_targets => conns.len(),
                Some(_) => conns.iter().map(|c| c.target).collect::<HashSet<_>>().len(),
            };
            connections.extend(conns);
            if let Some(guard) = &mut selectivity {
                guard.record(num_targets);
            }
            limit_reached = max.is_some_and(|max| connections.len() >= max);
            if limit_reached || selectivity.as_ref().is_some_and(|g| g.terminated) {
                break 'scan;
            }
        }
        scanned += chunk.len();
        progress.add(chunk.len() as u64);
    }
    finish_scan(progress, total_sources, scanned);

    let connections = apply_event_selection(connections, params.selection);
    Ok(ConnectionResult {
        connections,
        limit_reached,
        selectivity_pruned: selectivity.is_some_and(|g| g.terminated),
    })
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
/// via `connections_from` when repeated types or non-deduped targets require
/// the path-enumerating DFS.
///
/// Reports progress with stage `"Summarizing connections"`, where `done` counts the source
/// entities scanned out of all of them.
#[controlled]
pub fn summarize_connections(
    ocel: &SlimLinkedOCEL,
    schema: &ResolvedPathSchema,
    source_entities: &[EntityRef],
    params: &PathConnectionParams,
    ctl: &Control<'_>,
) -> Result<ConnectionSummary, Cancelled> {
    let progress = ctl.counter(SUMMARIZE_STAGE, Some(source_entities.len() as u64));
    summarize_scan(ocel, schema, source_entities, params, ctl, &progress)
}

/// [`summarize_connections`], adding each scanned source chunk to `progress` and checking `ctl`
/// between chunks.
pub(super) fn summarize_scan(
    ocel: &SlimLinkedOCEL,
    schema: &ResolvedPathSchema,
    source_entities: &[EntityRef],
    params: &PathConnectionParams,
    ctl: &Control<'_>,
    progress: &Counter<'_, '_>,
) -> Result<ConnectionSummary, Cancelled> {
    let total_sources = source_entities.len();
    if total_sources == 0 || params.max_connections == Some(0) {
        finish_scan(progress, total_sources, 0);
        return Ok(ConnectionSummary {
            limit_reached: total_sources > 0,
            ..ConnectionSummary::default()
        });
    }

    let use_bfs = params.dedup_targets && has_distinct_types(schema);
    if !use_bfs {
        return Ok(summary_from_connections(&scan_connections(
            ocel,
            schema,
            source_entities,
            params,
            ctl,
            progress,
        )?));
    }

    let bound = temporal_bound(params.temporal);
    let mut selectivity = params
        .selectivity_threshold
        .map(|theta| SelectivityGuard::new(theta, total_sources));

    let mut acc = SummaryAcc::default();
    let mut limit_reached = false;
    let mut scanned = 0;
    'scan: for chunk in source_chunks(source_entities) {
        ctl.check()?;
        let per_source: Vec<(Option<DateTime<FixedOffset>>, Vec<EntityRef>)> = chunk
            .par_iter()
            .map(|&source| {
                let source_time = event_time(ocel, &source);
                let targets = reachable_targets(
                    ocel,
                    &schema.steps,
                    params.temporal,
                    bound,
                    source,
                    source_time,
                );
                (source_time, targets)
            })
            .collect();

        for (&source, (source_time, targets)) in chunk.iter().zip(per_source) {
            let num_targets = targets.len();
            if num_targets > 0 {
                acc.sources_with_paths += 1;
                acc.support += num_targets;
                for &target in &targets {
                    acc.hash ^= pair_hash(source, target);
                    acc.targets.insert(target);
                    if let (Some(s), Some(t)) = (source_time, event_time(ocel, &target)) {
                        acc.durations
                            .push(t.signed_duration_since(s).num_milliseconds() as f64 / 1000.0);
                    }
                }
            }
            if let Some(guard) = &mut selectivity {
                guard.record(num_targets);
            }
            limit_reached = params.max_connections.is_some_and(|max| acc.support >= max);
            if limit_reached || selectivity.as_ref().is_some_and(|g| g.terminated) {
                break 'scan;
            }
        }
        scanned += chunk.len();
        progress.add(chunk.len() as u64);
    }
    finish_scan(progress, total_sources, scanned);

    Ok(ConnectionSummary {
        support: acc.support,
        sources_with_paths: acc.sources_with_paths,
        distinct_targets: acc.targets.len(),
        durations: acc.durations,
        hash: acc.hash,
        limit_reached,
        selectivity_pruned: selectivity.is_some_and(|g| g.terminated),
    })
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
    cancel: Option<&'a AtomicBool>,
}

/// Per-source mutable traversal state.
struct SourceState {
    source: EntityRef,
    source_time: Option<DateTime<FixedOffset>>,
    visited: Vec<EntityRef>,
    seen_targets: Option<HashSet<EntityRef>>,
    connections: Vec<Connection>,
    /// Stop once this source has this many connections.
    cap: Option<usize>,
}

impl SourceState {
    fn limit_reached(&self) -> bool {
        self.cap.is_some_and(|cap| self.connections.len() >= cap)
    }
}

impl Walk<'_> {
    fn cancelled(&self) -> bool {
        self.cancel.is_some_and(|c| c.load(Ordering::Relaxed))
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
        if state.limit_reached() || self.cancelled() {
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
            if state.limit_reached() || self.cancelled() {
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

#[cfg(test)]
mod tests {
    use super::super::{enumerate_schemas, TypeGraph, DEFAULT_MAX_SCHEMAS};
    use super::*;
    use crate::core::io::Importable;
    use crate::test_utils::get_test_data_path;

    #[test]
    fn dfs_stops_once_the_cancel_flag_is_set() {
        let path = get_test_data_path()
            .join("ocel")
            .join("order-management.json");
        let ocel = SlimLinkedOCEL::import_from_path(&path).unwrap();
        let graph = TypeGraph::from_linked_ocel(&ocel);
        let src = TypeRef::Event(graph.event_types[0].clone());
        let schema = enumerate_schemas(&graph, &src, None, 2, false, None, DEFAULT_MAX_SCHEMAS)
            .schemas
            .into_iter()
            .map(|s| s.resolve(&graph))
            .find(|s| s.len() == 2)
            .unwrap();
        let source = get_entities_of_type(&ocel, &schema.source)[0];
        let walk_with = |cancel| {
            let walk = Walk {
                ocel: &ocel,
                steps: &schema.steps,
                temporal: TemporalConstraint::None,
                bound: None,
                cancel,
            };
            let source_time = event_time(&ocel, &source);
            let mut state = SourceState {
                source,
                source_time,
                visited: vec![source],
                seen_targets: None,
                connections: Vec::new(),
                cap: None,
            };
            walk.dfs(&mut state, 0, source, source_time);
            state.connections.len()
        };
        let flag = AtomicBool::new(false);
        assert!(walk_with(Some(&flag)) > 0);
        flag.store(true, Ordering::Relaxed);
        assert_eq!(walk_with(Some(&flag)), 0);
    }
}
