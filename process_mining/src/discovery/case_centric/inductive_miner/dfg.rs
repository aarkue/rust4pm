//! The directly-follows graph the Inductive Miner detects cuts in.
//!
//! Not the crate's [`DirectlyFollowsGraph`](crate::core::process_models::dfg::DirectlyFollowsGraph):
//! the miner rebuilds a graph per sub-log, and once per activity inside the activity-concurrent
//! fall through, so [`ActivityDfg`] indexes activities densely and stores edges as a sorted
//! adjacency list, never touching a string. Local indices ascend with the [`ActivityID`], which
//! keeps cut detection deterministic.

use super::log::{ActivityID, ActivityLog};

/// Directly-follows graph of an [`ActivityLog`], indexed by dense local activity indices.
///
/// See the [module documentation](self) for the rationale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityDfg {
    /// Global activity id of each local index, ascending.
    activities: Vec<ActivityID>,
    /// Local index of each global activity id, or [`usize::MAX`] if it does not occur.
    local_of: Vec<usize>,
    /// Number of traces starting with each activity.
    start: Vec<u64>,
    /// Number of traces ending with each activity.
    end: Vec<u64>,
    /// Number of occurrences of each activity.
    occurrences: Vec<u64>,
    /// Number of empty traces. IM handles them before building a graph, so only the
    /// [DFG variant](super::dfg_miner) reads this.
    empty_traces: u64,
    /// Outgoing edges, with their frequencies.
    successors: Adjacency,
    /// Incoming edges, without frequencies.
    predecessors: Adjacency,
}

/// Edges of the graph in compressed-sparse-row form.
///
/// Real graphs are sparse, so this is cheaper to build and filter than an `n x n` matrix, at the
/// cost of a binary search per edge lookup.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Adjacency {
    /// Neighbours of activity `a` are `flat[offsets[a]..offsets[a + 1]]`, ascending.
    offsets: Vec<u32>,
    flat: Vec<u32>,
    /// Frequency of each edge in `flat`, or empty if this direction does not track them.
    counts: Vec<u64>,
}

impl Adjacency {
    fn neighbours(&self, activity: usize) -> &[u32] {
        &self.flat[self.range(activity)]
    }

    fn range(&self, activity: usize) -> std::ops::Range<usize> {
        self.offsets[activity] as usize..self.offsets[activity + 1] as usize
    }

    /// Position of `neighbour` in `flat`.
    fn position_of(&self, activity: usize, neighbour: usize) -> Option<usize> {
        let range = self.range(activity);
        self.flat[range.clone()]
            .binary_search(&(neighbour as u32))
            .ok()
            .map(|offset| range.start + offset)
    }

    /// Builds the adjacency from edges given as `(from, to, count)`, sorted by `from` then `to`.
    fn from_sorted_edges(n: usize, edges: &[(u32, u32, u64)], with_counts: bool) -> Self {
        let mut offsets = Vec::with_capacity(n + 1);
        let mut flat = Vec::with_capacity(edges.len());
        let mut counts = Vec::with_capacity(if with_counts { edges.len() } else { 0 });

        let mut edge = 0;
        for activity in 0..n {
            offsets.push(flat.len() as u32);
            while edge < edges.len() && edges[edge].0 as usize == activity {
                flat.push(edges[edge].1);
                if with_counts {
                    counts.push(edges[edge].2);
                }
                edge += 1;
            }
        }
        offsets.push(flat.len() as u32);

        Self {
            offsets,
            flat,
            counts,
        }
    }
}

/// Sorts edges by source and then target, summing the counts of repeated edges.
///
/// Discovery yields one edge per pair of adjacent events. A counting sort by source followed by
/// a sort per source is much faster than one sort over all of them, since activities have few
/// distinct successors.
fn sort_edges_by_source(n: usize, edges: Vec<(u32, u32, u64)>) -> Vec<(u32, u32, u64)> {
    let mut offsets = vec![0u32; n + 2];
    for &(from, _, _) in &edges {
        offsets[from as usize + 1] += 1;
    }
    for source in 0..=n {
        offsets[source + 1] += offsets[source];
    }

    let mut bucketed = vec![(0u32, 0u32, 0u64); edges.len()];
    let mut next = offsets.clone();
    for edge in edges {
        let slot = &mut next[edge.0 as usize];
        bucketed[*slot as usize] = edge;
        *slot += 1;
    }

    let mut sorted: Vec<(u32, u32, u64)> = Vec::with_capacity(bucketed.len());
    for source in 0..n {
        let bucket = &mut bucketed[offsets[source] as usize..offsets[source + 1] as usize];
        bucket.sort_unstable_by_key(|&(_, to, _)| to);
        for &(from, to, count) in bucket.iter() {
            match sorted.last_mut() {
                Some(last) if last.0 == from && last.1 == to => last.2 += count,
                _ => sorted.push((from, to, count)),
            }
        }
    }

    sorted
}

/// The outgoing and incoming adjacency of unsorted, possibly repeated edges.
fn adjacencies(n: usize, edges: Vec<(u32, u32, u64)>) -> (Adjacency, Adjacency) {
    let sorted = sort_edges_by_source(n, edges);
    let successors = Adjacency::from_sorted_edges(n, &sorted, true);
    let reversed = sorted
        .iter()
        .map(|&(from, to, count)| (to, from, count))
        .collect();
    let predecessors = Adjacency::from_sorted_edges(n, &sort_edges_by_source(n, reversed), false);
    (successors, predecessors)
}

/// Maps each global activity id to its local index, or [`usize::MAX`] if it does not occur.
fn local_indices(alphabet_size: usize, activities: &[ActivityID]) -> Vec<usize> {
    let mut local_of = vec![usize::MAX; alphabet_size];
    for (local, &activity) in activities.iter().enumerate() {
        local_of[activity] = local;
    }
    local_of
}

impl ActivityDfg {
    /// Discovers the directly-follows graph of the given log. Empty traces only contribute to
    /// [`ActivityDfg::empty_traces`].
    pub fn discover(log: &ActivityLog) -> Self {
        let activities = log.activities();
        let n = activities.len();
        let local_of = local_indices(log.alphabet_size(), &activities);

        let mut start = vec![0u64; n];
        let mut end = vec![0u64; n];
        let mut occurrences = vec![0u64; n];
        let mut edges: Vec<(u32, u32, u64)> = Vec::new();

        for variant in log.variants() {
            let trace = &variant.activities;
            if trace.is_empty() {
                continue;
            }

            let mut previous = local_of[trace[0]];
            start[previous] += variant.count;
            end[local_of[trace[trace.len() - 1]]] += variant.count;
            occurrences[previous] += variant.count;
            for &activity in &trace[1..] {
                let current = local_of[activity];
                edges.push((previous as u32, current as u32, variant.count));
                occurrences[current] += variant.count;
                previous = current;
            }
        }

        let (successors, predecessors) = adjacencies(n, edges);
        Self {
            activities,
            local_of,
            start,
            end,
            occurrences,
            empty_traces: log.num_empty_traces(),
            successors,
            predecessors,
        }
    }

    /// Builds a graph from its parts, the way the [DFG splits](super::dfg_miner) produce them.
    ///
    /// `activities` are global ids in ascending order. `start`, `end` and `edges` use local
    /// indices, and repeated edges are merged. With no log to count in, the occurrences of an
    /// activity are its start weight plus its incoming edge weights.
    pub(crate) fn from_parts(
        alphabet_size: usize,
        activities: Vec<ActivityID>,
        start: Vec<u64>,
        end: Vec<u64>,
        edges: Vec<(u32, u32, u64)>,
        empty_traces: u64,
    ) -> Self {
        let mut occurrences = start.clone();
        for &(_, to, count) in &edges {
            occurrences[to as usize] += count;
        }

        let (successors, predecessors) = adjacencies(activities.len(), edges);
        Self {
            local_of: local_indices(alphabet_size, &activities),
            activities,
            start,
            end,
            occurrences,
            empty_traces,
            successors,
            predecessors,
        }
    }

    /// The activities directly following `activity`, ascending.
    pub fn successors(&self, activity: usize) -> &[u32] {
        self.successors.neighbours(activity)
    }

    /// The activities directly preceding `activity`, ascending.
    pub fn predecessors(&self, activity: usize) -> &[u32] {
        self.predecessors.neighbours(activity)
    }

    /// The number of directly-follows edges in the graph.
    pub fn num_edges(&self) -> usize {
        self.successors.flat.len()
    }

    /// The number of activities in the graph.
    pub fn len(&self) -> usize {
        self.activities.len()
    }

    /// Returns `true` if the graph has no activities.
    pub fn is_empty(&self) -> bool {
        self.activities.is_empty()
    }

    /// The global [`ActivityID`] of the activity with the given local index.
    pub fn activity(&self, local: usize) -> ActivityID {
        self.activities[local]
    }

    /// The global [`ActivityID`]s of all activities, in local-index order.
    pub fn activities(&self) -> &[ActivityID] {
        &self.activities
    }

    /// The local index of a global [`ActivityID`], or `None` if it does not occur in this graph.
    pub fn local_index(&self, activity: ActivityID) -> Option<usize> {
        self.local_of
            .get(activity)
            .copied()
            .filter(|&local| local != usize::MAX)
    }

    /// Returns `true` if `from` is directly followed by `to` at least once.
    pub fn follows(&self, from: usize, to: usize) -> bool {
        self.successors.position_of(from, to).is_some()
    }

    /// How often `from` is directly followed by `to`.
    pub fn edge_count(&self, from: usize, to: usize) -> u64 {
        self.successors
            .position_of(from, to)
            .map_or(0, |position| self.successors.counts[position])
    }

    /// Returns `true` if the activity starts at least one trace.
    pub fn is_start(&self, activity: usize) -> bool {
        self.start[activity] > 0
    }

    /// Returns `true` if the activity ends at least one trace.
    pub fn is_end(&self, activity: usize) -> bool {
        self.end[activity] > 0
    }

    /// How often the activity occurs in the log.
    pub fn occurrences(&self, activity: usize) -> u64 {
        self.occurrences[activity]
    }

    /// How many traces start with the activity.
    pub fn start_count(&self, activity: usize) -> u64 {
        self.start[activity]
    }

    /// How many traces end with the activity.
    pub fn end_count(&self, activity: usize) -> u64 {
        self.end[activity]
    }

    /// The number of empty traces behind the graph.
    pub fn empty_traces(&self) -> u64 {
        self.empty_traces
    }

    /// Size of the activity alphabet the global ids come from.
    pub fn alphabet_size(&self) -> usize {
        self.local_of.len()
    }

    /// All edges as `(from, to, count)` over local indices, sorted by source and then target.
    pub fn edges(&self) -> impl Iterator<Item = (usize, usize, u64)> + '_ {
        (0..self.len()).flat_map(move |from| {
            let range = self.successors.range(from);
            self.successors.flat[range.clone()]
                .iter()
                .zip(&self.successors.counts[range])
                .map(move |(&to, &count)| (from, to as usize, count))
        })
    }

    /// This graph without its empty traces.
    pub fn without_empty_traces(&self) -> Self {
        Self {
            empty_traces: 0,
            ..self.clone()
        }
    }

    /// Local indices of all start activities, ascending.
    pub fn start_activities(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len()).filter(|&a| self.is_start(a))
    }

    /// Local indices of all end activities, ascending.
    pub fn end_activities(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len()).filter(|&a| self.is_end(a))
    }

    /// Removes infrequent edges, as `IMf` does before retrying cut detection.
    ///
    /// An edge `a → b` is kept only if it occurs at least `noise_threshold` times as often as the
    /// most frequent edge leaving `a`. Starting a trace counts as an edge from an artificial start
    /// activity, and ending one as an edge to an artificial end activity, so infrequent start and
    /// end weights are dropped too. Activities are never removed, and `0.0` removes nothing.
    pub fn filtered(&self, noise_threshold: f64) -> Self {
        let start_cutoff = noise_threshold * self.start.iter().copied().max().unwrap_or(0) as f64;
        let start = self
            .start
            .iter()
            .map(|&count| {
                if (count as f64) < start_cutoff {
                    0
                } else {
                    count
                }
            })
            .collect();

        let mut end = self.end.clone();
        let mut kept = Vec::with_capacity(self.num_edges());
        for (from, end_count) in end.iter_mut().enumerate() {
            let range = self.successors.range(from);
            let max_outgoing = self.successors.counts[range.clone()]
                .iter()
                .copied()
                .fold(*end_count, u64::max);
            let cutoff = noise_threshold * max_outgoing as f64;

            for position in range {
                let count = self.successors.counts[position];
                if count as f64 >= cutoff {
                    kept.push((from as u32, self.successors.flat[position], count));
                }
            }
            if (*end_count as f64) < cutoff {
                *end_count = 0;
            }
        }

        let (successors, predecessors) = adjacencies(self.len(), kept);
        Self {
            activities: self.activities.clone(),
            local_of: self.local_of.clone(),
            start,
            end,
            occurrences: self.occurrences.clone(),
            empty_traces: self.empty_traces,
            successors,
            predecessors,
        }
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use super::{adjacencies, ActivityDfg};

    /// Builds a graph directly from edges and start/end activities, for the cases that are
    /// awkward or impossible to write down as a log. Frequencies are all `1`.
    pub fn dfg_of(
        activities: usize,
        edges: &[(usize, usize)],
        start: &[usize],
        end: &[usize],
    ) -> ActivityDfg {
        let (successors, predecessors) = adjacencies(
            activities,
            edges
                .iter()
                .map(|&(from, to)| (from as u32, to as u32, 1))
                .collect(),
        );
        let weights = |set: &[usize]| {
            (0..activities)
                .map(|a| u64::from(set.contains(&a)))
                .collect()
        };
        ActivityDfg {
            activities: (0..activities).collect(),
            local_of: (0..activities).collect(),
            start: weights(start),
            end: weights(end),
            occurrences: vec![1; activities],
            empty_traces: 0,
            successors,
            predecessors,
        }
    }
}

#[cfg(test)]
mod test_activity_dfg {
    use super::super::log::test_utils::log_of;
    use super::*;

    #[test]
    fn test_discover() {
        let (_, log) = log_of(&[&["a", "b", "c"], &["a", "b", "c"]]);
        let dfg = ActivityDfg::discover(&log);

        assert_eq!(dfg.activities(), &[0, 1, 2]);
        assert!(dfg.follows(0, 1) && dfg.follows(1, 2) && !dfg.follows(1, 0));
        assert_eq!(dfg.edge_count(0, 1), 2);
        assert_eq!(dfg.edge_count(2, 0), 0);
        assert!(dfg.is_start(0) && !dfg.is_start(1));
        assert!(dfg.is_end(2) && !dfg.is_end(0));
        assert_eq!(dfg.occurrences(1), 2);
    }

    #[test]
    fn test_local_indices_are_dense() {
        // Only "a" and "c" occur, so they get the local indices 0 and 1.
        let (_, log) = log_of(&[&["a"], &["b"], &["c"]]);
        let dfg = ActivityDfg::discover(&log.derive([(vec![0, 2], 1)]));

        assert_eq!(dfg.len(), 2);
        assert_eq!(dfg.activities(), &[0, 2]);
        assert!(dfg.follows(0, 1));
        assert_eq!(dfg.local_index(2), Some(1));
        assert_eq!(dfg.local_index(1), None);
    }

    #[test]
    fn test_empty_traces_are_ignored() {
        let (_, log) = log_of(&[&[], &["a"]]);
        let dfg = ActivityDfg::discover(&log);
        assert_eq!(dfg.len(), 1);
        assert!(dfg.is_start(0) && dfg.is_end(0));

        let (_, log) = log_of(&[&["a"]]);
        assert!(ActivityDfg::discover(&log.derive([(vec![], 3)])).is_empty());
    }

    #[test]
    fn test_filter_removes_infrequent_edges() {
        // 10x a to b, once a to c: with f = 0.2 the edge a to c falls below 0.2 * 10 = 2.
        let (_, log) = log_of(&[&["a", "b", "c"]]);
        let log = log.derive([(vec![0, 1], 10), (vec![0, 2], 1)]);
        let dfg = ActivityDfg::discover(&log);
        assert!(dfg.follows(0, 1) && dfg.follows(0, 2));

        let filtered = dfg.filtered(0.2);
        assert!(filtered.follows(0, 1));
        assert!(!filtered.follows(0, 2));
        // The activity is kept, it just has no incoming edge any more.
        assert_eq!(filtered.len(), 3);
        assert!(filtered.is_end(2));
    }

    #[test]
    fn test_filter_removes_infrequent_start_and_end() {
        // 10 traces start with a and end with b, one starts with b and ends with a.
        let (_, log) = log_of(&[&["a", "b"]]);
        let log = log.derive([(vec![0, 1], 10), (vec![1, 0], 1)]);
        let dfg = ActivityDfg::discover(&log);

        let filtered = dfg.filtered(0.2);
        assert!(filtered.is_start(0) && !filtered.is_start(1));
        assert!(filtered.is_end(1) && !filtered.is_end(0));
        assert_eq!(dfg.filtered(0.0), dfg);
    }
}
