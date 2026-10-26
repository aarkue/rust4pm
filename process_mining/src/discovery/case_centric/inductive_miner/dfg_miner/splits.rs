//! Directly-follows graph splitting: dividing a graph into the subgraphs the recursion continues
//! on.
//!
//! Where IM splits the log and rebuilds a graph per sub-log, the DFG variant projects the graph
//! itself onto the parts of a cut (§6.6.3.2). An edge crossing a part boundary cannot stay an
//! edge. Depending on the operator, it becomes a start or end weight of the part it touches, or an
//! empty trace of a part it bypasses entirely.

use crate::core::process_models::process_tree::OperatorType;

use super::super::cut_finder::cut::Cut;
use super::super::dfg::ActivityDfg;
use super::super::log::ActivityID;

/// Splits a graph according to a cut, yielding one subgraph per part.
///
/// The subgraphs are returned in the order of the cut's parts. Activities of the graph that no
/// part contains are dropped, along with their edges.
pub fn split_dfg(dfg: &ActivityDfg, cut: &Cut) -> Vec<ActivityDfg> {
    match cut.operator() {
        OperatorType::ExclusiveChoice | OperatorType::Concurrency => simple_split(dfg, cut),
        OperatorType::Sequence => sequence_split(dfg, cut),
        OperatorType::Loop => loop_split(dfg, cut),
        OperatorType::InclusiveChoice | OperatorType::Interleaving => {
            unreachable!("the DFG variant only detects the four footprint cuts")
        }
    }
}

/// Subgraph accumulators, filled in one pass over the parent graph.
struct Parts {
    /// Part of each parent-local activity, or `usize::MAX` if no part contains it.
    part_of: Vec<usize>,
    /// Index within its part of each parent-local activity.
    local_in_part: Vec<usize>,
    /// Global activity ids per part, ascending.
    activities: Vec<Vec<ActivityID>>,
    start: Vec<Vec<u64>>,
    end: Vec<Vec<u64>>,
    edges: Vec<Vec<(u32, u32, u64)>>,
    empty: Vec<u64>,
}

impl Parts {
    fn new(dfg: &ActivityDfg, cut: &Cut) -> Self {
        let global_part = cut.partition_lookup(dfg.alphabet_size());
        let mut part_of = vec![usize::MAX; dfg.len()];
        let mut local_in_part = vec![usize::MAX; dfg.len()];
        let mut activities = vec![Vec::new(); cut.len()];

        for local in 0..dfg.len() {
            let part = global_part[dfg.activity(local)];
            if part == usize::MAX {
                continue;
            }
            part_of[local] = part;
            local_in_part[local] = activities[part].len();
            activities[part].push(dfg.activity(local));
        }

        let sizes: Vec<usize> = activities.iter().map(Vec::len).collect();
        Self {
            part_of,
            local_in_part,
            activities,
            start: sizes.iter().map(|&n| vec![0; n]).collect(),
            end: sizes.iter().map(|&n| vec![0; n]).collect(),
            edges: vec![Vec::new(); cut.len()],
            empty: vec![0; cut.len()],
        }
    }

    /// The part and part-local index of a parent-local activity, or `None` if it has no part.
    fn place(&self, local: usize) -> Option<(usize, usize)> {
        (self.part_of[local] != usize::MAX)
            .then(|| (self.part_of[local], self.local_in_part[local]))
    }

    fn intra_edge(&mut self, part: usize, from: usize, to: usize, count: u64) {
        self.edges[part].push((
            self.local_in_part[from] as u32,
            self.local_in_part[to] as u32,
            count,
        ));
    }

    fn finish(self, dfg: &ActivityDfg) -> Vec<ActivityDfg> {
        let alphabet_size = dfg.alphabet_size();
        self.activities
            .into_iter()
            .zip(self.start)
            .zip(self.end)
            .zip(self.edges)
            .zip(self.empty)
            .map(|((((activities, start), end), edges), empty)| {
                ActivityDfg::from_parts(alphabet_size, activities, start, end, edges, empty)
            })
            .collect()
    }
}

/// The split for exclusive choice and concurrency: each part keeps its internal edges and the
/// original start and end weights of its activities. Edges between parts are dropped.
fn simple_split(dfg: &ActivityDfg, cut: &Cut) -> Vec<ActivityDfg> {
    let mut parts = Parts::new(dfg, cut);
    parts.empty.fill(dfg.empty_traces());

    for local in 0..dfg.len() {
        if let Some((part, in_part)) = parts.place(local) {
            parts.start[part][in_part] = dfg.start_count(local);
            parts.end[part][in_part] = dfg.end_count(local);
        }
    }
    for (from, to, count) in dfg.edges() {
        let (from_part, to_part) = (parts.part_of[from], parts.part_of[to]);
        if from_part != usize::MAX && from_part == to_part {
            parts.intra_edge(from_part, from, to, count);
        }
    }

    parts.finish(dfg)
}

/// The split for a sequence cut: an edge entering a part becomes a start weight, one leaving it an
/// end weight, and one bypassing it entirely an empty trace, the part not having run in that
/// execution. A trace start counts as an edge from before the first part, and a trace end as an
/// edge to after the last. Backwards edges, which only a cut from a filtered graph leaves behind,
/// are dropped.
fn sequence_split(dfg: &ActivityDfg, cut: &Cut) -> Vec<ActivityDfg> {
    let mut parts = Parts::new(dfg, cut);
    let num_parts = cut.len();
    // Each bypass covers a contiguous range of parts, summed up by a difference array.
    let mut bypass = vec![0i64; num_parts + 1];

    for local in 0..dfg.len() {
        let Some((part, in_part)) = parts.place(local) else {
            continue;
        };
        let start = dfg.start_count(local);
        parts.start[part][in_part] += start;
        bypass[0] += start as i64;
        bypass[part] -= start as i64;

        let end = dfg.end_count(local);
        parts.end[part][in_part] += end;
        bypass[part + 1] += end as i64;
        bypass[num_parts] -= end as i64;
    }

    for (from, to, count) in dfg.edges() {
        match (parts.part_of[from], parts.part_of[to]) {
            (usize::MAX, _) | (_, usize::MAX) => {}
            (from_part, to_part) if from_part == to_part => {
                parts.intra_edge(from_part, from, to, count);
            }
            (from_part, to_part) if from_part < to_part => {
                parts.end[from_part][parts.local_in_part[from]] += count;
                parts.start[to_part][parts.local_in_part[to]] += count;
                bypass[from_part + 1] += count as i64;
                bypass[to_part] -= count as i64;
            }
            _ => {}
        }
    }

    let mut bypassing = 0i64;
    for (part, delta) in bypass.into_iter().take(num_parts).enumerate() {
        bypassing += delta;
        parts.empty[part] = dfg.empty_traces() + bypassing as u64;
    }

    parts.finish(dfg)
}

/// The split for a loop cut, giving the graphs of the sub-logs that splitting the log would give.
/// Every part keeps its internal edges, and an edge between two parts ends a run of its source
/// part and starts a run of its target part.
///
/// The body picks up an empty trace wherever execution bypassed it: a trace starting or ending in
/// a redo part, or an edge between two different redo parts (§6.6.3.2, Figure 6.23). For a cut
/// adhering to its footprint none of the three exists, but a cut from a filtered graph splits the
/// unfiltered one, where they can.
///
/// The body's start and end weights count every iteration, as in Figure 6.24, not only the trace
/// starts and ends as in the pseudo-code of `loopDfgSplit`.
fn loop_split(dfg: &ActivityDfg, cut: &Cut) -> Vec<ActivityDfg> {
    const BODY: usize = 0;
    let mut parts = Parts::new(dfg, cut);

    for local in 0..dfg.len() {
        let Some((part, in_part)) = parts.place(local) else {
            continue;
        };
        let (start, end) = (dfg.start_count(local), dfg.end_count(local));
        parts.start[part][in_part] += start;
        parts.end[part][in_part] += end;
        if part != BODY {
            parts.empty[BODY] += start + end;
        }
    }

    for (from, to, count) in dfg.edges() {
        match (parts.part_of[from], parts.part_of[to]) {
            (usize::MAX, _) | (_, usize::MAX) => {}
            (from_part, to_part) if from_part == to_part => {
                parts.intra_edge(from_part, from, to, count);
            }
            (from_part, to_part) => {
                parts.end[from_part][parts.local_in_part[from]] += count;
                parts.start[to_part][parts.local_in_part[to]] += count;
                if from_part != BODY && to_part != BODY {
                    parts.empty[BODY] += count;
                }
            }
        }
    }

    parts.finish(dfg)
}

#[cfg(test)]
mod test_split_dfg {
    use super::super::super::log::test_utils::log_of;
    use super::*;

    /// A subgraph as `(activities, starts, ends, edges, empty traces)` over global ids.
    type Description = (
        Vec<usize>,
        Vec<u64>,
        Vec<u64>,
        Vec<(usize, usize, u64)>,
        u64,
    );

    fn describe(dfg: &ActivityDfg) -> Description {
        (
            dfg.activities().to_vec(),
            (0..dfg.len()).map(|a| dfg.start_count(a)).collect(),
            (0..dfg.len()).map(|a| dfg.end_count(a)).collect(),
            dfg.edges()
                .map(|(from, to, count)| (dfg.activity(from), dfg.activity(to), count))
                .collect(),
            dfg.empty_traces(),
        )
    }

    fn split(
        traces: &[&[&str]],
        operator: OperatorType,
        partitions: &[&[usize]],
    ) -> Vec<Description> {
        let (_, log) = log_of(traces);
        let cut = Cut::new(operator, partitions.iter().map(|p| p.to_vec()).collect());
        split_dfg(&ActivityDfg::discover(&log), &cut)
            .iter()
            .map(describe)
            .collect()
    }

    /// Figure 6.20: an exclusive choice split keeps each part's edges and start/end weights.
    #[test]
    fn test_simple_split() {
        let subs = split(
            &[&["a"], &["b", "c"]],
            OperatorType::ExclusiveChoice,
            &[&[0], &[1, 2]],
        );
        assert_eq!(subs[0], (vec![0], vec![1], vec![1], vec![], 0));
        assert_eq!(
            subs[1],
            (vec![1, 2], vec![1, 0], vec![0, 1], vec![(1, 2, 1)], 0)
        );

        // Figure 6.21: for concurrency, the edges between the parts are dropped.
        let subs = split(
            &[&["a", "b"], &["b", "a"]],
            OperatorType::Concurrency,
            &[&[0], &[1]],
        );
        assert_eq!(subs[0], (vec![0], vec![1], vec![1], vec![], 0));
        assert_eq!(subs[1], (vec![1], vec![1], vec![1], vec![], 0));
    }

    /// Figure 6.22: the trace `⟨a, c⟩` bypasses the part `{b}`, which becomes an empty trace of
    /// its subgraph, and the edges into and out of `b` become its start and end weights.
    #[test]
    fn test_sequence_split() {
        let subs = split(
            &[&["a", "b", "c"], &["a", "c"]],
            OperatorType::Sequence,
            &[&[0], &[1], &[2]],
        );
        assert_eq!(subs[0], (vec![0], vec![2], vec![2], vec![], 0));
        assert_eq!(subs[1], (vec![1], vec![1], vec![1], vec![], 1));
        assert_eq!(subs[2], (vec![2], vec![2], vec![2], vec![], 0));
    }

    /// Figure 6.24: an edge between two redo parts means the body was skipped in between, so it
    /// becomes an empty trace of the body's subgraph. Every iteration starts and ends the body.
    #[test]
    fn test_loop_split() {
        let subs = split(
            &[
                &["a", "b"],
                &["a", "b", "c", "a", "b"],
                &["a", "b", "d", "a", "b"],
                &["a", "b", "c", "d", "a", "b"],
            ],
            OperatorType::Loop,
            &[&[0, 1], &[2], &[3]],
        );
        assert_eq!(
            subs[0],
            (vec![0, 1], vec![7, 0], vec![0, 7], vec![(0, 1, 7)], 1)
        );
        assert_eq!(subs[1], (vec![2], vec![2], vec![2], vec![], 0));
        assert_eq!(subs[2], (vec![3], vec![2], vec![2], vec![], 0));
    }

    /// Splitting the graph of a log gives the graphs of the sub-logs, for every operator whose log
    /// split keeps runs of events together.
    #[test]
    fn test_split_dfg_matches_split_log() {
        use super::super::super::splits::split_log;

        let check = |traces: &[&[&str]], operator: OperatorType, partitions: &[&[usize]]| {
            let (_, log) = log_of(traces);
            let cut = Cut::new(operator, partitions.iter().map(|p| p.to_vec()).collect());
            let from_graph: Vec<Description> = split_dfg(&ActivityDfg::discover(&log), &cut)
                .iter()
                .map(describe)
                .collect();
            let from_log: Vec<Description> = split_log(&log, &cut)
                .iter()
                .map(|sub_log| describe(&ActivityDfg::discover(sub_log)))
                .collect();
            assert_eq!(from_graph, from_log, "{operator:?} split of {traces:?}");
        };

        check(
            &[&["a", "b"], &["c"], &["c", "c"]],
            OperatorType::ExclusiveChoice,
            &[&[0, 1], &[2]],
        );
        check(
            &[
                &["a", "b", "c"],
                &["a", "c"],
                &["b", "c"],
                &["a", "b", "b", "c"],
            ],
            OperatorType::Sequence,
            &[&[0], &[1], &[2]],
        );
        check(
            &[
                &["a", "b"],
                &["a", "b", "c", "a", "b"],
                &["a", "b", "c", "d", "a", "b"],
            ],
            OperatorType::Loop,
            &[&[0, 1], &[2], &[3]],
        );
        // Starting and ending in a redo part, and switching between redo parts.
        check(
            &[
                &["c", "a", "b"],
                &["a", "b", "c", "d", "a", "b"],
                &["a", "b", "d"],
            ],
            OperatorType::Loop,
            &[&[0, 1], &[2], &[3]],
        );
    }
}
