//! Detection of interleaved (`↔`) cuts.

use crate::core::process_models::process_tree::OperatorType;

use super::super::dfg::ActivityDfg;
use super::super::log::{ActivityID, ActivityLog};
use super::super::structures::union_find::UnionFind;
use super::cut::Cut;

/// Finds the maximal interleaved cut, if there is one, as `interleavedCut` of `IMa` does (§6.4.2.1).
///
/// Requirement ↔.1 asks that between parts, exactly the edges from an end to a start activity
/// exist. So an activity that does not start a trace shares the part of its predecessors, one
/// that does not end a trace shares the part of its successors, and an end and a start activity
/// without an edge between them share a part. The pseudo-code swaps the pair in this last rule,
/// which would reject the thesis' own `↔(→(a, b), c)`.
///
/// The graph does not tell whether the parts actually run one after the other, so this also reads
/// the traces: an interleaved part runs to completion before the next starts, and its activities
/// occupy one uninterrupted block. Activities whose blocks overlap in some trace share a part,
/// which also covers ∧↔.1. Merging can create new overlaps, so this repeats until it settles.
pub fn interleaved_cut(log: &ActivityLog, dfg: &ActivityDfg) -> Option<Cut> {
    let alphabet_size = log.alphabet_size();
    let mut parts = UnionFind::new(alphabet_size);

    for (from, to, _) in dfg.edges() {
        if !dfg.is_end(from) || !dfg.is_start(to) {
            parts.union(dfg.activity(from), dfg.activity(to));
        }
    }
    for end in dfg.end_activities() {
        for start in dfg.start_activities() {
            if !dfg.follows(end, start) {
                parts.union(dfg.activity(end), dfg.activity(start));
            }
        }
    }

    let mut last_position = vec![0usize; alphabet_size];
    loop {
        let mut merged = false;
        for variant in log.variants() {
            merged |= merge_blocks(&variant.activities, &mut parts, &mut last_position);
        }
        if !merged {
            break;
        }
    }

    let cut = Cut::new(OperatorType::Interleaving, groups_of(log, &mut parts));
    cut.is_non_trivial().then_some(cut)
}

/// The activities of the log grouped by part, each group in ascending order.
fn groups_of(log: &ActivityLog, parts: &mut UnionFind) -> Vec<Vec<ActivityID>> {
    let mut group_of = vec![usize::MAX; log.alphabet_size()];
    let mut groups: Vec<Vec<ActivityID>> = Vec::new();
    for activity in log.activities() {
        let root = parts.find(activity);
        if group_of[root] == usize::MAX {
            group_of[root] = groups.len();
            groups.push(Vec::new());
        }
        groups[group_of[root]].push(activity);
    }
    groups
}

/// Merges the parts of the activities sharing a block of one trace, and reports whether that
/// changed anything.
///
/// A block reaches at least as far as the last occurrence of every activity in it, so extending the
/// end while sweeping divides the trace into the smallest blocks that do not overlap.
fn merge_blocks(trace: &[ActivityID], parts: &mut UnionFind, last_position: &mut [usize]) -> bool {
    let roots: Vec<usize> = trace.iter().map(|&a| parts.find(a)).collect();
    for (position, &root) in roots.iter().enumerate() {
        last_position[root] = position;
    }

    let mut merged = false;
    let mut block_start = 0;
    let mut block_end = 0;
    for (position, &root) in roots.iter().enumerate() {
        block_end = block_end.max(last_position[root]);
        merged |= parts.union(roots[block_start], root);
        if position == block_end {
            block_start = position + 1;
        }
    }
    merged
}

#[cfg(test)]
mod test_interleaved_cut {
    use super::super::test_utils::parts;
    use super::*;

    fn cut(traces: &[&[&str]]) -> Option<Vec<Vec<String>>> {
        let (labels, log) = super::super::super::log::test_utils::log_of(traces);
        interleaved_cut(&log, &ActivityDfg::discover(&log)).map(|cut| {
            cut.partitions()
                .iter()
                .map(|part| part.iter().map(|&a| labels[a].clone()).collect())
                .collect()
        })
    }

    #[test]
    fn test_blocks_in_any_order() {
        // (a b) and c never overlap, but their order varies. No directly-follows cut applies.
        assert_eq!(
            cut(&[&["a", "b", "c"], &["c", "a", "b"]]),
            parts(&[&["a", "b"], &["c"]])
        );
    }

    #[test]
    fn test_overlapping_blocks_are_merged() {
        // L109 (Figure 6.15): the graph allows ↔({a}, {b, c}), but the first trace runs a around
        // b, c.
        let rest: [&[&str]; 2] = [&["b", "c", "a"], &["a", "b", "c"]];
        assert_eq!(cut(&rest), parts(&[&["a"], &["b", "c"]]));
        assert_eq!(cut(&[&["a", "b", "c", "a"], rest[0], rest[1]]), None);
    }

    #[test]
    fn test_footprint() {
        // b never starts a trace, so it belongs to the part of a.
        assert_eq!(cut(&[&["a", "b"]]), None);
        // a ends and b starts a trace, but b never follows a.
        assert_eq!(cut(&[&["a"], &["b"]]), None);
        // The blocks never overlap, but c ends no trace and is followed by d.
        assert_eq!(
            cut(&[&["a", "c", "d"], &["b", "d"], &["d", "b", "a", "b", "a"]]),
            None
        );
        // b starts no trace, yet is preceded by both a and c.
        assert_eq!(
            cut(&[&["a", "b", "c"], &["c", "b", "a"], &["c", "a", "b"]]),
            None
        );
    }

    #[test]
    fn test_no_cut() {
        assert_eq!(cut(&[&["a"]]), None);
        assert_eq!(cut(&[]), None);
        assert_eq!(cut(&[&[]]), None);
    }
}
