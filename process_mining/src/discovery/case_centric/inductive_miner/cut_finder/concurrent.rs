//! Detection of concurrent (`∧`) cuts.

use crate::core::process_models::process_tree::OperatorType;

use super::super::dfg::ActivityDfg;
use super::super::structures::complement_components::complement_components;
use super::super::structures::minimum_self_distance::MinimumSelfDistance;
use super::cut::Cut;

/// Finds the maximal concurrent cut, if there is one.
///
/// Two requirements: every part contains a start and an end activity (∧.1), and all parts are
/// connected to each other in both directions (∧.2). Two activities that are not connected both
/// ways therefore have to share a part. If a minimum-self-distance relation is given, activities
/// that loop around each other are merged as well (∧↔.1). This prevents reporting a loop as
/// concurrency.
pub fn concurrent_cut(
    dfg: &ActivityDfg,
    minimum_self_distance: Option<&MinimumSelfDistance>,
) -> Option<Cut> {
    let n = dfg.len();
    if n == 0 {
        return None;
    }

    let may_be_separated = |a: usize, b: usize| {
        let fully_connected = dfg.follows(a, b) && dfg.follows(b, a);
        let loops_around = minimum_self_distance
            .is_some_and(|msd| msd.are_related(dfg.activity(a), dfg.activity(b)));
        fully_connected && !loops_around
    };

    let groups = complete_parts(dfg, complement_components(n, may_be_separated));
    let cut = Cut::new(
        OperatorType::Concurrency,
        super::to_partitions(dfg, &groups),
    );
    cut.is_non_trivial().then_some(cut)
}

/// Merges parts until every part has a start and an end activity (∧.1), keeping as many parts as
/// possible: parts that complement each other are paired first, and only the rest joins an
/// already complete part.
fn complete_parts(dfg: &ActivityDfg, groups: Vec<Vec<usize>>) -> Vec<Vec<usize>> {
    let mut absorbing = Vec::new();
    let mut missing_end = Vec::new();
    let mut missing_start = Vec::new();
    let mut missing_both = Vec::new();
    for (index, group) in groups.iter().enumerate() {
        let has_start = group.iter().any(|&a| dfg.is_start(a));
        let has_end = group.iter().any(|&a| dfg.is_end(a));
        match (has_start, has_end) {
            (true, true) => absorbing.push(index),
            (true, false) => missing_end.push(index),
            (false, true) => missing_start.push(index),
            (false, false) => missing_both.push(index),
        }
    }

    let mut merged_into: Vec<usize> = (0..groups.len()).collect();

    let paired = missing_end.len().min(missing_start.len());
    for (&with_start, &with_end) in missing_end.iter().zip(&missing_start) {
        merged_into[with_end] = with_start;
        absorbing.push(with_start);
    }

    let leftover: Vec<usize> = missing_end
        .into_iter()
        .skip(paired)
        .chain(missing_start.into_iter().skip(paired))
        .chain(missing_both)
        .collect();
    if !leftover.is_empty() {
        match absorbing.iter().min() {
            Some(&target) => leftover.into_iter().for_each(|i| merged_into[i] = target),
            // Nothing can satisfy ∧.1, so everything collapses into one part and there is no cut.
            None => merged_into.iter_mut().for_each(|target| *target = 0),
        }
    }

    let mut merged = vec![Vec::new(); groups.len()];
    for (group, &target) in groups.into_iter().zip(&merged_into) {
        merged[target].extend(group);
    }
    merged
        .into_iter()
        .filter(|group| !group.is_empty())
        .map(|mut group| {
            group.sort_unstable();
            group
        })
        .collect()
}

#[cfg(test)]
mod test_concurrent_cut {
    use super::super::super::dfg::test_utils::dfg_of;
    use super::super::super::log::test_utils::log_of;
    use super::super::test_utils::{cut_of, parts};
    use super::*;

    fn cut(traces: &[&[&str]]) -> Option<Vec<Vec<String>>> {
        cut_of(traces, |dfg| concurrent_cut(dfg, None))
    }

    #[test]
    fn test_concurrent_activities() {
        assert_eq!(cut(&[&["a", "b"], &["b", "a"]]), parts(&[&["a"], &["b"]]));
        // c is concurrent to the sequence a, b.
        assert_eq!(
            cut(&[&["a", "b", "c"], &["a", "c", "b"], &["c", "a", "b"]]),
            parts(&[&["a", "b"], &["c"]])
        );
        assert_eq!(
            cut(&[
                &["a", "b", "c"],
                &["a", "c", "b"],
                &["b", "a", "c"],
                &["b", "c", "a"],
                &["c", "a", "b"],
                &["c", "b", "a"],
            ]),
            parts(&[&["a"], &["b"], &["c"]])
        );
    }

    #[test]
    fn test_no_cut() {
        // A sequence, a choice with no edges between the branches, a graph missing the edges
        // c ↦ a and b ↦ c, and a loop over b in which b neither starts nor ends a trace.
        assert_eq!(cut(&[&["a", "b", "c"], &["a", "d", "c"]]), None);
        assert_eq!(cut(&[&["a", "b"], &["c", "d"]]), None);
        assert_eq!(
            cut(&[&["b", "a", "c"], &["a", "c", "b"], &["c", "b", "a"]]),
            None
        );
        assert_eq!(cut(&[&["a"], &["a", "b", "a"]]), None);
    }

    #[test]
    fn test_minimum_self_distance_prevents_a_cut() {
        // b sits between the two closest a's, so a loops around b rather than running next to it.
        let (_, log) = log_of(&[&["a", "b", "a"], &["b", "a", "b"]]);
        let dfg = ActivityDfg::discover(&log);

        assert!(concurrent_cut(&dfg, None).is_some());
        assert!(concurrent_cut(&dfg, Some(&MinimumSelfDistance::discover(&log))).is_none());
    }

    /// All three activities are connected both ways, so only the start and end activities decide.
    fn triangle(start: &[usize], end: &[usize]) -> ActivityDfg {
        dfg_of(
            3,
            &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)],
            start,
            end,
        )
    }

    #[test]
    fn test_parts_are_completed_keeping_as_many_as_possible() {
        // Every part is complete already.
        let cut = concurrent_cut(&triangle(&[0, 1, 2], &[0, 1, 2]), None).unwrap();
        assert_eq!(cut.partitions(), &[vec![0], vec![1], vec![2]]);

        // {a} only starts, {b} starts and ends, {c} only ends: pairing a with c keeps two parts.
        let cut = concurrent_cut(&triangle(&[0, 1], &[1, 2]), None).unwrap();
        assert_eq!(cut.partitions(), &[vec![0, 2], vec![1]]);

        // {b} neither starts nor ends and has to join a complete part.
        let cut = concurrent_cut(&triangle(&[0, 2], &[0, 2]), None).unwrap();
        assert_eq!(cut.len(), 2);
    }

    #[test]
    fn test_no_cut_when_parts_cannot_be_completed() {
        // One start and two ends: a must join one of them, leaving the other without a start.
        assert!(concurrent_cut(&triangle(&[0], &[1, 2]), None).is_none());
        assert!(concurrent_cut(&triangle(&[0, 1], &[2]), None).is_none());
    }
}
