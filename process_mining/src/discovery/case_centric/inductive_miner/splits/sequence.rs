//! Splitting a log according to a sequence (`→`) cut.

use super::super::cut_finder::cut::Cut;
use super::super::log::{ActivityID, ActivityLog};

/// Splits a log according to a sequence cut by cutting each trace into `n` consecutive segments.
///
/// If the cut adheres to →.1, every trace is already of the form `t1 · t2 · ... · tn` with the
/// activities of `ti` in `Σi`, and the split just finds those boundaries. If it does not, which
/// only happens for a cut `IMf` detected on a filtered graph, the boundaries are placed so that as
/// few events as possible end up on the wrong side. Those are dropped.
///
/// Sub-logs may contain empty traces: splitting `⟨b, b, a⟩` on `(→, {a}, {b})` yields `[ε]` for
/// `{a}` and `[⟨b, b⟩]` for `{b}`. A later recursion decides whether that is frequent enough to
/// model the branch as optional.
pub fn sequence_split(log: &ActivityLog, cut: &Cut) -> Vec<ActivityLog> {
    let partition_of = cut.partition_lookup(log.alphabet_size());
    let mut sub_traces = vec![Vec::new(); cut.len()];

    for variant in log.variants() {
        let trace = &variant.activities;
        let mut segment_start = 0;

        for (partition, segments) in sub_traces.iter_mut().enumerate() {
            let segment_end = find_split_point(trace, &partition_of, partition, segment_start);
            let segment: Vec<ActivityID> = trace[segment_start..segment_end]
                .iter()
                .copied()
                .filter(|&activity| partition_of[activity] == partition)
                .collect();
            segments.push((segment, variant.count));
            segment_start = segment_end;
        }
    }

    sub_traces
        .into_iter()
        .map(|traces| log.derive(traces))
        .collect()
}

/// Finds where the segment of `partition` ends, starting the search at `start`.
///
/// Walks the rest of the trace once, keeping a running cost: an event of `partition` makes
/// including it one step cheaper, an event of a later part one step more expensive. Events of
/// earlier parts are ignored, since they are already misplaced and cutting around them changes
/// nothing. The cheapest position leaves the fewest events on the wrong side.
fn find_split_point(
    trace: &[ActivityID],
    partition_of: &[usize],
    partition: usize,
    start: usize,
) -> usize {
    let mut least_cost = 0i64;
    let mut split_point = start;
    let mut cost = 0i64;

    for (position, &activity) in trace.iter().enumerate().skip(start) {
        let part = partition_of[activity];
        if part == partition {
            cost -= 1;
        } else if part > partition {
            // Belongs to a later part (or to no part at all, `usize::MAX`).
            cost += 1;
        }

        if cost < least_cost {
            least_cost = cost;
            split_point = position + 1;
        }
    }

    split_point
}

#[cfg(test)]
mod test_sequence_split {
    use super::super::super::log::test_utils::{describe, expect, log_of};
    use super::*;
    use crate::core::process_models::process_tree::OperatorType::Sequence;

    #[test]
    fn test_split_on_the_part_boundaries() {
        let (labels, log) = log_of(&[&["a", "b", "c"], &["b", "a", "c"]]);
        let cut = Cut::new(Sequence, vec![vec![0, 1], vec![2]]);

        let sub_logs = sequence_split(&log, &cut);
        assert_eq!(
            describe(&sub_logs[0], &labels),
            expect(&[(&["a", "b"], 1), (&["b", "a"], 1)])
        );
        assert_eq!(describe(&sub_logs[1], &labels), expect(&[(&["c"], 2)]));

        let (labels, log) = log_of(&[&["a", "b", "c", "d"]]);
        let cut = Cut::new(Sequence, vec![vec![0], vec![1], vec![2], vec![3]]);
        let sub_logs = sequence_split(&log, &cut);
        assert_eq!(sub_logs.len(), 4);
        assert_eq!(describe(&sub_logs[3], &labels), expect(&[(&["d"], 1)]));
    }

    #[test]
    fn test_repeated_activities() {
        let (labels, log) = log_of(&[&["a", "b", "c", "b", "c"], &["a", "a", "c"]]);
        let cut = Cut::new(Sequence, vec![vec![0], vec![1, 2]]);

        let sub_logs = sequence_split(&log, &cut);
        assert_eq!(
            describe(&sub_logs[0], &labels),
            expect(&[(&["a"], 1), (&["a", "a"], 1)])
        );
        assert_eq!(
            describe(&sub_logs[1], &labels),
            expect(&[(&["b", "c", "b", "c"], 1), (&["c"], 1)])
        );
    }

    #[test]
    fn test_deviating_event_is_dropped() {
        // The leading c of the third trace is on the wrong side of the split point.
        let (labels, log) = log_of(&[&["a", "b", "c"], &["b", "a", "c"], &["c", "a", "b", "c"]]);
        let cut = Cut::new(Sequence, vec![vec![0, 1], vec![2]]);

        let sub_logs = sequence_split(&log, &cut);
        assert_eq!(
            describe(&sub_logs[0], &labels),
            expect(&[(&["a", "b"], 2), (&["b", "a"], 1)])
        );
        assert_eq!(describe(&sub_logs[1], &labels), expect(&[(&["c"], 3)]));
    }

    #[test]
    fn test_split_may_produce_empty_traces() {
        // Splitting ⟨b, b, a⟩ on (→, {a}, {b}) leaves the {a} sub-log empty.
        let (labels, log) = log_of(&[&["b", "b", "a"]]);
        let cut = Cut::new(Sequence, vec![vec![0], vec![1]]);

        let sub_logs = sequence_split(&log, &cut);
        assert_eq!(describe(&sub_logs[0], &labels), expect(&[(&[], 1)]));
        assert_eq!(describe(&sub_logs[1], &labels), expect(&[(&["b", "b"], 1)]));
    }
}
