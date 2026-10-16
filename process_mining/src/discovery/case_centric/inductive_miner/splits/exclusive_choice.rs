//! Splitting a log according to an exclusive choice (`×`) cut.

use super::super::cut_finder::cut::Cut;
use super::super::log::ActivityLog;

/// Splits a log according to an exclusive choice cut.
///
/// A trace belongs to exactly one branch, so each trace goes into a single sub-log: the one whose
/// part covers most of its events. Events outside that part are dropped. For a cut adhering to
/// ×.1 no trace can contain events from two parts anyway, since that would need an edge between
/// them somewhere along the trace.
///
/// Sub-logs may come out empty, which the empty-log base case handles.
pub fn exclusive_choice_split(log: &ActivityLog, cut: &Cut) -> Vec<ActivityLog> {
    let partition_of = cut.partition_lookup(log.alphabet_size());
    let mut sub_traces = vec![Vec::new(); cut.len()];

    let mut events_per_partition = vec![0usize; cut.len()];
    for variant in log.variants() {
        events_per_partition.fill(0);
        for &activity in &variant.activities {
            let partition = partition_of[activity];
            if partition != usize::MAX {
                events_per_partition[partition] += 1;
            }
        }

        // Ties go to the first part, so empty traces and traces of only unassigned activities
        // land in the first sub-log.
        let branch = events_per_partition
            .iter()
            .enumerate()
            .max_by_key(|&(index, &events)| (events, std::cmp::Reverse(index)))
            .map_or(0, |(index, _)| index);

        let projected: Vec<usize> = variant
            .activities
            .iter()
            .copied()
            .filter(|&activity| partition_of[activity] == branch)
            .collect();
        sub_traces[branch].push((projected, variant.count));
    }

    sub_traces
        .into_iter()
        .map(|traces| log.derive(traces))
        .collect()
}

#[cfg(test)]
mod test_exclusive_choice_split {
    use super::super::super::log::test_utils::{describe, expect, log_of};
    use super::*;
    use crate::core::process_models::process_tree::OperatorType::ExclusiveChoice;

    #[test]
    fn test_leemans_example() {
        let (labels, log) = log_of(&[&["a", "b"], &["c", "c", "c"]]);
        let cut = Cut::new(ExclusiveChoice, vec![vec![0, 1], vec![2]]);

        let sub_logs = exclusive_choice_split(&log, &cut);
        assert_eq!(describe(&sub_logs[0], &labels), expect(&[(&["a", "b"], 1)]));
        assert_eq!(
            describe(&sub_logs[1], &labels),
            expect(&[(&["c", "c", "c"], 1)])
        );
    }

    #[test]
    fn test_traces_are_counted_with_multiplicity() {
        let (labels, log) = log_of(&[&["a"], &["a"], &["b"]]);
        let cut = Cut::new(ExclusiveChoice, vec![vec![0], vec![1]]);

        let sub_logs = exclusive_choice_split(&log, &cut);
        assert_eq!(describe(&sub_logs[0], &labels), expect(&[(&["a"], 2)]));
        assert_eq!(describe(&sub_logs[1], &labels), expect(&[(&["b"], 1)]));
    }

    #[test]
    fn test_deviating_events_are_dropped() {
        // The trace a, b, b has more b's than a's, so it joins the b branch and the a is dropped.
        let (labels, log) = log_of(&[&["a", "b", "b"]]);
        let cut = Cut::new(ExclusiveChoice, vec![vec![0], vec![1]]);

        let sub_logs = exclusive_choice_split(&log, &cut);
        assert!(sub_logs[0].is_empty());
        assert_eq!(describe(&sub_logs[1], &labels), expect(&[(&["b", "b"], 1)]));
    }

    #[test]
    fn test_empty_traces_go_to_the_first_branch() {
        let (labels, log) = log_of(&[&[], &["a"], &["b"]]);
        let cut = Cut::new(ExclusiveChoice, vec![vec![0], vec![1]]);

        let sub_logs = exclusive_choice_split(&log, &cut);
        assert_eq!(
            describe(&sub_logs[0], &labels),
            expect(&[(&[], 1), (&["a"], 1)])
        );
        assert_eq!(describe(&sub_logs[1], &labels), expect(&[(&["b"], 1)]));
    }
}
