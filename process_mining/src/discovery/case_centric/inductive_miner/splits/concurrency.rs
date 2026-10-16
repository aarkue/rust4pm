//! Splitting a log according to a concurrent (`∧`) or inclusive choice (`∨`) cut.

use super::super::cut_finder::cut::Cut;
use super::super::log::ActivityLog;

/// Splits a log according to a concurrent cut by projecting every trace onto every part.
///
/// A concurrent operator allows all interleavings of its children, so no event can be deviating
/// and nothing is filtered out. Each trace contributes its projection to every sub-log, keeping
/// the relative order of the events within a part.
pub fn concurrency_split(log: &ActivityLog, cut: &Cut) -> Vec<ActivityLog> {
    split(log, cut, true)
}

/// Splits a log according to an inclusive choice cut.
///
/// Like [`concurrency_split`], but a part the trace does not touch gets no trace instead of an
/// empty one, since `∨` already models not choosing it.
pub fn inclusive_choice_split(log: &ActivityLog, cut: &Cut) -> Vec<ActivityLog> {
    split(log, cut, false)
}

fn split(log: &ActivityLog, cut: &Cut, keep_empty: bool) -> Vec<ActivityLog> {
    let partition_of = cut.partition_lookup(log.alphabet_size());
    let mut sub_traces = vec![Vec::new(); cut.len()];

    for variant in log.variants() {
        let mut projections = vec![Vec::new(); cut.len()];
        for &activity in &variant.activities {
            let partition = partition_of[activity];
            if partition != usize::MAX {
                projections[partition].push(activity);
            }
        }
        for (partition, projection) in projections.into_iter().enumerate() {
            if keep_empty || !projection.is_empty() {
                sub_traces[partition].push((projection, variant.count));
            }
        }
    }

    sub_traces
        .into_iter()
        .map(|traces| log.derive(traces))
        .collect()
}

#[cfg(test)]
mod test_concurrency_split {
    use super::super::super::log::test_utils::{describe, expect, log_of};
    use super::*;
    use crate::core::process_models::process_tree::OperatorType::Concurrency;

    #[test]
    fn test_leemans_example() {
        let (labels, log) = log_of(&[&["a", "b", "c"], &["a", "c", "b"], &["c", "a", "b"]]);
        let cut = Cut::new(Concurrency, vec![vec![0, 1], vec![2]]);

        let sub_logs = concurrency_split(&log, &cut);
        assert_eq!(describe(&sub_logs[0], &labels), expect(&[(&["a", "b"], 3)]));
        assert_eq!(describe(&sub_logs[1], &labels), expect(&[(&["c"], 3)]));
    }

    #[test]
    fn test_every_trace_reaches_every_part() {
        // A trace touching only one part still produces a trace in each sub-log, empty for the
        // branch that did nothing.
        let (labels, log) = log_of(&[&[], &["a", "b"], &["b", "a"]]);
        let cut = Cut::new(Concurrency, vec![vec![0], vec![1]]);

        let sub_logs = concurrency_split(&log, &cut);
        assert_eq!(
            describe(&sub_logs[0], &labels),
            expect(&[(&[], 1), (&["a"], 2)])
        );
        assert_eq!(
            describe(&sub_logs[1], &labels),
            expect(&[(&[], 1), (&["b"], 2)])
        );
    }
}
