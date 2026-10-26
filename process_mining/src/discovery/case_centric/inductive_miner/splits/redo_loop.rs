//! Splitting a log according to a loop (`↺`) cut.

use super::super::cut_finder::cut::Cut;
use super::super::log::{ActivityID, ActivityLog};

/// Index of the loop body among the parts of a loop cut.
const BODY: usize = 0;

/// Splits a log according to a loop cut, starting a new sub-trace whenever execution leaves a
/// part.
///
/// A trace of a loop alternates between the body and the redo parts, so every maximal run of
/// events in the same part becomes one trace of that part's sub-log.
///
/// A loop starts and ends with its body and runs it between any two redo parts. Where a trace
/// skips the body, the split inserts an empty trace into the body's sub-log and lets the recursion
/// decide whether the body can be skipped.
pub fn loop_split(log: &ActivityLog, cut: &Cut) -> Vec<ActivityLog> {
    let partition_of = cut.partition_lookup(log.alphabet_size());
    let mut sub_traces = vec![Vec::new(); cut.len()];

    for variant in log.variants() {
        let mut current_partition = BODY;
        let mut run: Vec<ActivityID> = Vec::new();

        for &activity in &variant.activities {
            // Loop cuts cover every activity. Anything else goes to the body instead of being lost.
            let partition = match partition_of[activity] {
                usize::MAX => BODY,
                partition => partition,
            };

            if partition != current_partition {
                if current_partition != BODY && partition != BODY {
                    sub_traces[BODY].push((Vec::new(), variant.count));
                }
                sub_traces[current_partition].push((std::mem::take(&mut run), variant.count));
                current_partition = partition;
            }
            run.push(activity);
        }
        sub_traces[current_partition].push((run, variant.count));

        // The trace ended inside a redo part, so the final body execution is missing.
        if current_partition != BODY {
            sub_traces[BODY].push((Vec::new(), variant.count));
        }
    }

    sub_traces
        .into_iter()
        .map(|traces| log.derive(traces))
        .collect()
}

#[cfg(test)]
mod test_loop_split {
    use super::super::super::log::test_utils::{describe, expect, log_of};
    use super::*;
    use crate::core::process_models::process_tree::OperatorType::Loop;

    #[test]
    fn test_leemans_example() {
        let (labels, log) = log_of(&[
            &["a", "b"],
            &["a", "b", "c", "a", "b"],
            &["a", "b", "c", "a", "b", "c", "a", "b"],
        ]);
        let cut = Cut::new(Loop, vec![vec![0, 1], vec![2]]);

        let sub_logs = loop_split(&log, &cut);
        assert_eq!(describe(&sub_logs[0], &labels), expect(&[(&["a", "b"], 6)]));
        assert_eq!(describe(&sub_logs[1], &labels), expect(&[(&["c"], 3)]));
    }

    #[test]
    fn test_body_with_several_variants() {
        let (labels, log) = log_of(&[
            &["a", "b"],
            &["a", "b", "c", "a", "b"],
            &["a", "d", "b"],
            &["a", "d", "b", "c", "a", "d", "b"],
            &["a", "d", "b", "c", "a", "b"],
        ]);
        let cut = Cut::new(Loop, vec![vec![0, 1, 3], vec![2]]);

        let sub_logs = loop_split(&log, &cut);
        assert_eq!(
            describe(&sub_logs[0], &labels),
            expect(&[(&["a", "b"], 4), (&["a", "d", "b"], 4)])
        );
        assert_eq!(describe(&sub_logs[1], &labels), expect(&[(&["c"], 3)]));
    }

    #[test]
    fn test_two_redo_parts() {
        let (labels, log) = log_of(&[&["a", "c", "a", "d", "a"]]);
        let cut = Cut::new(Loop, vec![vec![0], vec![1], vec![2]]);

        let sub_logs = loop_split(&log, &cut);
        assert_eq!(describe(&sub_logs[0], &labels), expect(&[(&["a"], 3)]));
        assert_eq!(describe(&sub_logs[1], &labels), expect(&[(&["c"], 1)]));
        assert_eq!(describe(&sub_logs[2], &labels), expect(&[(&["d"], 1)]));

        // Going from c straight to d skips the body in between.
        let (labels, log) = log_of(&[&["a", "c", "d", "a"]]);
        let sub_logs = loop_split(&log, &cut);
        assert_eq!(
            describe(&sub_logs[0], &labels),
            expect(&[(&[], 1), (&["a"], 2)])
        );
        assert_eq!(describe(&sub_logs[1], &labels), expect(&[(&["c"], 1)]));
        assert_eq!(describe(&sub_logs[2], &labels), expect(&[(&["d"], 1)]));
    }

    #[test]
    fn test_traces_not_bounded_by_the_body_are_repaired() {
        let cut = Cut::new(Loop, vec![vec![0], vec![1]]);

        // Neither ⟨a, b⟩ nor ⟨b, a⟩ fits ↺(a, b), so the missing body execution becomes an
        // empty trace.
        for traces in [&[&["a", "b"] as &[&str]] as &[&[&str]], &[&["b", "a"]]] {
            let (labels, log) = log_of(traces);
            let sub_logs = loop_split(&log, &cut);
            assert_eq!(
                describe(&sub_logs[0], &labels),
                expect(&[(&[], 1), (&["a"], 1)])
            );
            assert_eq!(describe(&sub_logs[1], &labels), expect(&[(&["b"], 1)]));
        }

        // An empty trace is one empty body execution.
        let (labels, base) = log_of(&[&["a", "b"]]);
        let log = base.derive([(vec![], 1), (vec![0], 1)]);
        let sub_logs = loop_split(&log, &cut);
        assert_eq!(
            describe(&sub_logs[0], &labels),
            expect(&[(&[], 1), (&["a"], 1)])
        );
        assert!(sub_logs[1].is_empty());
    }
}
