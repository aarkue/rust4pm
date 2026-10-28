//! The activity-concurrent fall through.

use rayon::prelude::*;

use super::super::cut_finder::{find_cut, find_cut_filtering};
use super::super::dfg::ActivityDfg;
use super::super::log::{ActivityID, ActivityLog};
use super::super::InductiveMinerOptions;
use super::Fallthrough;

/// The activities of the log by name, or by descending frequency with ties by name. Ids follow the
/// names.
fn candidates(log: &ActivityLog, by_name: bool) -> Vec<ActivityID> {
    let mut activities = log.activities();
    if !by_name {
        let mut occurrences = vec![0u64; log.alphabet_size()];
        for variant in log.variants() {
            for &activity in &variant.activities {
                occurrences[activity] += variant.count;
            }
        }
        activities.sort_by_key(|&activity| (std::cmp::Reverse(occurrences[activity]), activity));
    }
    activities
}

/// Takes one activity out of the log if that makes a cut appear, and puts it next to the rest.
///
/// The extracted activity may occur several times in a trace, so this comes after
/// [`activity_once_per_trace`](super::activity_once_per_trace).
///
/// The probe reads the unfiltered graph, even under `IMf`. See
/// [`filter_activity_concurrent_probe`](InductiveMinerOptions::filter_activity_concurrent_probe).
pub fn activity_concurrent(
    log: &ActivityLog,
    options: &InductiveMinerOptions,
) -> Option<Fallthrough> {
    if log.activities().len() < 2 {
        return None;
    }

    // `find_map_first` returns the first candidate in order, so the result is deterministic.
    candidates(log, options.activity_concurrent_by_name)
        .into_par_iter()
        .find_map_first(|activity| {
            // Emptied traces don't change the graph. The recursion on the rest makes them optional.
            let rest = log.without_activity(activity);
            let dfg = ActivityDfg::discover(&rest);
            let cut = if options.filter_activity_concurrent_probe {
                find_cut_filtering(&rest, &dfg, options)
            } else {
                find_cut(&rest, &dfg, options)
            };
            cut.map(|_| (activity, rest))
        })
        .map(|(activity, rest)| Fallthrough::ActivityConcurrent {
            activity_log: log.projected_onto(activity),
            rest,
        })
}

/// Puts the two activities of a log next to each other, the last thing to try before the flower
/// model.
///
/// Only a log over two activities gets here, since anything larger leaves a rest that
/// [`activity_concurrent`] has already checked for a cut.
pub fn two_activities_concurrent(
    log: &ActivityLog,
    options: &InductiveMinerOptions,
) -> Option<Fallthrough> {
    if log.activities().len() != 2 {
        return None;
    }

    let activity = candidates(log, options.activity_concurrent_by_name)[0];
    Some(Fallthrough::ActivityConcurrent {
        activity_log: log.projected_onto(activity),
        rest: log.without_activity(activity),
    })
}

#[cfg(test)]
mod test_activity_concurrent {
    use super::super::super::log::test_utils::{describe, expect, log_of};
    use super::*;

    #[test]
    fn test_leemans_example() {
        // L81 has no cut, but removing d exposes the sequence a → b → c in the rest.
        // (In the full miner, activity-once-per-trace catches this log first.)
        let (labels, log) = log_of(&[
            &["a", "b", "c", "d"],
            &["d", "a", "b"],
            &["a", "d", "c"],
            &["b", "c", "d"],
        ]);

        let Some(Fallthrough::ActivityConcurrent { activity_log, rest }) =
            activity_concurrent(&log, &InductiveMinerOptions::default())
        else {
            panic!("expected the fall through to apply");
        };

        assert_eq!(describe(&activity_log, &labels), expect(&[(&["d"], 4)]));
        assert_eq!(rest.activities(), vec![0, 1, 2]);
    }

    #[test]
    fn test_extracted_activity_may_repeat() {
        // x occurs twice in one trace, so its projection is not a single-activity log.
        let (labels, log) = log_of(&[&["x", "a", "x", "b"], &["b", "x", "a"], &["x", "a", "b"]]);

        let Some(Fallthrough::ActivityConcurrent { activity_log, .. }) =
            activity_concurrent(&log, &InductiveMinerOptions::default())
        else {
            panic!("expected the fall through to apply");
        };
        assert_eq!(
            describe(&activity_log, &labels),
            expect(&[(&["x", "x"], 1), (&["x"], 2)])
        );
    }

    #[test]
    fn test_two_activities() {
        // b repeats around a, so no cut and no tau loop applies. Putting b next to a beats a
        // flower model over both.
        let (labels, log) = log_of(&[&["a", "b", "b", "a"], &["b", "a", "a", "b"]]);
        let Some(Fallthrough::ActivityConcurrent { activity_log, rest }) =
            two_activities_concurrent(&log, &InductiveMinerOptions::default())
        else {
            panic!("expected the fall through to apply");
        };
        assert_eq!(
            describe(&activity_log, &labels),
            expect(&[(&["a", "a"], 2)])
        );
        assert_eq!(describe(&rest, &labels), expect(&[(&["b", "b"], 2)]));

        // Three activities are for activity_concurrent to handle.
        let (_, log) = log_of(&[&["a", "b", "c", "a"], &["c", "b", "a", "b"]]);
        assert!(two_activities_concurrent(&log, &InductiveMinerOptions::default()).is_none());

        // Taking a out empties a trace, which the rest then makes optional.
        let (labels, log) = log_of(&[&["a", "b", "b", "a"], &["a", "a"]]);
        let Some(Fallthrough::ActivityConcurrent { rest, .. }) =
            two_activities_concurrent(&log, &InductiveMinerOptions::default())
        else {
            panic!("expected the fall through to apply");
        };
        assert_eq!(
            describe(&rest, &labels),
            expect(&[(&[], 1), (&["b", "b"], 1)])
        );
    }

    #[test]
    fn test_the_probe_reads_the_unfiltered_graph() {
        // Taking out any one activity leaves a cycle. Filtering the rest breaks it and invents a
        // cut.
        let (_, log) = log_of(&[
            &["a", "b", "b", "b", "b"],
            &["a", "b", "c", "b", "b", "c"],
            &["a", "b", "c", "b", "a", "b"],
        ]);

        let imf = InductiveMinerOptions::imf(0.2);
        assert!(activity_concurrent(&log, &imf).is_none());
        assert!(activity_concurrent(
            &log,
            &InductiveMinerOptions {
                filter_activity_concurrent_probe: true,
                ..imf
            }
        )
        .is_some());

        // Plain IM never filters, so the option cannot reach it.
        let im = InductiveMinerOptions::default();
        assert_eq!(
            activity_concurrent(&log, &im),
            activity_concurrent(
                &log,
                &InductiveMinerOptions {
                    filter_activity_concurrent_probe: true,
                    ..im
                }
            )
        );
    }

    #[test]
    fn test_does_not_apply() {
        let options = InductiveMinerOptions::default();

        // Removing either activity leaves a log over a single activity, which has no cut.
        let (_, log) = log_of(&[&["a", "b", "a"], &["b", "a", "b"]]);
        assert!(activity_concurrent(&log, &options).is_none());

        // Every removal empties a trace.
        let (_, log) = log_of(&[&["a", "b"], &["b", "a"], &["a"], &["b"]]);
        assert!(activity_concurrent(&log, &options).is_none());

        // Nothing left to be concurrent to.
        let (_, log) = log_of(&[&["a", "a"]]);
        assert!(activity_concurrent(&log, &options).is_none());
    }
}
