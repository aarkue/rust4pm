//! The activity-once-per-trace fall through.

use rayon::prelude::*;

use super::super::cut_finder::cut::precedence;
use super::super::cut_finder::find_cut;
use super::super::dfg::ActivityDfg;
use super::super::log::{ActivityID, ActivityLog};
use super::super::InductiveMinerOptions;
use super::Fallthrough;

/// Puts an activity that occurs exactly once in every trace next to the rest of the log.
///
/// Whatever structure the remaining activities have, such an `a` can happen anywhere in it, which
/// is concurrency. The log is split into the projection onto `a`, which is `[⟨a⟩ⁿ]` and discovers
/// the leaf `a`, and the log without `a`, giving `∧(a, IM(L \ a))`. Taking `a` out loses no
/// information, which makes this the most precise of the fall throughs.
///
/// If several activities qualify, the one whose remaining log admits the earliest cut is taken.
/// Returns `None` if none does, or if the log has only one activity.
pub fn activity_once_per_trace(
    log: &ActivityLog,
    options: &InductiveMinerOptions,
) -> Option<Fallthrough> {
    let activities = log.activities();
    if activities.len() < 2 {
        return None;
    }

    let mut once_per_trace = vec![true; log.alphabet_size()];
    let mut occurrences = vec![0usize; log.alphabet_size()];
    for variant in log.variants() {
        occurrences.fill(0);
        for &activity in &variant.activities {
            occurrences[activity] += 1;
        }
        for &activity in &activities {
            once_per_trace[activity] &= occurrences[activity] == 1;
        }
    }

    let candidates: Vec<ActivityID> = activities
        .into_iter()
        .filter(|&activity| once_per_trace[activity])
        .collect();
    let activity = best_candidate(log, &candidates, options)?;

    Some(Fallthrough::ActivityConcurrent {
        activity_log: log.projected_onto(activity),
        rest: log.without_activity(activity),
    })
}

/// Of the activities that qualify, the one whose remaining log the miner can structure best.
///
/// Taking any of them out fits the log, but the choice decides what is left to discover. In
/// `⟨a,b,c⟩, ⟨c,a,b⟩` all three occur once per trace. Leaving out `c` leaves the sequence `a, b`,
/// and leaving out `a` leaves the less precise concurrency of `b` and `c`. Candidates are ranked by
/// the cut their remaining log admits, in the order cut detection tries them, then by activity id.
///
/// Cut detection has to run once per candidate, so it is skipped where there is nothing to choose.
fn best_candidate(
    log: &ActivityLog,
    candidates: &[ActivityID],
    options: &InductiveMinerOptions,
) -> Option<ActivityID> {
    match candidates {
        [] => None,
        [only] => Some(*only),
        // The key ends in the activity id, so ties cannot make the parallel minimum vary.
        _ => candidates.par_iter().copied().min_by_key(|&activity| {
            let rest = log.without_activity(activity);
            let cut = find_cut(&rest, &ActivityDfg::discover(&rest), options);
            (
                cut.map_or(usize::MAX, |cut| precedence(cut.operator())),
                activity,
            )
        }),
    }
}

#[cfg(test)]
mod test_activity_once_per_trace {
    use super::super::super::log::test_utils::{describe, expect, log_of};
    use super::*;

    fn once_per_trace(log: &ActivityLog) -> Option<Fallthrough> {
        activity_once_per_trace(log, &InductiveMinerOptions::default())
    }

    #[test]
    fn test_leemans_example() {
        // L81: d occurs exactly once in every trace, so it is put concurrent to the rest.
        let (labels, log) = log_of(&[
            &["a", "b", "c", "d"],
            &["d", "a", "b"],
            &["a", "d", "c"],
            &["b", "c", "d"],
        ]);

        let Some(Fallthrough::ActivityConcurrent { activity_log, rest }) = once_per_trace(&log)
        else {
            panic!("expected the fall through to apply");
        };
        assert_eq!(describe(&activity_log, &labels), expect(&[(&["d"], 4)]));
        assert_eq!(
            describe(&rest, &labels),
            expect(&[
                (&["a", "b", "c"], 1),
                (&["a", "b"], 1),
                (&["a", "c"], 1),
                (&["b", "c"], 1),
            ])
        );
    }

    #[test]
    fn test_which_activity_is_taken() {
        // All three qualify; taking a or b out leaves a sequence, taking c out leaves a
        // concurrency, so the id decides between the first two.
        let (labels, log) = log_of(&[&["a", "b", "c"], &["b", "a", "c"]]);
        let Some(Fallthrough::ActivityConcurrent { activity_log, .. }) = once_per_trace(&log)
        else {
            panic!("expected the fall through to apply");
        };
        assert_eq!(describe(&activity_log, &labels), expect(&[(&["a"], 2)]));

        // Only b occurs once in every trace here.
        let (_, log) = log_of(&[&["a", "b"], &["b"]]);
        let Some(Fallthrough::ActivityConcurrent { activity_log, .. }) = once_per_trace(&log)
        else {
            panic!("expected the fall through to apply");
        };
        assert_eq!(activity_log.activities(), vec![1]);
    }

    #[test]
    fn test_does_not_apply() {
        // No activity occurs exactly once in every trace.
        let (_, log) = log_of(&[&["a", "b", "a"], &["b", "a", "b"]]);
        assert!(once_per_trace(&log).is_none());

        // Taking the only activity out would leave nothing to be concurrent to.
        let (_, log) = log_of(&[&["a"], &["a"]]);
        assert!(once_per_trace(&log).is_none());

        let (_, log) = log_of(&[]);
        assert!(once_per_trace(&log).is_none());
    }
}
