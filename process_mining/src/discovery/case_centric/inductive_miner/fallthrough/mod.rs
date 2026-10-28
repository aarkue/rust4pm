//! Fall throughs: what to do when neither a base case nor a cut applies.
//!
//! The miner has to return a process tree for every log, including logs whose directly-follows
//! graph shows none of the four footprints. The fall throughs are tried in a fixed order chosen to
//! preserve as much precision as possible (§6.1.2.4), each giving up a little more structure than
//! the previous. The last one, the flower model, always applies.

use super::dfg::ActivityDfg;
use super::log::{ActivityID, ActivityLog};
use super::InductiveMinerOptions;

mod activity_concurrent;
mod activity_once_per_trace;
mod tau_loop;

pub use activity_concurrent::{activity_concurrent, two_activities_concurrent};
pub use activity_once_per_trace::activity_once_per_trace;
pub use tau_loop::{strict_tau_loop, tau_loop};

/// The sub-tree a fall through decided on, described in terms of the logs to recurse on.
#[derive(Debug, Clone, PartialEq)]
pub enum Fallthrough {
    /// `×(τ, IM(log))`, for a log with empty traces.
    OptionalEmptyTraces(ActivityLog),
    /// `IM(log)`, the log having contained too few empty traces to be worth modeling, so they were
    /// dropped. Only `IMf` produces this.
    DroppedEmptyTraces(ActivityLog),
    /// `∧(IM(activity_log), IM(rest))`, taking one activity out of the log and putting it next
    /// to the rest.
    ActivityConcurrent {
        /// The log projected onto the extracted activity.
        activity_log: ActivityLog,
        /// The log without the extracted activity.
        rest: ActivityLog,
    },
    /// `↺(IM(log), τ)`, for a log that looks like repeated execution of a smaller one.
    TauLoop(ActivityLog),
    /// `↺(×(a₁, ..., aₙ), τ)`, the flower model.
    FlowerModel(Vec<ActivityID>),
}

/// Handles empty traces, the first fall through.
///
/// Empty traces mean the whole sub-process could be skipped, modeled as `×(τ, ...)`. `IMf` only
/// models them if at least a fraction
/// [`noise_threshold`](InductiveMinerOptions::noise_threshold) of the traces are
/// empty (more than that without
/// [`keep_at_threshold`](InductiveMinerOptions::keep_at_threshold)). At `0.0` every empty trace
/// counts.
///
/// Returns `None` if the log contains no empty traces.
pub fn empty_traces(log: &ActivityLog, options: &InductiveMinerOptions) -> Option<Fallthrough> {
    if !log.contains_empty_trace() {
        return None;
    }

    let rest = log.without_empty_traces();
    let empty = log.num_empty_traces() as f64;
    let cutoff = log.num_traces() as f64 * options.noise_threshold;
    let frequent_enough = if options.keep_at_threshold {
        empty >= cutoff
    } else {
        empty > cutoff
    };

    Some(if frequent_enough {
        Fallthrough::OptionalEmptyTraces(rest)
    } else {
        Fallthrough::DroppedEmptyTraces(rest)
    })
}

/// The flower model, the fall through of last resort. Allows any sequence of the log's
/// activities, so it always fits but is as imprecise as a model can get.
pub fn flower_model(log: &ActivityLog) -> Fallthrough {
    Fallthrough::FlowerModel(log.activities())
}

/// Picks the fall through to apply to a log for which no base case and no cut worked.
///
/// Expects a log without empty traces, which [`empty_traces`] handles before base cases and cut
/// detection are attempted. Returns the flower model right away if fall throughs are disabled.
pub fn find_fallthrough(
    log: &ActivityLog,
    dfg: &ActivityDfg,
    options: &InductiveMinerOptions,
) -> Fallthrough {
    if !options.use_fallthroughs {
        return flower_model(log);
    }

    activity_once_per_trace(log, options)
        .or_else(|| activity_concurrent(log, options))
        .or_else(|| strict_tau_loop(log, dfg, options.use_degenerate_tau_loops))
        .or_else(|| tau_loop(log, dfg, options.use_degenerate_tau_loops))
        .or_else(|| two_activities_concurrent(log, options))
        .unwrap_or_else(|| flower_model(log))
}

#[cfg(test)]
mod test_fallthrough {
    use super::super::log::test_utils::{describe, expect, log_of};
    use super::*;

    #[test]
    fn test_empty_traces() {
        let (labels, base) = log_of(&[&["a"]]);
        let im = InductiveMinerOptions::default();
        let imf = InductiveMinerOptions::imf(0.2);
        assert_eq!(empty_traces(&base, &im), None);

        // Plain IM models any empty trace as optionality.
        let log = base.derive([(vec![], 1), (vec![0], 1000)]);
        let Some(Fallthrough::OptionalEmptyTraces(rest)) = empty_traces(&log, &im) else {
            panic!("expected the empty traces to be modeled as optional");
        };
        assert_eq!(describe(&rest, &labels), expect(&[(&["a"], 1000)]));

        // IMf drops one in a thousand but keeps a fifth.
        assert!(matches!(
            empty_traces(&log, &imf),
            Some(Fallthrough::DroppedEmptyTraces(_))
        ));
        let log = base.derive([(vec![], 250), (vec![0], 1000)]);
        assert!(matches!(
            empty_traces(&log, &imf),
            Some(Fallthrough::OptionalEmptyTraces(_))
        ));

        // A fifth of the traces exactly. `PM4Py` wants more than that.
        let log = base.derive([(vec![], 1), (vec![0], 4)]);
        assert!(matches!(
            empty_traces(&log, &imf),
            Some(Fallthrough::OptionalEmptyTraces(_))
        ));
        assert!(matches!(
            empty_traces(&log, &InductiveMinerOptions::pm4py(0.2)),
            Some(Fallthrough::DroppedEmptyTraces(_))
        ));
    }

    #[test]
    fn test_order_and_flower_model() {
        // L81, where d occurs exactly once in every trace.
        let (_, log) = log_of(&[
            &["a", "b", "c", "d"],
            &["d", "a", "b"],
            &["a", "d", "c"],
            &["b", "c", "d"],
        ]);
        let dfg = ActivityDfg::discover(&log);

        let Fallthrough::ActivityConcurrent { activity_log, .. } =
            find_fallthrough(&log, &dfg, &InductiveMinerOptions::default())
        else {
            panic!("expected the activity-once-per-trace fall through");
        };
        assert_eq!(activity_log.activities(), vec![3]);

        let options = InductiveMinerOptions {
            use_fallthroughs: false,
            ..InductiveMinerOptions::default()
        };
        assert_eq!(
            find_fallthrough(&log, &dfg, &options),
            Fallthrough::FlowerModel(vec![0, 1, 2, 3])
        );
        assert_eq!(
            flower_model(&log),
            Fallthrough::FlowerModel(vec![0, 1, 2, 3])
        );
    }
}
