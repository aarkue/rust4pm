//! Base cases: the logs the miner can describe without recursing.

use super::log::{ActivityID, ActivityLog};

/// A log the miner can describe directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaseCase {
    /// The log contains no traces, so the model is `τ`.
    EmptyLog,
    /// The log consists of single executions of this activity, so the model is its leaf.
    SingleActivity(ActivityID),
}

/// Checks whether a base case applies to the given log. Expects a log without empty traces, which
/// [`empty_traces`](super::fallthrough::empty_traces) handles first.
///
/// For a log over a single activity, `IMf` reads the trace lengths as a geometric distribution
/// with `p = |L| / (‖L‖ + |L|)` and returns the leaf if `|p - 0.5| ≤ noise_threshold` (§6.2.2.3).
/// At a threshold of `0.0`, as in plain IM, every trace must hold exactly one event.
pub fn find_base_case(log: &ActivityLog, noise_threshold: f64) -> Option<BaseCase> {
    if log.is_empty() {
        return Some(BaseCase::EmptyLog);
    }

    let activities = log.activities();
    let [activity] = activities[..] else {
        return None;
    };

    let traces = log.num_traces() as f64;
    let events = log.num_events() as f64;
    let p = traces / (events + traces);

    ((p - 0.5).abs() <= noise_threshold).then_some(BaseCase::SingleActivity(activity))
}

#[cfg(test)]
mod test_base_cases {
    use super::super::log::test_utils::log_of;
    use super::*;

    #[test]
    fn test_base_cases() {
        let (_, empty) = log_of(&[]);
        assert_eq!(find_base_case(&empty, 0.0), Some(BaseCase::EmptyLog));

        let (_, log) = log_of(&[&["a"], &["a"], &["a"]]);
        assert_eq!(find_base_case(&log, 0.0), Some(BaseCase::SingleActivity(0)));

        // Several activities, or one activity that repeats, is not a base case for plain IM.
        let (_, log) = log_of(&[&["a"], &["b"]]);
        assert_eq!(find_base_case(&log, 0.0), None);
        let (_, log) = log_of(&[&["a"], &["a", "a"]]);
        assert_eq!(find_base_case(&log, 0.0), None);
    }

    #[test]
    fn test_imf_tolerates_a_few_repetitions() {
        // 100 traces with a single a and one with two: p is about 0.4975, close enough to 0.5.
        let (_, base) = log_of(&[&["a"]]);
        let log = base.derive([(vec![0], 100), (vec![0, 0], 1)]);
        assert_eq!(find_base_case(&log, 0.0), None);
        assert_eq!(find_base_case(&log, 0.2), Some(BaseCase::SingleActivity(0)));

        // Ten a's in every trace gives p of about 0.09, which is a loop and not a leaf.
        let log = base.derive([(vec![0; 10], 100)]);
        assert_eq!(find_base_case(&log, 0.2), None);
    }
}
