//! The minimum-self-distance relation of an event log.
//!
//! The self distance of an activity `a` is the number of events between two consecutive
//! occurrences of `a`, and the minimum over the log is its minimum self distance. The activities
//! occurring between two occurrences at exactly that distance are in the minimum-self-distance
//! relation with `a`.
//!
//! Concurrency detection uses this to tell a loop from concurrency (∧↔.1), which the
//! directly-follows graph alone cannot do. In the log `[⟨a, b, a⟩, ⟨b, a, b⟩]` both `a ↦ b` and
//! `b ↦ a` hold, so `a` and `b` look concurrent, but `b` always sits between two `a`s.

use std::collections::BTreeSet;

use super::super::log::{ActivityID, ActivityLog};

/// The minimum-self-distance relation of a log.
///
/// See the [module documentation](self).
#[derive(Debug, Clone)]
pub struct MinimumSelfDistance {
    /// Per activity, its minimum self distance and what occurs between two of its occurrences
    /// that far apart, or `None` if it never occurs twice in a trace.
    closest: Vec<Option<(usize, BTreeSet<ActivityID>)>>,
}

impl MinimumSelfDistance {
    /// Computes the minimum-self-distance relation of the given log.
    pub fn discover(log: &ActivityLog) -> Self {
        let n = log.alphabet_size();
        let mut closest: Vec<Option<(usize, BTreeSet<ActivityID>)>> = vec![None; n];

        let mut last_seen: Vec<Option<usize>> = vec![None; n];
        for variant in log.variants() {
            let trace = &variant.activities;

            for (position, &activity) in trace.iter().enumerate() {
                if let Some(previous) = last_seen[activity] {
                    let distance = position - previous - 1;
                    let between = &trace[previous + 1..position];
                    match &mut closest[activity] {
                        Some((known, set)) if distance == *known => set.extend(between),
                        Some((known, _)) if distance > *known => {}
                        entry => *entry = Some((distance, between.iter().copied().collect())),
                    }
                }
                last_seen[activity] = Some(position);
            }

            // Positions are trace-local, so forget them before moving on.
            for &activity in trace {
                last_seen[activity] = None;
            }
        }

        Self { closest }
    }

    /// The minimum self distance of an activity, or `None` if it never repeats within a trace.
    #[cfg(test)]
    pub fn minimum_distance(&self, activity: ActivityID) -> Option<usize> {
        self.closest[activity]
            .as_ref()
            .map(|(distance, _)| *distance)
    }

    /// Returns `true` if `between` occurs between two closest occurrences of `activity`.
    pub fn intervenes(&self, activity: ActivityID, between: ActivityID) -> bool {
        self.closest[activity]
            .as_ref()
            .is_some_and(|(_, set)| set.contains(&between))
    }

    /// Returns `true` if either activity occurs between two closest occurrences of the other,
    /// the symmetric relation concurrency detection uses.
    pub fn are_related(&self, a: ActivityID, b: ActivityID) -> bool {
        self.intervenes(a, b) || self.intervenes(b, a)
    }
}

#[cfg(test)]
mod test_minimum_self_distance {
    use super::super::super::log::test_utils::log_of;
    use super::*;

    /// Computes the relation and looks activities up by name.
    fn relation_of(traces: &[&[&str]]) -> impl Fn(&str) -> Option<(usize, Vec<String>)> {
        let (labels, log) = log_of(traces);
        let msd = MinimumSelfDistance::discover(&log);
        move |name: &str| {
            let activity = labels.iter().position(|l| l == name).unwrap();
            msd.minimum_distance(activity).map(|distance| {
                (
                    distance,
                    (0..labels.len())
                        .filter(|&b| msd.intervenes(activity, b))
                        .map(|b| labels[b].clone())
                        .collect(),
                )
            })
        }
    }

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn test_equally_close_pairs_are_merged() {
        let msd = relation_of(&[&["a", "c", "a"], &["a", "b", "a"]]);
        assert_eq!(msd("a"), Some((1, names(&["b", "c"]))));
    }

    #[test]
    fn test_closer_pair_in_a_later_trace_replaces_the_intervening_set() {
        let msd = relation_of(&[&["a", "b", "a"], &["a", "a"]]);
        assert_eq!(msd("a"), Some((0, vec![])));
    }

    #[test]
    fn test_complex_trace() {
        let msd = relation_of(&[&[
            "a", "b", "d", "e", "a", "d", "g", "g", "d", "b", "f", "a", "c",
        ]]);

        assert_eq!(msd("a"), Some((3, names(&["b", "d", "e"]))));
        assert_eq!(msd("b"), Some((7, names(&["a", "d", "e", "g"]))));
        assert_eq!(msd("c"), None);
        // Two pairs of d's are two apart, so both intervening sets are merged.
        assert_eq!(msd("d"), Some((2, names(&["a", "e", "g"]))));
        assert_eq!(msd("e"), None);
        assert_eq!(msd("f"), None);
        assert_eq!(msd("g"), Some((0, vec![])));
    }

    #[test]
    fn test_positions_do_not_leak_between_traces() {
        // Without resetting, the a of the second trace would be measured against the first one.
        let (labels, log) = log_of(&[&["a", "b"], &["b", "a"]]);
        let msd = MinimumSelfDistance::discover(&log);
        let a = labels.iter().position(|l| l == "a").unwrap();
        assert_eq!(msd.minimum_distance(a), None);
    }

    #[test]
    fn test_symmetric_relation() {
        let (labels, log) = log_of(&[&["a", "b", "a"]]);
        let msd = MinimumSelfDistance::discover(&log);
        let (a, b) = (
            labels.iter().position(|l| l == "a").unwrap(),
            labels.iter().position(|l| l == "b").unwrap(),
        );

        assert!(msd.intervenes(a, b));
        assert!(!msd.intervenes(b, a));
        assert!(msd.are_related(a, b));
        assert!(msd.are_related(b, a));
    }

    #[test]
    fn test_empty_log() {
        let (_, log) = log_of(&[]);
        assert!(MinimumSelfDistance::discover(&log).closest.is_empty());
    }
}
