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

use super::super::log::{ActivityID, ActivityLog};

/// The minimum-self-distance relation of a log.
///
/// See the [module documentation](self).
#[derive(Debug, Clone)]
pub struct MinimumSelfDistance {
    /// Minimum self distance per activity, or `None` if it never occurs twice in a trace.
    minimum_distance: Vec<Option<usize>>,
    /// One bit row per activity: bit `b` of row `a` is set iff `b` occurs between two occurrences
    /// of `a` that are exactly `minimum_distance[a]` apart.
    ///
    /// Bits rather than bytes because this is rebuilt for every candidate of the
    /// activity-concurrent fall through.
    intervenes: Vec<u64>,
    words_per_row: usize,
}

/// Number of activities covered by one word of the bit matrix.
const BITS: usize = u64::BITS as usize;

impl MinimumSelfDistance {
    /// Computes the minimum-self-distance relation of the given log.
    pub fn discover(log: &ActivityLog) -> Self {
        let n = log.alphabet_size();
        let words_per_row = n.div_ceil(BITS);
        let mut relation = Self {
            minimum_distance: vec![None; n],
            intervenes: vec![0; n * words_per_row],
            words_per_row,
        };

        let mut last_seen: Vec<Option<usize>> = vec![None; n];
        for variant in log.variants() {
            let trace = &variant.activities;

            for (position, &activity) in trace.iter().enumerate() {
                if let Some(previous) = last_seen[activity] {
                    let distance = position - previous - 1;
                    let row = activity * words_per_row;
                    if relation.minimum_distance[activity].is_none_or(|known| distance < known) {
                        relation.minimum_distance[activity] = Some(distance);
                        relation.intervenes[row..row + words_per_row].fill(0);
                    }
                    if relation.minimum_distance[activity] == Some(distance) {
                        for &between in &trace[previous + 1..position] {
                            relation.intervenes[row + between / BITS] |= 1 << (between % BITS);
                        }
                    }
                }
                last_seen[activity] = Some(position);
            }

            // Positions are trace-local, so forget them before moving on.
            for &activity in trace {
                last_seen[activity] = None;
            }
        }

        relation
    }

    /// The minimum self distance of an activity, or `None` if it never repeats within a trace.
    pub fn minimum_distance(&self, activity: ActivityID) -> Option<usize> {
        self.minimum_distance[activity]
    }

    /// Returns `true` if `between` occurs between two closest occurrences of `activity`.
    pub fn intervenes(&self, activity: ActivityID, between: ActivityID) -> bool {
        self.intervenes[activity * self.words_per_row + between / BITS] & (1 << (between % BITS))
            != 0
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
        assert!(MinimumSelfDistance::discover(&log)
            .minimum_distance
            .is_empty());
    }
}
