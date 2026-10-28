//! The activity-projected event log the Inductive Miner recurses on.
//!
//! The miner only looks at the activity sequence of a trace, so [`ActivityLog`] stores trace
//! variants over `usize` activity ids with their counts.

use rayon::prelude::*;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use crate::core::event_data::case_centric::utils::activity_projection::EventLogActivityProjection;
use crate::core::event_data::case_centric::{
    AttributeValue, Event, EventLogClassifier, XESEditableAttribute,
};
use crate::EventLog;

/// An activity, represented by its index in the activity alphabet of a mining run.
pub type ActivityID = usize;

/// One distinct activity sequence of a log, together with the number of traces having it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceVariant {
    /// The activity sequence.
    pub activities: Vec<ActivityID>,
    /// How many traces of the log have exactly this activity sequence. Always `> 0`.
    pub count: u64,
}

/// An event log projected onto activity ids, stored as trace variants with multiplicities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityLog {
    /// Size of the activity alphabet of the whole mining run. Sub-logs keep the alphabet of the
    /// original log, so an activity id means the same thing in every recursion step.
    alphabet_size: usize,
    /// Sorted by activity sequence, with no sequence occurring twice.
    variants: Vec<TraceVariant>,
}

impl ActivityLog {
    /// Creates a log over an alphabet of `alphabet_size` activities from the given traces.
    ///
    /// Traces with an identical activity sequence are aggregated into a single variant, and traces
    /// with a count of zero are dropped. Variants end up sorted, which makes a log independent of
    /// the order its traces arrived in and so keeps discovery reproducible.
    pub fn new(
        alphabet_size: usize,
        traces: impl IntoIterator<Item = (Vec<ActivityID>, u64)>,
    ) -> Self {
        let mut variants: Vec<TraceVariant> = traces
            .into_iter()
            .filter(|(_, count)| *count > 0)
            .map(|(activities, count)| TraceVariant { activities, count })
            .collect();

        variants.sort_unstable_by(|a, b| a.activities.cmp(&b.activities));
        variants.dedup_by(|duplicate, kept| {
            if duplicate.activities == kept.activities {
                kept.count += duplicate.count;
                true
            } else {
                false
            }
        });

        Self {
            alphabet_size,
            variants,
        }
    }

    /// Creates a sub-log over the same alphabet as this one from the given traces, aggregated as
    /// in [`ActivityLog::new`].
    pub fn derive(&self, traces: impl IntoIterator<Item = (Vec<ActivityID>, u64)>) -> Self {
        Self::new(self.alphabet_size, traces)
    }

    /// Size of the activity alphabet of the mining run. An upper bound for the ids in this log,
    /// which usually contains only a subset (see [`ActivityLog::activities`]).
    pub fn alphabet_size(&self) -> usize {
        self.alphabet_size
    }

    /// The distinct trace variants of this log.
    pub fn variants(&self) -> &[TraceVariant] {
        &self.variants
    }

    /// Returns `true` if the log contains no traces at all. A log of only empty traces is not
    /// empty in this sense.
    pub fn is_empty(&self) -> bool {
        self.variants.is_empty()
    }

    /// The number of traces in the log, counting duplicates.
    pub fn num_traces(&self) -> u64 {
        self.variants.iter().map(|v| v.count).sum()
    }

    /// The number of events in the log, counting duplicates.
    pub fn num_events(&self) -> u64 {
        self.variants
            .iter()
            .map(|v| v.count * v.activities.len() as u64)
            .sum()
    }

    /// The number of empty traces in the log (`|ε ∈ L|`).
    pub fn num_empty_traces(&self) -> u64 {
        self.variants
            .iter()
            .filter(|v| v.activities.is_empty())
            .map(|v| v.count)
            .sum()
    }

    /// Returns `true` if the log contains at least one empty trace. Base cases and cut detection
    /// are only applied to logs without them.
    pub fn contains_empty_trace(&self) -> bool {
        self.variants.iter().any(|v| v.activities.is_empty())
    }

    /// The activities occurring in this log, in ascending id order.
    pub fn activities(&self) -> Vec<ActivityID> {
        let mut seen = vec![false; self.alphabet_size];
        for variant in &self.variants {
            for &activity in &variant.activities {
                seen[activity] = true;
            }
        }
        (0..self.alphabet_size).filter(|&a| seen[a]).collect()
    }

    /// Returns the projection of this log onto a single activity. Traces not containing it become
    /// empty traces.
    pub fn projected_onto(&self, activity: ActivityID) -> Self {
        self.retaining(|a| a == activity)
    }

    /// Returns this log with all occurrences of `activity` removed. The number of traces is
    /// preserved, so traces consisting only of `activity` become empty.
    pub fn without_activity(&self, activity: ActivityID) -> Self {
        self.retaining(|a| a != activity)
    }

    /// This log with only the events `keep` accepts, keeping every trace.
    fn retaining(&self, keep: impl Fn(ActivityID) -> bool) -> Self {
        self.derive(self.variants.iter().map(|variant| {
            (
                variant
                    .activities
                    .iter()
                    .copied()
                    .filter(|&a| keep(a))
                    .collect(),
                variant.count,
            )
        }))
    }

    /// This log with all empty traces removed.
    pub fn without_empty_traces(&self) -> Self {
        Self {
            alphabet_size: self.alphabet_size,
            variants: self
                .variants
                .iter()
                .filter(|v| !v.activities.is_empty())
                .cloned()
                .collect(),
        }
    }
}

/// The activity an event belongs to, borrowed from the event if the classifier has a single
/// attribute.
fn class_identity<'a>(classifier: &EventLogClassifier, event: &'a Event) -> Cow<'a, str> {
    match classifier.keys.as_slice() {
        [key] => match event.attributes.get_by_key(key).map(|a| &a.value) {
            Some(AttributeValue::String(value)) => Cow::Borrowed(value.as_str()),
            _ => Cow::Borrowed(""),
        },
        _ => Cow::Owned(classifier.get_class_identity(event)),
    }
}

/// Projects an [`EventLog`] onto activity ids using the given classifier.
///
/// Returns the activity labels (an activity id is its index in that vector) and the projected log.
/// Labels are sorted, so the ids, and with them the order in which cut detection reports its
/// partitions, depend only on the activity names.
pub fn project_event_log(
    log: &EventLog,
    classifier: &EventLogClassifier,
) -> (Vec<String>, ActivityLog) {
    // Pass one: the alphabet.
    let mut labels: Vec<String> = log
        .traces
        .par_iter()
        .fold(HashSet::new, |mut seen: HashSet<Cow<'_, str>>, trace| {
            seen.extend(trace.events.iter().map(|e| class_identity(classifier, e)));
            seen
        })
        .reduce(HashSet::new, |mut left, right| {
            left.extend(right);
            left
        })
        .into_iter()
        .map(Cow::into_owned)
        .collect();
    labels.sort_unstable();

    let id_of: HashMap<&str, ActivityID> = labels
        .iter()
        .enumerate()
        .map(|(i, label)| (label.as_str(), i))
        .collect();

    // Pass two: the traces, aggregated into variants while reading.
    let variants: HashMap<Vec<ActivityID>, u64> = log
        .traces
        .par_iter()
        .fold(HashMap::new, |mut variants: HashMap<_, u64>, trace| {
            let projected: Vec<ActivityID> = trace
                .events
                .iter()
                .map(|event| id_of[class_identity(classifier, event).as_ref()])
                .collect();
            *variants.entry(projected).or_default() += 1;
            variants
        })
        .reduce(HashMap::new, |mut left, right| {
            for (variant, count) in right {
                *left.entry(variant).or_default() += count;
            }
            left
        });

    let activity_log = ActivityLog::new(labels.len(), variants);
    (labels, activity_log)
}

/// Converts an [`EventLogActivityProjection`] into the miner's log representation. The labels are
/// re-sorted, so the activity ids generally differ from those of the input projection.
pub fn from_activity_projection(
    projection: &EventLogActivityProjection,
) -> (Vec<String>, ActivityLog) {
    let mut labels = projection.activities.clone();
    labels.sort_unstable();

    let id_of: HashMap<&str, ActivityID> = labels
        .iter()
        .enumerate()
        .map(|(i, label)| (label.as_str(), i))
        .collect();
    let remap: Vec<ActivityID> = projection
        .activities
        .iter()
        .map(|label| id_of[label.as_str()])
        .collect();

    let activity_log = ActivityLog::new(
        labels.len(),
        projection
            .traces
            .iter()
            .map(|(trace, count)| (trace.iter().map(|&a| remap[a]).collect(), *count)),
    );

    (labels, activity_log)
}

#[cfg(test)]
pub(crate) mod test_utils {
    use super::{ActivityID, ActivityLog};
    use std::collections::HashMap;

    /// Builds an [`ActivityLog`] and its labels from traces given as activity names. The alphabet
    /// is the sorted set of names occurring in `traces`.
    pub fn log_of(traces: &[&[&str]]) -> (Vec<String>, ActivityLog) {
        let mut labels: Vec<String> = traces
            .iter()
            .flat_map(|t| t.iter().map(|a| a.to_string()))
            .collect();
        labels.sort_unstable();
        labels.dedup();

        let id_of: HashMap<&str, ActivityID> = labels
            .iter()
            .enumerate()
            .map(|(i, l)| (l.as_str(), i))
            .collect();

        let log = ActivityLog::new(
            labels.len(),
            traces
                .iter()
                .map(|t| (t.iter().map(|a| id_of[*a]).collect(), 1)),
        );
        (labels, log)
    }

    /// Renders a log as a sorted multiset of activity-name sequences, for readable assertions.
    pub fn describe(log: &ActivityLog, labels: &[String]) -> Vec<(Vec<String>, u64)> {
        let mut described: Vec<(Vec<String>, u64)> = log
            .variants()
            .iter()
            .map(|v| {
                (
                    v.activities.iter().map(|&a| labels[a].clone()).collect(),
                    v.count,
                )
            })
            .collect();
        described.sort();
        described
    }

    /// Shorthand for building the expected value of [`describe`].
    pub fn expect(variants: &[(&[&str], u64)]) -> Vec<(Vec<String>, u64)> {
        let mut expected: Vec<(Vec<String>, u64)> = variants
            .iter()
            .map(|(t, c)| (t.iter().map(|a| a.to_string()).collect(), *c))
            .collect();
        expected.sort();
        expected
    }
}

#[cfg(test)]
mod test_activity_log {
    use super::test_utils::{describe, expect, log_of};
    use super::*;
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::event_log;

    #[test]
    fn test_duplicate_traces_are_aggregated() {
        let (labels, log) = log_of(&[&["a", "b"], &["c"], &["a", "b"]]);

        assert_eq!(labels, vec!["a", "b", "c"]);
        assert_eq!(log.variants().len(), 2);
        assert_eq!(log.num_traces(), 3);
        assert_eq!(log.num_events(), 5);
        assert_eq!(
            describe(&log, &labels),
            expect(&[(&["a", "b"], 2), (&["c"], 1)])
        );
    }

    #[test]
    fn test_ids_are_assigned_in_label_order() {
        // "c" occurs first in the log but must still get the highest id, and variants come out
        // sorted, so the trace ⟨b⟩ = [1] precedes ⟨c, a⟩ = [2, 0].
        let (labels, log) = log_of(&[&["c", "a"], &["b"]]);

        assert_eq!(labels, vec!["a", "b", "c"]);
        assert_eq!(log.activities(), vec![0, 1, 2]);
        assert_eq!(log.variants()[0].activities, vec![1]);
        assert_eq!(log.variants()[1].activities, vec![2, 0]);
    }

    #[test]
    fn test_empty_traces_and_empty_logs() {
        let (_, log) = log_of(&[&[], &["a"], &[]]);
        assert!(!log.is_empty());
        assert!(log.contains_empty_trace());
        assert_eq!(log.num_empty_traces(), 2);
        assert_eq!(log.without_empty_traces().num_traces(), 1);

        let (_, log) = log_of(&[]);
        assert!(log.is_empty());
        assert!(!log.contains_empty_trace());
        assert!(log.activities().is_empty());
    }

    #[test]
    fn test_sub_logs_keep_the_alphabet() {
        let (_, log) = log_of(&[&["a"], &["b"], &["c"]]);
        let sub = log.derive([(vec![0], 1), (vec![2], 3)]);

        assert_eq!(sub.alphabet_size(), 3);
        assert_eq!(sub.activities(), vec![0, 2]);
        assert_eq!(sub.num_traces(), 4);
    }

    #[test]
    fn test_projected_onto_and_without_activity() {
        let (labels, log) = log_of(&[&["a", "b", "a"], &["b"]]);

        assert_eq!(
            describe(&log.projected_onto(0), &labels),
            expect(&[(&["a", "a"], 1), (&[], 1)])
        );
        assert_eq!(
            describe(&log.without_activity(0), &labels),
            expect(&[(&["b"], 2)])
        );
    }

    #[test]
    fn test_projecting_an_event_log() {
        let log = event_log!(["a", "b"], ["a", "b"], ["c"]);
        let (labels, projected) = project_event_log(&log, &EventLogClassifier::default());
        assert_eq!(labels, vec!["a", "b", "c"]);
        assert_eq!(
            describe(&projected, &labels),
            expect(&[(&["a", "b"], 2), (&["c"], 1)])
        );

        // The alphabet of a projection is re-sorted, so its ids generally change.
        let log = event_log!(["c", "a"], ["b"]);
        let projection: EventLogActivityProjection = (&log).into();
        let (labels, projected) = from_activity_projection(&projection);
        assert_eq!(labels, vec!["a", "b", "c"]);
        assert_eq!(
            describe(&projected, &labels),
            expect(&[(&["b"], 1), (&["c", "a"], 1)])
        );
    }
}
