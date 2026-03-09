use crate::{event_log, trace, EventLog};
use std::collections::{HashMap, HashSet};
use crate::core::event_data::case_centric::{EventLogClassifier, Trace};
use crate::core::process_models::dfg::Activity;

type Index = usize;
type MinDist = usize;
type InterveningSet = HashSet<String>;

/// Stores for every activity, its minimum self-distance and the set of activities occurring
/// between two minium-distance instances of that activity.
pub struct MinimumSelfDistance {
    minimum_distance_relation: HashMap<String, (MinDist, HashSet<String>)>,
}

impl MinimumSelfDistance {

    /// Constructs the new minimum self-distance relation from a given log and classifier.
    pub fn new(
        log: &EventLog, event_log_classifier: &EventLogClassifier) -> MinimumSelfDistance {
        Self{minimum_distance_relation: Self::minimum_distances_interleave(log, event_log_classifier)}
    }


    /// Returns the minimum self-distance for a given activity and the set of activities occurring
    /// between two minimum-distance instances of that activity.
    pub fn get_minimum_distance(&self, activity: &str) -> Option<&(MinDist, HashSet<String>)> {
        self.minimum_distance_relation.get(activity)
    }

    /// Computes minimum self-distances for all activities within a single trace.
    ///
    /// For each activity, the minimum number of events between two consecutive
    /// executions is determined, together with the set of intervening activities
    /// observed at that minimum distance.

    fn extract_interleaving_activities(
        start: Index,
        end: Index,
        trace: &Trace,
        event_log_classifier: &EventLogClassifier,
    ) -> HashSet<String> {
        let mut interleaving_activities = HashSet::new();
        for i in start + 1..end {
            if let Some(event) = trace.events.get(i) {
                interleaving_activities.insert(event_log_classifier.get_class_identity(event));
            }
        }

        interleaving_activities
    }

    /// Two activities 'a' and 'b' are in a minimum distance relation iff 'b' appears between two
    /// minimum distance executions of a.
    /// This function evaluates the minimum distance between two executions of an activity and
    /// count the appearing activities.
    ///
    ///
    /// This function calculates the minimum distance relation of every activity.
    fn minimum_distances_trace(
        trace: &Trace,
        event_log_classifier: &EventLogClassifier,
    ) -> HashMap<Activity, (MinDist, InterveningSet)> {
        let mut last_seen: HashMap<Activity, Index> = HashMap::new();
        let mut results: HashMap<Activity, (MinDist, InterveningSet)> = HashMap::new();
        for (index, event) in trace.events.iter().enumerate() {
            let activity = event_log_classifier.get_class_identity(event);
            if let Some(last_index) = last_seen.get(&activity) {
                // calculate distance between the two indexes
                let dist = index - *last_index - 1;
                if let Some((prev_dist, acts)) = results.get_mut(&activity) {
                    if *prev_dist > dist {
                        // previous distance is smaller than the current, so it can't be minimum
                        *prev_dist = dist;
                        *acts = Self::extract_interleaving_activities(
                            *last_index,
                            index,
                            trace,
                            event_log_classifier,
                        );
                    } else if *prev_dist == dist {
                        acts.extend(Self::extract_interleaving_activities(
                            *last_index,
                            index,
                            trace,
                            event_log_classifier,
                        ));
                    }
                    // skip, the distance is greater than the one we got previously
                } else {
                    // the first time we found a loop
                    results.insert(
                        activity.clone(), // clone as we need to update activity later
                        (
                            dist,
                            Self::extract_interleaving_activities(
                                *last_index,
                                index,
                                trace,
                                event_log_classifier,
                            ),
                        ),
                    );
                }


            }
            // update the last seen index of this activity
            last_seen.insert(activity, index);

        }
        results
    }

    /// Aggregates minimum self-distance information over all traces in the log.
    ///
    /// For each activity, the globally smallest self-distance is retained and
    /// the intervening activity sets for equal minimum distances are merged.
    fn minimum_distances_interleave(log: &EventLog, event_log_classifier: &EventLogClassifier) -> HashMap<Activity, (MinDist, InterveningSet)> {
        let mut results: HashMap<Activity, (MinDist, InterveningSet)> = HashMap::new();

        // Go through every trace
        for trace in log.traces.iter(){
            for (activity, (dist, interleaving_acts)) in Self::minimum_distances_trace(trace, event_log_classifier) {
                if let Some(( min_dist,  interleaving_set)) = results.get_mut(&activity) {
                    if *min_dist > dist{
                        *min_dist = dist;
                        *interleaving_set = interleaving_acts;
                    } else if *min_dist == dist {
                        interleaving_set.extend(interleaving_acts);
                    } else {
                        // skip if the new distance is greater tan the already saved distance
                    }
                } else {
                    results.insert(activity, (dist, interleaving_acts));
                }
            }
        }
        results
    }
}


#[test]
fn test_extract_interleaving_activities() {
    let t = trace!("a", "b", "c", "d", "e", "f");
    let s = MinimumSelfDistance::extract_interleaving_activities(0, 6, &t, &EventLogClassifier::default());
    assert_eq!(s, HashSet::from(["b".into(), "c".into(), "d".into(), "e".into(), "f".into()]));
}
#[test]
fn test_extract_from_empty_trace() {
    let t = trace!();
    let s = MinimumSelfDistance::extract_interleaving_activities(0, 6, &t, &EventLogClassifier::default());
    assert!(s.is_empty());}

// ------------ Tests using binary events
#[test]
fn test_one_loop_distance() {
    let t = trace!("a", "b", "a");

    let r = MinimumSelfDistance::minimum_distances_trace(&t, &EventLogClassifier::default());
    assert!(r.contains_key("a"));
    assert_eq!(r.get("a").unwrap().0, 1);
    assert!(r.get("a").unwrap().1.contains("b"));
}

#[test]
fn test_loop_zero_distance(){
    let t = trace!("a","a");

    let r = MinimumSelfDistance::minimum_distances_trace(&t, &EventLogClassifier::default());
    assert!(r.contains_key("a"));
    assert_eq!(r.get("a").unwrap().0, 0);
    assert!(r.get("a").unwrap().1.is_empty());
}

#[test]
fn test_retrieve_smaller_later_loop(){
    let t = trace!("a", "b", "b", "a", "b", "b", "b", "a", "b", "a");

    let r = MinimumSelfDistance::minimum_distances_trace(&t, &EventLogClassifier::default());
    assert!(r.contains_key("a"));
    assert_eq!(r.get("a").unwrap().0, 1);
    assert!(r.get("a").unwrap().1.contains("b"));

    // trivial, b should have 0 minimum self distance in this example
    assert!(r.contains_key("b"));
    assert_eq!(r.get("b").unwrap().0, 0);
    assert!(r.get("b").unwrap().1.is_empty());
}


// -------------------------------- Test using more than two different activities

#[test]
fn test_complex_trace(){
    let t = trace!("a", "b", "d", "e", "a", "d", "g", "g", "d","b", "f", "a", "c");
    let r = MinimumSelfDistance::minimum_distances_trace(&t, &EventLogClassifier::default());

    // check if loops are contained
    assert!(r.contains_key("a"));
    assert_eq!(r.get("a").unwrap().0, 3);
    assert_eq!(r.get("a").unwrap().1, HashSet::from(["b".into(), "d".into(), "e".into()]));


    assert!(r.contains_key("b"));
    assert_eq!(r.get("b").unwrap().0, 7);
    assert_eq!(r.get("b").unwrap().1, HashSet::from(["a".into(), "e".into(), "d".into(), "g".into()]));

    assert!(!r.contains_key("c"));

    // special case, because there are two loops with same minimum distance two
    assert!(r.contains_key("d"));
    assert_eq!(r.get("d").unwrap().0, 2);
    // merged activities
    assert_eq!(r.get("d").unwrap().1, HashSet::from(["e".into(), "a".into(), "g".into()]));


    // not appearing twice
    assert!(!r.contains_key("e"));
    assert!(!r.contains_key("f"));

    // only one trace where g follows after g
    assert!(r.contains_key("g"));
    assert_eq!(r.get("g").unwrap().0, 0);
    assert!(r.get("g").unwrap().1.is_empty());
}


#[test]
fn test_empty_log(){
    let log = event_log!();
    let r = MinimumSelfDistance::minimum_distances_interleave(&log, &EventLogClassifier::default());

    assert!(r.is_empty());
}

#[test]
fn test_zero_loops_log(){
    let log = event_log!(["a", "a"], ["b", "b"]);
    let r = MinimumSelfDistance::minimum_distances_interleave(&log, &EventLogClassifier::default());

    assert!(r.contains_key("a"));
    assert_eq!(r.get("a").unwrap().0, 0);

    assert!(r.contains_key("b"));
    assert_eq!(r.get("b").unwrap().0, 0);
}

#[test]
fn test_find_smaller_loop(){
    let log = event_log!(["a", "a"], ["a", "b", "a"]);
    let r = MinimumSelfDistance::minimum_distances_interleave(&log, &EventLogClassifier::default());

    assert!(r.contains_key("a"));
    assert_eq!(r.get("a").unwrap().0, 0);

    assert!(!r.contains_key("b"));
}

#[test]
fn test_merge_relations(){
    let log = event_log!(["a", "c", "a"], ["a", "b", "a"]);
    let r = MinimumSelfDistance::minimum_distances_interleave(&log, &EventLogClassifier::default());

    assert!(r.contains_key("a"));
    assert_eq!(r.get("a").unwrap().0, 1);
    assert_eq!(r.get("a").unwrap().1, HashSet::from(["b".into(), "c".into()]));
}



