//! Utility for splitting a log according to a loop cut
//!
//!
//! # Implementation Notes
//! This implementation adopts the loop-splitting algorithm as implemented in
//! the ProM framework (`InductiveMiner`), originally written in Java.
//!
//! Reference:
//! - Leemans, S.J.J., Fahland, D., van der Aalst, W.M.P.:
//!   "Discovering Block-Structured Process Models from Event Logs – A Constructive Approach."
//!   Application of Concurrency to System Design (ACSD), 2013.
//! - Leemans S.J.J., "Robust process mining with guarantees", Ph.D. Thesis, Eindhoven
//!   University of Technology, 09.05.2017
//! - ProM source code:
//!   https://github.com/promworkbench/InductiveMiner/blob/main/src/org/processmining/plugins/inductiveminer2/framework/logsplitter/LogSplitterLoop.java

use std::collections::HashMap;
use crate::EventLog;
use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::process_tree::OperatorType::Loop;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
use crate::discovery::case_centric::inductive_miner_app::splits::split::Split;
/// Splits an event log according to the partition of a Loop-cut.
///
/// Recall that a loop cut identifies a structure consisting of a main body (do-part) and at least one redo part.
/// The partitions of the cut represent activity sets that belong to different segments of the loop structure.
/// The first partition belongs to the do segment.
///
/// Creates one sub log for each partition in the cut
/// Iterates over every trace, grouping activities to the same sub trace as long as they belong to the same partition.
/// If a partition changes the current sub trace is finalized and added to the sub log
///
/// # Returns
/// Some(split) containing filtered traces
/// None if the cut is not a valid loop cut
///
/// # Notes
/// - number of traces in each sublog may differ
/// - event order is preserved
/// - activities not encountered in any partition are being ignored

pub fn loop_split<'a>(log: &EventLog, classifier: &EventLogClassifier, cut: Cut<'a>) -> Option<Split> {
    if Loop != cut.get_operator() {
        return None;
    }
    // Prologue - preparations
    let k = cut.len();
    let mut result: Vec<EventLog> = Vec::with_capacity(k);

    // Create empty sublogs
    for _ in 0..k {
        result.push(log.clone_without_traces());
    }
    // get partitions
    let partitions = cut.get_own();

    // Pre-map activities to partition index for fast lookup - just transfer activity to index of set
    let mut activity_to_log_map = HashMap::new();
    for (i, part) in partitions.iter().enumerate() {
        // at least two partitions, if more loops there can be more
        for a in part {
            activity_to_log_map.insert(a.clone(), i);
        }
    }

    // iterate over each trace of the original log
    for trace in &log.traces {
        //each sublogs gets one clean trace
        let mut sub_trace = trace.clone_without_events();

        let mut last_partition: Option<usize> = None; // init too None to signal the start of a new trace

        for event in &trace.events {
            let activity = classifier.get_class_identity(event);

            // get the log index / the index of the partition the activity is part of (exactly one partition)
            let Some(log_index) = activity_to_log_map.get(activity.as_str()) else {
                eprintln!("Encountered unexpeceted activity {} in loop splitter using the following cut {:?}: on event log.", activity, partitions);
                // if the activity is not in the block, this means that it's not part of the loop - it shouldn't be in here
                continue;
            };

            if last_partition.is_some() && last_partition.unwrap() != *log_index {
                // if the last partition is not the same as in the block index of the current activity,
                // we need to create a new sub_trace and push the last one to the existing ones

                // as last_partition is some, we can just push the trace to the result log index at last partiton
                result[last_partition.unwrap()].traces.push(sub_trace);
                sub_trace = trace.clone_without_events();
            }
            // At the current state, the event belongs to the subtrace of the log_index which

            // push current activity to sub_trace of block_index sublog
            sub_trace.events.push(event.clone());
            // update the last partition
            last_partition = Some(*log_index);
        }
        // at this point we have a sub_trace which is empty or contains at least one element,
        // if the last_partition variable is set, there is at least one element in the log
        if last_partition.is_some() {
            result[last_partition.unwrap()].traces.push(sub_trace);
        } else {
            // trace is empty, nothing to do
        }
    }

    Some(Split::new(Loop, result))
}

#[cfg(test)]
mod test_loop_split {
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::core::process_models::dfg::DirectlyFollowsGraph;
    use crate::discovery::case_centric::inductive_miner_app::cut_finder::loop_cut::redo_loop_cut_wrapper;
    use crate::discovery::case_centric::inductive_miner_app::splits::redo_loop::loop_split;
    use crate::event_log;
    use crate::EventLog;

    fn events_equal(log: &EventLog, o_log: &EventLog, event_log_classifier: EventLogClassifier) -> bool {
        if log.traces.len() == o_log.traces.len() {
            for (t0, t1) in log.traces.iter().zip(o_log.traces.iter()) {
                if t0.events.len() == t1.events.len() {
                    for (e0,e1) in t0.events.iter().zip(t1.events.iter()) {
                        let a0 = event_log_classifier.get_class_identity(e0);
                        let a1 = event_log_classifier.get_class_identity(e1);
                        if a0 != a1 {
                            return false;
                        }
                    }
                }
            }
            return true;
        }
        false
    }
    #[test]
    fn test_loop_split_leemans_example() {
        let log = event_log!(
            ["a", "b"],
            ["a", "b", "c", "a", "b"],
            ["a", "b", "c", "a", "b", "c", "a", "b"]
        );

        let dfg = DirectlyFollowsGraph::discover(&log);
        let cut = redo_loop_cut_wrapper(&dfg);
        assert!(cut.is_some());
        let split = loop_split(&log, &EventLogClassifier::default(), cut.unwrap());
        assert!(split.is_some());
        let split = split.unwrap();
        assert_eq!(split.sub_logs.len(), 2);

        // created expected event logs
        let do_log = event_log!(
            ["a", "b"],
            ["a", "b"],
            ["a", "b"],
            ["a", "b"],
            ["a", "b"],
            ["a", "b"]
        );

        let redo_log = event_log!(["c"], ["c"], ["c"]);

        for log in split.get_own() {
            if log.traces.len() == 6 {
                // expected length of 6
                assert!(events_equal(&log, &do_log, EventLogClassifier::default()));
            } else if log.traces.len() == 3 {
                // expected length of 3
                assert!(events_equal(&log, &redo_log, EventLogClassifier::default()));
            } else {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_more_complex_loop() {
        let log = event_log!(
            ["a", "b"],
            ["a", "b", "c", "a", "b"],
            ["a", "d", "b"],
            ["a", "d", "b", "c", "a", "d", "b"],
            ["a", "d", "b", "c", "a", "b"]
        );

        let do_log = event_log!(
            ["a", "b"],
            ["a", "b"],
            ["a", "b"],
            ["a", "d", "b"],
            ["a", "d", "b"],
            ["a", "d", "b"],
            ["a", "d", "b"],
            ["a", "b"]
        );

        let redo_log = event_log!(["c"], ["c"], ["c"]);

        let dfg = DirectlyFollowsGraph::discover(&log);
        let cut = redo_loop_cut_wrapper(&dfg);
        assert!(cut.is_some());
        let split = loop_split(&log, &EventLogClassifier::default(), cut.unwrap());
        assert!(split.is_some());
        let split = split.unwrap();
        assert_eq!(split.sub_logs.len(), 2);

        for log in split.get_own() {
            if log.traces.len() == do_log.traces.len() {
                // expected length of 6
                assert!(events_equal(&log, &do_log, EventLogClassifier::default()));
            } else if log.traces.len() == redo_log.traces.len() {
                // expected length of 3
                assert!(events_equal(&log, &redo_log, EventLogClassifier::default()));
            } else {
                assert!(false);
            }
        }
    }
}
