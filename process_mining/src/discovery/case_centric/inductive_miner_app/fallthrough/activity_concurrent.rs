//! Activity Concurrent fallthrough detection utilities.
//!
//! This module implements the **activity concurrent** fallthrough used by the inductive miner.
//!
//! The activity concurrent fallthrough assumes concurrent behavior when a single activity in the event log
//! can occur independently of the ordering of the other activities. In such a case, the activity is
//! considered to run in parallel with the remaining behavior of the log.
//!
//! When this pattern is detected, the activity is separated from the log and modeled as executing
//! concurrently with the rest of the process.

use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::process_tree::{Node, OperatorType};
use crate::discovery::case_centric::dfg::discover_dfg_with_classifier;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::find_cut;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough::{ActivityConcurrent, Return};
use crate::discovery::case_centric::inductive_miner_app::splits::perform_split;
use crate::discovery::case_centric::inductive_miner_app::structures::parameter::Parameters;
use crate::EventLog;

/// Filters an event log by removing all events whose activity matches a pivot.
///
/// The function splits the input log into two logs:
/// - one log containing the original traces **without** the pivot activity
/// - one log containing traces consisting only of the filtered-out pivot events
///
/// The number of traces is preserved in both logs.
///
/// # Returns
/// A tuple `(filtered_out_log, filtered_log)` where:
///
/// - `filtered_out_log` contains only the removed pivot events (possibly empty traces).
/// - `filtered_log` contains the original behavior without the pivot events.
fn filter_out_activity(
    log: EventLog,
    event_log_classifier: &EventLogClassifier,
    pivot: String,
) -> (EventLog, EventLog) {
    let mut filtered_log = log.clone_without_traces(); // the logs containing the filtered activities
    let mut filtered_out_log = log.clone_without_traces(); // the log containing left behavior

    for trace in log.traces {
        // get the trace length
        let len_t = trace.events.len();

        // do the same for the traces again
        let mut new_trace = trace.clone_without_events();
        let mut other_new_trace = trace.clone_without_events();

        // need the option for initialization purpose, this option marks whether the element was actually contained in the trace
        let mut pivot_event = None; // if set the activity was actually contained in this trace

        // check on every event in this trace
        for event in trace.events {
            let other = event_log_classifier.get_class_identity(&event);
            if pivot != other {
                new_trace.events.push(event);
            } else if pivot_event.is_none() {
                // set the pivot event
                pivot_event = Some(event)
            }
        }

        // check whether the event was actually part of the trace
        if pivot_event.is_some() {
            // if so push the trace, as it is (excluding the left out events)
            let event = pivot_event.unwrap();
            // push the pivot event as often as it has been filtered out (maybe use a counter here)
            for _ in 0..(len_t - new_trace.events.len()) {
                other_new_trace.events.push(event.clone());
            }
            // push the filtered logs
            filtered_log.traces.push(new_trace);
            filtered_out_log.traces.push(other_new_trace);
        } else {
            // new trace equals the trace from before, therefore we should not push the empty lg (right?)
            filtered_log.traces.push(new_trace);


            //mind that empty traces are being pushed too
            filtered_out_log.traces.push(other_new_trace);

        }

    }

    (filtered_out_log, filtered_log)
}

/// Attempts to detect an *activity concurrent* fall-through pattern.
///
/// This fall through iteratively removes one activity at a time
/// (starting with the most frequent one) and checks whether the remaining logs yield any valid cut.
/// If removing the activity yields a valid cut, the activity is considered concurrent to the rest of the process.
///
/// The split operations is performed on a valid cut as well, for efficiency reasons.
///
/// # Returns
/// - 'ActivityConcurrent(...)' enum if a concurrent activity is detected, containing the constructed concurrency node, the log of removed activity instances and the already performed split.
/// - 'Return(log)' the original log without changes
fn activity_concurrent(
    log: EventLog,
    event_log_classifier: &EventLogClassifier,
    parameters: &Parameters) -> Fallthrough {
    let dfg = discover_dfg_with_classifier(&log, event_log_classifier);

    // get the activities and transform into a vector
    let mut activities: Vec<(String,u32)> = dfg.activities.clone().into_iter().collect();
    // sort by cardinality (descending)
    (&mut activities).sort_by(|a,b| a.1.partial_cmp(&b.1).unwrap()); // safe unwrap as working with u32 here

    // now leave out one activity after another and try to find a cut
    for (activity, _) in activities.into_iter().rev() {
        // remove activity from this log
        let (filtered_out_log, filtered_log) =
            filter_out_activity(log.clone(), event_log_classifier, activity);

        // build a dfg in order to use already established find_cut method
        let dfg = discover_dfg_with_classifier(&filtered_log, event_log_classifier);
        match find_cut(&dfg, &filtered_log, event_log_classifier, parameters) {
            None => continue, // leave out another activity (if another is left)
            Some(cut) => {
                // do the split here
                let split  = perform_split(&filtered_log, event_log_classifier, cut);

                // create a node without children, as this has to be processed in the more high level functions
                let node = Node::new_operator(OperatorType::Concurrency);

                // return if a cut is found
                return ActivityConcurrent(node, filtered_out_log, split);
            }
        }
    }

    // default return
    Return(log)
}

/// Public wrapper for [`activity_concurrent`].
///
/// This function simply forwards its arguments to
/// `activity_concurrent` and exists for consistency
/// with other fall-through detection wrappers.
pub fn activity_concurrent_wrapper(log: EventLog, 
                                   event_log_classifier: &EventLogClassifier,
                                   parameters: &Parameters) -> Fallthrough {
    activity_concurrent(log, event_log_classifier, parameters)
}

#[cfg(test)]
mod test_activity_concurrent {
    use std::collections::HashSet;
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::{event_log, EventLog};
    use crate::core::process_models::process_tree::{Node, OperatorType};
    use crate::discovery::case_centric::inductive_miner_app::fallthrough::activity_concurrent::{activity_concurrent, filter_out_activity};
    use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough::ActivityConcurrent;

    fn events_equal(log: &EventLog, o_log: &EventLog, event_log_classifier: &EventLogClassifier) -> bool {
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
    fn test_filter_out_activity_and_activity_concurrent_yield_same_result() {
        let log = event_log!(
            ["a", "b", "c", "d"],
            ["d", "a", "b"],
            ["a", "d", "c"],
            ["b", "c", "d"],
        );

        // mind the empty trace
        let ex1 = event_log!(["b"], ["b"],[], ["b"]);

        let ex2 = event_log!(
            ["a", "c", "d"],
            ["d","a"],
            ["a", "d", "c"],
            ["c", "d"],
        );

        let classifier = EventLogClassifier::default();

        let (log1, log2) =
            filter_out_activity(log.clone(), &EventLogClassifier::default(), "b".to_string());
        
        assert!(events_equal(&log1, &ex1, &classifier));
        assert!(events_equal(&log2, &ex2, &classifier));
        let ActivityConcurrent(node, log1, split)= activity_concurrent(log, &classifier, &HashSet::new()) else { return assert!(false); };
        assert!(!log1.traces.is_empty() && !split.sub_logs.is_empty());
        let ex_node = Node::new_operator(OperatorType::Concurrency);
        assert_eq!(node, ex_node);

    }



}
