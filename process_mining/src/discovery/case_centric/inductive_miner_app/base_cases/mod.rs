//! This module contains utilities for detecting the base cases 'Empty' and 'Single Activity' used in the Inductive Miner.
use crate::core::event_data::case_centric::EventLogClassifier;
use crate::EventLog;


/// Enum Representing whether and if so which type of base case was found.
#[derive(Debug)]
pub enum BaseCases {
    None, // No base case is found
    Empty, // the event log is completely empty
    SingleActivity(String) // just one activity in every single trace in the event log
}

/// Checks whether the base case single activity applies to the given event log.
/// The BaseCase applies if the event log only contains traces with precisely one event,
/// which must have the same activity attribute.
fn check_single_activity_case(log: &EventLog, classifier: &EventLogClassifier) -> Option<String> {
    let mut activity: Option<String> = None;
    for t in &log.traces{
        if t.events.len() != 1{ // catch empty traces
            return None;
        }
        let act = classifier.get_class_identity(&t.events[0]);
        if let Some(activity) = &activity{
            if act != *activity{
                return None;
            }
        } else {
            activity = Some(act);
        }
    }
    activity
}

/// Checks whether a BaseCase applies to a given event log.
/// 
/// There are two possible base cases:
/// - 'empty trace' where the entire event log consists of one single empty trace,
/// - 'single activity' where the entire event log consist of traces containing only one single event with the same activity attribute. 
pub fn find_base_case(log: &EventLog, event_log_classifier: &EventLogClassifier) -> BaseCases {

    if log.traces.len() == 0{
        // this just checks for an empty event log, this means, even if there are only empty traces, this case case does not apply
         BaseCases::Empty
    } else if let Some(activity) = check_single_activity_case(log, event_log_classifier){
         BaseCases::SingleActivity(activity)
    } else {
        // no base case applied to this one
        BaseCases::None
    }
}