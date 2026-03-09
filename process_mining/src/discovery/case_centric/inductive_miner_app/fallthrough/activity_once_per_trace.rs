
use std::collections::HashMap;
use crate::core::event_data::case_centric::EventLogClassifier;
use crate::{event_log, EventLog};
use crate::core::process_models::process_tree::{Node, OperatorType};
use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough::{ActivityOncePerTrace, Return};
use crate::discovery::case_centric::inductive_miner_app::structures::parameter::Parameters;

/// This function iterates over every event of every trace and removes the specified event
fn remove_activity_from_log(
    mut log: EventLog,
    event_log_classifier: &EventLogClassifier,
    activity: String,
) -> EventLog {
    log.traces = log
        .traces
        .into_iter()
        .map(|mut trace| {
            trace.events = trace
                .events
                .into_iter()
                .filter(|event| {
                    let other = event_log_classifier.get_class_identity(event);
                    activity != other
                })
                .collect();
            trace
        })
        .collect();

    // experimental, what if we only retain traces not empty?
    // log.traces.retain(|trace| {trace.events.len() > 0});
    log
}

#[test]
fn test_remove_activity_with_empty_trace() {
    let log = event_log!([], ["a"], ["a", "b"]);
    let r = remove_activity_from_log(log, &EventLogClassifier::default(), "a".to_string());

    let expected = event_log!([], [], ["b"],);
    assert_eq!(r, expected);
}

/// Helper struct to count the occurrences of each activity in the whole log and in every trace.
/// In 'trace_activities' each index corresponds to a trace at the same index in the event log.
/// The 'activities' member contains information about how often every activity occurs in the whole event log.
struct ActivityTraceCounter {
    activities: HashMap<String, usize>,
    trace_activities: Vec<HashMap<String, usize>>,
}

impl ActivityTraceCounter {
    /// Counts how often every activity of the event log occurs in every trace and in the whole
    /// event log.
    fn new(log: &EventLog, event_log_classifier: &EventLogClassifier) -> ActivityTraceCounter {
        let mut activities = HashMap::new();
        let mut trace_activities = Vec::with_capacity(log.traces.len());

        for (i, trace) in log.traces.iter().enumerate() {
            trace_activities.push(HashMap::new());
            for event in &trace.events {
                let activity = event_log_classifier.get_class_identity(event);
                // update activities
                if let Some(count) = activities.get_mut(&activity) {
                    *count += 1;
                } else {
                    activities.insert(activity.clone(), 1);
                }

                if let Some(count) = trace_activities[i].get_mut(&activity) {
                    *count += 1;
                } else {
                    trace_activities[i].insert(activity, 1);
                }
            }
        }

        ActivityTraceCounter {
            activities,
            trace_activities,
        }
    }

    /// Consume the object and returns the activity count as well as the vector containing the activity count for every trace.
    fn get(self) -> (HashMap<String, usize>, Vec<HashMap<String, usize>>) {
        (self.activities, self.trace_activities)
    }
}

fn cleanup_log(
    log: EventLog,
    event_log_classifier: &EventLogClassifier,
    activity: String,
) -> Fallthrough {
    let log = remove_activity_from_log(log, event_log_classifier, activity.clone());

    let mut node = Node::new_operator(OperatorType::Concurrency);
    let activity_leaf = Node::new_leaf(Some(activity));
    node.add_child(activity_leaf);

    ActivityOncePerTrace(node,log)
}

///This fall through applies if an activity occurs once in every trace of the log.
/// In case this applies to multiple ones an arbitrary is chosen (with the lowest cardinality)
pub fn activity_once_per_trace(
    log: EventLog,
    event_log_classifier: &EventLogClassifier,
) -> Fallthrough {
    let k = log.traces.len();
    // count how often every activity occurs in the event log and in every trace
    let (activities, trace_activities) =
        ActivityTraceCounter::new(&log, event_log_classifier).get();
    let mut activities: Vec<(String, usize)> = activities.into_iter().collect(); // transform to vector in order to sort the activities according to cardinality

    // Sort the activities by cardinality
    (&mut activities).sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap()); // safe unwrap as we compare u32 to other u32

    // set result to none (for now)
    let mut result: Option<String> = None;

    // reverse iterate over the activities, as the activities with more occurrences are more likely to appear precisely once every trace
    'activity_loop: for (activity, cardinality) in activities.into_iter().rev() {
        // activity has to appear precisely once in every trace, therefore skip if it does not appear as often as we have traces
        if cardinality != k {
            continue 'activity_loop;
        }
        for trace in &trace_activities {
            // has to appear precisely one time
            if let Some(count) = trace.get(&activity) {
                if *count != 1 {
                    continue 'activity_loop;
                }
            } else {
                // activity did not appear in the trace or in the event log at all
            }
        }
        // at this point the activity has appeared precisely one time in every trace
        result = Some(activity);
        break 'activity_loop;
    }

    // check result of activity loop
    if result.is_some() {
        cleanup_log(log, event_log_classifier, result.unwrap())
    } else {
        // does not apply - return the event log to be used in other fallthrough cases
        Return(log)
    }
}

/// Public wrapper for [`activity_once_per_trace`].
///
/// This function simply forwards its arguments to
/// `activity_once_per_trace` and exists for consistency
/// with other fall-through detection wrappers.
pub fn activity_once_per_trace_wrapper(
    log: EventLog,
    event_log_classifier: &EventLogClassifier,
    _: &Parameters,
) -> Fallthrough {
    activity_once_per_trace(log, event_log_classifier)
}

mod test_activity_once_per_trace {
    use crate::{event_log, EventLog};
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::core::process_models::process_tree::{Node, OperatorType};
    use crate::discovery::case_centric::inductive_miner_app::fallthrough::activity_once_per_trace::activity_once_per_trace;
    use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough;
    use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough::{ActivityOncePerTrace, Return};

    fn events_equal(log: &EventLog, o_log: &EventLog, event_log_classifier: &EventLogClassifier) -> bool {
        if log.traces.len() == o_log.traces.len() {
            for (t0, t1) in log.traces.iter().zip(o_log.traces.iter()) {
                if t0.events.len() == t1.events.len() {
                    for (e0,e1) in t0.events.iter().zip(t1.events.iter()) {
                        let a0 = event_log_classifier.get_class_identity(e0);
                        let a1 = event_log_classifier.get_class_identity(e1);
                        if a0 != a1 {
                            println!("Two activities did not match{:?}", (a0, a1));

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
    /// The example as defined in Robust Process Mining with Guarantees
    fn leeman_example() {
        let log = event_log!(
            ["a", "b", "c", "d"],
            ["d", "a", "b"],
            ["a", "d", "c"],
            ["b", "c", "d"],
        );

        let result = activity_once_per_trace(log, &EventLogClassifier::default());
        match result {
            ActivityOncePerTrace(node, log) => {
                let expected_log = event_log!(
                    ["a", "b", "c"],
                    ["a", "b"],
                    ["a", "c"],
                    ["b", "c"],
                );
                assert!(events_equal(&log, &expected_log, &EventLogClassifier::default()));

                let mut expected_node = Node::new_operator(OperatorType::Concurrency);
                expected_node.add_child(Node::new_leaf(Some(String::from("d"))));

                assert_eq!(node, expected_node);
            }
            _ => assert!(false),
        }
    }

    #[test]
    /// Assert that the function returns none if there is no activity once in every trace, but almost
    fn test_log_with_no_ft() {
        // fist case - first trace
        let log = event_log!(
            ["a", "b", "c"], // here i removed the 'd'
            ["d", "a", "b"],
            ["a", "d", "c"],
            ["b", "c", "d"],
        );

        let Return(expected_log) = activity_once_per_trace(log.clone(), &EventLogClassifier::default())
        else {
            return assert!(false);
        };

        let log1 = event_log!(
            ["a", "b", "c", "d"],
            ["d", "a", "b"],
            ["a", "d", "c"],
            ["b", "c"], // now the d is missing here
        );

        assert!(events_equal(&log, &expected_log, &EventLogClassifier::default()));

        let Return(log2) = activity_once_per_trace(log1.clone(), &EventLogClassifier::default())
        else {
            return assert!(false);
        };
        assert!(events_equal(&log1, &log2, &EventLogClassifier::default()));
    }

    #[test]
    fn test_with_multiple_activities_appearing_once() {
        let log = event_log!(
            ["a", "b", "c", "d"], // here i removed the 'd'
            ["d", "a", "b", "c"],
            ["a", "d", "c"],
            ["b", "c", "d"],
        );
        let ActivityOncePerTrace(process_node, log) =
            activity_once_per_trace(log, &EventLogClassifier::default())
        else {
            return assert!(false);
        };

        let expected_log = event_log!(
            ["a", "b", "d"],
            ["d", "a", "b"],
            ["a", "d"],
            ["b", "d"],
        );
        let expected_log2 = event_log!(
            ["a", "b", "c"],
            ["a", "b", "c"],
            ["a", "c"],
            ["b", "c"],
        );

        // it really is arbitrary whether c or d is chosen
        assert!(events_equal(&log, &expected_log, &EventLogClassifier::default()) ||
            events_equal(&log, &expected_log2, &EventLogClassifier::default()));


        let mut expected_node = Node::new_operator(OperatorType::Concurrency);
        expected_node.add_child(Node::new_leaf(Some(String::from("c"))));

        let mut expected_node2 = Node::new_operator(OperatorType::Concurrency);
        expected_node2.add_child(Node::new_leaf(Some(String::from("d"))));

        assert!(process_node == expected_node || process_node == expected_node2)
    }

    #[test]
    fn test_two_activites_in_trace() {
        let log = event_log!(
            ["a", "b", "c", "d"],
            ["d", "a", "b", "d"],
            ["a", "d", "c"],
            ["b", "c", "d"],
        );
        let Return(log1) = activity_once_per_trace(log.clone(), &EventLogClassifier::default())
        else {
            return assert!(false);
        };

        assert!(events_equal(&log, &log1, &EventLogClassifier::default()));
    }

    #[test]
    fn test_with_empty_log() {
        let log = event_log!(["a", "b"], []);
        // the fallthrough should not find anything, as there is a trace containing no element
        let Return(_) = activity_once_per_trace(log.clone(), &EventLogClassifier::default()) else {
            return assert!(false);
        };

        let log2 = event_log!(["a", "b"]);
        let r = activity_once_per_trace(log2, &EventLogClassifier::default());
        assert!(r.same_enum_variant(&Fallthrough::ActivityOncePerTrace(
            Node::new_operator(OperatorType::Concurrency),
            event_log!()
        )));
    }
}
