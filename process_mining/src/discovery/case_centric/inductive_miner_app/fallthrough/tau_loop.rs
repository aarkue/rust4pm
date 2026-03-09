use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::process_tree::Node;
use crate::core::process_models::process_tree::OperatorType::Loop;
use crate::discovery::case_centric::dfg::discover_dfg_with_classifier;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough::Return;
use crate::discovery::case_centric::inductive_miner_app::structures::parameter::Parameters;
use crate::EventLog;

/// Splits the event log according to the semantics of the `tau_loop` fall-through.
///
/// Each trace is split at every occurrence of a *start activity*
/// Whenever a start activity appears and the current subtrace is
/// non-empty, a new trace is created.
///
/// Empty traces are not inserted into the resulting log
///
/// # Returns
/// A new 'Eventlog' in which traces are split at occurrences of start activities.
///  The total number of traces may increase
/// if loop behavior is detected.

fn split_log_according_to_tau_loop(log: EventLog, classifier: &EventLogClassifier) -> EventLog{
    // simply split a trace at the occurrence of any starting activity
    let dfg = discover_dfg_with_classifier(&log, classifier);
    let mut result_log = log.clone_without_traces();


    for trace in log.traces{
        let mut new_trace = trace.clone_without_events();


        for event in trace.events{
            let activity = classifier.get_class_identity(&event);


            // check condition
            if  dfg.start_activities.contains(&activity) && !new_trace.events.is_empty(){
                // condition satisfied, this activity is a start activity
                let help_trace = new_trace.clone_without_events();
                result_log.traces.push(new_trace);
                new_trace = help_trace;
            }

            new_trace.events.push(event);
        }

        // if the trace hasn't been pushed, we need to push it now, but exclude empty traces
        if !new_trace.events.is_empty(){
        result_log.traces.push(new_trace);
        }
    }
    // we need to iterate through the entire log and split a trace if after an end activity an start activity appears
    result_log
}

/// Attempts to apply the 'tau_loop' Fallthrough by
fn tau_loop(log: EventLog, classifier: &EventLogClassifier) -> Fallthrough {
    let k = log.traces.len();
    let log = split_log_according_to_tau_loop(log, classifier);

    if k < log.traces.len(){

        let mut node = Node::new_operator(Loop);
        node.add_child(Node::new_leaf(None)); // placeholder transition, will be replaced
        node.add_child(Node::new_leaf(None)); // silent transition as redo part
        Fallthrough::TauLoop(
            // first return a process node with the required structure
            node,
            log
        )
    } else if k > log.traces.len(){
        panic!("Original log contains more traces, than the log split according to strict tau.")
    }else {
        // default return
        Return(log)
    }

}

/// Public wrapper for [`tau_loop`].
///
/// This function simply forwards its arguments to
/// `tau_loop` and exists for consistency
/// with other fall-through detection wrappers.
pub fn tau_loop_wrapper(log: EventLog, classifier: &EventLogClassifier, _:&Parameters) -> Fallthrough {
    tau_loop(log, classifier)
}



mod test_tau_loop{
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough::TauLoop;
    use crate::discovery::case_centric::inductive_miner_app::fallthrough::tau_loop::tau_loop;
    use crate::{event_log, EventLog};

    fn equal_events(log: &EventLog, o_log: &EventLog, classifier: &EventLogClassifier) -> bool {
        log.traces.len() == o_log.traces.len() && !log.traces.iter().zip(o_log.traces.iter()).any(|(t, o)| {
            t.events.len() != o.events.len() || t.events.iter().zip(o.events.iter()).any(|(e0,e1)| {
                classifier.get_class_identity(e0) != classifier.get_class_identity(e1)
            })
        })
    }
    #[test]
    fn test_split(){
        let log = event_log!(
            ["a", "b", "c", "d"], // here i removed the 'd'
            ["d", "a", "b"],
            ["a", "d", "c"],
            ["b", "c", "d"],
        );

        let expected_log = event_log!(
            ["a"],
            ["b", "c"],
            ["d"],
            ["d"],
            ["a"],
            ["b"],
            ["a"],
            ["d", "c"],
            ["b", "c"],
            ["d"]
        );

        let TauLoop(_node, log)= tau_loop(log, &EventLogClassifier::default()) else { return assert!(false);};

        assert!(equal_events(&log, &expected_log, &EventLogClassifier::default()));
    }
}