use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::process_tree::Node;
use crate::core::process_models::process_tree::OperatorType::Loop;
use crate::discovery::case_centric::dfg::discover_dfg_with_classifier;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough::{Return, StrictTauLoop};
use crate::discovery::case_centric::inductive_miner_app::structures::parameter::Parameters;
use crate::EventLog;

fn split_log_according_to_strict_tau(log: EventLog, classifier: &EventLogClassifier) -> EventLog{
    let dfg = discover_dfg_with_classifier(&log, classifier);
    let mut result_log = log.clone_without_traces();


    for trace in log.traces{
        let mut last_event_was_end = false;
        let mut new_trace = trace.clone_without_events();


        for event in trace.events{
            let activity = classifier.get_class_identity(&event);

            // check condition
            if last_event_was_end && dfg.start_activities.contains(&activity){
                // condition satisfied, the last activity was an end activity, this one is  a start,
                // we need to split the current trace at this point right now
                let help_trace = new_trace.clone_without_events();
                result_log.traces.push(new_trace);
                new_trace = help_trace;
            }

            // push event to new_trace
            new_trace.events.push(event);

            // if this activity is an end activity set the according flag
            last_event_was_end = dfg.end_activities.contains(&activity);

        }

        // if the trace hasn't been pushed, we need to push it now -- this includes empty traces
        result_log.traces.push(new_trace);
    }
    // we need to iterate through the entire log and split a trace if after an end activity an start activity appears
    result_log
}

///
fn strict_tau_loop(log: EventLog, classifier: &EventLogClassifier) -> Fallthrough {
    let k = log.traces.len();
    let log = split_log_according_to_strict_tau(log, classifier);

    if k < log.traces.len(){
        let mut node = Node::new_operator(Loop);
        node.add_child(Node::new_leaf(None)); // temporary at index 0
        node.add_child(Node::new_leaf(None)); // redo part is silent


        StrictTauLoop(
            // first return a process node with the required structure
            node,
            // secondly return the new event log
            log
        )
    } else if k > log.traces.len(){
        panic!("Original log contains more traces, than the log split according to strict tau.")
    }else {
        // default return
        Return(log)
    }

}

/// Public wrapper for [`strict_tau_loop`].
///
/// This function simply forwards its arguments to
/// `strict_tau_loop` and exists for consistency
/// with other fall-through detection wrappers.
pub fn strict_tau_loop_wrapper(log: EventLog, classifier: &EventLogClassifier, _:&Parameters) -> Fallthrough {
    strict_tau_loop(log, classifier)
}



mod test_strict_tau_loop{
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough;
    use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough::{Return, StrictTauLoop};
    use crate::{event_log, EventLog};
    use crate::discovery::case_centric::inductive_miner_app::fallthrough::strict_tau_loop::strict_tau_loop;


    fn cmp_logs(log: Fallthrough, expected: EventLog){
        let classifier = EventLogClassifier::default();
        assert!(if let StrictTauLoop(_, log) = log {
            log.traces.len() == expected.traces.len() && !log.traces.iter().zip(expected.traces.iter()).any(|(t0,t1)|
                t0.events.len() != t1.events.len() || t0.events.iter().zip(t1.events.iter()).any(|(e0,e1)| {
                    classifier.get_class_identity(e0) != classifier.get_class_identity(e1)
            })
            )
        } else {
            false
        })
    }
    #[test]
    fn test_split(){
        let log = event_log!(
            ["a", "b", "c", "d"],
            ["d", "a", "b"],
            ["a", "d", "c"],
            ["b", "c", "d"],
        );

        let expected_log = event_log!(
            ["a", "b", "c"],
            ["d"],
            ["d"],
            ["a", "b"],
            ["a", "d", "c"],
            ["b", "c"],
            ["d"]
        );

        cmp_logs(strict_tau_loop(log, &EventLogClassifier::default()), expected_log);


    }

    #[test]
    fn strict_tau_loop_simple_split() {
        let log = event_log!(
        ["a", "b", "c", "a", "c"], // contains c (end) followed by a (start) -> split
    );

        // Splitting at c|a -> two traces: "a b c" and "a d"
        // L.len() = 1, L1.len() = 2  => strict tau-loop discovered
        let expected_log = event_log!(
        ["a", "b", "c"],
        ["a", "c"],
    );

        cmp_logs(strict_tau_loop(log, &EventLogClassifier::default()), expected_log);

    }


    #[test]
    fn strict_tau_multiple_splits_in_trace() {
        let log = event_log!(
        // start set will contain "a" (first event of every trace if all traces start with a),
        // end set will contain "c" (last events),
        // here we have "... c a ... c a ..." -> two splits -> three traces after split
        ["a", "b", "c", "a", "b", "c", "a", "b", "c"],
    );

        // Splits at each c|a produce three identical traces "a b c"
        // L.len() = 1, L1.len() = 3 => tau-loop discovered
        let expected_log = event_log!(
        ["a", "b", "c"],
        ["a", "b", "c"],
        ["a", "b", "c"],
    );

        cmp_logs(strict_tau_loop(log, &EventLogClassifier::default()), expected_log);


    }

    #[test]
    fn strict_tau_no_split() {
        let log = event_log!(
        ["a", "b", "c"],  // starts with a, ends with c
        ["d", "e"],       // starts with d, ends with e
        ["f", "g", "h"]   // starts with f, ends with h
    );

        // start set = {a, d, f}, end set = {c, e, h}
        // There is no occurrence inside any trace of (c|e|h) followed immediately by (a|d|f)
        // => L1.len() == L.len() -> no tau-loop found
        let expected_log = event_log!(
        ["a", "b", "c"],
        ["d", "e"],
        ["f", "g", "h"]
    );

        if let Return(log) = strict_tau_loop(log, &EventLogClassifier::default()){
            assert_eq!(log, expected_log);
        }

    }

    #[test]
    fn strict_tau_start_end_overlap() {
        let log = event_log!(
            ["a", "b", "a", "c", "a"],  // start set contains "a", end set contains "a"
            ["c", "d"]                      // trivial trace starting and ending with a
        );
        let expected_log = event_log!(
            ["a", "b", "a"],   // prefix up to first split
            ["c", "a"],        // remainder after that split
            ["c", "d"],             // original second trace unchanged
        );

        cmp_logs(strict_tau_loop(log, &EventLogClassifier::default()), expected_log);

    }

    #[test]
    fn strict_tau_single_trace_to_many() {
        let log = event_log!(
        ["x", "a", "b", "a", "x", "y", "a"], // suppose start set includes x and end set includes a
    );
        let expected_log = event_log!(
        ["x", "a", "b", "a"],
        ["x", "y", "a"],
    );
        cmp_logs(strict_tau_loop(log, &EventLogClassifier::default()), expected_log);
    }


    // 7) Edge case: traces of length 1 where start==end; adjacent repetition inside a longer trace causes multiple tiny splits
    #[test]
    fn strict_tau_length_one_traces_and_adjacent_repeats() {
        let log = event_log!(
        ["a"],                     // start/end = a
        ["a", "a", "b", "a", "a"], // many a|a adjacencies
    );

        // start set = {a}, end set = {a, a} => {a}
        // split at every a|a adjacency inside second trace -> many fragments
        // One reasonable expected L1 (fragmenting around adjacent a's) could be:
        let expected_log = event_log!(
        ["a"],           // first trace unchanged
        ["a"],           // fragment from leading 'a' in second trace
        ["a", "b", "a"], // middle fragment
        ["a"],           // trailing fragment
    );
        cmp_logs(strict_tau_loop(log, &EventLogClassifier::default()), expected_log);
    }
}