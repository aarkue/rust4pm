//! Utility for splitting a log according to a loop cut

use std::collections::HashMap;
use crate::EventLog;
use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::process_tree::OperatorType::Loop;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
use crate::discovery::case_centric::inductive_miner_app::splits::split::Split;

/// Splits an event log according to the partition of a Loop-cut.
pub fn loop_split<'a>(log: &EventLog, classifier: &EventLogClassifier, cut: Cut<'a>) -> Option<Split> {
    if Loop != cut.get_operator() {
        return None;
    }
    
    let k = cut.len();
    let mut result: Vec<EventLog> = vec![log.clone_without_traces(); k];
    let partitions = cut.get_own();

    todo!();

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
