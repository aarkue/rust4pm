//! Empty traces fallthrough detection utilities.

use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::process_tree::{Node, OperatorType};
use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough::{EmptyTraces, Return};
use crate::discovery::case_centric::inductive_miner_app::structures::parameter::Parameters;
use crate::EventLog;

/// Checks whether the empty traces fallthrough applies to a given log,
/// it applies when the log contains empty traces.
/// 
/// # Returns
/// - [EmptyTraces] if the event log contained empty traces 
/// - [Return] if the event log contained no empty traces
 fn empty_traces(mut log: EventLog, _event_log_classifier: &EventLogClassifier) -> Fallthrough {
    let len_before = log.traces.len();
    log.traces = log.traces.into_iter().filter(|trace| !trace.events.is_empty()).collect();

    if len_before != log.traces.len(){
        // if the len of the trace has changed in the meantime, this means there are some traces lost,
        // due to that they have been empty

        // return a Process node together with the resulting unprocessed traces of the event log

        let mut node = Node::new_operator(OperatorType::ExclusiveChoice);
        node.add_child(Node::new_leaf(None));
        EmptyTraces(node, log)
    } else {
        // otherwise this fallthrough does not apply
        Return(log)
    }
}

/// Public wrapper for [`empty_traces`].
///
/// This function simply forwards its arguments to
/// `empty_traces` and exists for consistency
/// with other fall-through detection wrappers.
pub fn empty_traces_wrapper(log: EventLog, _event_log_classifier: &EventLogClassifier, _: &Parameters) -> Fallthrough {
    empty_traces(log, _event_log_classifier)
}

#[cfg(test)]
mod test_empty_traces_ft{
    use crate::{event_log, event};
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::core::process_models::process_tree::{Node, OperatorType};
    use crate::discovery::case_centric::inductive_miner_app::fallthrough::empty_traces::empty_traces;
    use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough::{EmptyTraces, Return};

    #[test]
    /// test the simplest case, the log should retrun a xor-node with one child from type empty and
    /// a log containing the only not empty trace
    fn test_empty_traces(){
        let log = event_log!(
            [],
            [],
            [],
            ["a"],
            [],
        );

        let EmptyTraces(node, log) = empty_traces(log, &EventLogClassifier::default()) else { return assert!(false); };
        assert_eq!(log.traces.len(), 1);
        assert_eq!(log.traces[0].events.len(), 1);
        assert_eq!(log.traces[0].events[0], event!("a"));

        let mut expected_node = Node::new_operator(OperatorType::ExclusiveChoice);
        expected_node.add_child(Node::new_leaf(None));

        assert_eq!(node, expected_node);
    }

    #[test]
    /// Assert that an event log
    fn test_not_empty_traces(){
        let log = event_log!(
            ["a"],
            ["b"],
            ["f"],
            ["a"],
            ["g"],
        );

        let Return(log1) = empty_traces(log.clone(), &EventLogClassifier::default()) else { return assert!(false); };

        assert_eq!(log, log1);
    }

    #[test]
    /// assert that an empty event log results in no result ('None'),
    /// as this is the basecase
    fn test_empty_log(){
        let log = event_log!();
        let res = empty_traces(log.clone(), &EventLogClassifier::default());
        match res {
            Return(log1) => assert_eq!(log, log1),
            _ => assert!(false),
        }
    }


    #[test]
    fn test_log_only_empty_traces(){
        let log = event_log!(
            [], [], []
        );

        let res = empty_traces(log, &EventLogClassifier::default());
        match res {
            EmptyTraces(node,log1) => {
                assert_eq!(log1.traces.len(), 0);
                assert_eq!(log1, event_log!());
                let mut expected_node = Node::new_operator(OperatorType::ExclusiveChoice);
                expected_node.add_child(Node::new_leaf(None));
                assert_eq!(node, expected_node);
            },
            _ => assert!(false),
        }

    }


}