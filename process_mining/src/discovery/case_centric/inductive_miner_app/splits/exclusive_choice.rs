//! Utility for splitting an event log according to an exclusive choice cut.

use std::collections::HashMap;
use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::process_tree::OperatorType::ExclusiveChoice;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
use crate::discovery::case_centric::inductive_miner_app::splits::split::Split;
use crate::EventLog;

/// This functions splits an event log according to a provided valid xor cut.
pub fn xor_split<'a>(log: &EventLog, activity_classifier: &EventLogClassifier, cut: Cut<'a>) -> Option<Split> {
    if cut.get_operator() != ExclusiveChoice || cut.is_empty() {
        return None;
    }
    
    let k = cut.len();
    let partition = cut.get_own();
    let mut result: Vec<EventLog> = vec![log.clone_without_traces(); k];

    todo!();

    Some(Split::new(ExclusiveChoice, result))
}

#[cfg(test)]
mod tests_xor_split{
    use std::collections::HashSet;
    use crate::core::chrono::Utc;
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::core::process_models::dfg::DirectlyFollowsGraph;
    use crate::core::process_models::process_tree::OperatorType::ExclusiveChoice;
    use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
    use crate::discovery::case_centric::inductive_miner_app::cut_finder::exclusive_choice::exclusive_choice_cut_wrapper;
    use crate::discovery::case_centric::inductive_miner_app::splits::exclusive_choice::xor_split;
    use crate::event_log;

    #[test]
    fn test_basic(){
        let log = event_log!(
            ["A", "A", "B", "e"],
            ["C", "D"]
        );

        let dfg = DirectlyFollowsGraph::discover(&log);
        let cut = exclusive_choice_cut_wrapper(&dfg);

        assert!(cut.is_some());
        let cut = cut.unwrap();

        let x = xor_split(&log, &EventLogClassifier::default(), cut);
        assert!(x.is_some());
        let x = x.unwrap();
        assert_eq!(x.sub_logs.len(), 2);
    }

    #[test]
    fn test_only_empty_traces_and_cut(){
        let log = event_log!(
            [],
            []
        );

        let mut cut = Vec::new();
        cut.push(HashSet::new());
        cut.push(HashSet::new());
        cut.push(HashSet::new());
        let cut = Cut::new(ExclusiveChoice, cut);
        let x = xor_split(&log, &EventLogClassifier::default(), cut);
        assert!(x.is_some());
        let x = x.unwrap().get_own();
        assert_eq!(x.len(), 3); // exactly 3 sublogs
        for log in x{
            // each sublog has exactly 2 empty logs
            assert_eq!(log.traces.len(), 2);
            for trace in log.traces{
                assert!(trace.events.is_empty())
            }
        }
    }

    #[test]
    fn test_leeman_example(){
        let time = Utc::now();
        let log = event_log!(
            ["A";{"time:timestamp" => time.clone()}, "B";{"time:timestamp" => time.clone()}],
            ["C";{"time:timestamp" => time.clone()}, "C";{"time:timestamp" => time.clone()}, "C";{"time:timestamp" => time.clone()}]
        );

        let dfg = DirectlyFollowsGraph::discover(&log);
        let cut = exclusive_choice_cut_wrapper(&dfg);
        assert!(cut.is_some());
        let cut = cut.unwrap();
        let x = xor_split(&log, &EventLogClassifier::default(), cut);
        assert!(x.is_some());
        let x = x.unwrap().get_own();
        assert_eq!(x.len(), 2);
        for log in x{
            if log.traces.len() == 1{
                if log.traces[0].events.len() == 2{
                    assert_eq!(log, event_log!(["A";{"time:timestamp" => time.clone()}, "B";{"time:timestamp" => time.clone()}] {"concept:name" => 0},));
                } else {
                    assert_eq!(log, event_log!(["C";{"time:timestamp" => time.clone()}, "C";{"time:timestamp" => time.clone()}, "C";{"time:timestamp" => time.clone()}]{"concept:name" => 1}));
                }
            } else {
                // if there is not exactly one trace per log, sth is really wrong
                assert!(false);
            }
        }
    }
}