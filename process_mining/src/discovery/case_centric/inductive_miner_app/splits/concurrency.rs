use std::borrow::Cow;
use std::collections::HashSet;
use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::process_tree::OperatorType::Concurrency;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
use crate::discovery::case_centric::inductive_miner_app::splits::split::Split;
use crate::EventLog;

/// Splits an event log according to the partitions of an AND-Cut (concurrency cut).
///
/// For each partition of the cut a new sub log is created, the traces belonging to these sublogs are retained events of the original trace,
/// those are filtered s.t. only events whose activity belongs to the partition are retained.
///
/// The result is a vector of sub-logs, one per partition, that together form
/// the split required for recursive process tree discovery.
///
/// # Returns
/// Some(split) if the cut struct is a valid and cut
/// None if the cut is not a valid and cut
///
///
/// # Notes
/// - event order within traces is preserved
/// - empty traces may occur if a trace contains no events from a partition
pub fn and_split<'a>(log: &EventLog, activity_classifier: &EventLogClassifier, cut: Cut<'a>) -> Option<Split> {

    // only perform split if the cut is of the type concurrent
    if cut.get_operator() != Concurrency{
        return None;
    }

    // result vector containing sub logs
    let mut result: Vec<EventLog> = Vec::new();
    // the found partitions of the cut
    let partitions: Vec<HashSet<Cow<'a, str>>> = cut.get_own();

    for partition  in partitions.into_iter(){
        let mut new_log = log.clone_without_traces();

        for trace in & log.traces{
            let mut new_trace = trace.clone_without_events();

            for event in trace.events.iter(){
                let activity = activity_classifier.get_class_identity(event);
                if partition.contains(activity.as_str()){
                    new_trace.events.push(event.clone());
                }
            }
            new_log.traces.push(new_trace);
        }

        result.push(new_log);
    }
    Some(Split::new(Concurrency, result))
}


#[allow(unused_imports)]
mod test_and_split{
    use crate::core::chrono::Utc;
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::core::process_models::dfg::DirectlyFollowsGraph;
    use crate::discovery::case_centric::inductive_miner_app::cut_finder::concurrent::concurrent_cut_wrapper;
    use crate::discovery::case_centric::inductive_miner_app::splits::concurrency::and_split;
    use crate::{event_log, EventLog};

    #[test]
    fn test_simple_and_cut_and_split(){
        let time = Utc::now(); // need same timestamp attributes
        let test_log = event_log!(
            ["A";{"time:timestamp" => time.clone()}, "B";{"time:timestamp" => time.clone()}, "C"; {"time:timestamp" => time.clone()}],
            ["A"; {"time:timestamp" => time.clone()}, "C"; {"time:timestamp" => time.clone()}, "B"; {"time:timestamp" => time.clone()}],
            ["C"; {"time:timestamp" => time.clone()}, "A"; {"time:timestamp" => time.clone()}, "B"; {"time:timestamp" => time.clone()}],
        );

        let dfg = DirectlyFollowsGraph::discover(&test_log);
        let cut = concurrent_cut_wrapper(&dfg, None);
        assert!(cut.is_some());
        println!("{:?}", cut);
        let split = and_split(&test_log,&EventLogClassifier::default(), cut.unwrap());
        assert!(split.is_some());
        let split = split.unwrap().get_own();
        println!("{}", split.len());

        let log1 = event_log!(["A"; {"time:timestamp" => time.clone()}, "B"; {"time:timestamp" => time.clone()}], ["A"; {"time:timestamp" => time.clone()}, "B"; {"time:timestamp" => time.clone()}], ["A"; {"time:timestamp" => time.clone()}, "B"; {"time:timestamp" => time.clone()}]);
        let log2 = event_log!(["C"; {"time:timestamp" => time.clone()}], ["C"; {"time:timestamp" => time.clone()}], ["C"; {"time:timestamp" => time.clone()}]);

        let mut b1 = false;
        let mut b2 = false;


        for log in split{
            if log == log1 && !b1{
                b1 = true;
            } else if log == log2 && !b2{
                b2 = true;
            } else {
                assert!(false);
            }
        }
    }

    #[test]
    fn test(){
        let test_log = event_log!([], ["A", "B"], ["B", "A"]);
        let dfg = DirectlyFollowsGraph::discover(&test_log);
        let cut = concurrent_cut_wrapper(&dfg, None);
        assert!(cut.is_some());
        let split = and_split(&test_log,&EventLogClassifier::default(), cut.unwrap());
        assert!(split.is_some());
        let split = split.unwrap().get_own();


        for log in split.into_iter().enumerate(){
            println!("Log: {}", log.0);
            for t in log.1.traces.into_iter().enumerate(){
                println!("trace{}", t.0);
                for e in t.1.events.into_iter().enumerate(){
                    println!(" {}", e.0);
                }

            }
        }

    }
}