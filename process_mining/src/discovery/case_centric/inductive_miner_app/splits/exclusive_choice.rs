//! Utility for splitting an event log according to an exclusive choice cut.
//! 
//! 
//! # Implementation Notes
//! This implementation adopts the xor-split algorithm as implemented in
//! the ProM framework (`InductiveMiner`), originally written in Java.
//!
//! Reference:
//! - Leemans, S.J.J., Fahland, D., van der Aalst, W.M.P.:
//!   "Discovering Block-Structured Process Models from Event Logs – A Constructive Approach."
//!   Application of Concurrency to System Design (ACSD), 2013.
//! - Leemans S.J.J., "Robust process mining with guarantees", Ph.D. Thesis, Eindhoven
//!   University of Technology, 09.05.2017
//! - ProM source code:
//!   https://github.com/promworkbench/InductiveMiner/blob/main/src/org/processmining/plugins/inductiveminer2/framework/logsplitter/LogSplitterXorFiltering.java
use std::collections::HashMap;
use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::process_tree::OperatorType::ExclusiveChoice;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
use crate::discovery::case_centric::inductive_miner_app::splits::split::Split;
use crate::EventLog;




/// This functions splits an event log according to a provided valid xor cut.
///
/// # Parameters
/// - 'log': the event log to split
/// - 'activity_classifier': the classifier to identify the activities in the events
/// - 'cut': the previously found sequence cut (check the operator)
///
/// # Returns
/// - Some(Split) containing as many logs as the number of partitions in the split.
/// - None if the cut was not a sequence cut nor valid
pub fn xor_split<'a>(log: &EventLog, activity_classifier: &EventLogClassifier, cut: Cut<'a>) -> Option<Split> {
    if cut.get_operator() != ExclusiveChoice || cut.is_empty() {
        // if this is not the demanded operator, return none
        return None;
    }
    let k = cut.len();

    // get partitions from cut
    let partition = cut.get_own();

    // According to the pseudocode in "Leemans S.J.J., "Robust process mining with guarantees", Ph.D. Thesis, Eindhoven
    // University of Technology, 09.05.2017" the algorithm splits the log into several sublogs, by only adding the trace t_i to the sublog L_i
    // if the partition p_i contains all events of t_i

    // assume a cut / partitions like {{A,B}, {C}, {D},{E}}

    // assign every activity an index for faster access later - activites in the same partition get the same index
    // if the assumed cut is used, you would get a map like
    //{ #activity -> index
    // A -> 0,
    // B -> 0,
    // C -> 1,
    // D -> 2,
    // E -> 2
    // }
    //
    let mut activity_partition_idx_map = HashMap::new();
    for (idx, activity_set) in partition.iter().enumerate() {
        for act in activity_set{
            // every unique activity gets another index
            activity_partition_idx_map.insert(act.clone(), idx);
        }
    }

    // produce result vector with k empty logs
    let mut result: Vec<EventLog> = (0..k).map(|_| EventLog::new()).collect();

    // iterate over every tracce, for the example assume a trace [A,A, B, A, B,B]
    for trace in log.traces.iter(){
        let mut counts = vec![0usize; k];

        //count incidents of activities within a partition of the trace
        // for the example trace above we would get a counts-vec : [6,0,0] as all events occur in the
        // very first partition, the latter partitions contain no activity which occurs here
        for event in trace.events.iter(){
            let activity = activity_classifier.get_class_identity(event);
            if let Some(idx) = activity_partition_idx_map.get(activity.as_str()){
                if *idx >= counts.len(){
                    eprintln!("Length matches exactly index! index: {}, counts: {:?}\n activity: {}\n map{:?} ", *idx,counts, activity, activity_partition_idx_map);
                }
                counts[*idx] += 1;
            }
        }

        // get the partition, which contains the maximum occurrences in count
        // for the example it is the partition at index 0 in count as 6 > 0
        let max = if trace.events.is_empty(){
            None
        } else {
            let mut max_idx = 0; // index of activity having most incidents
            let mut max_val = 0; // actual activity with most incidents

            for (i, count) in counts.iter().enumerate(){
                // a tie within the same trace should not occur, because this is a xor cut (maybe in noisy loops??)
                if *count > max_val{
                    max_val  = *count;
                    max_idx =i;
                }
            }

            Some(max_idx)
        };

        // build new sublog - iterate over all indexes, to keep empty traces in every possible sublog, if there is one
        for sublog_idx in 0..k{ // iterate over partition size

            // only do this
            if let Some(winning_partition) = max {
                if winning_partition != sublog_idx {
                    // remove trace from this sublog
                    continue;
                }// else we got the index of the activity within the trace which appears mostly
            } // else trace is empty (max == None)

            let mut new_trace = trace.clone(); // clone current trace
            // Filter events: keep only those
            new_trace.events.retain(|e| {
                // keep only the events of the trace, which appear in the winning partition
                if let Some(act_idx) = activity_partition_idx_map.get(activity_classifier.get_class_identity(e).as_str()){
                    sublog_idx == *act_idx
                } else {
                    false
                }
            });

            // push new trace to trace vec
            result[sublog_idx].traces.push(new_trace);
        }
    }
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