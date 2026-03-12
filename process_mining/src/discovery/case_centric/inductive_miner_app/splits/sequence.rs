//! Utility for resolving sequence cuts into sequence splits.
//! 
//! # Implementation Notes
//! Port of the sequence-split algorithm as implemented in
//! the ProM framework (`InductiveMiner`), originally written in Java.
//!
//! # Reference:
//! - Leemans, S.J.J., Fahland, D., van der Aalst, W.M.P.:
//!   "Discovering Block-Structured Process Models from Event Logs – A Constructive Approach."
//!   Application of Concurrency to System Design (ACSD), 2013.
//! - Leemans S.J.J., "Robust process mining with guarantees", Ph.D. Thesis, Eindhoven
//!   University of Technology, 09.05.2017
//! - ProM source code:
//!   https://github.com/promworkbench/InductiveMiner/blob/main/src/org/processmining/plugins/inductiveminer2/framework/logsplitter/LogSplitterSequenceFiltering.javang
use std::borrow::Cow;
use std::collections::HashSet;
use std::ops::Deref;
use crate::core::event_data::case_centric::{EventLogClassifier, Trace};
use crate::core::process_models::process_tree::OperatorType::Sequence;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
use crate::discovery::case_centric::inductive_miner_app::splits::split::Split;
use crate::EventLog;

/// Finds the postion inside a trace for a split, that most strongly matches a given activity partition.
fn find_optimal_split(
    trace: &Trace,
    partition: &HashSet<Cow<'_, str>>,
    start_pos: usize,
    ignore: &HashSet<String>,
    classifier: &EventLogClassifier,
) -> usize {
    let mut position_least_cost = start_pos; // default
    let mut least_cost = 0;
    let mut cost: i32 = 0;
    let mut position = start_pos;

    // iterate through events of trace from start position to end
    while position < trace.events.len() {
        // get string activity attribute
        let activity = classifier.get_class_identity(&trace.events[position]);

        if ignore.contains(&activity) {
            // skip: contributes nothing to cost
        } else if partition.contains(activity.as_str()) {
            // decrease cost
            cost -= 1;
        } else {
            cost += 1;
        }

        position += 1;

        if cost < least_cost {
            least_cost = cost;
            position_least_cost = position;
        }
    }
    position_least_cost
}
/// Splits an event log according to the partitions of a sequence cut.
///
/// # Returns
/// - Some(Split) containing as many logs as the number of partitions in the split.
/// - None if the cut was not a sequence cut nor valid
pub fn sequence_split<'a>(
    log: &EventLog,
    activity_classifier: &EventLogClassifier,
    cut: Cut<'a>,
) -> Option<Split> {

    if cut.get_operator() != Sequence{
        return None;
    }
    // create results vec with empty event logs
    let k = cut.len();
    let mut result: Vec<EventLog> = Vec::with_capacity(k);

    for _ in 0..k {
        // clone log structure - safe attributes of log and traces, but without events
        let mut sub_log = log.clone_without_traces();
        for trace in &log.traces {
            sub_log.traces.push(trace.clone_without_events());
        }
        result.push(sub_log);
    }

    // get partitions
    let partitions = cut.get_own();
    for (trace_idx, trace) in log.traces.iter().enumerate() {
        let mut curr_position = 0;
        let mut ignore: HashSet<String> = HashSet::new();

        for (partition_idx, partition) in partitions.iter().enumerate() {
            let new_postion = if partition_idx + 1 < k {
                find_optimal_split(
                    trace,
                    partition,
                    curr_position,
                    &ignore,
                    activity_classifier,
                )
            } else {
                // only last partition gets here, it must finish the trace
                trace.events.len()
            };

            // for positions in range [curr_postion, new_position) copy events that belong to the partition

            if new_postion > curr_position {
                // destination trace in result[i] for trace_idx
                let dest_trace = &mut result[partition_idx].traces[trace_idx];

                for pos in curr_position..new_postion {
                    // get trace and retrieve activity
                    let event = &trace.events[pos];
                    let activity = activity_classifier.get_class_identity(event);

                    if partition.contains(activity.as_str()) {
                        dest_trace.events.push(event.clone());
                    }
                }
            }

            // add events from current partition to ignore set
            for act in partition {
                ignore.insert(act.deref().to_string());
            }

            // update position
            curr_position = new_postion;
        }
    }

    Some(Split::new(Sequence, result))
}

#[cfg(test)]
mod test_sequence_split {
    use std::collections::HashSet;
    use crate::core::chrono::Utc;
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::core::process_models::dfg::DirectlyFollowsGraph;
    use crate::discovery::case_centric::inductive_miner_app::cut_finder::sequence_cut::sequence_cut_wrapper;
    use crate::discovery::case_centric::inductive_miner_app::splits::sequence::sequence_split;
    use crate::event_log;

    #[test]
    fn test_sequence_split() {
        let time = Utc::now();
        let log = event_log!(
            ["a"; {"time:timestamp" => time.clone()}, "b"; {"time:timestamp" => time.clone()}, "c"; {"time:timestamp" => time.clone()}],
            ["b"; {"time:timestamp" => time.clone()}, "a"; {"time:timestamp" => time.clone()}, "c"; {"time:timestamp" => time.clone()}]
        );

        let dfg = DirectlyFollowsGraph::discover(&log);
        let cut = sequence_cut_wrapper(&dfg, &HashSet::new());
        assert!(cut.is_some());
        let cut = cut.unwrap();

        let split = sequence_split(&log, &EventLogClassifier::default(), cut);
        assert!(split.is_some());

        let split = split.unwrap().get_own();
        // to the actual split
        let log1 = event_log!(["a"; {"time:timestamp" => time.clone()}, "b"; {"time:timestamp" => time.clone()}], ["b"; {"time:timestamp" => time.clone()}, "a"; {"time:timestamp" => time.clone()}]);
        let log2 = event_log!(["c"; {"time:timestamp" => time.clone()}], ["c"; {"time:timestamp" => time.clone()}]);

        let mut b1 = false;
        let mut b2 = false;
        for log in split {
            // make certain every log is only compared one time, as we don't know the order
            if log == log1 && !b1 {
                b1 = true;
            } else if log == log2 && !b2 {
                b2 = true;
            } else {
                // no matching log or multiple matchings -> immediately false
                assert!(false);
            }
        }
    }

    #[test]
    fn test_sequence_split2() {
        // this log contains a sequence cut, as b or c never reach an "a"
        let time = Utc::now();
        let log = event_log!(
            ["a"; {"time:timestamp" => time.clone()}, "b"; {"time:timestamp" => time.clone()}, "c"; {"time:timestamp" => time.clone()}, "b"; {"time:timestamp" => time.clone()}, "c"; {"time:timestamp" => time.clone()}],
            ["a"; {"time:timestamp" => time.clone()}, "a"; {"time:timestamp" => time.clone()}, "c"; {"time:timestamp" => time.clone()}]
        );
        // we cut this log and sepreate the "a"s from "b's" and "c's"
        // after definition the resulting sublogs contain only those elements which are also in the partition
        // create expected logs
        let log0 = event_log!(["b"; {"time:timestamp" => time.clone()}, "c"; {"time:timestamp" => time.clone()}, "b"; {"time:timestamp" => time.clone()}, "c"; {"time:timestamp" => time.clone()}], ["c"; {"time:timestamp" => time.clone()}]);
        let log1 = event_log!(["a"; {"time:timestamp" => time.clone()}], ["a"; {"time:timestamp" => time.clone()}, "a"; {"time:timestamp" => time.clone()}]);

        let dfg = DirectlyFollowsGraph::discover(&log);
        let cut = sequence_cut_wrapper(&dfg, &HashSet::new());
        assert!(cut.is_some());
        let cut = cut.unwrap();
        let split = sequence_split(&log, &EventLogClassifier::default(), cut);
        assert!(split.is_some());
        let split = split.unwrap().get_own();

        assert_eq!(split.len(), 2);

        // check that both resulting logs match the expected sequence of activities
        let mut b0 = false;
        let mut b1 = false;
        for log in split {
            if log == log0 && !b0 {
                b0 = true;
            } else if log == log1 && !b1 {
                b1 = true;
            }
        }
        assert!(b1);
        assert!(b0);
    }


    #[test]
    fn test_sequence_split3() {
        let log = event_log!(["a", "b", "c", "d"]);
        let dfg = DirectlyFollowsGraph::discover(&log);
        let cut = sequence_cut_wrapper(&dfg, &HashSet::new());
        assert!(cut.is_some());
        let split = sequence_split(&log, &EventLogClassifier::default(), cut.unwrap());
        assert!(split.is_some());

    }


}
