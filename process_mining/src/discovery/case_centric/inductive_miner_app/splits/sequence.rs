//! Utility for resolving sequence cuts into sequence splits.
//! 
//!
//! # Reference:
//! - Leemans, S.J.J., Fahland, D., van der Aalst, W.M.P.:
//!   "Discovering Block-Structured Process Models from Event Logs – A Constructive Approach."
//!   Application of Concurrency to System Design (ACSD), 2013.
//! - Leemans S.J.J., "Robust process mining with guarantees", Ph.D. Thesis, Eindhoven
//!   University of Technology, 09.05.2017

use std::borrow::Cow;
use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::process_tree::OperatorType::Sequence;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
use crate::discovery::case_centric::inductive_miner_app::splits::split::Split;
use crate::EventLog;

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
    if cut.get_operator() != Sequence {
        return None;
    }

    let k = cut.len();
    let partitions = cut.get_own();
    
    // Create k empty sublogs
    let mut result: Vec<EventLog> = vec![log.clone_without_traces(); k];

    todo!();

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
