//! This module contains algorithms for detecting a cut in a given Directly Follows Graph.

use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::dfg::DirectlyFollowsGraph;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::concurrent::concurrent_cut_wrapper;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::exclusive_choice::exclusive_choice_cut_wrapper;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::loop_cut::redo_loop_cut_wrapper;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::sequence_cut::sequence_cut_wrapper;
use crate::discovery::case_centric::inductive_miner_app::structures::minimum_self_distance::MinimumSelfDistance;
use crate::discovery::case_centric::inductive_miner_app::structures::parameter::{Parameter, Parameters};
use crate::EventLog;

pub mod exclusive_choice;
pub mod cut;
pub mod sequence_cut;
pub mod concurrent;
pub mod loop_cut;




/// Attempts to find a valid cut in the given DirectlyFollowsGraph, by evaluating possible cut types
/// in the following strict order:
/// 1. exclusive choice cut [`exclusive_choice_cut_wrapper`]
/// 2. Sequence cut [`sequence_cut_wrapper`]
/// 3. Concurrent / AND cut [`concurrent_cut_wrapper`]
/// 4. Loop cut [`redo_loop_cut_wrapper`]
///
/// # Returns
/// - Some([`Cut`]) containing the first detected cut according to the strict order.
/// - None otherwise
pub fn find_cut<'a>(dfg: &'a DirectlyFollowsGraph<'_>, log: &EventLog, event_log_classifier: &EventLogClassifier, parameters: &Parameters) -> Option<Cut<'a>>{
    // if any cut is found in the presented order, return the first one
    if let Some(cut) = exclusive_choice_cut_wrapper(dfg){
        Some(cut)
    } else if let Some(cut) = sequence_cut_wrapper(dfg, parameters){
        Some(cut)
    } else  {
        // check whether minimum self distance shall be used
        let mindist = if parameters.contains(&Parameter::MinimumSelfDistance) {
            Some(MinimumSelfDistance::new(log, event_log_classifier))
        } else { None };

        if let Some(cut) = concurrent_cut_wrapper(dfg, mindist) {
            Some(cut)
        } else if let Some(cut) = redo_loop_cut_wrapper(dfg) {
            Some(cut)
        } else {
            None // if no cut is found return none
        }
    }
}

#[cfg(test)]
mod test_cut_finder{
    use std::collections::HashSet;
    use crate::{
        discovery::case_centric::dfg::discover_dfg,
        core::event_data::case_centric::EventLogClassifier,
        event_log,
        discovery::case_centric::inductive_miner_app::cut_finder::find_cut
    };

    #[test]
    fn test_log_with_no_cut(){
        let log = event_log!(
            ["a", "b", "c", "d"],
            ["d", "a", "b"],
            ["a", "d", "c"],
            ["b", "c", "d"],
        );

        let dfg = discover_dfg(&log);
        let cut = find_cut(&dfg, &log, &EventLogClassifier::default(), &HashSet::new());
        assert!(cut.is_none());
    }


}