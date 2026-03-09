use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::process_tree::OperatorType;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
use crate::discovery::case_centric::inductive_miner_app::splits::concurrency::and_split;
use crate::discovery::case_centric::inductive_miner_app::splits::exclusice_choice::xor_split;
use crate::discovery::case_centric::inductive_miner_app::splits::redo_loop::loop_split;
use crate::discovery::case_centric::inductive_miner_app::splits::sequence::sequence_split;
use crate::discovery::case_centric::inductive_miner_app::splits::split::Split;
use crate::EventLog;

mod concurrency;
mod sequence;
mod exclusice_choice;
mod redo_loop;
pub mod split;


/// A wrapper for the actual split function.  
///
/// This function simply forwards its arguments to [`splitting`].
///
/// # Panic
/// This function panics if the provided cut somehow could not be handled by the splitting algorithm, 
/// this should only be the case iff the operator of the cut finds no split operator.
pub fn perform_split<'a>(log: &EventLog, classifier: &EventLogClassifier, cut: Cut<'a>) -> Split{
    if let Some(split) = splitting(log, classifier, cut) {
        split
    } else {
        panic!("No split function found for the cut operator.")
    }

}


/// Core Split function matching the cut operator to the matching split function. 
///
/// [`xor_split`] 
///
/// [`sequence_split`]
///
/// [`and_split`]
///
/// [`loop_split`]
fn splitting<'a>(log: &EventLog, classifier: &EventLogClassifier, cut: Cut<'a>) -> Option<Split>{
    // match the operator and perform the matching split
    match cut.get_operator() {
        OperatorType::ExclusiveChoice => {
            xor_split(log, classifier, cut)
        }
        OperatorType::Sequence => {
            sequence_split(log, classifier, cut)
        }
        OperatorType::Concurrency => {
            and_split(log, classifier, cut)
        }
        OperatorType::Loop => {
            loop_split(log, classifier, cut)
        }
    }
}


mod test_splits{
    use std::collections::HashSet;
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::core::process_models::dfg::DirectlyFollowsGraph;
    use crate::discovery::case_centric::inductive_miner_app::cut_finder::sequence_cut::sequence_cut_wrapper;
    use crate::discovery::case_centric::inductive_miner_app::splits::sequence::sequence_split;
    use crate::event_log;

    #[test]
    fn test_sequence_split() {
        let log = event_log!(["a", "b", "c", "d"]);
        let dfg = DirectlyFollowsGraph::discover(&log);
        let cut = sequence_cut_wrapper(&dfg, &HashSet::new());
        assert!(cut.is_some());
        let split = sequence_split(&log, &EventLogClassifier::default(), cut.unwrap());
        assert!(split.is_some());
    }
}