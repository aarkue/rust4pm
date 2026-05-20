//! inductive miner discovery algorithm

use base_cases::{find_base_case, BaseCases};
use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::process_tree::{Node, ProcessTree};
use crate::discovery::case_centric::dfg::discover_dfg_with_classifier;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::find_cut;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::apply_fallthrough;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough;
use crate::discovery::case_centric::inductive_miner_app::splits::perform_split;
use crate::discovery::case_centric::inductive_miner_app::structures::parameter::{Parameter, Parameters};
use crate::EventLog;

mod cut_finder;
mod structures;
mod splits;
mod fallthrough;
mod base_cases;


/// Mines a process tree from the given event log using the Inductive Miner
/// with default parameter settings.
///
/// This function initializes the default mining parameters, recursively
/// builds the process tree, and applies post-processing (folding)
/// if configured in the parameters.
///
/// # Parameters
/// - `log`: The event log to mine.
/// - `event_log_classifier`: Classifier used to determine activity identities.
///
/// # Returns
/// The root `ProcessNode` of the discovered process tree.
pub fn inductive_miner_default_parameters(log: EventLog, event_log_classifier: &EventLogClassifier) -> ProcessTree {
    // uses default parameters while for mining the process tree model
    let parameters = Parameter::generate_default_parameters();
    let node = build_tree(log, event_log_classifier, &parameters, 0);
    ProcessTree::new(node).fold()
}


/// Converts a detected cut into a corresponding process tree node.
///
/// The event log is split according to the cut ([`perform_split`]), and for each resulting
/// sub-log the Inductive Miner is recursively applied. The resulting
/// subtrees become the children of a new process node labeled with
/// the cut's operator.
///
/// # Parameters
/// - `cut`: The detected cut.
/// - `event_log_classifier`: Activity classifier.
/// - `log`: The event log to split.
/// - `parameters`: Mining parameters.
/// - `depth`: Current recursion depth - debug reasons
///
/// # Returns
/// A `ProcessNode` representing the cut and its recursively mined children.
fn convert_cut_to_process_node<'a>(cut: Cut<'a>, event_log_classifier: &EventLogClassifier, log: EventLog, parameters: &Parameters, depth: usize) -> Node {
    // extract operator and split the original event log
    let operator = cut.get_operator();
    let split = perform_split(&log, event_log_classifier, cut);

    // acquire ownership of the split vector
    let split = split.get_own();

    // create new node
    let mut cut_node = Node::new_operator(operator);

    // this could be done in parallel
    for log in split{
        cut_node.add_child(build_tree(log, &event_log_classifier, parameters,depth +1));
    }

    // return new process node
    cut_node
}

/// Applies fallthrough strategies ([`apply_fallthrough`]) if no valid cut can be found.
///
///
/// Fallthroughs ensure that a process tree can always be constructed,
/// even if the log does not yield a structured cut. Depending on the
/// detected pattern, additional recursive mining steps may be performed.
///
/// # Parameters
/// - `log`: The event log.
/// - `event_log_classifier`: Activity classifier.
/// - `parameters`: Mining parameters.
/// - `depth`: Current recursion depth - debug reasons
///
/// # Returns
/// A `ProcessNode` representing the fallthrough model.
fn fallthrough_finder(log: EventLog, event_log_classifier: &EventLogClassifier, parameters: &Parameters, depth: usize) -> Node {
    // default fallthrough
    // We are getting a guaranteed fallthrough, default is flower model
    match apply_fallthrough(log, event_log_classifier, parameters){
        #[allow(unused_mut)]
        Fallthrough::EmptyTraces(mut node, log) |
        Fallthrough::ActivityOncePerTrace(mut node, log) => {
            node.add_child(build_tree(log, &event_log_classifier, parameters,depth+1));
            node
        }
        #[allow(unused_mut)]
        Fallthrough::StrictTauLoop(mut node, log) |
        Fallthrough::TauLoop(mut node, log) => {
            if let Node::Operator(op) = &mut node{
                // replace the placeholder node at index 0
                op.children[0] = build_tree(log, event_log_classifier, parameters,depth+1);
            } else {
                panic!("TauLoop node is not an operator node.")
            }
            node
        }
        Fallthrough::ActivityConcurrent(mut node, filtered_out_log, split) => {
            // The AND-node already holds the concurrent activity as its first child.
            // Build the sub-tree for the extracted (concurrent) activity.
            node.add_child(build_tree(filtered_out_log, event_log_classifier, parameters, depth + 1));

            // The split was already performed inside the fallthrough; add each
            // resulting sub-log as a further grand-children
            let operator_type = split.get_operator().clone();
            let split = split.get_own();
            let mut child = Node::new_operator(operator_type);
            for log in split {
                child.add_child(build_tree(log, event_log_classifier, parameters, depth + 1));
            }
            node.add_child(child);
            node
        }
        Fallthrough::FlowerModel(node) => { node} // not much to do, this is the default
        Fallthrough::Return(_) => { // THis point should not be reached at all, as the flower model is the default
            panic!("Fallthrough::Return in build tree function - must not happen");
        }
    }

}

/// Core recursive function of the Inductive Miner.
///
/// The algorithm proceeds as follows:
/// 1. Check for base cases (empty log or single activity): [`find_base_case`]
/// 2. If none apply, construct the directly-follows graph (DFG) [`DirectlyFollowsGraph::create_from_log`]
/// 3. Attempt to find a valid cut.#: [`find_cut`]
/// 4. If a cut is found, split the log and recurse on each sub-log: [`convert_cut_to_process_node`]
/// 5. Otherwise, apply a fallthrough strategy: [`fallthrough_finder`]
///
/// # Parameters
/// - `log`: The event log to mine.
/// - `event_log_classifier`: Activity classifier.
/// - `parameters`: Mining parameters.
/// - `depth`: Current recursion depth.
///
/// # Returns
/// The root `ProcessNode` of the mined (sub)tree.
pub fn build_tree(log: EventLog, event_log_classifier: &EventLogClassifier, parameters: &Parameters, depth: usize) -> Node{
    match find_base_case(&log, event_log_classifier){
        BaseCases::None => {
            let dfg = discover_dfg_with_classifier(&log, event_log_classifier);
            let cut = find_cut(&dfg, &log, event_log_classifier, parameters); // find cut, if there is some
            if cut.is_some(){
                convert_cut_to_process_node(cut.unwrap(), event_log_classifier, log, parameters, depth)
            } else {
                fallthrough_finder(log, event_log_classifier, parameters, depth)
            }
        }
        BaseCases::Empty => {
            Node::new_leaf(None)
        }
        BaseCases::SingleActivity(activity) => {
            Node::new_leaf(Some(activity))
        }
    }

}


#[cfg(test)]
mod tests {

    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::core::process_models::process_tree::Node;
    use crate::core::process_models::process_tree::OperatorType::{ExclusiveChoice, Loop};
    use crate::discovery::case_centric::inductive_miner_app::inductive_miner_default_parameters;
    use crate::event_log;

    #[test]
    fn test_works_without_panic() {
        let log = event_log!(
            ["a", "b", "c", "d"],
            ["a", "b", "c", "d", "e", "a", "b", "c", "d"],
        );
        let event_log_classifier = EventLogClassifier::default();

        let node = inductive_miner_default_parameters(log, &event_log_classifier);
        assert!(node.is_valid());
    }

    #[test]
    fn test_loop_over_same_activity(){
        let log = event_log!(["a", "a"]);


        let node = inductive_miner_default_parameters(log, &EventLogClassifier::default());

        let mut expected = Node::new_operator(Loop);
        expected.add_child(Node::new_leaf(Some(String::from("a"))));
        expected.add_child(Node::new_leaf(None));

        assert!(node.is_valid());
        assert_eq!(node.root, expected);
    }

    #[test]
    fn test_complex_log(){
        let log = event_log![
            ["a", "b", "d"],
            ["a", "d", "b"],
            ["a", "b", "c", "a", "b"],
            ["a", "d", "c", "a", "d"],
            ["a", "b", "d", "c", "a", "d", "b"],
            ["a", "d", "b", "c", "a", "b", "d"],
        ];
        let node = inductive_miner_default_parameters(log, &EventLogClassifier::default());

        assert!(node.is_valid())
    }


    #[test]
    fn test_loop_over_same_activity_with_empty_trace(){
        let log = event_log!(
            [],
            ["a", "a"],
        );

        let node = inductive_miner_default_parameters(log, &EventLogClassifier::default());

        let mut expected_sub = Node::new_operator(Loop);
        expected_sub.add_child(Node::new_leaf(Some(String::from("a"))));
        expected_sub.add_child(Node::new_leaf(None));

        let mut expected = Node::new_operator(ExclusiveChoice);
        expected.add_child(Node::new_leaf(None));
        expected.add_child(expected_sub);


        assert!(node.is_valid());
        assert_eq!(node.root, expected);
    }

    #[test]
    fn test_empty_trace_plus_base_case(){
        let log = event_log!(["a"],[]);
        let node = inductive_miner_default_parameters(log, &EventLogClassifier::default());

        let mut expected = Node::new_operator(ExclusiveChoice);
        expected.add_child(Node::new_leaf(None));
        expected.add_child(Node::new_leaf(Some(String::from("a"))));

        assert!(node.is_valid());
        assert_eq!(node.root, expected);
    }
}

/// Integration tests that require external test data files.
/// Run with: cargo test sepsis -- --nocapture
#[cfg(test)]
mod integration_tests {
    use crate::core::event_data::case_centric::xes::import_xes::import_xes_path;
    use crate::core::event_data::case_centric::xes::import_xes::XESImportOptions;
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::discovery::case_centric::inductive_miner_app::inductive_miner_default_parameters;
    use crate::test_utils::get_test_data_path;

    /// Mine the Sepsis Cases event log and print the resulting process tree
    /// in canonical parenthesis notation (SEQ/XOR/AND/LOOP/tau).
    ///
    /// The output can be compared against pm4py:
    ///   python3 pm4py_sepsis_tree.py
    ///
    /// The test asserts:
    ///   1. The log can be imported (1050 traces).
    ///   2. The discovered tree passes is_valid().
    ///   3. Every activity in the log appears as a leaf in the tree.
    #[test]
    fn test_sepsis_tree_is_valid_and_print() {
        let path = get_test_data_path()
            .join("xes")
            .join("Sepsis Cases - Event Log.xes.gz");

        if !path.exists() {
            eprintln!(
                "Skipping Sepsis integration test: file not found at {:?}.\n\
                 Download the test data from https://rwth-aachen.sciebo.de/s/4cvtTU3lLOgtxt1 \
                 and place it under process_mining/test_data/xes/",
                path
            );
            return;
        }

        let log = import_xes_path(&path, XESImportOptions::default())
            .expect("Failed to import Sepsis log");
        assert_eq!(log.traces.len(), 1050, "Expected 1050 traces");

        let classifier = EventLogClassifier::default();
        let tree = inductive_miner_default_parameters(log.clone(), &classifier);

        // --- validity ---
        assert!(tree.is_valid(), "Discovered tree is not valid");

        // --- all activities present ---
        let mut expected_activities: std::collections::HashSet<String> = std::collections::HashSet::new();
        for trace in &log.traces {
            for event in &trace.events {
                expected_activities.insert(classifier.get_class_identity(event));
            }
        }
        let tree_leaves: std::collections::HashSet<String> = tree
            .find_all_leaves()
            .into_iter()
            .filter_map(|l| {
                use crate::core::process_models::case_centric::process_tree::process_tree_struct::LeafLabel;
                match &l.activity_label {
                    LeafLabel::Activity(s) => Some(s.clone()),
                    LeafLabel::Tau => None,
                }
            })
            .collect();

        for act in &expected_activities {
            assert!(
                tree_leaves.contains(act),
                "Activity '{act}' is missing from the process tree"
            );
        }

        // --- print for manual comparison ---
        println!("\n=== Rust Inductive Miner – Sepsis Cases ===");
        println!("Unique activities in log : {}", expected_activities.len());
        println!("Leaves in tree           : {}", tree_leaves.len());
        println!("\nProcess tree (canonical):\n{}", tree);
    }
}
