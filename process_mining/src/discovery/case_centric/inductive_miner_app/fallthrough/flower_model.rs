use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::process_tree::{Node, OperatorType};
use crate::discovery::case_centric::dfg::discover_dfg_with_classifier;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough;
use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough::FlowerModel;
use crate::EventLog;

/// This is the last resort of the fallthrough's of the inductive miner.
/// This FT should only be applied if the event log does not contain any empty trace
pub fn flower_model(log: EventLog, event_log_classifier: &EventLogClassifier) -> Fallthrough {
    let dfg = discover_dfg_with_classifier(&log, event_log_classifier);

    // get all activities in the directly follows graph
    let mut activities: Vec<String> = dfg.activities.iter().map(|(a,_)| a.clone()).collect();

    // sort activities to allow for a defined behavior or so
    (&mut activities).sort();

    // create a concurrency relation over all non-empty activities
    let mut sub_tree = Node::new_operator(OperatorType::Concurrency);

    // add a leaf for each activity
    for activity in activities {
        sub_tree.add_child(Node::new_leaf(Some(activity)));
    }

    // flower root
    let mut flower_node_root = Node::new_operator(OperatorType::Loop);
    // first child of flower model is a concurrency relation over all non-empty activities - do part
    flower_node_root.add_child(sub_tree);

    // add silent transition as second child - redo part
    flower_node_root.add_child(Node::new_leaf(None));

    FlowerModel(flower_node_root)
}


mod test_flower_model {
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::core::process_models::process_tree::{Node, OperatorType};
    use crate::discovery::case_centric::inductive_miner_app::fallthrough::fallthrough::Fallthrough;
    use crate::discovery::case_centric::inductive_miner_app::fallthrough::flower_model::flower_model;
    use crate::event_log;

    #[test]
    fn test_basic_flower_model_leemans(){
        let log = event_log!(
            ["a", "b", "c", "d"],
            ["d", "a", "b"],
            ["a", "d", "c"],
            ["b", "c", "d"],
        );

        let flower = flower_model(log, &EventLogClassifier::default());

        // do part consist of all activities in a concurrency relation
        let mut expected_sub_flower = Node::new_operator(OperatorType::Concurrency);
        expected_sub_flower.add_child(Node::new_leaf(Some(String::from("a"))));
        expected_sub_flower.add_child(Node::new_leaf(Some(String::from("b"))));
        expected_sub_flower.add_child(Node::new_leaf(Some(String::from("c"))));
        expected_sub_flower.add_child(Node::new_leaf(Some(String::from("d"))));

        // build expected flower model
        let mut expected_flower = Node::new_operator(OperatorType::Loop);
        expected_flower.add_child(expected_sub_flower);
        
        // the redo part is just a silent transition
        expected_flower.add_child(Node::new_leaf(None));



        if let Fallthrough::FlowerModel(flower) = flower {
            assert_eq!(expected_flower, flower);
        } else { 
            assert!(false);
        }

    }
}