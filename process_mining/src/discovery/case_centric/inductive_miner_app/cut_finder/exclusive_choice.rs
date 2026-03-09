//**
// This Code is based on the paper:
//
// Discovering Block-Structured Process Models From Event Logs - A Constructive Approach
//          by S.J.J. Leemans, D. Fahland, and W.M.P. van der Aalst
//
//
// The algorithm works by recursively identifying splits in the process behavior,
// constructing a hierarchical representation (in case of a process tree).
//
// There are typically four split conditions:
//
// 1. Exclusive choice (xor)
// 2. Sequence
// 3. Concurrent (parallel)
// 4. Loop
//
// If a split condition is matched, an accordingly named cut function is used to cut the log,
// the algorithm continues recursively.

use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use crate::core::process_models::dfg::DirectlyFollowsGraph;
use crate::core::process_models::process_tree::OperatorType;

/// Calculates all connected components of a Graph.
/// For this it starts from every unvisited activity a Breadth First Search over the Graph.
///
/// # Returns
/// A vector containing all connected components
fn calc_connected_components<'a>(
    activities: &'a HashMap<String, u32>,
    adjacent: HashMap<Cow<'a, str>, HashSet<Cow<'a, str>>>,
) -> Vec<HashSet<Cow<'a, str>>> {
    // visited nodes
    let mut visited: HashSet<Cow<'a, str>> = HashSet::new();
    // components (if cut)
    let mut components: Vec<HashSet<Cow<'a, str>>> = Vec::new();

    // iterate over every activity
    for node in activities.keys() {
        let node = Cow::from(node);
        if !visited.contains(&node) {
            // search in components

            // components of the components xd
            let mut comp = HashSet::new();
            let mut queue = VecDeque::new();

            // mark node as already visited
            visited.insert(node.clone());
            // Push starting node
            queue.push_back(node);
            // Explore connected component by looking at every edge of this activity
            while let Some(current) = queue.pop_front() {
                // the starting node is ofc the first node of this nodes component
                comp.insert(current.clone());

                // insert every other node which is reachable and has not already been visited
                if let Some(neighbors) = adjacent.get(&current) {
                    for neighbor in neighbors {
                        if !visited.contains(neighbor) {
                            visited.insert(neighbor.clone());
                            queue.push_back(neighbor.clone());
                        }
                    }
                }
            }
            components.push(comp);
        }
    }

    components
}

/// Calculates an undirected adjacency matrix of a given Directly Follows Graph.
/// The matrix is calculated based on direct reachability and does not include
/// transitive reachability.
///
/// # Returns
/// A hashset mapping each activity to it's neighboring activities, i.e. to activities occurring in an edge with this one
///
/// Note: Only activities occurring at least once inside an edge are taken into account.
pub fn calculate_undirected_adjacency_matrix<'a>(
    dfg: &DirectlyFollowsGraph<'a>,
) -> HashMap<Cow<'a, str>, HashSet<Cow<'a, str>>> {
    let mut adjacent = HashMap::new();

    for ((a1, a2), _) in &dfg.directly_follows_relations {
        // insert both directions
        adjacent
            .entry(a1.clone())
            .or_insert(HashSet::new())
            .insert(a2.clone());
        adjacent
            .entry(a2.clone())
            .or_insert(HashSet::new())
            .insert(a1.clone());
    }
    adjacent
}


/// Attempts to find an exclusive choice cut in the given Directly Follows Graph, by calculating the connected components of the Graph.
///
/// Public wrapper for [`calc_connected_components`]
///
/// # Returns
/// Some(cut) containing the partitions/ connected components found, otherwise None.
#[allow(dead_code)]
pub fn exclusive_choice_cut_wrapper<'a>(dfg: &'a DirectlyFollowsGraph<'_>) -> Option<Cut<'a>> {
    // no start or end activity results in no cut
    if dfg.start_activities.is_empty() || dfg.end_activities.is_empty() {
        return None;
    }

    let components =
        calc_connected_components(&dfg.activities, calculate_undirected_adjacency_matrix(dfg));

    // XOR cut only if > 1 disjoint component
    if components.len() > 1 {
        Some(Cut::new(OperatorType::ExclusiveChoice, components))
    } else {
        None
    }
}


#[allow(unused_imports)]
mod tests {
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::core::process_models::dfg::DirectlyFollowsGraph;
    use crate::discovery::case_centric::inductive_miner_app::cut_finder::exclusive_choice::exclusive_choice_cut_wrapper;
    use crate::{event, event_log, trace};


    #[test]
    fn test_exclusive_choice_cut_2() {
        let log = event_log!(["a", "b"], ["e"]);
        let dfg: DirectlyFollowsGraph<'_> = DirectlyFollowsGraph::discover(&log);
        let result = exclusive_choice_cut_wrapper(&dfg);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 2);
    }

    // Case 1: Clear XOR between b and c
    // Traces: start -> b -> d   OR   start -> c -> d
    #[test]
    fn xor_cut_simple_two_branches() {
        let log = event_log!(["b", "d"], ["c", "e"]);
        let dfg = DirectlyFollowsGraph::discover(&log);

        let cut = exclusive_choice_cut_wrapper(&dfg).unwrap();

        // Expect two components: {"b","d"} and {"e","c"}
        //
        assert_eq!(cut.len(), 2);
        assert!(cut.get_iter().any(|comp| comp.contains("b")));
        assert!(cut.get_iter().any(|comp| comp.contains("c")));
    }

    // Case 2: XOR with 3 different branches
    // Traces: start -> b -> e,  start -> c -> f,  start -> d -> g
    #[test]
    fn xor_cut_three_way_branch() {
        let log = event_log!(["b", "e"], ["c", "f"], ["d", "g"]);
        let dfg = DirectlyFollowsGraph::discover(&log);

        let cut = exclusive_choice_cut_wrapper(&dfg).unwrap();

        // Expect three components: one with b, one with c, one with d
        assert_eq!(cut.len(), 3);
        assert!(cut.get_iter().any(|comp| comp.contains("b")));
        assert!(cut.get_iter().any(|comp| comp.contains("c")));
        assert!(cut.get_iter().any(|comp| comp.contains("d")));
    }

    // Case 3: No XOR (sequence only)
    // Traces: a -> b -> c (repeated)
    #[test]
    fn no_xor_cut_sequence() {
        let log = event_log!(
            ["a", "b", "c"],
            ["a", "b", "c"],
            ["a", "b", "c"],
            ["a", "b", "c"],
            ["a", "b", "c"]
        );
        let dfg = DirectlyFollowsGraph::discover(&log);

        let cut = exclusive_choice_cut_wrapper(&dfg);

        // Should be None because it’s just a sequence
        assert!(cut.is_none());
    }

    // Case 4: Single-event traces -> XOR between start activities
    // Traces: ["a"], ["e"], ["f"]
    #[test]
    fn xor_cut_multiple_single_events() {
        let log = event_log!(["a"], ["e"], ["f"]);
        let dfg = DirectlyFollowsGraph::discover(&log);

        let cut = exclusive_choice_cut_wrapper(&dfg).unwrap();

        // Expect 3 disjoint components
        assert_eq!(cut.len(), 3);
        assert!(cut.get_iter().any(|comp| comp.contains("a")));
        assert!(cut.get_iter().any(|comp| comp.contains("e")));
        assert!(cut.get_iter().any(|comp| comp.contains("f")));
    }

    #[test]
    fn greater_test() {
        let log = event_log!(["a", "b", "c"], ["e", "f"]);
        let dfg = DirectlyFollowsGraph::discover(&log);
        let cut = exclusive_choice_cut_wrapper(&dfg);

        assert!(cut.is_some());
    }

    #[test]
    fn test_parallel_log_no_cut() {
        let log = event_log!(["a", "b"], ["b", "a"]);
        let dfg = DirectlyFollowsGraph::discover(&log);
        let cut = exclusive_choice_cut_wrapper(&dfg);

        // This is a parallel cut, not an exclusive choice cut
        assert!(cut.is_none());
    }
}
