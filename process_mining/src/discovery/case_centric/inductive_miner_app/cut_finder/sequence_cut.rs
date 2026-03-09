use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use crate::core::process_models::dfg::{Activity, DirectlyFollowsGraph};
use crate::core::process_models::process_tree::OperatorType;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
use crate::discovery::case_centric::inductive_miner_app::structures::parameter::Parameters;

/// Calculate transitiv reachability using Floyd Warshall
fn compute_reachability_matrix(dfg: &DirectlyFollowsGraph<'_>) -> (HashMap<Activity, usize>, Vec<Vec<bool>>) {
    let activities = dfg.activities.iter().map(|(a,_)| a.clone()).collect::<Vec<_>>();
    let n = activities.len();
    let mut map = HashMap::new();

    // Activity_string -> index
    for (i, act) in activities.iter().enumerate() {
        map.insert(act.clone(), i);
    }

    // initialize matrix
    let mut matrix = vec![vec![false; n]; n];

    // mark direct edges
    for ((a, b), _) in &dfg.directly_follows_relations{
        if let (Some(idx_a), Some(idx_b)) = (map.get(a.as_ref()), map.get(b.as_ref())) {
            matrix[*idx_a][*idx_b] = true;
        }
    }

    // Floyd Warshall
    for k in 0..n {
        for i in 0..n{
            for j in 0..n{
                // only update if cell isn't already true
                matrix[i][j] = matrix[i][j] || (matrix[i][k] && matrix[k][j]);
            }
        }
    }

    (map, matrix)
}

/// Helper function which calculates whether a set of activities a can reach another set of activities b.
///
/// # Returns
/// - 'true' if at least one activity in a can transitively reach any activity in b
fn reaches_any_transitive(a: &HashSet<Cow<'_, str>>, b: &HashSet<Cow<'_, str>>,
                          idx_map: &HashMap<String, usize>,
                          matrix: &Vec<Vec<bool>>
) -> bool {
    for act_a in a {
        for act_b in b {
            if let (Some(&idx_a), Some(&idx_b)) = (idx_map.get(act_a.as_ref()), idx_map.get(act_b.as_ref())) {
                if matrix[idx_a][idx_b] {
                    return true;
                }
            }
        }
    }
    false
}


/// Helper function which calculates whether every activity in a set a can reach every activity in another set b.
fn reaches_all_transitive(a: &HashSet<Cow<'_, str>>, b: &HashSet<Cow<'_, str>>,
                          idx_map: &HashMap<String, usize>,
                          matrix: &Vec<Vec<bool>>) -> bool {
    for act_a in a {
        for act_b in b {
            if let (Some(&idx_a), Some(&idx_b)) = (idx_map.get(act_a.as_ref()), idx_map.get(act_b.as_ref())) {
                if !matrix[idx_a][idx_b] {
                    return false;
                }
            }
        }
    }
    true
}


/// Calculates Activity Sequences in a given Directly Follows Graph.
/// Two activities are in sequence if they are neither mutually reachable nor mutually unreachable.
///
/// # Returns
/// A vector of activity partitions representing a candidate sequence cut.
/// Each hashset contains the activity labels belonging to the same sequence block.
/// The partitions are ordered s.t. for any 'i < j', activities in partitions\[i] can (transitively)
/// reach activities in partitions\[j].
fn calc_sequences<'a>(dfg: &'a DirectlyFollowsGraph<'_>) -> Vec<HashSet<Cow<'a, str>>>{
    let (idx_map, matrix) = compute_reachability_matrix(dfg);

    // Initialize each activity with its own partition
    let mut partitions : Vec<HashSet<Cow<'a, str>>> = dfg.activities.keys().map(
        |a| {
            let mut s = HashSet::new();
            s.insert(a.into());
            s
        }
    ).collect();

    // break flag
    let mut changed = true;
    while changed {
        changed = false;
        // iterative over all activities and find bidirectional reachacble components or mutually non reachable components
        let mut i = 0;
        while i < partitions.len() {
            // safe some iterations as the edges are non directional
            let mut j = i + 1;
            while j < partitions.len() {
                // get the current working partitions
                let p_a = &partitions[i];
                let p_b = &partitions[j];

                // Check connectivity between groups - true if at least one activity in p_a reaches at least one other activity in p_b
                let a_reaches_b = reaches_any_transitive(p_a, p_b, &idx_map, &matrix);
                let b_reaches_a = reaches_any_transitive(p_b, p_a, &idx_map, &matrix);

                // Merge if:
                // 1. Mutually reachable (Loop)
                // 2. Mutually unreachable (Exclusive Choice / Parallelism)
                if (a_reaches_b && b_reaches_a) || (!a_reaches_b && !b_reaches_a) {
                    // Merge the whole partition j into partition i
                    let part_j = partitions.remove(j);
                    partitions[i].extend(part_j);
                    // as we changed this partition, we need to iterate over all partitions again, bc maybe the merged partitions are reachable
                    changed = true;
                    // Don't increment j, as the vector shrunk
                } else {
                    // process with next partition
                    j += 1;
                }
            }
            i += 1;
        }
    }

    // 2. Sort partitions to form the candidate sequence
    partitions.sort_by(|p1, p2| {
        let p1_to_p2 = reaches_any_transitive(p1, p2, &idx_map, &matrix);
        let p2_to_p1 = reaches_any_transitive(p2, p1, &idx_map, &matrix);
        // p1 reaches more than p2
        if p1_to_p2 && !p2_to_p1 { // p1 -> p2 but not p2 -> p1
            std::cmp::Ordering::Less
        } else if !p1_to_p2 && p2_to_p1 { // p2 -> p1 but not p1 -> p2
            std::cmp::Ordering::Greater
        } else { // mutually reachable or not reachable - should not happen at all
            panic!("Partitions are in sequence cut are nevertheless mutually reachable or not reachable");
        }
    });

    partitions
}

/// Public wrapper for [`calc_sequences`].
///
/// This function simply forwards its arguments to
/// `calc_sequences` and returns Some(cut) if a cut is found, otherwise None.
///
/// If a [`strict_sequence_cut`] should be applied, this has to be set in a [`Parameter`]
pub fn sequence_cut_wrapper<'a>(dfg: &'a DirectlyFollowsGraph<'_>, _parameters: &Parameters) -> Option<Cut<'a>>{
    // calculate sequence blocks
    let sequences = calc_sequences(dfg);

    // early return
    if sequences.len() <= 1{
        return None;
    }
    
    // at this point we could check whether the sequence satisfies the conditions for a strict sequence cut

    // if there is more than one sequence block, a cut is found successfully
    if sequences.len() > 1 {
        Some(Cut::new(OperatorType::Sequence, sequences))
    } else {
        None
    }
}

#[allow(unused_imports)]
mod test_sequence_cut{
    use std::borrow::Cow;
    use crate::discovery::case_centric::inductive_miner_app::cut_finder::sequence_cut::calc_sequences;
    use std::collections::HashSet;
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::core::process_models::dfg::DirectlyFollowsGraph;
    use crate::discovery::case_centric::dfg::discover_dfg;
    use crate::discovery::case_centric::inductive_miner_app::cut_finder::sequence_cut::{ sequence_cut_wrapper};
    use crate::{event_log, trace, event};
    #[test]
    fn test_single_activity(){
        let dfg = DirectlyFollowsGraph::discover(&event_log!(["a"]));
        let cut = calc_sequences(&dfg);
        let expected = vec![HashSet::from([Cow::from("a".to_string())])];
        assert_eq!(cut, expected);
    }

    #[test]
    fn test_exclusive_choice_cut(){
        let input = event_log!(["a", "b", "c"], ["d"]);
        let dfg = DirectlyFollowsGraph::discover(&input);
        let result = sequence_cut_wrapper(&dfg, &HashSet::new());
        println!("{:?}", result);
        assert!(result.is_some());
        assert_eq!(result.unwrap().get_own().len(), 3);

    }
    #[test]
    fn test_simple_sequence(){
        let input = event_log!(["a", "b", "c"]);
        let dfg = DirectlyFollowsGraph::discover(&input);
        let result = calc_sequences(&dfg);
        let expected = vec![HashSet::from(["a".into()]) , HashSet::from(["b".into()]), HashSet::from(["c".into()])];
        assert_eq!(expected, result)
    }


    #[test]
    fn test_leemans_example(){
        let input = event_log!(["a", "c", "d"], ["b", "c", "e "]);
        let dfg = DirectlyFollowsGraph::discover(&input);
        println!("{:?}", calc_sequences(&dfg));
        let result = sequence_cut_wrapper(&dfg, &HashSet::new());
        assert!(result.is_some());
        let result = result.unwrap();
        println!("{:?}", result);
        assert_eq!(result.get_own().len(), 3);
    }


    #[test]
    fn test_sequence_with_internal_parallelism() {
        // Log: A -> (B || C) -> D
        // Traces: A->B->C->D, A->C->B->D
        let dfg = DirectlyFollowsGraph::discover(&event_log!(
            ["A", "B", "C", "D"],
            ["A", "C", "B", "D"]
        ));

        let cut = sequence_cut_wrapper(&dfg, &HashSet::new()).unwrap();
        let expected: Vec<HashSet<Cow<'_, str>>> = vec![
            HashSet::from(["A".into()]),
            HashSet::from(["B".into(), "C".into()]),
            HashSet::from(["D".into()]),
        ];

        assert_eq!(cut.get_own(), expected);
    }

    #[test]
    fn test_parallel_branches_no_sequence_cut() {
        // Log: A -> B and A -> C in parallel
        let dfg = DirectlyFollowsGraph::discover(&event_log!(
            ["B", "C"],
            ["C", "B"]
        ));
        let cut = sequence_cut_wrapper(&dfg, &HashSet::new());
        assert!(cut.is_none());
    }

    #[test]
    fn test_xor_branch_sequence_cut() {
        // Log: A -> B -> D OR A -> C -> D
        let dfg = DirectlyFollowsGraph::discover(&event_log!(
            ["A", "B", "D"],
            ["A", "C", "D"],
        ));
        let cut = sequence_cut_wrapper(&dfg, &HashSet::new());
        assert!(cut.is_some());
        let cut = cut.unwrap();
        let expected: Vec<HashSet<Cow<'_, str>>> = vec![HashSet::from(["A".into()]) , HashSet::from(["B".into(), "C".into()]), HashSet::from(["D".into()])];
        assert_eq!(cut.get_own(), expected);

    }


    #[test]
    fn test_with_loop(){
        let dfg = DirectlyFollowsGraph::discover(&event_log!(
            ["B", "C"],
            ["C", "B"],
            ["B", "C", "E", "F", "B", "C"],
            ["C", "B", "E", "F", "B", "C"],
            ["B", "C", "E", "F", "C", "B"],
            ["C", "B", "E", "F", "B", "C", "E", "F", "C", "B"],
        ));
        assert!(sequence_cut_wrapper(&dfg, &HashSet::new()).is_none());
    }


    #[test]
    fn test_triangle_cut() {
        let dfg = DirectlyFollowsGraph::discover(&event_log!(
            ["A", "C"],
            ["B", "C", "D"],
            ["B", "D"]
        ));


        let cut = sequence_cut_wrapper(&dfg, &HashSet::new());

        if let Some(c) = cut {
            assert_eq!(c.get_own() , Vec::from([HashSet::from(["A".into(), "B".into()]) , HashSet::from(["C".into()]), HashSet::from(["D".into()])]));
        }
    }


    #[test]
    fn test_strict_sequence_cut_wrapper(){
        let log = event_log!(
            ["a", "b", "c"],
            ["a", "c"],
        );
        let dfg = DirectlyFollowsGraph::discover(&log);
        let cut = sequence_cut_wrapper(&dfg, &HashSet::new()).unwrap();
        println!("{:?}", cut);
    }

}