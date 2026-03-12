//! Utility for detecting a concurrency cut in a given Directly Follows Graph. 
//! 
//! This implementation ports the parallel cut algorithm as implemented in
//! the ProM framework (`InductiveMiner`), originally written in Java.
//!
//! Reference:
//! - Leemans, S.J.J., Fahland, D., van der Aalst, W.M.P.:
//!   "Discovering Block-Structured Process Models from Event Logs – A Constructive Approach."
//!   Application of Concurrency to System Design (ACSD), 2013.
//! - Leemans S.J.J., "Robust process mining with guarantees", Ph.D. Thesis, Eindhoven
//!   University of Technology, 09.05.2017
//! - ProM source code:
//!  https://github.com/promworkbench/InductiveMiner/blob/main/src/org/processmining/plugins/inductiveminer2/framework/cutfinders/CutFinderIMConcurrent.java

use std::borrow::Cow;
use std::collections::HashSet;
use crate::core::process_models::dfg::DirectlyFollowsGraph;
use crate::core::process_models::process_tree::OperatorType;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
use crate::discovery::case_centric::inductive_miner_app::structures::components::Components;
use crate::discovery::case_centric::inductive_miner_app::structures::minimum_self_distance::MinimumSelfDistance;
// Following the definition of an parallel cut, every element has to be either a starting activity or an end activity.
// Also, every element has to be connected to each other element - like a mesh

// Example
//           / -> A -> B -\
// START -->|              |-> END
//           \ -> B -> A -/
// .



/// Ensures that every resulting component has both start and end activities,
/// a concurrent cut only makes sense if every isolated component can be entered or left independently.
///
/// To do this, this functions categorizes each connected component into one of four categories:
/// (start & end, start only, end only, neither start nor end).
/// Every not start & end category components is merged with an arbitrary component (here the first one)
/// containing both  start & end activities.
#[allow(dead_code)]
fn ensure_start_end_in_each<'a>(
    dfg: &'a DirectlyFollowsGraph<'_>,
    con_components: Vec<HashSet<Cow<'a, str>>>,
) -> Option<Vec<HashSet<Cow<'a, str>>>> {
    // create for different classes of components

    let mut start_end = Vec::new();
    let mut start = Vec::new();
    let mut end = Vec::new();
    let mut neither = Vec::new();

    for component in con_components {
        let has_start = component
            .iter()
            .any(|act| dfg.start_activities.contains(act.as_ref()));
        let has_end = component
            .iter()
            .any(|act| dfg.end_activities.contains(act.as_ref()));

        match (has_start, has_end) {
            (true, true) => {
                // components which have both start and end activities
                start_end.push(component);
            }
            (true, false) => {
                // components which contain start and no end activity
                start.push(component);
            }
            (false, true) => {
                // components which contains no start but end activities
                end.push(component);
            }
            (false, false) => {
                // neither start nor end activities in this components
                neither.push(component);
            }
        }
    }

    // no component with start and end -> no parallel cut
    if start_end.len() == 0 {
        return None;
    }

    // Start building final components
    let mut result = start_end;

    loop {
        match (start.pop(), end.pop()) {
            // combine start-only and end-only components
            (Some(mut start), Some(end)) => {
                start.extend(end);
                result.push(start);
            }

            (Some(start), None) => {
                // add remaining start only components to any component
                (&mut result[0]).extend(start);
            }
            (None, Some(end)) => {
                // add remaining end only components to any component
                (&mut result[0]).extend(end);
            }
            (None, None) => {
                // add components that have neither start nor end
                for component in neither {
                    (&mut result[0]).extend(component)
                }
                // no components left -> break the loop
                break;
            }
        }
    }
    Some(result)
}

///Partitions activities into components, such that activities in different components can occur
/// concurrently. Two activities are in the same component if they are not bidirectionally reachable.
///
/// Optionally, a minimum self distance constraint can further restrict concurrency, by
/// forcing activities, which are in a minimum self distance relation with other activities,
/// into the same component.
///
/// # Parameters
fn concurrent_cut<'a>(dfg: &'a DirectlyFollowsGraph<'_>, mindist: &Option<MinimumSelfDistance>) -> Option<Vec<HashSet<Cow<'a, str>>>> {
    let activities: Vec<Cow<'_, str>> = dfg.activities.keys().map(|act| Cow::from(act)).collect();
    if activities.is_empty() {
        return None;
    }

    // merge activities into components (based on which other activities are reachable)
    let mut components = Components::new(&activities);

    for (i, act1) in activities.iter().enumerate() {
        for (j, act2) in activities.iter().enumerate() {
            // do not do that for the same activity
            if i < j && !components.same_component(act1, act2) {
                // merge only bidirectional activities
                let has_a1_a2 = dfg.contains_df_relation((act1.clone(), act2.clone()));
                let has_a2_a1 = dfg.contains_df_relation((act2.clone(), act1.clone()));

                if !has_a1_a2 || !has_a2_a1 {
                    components.merge_components_of(act1, act2);
                }
            }
        }
    }

    // optional minimum self distance
    if let Some(mindist) = mindist {
        for activity1 in activities.iter(){
            if let Some(mindist) = mindist.get_minimum_distance(activity1){
                for activity2 in &mindist.1{
                    components.merge_components_of(activity1, activity2.as_str());
                }
            }
        }
    }

    let components = components.get_components();
    if components.len() > 1 {
        ensure_start_end_in_each(dfg, components)
    } else {
        None
    }
}



/// Examines whether in a given Directly Follows Graph a concurrent cut can be applied.
///
/// Public wrapper for [`concurrent_cut`]
///
/// # Parameters
/// - 'dfg': the directly follows Graph which shall be examined
/// - 'mindist': Optional a minimum self distance constraint can be applied, by providing a Minimum self distance struct.
/// # Returns
/// - a cut struct containing at least 2 components of concurrent activities
/// - None, otherwise (this means a concurrent cut can not be applied)
pub fn concurrent_cut_wrapper<'a>(dfg: &'a DirectlyFollowsGraph<'_>, mindist: Option<MinimumSelfDistance>) -> Option<Cut<'a>> {
    // if there are not start or end activities, there is no cut
    if dfg.start_activities.is_empty() || dfg.end_activities.is_empty() {
        return None;
    }

    let result = concurrent_cut(dfg, &mindist);
    if let Some(result) = result {
        if result.len() <= 1 {
            None
        } else {
            Some(Cut::new(OperatorType::Concurrency, result))
        }
    } else {
        None
    }
}

#[cfg(test)]
mod test_parallel_cut {
    use std::borrow::Cow;
    use std::collections::{HashMap, HashSet};
    use crate::core::process_models::dfg::DirectlyFollowsGraph;
    use crate::discovery::case_centric::inductive_miner_app::cut_finder::concurrent::{concurrent_cut, concurrent_cut_wrapper};
    use crate::event_log;

    #[test]
    fn test_leeman_example() {
        let log = event_log!(
            ["a", "b", "c"],
            ["a", "c", "b"],
            ["c", "a", "b"]
        );
        let dfg = &DirectlyFollowsGraph::discover(&log);
        let cut = concurrent_cut_wrapper(&dfg, None);
        assert!(cut.is_some());
        let mut partitions = cut.unwrap().get_own();
        // sort to ensure order
        partitions.sort_by(|x,y| x.len().cmp(&y.len()));
        assert_eq!(
            partitions,
            Vec::from([
                HashSet::from(["c".into()]),
                HashSet::from(["a".into(), "b".into()])
            ])
        );
    }

    #[test]
    fn test_parallel_cut_with_trailing_activity() {
        let dfg = DirectlyFollowsGraph::discover(
            &event_log!(["a", "b", "c"], ["b", "a", "c"])
        );
        let cut = concurrent_cut(&dfg, &None);
    }

    #[test]
    fn test_easy_parallel_cut_wrapper() {
        let dfg = DirectlyFollowsGraph::discover(
            &event_log!(["a", "b"], ["b", "a"])
        );
        let cut = concurrent_cut_wrapper(&dfg, None);
        assert!(cut.is_some());
        assert_eq!(cut.unwrap().len(), 2);
    }

    #[test]
    fn test_three_branch_parallel() {
        let dfg = DirectlyFollowsGraph::discover(
            &event_log!(
                ["a", "b"],
                ["b", "c"],
                ["c", "a"],
                ["a", "c"],
                ["b", "a"],
                ["c", "b"]
            )
        );

        let cut = concurrent_cut_wrapper(&dfg, None);
        assert!(cut.is_some());

        let parts = cut.unwrap();
        assert_eq!(parts.len(), 3);

        let flattened: HashSet<Cow<'_, str>> = parts.partitions
            .iter()
            .flat_map(|p| p.iter().map(|s| s.clone()))
            .collect();

        assert!(flattened.contains("a"));
        assert!(flattened.contains("b"));
        assert!(flattened.contains("c"));
    }

    #[test]
    fn test_sequence_cut_in_parallel() {
        let dfg = DirectlyFollowsGraph::discover(
            &event_log!(["a", "b", "c"], ["a", "d", "c"])
        );
        assert!(concurrent_cut_wrapper(&dfg, None).is_none());
    }

    #[test]
    fn test_hard_parallel_cut_multiple_starts_and_endings() {
        let mut dfg = DirectlyFollowsGraph::new();
        dfg.activities = HashMap::from([("a".into(), 1), ("b".into(), 2), ("c".into(), 3)]);

        dfg.start_activities = HashSet::from(["a".into()]);
        dfg.end_activities = HashSet::from(["c".into(), "b".into()]);
        dfg.directly_follows_relations = HashMap::from([
            (("a".into(), "b".into()), 1),
            (("b".into(), "a".into()), 1),
            // a <-> c
            (("a".into(), "c".into()), 1),
            (("c".into(), "a".into()), 1),
            // c <-> b
            (("b".into(), "c".into()), 1),
            (("c".into(), "b".into()), 1),
        ]);

        assert!(concurrent_cut_wrapper(&dfg, None).is_none());

        // // set multiple starts
        dfg.start_activities = HashSet::from(["a".to_string(), "b".to_string()]);
        dfg.end_activities = HashSet::from(["c".to_string()]);
        assert!(concurrent_cut_wrapper(&dfg, None).is_none());

        // overlap
        dfg.end_activities = HashSet::from(["c".to_string(), "b".to_string()]);
        assert!(concurrent_cut_wrapper(&dfg, None).is_some());

        // everything is end and start activity
        dfg.start_activities = HashSet::from(["a".to_string(), "b".to_string(), "c".to_string()]);
        dfg.end_activities = HashSet::from(["a".to_string(), "b".to_string(), "c".to_string()]);
        assert!(concurrent_cut_wrapper(&dfg, None).is_some());

        // no ending or start at b -> AND cut
        dfg.start_activities = HashSet::from(["a".to_string(), "c".to_string()]);
        dfg.end_activities = HashSet::from(["a".to_string(), "c".to_string()]);
        let cut = concurrent_cut_wrapper(&dfg, None);
        assert!(cut.is_some());
        assert_eq!(cut.unwrap().len(), 2);
    }

    #[test]
    fn test_perfect_parallel_three_branches() {
        // all permutations of a, b, c to allow full bidirectional behavior
        let log = event_log!(
            ["a", "b", "c"],
            ["a", "c", "b"],
            ["b", "a", "c"],
            ["b", "c", "a"],
            ["c", "a", "b"],
            ["c", "b", "a"]
        );
        let dfg = DirectlyFollowsGraph::discover(&log);
        let cut = concurrent_cut_wrapper(&dfg, None);
        assert!(cut.is_some());
        assert_eq!(cut.unwrap().get_own().len(), 3);
    }

    #[test]
    fn test_sequence_cut() {
        let log = event_log!(
            ["a", "b", "c"],
            ["a", "b", "c"],
            ["a", "b", "c"]
        );
        let dfg = DirectlyFollowsGraph::discover(&log);

        assert!(concurrent_cut_wrapper(&dfg, None).is_none());
    }

    #[test]
    fn test_xor_cut() {
        let log = event_log!(["a", "b"], ["c", "d"], ["a", "b"]);
        let dfg = DirectlyFollowsGraph::discover(&log);

        // XOR-Components would be {a, b} and {c, d}
        // Parallel Cut has to be None as there are no edges between {a,b} and {c,d}
        assert!(concurrent_cut_wrapper(&dfg, None).is_none());
    }

    #[test]
    fn test_noisy_parallel_fails_without_filter() {
        let log = event_log!(
            // ("a", "b", "c"), // b-> c missing
            ["b", "a", "c"],
            ["a", "c", "b"],
            //("b", "c", "a"), // c-> a missing
            ["c", "b", "a"] //o_trace!("c", "a", "b") is missing -> no edge c -> a
        );
        // there are two edges missing c->a and b-> c, therefore there is no bidirectional relation in any case
        let dfg = DirectlyFollowsGraph::discover(&log);

        assert!(concurrent_cut_wrapper(&dfg, None).is_none());
    }

    #[test]
    fn test_loop_cut() {
        let log = event_log!(
            ["a"],                     //  Start
            ["a", "b", "a"],           //  Loop
            ["a", "b", "a", "b", "a"]  //  Loop
        );
        let dfg = DirectlyFollowsGraph::discover(&log);

        assert!(concurrent_cut_wrapper(&dfg, None).is_none());
    }
}
