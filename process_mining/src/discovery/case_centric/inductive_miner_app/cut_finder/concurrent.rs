//! Utility for detecting a concurrency cut in a given Directly Follows Graph. 

use std::borrow::Cow;
use std::collections::HashSet;
use crate::core::process_models::dfg::DirectlyFollowsGraph;
use crate::core::process_models::process_tree::OperatorType;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
use crate::discovery::case_centric::inductive_miner_app::structures::minimum_self_distance::MinimumSelfDistance;

///Partitions activities into components, such that activities in different components can occur
/// concurrently. Two activities are in the same component if they are not bidirectionally reachable.
///
/// Optionally, a minimum self distance constraint can further restrict concurrency, by
/// forcing activities, which are in a minimum self distance relation with other activities,
/// into the same component.
///
/// # Parameters
fn concurrent_cut<'a>(dfg: &'a DirectlyFollowsGraph<'_>, mindist: &Option<MinimumSelfDistance>) -> Option<Vec<HashSet<Cow<'a, str>>>> {
    todo!()
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
        let _cut = concurrent_cut(&dfg, &None);
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
