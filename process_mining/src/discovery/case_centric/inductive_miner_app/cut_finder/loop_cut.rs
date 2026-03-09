/// This implementation follows the Loop cut finder algorithm as implemented in
/// the ProM framework (`InductiveMiner`), originally written in Java.
///
/// Reference:
/// - Leemans, S.J.J., Fahland, D., van der Aalst, W.M.P.:
///   "Discovering Block-Structured Process Models from Event Logs – A Constructive Approach."
///   Application of Concurrency to System Design (ACSD), 2013.
/// - Leemans S.J.J., "Robust process mining with guarantees", Ph.D. Thesis, Eindhoven
///   University of Technology, 09.05.2017
/// - ProM source code:
///   https://github.com/promworkbench/InductiveMiner/blob/main/src/org/processmining/plugins/inductiveminer2/framework/cutfinders/CutFinderIMLoop.java
use std::borrow::Cow;
use std::collections::HashSet;
use crate::core::process_models::dfg::DirectlyFollowsGraph;
use crate::core::process_models::process_tree::OperatorType;
use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
use crate::discovery::case_centric::inductive_miner_app::structures::components::Components;


/// Attempts to find a loop cut in a given Directly Follows Graph (DFG).
///
/// The algorithm groups activities into connected components by using a union-find like structure.
///
/// 1. Selects a pivot activity from the sets of start activities.
/// 2. Merges all start and end activities with component of pivot.
/// 3. Merges internal activities (no start nor end activity) based on the edges in the DFG, excluding
/// edges that would violate redo-loop semantic.
/// 4. Merges components based on certain rules about their connectivity
///
///
/// The resulting vector represents the activity partitions of the
/// candidate redo-loop cut. The first partition corresponds to the
/// component containing the pivot (the "do" part),
/// and the remaining partitions correspond to the "redo" part(s).
///
/// # Panic
/// Panics if the dfg contains no start activity
fn redo_loop_cut<'a>(dfg: &'a DirectlyFollowsGraph<'_>) -> Vec<HashSet<Cow<'a, str>>> {
    // activities
    let nodes: Vec<Cow<'a, str>> = dfg.activities.iter().map(|(act, _)| Cow::from(act)).collect();
    let mut components = Components::new(&nodes);

    // start element as pivot element -> safe unwrap as there has to be at least one start element
    let pivot = dfg.start_activities.iter().next().unwrap();
    for start in &dfg.start_activities {
        components.merge_components_of(pivot, start);
    }
    for end in &dfg.end_activities {
        components.merge_components_of(pivot, end);
    }

    // merge inner components
    for ((v0, v1), _) in &dfg.directly_follows_relations {
        let v0_is_start = dfg.start_activities.contains(v0.as_ref());
        let v0_is_end = dfg.end_activities.contains(v0.as_ref());
        let v1_is_start = dfg.start_activities.contains(v1.as_ref());

        if !v0_is_start {
            if !v0_is_end {
                if !v1_is_start {
                    components.merge_components_of(v0, v1);
                }
            }
        } else if v0_is_end {
            components.merge_components_of(v0, v1);
        }
    }

    // create sub end and sub start activities
    let mut sub_end_activities = HashSet::new();
    let mut sub_start_activities = HashSet::new();

    // sort edges into components
    for ((v0, v1), _) in &dfg.directly_follows_relations {
        if components.same_component(&v0, &v1) {
            sub_start_activities.insert(v0);
            sub_end_activities.insert(v1);
        }
    }

    // check if sub-end-activities are connected to all start activities
    for sub_end in sub_end_activities {
        for start in &dfg.start_activities {
            if components.same_component(sub_end, start) {
                break;
            }
            if !dfg.contains_df_relation((sub_end.clone(), start.into())) {
                components.merge_components_of(sub_end, start);
                break;
            }
        }
    }

    for sub_start in sub_start_activities {
        for end_activity in dfg.end_activities.iter() {
            if components.same_component(&sub_start, &end_activity) {
                break;
            }
            if dfg.contains_df_relation((sub_start.clone(), end_activity.into())) {
                components.merge_components_of(sub_start, end_activity);
                break;
            }
        }
    }

    // reorder so that pivot comes first
    let mut partition = components.get_components();
    let pivot = Cow::Owned(pivot.to_string());
    if let Some(pos) = partition.iter().position(|set| set.contains(&pivot)) {
        partition.swap(0, pos);
    }

    partition

    //  check whether those sub component belongs to the do or the redo
}

/// Attempts to find a Loop cut in a given DFG.
///
/// Public wrapper for [`redo_loop_cut`]
///
/// #Returns
/// Some(cut) if a loop cut has successfully been discovered, None otherwise
pub fn redo_loop_cut_wrapper<'a>(dfg: &'a DirectlyFollowsGraph<'_>) -> Option<Cut<'a>>{

    // only possible if there are start and end activities
    if dfg.start_activities.is_empty() || dfg.end_activities.is_empty() {
        return None;
    }

    // calculate do-redo loop components
    let components = redo_loop_cut(dfg);

    // a cut is found if there is more than one component
    if components.len() > 1{
        Some(Cut::new(OperatorType::Loop, components))
    } else {
        None
    }


}

#[allow(unused_imports)]
mod test_redo_loop_cut{
    use std::collections::HashMap;
    use crate::{event_log, trace, event};
    use crate::core::process_models::dfg::DirectlyFollowsGraph;
    use crate::core::process_models::process_tree::OperatorType;
    use crate::discovery::case_centric::inductive_miner_app::cut_finder::cut::Cut;
    use super::*;

    #[test]
    fn test_redo_on_single_activity(){
        let log = event_log!(
                ["a", "c"],
                ["a", "c", "b", "a", "c"]
        );
        let dfg = DirectlyFollowsGraph::discover(&log);
        let cut = redo_loop_cut(&dfg);

        assert_eq!(cut.len(), 2);
        assert!(cut[0].contains("a") && cut[0].contains("c"));
        assert!(cut[1].contains("b"));
    }

    #[test]
    fn test_no_loop() {
        let log = event_log!(
            ["a", "b", "c"],
            ["a", "b", "c"],
        );

        let dfg = DirectlyFollowsGraph::discover(&log);

        let cut = redo_loop_cut(&dfg);

        assert_eq!(cut.len(), 1);
        assert!(cut[0].contains("a") && cut[0].contains("b") && cut[0].contains("c"));
    }

    #[test]
    fn test_multi_activity_redo() {
        let log = event_log!(
            ["a", "c"],
            ["a", "c", "b", "d", "a", "c"],
        );

        let dfg = DirectlyFollowsGraph::discover(&log);

        let cut = redo_loop_cut(&dfg);

        assert_eq!(cut.len(), 2);

        let do_group = &cut[0];
        let redo_group = &cut[1];

        assert!(do_group.contains("a") && do_group.contains("c"));
        assert!(redo_group.contains("b") && redo_group.contains("d"));
    }

    #[test]
    fn test_nested_loops_only_outer_cut() {
        let log = event_log!(
            ["s", "a", "c", "e"],
            ["s", "a", "c", "b", "a", "c", "e"], // inner loop
            ["s", "a", "c", "e", "g", "s", "a", "c", "e"],
            ["s", "a", "c", "b", "a", "c", "b", "a", "c", "e"],
        );

        let dfg = DirectlyFollowsGraph::discover(&log);


        let cut = redo_loop_cut(&dfg);

        assert_eq!(cut.len(), 2);

        assert!(cut[1].contains("g"));
        assert!(cut[0].contains("a") && cut[0].contains("c"));
    }


    #[test]
    fn test_complex_test(){
        let mut dfg = DirectlyFollowsGraph::new();
        dfg.activities = HashMap::from([("a".to_string(), 1), ("b".to_string(), 1),("c".to_string(), 1)]);
        dfg.directly_follows_relations =
            HashMap::from([
                (("a".into(),"b".into()),1),
                (("b".into(),"a".into()),1),
                (("b".into(),"c".into()),1),
                (("c".into(),"b".into()),1),
                (("c".into(),"a".into()),1),
                (("a".into(),"c".into()),1),
            ]
            );
        dfg.start_activities = HashSet::from(["a".to_string(), "b".to_string()]);
        dfg.end_activities = HashSet::from(["c".to_string()]);

        println!("Found component: {:?}", redo_loop_cut(&dfg));
    }



    #[test]
    fn test_double_loop(){
        let log = event_log!(
            ["a", "b"],
            ["a", "b", "c", "a", "b"],
            ["a", "b", "d", "a", "b"],
            ["a", "b", "d", "a", "b", "a", "b", "c", "a", "b"],
            ["a", "b", "c", "a", "b", "a", "b", "d", "a", "b"]
        );
        let dfg = DirectlyFollowsGraph::discover(&log);
        let cut = redo_loop_cut_wrapper(&dfg);
        assert!(cut.is_some());

        //expect a cut of three partitions
        let expectations = Cut::new(OperatorType::Loop, vec![
            HashSet::from(["a".into(), "b".into()]),
            HashSet::from(["c".into()]),
            HashSet::from(["d".into()])
        ]);
        assert_eq!(cut.unwrap(), expectations);
    }

    #[test]
    fn test_loop_over_parallel(){
        let log = event_log!(
            ["a", "b"],
            ["a", "b", "c", "a", "b"],
            ["a", "d", "b"],
            ["a", "d", "b", "c", "a", "d", "b" ],
            ["a", "d", "b", "c", "a", "b" ]
        );
        let dfg = DirectlyFollowsGraph::discover(&log);

        let cut = redo_loop_cut_wrapper(&dfg);

        assert!(cut.is_some());
        let expectations = Cut::new(OperatorType::Loop,
                                    vec![
                                        HashSet::from(
                                            ["a".into(), "b".into(), "d".into()]),
                                        HashSet::from(["c".into()])]
        );

        assert_eq!(cut.unwrap(), expectations);
    }


}
