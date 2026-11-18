use std::collections::HashSet;

use macros_process_mining::register_binding;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::{
    event_data::case_centric::utils::activity_projection::{
        add_start_end_acts_proj, ActivityProjectionDFG, EventLogActivityProjection, END_ACTIVITY,
        START_ACTIVITY,
    },
    process_models::case_centric::petri_net::{ArcType, Marking, TransitionID},
    AcceptingPetriNet, PetriNet,
};

use super::{
    candidate_building::build_candidates,
    candidate_pruning::prune_candidates,
    log_repair::{
        add_artificial_acts_for_loops, add_artificial_acts_for_skips, filter_dfg, SILENT_ACT_PREFIX,
    },
};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, JsonSchema)]
/// Algorithm parameters for Alpha+++
pub struct AlphaPPPConfig {
    /// Balance threshold (for filtering place candidates)
    pub balance_thresh: f32,
    /// Fitness threshold (for filtering place candidates)
    pub fitness_thresh: f32,
    /// Replay threshold (for filtering place candidates)
    pub replay_thresh: f32,
    /// Log repair threshold for skips (wrt. to weighted DFG)
    pub log_repair_skip_df_thresh_rel: f32,
    /// Log repair threshold for loops (wrt. to weighted DFG)
    pub log_repair_loop_df_thresh_rel: f32,
    /// Absolute threshold for weighted DFG cleaning
    pub absolute_df_clean_thresh: u64,
    /// Relative threshold for weighted DFG cleaning
    pub relative_df_clean_thresh: f32,
}
impl Default for AlphaPPPConfig {
    fn default() -> Self {
        Self {
            balance_thresh: 0.2,
            fitness_thresh: 0.75,
            replay_thresh: 0.0,
            log_repair_skip_df_thresh_rel: 2.0,
            log_repair_loop_df_thresh_rel: 2.0,
            absolute_df_clean_thresh: 10,
            relative_df_clean_thresh: 0.1,
        }
    }
}

/// Discovers an [`AcceptingPetriNet`] with the Alpha+++ process discovery algorithm.
#[register_binding]
pub fn discover_alphappp_net(
    log: &EventLogActivityProjection,
    #[bind(default = Default::default())] options: AlphaPPPConfig,
) -> AcceptingPetriNet {
    let mut log_proj = log.clone();
    add_start_end_acts_proj(&mut log_proj);
    let dfg = ActivityProjectionDFG::from_event_log_projection(&log_proj);
    let dfg_sum: u64 = dfg.edges.values().sum();
    let mean_dfg = dfg_sum as f32 / dfg.edges.len() as f32;

    let start_act = log_proj.act_to_index.get(START_ACTIVITY).unwrap();
    let end_act = log_proj.act_to_index.get(END_ACTIVITY).unwrap();
    let (log_proj, _) = add_artificial_acts_for_loops(
        &log_proj,
        (options.log_repair_loop_df_thresh_rel * mean_dfg).ceil() as u64,
    );

    let (log_proj, _) = add_artificial_acts_for_skips(
        &log_proj,
        (options.log_repair_skip_df_thresh_rel * mean_dfg).ceil() as u64,
    );

    let mut act_count = vec![0_i128; log_proj.activities.len()];
    log_proj.traces.iter().for_each(|(trace, w)| {
        trace.iter().for_each(|act| {
            act_count[*act] += *w as i128;
        })
    });
    let dfg = ActivityProjectionDFG::from_event_log_projection(&log_proj);
    let dfg = filter_dfg(
        &dfg,
        options.absolute_df_clean_thresh,
        options.relative_df_clean_thresh,
    );
    let cnds: HashSet<(Vec<usize>, Vec<usize>)> = build_candidates(&dfg);
    let mut sel = prune_candidates(
        &cnds,
        options.balance_thresh,
        options.fitness_thresh,
        options.replay_thresh,
        act_count,
        &log_proj,
    );
    let mut pn = PetriNet::new();
    let mut initial_marking = Marking::new();
    let mut final_marking = Marking::new();
    let connected: HashSet<usize> = sel
        .iter()
        .flat_map(|(a, b)| a.iter().chain(b))
        .copied()
        .collect();
    // Activity indices follow hash order, so transitions, places and arcs are added in activity
    // name order to give the same net, ids included, on every run.
    let names = &log_proj.activities;
    let mut acts_by_name: Vec<usize> = (0..names.len()).collect();
    acts_by_name.sort_by(|x, y| names[*x].cmp(&names[*y]));
    let mut transitions: Vec<Option<TransitionID>> = vec![None; names.len()];
    for &act in &acts_by_name {
        let act_name = &names[act];
        let is_silent = act_name.starts_with(SILENT_ACT_PREFIX);
        let is_start_or_end = act_name == START_ACTIVITY || act_name == END_ACTIVITY;
        // A silent activity no candidate place connects would become an isolated transition
        if !is_start_or_end && (!is_silent || connected.contains(&act)) {
            transitions[act] =
                Some(pn.add_transition((!is_silent).then(|| act_name.clone()), None));
        }
    }
    for (a, b) in &mut sel {
        a.sort_by(|x, y| names[*x].cmp(&names[*y]));
        b.sort_by(|x, y| names[*x].cmp(&names[*y]));
    }
    let named = |v: &[usize]| v.iter().map(|i| &names[*i]).collect::<Vec<_>>();
    sel.sort_by_cached_key(|(a, b)| (named(a), named(b)));
    sel.iter().for_each(|(a, b)| {
        let place_id = pn.add_place(None);
        a.iter().for_each(|in_act| {
            if in_act == start_act {
                initial_marking.add(place_id, 1);
            } else {
                pn.add_arc(
                    ArcType::transition_to_place(transitions[*in_act].unwrap(), place_id),
                    None,
                )
            }
        });
        b.iter().for_each(|out_act| {
            if out_act == end_act {
                final_marking.add(place_id, 1);
            } else {
                pn.add_arc(
                    ArcType::place_to_transition(place_id, transitions[*out_act].unwrap()),
                    None,
                )
            }
        });
    });
    AcceptingPetriNet::new(pn, initial_marking, final_marking)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::get_test_data_path;
    use crate::{EventLog, Importable};

    #[test]
    fn discovery_gives_the_same_net_on_every_run() {
        let log =
            EventLog::import_from_path(get_test_data_path().join("xes").join("RepairExample.xes"))
                .unwrap();
        let discover = || {
            let proj = EventLogActivityProjection::from(&log);
            let pn = discover_alphappp_net(&proj, AlphaPPPConfig::default());
            serde_json::to_value(&pn).unwrap()
        };
        let first = discover();
        for _ in 0..5 {
            assert_eq!(discover(), first);
        }
    }
}
