use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::{
    add_start_end_acts_proj,
    event_log::activity_projection::{ActivityProjectionDFG, EventLogActivityProjection},
    petri_net::petri_net_struct::{ArcType, Marking, PetriNet, TransitionID},
    END_EVENT, START_EVENT,
};

use super::{
    candidate_building::build_candidates,
    candidate_pruning::prune_candidates,
    log_repair::{
        add_artificial_acts_for_loops, add_artificial_acts_for_skips, filter_dfg, SILENT_ACT_PREFIX,
    },
};

#[derive(Debug, Serialize, Deserialize)]
pub struct AlphaPPPConfig {
    pub balance_thresh: f32,
    pub fitness_thresh: f32,
    pub log_repair_skip_df_thresh: u64,
    pub log_repair_loop_df_thresh: u64,
    pub absolute_df_clean_thresh: u64,
    pub relative_df_clean_thresh: f32,
}
pub fn alphappp_discover_petri_net(
    log_proj: &EventLogActivityProjection,
    config: AlphaPPPConfig,
) -> PetriNet {
    let mut log_proj = log_proj.clone();
    add_start_end_acts_proj(&mut log_proj);
    let log_proj = add_artificial_acts_for_skips(&log_proj, config.log_repair_skip_df_thresh);
    let log_proj = add_artificial_acts_for_loops(&log_proj, config.log_repair_loop_df_thresh);
    let dfg = ActivityProjectionDFG::from_event_log_projection(&log_proj);
    let dfg = filter_dfg(
        &dfg,
        config.absolute_df_clean_thresh,
        config.relative_df_clean_thresh,
    );
    let cnds: HashSet<(Vec<usize>, Vec<usize>)> = build_candidates(&dfg);
    let sel = prune_candidates(
        &cnds,
        config.balance_thresh,
        config.fitness_thresh,
        &log_proj,
    );
    let mut pn = PetriNet::new();
    let mut initial_marking: Marking = Marking::new();
    let mut final_marking: Marking = Marking::new();
    let start_act = log_proj.act_to_index.get(&START_EVENT.to_string()).unwrap();
    let end_act = log_proj.act_to_index.get(&END_EVENT.to_string()).unwrap();
    let transitions: Vec<Option<TransitionID>> = log_proj
        .activities
        .iter()
        // TODO: Mark certain transitions as silent
        .map(|act_name| {
            if act_name != &START_EVENT.to_string() && act_name != &END_EVENT.to_string() {
                Some(pn.add_transition(
                    if act_name.starts_with(SILENT_ACT_PREFIX) {
                        None
                    } else {
                        Some(act_name.clone())
                    },
                    None,
                ))
            } else {
                None
            }
        })
        .collect();
    sel.iter().for_each(|(a, b)| {
        let place_id = pn.add_place(None);
        a.iter().for_each(|in_act| {
            if in_act == start_act {
                *initial_marking.entry(place_id).or_insert(0) += 1;
            } else {
                pn.add_arc(
                    ArcType::transition_to_place(transitions[*in_act].unwrap(), place_id),
                    None,
                )
            }
        });
        b.iter().for_each(|out_act| {
            if out_act == end_act {
                *final_marking.entry(place_id).or_insert(0) += 1;
            } else {
                pn.add_arc(
                    ArcType::place_to_transition(place_id, transitions[*out_act].unwrap()),
                    None,
                )
            }
        });
    });

    pn.initial_marking = Some(initial_marking);
    pn.final_markings = Some(vec![final_marking]);
    return pn;
}
