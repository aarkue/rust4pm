use std::collections::HashSet;

use crate::{
    add_start_end_acts_proj,
    event_log::activity_projection::{ActivityProjectionDFG, EventLogActivityProjection},
    petri_net::petri_net_struct::{ArcType, PetriNet, TransitionID},
};

use super::{
    candidate_building::build_candidates,
    candidate_pruning::prune_candidates,
    log_repair::{
        add_artificial_acts_for_loops, add_artificial_acts_for_skips, filter_dfg, SILENT_ACT_PREFIX,
    },
};

pub fn alphappp_discover_petri_net(log_proj: &EventLogActivityProjection) -> PetriNet {
    let mut log_proj = log_proj.clone();
    add_start_end_acts_proj(&mut log_proj);
    let df_threshold = 50;
    let log_proj = add_artificial_acts_for_skips(&log_proj, df_threshold);
    let log_proj = add_artificial_acts_for_loops(&log_proj, df_threshold);
    let dfg = ActivityProjectionDFG::from_event_log_projection(&log_proj);
    let dfg = filter_dfg(&dfg, 2, 0.01);
    let cnds: HashSet<(Vec<usize>, Vec<usize>)> = build_candidates(&dfg);
    let sel = prune_candidates(&cnds, 0.1, 0.8, &log_proj);
    let mut pn = PetriNet::new();
    let transitions: Vec<TransitionID> = log_proj
        .activities
        .iter()
        // TODO: Mark certain transitions as silent
        .map(|act_name| {
            pn.add_transition(
                if act_name.starts_with(SILENT_ACT_PREFIX) {
                    None
                } else {
                    Some(act_name.clone())
                },
                None,
            )
        })
        .collect();
    sel.iter().for_each(|(a, b)| {
        let place_id = pn.add_place(None);
        a.iter().for_each(|in_act| {
            pn.add_arc(
                ArcType::transition_to_place(transitions[*in_act], place_id),
                None,
            )
        });
        b.iter().for_each(|in_act| {
            pn.add_arc(
                ArcType::place_to_transition(place_id, transitions[*in_act]),
                None,
            )
        });
    });
    return pn;
}
