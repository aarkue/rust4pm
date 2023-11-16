use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::{
    add_start_end_acts_proj,
    event_log::activity_projection::{ActivityProjectionDFG, EventLogActivityProjection},
    petri_net::petri_net_struct::{ArcType, Marking, PetriNet, TransitionID},
    END_ACTIVITY, START_ACTIVITY,
};

use super::{
    candidate_building::build_candidates,
    candidate_pruning::prune_candidates,
    log_repair::{
        add_artificial_acts_for_loops, add_artificial_acts_for_skips, filter_dfg, SILENT_ACT_PREFIX,
    },
};

#[derive(Debug, Serialize, Deserialize)]
pub struct AlgoDuration {
    pub loop_repair: f32,
    pub skip_repair: f32,
    pub filter_dfg: f32,
    pub cnd_building: f32,
    pub prune_cnd: f32,
    pub build_net: f32,
    pub total: f32,
}
impl AlgoDuration {
    pub fn to_json(self: &Self) -> String {
        serde_json::to_string(self).unwrap()
    }
    pub fn from_json(json: &str) -> Self {
        serde_json::from_str(json).unwrap()
    }
}

fn get_current_time_fun() -> f32 {
    return std::time::UNIX_EPOCH.elapsed().unwrap().as_secs_f32();
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct AlphaPPPConfig {
    pub balance_thresh: f32,
    pub fitness_thresh: f32,
    pub replay_thresh: f32,
    pub log_repair_skip_df_thresh_rel: f32,
    pub log_repair_loop_df_thresh_rel: f32,
    pub absolute_df_clean_thresh: u64,
    pub relative_df_clean_thresh: f32,
}
impl AlphaPPPConfig {
    pub fn to_json(self: &Self) -> String {
        serde_json::to_string(self).unwrap()
    }
    pub fn from_json(json: &str) -> Self {
        serde_json::from_str(&json).unwrap()
    }
}

pub fn alphappp_discover_petri_net(
    log_proj: &EventLogActivityProjection,
    config: AlphaPPPConfig,
) -> (PetriNet, AlgoDuration) {
    return alphappp_discover_petri_net_with_timing_fn(log_proj, config, &get_current_time_fun);
}

pub fn alphappp_discover_petri_net_with_timing_fn(
    log_proj: &EventLogActivityProjection,
    config: AlphaPPPConfig,
    get_time_fun: &dyn Fn() -> f32,
) -> (PetriNet, AlgoDuration) {
    println!("Started Alpha+++ Discovery");
    let mut algo_dur = AlgoDuration {
        loop_repair: 0.0,
        skip_repair: 0.0,
        filter_dfg: 0.0,
        cnd_building: 0.0,
        prune_cnd: 0.0,
        build_net: 0.0,
        total: 0.0,
    };
    let total_start: f32 = get_time_fun();
    let mut start = get_time_fun();
    let mut log_proj = log_proj.clone();
    add_start_end_acts_proj(&mut log_proj);
    let dfg = ActivityProjectionDFG::from_event_log_projection(&log_proj);
    let dfg_sum: u64 = dfg.edges.values().sum();
    let mean_dfg = dfg_sum as f32 / dfg.edges.len() as f32;

    let start_act = log_proj
        .act_to_index
        .get(&START_ACTIVITY.to_string())
        .unwrap();
    let end_act = log_proj
        .act_to_index
        .get(&END_ACTIVITY.to_string())
        .unwrap();
    println!("Adding start/end acts took: {:.2?}", get_time_fun() - start);
    start = get_time_fun();
    let (log_proj, added_loop) = add_artificial_acts_for_loops(
        &log_proj,
        (config.log_repair_loop_df_thresh_rel * mean_dfg).ceil() as u64,
    );
    algo_dur.loop_repair = get_time_fun() - start;
    println!(
        "Using Loop Log Repair with df_threshold of {}",
        (config.log_repair_loop_df_thresh_rel * mean_dfg).ceil() as u64,
    );
    println!("#Added for loop: {}", added_loop.len());

    start = get_time_fun();
    let (log_proj, added_skip) = add_artificial_acts_for_skips(
        &log_proj,
        (config.log_repair_skip_df_thresh_rel * mean_dfg).ceil() as u64,
    );
    algo_dur.skip_repair = get_time_fun() - start;
    println!("Log Skip/Loop Repair took: {:.2?}", algo_dur.skip_repair);
    start = get_time_fun();

    let mut act_count = vec![0 as i128; log_proj.activities.len()];
    log_proj.traces.iter().for_each(|(trace, w)| {
        trace.iter().for_each(|act| {
            act_count[*act] += *w as i128;
        })
    });
    println!("Act count: {:?}", act_count);
    println!(
        "Acts: {:?}",
        log_proj
            .activities
            .iter()
            .zip(act_count.clone())
            .collect::<Vec<(&String, i128)>>()
    );

    println!("#Added for skip: {}", added_skip.len());
    let dfg = ActivityProjectionDFG::from_event_log_projection(&log_proj);
    let dfg = filter_dfg(
        &dfg,
        config.absolute_df_clean_thresh,
        config.relative_df_clean_thresh,
    );
    println!(
        "Filtered DFG (aDFG) #Edges: {}, Weight Sum: {}",
        dfg.edges.len(),
        dfg.edges.values().sum::<u64>()
    );
    algo_dur.filter_dfg = get_time_fun() - start;
    println!("Filtering DFG took: {:.2?}", algo_dur.filter_dfg);
    start = get_time_fun();
    let cnds: HashSet<(Vec<usize>, Vec<usize>)> = build_candidates(&dfg);
    println!("Built candidates {}", cnds.len());

    algo_dur.cnd_building = get_time_fun() - start;
    println!("Building candidates took: {:.2?}", algo_dur.cnd_building);
    start = get_time_fun();
    let sel = prune_candidates(
        &cnds,
        config.balance_thresh,
        config.fitness_thresh,
        config.replay_thresh,
        act_count,
        &log_proj,
    );
    // sel.iter().for_each(|(a, b)| {
    //     let mut a = log_proj.acts_to_names(a);
    //     let mut b = log_proj.acts_to_names(b);
    //     a.sort();
    //     b.sort();
    //     println!("{:?} => {:?}", a,b);
    // });
    println!("Final pruned candidates: {}", sel.len());
    algo_dur.prune_cnd = get_time_fun() - start;
    println!("Pruning candidates took: {:.2?}", algo_dur.prune_cnd);
    start = get_time_fun();
    let mut pn = PetriNet::new();
    let mut initial_marking: Marking = Marking::new();
    let mut final_marking: Marking = Marking::new();
    let transitions: Vec<Option<TransitionID>> = log_proj
        .activities
        .iter()
        .map(|act_name| {
            if act_name != &START_ACTIVITY.to_string() && act_name != &END_ACTIVITY.to_string() {
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

    let trans_copy = pn.transitions.clone();
    trans_copy.into_iter().for_each(|(id, t)| {
        if t.label.is_none()
            && pn.postset_of_transition((&t).into()).is_empty()
            && pn.preset_of_transition((&t).into()).is_empty()
        {
            pn.transitions.remove(&id).unwrap();
        }
    });

    pn.initial_marking = Some(initial_marking);
    pn.final_markings = Some(vec![final_marking]);
    algo_dur.build_net = get_time_fun() - start;
    println!("Building PN took: {:.2?}", algo_dur.build_net);

    algo_dur.total = get_time_fun() - total_start;
    println!("\n====\nWhole Discovery took: {:.2?}", algo_dur.total);
    return (pn, algo_dur);
}

pub fn cnds_to_names(
    log_proj: &EventLogActivityProjection,
    cnd: &Vec<(Vec<usize>, Vec<usize>)>,
) -> Vec<(Vec<String>, Vec<String>)> {
    cnd.iter()
        .map(|(a, b)| (log_proj.acts_to_names(a), log_proj.acts_to_names(b)))
        .collect()
}
