use std::{
    collections::HashSet,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::{
    event_log::activity_projection::{
        add_start_end_acts_proj, ActivityProjectionDFG, EventLogActivityProjection, END_ACTIVITY,
        START_ACTIVITY,
    },
    petri_net::petri_net_struct::{ArcType, Marking, PetriNet, TransitionID},
};

use super::{
    candidate_building::build_candidates,
    candidate_pruning::prune_candidates,
    log_repair::{
        add_artificial_acts_for_loops, add_artificial_acts_for_skips, filter_dfg, SILENT_ACT_PREFIX,
    },
};

#[derive(Debug, Serialize, Deserialize)]
/// Duration (in seconds) per parts of the Alpha+++ algorithm (+ total time)
pub struct AlgoDuration {
    /// Duration for loop repair (in seconds)
    pub loop_repair: f32,
    /// Duration for skip repair (in seconds)
    pub skip_repair: f32,
    /// Duration for filtering DFG (in seconds)
    pub filter_dfg: f32,
    /// Duration for building place candidates (in seconds)
    pub cnd_building: f32,
    /// Duration for pruning place candidates (in seconds)
    pub prune_cnd: f32,
    /// Duration for constructing Petri net (in seconds)
    pub build_net: f32,
    /// Total duration (in seconds)
    pub total: f32,
}
impl AlgoDuration {
    /// Serialize to JSON string
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
    /// Deerialize from JSON string
    pub fn from_json(json: &str) -> Self {
        serde_json::from_str(json).unwrap()
    }
}
/// Get current system time milliseconds
pub fn get_current_time_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards ;)")
        .as_millis()
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
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
impl AlphaPPPConfig {
    /// Serialize Alpha+++ parameters to JSON string
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
    /// Deserialize Alpha+++ parameters from JSON string
    pub fn from_json(json: &str) -> Self {
        serde_json::from_str(json).unwrap()
    }
}

///
/// Discover a [`PetriNet`] using the Alpha+++ Process Discovery algorithm
///
/// Additionally returns the durations for performance measurements
///
pub fn alphappp_discover_petri_net(
    log_proj: &EventLogActivityProjection,
    config: AlphaPPPConfig,
) -> (PetriNet, AlgoDuration) {
    alphappp_discover_petri_net_with_timing_fn(log_proj, config, &get_current_time_millis)
}

/// Run Alpha+++ discovery
///
/// Measures [`AlgoDuration`] using the passed `get_time_millis_fn` function
pub fn alphappp_discover_petri_net_with_timing_fn(
    log_proj: &EventLogActivityProjection,
    config: AlphaPPPConfig,
    get_time_millis_fn: &dyn Fn() -> u128,
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
    let total_start = get_time_millis_fn();
    let mut start = get_time_millis_fn();
    let mut log_proj = log_proj.clone();
    add_start_end_acts_proj(&mut log_proj);
    let dfg = ActivityProjectionDFG::from_event_log_projection(&log_proj);
    let dfg_sum: u64 = dfg.edges.values().sum();
    let mean_dfg = dfg_sum as f32 / dfg.edges.len() as f32;

    let start_act = log_proj
        .act_to_index
        .get(START_ACTIVITY)
        .unwrap();
    let end_act = log_proj
        .act_to_index
        .get(END_ACTIVITY)
        .unwrap();
    println!(
        "Adding start/end acts took: {:.4}s",
        get_time_millis_fn() - start
    );
    start = get_time_millis_fn();
    let (log_proj, added_loop) = add_artificial_acts_for_loops(
        &log_proj,
        (config.log_repair_loop_df_thresh_rel * mean_dfg).ceil() as u64,
    );
    algo_dur.loop_repair = (get_time_millis_fn() - start) as f32 / 1000.0;
    println!(
        "Using Loop Log Repair with df_threshold of {}",
        (config.log_repair_loop_df_thresh_rel * mean_dfg).ceil() as u64,
    );
    println!("#Added for loop: {}", added_loop.len());

    start = get_time_millis_fn();
    let (log_proj, added_skip) = add_artificial_acts_for_skips(
        &log_proj,
        (config.log_repair_skip_df_thresh_rel * mean_dfg).ceil() as u64,
    );
    algo_dur.skip_repair = (get_time_millis_fn() - start) as f32 / 1000.0;
    println!("Log Skip/Loop Repair took: {:.4}s", algo_dur.skip_repair);
    start = get_time_millis_fn();

    let mut act_count = vec![0_i128; log_proj.activities.len()];
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
    algo_dur.filter_dfg = (get_time_millis_fn() - start) as f32 / 1000.0;
    println!("Filtering DFG took: {:.4}s", algo_dur.filter_dfg);
    start = get_time_millis_fn();
    let cnds: HashSet<(Vec<usize>, Vec<usize>)> = build_candidates(&dfg);
    println!("Built candidates {}", cnds.len());

    algo_dur.cnd_building = (get_time_millis_fn() - start) as f32 / 1000.0;
    println!("Building candidates took: {:.4}s", algo_dur.cnd_building);
    start = get_time_millis_fn();
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
    algo_dur.prune_cnd = (get_time_millis_fn() - start) as f32 / 1000.0;
    println!("Pruning candidates took: {:.4}s", algo_dur.prune_cnd);
    start = get_time_millis_fn();
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
    algo_dur.build_net = (get_time_millis_fn() - start) as f32 / 1000.0;
    println!("Building PN took: {:.4}s", algo_dur.build_net);

    algo_dur.total = (get_time_millis_fn() - total_start) as f32 / 1000.0;
    println!("\n====\nWhole Discovery took: {:.4}s", algo_dur.total);
    (pn, algo_dur)
}

/// Helper function to transform a place candidate to a list of input and output transition names/label
pub fn cnds_to_names(
    log_proj: &EventLogActivityProjection,
    cnd: &[(Vec<usize>, Vec<usize>)],
) -> Vec<(Vec<String>, Vec<String>)> {
    cnd.iter()
        .map(|(a, b)| (log_proj.acts_to_names(a), log_proj.acts_to_names(b)))
        .collect()
}
