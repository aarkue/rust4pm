use crate::core::{
    event_data::case_centric::utils::activity_projection::EventLogActivityProjection,
    process_models::case_centric::petri_net::{AcceptingPetriNet, PlaceID, TransitionID},
};

use super::full::{discover_alphappp_net, AlphaPPPConfig};
const AUTO_CONFIGS: &[AlphaPPPConfig] = &[
    AlphaPPPConfig {
        balance_thresh: 0.6,
        fitness_thresh: 0.4,
        replay_thresh: 0.0,
        log_repair_skip_df_thresh_rel: 4.0,
        log_repair_loop_df_thresh_rel: 4.0,
        absolute_df_clean_thresh: 1,
        relative_df_clean_thresh: 0.01,
    },
    AlphaPPPConfig {
        balance_thresh: 0.6,
        fitness_thresh: 0.4,
        replay_thresh: 0.0,
        log_repair_skip_df_thresh_rel: 2.0,
        log_repair_loop_df_thresh_rel: 2.0,
        absolute_df_clean_thresh: 1,
        relative_df_clean_thresh: 0.01,
    },
    AlphaPPPConfig {
        balance_thresh: 0.4,
        fitness_thresh: 0.6,
        replay_thresh: 0.0,
        log_repair_skip_df_thresh_rel: 4.0,
        log_repair_loop_df_thresh_rel: 4.0,
        absolute_df_clean_thresh: 1,
        relative_df_clean_thresh: 0.01,
    },
    AlphaPPPConfig {
        balance_thresh: 0.4,
        fitness_thresh: 0.6,
        replay_thresh: 0.0,
        log_repair_skip_df_thresh_rel: 2.0,
        log_repair_loop_df_thresh_rel: 2.0,
        absolute_df_clean_thresh: 1,
        relative_df_clean_thresh: 0.01,
    },
    AlphaPPPConfig {
        balance_thresh: 0.4,
        fitness_thresh: 0.6,
        replay_thresh: 0.0,
        log_repair_skip_df_thresh_rel: 2.0,
        log_repair_loop_df_thresh_rel: 2.0,
        absolute_df_clean_thresh: 5,
        relative_df_clean_thresh: 0.05,
    },
    AlphaPPPConfig {
        balance_thresh: 0.1,
        fitness_thresh: 0.8,
        replay_thresh: 0.0,
        log_repair_skip_df_thresh_rel: 2.0,
        log_repair_loop_df_thresh_rel: 2.0,
        absolute_df_clean_thresh: 5,
        relative_df_clean_thresh: 0.05,
    },
    AlphaPPPConfig {
        balance_thresh: 0.25,
        fitness_thresh: 0.75,
        replay_thresh: 0.0,
        log_repair_skip_df_thresh_rel: 2.0,
        log_repair_loop_df_thresh_rel: 2.0,
        absolute_df_clean_thresh: 25,
        relative_df_clean_thresh: 0.1,
    },
    AlphaPPPConfig {
        balance_thresh: 0.1,
        fitness_thresh: 0.8,
        replay_thresh: 0.0,
        log_repair_skip_df_thresh_rel: 4.0,
        log_repair_loop_df_thresh_rel: 4.0,
        absolute_df_clean_thresh: 1,
        relative_df_clean_thresh: 0.01,
    },
    AlphaPPPConfig {
        balance_thresh: 0.1,
        fitness_thresh: 0.8,
        replay_thresh: 0.0,
        log_repair_skip_df_thresh_rel: 2.0,
        log_repair_loop_df_thresh_rel: 2.0,
        absolute_df_clean_thresh: 1,
        relative_df_clean_thresh: 0.01,
    },
    AlphaPPPConfig {
        balance_thresh: 0.1,
        fitness_thresh: 0.9,
        replay_thresh: 0.0,
        log_repair_skip_df_thresh_rel: 4.0,
        log_repair_loop_df_thresh_rel: 4.0,
        absolute_df_clean_thresh: 1,
        relative_df_clean_thresh: 0.01,
    },
    AlphaPPPConfig {
        balance_thresh: 0.1,
        fitness_thresh: 0.9,
        replay_thresh: 0.0,
        log_repair_skip_df_thresh_rel: 2.0,
        log_repair_loop_df_thresh_rel: 2.0,
        absolute_df_clean_thresh: 1,
        relative_df_clean_thresh: 0.01,
    },
];

/// Automatically select parameters for Alpha+++ and discover a [`AcceptingPetriNet`] using the chosen parameters
///
/// Currently, tests out multiple paramater configurations and selects a best one based on the discovered Petri net
pub fn alphappp_discover_with_auto_parameters(
    log_proj: &EventLogActivityProjection,
) -> (AlphaPPPConfig, AcceptingPetriNet) {
    let mut best: Option<(AlphaPPPConfig, f32, AcceptingPetriNet)> = None;
    for c in AUTO_CONFIGS {
        let pn = discover_alphappp_net(log_proj, *c);
        let score = score_discovered_pn(&pn, c);
        match best {
            Some((_, best_score, _)) => {
                if score > best_score {
                    best = Some((*c, score, pn));
                }
            }
            None => {
                best = Some((*c, score, pn));
            }
        }
    }
    let (best_config, best_score, best_pn) = best.unwrap();
    println!("Best score: {best_score:.2} with config {best_config:?}");
    println!(
        "Resulting net has {} arcs, {} transitions and {} places",
        best_pn.net.arcs().count(),
        best_pn.net.num_transitions(),
        best_pn.net.num_places()
    );
    (best_config, best_pn)
}

fn score_discovered_pn(pn: &AcceptingPetriNet, config: &AlphaPPPConfig) -> f32 {
    let net = &pn.net;
    let is_labelled = |t: TransitionID| net.transition(t).label.is_some();
    // A place connects a transition if it is marked initially or finally, or if a labelled
    // transition sits on its other side
    let marked = |p: PlaceID| pn.initial_marking.tokens(p) > 0 || pn.final_marking.tokens(p) > 0;
    let is_well_connected = |t: TransitionID| {
        is_labelled(t)
            && net
                .preset_of_transition(t)
                .any(|p| marked(p) || net.preset_of_place(p).any(is_labelled))
            && net
                .postset_of_transition(t)
                .any(|p| marked(p) || net.postset_of_place(p).any(is_labelled))
    };
    let num_disconnected_trans = net
        .transitions()
        .filter(|(t, _)| !is_well_connected(*t))
        .count();

    config.fitness_thresh
        * (1.0 - config.balance_thresh)
        * (1.0
            - (num_disconnected_trans as f32
                / net.transitions().filter(|(_, t)| t.label.is_some()).count() as f32))
            .powf(2.0)
}
