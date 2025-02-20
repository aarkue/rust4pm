use crate::{
    event_log::activity_projection::EventLogActivityProjection,
    petri_net::petri_net_struct::{PetriNet, Transition},
};

use super::full::{alphappp_discover_petri_net, AlphaPPPConfig};
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

/// Automatically select parameters for Alpha+++ and discover a [`PetriNet`] using the chosen parameters
///
/// Currently, tests out multiple paramater configurations and selects a best one based on the discovered Petri net
pub fn alphappp_discover_with_auto_parameters(
    log_proj: &EventLogActivityProjection,
) -> (AlphaPPPConfig, PetriNet) {
    let mut best: Option<(AlphaPPPConfig, f32, PetriNet)> = None;
    for c in AUTO_CONFIGS {
        let (pn, _) = alphappp_discover_petri_net(log_proj, *c);
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
    println!(
        "Best score: {:.2} with config {:?}",
        best_score, best_config
    );
    println!(
        "Resulting net has {} arcs, {} transitions and {} places",
        best_pn.arcs.len(),
        best_pn.transitions.len(),
        best_pn.places.len()
    );
    (best_config, best_pn)
}

fn score_discovered_pn(pn: &PetriNet, config: &AlphaPPPConfig) -> f32 {
    fn is_transition_well_connected(pn: &PetriNet, t: &Transition) -> bool {
        if t.label.is_some() {
            let preset_connected = pn
                .preset_of_transition(t.into())
                .into_iter()
                .filter(|p| {
                    pn.is_in_initial_marking(p)
                        || pn.is_in_a_final_marking(p)
                        || pn
                            .preset_of_place(*p)
                            .into_iter()
                            .filter(|ot_id| {
                                pn.transitions
                                    .get(&ot_id.get_uuid())
                                    .unwrap()
                                    .label
                                    .is_some()
                            })
                            .count()
                            >= 1
                })
                .count()
                >= 1;

            let postset_connected = pn
                .postset_of_transition(t.into())
                .into_iter()
                .filter(|p| {
                    pn.is_in_initial_marking(p)
                        || pn.is_in_a_final_marking(p)
                        || pn
                            .postset_of_place(*p)
                            .into_iter()
                            .filter(|ot_id| {
                                pn.transitions
                                    .get(&ot_id.get_uuid())
                                    .unwrap()
                                    .label
                                    .is_some()
                            })
                            .count()
                            >= 1
                })
                .count()
                >= 1;

            preset_connected && postset_connected
        } else {
            false
        }
    }
    let num_disconnected_trans = pn
        .transitions
        .clone()
        .into_iter()
        .filter(|(_, t)| !is_transition_well_connected(pn, t))
        .count();

    config.fitness_thresh
        * (1.0 - config.balance_thresh)
        * (1.0
            - (num_disconnected_trans as f32
                / pn.transitions
                    .iter()
                    .filter(|(_, t)| t.label.is_some())
                    .count() as f32))
            .powf(2.0)
}
