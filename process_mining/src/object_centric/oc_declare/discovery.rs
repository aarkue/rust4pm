use std::collections::{HashMap, HashSet};

// use indicatif::ParallelProgressIterator;
use crate::ocel::linked_ocel::IndexLinkedOCEL;
use itertools::Itertools;
use rayon::prelude::*;

use super::{
    get_activity_object_involvements, get_object_to_object_involvements,
    get_rev_object_to_object_involvements, perf, OCDeclareArc, OCDeclareArcLabel, OCDeclareArcType,
    OCDeclareNode, ObjectInvolvementCounts, ObjectTypeAssociation, EXIT_EVENT_PREFIX,
    INIT_EVENT_PREFIX,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// O2O Mode for OC-DECLARE Discovery
///
/// Determines in what extend object-to-object (O2O) relationships are considered
pub enum O2OMode {
    /// O2O relationships are not considered at all
    None,
    /// Direct O2O relationships are considered (i.e., only one direction)
    Direct,
    /// Reverse direction of O2O relationships are considered
    Reversed,
    /// O2O relationships and there inverse directions are considered
    Bidirectional,
}

/// Options for the automatic discovery of OC-DECLARE constraints
#[derive(Debug, Clone, PartialEq)]
pub struct OCDeclareDiscoveryOptions {
    /// Noise threshold (i.e., what fraction of events are allowed to violate a discovered constraint)
    pub noise_threshold: f64,
    /// Determines if/how object-to-object relationships are considered
    pub o2o_mode: O2OMode,
    /// Activities to use for the discovery. If this is `None`, all activities of the OCEL are used
    pub acts_to_use: Option<Vec<String>>,
    /// What min/max counts to use for the candidate generation steps
    pub counts_for_generation: (Option<usize>, Option<usize>),
    /// What min/max counts to use for the candidate filtering step (when the arrow type is determined)
    pub counts_for_filter: (Option<usize>, Option<usize>),
}
impl Default for OCDeclareDiscoveryOptions {
    fn default() -> Self {
        Self {
            noise_threshold: 0.2,
            o2o_mode: O2OMode::None,
            acts_to_use: None,
            counts_for_generation: (Some(1), None),
            counts_for_filter: (Some(1), Some(20)),
        }
    }
}

/// Discover behavioral OC-DECLARE constraints
pub fn discover_behavior_constraints(
    locel: &IndexLinkedOCEL,
    options: OCDeclareDiscoveryOptions,
) -> Vec<OCDeclareArc> {
    let mut ret = Vec::new();
    let act_ob_inv: HashMap<String, HashMap<String, ObjectInvolvementCounts>> =
        get_activity_object_involvements(locel);
    let ob_ob_inv: HashMap<String, HashMap<String, ObjectInvolvementCounts>> =
        get_object_to_object_involvements(locel);
    let ob_ob_rev_inv = get_rev_object_to_object_involvements(locel);

    // First type of discovery: How many events of a specific type per object of specified type?
    // for ot in locel.get_ob_types() {
    //     // Only consider activities generally involved with objects of a type
    //     let mut ev_types_per_ob: HashMap<&str, Vec<usize>> = act_ob_inv
    //         .iter()
    //         .filter_map(|(act_name, ob_inv)| {
    //             if act_name.starts_with(INIT_EVENT_PREFIX)
    //                 || act_name.starts_with(EXIT_EVENT_PREFIX)
    //             {
    //                 return None;
    //             }
    //             if let Some(oi) = ob_inv.get(ot) {
    //                 return Some((act_name.as_str(), Vec::new()));
    //             }
    //             None
    //         })
    //         .collect();
    //     for ob in locel.get_obs_of_type(ot) {
    //         let ev_types = locel
    //             .get_e2o_rev(ob)
    //             .map(|(_q, e)| &locel.get_ev(e).event_type)
    //             .collect_vec();
    //         ev_types_per_ob.iter_mut().for_each(|(et, counts)| {
    //             counts.push(ev_types.iter().filter(|et2| *et2 == et).count());
    //         });
    //     }
    //     // Now decide on bounds
    //     for (act, counts) in ev_types_per_ob {
    //         // Start with mean
    //         let mean = counts.iter().sum::<usize>() as f64 / counts.len() as f64;
    //         if mean >= 20.0 {
    //             // Probably not interesting (i.e., resource related, grows with log)
    //             continue;
    //         }
    //         let mut n_min = mean.round() as usize;
    //         let mut n_max = n_min;
    //         let min_fitting_len = (counts.len() as f64 * (1.0 - noise_thresh)).ceil() as usize;
    //         while counts
    //             .iter()
    //             .filter(|c| c >= &&n_min && c <= &&n_max)
    //             .count()
    //             < min_fitting_len
    //         {
    //             n_min = if n_min <= 0 { n_min } else { n_min - 1 };
    //             n_max += 1;
    //         }
    //         if n_min == 0 {
    //             // Oftentimes this is just infrequent behavior
    //             continue;
    //         }
    //         if n_max >= 20 {
    //             // Probably not interesting (i.e., resource related, grows with log)
    //             continue;
    //         }
    //         // Got bounds!
    //         // println!("[{ot}] {act}: {n_min} - {n_max} (starting from {mean})");
    //         ret.push(OCDeclareArc {
    //             from: OCDeclareNode::new_ob_init(ot),
    //             to: OCDeclareNode::new_act(act),
    //             arc_type: OCDeclareArcType::ASS,
    //             label: OCDeclareArcLabel {
    //                 each: Vec::default(),
    //                 any: vec![ObjectTypeAssociation::new_simple(ot)],
    //                 all: Vec::default(),
    //             },
    //             counts: (Some(n_min), Some(n_max)),
    //         });
    //     }
    // }

    // Second type of discovery: How many objects of object type per event of specified activity/event type?
    // TODO

    // Third type of discovery: Eventually-follows
    //
    let direction = OCDeclareArcType::AS;
    let acts_to_use = options
        .acts_to_use
        .unwrap_or_else(|| locel.events_per_type.keys().cloned().collect());
    ret.par_extend(
        acts_to_use
            .iter()
            .cartesian_product(acts_to_use.iter())
            .par_bridge()
            // .progress_count(locel.events_per_type.len() as u64 * locel.events_per_type.len() as u64)
            .filter(|(act1, act2)| {
                if act1.starts_with(INIT_EVENT_PREFIX)
                    || act1.starts_with(EXIT_EVENT_PREFIX)
                    || act2.starts_with(INIT_EVENT_PREFIX)
                    || act2.starts_with(EXIT_EVENT_PREFIX)
                {
                    return false;
                }
                true
            })
            .flat_map(|(act1, act2)| {
                let mut act_arcs = Vec::new();
                let obj_invs = get_direct_or_indirect_object_involvements(
                    act1,
                    act2,
                    &act_ob_inv,
                    &ob_ob_inv,
                    &ob_ob_rev_inv,
                    options.o2o_mode,
                );
                for (ot, is_multiple) in obj_invs {
                    // ANY?
                    let any_label = OCDeclareArcLabel {
                        each: vec![],
                        any: vec![ot],
                        all: vec![],
                    };
                    let sat = perf::get_for_all_evs_perf_thresh(
                        act1,
                        act2,
                        &any_label,
                        &direction,
                        &options.counts_for_generation,
                        locel,
                        options.noise_threshold,
                    );
                    if sat {
                        // It IS a viable candidate!
                        act_arcs.push(any_label.clone());
                        // Also test Each/All:
                        if is_multiple && sat {
                            // All is also valid!
                            // Next, test Each:
                            let each_label = OCDeclareArcLabel {
                                all: vec![],
                                any: vec![],
                                each: any_label.any.clone(),
                            };
                            let each_sat = perf::get_for_all_evs_perf_thresh(
                                act1,
                                act2,
                                &each_label,
                                &direction,
                                &options.counts_for_generation,
                                locel,
                                options.noise_threshold,
                            );
                            if each_sat {
                                // All is also valid!
                                // act_arcs.push(all_label);
                                act_arcs.push(each_label);
                                let all_label = OCDeclareArcLabel {
                                    all: any_label.any.clone(),
                                    any: vec![],
                                    each: vec![],
                                };
                                // Otherwise, do not need to bother with differentiating Each/All!
                                let all_sat = perf::get_for_all_evs_perf_thresh(
                                    act1,
                                    act2,
                                    &all_label,
                                    &direction,
                                    &options.counts_for_generation,
                                    locel,
                                    options.noise_threshold,
                                );
                                if all_sat {
                                    act_arcs.push(all_label);
                                }
                            }
                        }
                    }
                }
                let mut changed = true;
                let mut old: HashSet<_> = act_arcs.iter().cloned().collect();
                let mut iteration = 1;
                while changed {
                    // println!("{}->{}, |act_arcs|={}",act1,act2,act_arcs.len());
                    let x = 0..act_arcs.len();
                    let new_res: HashSet<_> = x
                        .flat_map(|arc1_i| {
                            ((arc1_i + 1)..act_arcs.len()).map(move |arc2_i| (arc1_i, arc2_i))
                        })
                        .par_bridge()
                        .filter_map(|(arc1_i, arc2_i)| {
                            let arc1 = &act_arcs[arc1_i];
                            let arc2 = &act_arcs[arc2_i];
                            if arc1.is_dominated_by(arc2) || arc2.is_dominated_by(arc1) {
                                return None;
                            }
                            let new_arc_label = arc1.combine(arc2);
                            let new_n = new_arc_label.all.len()
                                + new_arc_label.any.len()
                                + new_arc_label.each.len();
                            if new_n != iteration + 1 {
                                return None;
                            }
                            let sat = perf::get_for_all_evs_perf_thresh(
                                act1,
                                act2,
                                &new_arc_label,
                                &direction,
                                &options.counts_for_generation,
                                locel,
                                options.noise_threshold,
                            );
                            if sat {
                                Some(new_arc_label)
                            } else {
                                None
                            }
                        })
                        .collect();

                    changed = !new_res.is_empty();
                    old.retain(|a: &OCDeclareArcLabel| {
                        !new_res.iter().any(|a2| a != a2 && a.is_dominated_by(a2))
                    });
                    old.extend(new_res.clone().into_iter());
                    act_arcs = new_res
                        .iter()
                        .filter(|a| !new_res.iter().any(|a2| *a != a2 && a.is_dominated_by(a2)))
                        .cloned()
                        .collect();
                    iteration += 1;
                }
                let v = old
                    .clone()
                    // .into_iter()
                    .into_par_iter()
                    .filter(move |arc1| {
                        !old.iter()
                            .any(|arc2| *arc1 != *arc2 && arc1.is_dominated_by(arc2))
                    })
                    .flat_map(move |label| {
                        let mut arc = OCDeclareArc {
                            from: OCDeclareNode::new(act1.clone()),
                            to: OCDeclareNode::new(act2.clone()),
                            arc_type: OCDeclareArcType::AS,
                            label,
                            counts: options.counts_for_filter,
                        };
                        if arc.get_for_all_evs_perf_thresh(locel, options.noise_threshold) {
                            arc.counts.1 = None;
                            get_stricter_arrows_for_as(arc, options.noise_threshold, locel)
                        } else {
                            vec![]
                        }
                    });
                v
            }),
    );

    ret
}

fn get_stricter_arrows_for_as(
    mut a: OCDeclareArc,
    noise_thresh: f64,
    locel: &IndexLinkedOCEL,
) -> Vec<OCDeclareArc> {
    let mut ret: Vec<OCDeclareArc> = Vec::new();
    {
        // Test EF
        a.arc_type = OCDeclareArcType::EF;
        if a.get_for_all_evs_perf_thresh(locel, noise_thresh) {
            // Test DF
            a.arc_type = OCDeclareArcType::DF;
            // let df_viol_frac = a.get_for_all_evs_perf(locel);
            if a.get_for_all_evs_perf_thresh(locel, noise_thresh) {
                ret.push(a.clone());
            } else {
                a.arc_type = OCDeclareArcType::EF;
                ret.push(a.clone());
            }
        }
    }
    {
        // Test EP
        a.arc_type = OCDeclareArcType::EP;
        // let ep_viol_frac = a.get_for_all_evs_perf(locel);
        if a.get_for_all_evs_perf_thresh(locel, noise_thresh) {
            // Test DFREV
            a.arc_type = OCDeclareArcType::DP;
            // let dp_viol_frac = a.get_for_all_evs_perf(locel);
            if a.get_for_all_evs_perf_thresh(locel, noise_thresh) {
                ret.push(a.clone());
            } else {
                a.arc_type = OCDeclareArcType::EP;
                ret.push(a.clone());
            }
        }
    }
    if ret.is_empty() && a.from != a.to {
        a.arc_type = OCDeclareArcType::AS;
        // if a.get_for_all_evs_perf_thresh(locel, noise_thresh) {
        ret.push(a);
        // }
    }
    ret
}

/// Returns an iterator over different object type associations
/// in particular each item (X,b) consists of an `ObjectTypeAssociation` X and a flag b, indicating if multiple objects are sometimes involved in the source (or through the O2O)
fn get_direct_or_indirect_object_involvements<'a>(
    act1: &'a str,
    act2: &'a str,
    act_ob_involvement: &'a HashMap<String, HashMap<String, ObjectInvolvementCounts>>,
    obj_obj_involvement: &'a HashMap<String, HashMap<String, ObjectInvolvementCounts>>,
    rev_obj_obj_involvement: &'a HashMap<String, HashMap<String, ObjectInvolvementCounts>>,
    o2o_mode: O2OMode,
) -> Vec<(ObjectTypeAssociation, bool)> {
    let act1_obs: HashSet<_> = act_ob_involvement.get(act1).unwrap().keys().collect();
    let act2_obs: HashSet<_> = act_ob_involvement.get(act2).unwrap().keys().collect();
    let mut res = act1_obs
        .iter()
        .filter(|ot| act2_obs.contains(*ot))
        .map(|ot| {
            (
                ObjectTypeAssociation::new_simple(*ot),
                act_ob_involvement.get(act1).unwrap().get(*ot).unwrap().max > 1,
            )
        })
        .collect_vec();
    if o2o_mode == O2OMode::Direct || o2o_mode == O2OMode::Bidirectional {
        res.extend(act1_obs.iter().flat_map(|ot| {
            obj_obj_involvement
                .get(*ot)
                .into_iter()
                .flat_map(|ots2| {
                    ots2.iter()
                        .filter(|(ot2, _)| act2_obs.contains(ot2))
                        // .filter(|(ot2, _)| *ot == "customers" && *ot2 == "employees")
                        .map(|(ot2, oi)| {
                            (
                                ot,
                                ot2,
                                oi.max > 1
                                    || act_ob_involvement.get(act1).unwrap().get(*ot).unwrap().max
                                        > 1,
                            )
                        })
                })
                .map(|(ot1, ot2, multiple)| (ObjectTypeAssociation::new_o2o(*ot1, ot2), multiple))
                .collect_vec()
        }));
    }
    if o2o_mode == O2OMode::Reversed || o2o_mode == O2OMode::Bidirectional {
        res.extend(act1_obs.iter().flat_map(|ot| {
            rev_obj_obj_involvement
                .get(*ot)
                .into_iter()
                .flat_map(|ots2| {
                    ots2.iter()
                        .filter(|(ot2, _)| act2_obs.contains(ot2))
                        .map(|(ot2, oi)| {
                            (
                                ot,
                                ot2,
                                oi.max > 1
                                    || act_ob_involvement.get(act1).unwrap().get(*ot).unwrap().max
                                        > 1,
                            )
                        })
                })
                .map(|(ot1, ot2, multiple)| {
                    (ObjectTypeAssociation::new_o2o_rev(*ot1, ot2), multiple)
                })
                .collect_vec()
        }));
    }
    res
}
