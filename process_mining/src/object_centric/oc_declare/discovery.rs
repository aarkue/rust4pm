use std::collections::{HashMap, HashSet};

// use indicatif::ParallelProgressIterator;
use crate::{
    object_centric::oc_declare::ALL_OC_DECLARE_ARC_TYPES, ocel::linked_ocel::IndexLinkedOCEL,
};
use itertools::Itertools;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use super::{
    get_activity_object_involvements, get_object_to_object_involvements,
    get_rev_object_to_object_involvements, perf, OCDeclareArc, OCDeclareArcLabel, OCDeclareArcType,
    OCDeclareNode, ObjectInvolvementCounts, ObjectTypeAssociation, EXIT_EVENT_PREFIX,
    INIT_EVENT_PREFIX,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

/// Mode for reducing OC-DECLARE constraints
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OCDeclareReductionMode {
    /// Do not reduce constraints at all
    None,
    /// Apply lossless reduction
    ///
    /// i.e., only removes constraints strictly implied by combining others
    Lossless,
    /// Apply lossy reduction
    ///
    /// May also remove constraints which are not implied by others
    Lossy,
}

/// Options for the automatic discovery of OC-DECLARE constraints
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// If/how the discovered constraints should be reduced
    pub reduction: OCDeclareReductionMode,
    /// The arrow types to consider when deriving the final constraints
    ///
    /// Should be non empty!
    pub considered_arrow_types: HashSet<OCDeclareArcType>,
}
impl Default for OCDeclareDiscoveryOptions {
    fn default() -> Self {
        Self {
            noise_threshold: 0.2,
            o2o_mode: O2OMode::None,
            acts_to_use: None,
            counts_for_generation: (Some(1), None),
            counts_for_filter: (Some(1), Some(20)),
            reduction: OCDeclareReductionMode::None,
            considered_arrow_types: ALL_OC_DECLARE_ARC_TYPES.iter().copied().collect(),
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
    let direction = OCDeclareArcType::AS;
    let acts_to_use = options
        .acts_to_use
        .clone()
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
                        // Also test Each/All:
                        if is_multiple {
                            act_arcs.push(any_label.clone());
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
                        } else {
                            // Not multiple? Then add as each
                            let each_label = OCDeclareArcLabel {
                                each: any_label.any.clone(),
                                any: vec![],
                                all: vec![],
                            };
                            act_arcs.push(each_label);
                            // act_arcs.push(any_label);
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
                    .flat_map(|label| {
                        let mut arc = OCDeclareArc {
                            from: OCDeclareNode::new(act1.clone()),
                            to: OCDeclareNode::new(act2.clone()),
                            arc_type: OCDeclareArcType::AS,
                            label,
                            counts: options.counts_for_filter,
                        };
                        if arc.get_for_all_evs_perf_thresh(locel, options.noise_threshold) {
                            arc.counts.1 = None;
                            get_stricter_arrows_for_as(arc, &options, locel)
                        } else {
                            vec![]
                        }
                    });
                v
            }),
    );

    // println!("Got {}", ret.len());

    // println!("Reduced to {}", reduced_ret.len());
    match options.reduction {
        OCDeclareReductionMode::None => ret,
        OCDeclareReductionMode::Lossless => reduce_oc_arcs(&ret, true),
        OCDeclareReductionMode::Lossy => reduce_oc_arcs(&ret, false),
    }
}

fn get_stricter_arrows_for_as(
    mut a: OCDeclareArc,
    options: &OCDeclareDiscoveryOptions,
    locel: &IndexLinkedOCEL,
) -> Vec<OCDeclareArc> {
    let mut ret: Vec<OCDeclareArc> = Vec::new();
    if options
        .considered_arrow_types
        .contains(&OCDeclareArcType::EF)
    {
        // Test EF
        a.arc_type = OCDeclareArcType::EF;
        if a.get_for_all_evs_perf_thresh(locel, options.noise_threshold) {
            // Test DF
            a.arc_type = OCDeclareArcType::DF;
            if options
                .considered_arrow_types
                .contains(&OCDeclareArcType::DF)
                && a.get_for_all_evs_perf_thresh(locel, options.noise_threshold)
            {
                ret.push(a.clone());
            } else {
                a.arc_type = OCDeclareArcType::EF;
                ret.push(a.clone());
            }
        }
    } else if options
        .considered_arrow_types
        .contains(&OCDeclareArcType::DF)
    {
        a.arc_type = OCDeclareArcType::DF;

        if a.get_for_all_evs_perf_thresh(locel, options.noise_threshold) {
            ret.push(a.clone());
        }
    }

    if options
        .considered_arrow_types
        .contains(&OCDeclareArcType::EP)
    {
        // Test EP
        a.arc_type = OCDeclareArcType::EP;
        if a.get_for_all_evs_perf_thresh(locel, options.noise_threshold) {
            // Test DP
            a.arc_type = OCDeclareArcType::DP;
            if options
                .considered_arrow_types
                .contains(&OCDeclareArcType::DP)
                && a.get_for_all_evs_perf_thresh(locel, options.noise_threshold)
            {
                ret.push(a.clone());
            } else {
                a.arc_type = OCDeclareArcType::EP;
                ret.push(a.clone());
            }
        }
    } else if options
        .considered_arrow_types
        .contains(&OCDeclareArcType::DP)
    {
        a.arc_type = OCDeclareArcType::DP;

        if a.get_for_all_evs_perf_thresh(locel, options.noise_threshold) {
            ret.push(a.clone());
        }
    }

    if ret.is_empty()
        && options
            .considered_arrow_types
            .contains(&OCDeclareArcType::AS)
        && a.from != a.to
    {
        a.arc_type = OCDeclareArcType::AS;
        if a.get_for_all_evs_perf_thresh(locel, options.noise_threshold) {
            ret.push(a);
        }
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

/// Reduce OC-DECLARE arcs based on lossless/lossy transitive reduction
///
pub fn reduce_oc_arcs(arcs: &Vec<OCDeclareArc>, lossless: bool) -> Vec<OCDeclareArc> {
    let mut ret = arcs.clone();

    for a in arcs {
        for b in arcs {
            if a.from != a.to && b.from == a.to && a.from != b.to {
                ret.retain(|c| {
                    let remove = c.from == a.from
                        && c.to == b.to
                        && c.arc_type.is_dominated_by_or_eq(&a.arc_type)
                        && c.arc_type.is_dominated_by_or_eq(&b.arc_type)
                        && (c.label.is_dominated_by(&a.label) && c.label.is_dominated_by(&b.label));

                    let bc_any_overlap = c.label.any.iter().any(|any_label| {
                        let b_is_any = b.label.any.iter().any(|l| l == any_label);
                        b_is_any
                    });

                    !remove || (lossless && bc_any_overlap)
                })
            }
        }
    }

    ret
}
