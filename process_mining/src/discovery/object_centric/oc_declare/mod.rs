//! Discovering OC-DECLARE Models from Object-Centric Event Data
use std::collections::{HashMap, HashSet, VecDeque};

use binding_macros::register_binding;
use itertools::Itertools;
use rayon::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    conformance::oc_declare::get_for_all_evs_perf_thresh,
    core::{
        event_data::object_centric::linked_ocel::SlimLinkedOCEL,
        process_models::oc_declare::{
            get_activity_object_involvements, get_object_to_object_involvements,
            get_rev_object_to_object_involvements, OCDeclareArc, OCDeclareArcLabel,
            OCDeclareArcType, OCDeclareNode, ObjectInvolvementCounts, ObjectTypeAssociation,
            ALL_OC_DECLARE_ARC_TYPES, EXIT_EVENT_PREFIX, INIT_EVENT_PREFIX,
        },
    },
};

/// O2O Mode for OC-DECLARE Discovery
///
/// Determines to what extent object-to-object (O2O) relationships are considered
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum O2OMode {
    /// O2O relationships are not considered at all
    None,
    /// Direct O2O relationships are considered (i.e., only one direction)
    Direct,
    /// Reverse direction of O2O relationships are considered
    Reversed,
    /// O2O relationships and their inverse directions are considered
    Bidirectional,
}

/// Mode for reducing OC-DECLARE constraints
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
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
    /// Determines if the object involvement of discovered constraints should be made more precise/strict after initial discovery and reduction
    pub refinement: bool,
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
            refinement: false,
            considered_arrow_types: ALL_OC_DECLARE_ARC_TYPES.iter().copied().collect(),
        }
    }
}

/// Discover behavioral OC-DECLARE constraints
#[register_binding(name = "discover_oc-declare")]
pub fn discover_behavior_constraints(
    locel: &SlimLinkedOCEL,
    #[bind(default = Default::default())] options: OCDeclareDiscoveryOptions,
) -> Vec<OCDeclareArc> {
    let act_ob_inv: HashMap<String, HashMap<String, ObjectInvolvementCounts>> =
        get_activity_object_involvements(locel);
    let ob_ob_inv: HashMap<String, HashMap<String, ObjectInvolvementCounts>> =
        get_object_to_object_involvements(locel);
    let ob_ob_rev_inv = get_rev_object_to_object_involvements(locel);
    let direction = OCDeclareArcType::AS;
    let acts_to_use = options
        .acts_to_use
        .clone()
        .unwrap_or_else(|| locel.get_ev_types().cloned().collect());
    let ret = acts_to_use
        .iter()
        .cartesian_product(acts_to_use.iter())
        .par_bridge()
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
            let obj_invs = get_direct_or_indirect_object_involvements(
                act1,
                act2,
                &act_ob_inv,
                &ob_ob_inv,
                &ob_ob_rev_inv,
                options.o2o_mode,
            );
            let act_arcs = get_oi_labels(
                act1,
                act2,
                obj_invs.clone(),
                direction,
                &options.counts_for_generation,
                options.noise_threshold,
                locel,
            );
            let old = combine_constraints(act_arcs, act1, act2, direction, &options, locel, true);
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
        })
        .collect();

    let reduced_ret = match options.reduction {
        OCDeclareReductionMode::None => ret,
        OCDeclareReductionMode::Lossless => reduce_oc_arcs(ret, true),
        OCDeclareReductionMode::Lossy => reduce_oc_arcs(ret, false),
    };
    if options.refinement {
        refine_oc_arcs(
            &reduced_ret,
            &act_ob_inv,
            &ob_ob_inv,
            &ob_ob_rev_inv,
            &options,
            locel,
        )
    } else {
        reduced_ret
    }
}

/// Get possible object involvement labels for given activity pair and object involvements
///
/// Returns the set of viable labels
pub fn get_oi_labels<'a>(
    act1: &'a str,
    act2: &'a str,
    obj_invs: Vec<(ObjectTypeAssociation, bool)>,
    direction: OCDeclareArcType,
    counts_for_generation: &(Option<usize>, Option<usize>),
    noise_threshold: f64,
    locel: &SlimLinkedOCEL,
) -> Vec<OCDeclareArcLabel> {
    let mut ret = Vec::new();
    for (ot, is_multiple) in obj_invs {
        // ANY?
        let any_label = OCDeclareArcLabel {
            each: vec![],
            any: vec![ot],
            all: vec![],
        };
        let sat = get_for_all_evs_perf_thresh(
            act1,
            act2,
            &any_label,
            &direction,
            counts_for_generation,
            locel,
            noise_threshold,
        );
        if sat {
            // It IS a viable candidate!
            // Also test Each/All:
            if is_multiple {
                ret.push(any_label.clone());
                // All is also valid!
                // Next, test Each:
                let each_label = OCDeclareArcLabel {
                    all: vec![],
                    any: vec![],
                    each: any_label.any.clone(),
                };
                let each_sat = get_for_all_evs_perf_thresh(
                    act1,
                    act2,
                    &each_label,
                    &direction,
                    counts_for_generation,
                    locel,
                    noise_threshold,
                );
                if each_sat {
                    // All is also valid!
                    ret.push(each_label);
                    let all_label = OCDeclareArcLabel {
                        all: any_label.any.clone(),
                        any: vec![],
                        each: vec![],
                    };
                    // Otherwise, do not need to bother with differentiating Each/All!
                    let all_sat = get_for_all_evs_perf_thresh(
                        act1,
                        act2,
                        &all_label,
                        &direction,
                        counts_for_generation,
                        locel,
                        noise_threshold,
                    );
                    if all_sat {
                        ret.push(all_label);
                    }
                }
            } else {
                // Not multiple? Then add as each
                let each_label = OCDeclareArcLabel {
                    each: any_label.any.clone(),
                    any: vec![],
                    all: vec![],
                };
                ret.push(each_label);
            }
        }
    }
    ret
}
/// Combine constraints by trying to merge their labels
///
/// Returns the set of combined constraints
///
pub fn combine_constraints<'a>(
    mut act_arcs: Vec<OCDeclareArcLabel>,
    act1: &'a str,
    act2: &'a str,
    direction: OCDeclareArcType,
    options: &OCDeclareDiscoveryOptions,
    locel: &SlimLinkedOCEL,
    iteration_check: bool,
) -> HashSet<OCDeclareArcLabel> {
    let mut changed = true;
    let mut old: HashSet<_> = act_arcs.iter().cloned().collect();
    let mut iteration = 1;
    while changed {
        let x = 0..act_arcs.len();
        let new_res: HashSet<_> = x
            .flat_map(|arc1_i| ((arc1_i + 1)..act_arcs.len()).map(move |arc2_i| (arc1_i, arc2_i)))
            // .par_bridge()
            .filter_map(|(arc1_i, arc2_i)| {
                let arc1 = &act_arcs[arc1_i];
                let arc2 = &act_arcs[arc2_i];
                if arc1.is_dominated_by(arc2) || arc2.is_dominated_by(arc1) {
                    return None;
                }
                let new_arc_label = arc1.combine(arc2);
                let new_n =
                    new_arc_label.all.len() + new_arc_label.any.len() + new_arc_label.each.len();
                if iteration_check && new_n != iteration + 1 {
                    return None;
                }
                let sat = get_for_all_evs_perf_thresh(
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
    let prev_old = old.clone();
    old.retain(|a: &OCDeclareArcLabel| !prev_old.iter().any(|a2| a != a2 && a.is_dominated_by(a2)));
    old
}
/// Try to find stricter constraints for an AS constraint
///
/// e.g., if AS is satisfied, check if EF, DF, EP, DP are also satisfied
fn get_stricter_arrows_for_as(
    mut a: OCDeclareArc,
    options: &OCDeclareDiscoveryOptions,
    locel: &SlimLinkedOCEL,
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

/// Refine OC-DECLARE arcs by trying to find stricter object involvement labels
///
/// Returns a list of refined arcs
pub fn refine_oc_arcs(
    all_arcs: &[OCDeclareArc],
    act_ob_inv: &HashMap<String, HashMap<String, ObjectInvolvementCounts>>,
    ob_ob_inv: &HashMap<String, HashMap<String, ObjectInvolvementCounts>>,
    ob_ob_rev_inv: &HashMap<String, HashMap<String, ObjectInvolvementCounts>>,
    options: &OCDeclareDiscoveryOptions,
    locel: &SlimLinkedOCEL,
) -> Vec<OCDeclareArc> {
    let act_pairs: HashSet<(_, _)> = all_arcs
        .iter()
        .map(|arc| (arc.from.as_str(), arc.to.as_str()))
        .collect();
    act_pairs
        .into_iter()
        // .par_bridge()
        .flat_map(|(act1, act2)| {
            let arcs = all_arcs
                .iter()
                .filter(|arc| arc.from.as_str() == act1 && arc.to.as_str() == act2)
                .cloned()
                .collect_vec();
            let obj_invs = get_direct_or_indirect_object_involvements(
                act1,
                act2,
                act_ob_inv,
                ob_ob_inv,
                ob_ob_rev_inv,
                options.o2o_mode,
            );
            let oi_labels = get_oi_labels(
                act1,
                act2,
                obj_invs.clone(),
                OCDeclareArcType::AS,
                &(Some(1), None),
                options.noise_threshold,
                locel,
            );

            // Try to combine with previous labels
            let mut new_arcs = Vec::new();
            for arc in arcs {
                let mut labels: Vec<_> = oi_labels
                    .iter()
                    .filter_map(|l| {
                        if l.is_dominated_by(&arc.label) {
                            None
                        } else {
                            let combined = l.combine(&arc.label);
                            let sat = get_for_all_evs_perf_thresh(
                                act1,
                                act2,
                                &combined,
                                &arc.arc_type,
                                &options.counts_for_filter,
                                locel,
                                options.noise_threshold,
                            );
                            if sat {
                                Some(combined)
                            } else {
                                None
                            }
                        }
                    })
                    .collect();
                labels.push(arc.label);
                let combined =
                    combine_constraints(labels, act1, act2, arc.arc_type, options, locel, false);
                new_arcs.extend(combined.into_iter().map(|a| OCDeclareArc {
                    from: OCDeclareNode::new(act1),
                    to: OCDeclareNode::new(act2),
                    arc_type: arc.arc_type,
                    label: a,
                    counts: (Some(1), None),
                }));
            }
            new_arcs
        })
        .collect()
}
/// Returns an iterator over different object type associations
///
/// In particular each item (X,b) consists of an `ObjectTypeAssociation` X and a flag b, indicating if multiple objects are sometimes involved in the source (or through the O2O)
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

// /// Reduce OC-DECLARE arcs based on lossless/lossy transitive reduction
// pub fn reduce_oc_arcs(arcs: &Vec<OCDeclareArc>, lossless: bool) -> Vec<OCDeclareArc> {
//     let mut ret: HashSet<_> = arcs.clone().into_iter().collect();

//     for a in arcs {
//         for b in arcs {
//             if a != b
//                 && a.from != a.to
//                 && b.from != b.to
//                 && b.from == a.to
//                 && a.from != b.to
//                 && ret.contains(&a)
//                 && ret.contains(&b)
//             {
//                 ret.retain(|c| {
//                     let remove = c != a
//                         && c != b
//                         && c.from == a.from
//                         && c.to == b.to
//                         && c.arc_type.is_dominated_by_or_eq(&a.arc_type)
//                         && c.arc_type.is_dominated_by_or_eq(&b.arc_type)
//                         && (c.label.is_dominated_by(&a.label) && c.label.is_dominated_by(&b.label));

//                     let bc_any_overlap = c.label.any.iter().any(|any_label| {
//                         let b_is_any = b.label.any.iter().any(|l| l == any_label);
//                         b_is_any
//                     });
//                     if remove && c.from.0 == "W_Shortened completion " && c.to.0 == "O_Refused" {
//                         println!("C:{:?}, A:{:?} B:{:?}\n\n ", c, a, b)
//                     }

//                     !remove || (lossless && bc_any_overlap)
//                 })
//             }
//         }
//     }

//     ret.into_iter().collect()
// }

/// Reduce OC-DECLARE arcs based on lossless/lossy transitive reduction
/// considering paths of arbitrary length.
/// Uses sequential processing to prevent mutual elimination in cycles.
pub fn reduce_oc_arcs(mut arcs: Vec<OCDeclareArc>, lossless: bool) -> Vec<OCDeclareArc> {
    // Sorting ensures deterministic processing order
    arcs.sort();
    // Adjacency list mapping Node Name -> Indices of outgoing arcs
    // Used for efficient traversal
    let mut adj: HashMap<&str, Vec<usize>> = HashMap::new();
    for (i, arc) in arcs.iter().enumerate() {
        adj.entry(arc.from.as_str()).or_default().push(i);
    }

    // Track which arcs are still active and can be used
    let mut active = vec![true; arcs.len()];

    // Process arcs sequentially
    for i in 0..arcs.len() {
        // If an arc is found to be redundant, mark it as inactive
        if has_dominating_path(i, &arcs, &adj, &active, lossless) {
            active[i] = false;
        }
    }

    // Return only the arcs that remained active
    arcs.into_iter()
        .enumerate()
        .filter(|(i, _)| active[*i])
        .map(|(_, arc)| arc.clone())
        .collect()
}

fn has_dominating_path(
    candidate_idx: usize,
    arcs: &[OCDeclareArc],
    adj: &HashMap<&str, Vec<usize>>,
    active: &[bool],
    lossless: bool,
) -> bool {
    let c = &arcs[candidate_idx];

    // Breadth-First Search Queue:
    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();

    queue.push_back((c.from.as_str(), 0));
    visited.insert(c.from.as_str());

    while let Some((curr_node, depth)) = queue.pop_front() {
        // If we reached the target via a path, 'c' is redundant.
        if curr_node == c.to.as_str() {
            return true;
        }

        if let Some(edge_indices) = adj.get(curr_node) {
            for &edge_idx in edge_indices {
                // Only traverse edges that are currently active
                // Do not traverse the candidate itself
                if !active[edge_idx] || edge_idx == candidate_idx {
                    continue;
                }

                let edge = &arcs[edge_idx];

                // Domination criteria check
                let dominated = c.arc_type.is_dominated_by_or_eq(&edge.arc_type)
                    && c.label.is_dominated_by(&edge.label);

                if !dominated {
                    continue;
                }

                // Check lossless criteria (any overlap)
                if lossless && depth >= 1 {
                    let overlap = c
                        .label
                        .any
                        .iter()
                        .any(|any_label| edge.label.any.iter().any(|l| l == any_label));
                    if overlap {
                        continue;
                    }
                }

                // Push reached node and continue search
                if !visited.contains(edge.to.as_str()) {
                    visited.insert(edge.to.as_str());
                    queue.push_back((edge.to.as_str(), depth + 1));
                }
            }
        }
    }

    false
}
