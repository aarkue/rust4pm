//! Object-centric OCEL and OCPT Abstraction for Conformance Checking
use itertools::MultiUnzip;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::ops::{AddAssign, DivAssign};
use uuid::Uuid;

use crate::core::event_data::object_centric::linked_ocel::index_linked_ocel::{
    EventIndex, ObjectIndex,
};
use crate::core::event_data::object_centric::linked_ocel::{IndexLinkedOCEL, LinkedOCELAccess};
use crate::core::process_models::object_centric::ocdfg::object_centric_dfg_struct::OCDirectlyFollowsGraph;
use crate::core::process_models::object_centric::ocpt::object_centric_process_tree_struct::{
    OCPTLeafLabel, OCPTNode, OCPT,
};
use crate::core::process_models::object_centric::ocpt::{EventType, ObjectType};

///
/// An object-centric language abstraction based on:
/// - the start event types,
/// - the end event types,
/// - the directly-follows event types,
/// - related event types,
/// - divergent event types,
/// - convergent event types,
/// - deficient event types,
/// - and optional event types per object type.
///
/// Conformance can be checked between two [`OCLanguageAbstraction`] in a footprint-based manner.
///
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct OCLanguageAbstraction {
    /// The start event types per object type
    pub start_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>>,
    /// The end event types per object type
    pub end_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>>,
    /// The directly-following event types per object type
    pub directly_follows_ev_types_per_ob_type: HashMap<ObjectType, HashSet<(EventType, EventType)>>,
    /// The related event types per object type
    pub related_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>>,
    /// The divergent event types per object type
    pub divergent_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>>,
    /// The convergent event types per object type
    pub convergent_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>>,
    /// The deficient event types per object type
    pub deficient_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>>,
    /// The optional event types per object type
    pub optional_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>>,
}

impl OCLanguageAbstraction {
    ///
    /// Creates an [`OCLanguageAbstraction`] from an [`OCPT`]
    ///
    pub fn create_from_oc_process_tree(ocpt: &OCPT) -> Self {
        // Returns an empty abstraction if the tree is invalid
        if ocpt.is_valid() {
            let node_uuids = ocpt.find_all_node_uuids();

            match &ocpt.root {
                OCPTNode::Operator(op) => {
                    // Information is stored per node
                    let node_map: HashMap<Uuid, HashMap<&EventType, HashSet<&ObjectType>>> =
                        node_uuids
                            .iter()
                            .map(|&uuid| (*uuid, HashMap::new()))
                            .collect();

                    // Recursively compute relatedness
                    let mut related_ev_type_per_ob_type: HashMap<
                        Uuid,
                        HashMap<&EventType, HashSet<&ObjectType>>,
                    > = node_map.clone();
                    op.compute_related(&mut related_ev_type_per_ob_type);

                    // Recursively compute divergence
                    let mut divergent_ev_type_per_ob_type: HashMap<
                        Uuid,
                        HashMap<&EventType, HashSet<&ObjectType>>,
                    > = node_map.clone();
                    op.compute_div(
                        &mut divergent_ev_type_per_ob_type,
                        &related_ev_type_per_ob_type,
                    );

                    // Recursively compute leaf convergence
                    let mut leaf_conv_ob_types_per_node: HashMap<
                        Uuid,
                        HashMap<&EventType, HashSet<&ObjectType>>,
                    > = node_map.clone();
                    op.compute_leaf_conv(&mut leaf_conv_ob_types_per_node);

                    // Recursively compute leaf deficiency
                    let mut leaf_def_ob_types_per_node: HashMap<
                        Uuid,
                        HashMap<&EventType, HashSet<&ObjectType>>,
                    > = node_map.clone();
                    op.compute_leaf_def(&mut leaf_def_ob_types_per_node);

                    // Recursively compute optionality
                    let mut optional_ev_type_per_ob_type: HashMap<
                        Uuid,
                        HashMap<&EventType, HashSet<&ObjectType>>,
                    > = divergent_ev_type_per_ob_type.clone();
                    op.compute_opt(
                        &mut optional_ev_type_per_ob_type,
                        &related_ev_type_per_ob_type,
                    );

                    // Extend convergence information
                    let convergent_ev_type_per_ob_type: HashMap<&EventType, HashSet<&ObjectType>> =
                        op.compute_conv(
                            related_ev_type_per_ob_type.get(&op.uuid).unwrap(),
                            optional_ev_type_per_ob_type.get(&op.uuid).unwrap(),
                            leaf_def_ob_types_per_node.get(&op.uuid).unwrap(),
                            divergent_ev_type_per_ob_type.get(&op.uuid).unwrap(),
                            leaf_conv_ob_types_per_node.get(&op.uuid).unwrap(),
                        );

                    // Extend deficiency information
                    let deficient_ev_type_per_ob_type: HashMap<&EventType, HashSet<&ObjectType>> =
                        op.compute_def(
                            related_ev_type_per_ob_type.get(&op.uuid).unwrap(),
                            optional_ev_type_per_ob_type.get(&op.uuid).unwrap(),
                            leaf_conv_ob_types_per_node.get(&op.uuid).unwrap(),
                            divergent_ev_type_per_ob_type.get(&op.uuid).unwrap(),
                            leaf_def_ob_types_per_node.get(&op.uuid).unwrap(),
                        );

                    // Compute directly-follows information
                    let dfg_per_ob_type = related_ev_type_per_ob_type
                        .get(&op.uuid)
                        .unwrap()
                        .values()
                        .flatten()
                        .collect::<HashSet<_>>()
                        .iter()
                        .map(|&&ob_type| {
                            (
                                ob_type,
                                op.get_directly_follows_relations(
                                    ob_type,
                                    &related_ev_type_per_ob_type,
                                    &divergent_ev_type_per_ob_type,
                                ),
                            )
                        })
                        .collect::<HashMap<&ObjectType, (_, _, _, _)>>();

                    let mut start_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>> =
                        HashMap::new();

                    let mut end_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>> =
                        HashMap::new();

                    let mut directly_follows_ev_types_per_ob_type: HashMap<
                        ObjectType,
                        HashSet<(EventType, EventType)>,
                    > = HashMap::new();

                    // Extract directly-follows information
                    dfg_per_ob_type
                        .iter()
                        .for_each(|(&ob_type, (start_evs, end_evs, dfr, _))| {
                            start_ev_type_per_ob_type.insert(
                                ob_type.clone(),
                                start_evs
                                    .iter()
                                    .map(|&start_ev_type| start_ev_type.clone())
                                    .collect(),
                            );

                            end_ev_type_per_ob_type.insert(
                                ob_type.clone(),
                                end_evs
                                    .iter()
                                    .map(|&end_ev_type| end_ev_type.to_owned())
                                    .collect(),
                            );

                            directly_follows_ev_types_per_ob_type.insert(
                                ob_type.clone(),
                                dfr.iter()
                                    .map(|&(from, to)| (from.clone(), to.clone()))
                                    .collect(),
                            );
                        });

                    let related_ob_type_per_ev_type: HashMap<ObjectType, HashSet<EventType>> =
                        Self::unpack_root_ev_type_per_ob_type(
                            &op.uuid,
                            related_ev_type_per_ob_type,
                        );
                    let ob_types = compute_rel_ob_types(&related_ob_type_per_ev_type);

                    Self {
                        start_ev_type_per_ob_type,
                        end_ev_type_per_ob_type,
                        directly_follows_ev_types_per_ob_type,
                        related_ev_type_per_ob_type: change_ob_type_as_key(
                            related_ob_type_per_ev_type,
                            &ob_types,
                        ),
                        divergent_ev_type_per_ob_type: change_ob_type_as_key(
                            Self::unpack_root_ev_type_per_ob_type(
                                &op.uuid,
                                divergent_ev_type_per_ob_type,
                            ),
                            &ob_types,
                        ),
                        convergent_ev_type_per_ob_type: change_ob_type_as_key(
                            convergent_ev_type_per_ob_type
                                .iter()
                                .map(|(&ob_type, ev_types)| {
                                    (
                                        ob_type.clone(),
                                        ev_types.iter().map(|&ev_type| ev_type.clone()).collect(),
                                    )
                                })
                                .collect(),
                            &ob_types,
                        ),
                        deficient_ev_type_per_ob_type: change_ob_type_as_key(
                            deficient_ev_type_per_ob_type
                                .iter()
                                .map(|(&ob_type, ev_types)| {
                                    (
                                        ob_type.clone(),
                                        ev_types.iter().map(|&ev_type| ev_type.clone()).collect(),
                                    )
                                })
                                .collect(),
                            &ob_types,
                        ),
                        optional_ev_type_per_ob_type: change_ob_type_as_key(
                            Self::unpack_root_ev_type_per_ob_type(
                                &op.uuid,
                                optional_ev_type_per_ob_type,
                            ),
                            &ob_types,
                        ),
                    }
                }

                OCPTNode::Leaf(leaf) => match &leaf.activity_label {
                    OCPTLeafLabel::Activity(label) => {
                        let (
                            start_ev_type_per_ob_type,
                            end_ev_type_per_ob_type,
                            directly_follows_ev_types_per_ob_type,
                            related_ev_type_per_ob_type,
                            divergent_ev_type_per_ob_type,
                            convergent_ev_type_per_ob_type,
                            deficient_ev_type_per_ob_type,
                            optional_ev_type_per_ob_type,
                        ) = leaf
                            .related_ob_types
                            .iter()
                            .map(|ob_type| {
                                let (start_evs, end_evs, dfr, _) =
                                    leaf.get_directly_follows_relations(ob_type);
                                (
                                    (ob_type.clone(), start_evs.into_iter().cloned().collect()),
                                    (ob_type.clone(), end_evs.into_iter().cloned().collect()),
                                    (
                                        ob_type.clone(),
                                        dfr.into_iter()
                                            .map(|(from, to)| (from.clone(), to.clone()))
                                            .collect(),
                                    ),
                                    (ob_type.clone(), HashSet::from([label.clone()])),
                                    (
                                        ob_type.clone(),
                                        if leaf.divergent_ob_types.contains(ob_type) {
                                            HashSet::from([label.clone()])
                                        } else {
                                            HashSet::new()
                                        },
                                    ),
                                    (
                                        ob_type.clone(),
                                        if leaf.convergent_ob_types.contains(ob_type) {
                                            HashSet::from([label.clone()])
                                        } else {
                                            HashSet::new()
                                        },
                                    ),
                                    (
                                        ob_type.clone(),
                                        if leaf.deficient_ob_types.contains(ob_type) {
                                            HashSet::from([label.clone()])
                                        } else {
                                            HashSet::new()
                                        },
                                    ),
                                    (
                                        ob_type.clone(),
                                        if leaf.divergent_ob_types.contains(ob_type) {
                                            HashSet::from([label.clone()])
                                        } else {
                                            HashSet::new()
                                        },
                                    ),
                                )
                            })
                            .multiunzip();

                        Self {
                            start_ev_type_per_ob_type,
                            end_ev_type_per_ob_type,
                            directly_follows_ev_types_per_ob_type,
                            related_ev_type_per_ob_type,
                            divergent_ev_type_per_ob_type,
                            convergent_ev_type_per_ob_type,
                            deficient_ev_type_per_ob_type,
                            optional_ev_type_per_ob_type,
                        }
                    }
                    OCPTLeafLabel::Tau => Self::default(),
                },
            }
        } else {
            Self::default()
        }
    }

    ///
    /// For a given node ID clone the stored information
    ///
    pub fn unpack_root_ev_type_per_ob_type(
        root_uuid: &Uuid,
        ev_type_per_ob_type: HashMap<Uuid, HashMap<&ObjectType, HashSet<&EventType>>>,
    ) -> HashMap<ObjectType, HashSet<EventType>> {
        ev_type_per_ob_type
            .get(root_uuid)
            .unwrap()
            .iter()
            .map(|(&ob_type, ev_types)| {
                (ob_type.clone(), ev_types.iter().cloned().cloned().collect())
            })
            .collect()
    }

    ///
    /// Creates an abstraction from an [`OCEL`](crate::core::event_data::object_centric::OCEL).
    ///
    /// Expects the input [`IndexLinkedOCEL`] to have all orphan [`OCELObjects`](crate::core::event_data::object_centric::OCELObject)
    ///  to be removed, i.e., they should not be contained if they do not have any e2o relation.
    ///
    pub fn create_from_ocel(locel: &IndexLinkedOCEL) -> Self {
        // Computes the directly-follows graphs for all object types
        let directly_follows_graph: OCDirectlyFollowsGraph<'_> =
            OCDirectlyFollowsGraph::create_from_locel(locel);

        // Sets up the result hashmaps
        let mut start_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>> = HashMap::new();
        let mut end_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>> = HashMap::new();
        let mut directly_follows_ev_types_per_ob_type: HashMap<
            ObjectType,
            HashSet<(EventType, EventType)>,
        > = HashMap::new();
        let mut related_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>> =
            HashMap::new();
        let mut divergent_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>> =
            HashMap::new();
        let mut convergent_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>> =
            HashMap::new();
        let mut deficient_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>> =
            HashMap::new();
        let mut optional_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>> =
            HashMap::new();

        // Extracts the DFG information
        locel.get_ob_types().for_each(|ob_type| {
            let ev_type_dfg = directly_follows_graph
                .object_type_to_dfg
                .get(ob_type)
                .unwrap();

            start_ev_type_per_ob_type
                .insert(ob_type.to_string(), ev_type_dfg.start_activities.clone());
            end_ev_type_per_ob_type.insert(ob_type.to_string(), ev_type_dfg.end_activities.clone());

            let ev_type_directly_follows: HashSet<(String, String)> = ev_type_dfg
                .directly_follows_relations
                .keys()
                .map(|(from, to)| (from.to_string(), to.to_string()))
                .collect();
            directly_follows_ev_types_per_ob_type
                .insert(ob_type.to_string(), ev_type_directly_follows);

            let ev_types: HashSet<String> = locel
                .get_ev_types()
                .map(|event_type| event_type.to_string())
                .collect();

            related_ev_type_per_ob_type.insert(ob_type.to_string(), ev_types);
            divergent_ev_type_per_ob_type.insert(ob_type.to_string(), HashSet::new());
            convergent_ev_type_per_ob_type.insert(ob_type.to_string(), HashSet::new());
            deficient_ev_type_per_ob_type.insert(ob_type.to_string(), HashSet::new());
            optional_ev_type_per_ob_type.insert(ob_type.to_string(), HashSet::new());
        });

        locel.get_ev_types().for_each(|ev_type| {
            let ev_type_e2o_relations: HashSet<(&EventIndex, &ObjectIndex)> = locel
                .get_evs_of_type(ev_type)
                .flat_map(|ev_index| {
                    locel
                        .get_e2o(ev_index)
                        .map(move |(_, ob_index)| (ev_index, ob_index))
                })
                .collect();

            locel.get_ob_types().for_each(|ob_type| {
                let ob_type_e2o_relations: HashSet<(&EventIndex, &ObjectIndex)> = locel
                    .get_obs_of_type(ob_type)
                    .flat_map(|ob_index| {
                        locel
                            .get_e2o_rev(ob_index)
                            .map(move |(_, ev_index)| (ev_index, ob_index))
                    })
                    .collect::<HashSet<(&EventIndex, &ObjectIndex)>>();

                let ev_ob_type_e2o_relations: HashSet<&(&EventIndex, &ObjectIndex)> =
                    ob_type_e2o_relations
                        .intersection(&ev_type_e2o_relations)
                        .collect::<HashSet<_>>();

                let unique_ev_count_e2o = ev_type_e2o_relations
                    .iter()
                    .map(|&(ev_index, _)| ev_index)
                    .collect::<HashSet<_>>()
                    .len();

                let unique_ev_count_e2o_rev = ev_ob_type_e2o_relations
                    .iter()
                    .map(|&(ev_index, _)| ev_index)
                    .collect::<HashSet<_>>()
                    .len();

                if unique_ev_count_e2o != unique_ev_count_e2o_rev {
                    if unique_ev_count_e2o_rev == 0 {
                        related_ev_type_per_ob_type
                            .get_mut(ob_type)
                            .unwrap()
                            .remove(&ev_type.to_string());
                    } else if unique_ev_count_e2o_rev < unique_ev_count_e2o {
                        deficient_ev_type_per_ob_type
                            .get_mut(ob_type)
                            .unwrap()
                            .insert(ev_type.to_string());
                    }
                }

                let num_of_obj_with_ob_type = locel.get_obs_of_type(ob_type).count();
                let num_of_obj_with_ob_and_ev_type = ev_ob_type_e2o_relations
                    .iter()
                    .map(|&&(_, ob_index)| ob_index)
                    .collect::<HashSet<_>>()
                    .len();

                if num_of_obj_with_ob_type > num_of_obj_with_ob_and_ev_type
                    && related_ev_type_per_ob_type
                        .get(ob_type)
                        .unwrap()
                        .contains(&ev_type.to_string())
                {
                    optional_ev_type_per_ob_type
                        .get_mut(ob_type)
                        .unwrap()
                        .insert(ev_type.to_string());
                }

                if Self::is_convergent_locel(locel, &ev_ob_type_e2o_relations, ob_type) {
                    convergent_ev_type_per_ob_type
                        .get_mut(ob_type)
                        .unwrap()
                        .insert(ev_type.to_string());
                }

                if Self::is_divergent_locel(locel, &ev_ob_type_e2o_relations, ob_type) {
                    divergent_ev_type_per_ob_type
                        .get_mut(ob_type)
                        .unwrap()
                        .insert(ev_type.to_string());
                }
            });
        });

        Self {
            start_ev_type_per_ob_type,
            end_ev_type_per_ob_type,
            directly_follows_ev_types_per_ob_type,
            related_ev_type_per_ob_type,
            divergent_ev_type_per_ob_type,
            convergent_ev_type_per_ob_type,
            deficient_ev_type_per_ob_type,
            optional_ev_type_per_ob_type,
        }
    }

    ///
    /// Finds an object type to be convergent if there is an event that has an e2o relation to two
    /// objects with the same object type
    ///
    pub fn is_convergent_locel(
        locel: &IndexLinkedOCEL,
        ev_ob_type_e2o_relations: &HashSet<&(&EventIndex, &ObjectIndex)>,
        ob_type: &str,
    ) -> bool {
        let mut object_index_to_event_indices = HashSet::new();

        for &&(ev_index, ob_index) in ev_ob_type_e2o_relations {
            if locel.get_ob(ob_index).object_type.eq(ob_type) {
                if object_index_to_event_indices.contains(&ev_index) {
                    return true;
                }

                object_index_to_event_indices.insert(ev_index);
            }
        }
        false
    }

    ///
    /// An object type is checked to be divergent if an object of the given type is related to
    /// multiple events
    ///
    pub fn is_divergent_locel(
        locel: &IndexLinkedOCEL,
        ev_ob_type_e2o_relations: &HashSet<&(&EventIndex, &ObjectIndex)>,
        ob_type: &str,
    ) -> bool {
        let mut object_index_to_event_indices = HashMap::new();

        ev_ob_type_e2o_relations
            .iter()
            .for_each(|&&(ev_index, ob_index)| {
                object_index_to_event_indices
                    .entry(ob_index)
                    .or_insert_with(HashSet::new)
                    .insert(ev_index);
            });

        for (_, ev_indices) in object_index_to_event_indices {
            if ev_indices.len() > 1 {
                let ob_indices_of_ev_indices = ev_indices
                    .iter()
                    .map(|&ev_index| {
                        locel
                            .get_e2o_set(ev_index)
                            .iter()
                            .filter(|&ob_index| !locel.get_ob(ob_index).object_type.eq(ob_type))
                            .collect::<HashSet<&ObjectIndex>>()
                    })
                    .collect::<Vec<_>>();

                let mut ob_indices_of_ev_indices_iter = ob_indices_of_ev_indices.into_iter();
                let reference_set = ob_indices_of_ev_indices_iter.next().unwrap();

                for curr_set in ob_indices_of_ev_indices_iter {
                    if !reference_set.eq(&curr_set) {
                        return true;
                    }
                }
            }
        }

        false
    }
}

///
/// Reverses a `HashMap<EventType, HashSet<ObjectType>>` to `HashMap<ObjectType, HashSet<EventType>>`
/// and inserts missing object types given in `ob_types`
///
pub fn change_ob_type_as_key(
    ob_type_per_ev_type_mapping: HashMap<EventType, HashSet<ObjectType>>,
    ob_types: &HashSet<ObjectType>,
) -> HashMap<ObjectType, HashSet<EventType>> {
    let mut result: HashMap<ObjectType, HashSet<EventType>> = ob_types
        .iter()
        .map(|ob_type| (ob_type.clone(), HashSet::new()))
        .collect();

    ob_type_per_ev_type_mapping
        .iter()
        .for_each(|(ev_type, ob_types)| {
            ob_types.iter().for_each(|ob_type| {
                result
                    .entry(ob_type.clone())
                    .or_default()
                    .insert(ev_type.clone());
            })
        });

    result
}

///
/// Finds all object types that are related to at least one event type
///
pub fn compute_rel_ob_types(
    related_ob_type_per_ev_type: &HashMap<EventType, HashSet<ObjectType>>,
) -> HashSet<ObjectType> {
    related_ob_type_per_ev_type
        .iter()
        .flat_map(|(_, ob_types)| ob_types.clone())
        .collect()
}

///
/// Computes fitness and precision by comparing the abstractions in a footprint-based manner,
/// weighting the conformance of directly-follows relations and all other properties equally
///
pub fn compute_fitness_precision(
    log_abstraction: &OCLanguageAbstraction,
    model_abstraction: &OCLanguageAbstraction,
) -> (f64, f64) {
    let all_ob_types = log_abstraction
        .related_ev_type_per_ob_type
        .keys()
        .collect::<HashSet<_>>()
        .union(
            &model_abstraction
                .related_ev_type_per_ob_type
                .keys()
                .collect::<HashSet<_>>(),
        )
        .cloned()
        .collect::<HashSet<_>>();

    let all_ev_types: HashSet<&EventType> = log_abstraction
        .directly_follows_ev_types_per_ob_type
        .iter()
        .flat_map(|(_, dfr)| {
            let mut dfr_ev_types: HashSet<&EventType> = HashSet::new();

            dfr.iter().for_each(|(from, to)| {
                dfr_ev_types.insert(from);
                dfr_ev_types.insert(to);
            });

            dfr_ev_types
        })
        .collect::<HashSet<_>>();
    #[allow(clippy::type_complexity)]
    let pattern: Vec<(
        &HashMap<ObjectType, HashSet<EventType>>,
        &HashMap<ObjectType, HashSet<EventType>>,
    )> = vec![
        (
            &log_abstraction.start_ev_type_per_ob_type,
            &model_abstraction.start_ev_type_per_ob_type,
        ),
        (
            &log_abstraction.end_ev_type_per_ob_type,
            &model_abstraction.end_ev_type_per_ob_type,
        ),
        (
            &log_abstraction.related_ev_type_per_ob_type,
            &model_abstraction.related_ev_type_per_ob_type,
        ),
        (
            &log_abstraction.divergent_ev_type_per_ob_type,
            &model_abstraction.divergent_ev_type_per_ob_type,
        ),
        (
            &log_abstraction.convergent_ev_type_per_ob_type,
            &model_abstraction.convergent_ev_type_per_ob_type,
        ),
        (
            &log_abstraction.deficient_ev_type_per_ob_type,
            &model_abstraction.deficient_ev_type_per_ob_type,
        ),
        (
            &log_abstraction.optional_ev_type_per_ob_type,
            &model_abstraction.optional_ev_type_per_ob_type,
        ),
    ];

    let mut fit_per_ev_type = all_ev_types
        .iter()
        .map(|&ev_type| (ev_type, 0.0))
        .collect::<HashMap<_, _>>();
    let mut prec_per_ev_type = fit_per_ev_type.clone();
    let mut valid_fit_ev_type_count = 1.0;
    let mut valid_prec_ev_type_count = 1.0;

    all_ev_types.iter().for_each(|&ev_type| {
        let mut fit_pattern_matching = 0.0;
        let mut prec_pattern_matching = 0.0;

        pattern
            .iter()
            .for_each(|&(log_ev_type_per_ob_type, model_ev_type_per_ob_type)| {
                let mut matches_log = 0.0;
                let mut matches_model = 0.0;
                let mut matches_log_model = 0.0;

                all_ob_types.iter().for_each(|&ob_type| {
                    let in_log: bool = log_ev_type_per_ob_type
                        .get(ob_type)
                        .unwrap_or(&HashSet::new())
                        .contains(ev_type);
                    let in_model: bool = model_ev_type_per_ob_type
                        .get(ob_type)
                        .unwrap_or(&HashSet::new())
                        .contains(ev_type);

                    if in_log && in_model {
                        matches_log += 1.0;
                        matches_model += 1.0;
                        matches_log_model += 1.0;
                    } else if in_log {
                        matches_log += 1.0;
                    } else if in_model {
                        matches_model += 1.0;
                    }
                });

                if matches_log != 0.0 {
                    fit_per_ev_type
                        .get_mut(ev_type)
                        .unwrap()
                        .add_assign(matches_log_model / matches_log);
                    fit_pattern_matching += 1.0;
                }

                if matches_model != 0.0 {
                    prec_per_ev_type
                        .get_mut(ev_type)
                        .unwrap()
                        .add_assign(matches_log_model / matches_model);
                    prec_pattern_matching += 1.0;
                }
            });

        if fit_pattern_matching != 0.0 {
            fit_per_ev_type
                .get_mut(ev_type)
                .unwrap()
                .div_assign(fit_pattern_matching);
            valid_fit_ev_type_count += 1.0;
        }
        if prec_pattern_matching != 0.0 {
            prec_per_ev_type
                .get_mut(ev_type)
                .unwrap()
                .div_assign(prec_pattern_matching);
            valid_prec_ev_type_count += 1.0;
        }
    });

    let mut fit_per_dfr = all_ev_types
        .iter()
        .flat_map(|&from| {
            all_ev_types
                .iter()
                .map(|&to| ((from, to), 0.0))
                .collect::<HashMap<_, _>>()
        })
        .collect::<HashMap<_, _>>();
    let mut prec_per_dfr = fit_per_dfr.clone();

    let mut dfr_matches_fit = 0.0;
    let mut dfr_matches_prec = 0.0;
    all_ev_types.iter().for_each(|&from| {
        all_ev_types.iter().for_each(|&to| {
            let mut matches_log = 0.0;
            let mut matches_model = 0.0;
            let mut matches_log_model = 0.0;

            all_ob_types.iter().for_each(|&ob_type| {
                let in_log: bool = log_abstraction
                    .directly_follows_ev_types_per_ob_type
                    .get(ob_type)
                    .unwrap_or(&HashSet::new())
                    .contains(&(from.to_string(), to.to_string()));
                let in_model: bool = model_abstraction
                    .directly_follows_ev_types_per_ob_type
                    .get(ob_type)
                    .unwrap_or(&HashSet::new())
                    .contains(&(from.to_string(), to.to_string()));

                if in_log && in_model {
                    matches_log += 1.0;
                    matches_model += 1.0;
                    matches_log_model += 1.0;
                } else if in_log {
                    matches_log += 1.0;
                } else if in_model {
                    matches_model += 1.0;
                }
            });

            if matches_log != 0.0 {
                fit_per_dfr
                    .get_mut(&(from, to))
                    .unwrap()
                    .add_assign(matches_log_model / matches_log);
                dfr_matches_fit += 1.0;
            }
            if matches_model != 0.0 {
                prec_per_dfr
                    .get_mut(&(from, to))
                    .unwrap()
                    .add_assign(matches_log_model / matches_model);
                dfr_matches_prec += 1.0;
            }
        })
    });

    let mut fitness_weighted_ev_types = 0.0;
    fit_per_ev_type.iter().for_each(|(_, fit)| {
        fitness_weighted_ev_types += fit;
    });
    if valid_fit_ev_type_count > 0.0 {
        fitness_weighted_ev_types /= valid_fit_ev_type_count;
    }

    let mut fitness_weighted_dfr = 0.0;
    fit_per_dfr.iter().for_each(|(_, fit)| {
        fitness_weighted_dfr += fit;
    });
    if dfr_matches_fit > 0.0 {
        fitness_weighted_dfr /= dfr_matches_fit;
    }

    let mut precision_weighted_ev_types = 0.0;
    prec_per_ev_type.iter().for_each(|(_, prec)| {
        precision_weighted_ev_types += prec;
    });
    if valid_prec_ev_type_count > 0.0 {
        precision_weighted_ev_types /= valid_prec_ev_type_count;
    }

    let mut precision_weighted_dfr = 0.0;
    prec_per_dfr.iter().for_each(|(_, prec)| {
        precision_weighted_dfr += prec;
    });
    if dfr_matches_prec > 0.0 {
        precision_weighted_dfr /= dfr_matches_prec;
    }

    (
        (fitness_weighted_ev_types + fitness_weighted_dfr) / 2.0,
        (precision_weighted_ev_types + precision_weighted_dfr) / 2.0,
    )
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::{
        conformance::object_centric::object_centric_language_abstraction::{
            compute_fitness_precision, OCLanguageAbstraction,
        },
        core::{
            event_data::object_centric::linked_ocel::IndexLinkedOCEL,
            process_models::object_centric::ocpt::object_centric_process_tree_struct::{
                OCPTNode, OCPTOperatorType, OCPT,
            },
            OCEL,
        },
        ocel,
    };

    fn create_test_tree() -> OCPT {
        let mut root_op = OCPTNode::new_operator(OCPTOperatorType::Sequence);

        let mut place: OCPTNode = OCPTNode::new_leaf(Some("place".to_string()));
        place.add_convergent_ob_type(&"i".to_string());
        place.add_divergent_ob_type(&"c".to_string());
        place.add_related_ob_type(&"c".to_string());
        place.add_related_ob_type(&"o".to_string());
        place.add_related_ob_type(&"i".to_string());
        root_op.add_child(place);

        let mut pay_pack_operator = OCPTNode::new_operator(OCPTOperatorType::Concurrency);

        let mut pay: OCPTNode = OCPTNode::new_leaf(Some("pay".to_string()));
        pay.add_convergent_ob_type(&"i".to_string());
        pay.add_divergent_ob_type(&"c".to_string());
        pay.add_related_ob_type(&"c".to_string());
        pay.add_related_ob_type(&"o".to_string());
        pay.add_related_ob_type(&"i".to_string());
        pay_pack_operator.add_child(pay);

        let mut pack: OCPTNode = OCPTNode::new_leaf(Some("pack".to_string()));
        pack.add_convergent_ob_type(&"i".to_string());
        pack.add_divergent_ob_type(&"o".to_string());
        pack.add_divergent_ob_type(&"e".to_string());
        pack.add_related_ob_type(&"o".to_string());
        pack.add_related_ob_type(&"i".to_string());
        pack.add_related_ob_type(&"e".to_string());
        pay_pack_operator.add_child(pack);

        root_op.add_child(pay_pack_operator);

        let mut refund_pickup_operator = OCPTNode::new_operator(OCPTOperatorType::ExclusiveChoice);

        let mut refund: OCPTNode = OCPTNode::new_leaf(Some("refund".to_string()));
        refund.add_convergent_ob_type(&"i".to_string());
        refund.add_divergent_ob_type(&"o".to_string());
        refund.add_divergent_ob_type(&"e".to_string());
        refund.add_related_ob_type(&"o".to_string());
        refund.add_related_ob_type(&"i".to_string());
        refund.add_related_ob_type(&"e".to_string());
        refund_pickup_operator.add_child(refund);

        let mut pickup: OCPTNode = OCPTNode::new_leaf(Some("pickup".to_string()));
        pickup.add_convergent_ob_type(&"i".to_string());
        pickup.add_deficient_ob_type(&"e".to_string());
        pickup.add_divergent_ob_type(&"c".to_string());
        pickup.add_divergent_ob_type(&"e".to_string());
        pickup.add_related_ob_type(&"c".to_string());
        pickup.add_related_ob_type(&"o".to_string());
        pickup.add_related_ob_type(&"i".to_string());
        pickup.add_related_ob_type(&"e".to_string());
        refund_pickup_operator.add_child(pickup);

        root_op.add_child(refund_pickup_operator);

        OCPT::new(root_op)
    }

    fn create_test_ocel() -> OCEL {
        ocel!(
            events:
            ("place", ["c:1", "o:1", "i:1", "i:2"]),
            ("pack", ["o:1", "i:2", "e:1"]),
            ("place", ["c:1", "o:2", "i:3", "i:4"]),
            ("pay", ["c:1", "o:1", "i:1", "i:2"]),
            ("pickup", ["c:1", "o:1", "i:2", "e:1"]),
            ("pay", ["c:1", "o:2", "i:3", "i:4"]),
            ("pack", ["o:1", "o:2", "i:1", "i:3", "e:1"]),
            ("place", ["c:1", "o:3", "i:5"]),
            ("pack", ["o:2", "i:4", "e:1", "e:2"]),
            ("pickup", ["c:1", "o:2", "i:4", "e:1"]),
            ("pay", ["c:1", "o:3", "i:5"]),
            ("pack", ["o:3", "i:5", "e:1"]),
            ("pickup", ["c:1", "o:1", "o:2", "i:1", "i:3"]),
            ("refund", ["o:3", "i:5", "e:1"]),
            o2o:
        )
    }

    #[test]
    fn test_fitness_precision_computation() {
        let tree = create_test_tree();
        let ocel = create_test_ocel();
        let preprocessed_ocel = ocel.remove_orphan_objects();
        let locel = IndexLinkedOCEL::from_ocel(preprocessed_ocel);

        let time_start = Instant::now();
        let abstraction_tree = OCLanguageAbstraction::create_from_oc_process_tree(&tree);
        let abstraction_log = OCLanguageAbstraction::create_from_ocel(&locel);

        let (fitness, precision) = compute_fitness_precision(&abstraction_log, &abstraction_tree);
        let time_elapsed = time_start.elapsed().as_millis();
        println!("Time elapsed is {time_elapsed}ms");
        println!("Fitness: {fitness}");
        println!("Precision: {precision}");
    }
}
