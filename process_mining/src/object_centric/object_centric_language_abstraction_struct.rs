use crate::object_centric::object_centric_dfg_struct::OCDirectlyFollowsGraph;
use crate::object_centric::object_centric_process_tree_struct::{
    EventType, OCLeafLabel, OCProcessTree, OCProcessTreeNode, ObjectType,
};
use crate::ocel::linked_ocel::index_linked_ocel::{EventIndex, ObjectIndex};
use crate::ocel::linked_ocel::{IndexLinkedOCEL, LinkedOCELAccess};
use crate::OCEL;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::ops::{AddAssign, DivAssign};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct OCLanguageAbstraction {
    start_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>>,
    end_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>>,
    directly_follows_ev_types_per_ob_type: HashMap<ObjectType, HashSet<(EventType, EventType)>>,
    related_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>>,
    divergent_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>>,
    convergent_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>>,
    deficient_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>>,
    optional_ev_type_per_ob_type: HashMap<ObjectType, HashSet<EventType>>,
}

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
                    .or_insert(HashSet::new())
                    .insert(ev_type.clone());
            })
        });

    result
}

pub fn compute_rel_ob_types(
    related_ob_type_per_ev_type: &HashMap<EventType, HashSet<ObjectType>>,
) -> HashSet<ObjectType> {
    related_ob_type_per_ev_type
        .iter()
        .flat_map(|(_, ob_types)| ob_types.clone())
        .collect()
}

impl OCLanguageAbstraction {
    pub fn create_from_oc_process_tree(ocpt: &OCProcessTree) -> Self {
        if ocpt.is_valid() {
            let node_uuids = ocpt.find_all_node_uuids();

            match &ocpt.root {
                OCProcessTreeNode::Operator(op) => {
                    let mut related_ev_type_per_ob_type: HashMap<
                        Uuid,
                        HashMap<&EventType, HashSet<&ObjectType>>,
                    > = node_uuids
                        .iter()
                        .map(|&uuid| (uuid.clone(), HashMap::new()))
                        .collect();
                    op.compute_related(&mut related_ev_type_per_ob_type);

                    let mut divergent_ev_type_per_ob_type: HashMap<
                        Uuid,
                        HashMap<&EventType, HashSet<&ObjectType>>,
                    > = node_uuids
                        .iter()
                        .map(|&uuid| (uuid.clone(), HashMap::new()))
                        .collect();
                    op.compute_div(
                        &mut divergent_ev_type_per_ob_type,
                        &related_ev_type_per_ob_type,
                    );

                    let mut leaf_conv_ob_types_per_node: HashMap<
                        Uuid,
                        HashMap<&EventType, HashSet<&ObjectType>>,
                    > = node_uuids
                        .iter()
                        .map(|&uuid| (uuid.clone(), HashMap::new()))
                        .collect();
                    op.compute_leaf_conv(&mut leaf_conv_ob_types_per_node);

                    let mut leaf_def_ob_types_per_node: HashMap<
                        Uuid,
                        HashMap<&EventType, HashSet<&ObjectType>>,
                    > = node_uuids
                        .iter()
                        .map(|&uuid| (uuid.clone(), HashMap::new()))
                        .collect();
                    op.compute_leaf_def(&mut leaf_def_ob_types_per_node);

                    let mut optional_ev_type_per_ob_type: HashMap<
                        Uuid,
                        HashMap<&EventType, HashSet<&ObjectType>>,
                    > = divergent_ev_type_per_ob_type.clone();
                    op.compute_opt(
                        &mut optional_ev_type_per_ob_type,
                        &related_ev_type_per_ob_type,
                        false,
                    );

                    let convergent_ev_type_per_ob_type: HashMap<&EventType, HashSet<&ObjectType>> =
                        op.compute_conv(
                            &related_ev_type_per_ob_type.get(&op.uuid).unwrap(),
                            &optional_ev_type_per_ob_type.get(&op.uuid).unwrap(),
                            &leaf_def_ob_types_per_node.get(&op.uuid).unwrap(),
                            &divergent_ev_type_per_ob_type.get(&op.uuid).unwrap(),
                            &leaf_conv_ob_types_per_node.get(&op.uuid).unwrap(),
                        );

                    let mut deficient_ev_type_per_ob_type: HashMap<
                        &EventType,
                        HashSet<&ObjectType>,
                    > = op.compute_def(
                        &related_ev_type_per_ob_type.get(&op.uuid).unwrap(),
                        &optional_ev_type_per_ob_type.get(&op.uuid).unwrap(),
                        &leaf_conv_ob_types_per_node.get(&op.uuid).unwrap(),
                        &divergent_ev_type_per_ob_type.get(&op.uuid).unwrap(),
                        &leaf_def_ob_types_per_node.get(&op.uuid).unwrap(),
                    );

                    let dfg_per_ob_type = related_ev_type_per_ob_type
                        .get(&op.uuid)
                        .unwrap()
                        .values()
                        .flat_map(|ob_types| ob_types)
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

                OCProcessTreeNode::Leaf(leaf) => match &leaf.activity_label {
                    OCLeafLabel::TreeActivity(label) => {
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
                                    (
                                        ob_type.clone(),
                                        start_evs
                                            .iter()
                                            .map(|&start_ev| start_ev.clone())
                                            .collect(),
                                    ),
                                    (
                                        ob_type.clone(),
                                        end_evs.iter().map(|&end_ev| end_ev.clone()).collect(),
                                    ),
                                    (
                                        ob_type.clone(),
                                        dfr.iter()
                                            .map(|&(from, to)| (from.clone(), to.clone()))
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
                            .collect::<(
                                HashMap<_, _>,
                                HashMap<_, _>,
                                HashMap<_, _>,
                                HashMap<_, _>,
                                HashMap<_, _>,
                                HashMap<_, _>,
                                HashMap<_, _>,
                                HashMap<_, _>,
                            )>();

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
                    OCLeafLabel::TreeTau => Self {
                        start_ev_type_per_ob_type: HashMap::new(),
                        end_ev_type_per_ob_type: HashMap::new(),
                        directly_follows_ev_types_per_ob_type: HashMap::new(),
                        related_ev_type_per_ob_type: HashMap::new(),
                        divergent_ev_type_per_ob_type: HashMap::new(),
                        convergent_ev_type_per_ob_type: HashMap::new(),
                        deficient_ev_type_per_ob_type: HashMap::new(),
                        optional_ev_type_per_ob_type: HashMap::new(),
                    },
                },
            }
        } else {
            Self {
                start_ev_type_per_ob_type: HashMap::new(),
                end_ev_type_per_ob_type: HashMap::new(),
                directly_follows_ev_types_per_ob_type: HashMap::new(),
                related_ev_type_per_ob_type: HashMap::new(),
                divergent_ev_type_per_ob_type: HashMap::new(),
                convergent_ev_type_per_ob_type: HashMap::new(),
                deficient_ev_type_per_ob_type: HashMap::new(),
                optional_ev_type_per_ob_type: HashMap::new(),
            }
        }
    }

    pub fn unpack_root_ev_type_per_ob_type(
        root_uuid: &Uuid,
        ev_type_per_ob_type: HashMap<Uuid, HashMap<&ObjectType, HashSet<&EventType>>>,
    ) -> HashMap<ObjectType, HashSet<EventType>> {
        ev_type_per_ob_type
            .get(root_uuid)
            .unwrap()
            .iter()
            .map(|(&ob_type, ev_types)| {
                (
                    ob_type.clone(),
                    ev_types.iter().map(|&ev_type| ev_type.clone()).collect(),
                )
            })
            .collect()
    }

    pub fn create_from_ocel(ocel: &OCEL) -> Self {
        let mut locel: IndexLinkedOCEL = IndexLinkedOCEL::from(ocel.clone());

        // Todo: Filter objects without e2o

        let objects_with_e2o = locel
            .e2o_rev_et
            .iter()
            .flat_map(|(_, o2e_set)| o2e_set.keys().cloned())
            .collect::<HashSet<_>>();

        let underlying_ocel = locel.get_ocel_mut();

        underlying_ocel.objects = underlying_ocel
            .objects
            .iter()
            .enumerate()
            .filter_map(
                |(index, obj)| match objects_with_e2o.contains(&ObjectIndex(index)) {
                    true => Some(obj.clone()),
                    false => None,
                },
            )
            .collect::<Vec<_>>();

        let locel = IndexLinkedOCEL::from(underlying_ocel.clone());

        let directly_follows_graph: OCDirectlyFollowsGraph =
            OCDirectlyFollowsGraph::create_from_locel(&locel);

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

            related_ev_type_per_ob_type.insert(ob_type.to_string(), ev_types.clone());
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

                // let ev_ob_type_e2o_relations = ev_type_e2o_relations
                //     .iter()
                //     .filter(|&(ev_index, ob_index)| {
                //         locel.get_ob(ob_index).object_type.eq(&ob_type.to_string())
                //     }).collect::<HashSet<_>>();

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

                if Self::is_convergent_locel(&locel, &ev_ob_type_e2o_relations, ob_type) {
                    convergent_ev_type_per_ob_type
                        .get_mut(ob_type)
                        .unwrap()
                        .insert(ev_type.to_string());
                }

                if Self::is_divergent_locel(&locel, &ev_ob_type_e2o_relations, ob_type) {
                    divergent_ev_type_per_ob_type
                        .get_mut(ob_type)
                        .unwrap()
                        .insert(ev_type.to_string());
                }
            });
        });

        let mut debug: HashMap<EventType, HashSet<ObjectType>> = HashMap::new();
        optional_ev_type_per_ob_type
            .iter()
            .for_each(|(ob_type, ev_types)| {
                ev_types.iter().for_each(|ev_type| {
                    debug
                        .entry(ev_type.to_string())
                        .or_insert_with(HashSet::new)
                        .insert(ob_type.to_string());
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

    pub fn is_divergent_locel(
        locel: &IndexLinkedOCEL,
        ev_ob_type_e2o_relations: &HashSet<&(&EventIndex, &ObjectIndex)>,
        ob_type: &str,
    ) -> bool {
        let mut object_index_to_event_indices = HashMap::new();

        ev_ob_type_e2o_relations
            .iter()
            .for_each(|&(ev_index, ob_index)| {
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
}
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
        .map(|&ob_type| ob_type)
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
    use crate::{
        object_centric::object_centric_language_abstraction_struct::HashSet,
        object_centric::object_centric_language_abstraction_struct::compute_fitness_precision,
        object_centric::object_centric_language_abstraction_struct::OCLanguageAbstraction,
        object_centric::object_centric_process_tree_struct::OCOperatorType,
        object_centric::object_centric_process_tree_struct::OCProcessTree,
        object_centric::object_centric_process_tree_struct::OCProcessTreeNode,
        ocel::ocel_struct::OCELEvent,
        ocel::ocel_struct::OCELObject,
        ocel::ocel_struct::OCELRelationship,
        ocel::ocel_struct::OCELType,
        utils::test_utils::get_test_data_path,
        import_ocel_json_from_slice,
        ocel,
        OCEL
    };
    use chrono::{TimeDelta, TimeZone, Utc};
    use std::fs::File;
    use std::io::Read;
    use std::ops::AddAssign;
    use std::time::Instant;

    #[test]
    fn test_log_abstraction() {
        let time_start_2 = Instant::now();
        let path = get_test_data_path()
            .join("ocel")
            .join("01_ocel_standard_p2p.json");
        let mut log_bytes = Vec::new();
        File::open(&path)
            .unwrap()
            .read_to_end(&mut log_bytes)
            .unwrap();

        let ocel = import_ocel_json_from_slice(&log_bytes);

        let time_start = Instant::now();
        let abstraction = OCLanguageAbstraction::create_from_ocel(&ocel.unwrap());
        let time_elapsed = time_start.elapsed().as_nanos();
        println!("Time elapsed is {}ns", time_elapsed);
        let time_elapsed_2 = time_start_2.elapsed().as_nanos();
        println!("Time elapsed is {}ns", time_elapsed_2);

        println!("{:?}", abstraction);
        println!("{:?}", abstraction);
    }

    fn create_test_tree() -> OCProcessTree {
        let mut root_op = OCProcessTreeNode::new_operator(OCOperatorType::Sequence);

        let mut leaf_1: OCProcessTreeNode =
            OCProcessTreeNode::new_leaf(Some("Create Purchase Requisition".to_string()));
        leaf_1.add_convergent_ob_type(&"material".to_string());
        leaf_1.add_related_ob_type(&"material".to_string());
        leaf_1.add_related_ob_type(&"purchase_requisition".to_string());
        root_op.add_child(leaf_1);

        let mut operator_2 = OCProcessTreeNode::new_operator(OCOperatorType::ExclusiveChoice);

        let mut leaf_2_1 =
            OCProcessTreeNode::new_leaf(Some("Approve Purchase Requisition".to_string()));
        leaf_2_1.add_convergent_ob_type(&"material".to_string());
        leaf_2_1.add_related_ob_type(&"material".to_string());
        leaf_2_1.add_related_ob_type(&"purchase_requisition".to_string());
        operator_2.add_child(leaf_2_1);

        let mut leaf_2_2 =
            OCProcessTreeNode::new_leaf(Some("Delegate Purchase Requisition Approval".to_string()));
        leaf_2_2.add_convergent_ob_type(&"material".to_string());
        leaf_2_2.add_related_ob_type(&"material".to_string());
        leaf_2_2.add_related_ob_type(&"purchase_requisition".to_string());
        operator_2.add_child(leaf_2_2);
        root_op.add_child(operator_2);

        let mut leaf_3 =
            OCProcessTreeNode::new_leaf(Some("Create Request for Quotation".to_string()));
        leaf_3.add_related_ob_type(&"purchase_requisition".to_string());
        leaf_3.add_related_ob_type(&"quotation".to_string());
        root_op.add_child(leaf_3);

        let mut leaf_4 = OCProcessTreeNode::new_leaf(Some("Create Purchase Order".to_string()));
        leaf_4.add_divergent_ob_type(&"quotation".to_string());
        leaf_4.add_related_ob_type(&"purchase_order".to_string());
        leaf_4.add_related_ob_type(&"quotation".to_string());
        root_op.add_child(leaf_4);

        let mut leaf_5 = OCProcessTreeNode::new_leaf(Some("Approve Purchase Order".to_string()));
        leaf_5.add_divergent_ob_type(&"quotation".to_string());
        leaf_5.add_related_ob_type(&"purchase_order".to_string());
        leaf_5.add_related_ob_type(&"quotation".to_string());
        root_op.add_child(leaf_5);

        let mut operator_6 = OCProcessTreeNode::new_operator(OCOperatorType::Loop(None));
        let mut operator_6_1 = OCProcessTreeNode::new_operator(OCOperatorType::ExclusiveChoice);

        let mut leaf_6_1_1 = OCProcessTreeNode::new_leaf(Some("Execute Payment".to_string()));
        leaf_6_1_1.add_convergent_ob_type(&"goods receipt".to_string());
        leaf_6_1_1.add_convergent_ob_type(&"purchase_order".to_string());
        leaf_6_1_1.add_related_ob_type(&"goods receipt".to_string());
        leaf_6_1_1.add_related_ob_type(&"purchase_order".to_string());
        leaf_6_1_1.add_related_ob_type(&"payment".to_string());
        leaf_6_1_1.add_related_ob_type(&"invoice receipt".to_string());
        operator_6_1.add_child(leaf_6_1_1);

        let mut leaf_6_1_2 =
            OCProcessTreeNode::new_leaf(Some("Create Invoice Receipt".to_string()));
        leaf_6_1_2.add_divergent_ob_type(&"invoice receipt".to_string());
        leaf_6_1_2.add_related_ob_type(&"goods receipt".to_string());
        leaf_6_1_2.add_related_ob_type(&"invoice receipt".to_string());
        operator_6_1.add_child(leaf_6_1_2);

        let mut leaf_6_1_3 = OCProcessTreeNode::new_leaf(Some("Create Goods Receipt".to_string()));
        leaf_6_1_3.add_divergent_ob_type(&"goods receipt".to_string());
        leaf_6_1_3.add_divergent_ob_type(&"purchase_order".to_string());
        leaf_6_1_3.add_related_ob_type(&"goods receipt".to_string());
        leaf_6_1_3.add_related_ob_type(&"purchase_order".to_string());
        operator_6_1.add_child(leaf_6_1_3);

        let mut leaf_6_1_4 = OCProcessTreeNode::new_leaf(Some("Perform Two-Way Match".to_string()));
        leaf_6_1_4.add_divergent_ob_type(&"invoice receipt".to_string());
        leaf_6_1_4.add_related_ob_type(&"goods receipt".to_string());
        leaf_6_1_4.add_related_ob_type(&"invoice receipt".to_string());
        operator_6_1.add_child(leaf_6_1_4);

        operator_6.add_child(operator_6_1);

        let all_ob_types = vec![
            "goods receipt",
            "invoice receipt",
            "material",
            "payment",
            "purchase_order",
            "purchase_requisition",
            "quotation",
        ];
        let mut leaf_6_2 = OCProcessTreeNode::new_leaf(None);
        all_ob_types.iter().for_each(|ob_type| {
            leaf_6_2.add_convergent_ob_type(&ob_type.to_string());
            leaf_6_2.add_deficient_ob_type(&ob_type.to_string());
            leaf_6_2.add_divergent_ob_type(&ob_type.to_string());
            leaf_6_2.add_related_ob_type(&ob_type.to_string());
        });

        operator_6.add_child(leaf_6_2);

        root_op.add_child(operator_6);

        OCProcessTree::new(root_op)
    }

    fn create_example_tree() -> OCProcessTree {
        let mut root_op = OCProcessTreeNode::new_operator(OCOperatorType::Sequence);

        let mut place: OCProcessTreeNode = OCProcessTreeNode::new_leaf(Some("place".to_string()));
        place.add_convergent_ob_type(&"i".to_string());
        place.add_divergent_ob_type(&"c".to_string());
        place.add_related_ob_type(&"c".to_string());
        place.add_related_ob_type(&"o".to_string());
        place.add_related_ob_type(&"i".to_string());
        root_op.add_child(place);

        let mut pay_pack_operator = OCProcessTreeNode::new_operator(OCOperatorType::Concurrency);

        let mut pay: OCProcessTreeNode = OCProcessTreeNode::new_leaf(Some("pay".to_string()));
        pay.add_convergent_ob_type(&"i".to_string());
        pay.add_divergent_ob_type(&"c".to_string());
        pay.add_related_ob_type(&"c".to_string());
        pay.add_related_ob_type(&"o".to_string());
        pay.add_related_ob_type(&"i".to_string());
        pay_pack_operator.add_child(pay);

        let mut pack: OCProcessTreeNode = OCProcessTreeNode::new_leaf(Some("pack".to_string()));
        pack.add_convergent_ob_type(&"i".to_string());
        pack.add_divergent_ob_type(&"o".to_string());
        pack.add_divergent_ob_type(&"e".to_string());
        pack.add_related_ob_type(&"o".to_string());
        pack.add_related_ob_type(&"i".to_string());
        pack.add_related_ob_type(&"e".to_string());
        pay_pack_operator.add_child(pack);

        root_op.add_child(pay_pack_operator);

        let mut refund_pickup_operator =
            OCProcessTreeNode::new_operator(OCOperatorType::ExclusiveChoice);

        let mut refund: OCProcessTreeNode = OCProcessTreeNode::new_leaf(Some("refund".to_string()));
        refund.add_convergent_ob_type(&"i".to_string());
        refund.add_divergent_ob_type(&"o".to_string());
        refund.add_divergent_ob_type(&"e".to_string());
        refund.add_related_ob_type(&"o".to_string());
        refund.add_related_ob_type(&"i".to_string());
        refund.add_related_ob_type(&"e".to_string());
        refund_pickup_operator.add_child(refund);

        let mut pickup: OCProcessTreeNode = OCProcessTreeNode::new_leaf(Some("pickup".to_string()));
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

        OCProcessTree::new(root_op)
    }

    fn create_example_ocel() -> OCEL {
        ocel!(
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
            ("refund", ["o:3", "i:5", "e:1"])
        )
    }

    #[test]
    fn test_tree_score_computation() {
        let tree = create_test_tree();

        let path = get_test_data_path()
            .join("ocel")
            .join("01_ocel_standard_p2p.json");
        let mut log_bytes = Vec::new();
        File::open(&path)
            .unwrap()
            .read_to_end(&mut log_bytes)
            .unwrap();

        let ocel = import_ocel_json_from_slice(&log_bytes);

        let time_start = Instant::now();
        let abstraction_log = OCLanguageAbstraction::create_from_ocel(&ocel.unwrap());
        let abstraction_tree = OCLanguageAbstraction::create_from_oc_process_tree(&tree);

        let (fitness, precision) = compute_fitness_precision(&abstraction_log, &abstraction_tree);
        let time_elapsed = time_start.elapsed().as_millis();
        println!("Time elapsed is {}ms", time_elapsed);
        println!("Fitness: {}", fitness);
        println!("Precision: {}", precision);
    }

    #[test]
    fn compute_example_fitness_precision() {
        let tree = create_example_tree();
        let ocel = create_example_ocel();

        let time_start = Instant::now();
        let abstraction_tree = OCLanguageAbstraction::create_from_oc_process_tree(&tree);
        let abstraction_log = OCLanguageAbstraction::create_from_ocel(&ocel);

        let (fitness, precision) = compute_fitness_precision(&abstraction_log, &abstraction_tree);
        let time_elapsed = time_start.elapsed().as_millis();
        println!("Time elapsed is {}ms", time_elapsed);
        println!("Fitness: {}", fitness);
        println!("Precision: {}", precision);
    }
}
