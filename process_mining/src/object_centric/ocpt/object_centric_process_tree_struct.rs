use crate::object_centric::ocpt::{
    add_all_dfr_from_to_alphabets, compute_shuffle_dfr_language, EventType, ObjectType,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

///
/// Leaf in an object-centric process tree
///
#[derive(Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum OCPTLeafLabel {
    /// Non-silent activity leaf
    Activity(EventType),
    /// Silent activity leaf
    Tau,
}

///
/// Node in an object-centric process tree
///
#[derive(Debug, Serialize, Deserialize)]
pub enum OCPTNode {
    /// Operator node of an object-centric process tree
    Operator(OCPTOperator),
    /// Leaf node of an object-centric process tree
    Leaf(OCPTLeaf),
}

impl OCPTNode {
    ///
    /// Returns the identifier of a node in an object-centric process tree
    ///
    pub fn get_uuid(&self) -> &Uuid {
        match self {
            OCPTNode::Operator(op) => &op.uuid,
            OCPTNode::Leaf(leaf) => &leaf.uuid,
        }
    }

    ///
    /// Creates a new operator with the given operator type
    ///
    pub fn new_operator(op_type: OCPTOperatorType) -> Self {
        OCPTNode::Operator(OCPTOperator::new(op_type))
    }

    ///
    /// Creates a new (non-silent) leaf
    ///
    pub fn new_leaf(leaf_label: Option<EventType>) -> Self {
        OCPTNode::Leaf(OCPTLeaf::new(leaf_label))
    }

    ///
    /// Adds a node as child if the node is an operator node
    ///
    pub fn add_child(&mut self, child: OCPTNode) {
        match self {
            OCPTNode::Operator(op) => {
                op.children.push(child);
            }
            OCPTNode::Leaf(_) => {
                panic!("Cannot add child to a leaf")
            }
        }
    }

    ///
    /// Returns `true` if a loop operator has at least two children or if all other operators
    /// have at least one child.
    ///
    pub fn check_children_valid(&self) -> bool {
        match self {
            OCPTNode::Operator(op) => match op.operator_type {
                OCPTOperatorType::Loop(_) => op.children.len() >= 2,
                _ => !op.children.is_empty(),
            },
            OCPTNode::Leaf(_) => true,
        }
    }

    ///
    /// Adds an object type to be convergent.
    /// If the node is a leaf, it gets directly added.
    /// If it as an operator, it is propagated to its descendants.
    ///
    pub fn add_convergent_ob_type(&mut self, ob_type: &ObjectType) {
        match self {
            OCPTNode::Operator(op) => {
                op.children
                    .iter_mut()
                    .for_each(|child| child.add_convergent_ob_type(ob_type));
            }
            OCPTNode::Leaf(ref mut leaf) => {
                leaf.convergent_ob_types.insert(ob_type.to_string());
            }
        }
    }

    ///
    /// Adds an object type to be deficient.
    /// If the node is a leaf, it gets directly added.
    /// If it as an operator, it is propagated to its descendants.
    ///
    pub fn add_deficient_ob_type(&mut self, ob_type: &ObjectType) {
        match self {
            OCPTNode::Operator(op) => {
                op.children
                    .iter_mut()
                    .for_each(|child| child.add_deficient_ob_type(ob_type));
            }
            OCPTNode::Leaf(ref mut leaf) => {
                leaf.deficient_ob_types.insert(ob_type.to_string());
            }
        }
    }

    ///
    /// Adds an object type to be divergent.
    /// If the node is a leaf, it gets directly added.
    /// If it as an operator, it is propagated to its descendants.
    ///
    pub fn add_divergent_ob_type(&mut self, ob_type: &ObjectType) {
        match self {
            OCPTNode::Operator(op) => {
                op.children
                    .iter_mut()
                    .for_each(|child| child.add_divergent_ob_type(ob_type));
            }
            OCPTNode::Leaf(ref mut leaf) => {
                leaf.divergent_ob_types.insert(ob_type.to_string());
            }
        }
    }

    ///
    /// Adds an object type to be related.
    /// If the node is a leaf, it gets directly added.
    /// If it as an operator, it is propagated to its descendants.
    ///
    pub fn add_related_ob_type(&mut self, ob_type: &ObjectType) {
        match self {
            OCPTNode::Operator(op) => {
                op.children
                    .iter_mut()
                    .for_each(|child| child.add_related_ob_type(ob_type));
            }
            OCPTNode::Leaf(ref mut leaf) => {
                leaf.related_ob_types.insert(ob_type.to_string());
            }
        }
    }

    ///
    /// Returns `true` if all event types for the given object type are either unrelated or divergent
    ///
    fn check_unrelated_or_divergent(
        &self,
        ob_type: &ObjectType,
        rel_ob_types_per_node: &HashMap<Uuid, HashMap<&EventType, HashSet<&ObjectType>>>,
        div_ob_types_per_node: &HashMap<Uuid, HashMap<&EventType, HashSet<&ObjectType>>>,
    ) -> bool {
        // Retrieve the related and diverging object types per event type
        let rel_ob_types_per_ev_type = rel_ob_types_per_node.get(self.get_uuid()).unwrap();
        let div_ob_types_per_ev_type = div_ob_types_per_node.get(self.get_uuid()).unwrap();

        let mut result = true;

        // For all related event types, check if all are divergent.
        // Otherwise, change the result value to be false.
        rel_ob_types_per_ev_type
            .iter()
            .for_each(|(&ev_type, ob_types)| {
                if ob_types.contains(ob_type) {
                    if !div_ob_types_per_ev_type
                        .get(ev_type)
                        .unwrap()
                        .contains(ob_type)
                    {
                        result = false;
                    }
                }
            });

        result
    }
}

///
/// Operator type enum for [`OCPTOperator`]
///
#[derive(Debug, Serialize, Deserialize)]
pub enum OCPTOperatorType {
    /// Sequence operator
    Sequence,
    /// Exclusive choice operator
    ExclusiveChoice,
    /// Concurrency operator
    Concurrency,
    /// Loop operator that, if given, restricts a given number of repetitions
    Loop(Option<u32>),
}

///
/// Object-centric process tree struct that contains [`OCPTNode`] as root
///
#[derive(Debug, Serialize)]
pub struct OCPT {
    /// The root of the object-centric process tree
    pub root: OCPTNode,
}

impl OCPT {
    ///
    /// Initializes the object-centric process tree with the given node as root
    ///
    pub fn new(root: OCPTNode) -> Self {
        Self { root }
    }

    ///
    /// Returns `true` if all nodes have the right number of children, if all operators have
    /// eventually descendants that are leaves, and if the tree is acyclic.
    ///
    pub fn is_valid(&self) -> bool {
        if !self.root.check_children_valid() {
            return false;
        }
        // Set up the iteration through the object-centric process tree
        let mut prev_ocpt_node_ids: HashSet<Uuid> = HashSet::new();
        let mut curr_ocpt_node_ids: HashSet<Uuid> = HashSet::new();
        curr_ocpt_node_ids.insert(*self.root.get_uuid());

        let mut curr_operators: Vec<&OCPTOperator> = Vec::new();
        match &self.root {
            OCPTNode::Operator(op) => {
                curr_operators.push(op);
            }
            OCPTNode::Leaf(_) => {}
        };

        // A child counter to check the tree to be acyclic
        let mut children_count: usize = 1;

        // Checking all nodes to have the right number of children
        let mut all_op_nodes_valid = true;

        // Iterate through the tree to count up the children, if a node is the child of many
        // operator nodes, the count computed here and the number of nodes in the process tree
        // disagree
        let mut next_operators = Vec::new();
        while !prev_ocpt_node_ids.eq(&curr_ocpt_node_ids) {
            curr_operators.iter().for_each(|op| {
                op.children.iter().for_each(|child| match child {
                    OCPTNode::Operator(op) => {
                        all_op_nodes_valid &= child.check_children_valid();

                        next_operators.push(op);
                        children_count += 1;
                        curr_ocpt_node_ids.insert(op.uuid);
                    }
                    OCPTNode::Leaf(leaf) => {
                        children_count += 1;
                        curr_ocpt_node_ids.insert(leaf.uuid);
                    }
                })
            });

            curr_operators = next_operators;
            next_operators = Vec::new();
            prev_ocpt_node_ids = curr_ocpt_node_ids.clone();
        }

        all_op_nodes_valid && (children_count == curr_ocpt_node_ids.len())
    }

    ///
    /// Returns all descendant [`OCPTLeaf`]
    ///
    pub fn find_all_leaves(&self) -> Vec<&OCPTLeaf> {
        let mut result: Vec<&OCPTLeaf> = Vec::new();

        let mut curr_operators: Vec<&OCPTOperator> = Vec::new();
        match &self.root {
            OCPTNode::Operator(op) => curr_operators.push(op),
            OCPTNode::Leaf(leaf) => result.push(leaf),
        };

        let mut next_operators = Vec::new();

        while !curr_operators.is_empty() {
            curr_operators.iter().for_each(|op| {
                op.children.iter().for_each(|child| match child {
                    OCPTNode::Operator(op) => next_operators.push(op),
                    OCPTNode::Leaf(leaf) => result.push(leaf),
                })
            });

            curr_operators = next_operators;
            next_operators = Vec::new();
        }

        result
    }

    ///
    /// Returns all `Uuid` of all [`OCPTOperator`] in the tree
    ///
    pub fn find_all_node_uuids(&self) -> Vec<&Uuid> {
        let mut result: Vec<&Uuid> = Vec::new();

        let mut curr_operators: Vec<&OCPTOperator> = Vec::new();
        match &self.root {
            OCPTNode::Operator(op) => {
                curr_operators.push(op);
                result.push(&op.uuid);
            }
            OCPTNode::Leaf(leaf) => {
                result.push(&leaf.uuid);
            }
        };

        let mut next_operators = Vec::new();

        while !curr_operators.is_empty() {
            curr_operators.iter().for_each(|op| {
                op.children.iter().for_each(|child| match child {
                    OCPTNode::Operator(op) => {
                        next_operators.push(op);
                        result.push(&op.uuid);
                    }
                    OCPTNode::Leaf(leaf) => result.push(&leaf.uuid),
                })
            });

            curr_operators = next_operators;
            next_operators = Vec::new();
        }

        result
    }
}

///
/// An operator node in an object-centric process tree
///
#[derive(Debug, Serialize, Deserialize)]
pub struct OCPTOperator {
    /// The node ID
    pub uuid: Uuid,
    /// The [`OCPTOperatorType`] of the tree itself
    pub operator_type: OCPTOperatorType,
    /// The children nodes of the operator node
    pub children: Vec<OCPTNode>,
}

impl OCPTOperator {
    ///
    /// A constructor for the struct that initializes with the given [`OCPTOperatorType`] and
    /// otherwise a fresh [`Uuid`] and an empty list of children
    ///
    pub fn new(operator_type: OCPTOperatorType) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            operator_type,
            children: Vec::new(),
        }
    }

    ///
    /// Returns all descendant [`OCPTNode`]'s Uuids
    ///
    pub fn find_all_descendants_uuids(&self) -> Vec<&Uuid> {
        let mut result: Vec<&Uuid> = Vec::new();

        self.children.iter().for_each(|child| match child {
            OCPTNode::Operator(op) => {
                result.push(child.get_uuid());
                op.find_all_descendants_uuids().iter().for_each(|&uuid| {
                    result.push(uuid);
                });
            }
            OCPTNode::Leaf(_) => {
                result.push(child.get_uuid());
            }
        });

        result
    }

    ///
    /// Recursively, finds the directly follows relations of an object-centric (sub)tree towards
    /// a given object type. Therefore, divergence and unrelatedness are considered to identify
    /// parts of the object-centric process tree that can be skipped.
    ///
    /// Returns all start [`EventType`], all end [`EventType`], and each directly follows relation
    /// of the type ([`EventType`], [`EventType`]) as `HashSet`s for the given [`ObjectType`]
    ///
    pub fn get_directly_follows_relations<'a>(
        &'a self,
        ob_type: &ObjectType,
        rel_ob_types_per_node: &HashMap<Uuid, HashMap<&'a EventType, HashSet<&ObjectType>>>,
        div_ob_types_per_node: &HashMap<Uuid, HashMap<&EventType, HashSet<&ObjectType>>>,
    ) -> (
        HashSet<&'a EventType>,
        HashSet<&'a EventType>,
        HashSet<(&'a EventType, &'a EventType)>,
        bool,
    ) {
        // Initializes the result sets
        let mut start_ev_types = HashSet::new();
        let mut end_ev_types = HashSet::new();
        let mut skippable: bool;

        // For the current node, identify the start and end event types and the directly-follows
        // relations by calling the method recursively
        let children_dfr: Vec<(
            HashSet<&EventType>,
            HashSet<&EventType>,
            HashSet<(&EventType, &EventType)>,
            bool,
        )> = self
            .children
            .iter()
            .map(|child| match child {
                OCPTNode::Operator(op) => op.get_directly_follows_relations(
                    ob_type,
                    rel_ob_types_per_node,
                    div_ob_types_per_node,
                ),
                OCPTNode::Leaf(leaf) => leaf.get_directly_follows_relations(ob_type),
            })
            .collect();

        // All childrens directly-follows relations are directly added
        let mut directly_follow_ev_types: HashSet<_> = children_dfr
            .iter()
            .flat_map(|(_, _, dfr_evs_child, _)| dfr_evs_child.to_owned())
            .collect();

        // For each operator type, start and end event types and directly-follows relation are
        // identified accordingly
        match self.operator_type {
            // For a sequence, we check for skippable (sub)parts an operator to identify start and
            // event types, and their directly-follows relations accordingly
            // The (Sub)parts are also skippable if they are unrelated or divergent
            OCPTOperatorType::Sequence => {
                skippable = true;

                let mut kept_end_evs = HashSet::new();
                let mut skip_forward = true;
                let mut kept_div_or_unrelated_evs: HashSet<&EventType> = HashSet::new();

                // Iterate forward to identify all start event types and to compute the
                // directly-follows relations by considering unrelatedness or divergence for
                // individual children, thus, making them skippable.
                children_dfr.iter().zip(&self.children).for_each(
                    |((start_evs_child, end_evs_child, _, skip_child), child)| {
                        // Iterate forward and skip as many children to identify all possible start
                        // event types
                        if skip_forward {
                            start_ev_types.extend(start_evs_child);
                        }
                        skip_forward &= skip_child;

                        // For all previous skippable children, they are tracked, and every new
                        // child has ingoing directly-follows relations from their alphabets
                        add_all_dfr_from_to_alphabets(
                            &mut directly_follow_ev_types,
                            &kept_end_evs,
                            start_evs_child,
                        );

                        // Update the skippable sets
                        if *skip_child {
                            kept_end_evs.extend(end_evs_child);
                        } else {
                            kept_end_evs = end_evs_child.clone();
                        }
                        skippable &= skip_child;

                        // Check if a child is skippable due to unrelatedness or divergence towards
                        // the given object type
                        let is_unrelated_or_divergent = child.check_unrelated_or_divergent(
                            ob_type,
                            rel_ob_types_per_node,
                            div_ob_types_per_node,
                        );

                        // If diverging or unrelated, add directly-follows relation as if the
                        // children are skippable
                        if is_unrelated_or_divergent {
                            let div_ob_types_per_ev_type =
                                div_ob_types_per_node.get(&child.get_uuid()).unwrap();

                            let curr_div_or_unrelated_evs = rel_ob_types_per_node
                                .get(&child.get_uuid())
                                .unwrap()
                                .iter()
                                .filter_map(|(&ev_type, ob_types)| {
                                    if ob_types.contains(ob_type)
                                        && div_ob_types_per_ev_type
                                            .get(ev_type)
                                            .unwrap()
                                            .contains(ob_type)
                                    {
                                        Some(ev_type)
                                    } else {
                                        None
                                    }
                                })
                                .collect::<HashSet<&EventType>>();

                            kept_div_or_unrelated_evs.iter().for_each(|&kept_ev| {
                                curr_div_or_unrelated_evs.iter().for_each(|&curr_ev| {
                                    directly_follow_ev_types.insert((kept_ev, curr_ev));
                                    directly_follow_ev_types.insert((curr_ev, kept_ev));
                                })
                            });

                            kept_div_or_unrelated_evs.extend(curr_div_or_unrelated_evs);
                        } else {
                            kept_div_or_unrelated_evs = HashSet::new();
                        }
                    },
                );

                // Iterate backward and skip as many children to identify all possible end event
                // types
                let mut skip_backward = true;
                children_dfr
                    .iter()
                    .rev()
                    .for_each(|(_, end_evs_child, _, skip_child)| {
                        if skip_backward {
                            end_ev_types.extend(end_evs_child);
                            skip_backward &= skip_child;
                        }
                    });
            }
            // For the exclusive choice operator, all children's start and end event types are
            // start and end event types
            // For the directly-follows relations there are only directly-follows relations
            // between children that are diverging
            OCPTOperatorType::ExclusiveChoice => {
                // Whether the node is skippable: this holds if any child is skippable
                skippable = false;
                // Checks which children are unrelated or divergent
                let mut unrelated_or_div_childs_pos: HashSet<usize> = HashSet::new();

                // Add start and end event types of all children
                // Check whether the node is skippable
                // Find all unrelated or divergent children by their position
                children_dfr
                    .iter()
                    .zip(&self.children)
                    .enumerate()
                    .for_each(
                        |(pos, ((start_evs_child, end_evs_child, _, skip_child), child))| {
                            start_ev_types.extend(start_evs_child);
                            end_ev_types.extend(end_evs_child);
                            skippable |= skip_child;

                            if child.check_unrelated_or_divergent(
                                ob_type,
                                rel_ob_types_per_node,
                                div_ob_types_per_node,
                            ) {
                                unrelated_or_div_childs_pos.insert(pos);
                            }
                        },
                    );

                let child_pos_iter = self.children.iter().enumerate();

                // Add relations between divergent event types
                child_pos_iter.clone().for_each(|(pos_1, child_1)| {
                    child_pos_iter.clone().for_each(|(pos_2, child_2)| {
                        if pos_1 != pos_2
                            && unrelated_or_div_childs_pos.contains(&pos_1)
                            && unrelated_or_div_childs_pos.contains(&pos_2)
                        {
                            rel_ob_types_per_node
                                .get(child_1.get_uuid())
                                .unwrap()
                                .iter()
                                .for_each(|(ev_type1, ob_types1)| {
                                    if ob_types1.contains(ob_type) {
                                        rel_ob_types_per_node
                                            .get(child_2.get_uuid())
                                            .unwrap()
                                            .iter()
                                            .for_each(|(ev_type2, ob_types2)| {
                                                if ob_types2.contains(ob_type) {
                                                    directly_follow_ev_types
                                                        .insert((ev_type1, ev_type2));
                                                }
                                            });
                                    }
                                });
                        }
                    })
                })
            }
            // If all children are skippable, the operator node itself is skippable
            // The directly-follows relations are computed from the childrens shuffle language
            OCPTOperatorType::Concurrency => {
                skippable = true;

                let child_alphabets: Vec<HashSet<&EventType>> = self
                    .children
                    .iter()
                    .map(|child| {
                        rel_ob_types_per_node
                            .get(child.get_uuid())
                            .unwrap()
                            .iter()
                            .filter_map(|(&ev_type, ob_types)| {
                                if ob_types.contains(ob_type) {
                                    Some(ev_type)
                                } else {
                                    None
                                }
                            })
                            .collect::<HashSet<&EventType>>()
                    })
                    .collect::<Vec<HashSet<&EventType>>>();

                children_dfr
                    .iter()
                    .for_each(|(start_evs_child, end_evs_child, _, skip_child)| {
                        start_ev_types.extend(start_evs_child);
                        end_ev_types.extend(end_evs_child);

                        skippable &= skip_child;
                    });

                directly_follow_ev_types.extend(compute_shuffle_dfr_language(&child_alphabets));
            }
            OCPTOperatorType::Loop(_) => {
                // The node is skippable if the first child is skippable
                skippable = children_dfr.get(0).unwrap().3;

                // Check if any other child is skippable that is a redo child
                let other_skippable = children_dfr
                    .iter()
                    .skip(1)
                    .find(|(_, _, _, skip_child)| *skip_child)
                    .is_some();

                // Adds relations from the end event types of the first child to the start event
                // types of all other children
                children_dfr
                    .iter()
                    .skip(1)
                    .for_each(|(start_evs, _, _, _)| {
                        add_all_dfr_from_to_alphabets(
                            &mut directly_follow_ev_types,
                            &children_dfr.get(0).unwrap().1,
                            start_evs,
                        );
                    });

                // Adds relations from the end event types of all other children to the start event
                // types of the first child
                children_dfr.iter().skip(1).for_each(|(_, end_evs, _, _)| {
                    add_all_dfr_from_to_alphabets(
                        &mut directly_follow_ev_types,
                        &end_evs,
                        &children_dfr.get(0).unwrap().0,
                    );
                });

                // If the first child is skippable, add all other events start and event types as
                // start event types
                // Further add relations from each redo children's end event type a to the start
                // event type of all redo children
                if skippable {
                    children_dfr.iter().skip(1).for_each(
                        |(start_evs_child, end_evs_child, _, _)| {
                            start_ev_types.extend(start_evs_child);
                            end_ev_types.extend(end_evs_child);
                        },
                    );

                    add_all_dfr_from_to_alphabets(
                        &mut directly_follow_ev_types,
                        &end_ev_types,
                        &start_ev_types,
                    );
                }

                // Adds the start event types and the end event types of the first child
                start_ev_types.extend(&children_dfr.get(0).unwrap().0);
                end_ev_types.extend(&children_dfr.get(0).unwrap().1);

                // If a redo child is skippable, add relation from end event types of the first
                // child to the start event types of the first child
                if other_skippable {
                    children_dfr.get(0).unwrap().0.iter().for_each(|start_ev| {
                        children_dfr.get(0).unwrap().1.iter().for_each(|end_ev| {
                            directly_follow_ev_types.insert((start_ev, end_ev));
                        });
                    })
                }
            }
        }

        (
            start_ev_types,
            end_ev_types,
            directly_follow_ev_types,
            skippable,
        )
    }

    ///
    /// Finds the related object types per event type per node by propagating the information
    /// in a bottom-up fashion: If an event type is related to an object type for a child, it also
    /// holds for the parent
    ///
    pub fn compute_related<'a>(
        &'a self,
        rel_ob_types_per_node_ot: &mut HashMap<
            Uuid,
            HashMap<&'a EventType, HashSet<&'a ObjectType>>,
        >,
    ) {
        // Call the routine for all children to make sure that their related object and event types
        // are identified
        self.children.iter().for_each(|child| match child {
            OCPTNode::Operator(op) => {
                op.compute_related(rel_ob_types_per_node_ot);
            }
            OCPTNode::Leaf(leaf) => match &leaf.activity_label {
                OCPTLeafLabel::Activity(leaf_label) => {
                    rel_ob_types_per_node_ot
                        .entry(leaf.uuid)
                        .or_insert(Default::default())
                        .insert(
                            leaf_label,
                            leaf.related_ob_types
                                .iter()
                                .collect(),
                        );
                }
                OCPTLeafLabel::Tau => {}
            },
        });

        // Aggregate the children's results
        let result: HashMap<&EventType, HashSet<&ObjectType>> = self
            .children
            .iter()
            .flat_map(|child| {
                rel_ob_types_per_node_ot
                    .get(child.get_uuid())
                    .unwrap()
                    .clone()
            })
            .collect::<_>();

        rel_ob_types_per_node_ot.insert(self.uuid, result);
    }

    ///
    /// Changes all object types per event type to be the set of related object types per event type
    /// for all descendants
    ///
    pub fn change_descendants_to_related<'a>(
        &'a self,
        target_ob_types_per_node: &mut HashMap<
            Uuid,
            HashMap<&'a EventType, HashSet<&'a ObjectType>>,
        >,
        rel_ob_types_per_node: &HashMap<Uuid, HashMap<&'a EventType, HashSet<&'a ObjectType>>>,
    ) {
        self.find_all_descendants_uuids().iter().for_each(|&uuid| {
            rel_ob_types_per_node
                .get(uuid)
                .unwrap()
                .iter()
                .for_each(|(&ev_type, ob_types)| {
                    target_ob_types_per_node
                        .get_mut(uuid)
                        .unwrap()
                        .insert(ev_type, ob_types.clone());
                })
        })
    }

    ///
    /// Aggregates the childrens object types per event types for a given node dictionary
    ///
    pub fn aggregate_children_ob_types_per_event_type<'a>(
        &'a self,
        ob_types_per_node: &mut HashMap<Uuid, HashMap<&'a EventType, HashSet<&'a ObjectType>>>,
    ) -> HashMap<&'a EventType, HashSet<&'a ObjectType>> {
        self.children
            .iter()
            .flat_map(|child| ob_types_per_node.get(&child.get_uuid()).unwrap().clone())
            .collect::<HashMap<_, _>>()
    }

    ///
    /// Finds the divergent object types per event type per node by propagating the information
    /// in a bottom-up fashion:
    /// If an event type is divergent to an object type for a child, it also holds for the parent
    ///
    pub fn compute_div<'a>(
        &'a self,
        div_ob_types_per_node: &mut HashMap<Uuid, HashMap<&'a EventType, HashSet<&'a ObjectType>>>,
        rel_ob_types_per_node: &HashMap<Uuid, HashMap<&'a EventType, HashSet<&'a ObjectType>>>,
    ) {
        match self.operator_type {
            // All descendants related event and object types are also divergent since they have a
            // loop operator as ancestor
            OCPTOperatorType::Loop(_) => {
                self.change_descendants_to_related(div_ob_types_per_node, rel_ob_types_per_node);
            }
            _ => {
                // Call the routine for all children to make sure that their divergent object
                // types per event type are identified
                self.children.iter().for_each(|child| match child {
                    OCPTNode::Operator(op) => {
                        op.compute_div(div_ob_types_per_node, rel_ob_types_per_node);
                    }
                    OCPTNode::Leaf(leaf) => match &leaf.activity_label {
                        OCPTLeafLabel::Activity(leaf_label) => {
                            div_ob_types_per_node
                                .entry(leaf.uuid)
                                .or_insert(Default::default())
                                .insert(
                                    leaf_label,
                                    leaf.divergent_ob_types
                                        .iter()
                                        .collect(),
                                );
                        }
                        OCPTLeafLabel::Tau => {}
                    },
                });
            }
        }

        let result: HashMap<&EventType, HashSet<&ObjectType>> =
            self.aggregate_children_ob_types_per_event_type(div_ob_types_per_node);

        div_ob_types_per_node.insert(self.uuid, result);
    }

    ///
    /// Computes all optional object types per event type for a node.
    /// Expects the optional event types to contain the divergent object types per event type.
    ///
    pub fn compute_opt<'a>(
        &'a self,
        opt_ob_types_per_node: &mut HashMap<Uuid, HashMap<&'a EventType, HashSet<&'a ObjectType>>>,
        rel_ob_types_per_node: &HashMap<Uuid, HashMap<&'a EventType, HashSet<&'a ObjectType>>>,
    ) {
        match self.operator_type {
            // All descendants related object types are also optional since they have an
            // exclusive choice operator as ancestor
            OCPTOperatorType::ExclusiveChoice => {
                self.change_descendants_to_related(opt_ob_types_per_node, rel_ob_types_per_node);
            }
            // Otherwise identify optionality from divergence
            _ => {
                self.children.iter().for_each(|child| match child {
                    OCPTNode::Operator(op) => {
                        op.compute_opt(opt_ob_types_per_node, rel_ob_types_per_node);
                    }
                    _ => {}
                });
            }
        }

        // Clone the current entry since it also contains the divergent object types per event type
        let mut result = opt_ob_types_per_node.get(&self.uuid).unwrap().clone();

        // Adds all object types as divergent that are additionally identified
        self.children.iter().for_each(|child| {
            opt_ob_types_per_node
                .get(&child.get_uuid())
                .unwrap()
                .iter()
                .for_each(|(&ev_type, ob_types)| {
                    result
                        .entry(ev_type)
                        .or_insert(HashSet::default())
                        .extend(ob_types);
                })
        });
        opt_ob_types_per_node.insert(self.uuid, result);
    }

    ///
    /// Computes convergence based on the convergence of the [`OCPTLeaf`]
    ///
    pub fn compute_leaf_conv<'a>(
        &'a self,
        leaf_conv_ob_types_per_node: &mut HashMap<
            Uuid,
            HashMap<&'a EventType, HashSet<&'a ObjectType>>,
        >,
    ) {
        // Recursively aggregates the converging object types of all leaves
        self.children.iter().for_each(|child| match child {
            OCPTNode::Operator(op) => {
                op.compute_leaf_conv(leaf_conv_ob_types_per_node);
            }
            OCPTNode::Leaf(leaf) => match &leaf.activity_label {
                OCPTLeafLabel::Activity(leaf_label) => {
                    leaf_conv_ob_types_per_node
                        .entry(leaf.uuid)
                        .or_insert(Default::default())
                        .insert(
                            leaf_label,
                            leaf.convergent_ob_types
                                .iter()
                                .collect(),
                        );
                }
                OCPTLeafLabel::Tau => {}
            },
        });

        let result = self.aggregate_children_ob_types_per_event_type(leaf_conv_ob_types_per_node);

        leaf_conv_ob_types_per_node.insert(self.uuid, result);
    }

    ///
    /// Computes deficiency based on the deficiency of the [`OCPTLeaf`]
    ///
    pub fn compute_leaf_def<'a>(
        &'a self,
        leaf_def_ob_types_per_node: &mut HashMap<
            Uuid,
            HashMap<&'a EventType, HashSet<&'a ObjectType>>,
        >,
    ) {
        // Recursively aggregates the deficient object types of all leaves
        self.children.iter().for_each(|child| match child {
            OCPTNode::Operator(op) => {
                op.compute_leaf_def(leaf_def_ob_types_per_node);
            }
            OCPTNode::Leaf(leaf) => match &leaf.activity_label {
                OCPTLeafLabel::Activity(leaf_label) => {
                    leaf_def_ob_types_per_node
                        .entry(leaf.uuid)
                        .or_insert(Default::default())
                        .insert(
                            leaf_label,
                            leaf.deficient_ob_types
                                .iter()
                                .collect(),
                        );
                }
                OCPTLeafLabel::Tau => {}
            },
        });

        let result = self.aggregate_children_ob_types_per_event_type(leaf_def_ob_types_per_node);

        leaf_def_ob_types_per_node.insert(self.uuid, result);
    }

    ///
    /// Computes convergence by:
    /// 1. Checking whether object types are deficient in the leaves or divergent
    /// 2. Recursively checks for a given event type and object type whether:
    ///     i. If an object type is convergent for all leaves of a sequence or parallel operator
    ///     ii. If an object type is convergent for any leaf of a choice operator
    ///     iii. If the object type is convergent for the first child of loop operator
    /// 3. Checks for a leaf whether the object type is converging or optional, or if all other
    /// object types, which are related to the leaf, are either divergent or deficient
    ///
    pub fn compute_conv<'a>(
        &'a self,
        root_rel_ob_types: &HashMap<&'a EventType, HashSet<&'a ObjectType>>,
        root_opt_ob_types: &HashMap<&'a EventType, HashSet<&'a ObjectType>>,
        root_def_ob_types: &HashMap<&'a EventType, HashSet<&'a ObjectType>>,
        root_div_ob_types: &HashMap<&'a EventType, HashSet<&'a ObjectType>>,
        root_conv_ob_types: &HashMap<&'a EventType, HashSet<&'a ObjectType>>,
    ) -> HashMap<&'a EventType, HashSet<&'a ObjectType>> {
        let mut result: HashMap<&EventType, HashSet<&ObjectType>> = HashMap::new();
        root_rel_ob_types.iter().for_each(|(&rel_ev_type, _)| {
            result.insert(rel_ev_type, HashSet::new());
        });

        let mut candidates: HashSet<(&EventType, &ObjectType)> = HashSet::new();

        root_conv_ob_types
            .iter()
            .for_each(|(&ev_type, leaf_conv_ob_types)| {
                leaf_conv_ob_types.iter().for_each(|leaf_conv_ob_type| {
                    if root_def_ob_types
                        .get(ev_type)
                        .unwrap_or(&Default::default())
                        .contains(leaf_conv_ob_type)
                        || root_div_ob_types
                            .get(ev_type)
                            .unwrap_or(&Default::default())
                            .contains(leaf_conv_ob_type)
                    {
                        result
                            .entry(ev_type)
                            .or_insert(Default::default())
                            .insert(leaf_conv_ob_type);
                    } else {
                        candidates.insert((ev_type, leaf_conv_ob_type));
                    }
                })
            });

        'outer_loop: for (ev_type, candidate_ob_type) in candidates {
            let mut competitors: HashSet<&ObjectType> = root_rel_ob_types
                .get(ev_type)
                .unwrap_or(&Default::default())
                .clone();
            competitors = competitors
                .difference(
                    root_conv_ob_types
                        .get(&ev_type)
                        .unwrap_or(&Default::default()),
                )
                .copied()
                .collect();
            competitors = competitors
                .difference(
                    root_opt_ob_types
                        .get(&ev_type)
                        .unwrap_or(&Default::default()),
                )
                .copied()
                .collect();
            competitors.remove(&candidate_ob_type);

            for competitor_ob_type in competitors {
                if !self.check_conv_subroutine_for_competitor(
                    candidate_ob_type,
                    competitor_ob_type,
                    root_opt_ob_types,
                ) {
                    continue 'outer_loop;
                }
            }

            result
                .entry(ev_type)
                .or_insert(Default::default())
                .insert(candidate_ob_type);
        }

        result
    }

    ///
    /// Handles the operator types in the convergence check as subroutine
    ///
    pub fn check_conv_subroutine_for_competitor(
        &self,
        candidate_ob_type: &ObjectType,
        competitor_ob_type: &ObjectType,
        root_opt_ob_types: &HashMap<&EventType, HashSet<&ObjectType>>,
    ) -> bool {
        match self.operator_type {
            OCPTOperatorType::Sequence | OCPTOperatorType::Concurrency => {
                for child in self.children.iter() {
                    if !check_conv_subroutine_for_competitor(
                        child,
                        candidate_ob_type,
                        competitor_ob_type,
                        root_opt_ob_types,
                    ) {
                        return false;
                    }
                }
                true
            }
            OCPTOperatorType::ExclusiveChoice => {
                for child in self.children.iter() {
                    if check_conv_subroutine_for_competitor(
                        child,
                        candidate_ob_type,
                        competitor_ob_type,
                        root_opt_ob_types,
                    ) {
                        return true;
                    }
                }
                false
            }
            OCPTOperatorType::Loop(_) => check_conv_subroutine_for_competitor(
                self.children.get(0).unwrap(),
                candidate_ob_type,
                competitor_ob_type,
                root_opt_ob_types,
            ),
        }
    }

    ///
    /// Computes deficiency by:
    /// 1. Checking whether object types are convergent in the leaves or optional
    /// 2. Recursively checks for a given event type and object type whether:
    ///     i. If an object type is divergent for all leaves of a sequence or parallel operator
    ///     ii. If an object type is divergent for any leaf of a choice operator
    ///     iii. If the object type is divergent for the first child of loop operator
    /// 3. Checks for a leaf whether the object type is divergent or deficient, or if all other
    /// object types, which are related to the leaf, are either converging or optional
    ///
    pub fn compute_def<'a>(
        &'a self,
        root_rel_ob_types: &HashMap<&'a EventType, HashSet<&'a ObjectType>>,
        root_opt_ob_types: &HashMap<&'a EventType, HashSet<&'a ObjectType>>,
        root_conv_ob_types: &HashMap<&'a EventType, HashSet<&'a ObjectType>>,
        root_div_ob_types: &HashMap<&'a EventType, HashSet<&'a ObjectType>>,
        root_def_ob_types: &HashMap<&'a EventType, HashSet<&'a ObjectType>>,
    ) -> HashMap<&'a EventType, HashSet<&'a ObjectType>> {
        let mut result: HashMap<&EventType, HashSet<&ObjectType>> = HashMap::new();
        root_rel_ob_types.iter().for_each(|(&rel_ev_type, _)| {
            result.insert(rel_ev_type, HashSet::new());
        });

        let mut candidates: HashSet<(&EventType, &ObjectType)> = HashSet::new();

        root_def_ob_types
            .iter()
            .for_each(|(&ev_type, leaf_conv_ob_types)| {
                leaf_conv_ob_types.iter().for_each(|leaf_conv_ob_type| {
                    if root_conv_ob_types
                        .get(ev_type)
                        .unwrap_or(&Default::default())
                        .contains(leaf_conv_ob_type)
                        || root_opt_ob_types
                            .get(ev_type)
                            .unwrap_or(&Default::default())
                            .contains(leaf_conv_ob_type)
                    {
                        result
                            .entry(ev_type)
                            .or_insert(Default::default())
                            .insert(leaf_conv_ob_type);
                    } else {
                        candidates.insert((ev_type, leaf_conv_ob_type));
                    }
                })
            });

        'outer_loop: for (ev_type, candidate_ob_type) in candidates {
            let mut competitors: HashSet<&ObjectType> = root_rel_ob_types
                .get(ev_type)
                .unwrap_or(&Default::default())
                .clone();
            competitors = competitors
                .difference(
                    root_def_ob_types
                        .get(&ev_type)
                        .unwrap_or(&Default::default()),
                )
                .copied()
                .collect();
            competitors = competitors
                .difference(
                    root_div_ob_types
                        .get(&ev_type)
                        .unwrap_or(&Default::default()),
                )
                .copied()
                .collect();
            competitors.remove(&candidate_ob_type);

            for competitor_ob_type in competitors {
                if !self.check_def_subroutine_for_competitor(
                    candidate_ob_type,
                    competitor_ob_type,
                    root_opt_ob_types,
                ) {
                    continue 'outer_loop;
                }
            }

            result
                .entry(ev_type)
                .or_insert(Default::default())
                .insert(candidate_ob_type);
        }

        result
    }

    ///
    /// Handles the operator types in the deficiency check as subroutine
    ///
    pub fn check_def_subroutine_for_competitor(
        &self,
        candidate_ob_type: &ObjectType,
        competitor_ob_type: &ObjectType,
        root_opt_ob_types: &HashMap<&EventType, HashSet<&ObjectType>>,
    ) -> bool {
        match self.operator_type {
            OCPTOperatorType::Sequence | OCPTOperatorType::Concurrency => {
                for child in self.children.iter() {
                    if !check_def_subroutine_for_competitor(
                        child,
                        candidate_ob_type,
                        competitor_ob_type,
                        root_opt_ob_types,
                    ) {
                        return false;
                    }
                }
                true
            }
            OCPTOperatorType::ExclusiveChoice => {
                for child in self.children.iter() {
                    if check_def_subroutine_for_competitor(
                        child,
                        candidate_ob_type,
                        competitor_ob_type,
                        root_opt_ob_types,
                    ) {
                        return true;
                    }
                }
                false
            }
            OCPTOperatorType::Loop(_) => check_def_subroutine_for_competitor(
                self.children.get(0).unwrap(),
                candidate_ob_type,
                competitor_ob_type,
                root_opt_ob_types,
            ),
        }
    }
}

///
/// Resolves the deficiency subroutine usage for an [`OCPTNode`] by calling the routine of an
/// [`OCPTOperator`] or [`OCPTLeaf`].
///
pub fn check_conv_subroutine_for_competitor(
    tree_node: &OCPTNode,
    candidate_ob_type: &ObjectType,
    competitor_ob_type: &ObjectType,
    root_opt_ob_types: &HashMap<&EventType, HashSet<&ObjectType>>,
) -> bool {
    match tree_node {
        OCPTNode::Operator(op) => op.check_conv_subroutine_for_competitor(
            candidate_ob_type,
            competitor_ob_type,
            root_opt_ob_types,
        ),
        OCPTNode::Leaf(leaf) => leaf.check_conv_subroutine_for_competitor(
            candidate_ob_type,
            competitor_ob_type,
            root_opt_ob_types,
        ),
    }
}

///
/// Resolves the deficiency subroutine usage for an [`OCPTNode`] by calling the routine of an
/// [`OCPTOperator`] or [`OCPTLeaf`].
///
pub fn check_def_subroutine_for_competitor(
    tree_node: &OCPTNode,
    candidate_ob_type: &ObjectType,
    competitor_ob_type: &ObjectType,
    root_opt_ob_types: &HashMap<&EventType, HashSet<&ObjectType>>,
) -> bool {
    match tree_node {
        OCPTNode::Operator(op) => op.check_def_subroutine_for_competitor(
            candidate_ob_type,
            competitor_ob_type,
            root_opt_ob_types,
        ),
        OCPTNode::Leaf(leaf) => leaf.check_def_subroutine_for_competitor(
            candidate_ob_type,
            competitor_ob_type,
            root_opt_ob_types,
        ),
    }
}

#[derive(Debug, Serialize, Deserialize)]
///
/// A leaf in an object-centric process tree
///
pub struct OCPTLeaf {
    /// The identifier of the leaf
    pub uuid: Uuid,
    /// The silent or non-silent activity label [`OCPTLeafLabel`]
    pub activity_label: OCPTLeafLabel,
    /// The related object types of the leaf
    pub related_ob_types: HashSet<ObjectType>,
    /// The divergent object types of the leaf
    pub divergent_ob_types: HashSet<ObjectType>,
    /// The convergent object types of the leaf
    pub convergent_ob_types: HashSet<ObjectType>,
    /// The deficient object types of the leaf
    pub deficient_ob_types: HashSet<ObjectType>,
}

impl OCPTLeaf {
    ///
    /// Creates a new [`OCPTLeaf`] either by using a given label or making it silent if a label
    /// is missing
    ///
    pub fn new(leaf_label: Option<EventType>) -> Self {
        if leaf_label.is_some() {
            Self {
                uuid: Uuid::new_v4(),
                activity_label: OCPTLeafLabel::Activity(leaf_label.unwrap().to_string()),
                related_ob_types: HashSet::new(),
                divergent_ob_types: HashSet::new(),
                convergent_ob_types: HashSet::new(),
                deficient_ob_types: HashSet::new(),
            }
        } else {
            Self {
                uuid: Uuid::new_v4(),
                activity_label: OCPTLeafLabel::Tau,
                related_ob_types: HashSet::new(),
                divergent_ob_types: HashSet::new(),
                convergent_ob_types: HashSet::new(),
                deficient_ob_types: HashSet::new(),
            }
        }
    }

    ///
    /// Checks the convergence routine for a leaf.
    ///
    pub fn check_conv_subroutine_for_competitor(
        &self,
        candidate_ob_type: &ObjectType,
        competitor_ob_type: &ObjectType,
        root_opt_ob_types: &HashMap<&EventType, HashSet<&ObjectType>>,
    ) -> bool {
        self.conv_def_subroutine(candidate_ob_type, competitor_ob_type, root_opt_ob_types)
    }

    ///
    /// Checks the deficiency routine for a leaf.
    ///
    pub fn check_def_subroutine_for_competitor(
        &self,
        candidate_ob_type: &ObjectType,
        competitor_ob_type: &ObjectType,
        root_opt_ob_types: &HashMap<&EventType, HashSet<&ObjectType>>,
    ) -> bool {
        self.conv_def_subroutine(competitor_ob_type, candidate_ob_type, root_opt_ob_types)
    }

    ///
    /// Interchangeable subroutine of converging or deficient object type computation
    ///
    pub fn conv_def_subroutine(
        &self,
        ob_type1: &ObjectType,
        ob_type2: &ObjectType,
        root_opt_ob_types: &HashMap<&EventType, HashSet<&ObjectType>>,
    ) -> bool {
        match &self.activity_label {
            OCPTLeafLabel::Tau => true,
            OCPTLeafLabel::Activity(leaf_label) => {
                leaf_label.is_empty()
                    || root_opt_ob_types
                        .get(leaf_label)
                        .unwrap_or(&HashSet::new())
                        .contains(ob_type1)
                    || self.convergent_ob_types.contains(ob_type1)
                    || self.divergent_ob_types.contains(ob_type2)
                    || self.deficient_ob_types.contains(ob_type2)
            }
        }
    }

    ///
    /// Computes the start and event types, directly-follows relations of a leaf, and whether it is
    /// skippable.
    /// A leaf is skippable if it is unrelated or silent.
    /// If an object type is divergent for an event type, the event type can repeat itself.
    ///
    pub fn get_directly_follows_relations(
        &self,
        ob_type: &str,
    ) -> (
        HashSet<&EventType>,
        HashSet<&EventType>,
        HashSet<(&EventType, &EventType)>,
        bool,
    ) {
        let mut start_ev_types = HashSet::new();
        let mut end_ev_types = HashSet::new();
        let mut directly_follow_ev_types = HashSet::new();
        let skippable;

        match &self.activity_label {
            OCPTLeafLabel::Activity(activity_label) => {
                if self.related_ob_types.contains(ob_type) {
                    start_ev_types.insert(activity_label);
                    end_ev_types.insert(activity_label);

                    if self.divergent_ob_types.contains(ob_type) {
                        directly_follow_ev_types.insert((activity_label, activity_label));
                    }

                    skippable = self.divergent_ob_types.contains(ob_type);
                } else {
                    skippable = true;
                }
            }
            OCPTLeafLabel::Tau => {
                skippable = true;
            }
        }

        (
            start_ev_types,
            end_ev_types,
            directly_follow_ev_types,
            skippable,
        )
    }
}
