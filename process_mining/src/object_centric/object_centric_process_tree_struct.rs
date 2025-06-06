use crate::object_centric::{
    add_all_dfr_from_to_alphabets, compute_shuffle_dfr_language, EventType, ObjectType,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

///
/// Leaf in an object-centric process tree
///
#[derive(Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum OCLeafLabel {
    /// Non-silent activity leaf
    TreeActivity(EventType),
    /// Silent activity leaf
    TreeTau,
}

///
/// Node in an object-centric process tree
///
#[derive(Debug, Serialize, Deserialize)]
pub enum OCProcessTreeNode {
    /// Operator node of an object-centric process tree
    Operator(OCProcessTreeOperatorNode),
    /// Leaf node of an object-centric process tree
    Leaf(OCProcessTreeLeaf),
}

impl OCProcessTreeNode {
    ///
    /// Returns the identifier of a node in an object-centric process tree
    ///
    pub fn get_uuid(&self) -> &Uuid {
        match self {
            OCProcessTreeNode::Operator(op) => &op.uuid,
            OCProcessTreeNode::Leaf(leaf) => &leaf.uuid,
        }
    }

    ///
    /// Creates a new operator with the given operator type
    ///
    pub fn new_operator(op_type: OCOperatorType) -> Self {
        OCProcessTreeNode::Operator(OCProcessTreeOperatorNode::new(op_type))
    }

    ///
    /// Creates a new (non-silent) leaf
    ///
    pub fn new_leaf(leaf_label: Option<EventType>) -> Self {
        OCProcessTreeNode::Leaf(OCProcessTreeLeaf::new(leaf_label))
    }

    ///
    /// Adds a node as child if the node is an operator node
    ///
    pub fn add_child(&mut self, child: OCProcessTreeNode) {
        match self {
            OCProcessTreeNode::Operator(op) => {
                op.children.push(child);
            }
            OCProcessTreeNode::Leaf(_) => {
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
            OCProcessTreeNode::Operator(op) => match op.operator_type {
                OCOperatorType::Loop(_) => op.children.len() >= 2,
                _ => !op.children.is_empty(),
            },
            OCProcessTreeNode::Leaf(_) => true,
        }
    }

    ///
    /// Adds an object type to be convergent.
    /// If the node is a leaf, it gets directly added.
    /// If it as an operator, it is propagated to its descendants.
    ///
    pub fn add_convergent_ob_type(&mut self, ob_type: &ObjectType) {
        match self {
            OCProcessTreeNode::Operator(op) => {
                op.children
                    .iter_mut()
                    .for_each(|child| child.add_convergent_ob_type(ob_type));
            }
            OCProcessTreeNode::Leaf(ref mut leaf) => {
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
            OCProcessTreeNode::Operator(op) => {
                op.children
                    .iter_mut()
                    .for_each(|child| child.add_deficient_ob_type(ob_type));
            }
            OCProcessTreeNode::Leaf(ref mut leaf) => {
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
            OCProcessTreeNode::Operator(op) => {
                op.children
                    .iter_mut()
                    .for_each(|child| child.add_divergent_ob_type(ob_type));
            }
            OCProcessTreeNode::Leaf(ref mut leaf) => {
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
            OCProcessTreeNode::Operator(op) => {
                op.children
                    .iter_mut()
                    .for_each(|child| child.add_related_ob_type(ob_type));
            }
            OCProcessTreeNode::Leaf(ref mut leaf) => {
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
/// Operator type enum for [`OCProcessTreeOperatorNode`]
///
#[derive(Debug, Serialize, Deserialize)]
pub enum OCOperatorType {
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
/// Object-centric process tree struct that contains [`OCProcessTreeNode`] as root
///
#[derive(Debug, Serialize)]
pub struct OCProcessTree {
    /// The root of the object-centric process tree
    pub root: OCProcessTreeNode,
}

impl OCProcessTree {
    ///
    /// Initializes the object-centric process tree with the given node as root
    ///
    pub fn new(root: OCProcessTreeNode) -> Self {
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
        // Setup the iteration through the object-centric process tree
        let mut prev_ocpt_node_ids: HashSet<Uuid> = HashSet::new();
        let mut curr_ocpt_node_ids: HashSet<Uuid> = HashSet::new();
        curr_ocpt_node_ids.insert(*self.root.get_uuid());

        let mut curr_operators: Vec<&OCProcessTreeOperatorNode> = Vec::new();
        match &self.root {
            OCProcessTreeNode::Operator(op) => {
                curr_operators.push(op);
            }
            OCProcessTreeNode::Leaf(_) => {}
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
                    OCProcessTreeNode::Operator(op) => {
                        all_op_nodes_valid &= child.check_children_valid();

                        next_operators.push(op);
                        children_count += 1;
                        curr_ocpt_node_ids.insert(op.uuid);
                    }
                    OCProcessTreeNode::Leaf(leaf) => {
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
    /// Returns all descendant [`OCProcessTreeLeaf`]
    ///
    pub fn find_all_leaves(&self) -> Vec<&OCProcessTreeLeaf> {
        let mut result: Vec<&OCProcessTreeLeaf> = Vec::new();

        let mut curr_operators: Vec<&OCProcessTreeOperatorNode> = Vec::new();
        match &self.root {
            OCProcessTreeNode::Operator(op) => curr_operators.push(op),
            OCProcessTreeNode::Leaf(leaf) => result.push(leaf),
        };

        let mut next_operators = Vec::new();

        while !curr_operators.is_empty() {
            curr_operators.iter().for_each(|op| {
                op.children.iter().for_each(|child| match child {
                    OCProcessTreeNode::Operator(op) => next_operators.push(op),
                    OCProcessTreeNode::Leaf(leaf) => result.push(leaf),
                })
            });

            curr_operators = next_operators;
            next_operators = Vec::new();
        }

        result
    }

    ///
    /// Returns all `Uuid` of all [`OCProcessTreeOperatorNode`] in the tree
    ///
    pub fn find_all_node_uuids(&self) -> Vec<&Uuid> {
        let mut result: Vec<&Uuid> = Vec::new();

        let mut curr_operators: Vec<&OCProcessTreeOperatorNode> = Vec::new();
        match &self.root {
            OCProcessTreeNode::Operator(op) => {
                curr_operators.push(op);
                result.push(&op.uuid);
            }
            OCProcessTreeNode::Leaf(leaf) => {
                result.push(&leaf.uuid);
            }
        };

        let mut next_operators = Vec::new();

        while !curr_operators.is_empty() {
            curr_operators.iter().for_each(|op| {
                op.children.iter().for_each(|child| match child {
                    OCProcessTreeNode::Operator(op) => {
                        next_operators.push(op);
                        result.push(&op.uuid);
                    }
                    OCProcessTreeNode::Leaf(leaf) => result.push(&leaf.uuid),
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
pub struct OCProcessTreeOperatorNode {
    /// The node ID
    pub uuid: Uuid,
    /// The [`OCOperatorType`] of the tree itself
    pub operator_type: OCOperatorType,
    /// The children nodes of the operator node
    pub children: Vec<OCProcessTreeNode>,
}

impl OCProcessTreeOperatorNode {
    ///
    /// A constructor for the struct that intializes with the given [`OCOperatorType`] and
    /// otherwise a fresh [`Uuid`] and an empty list of children
    ///
    pub fn new(operator_type: OCOperatorType) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            operator_type,
            children: Vec::new(),
        }
    }

    ///
    /// Recursively, finds the directly follows relations of an object-centric (sub)tree towards
    /// a given object type. Therefore, divergence and unrelatedness are considered to identify
    /// parts of the object-centric process tree that can be skipped.
    ///
    /// Returns all start [`EventType`],  all end [`EventType`], and each directly follows relation
    /// of type ([`EventType`], [`EventType`]) as `HashSet`s for the given [`ObjectType`]
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
                OCProcessTreeNode::Operator(op) => op.get_directly_follows_relations(
                    ob_type,
                    rel_ob_types_per_node,
                    div_ob_types_per_node,
                ),
                OCProcessTreeNode::Leaf(leaf) => leaf.get_directly_follows_relations(ob_type),
            })
            .collect();

        // All children's directly-follows relations are directly added
        let mut directly_follow_ev_types: HashSet<_> = children_dfr
            .iter()
            .flat_map(|(_, _, dfr_evs_child, _)| dfr_evs_child.to_owned())
            .collect();

        // For each operator type, start and end event types and directly-follows relation are
        // identified accordingly
        match self.operator_type {
            OCOperatorType::Sequence => {
                skippable = true;

                let mut kept_end_evs = HashSet::new();
                let mut skip_forward = true;
                let mut kept_div_or_unrel_evs: HashSet<&EventType> = HashSet::new();

                // Iterate forward to identify all start event types and to compute the
                // directly-follows relations by considering unrelatedness or divergence for
                // individual children, thus, making them skippable
                children_dfr.iter().zip(&self.children).for_each(
                    |((start_evs_child, end_evs_child, _, skip_child), child)| {
                        // Iterate foward and skip as many children to identify all possible start
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

                            let curr_div_or_unrel_evs = rel_ob_types_per_node
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

                            kept_div_or_unrel_evs.iter().for_each(|&kept_ev| {
                                curr_div_or_unrel_evs.iter().for_each(|&curr_ev| {
                                    directly_follow_ev_types.insert((kept_ev, curr_ev));
                                    directly_follow_ev_types.insert((curr_ev, kept_ev));
                                })
                            });

                            kept_div_or_unrel_evs.extend(curr_div_or_unrel_evs);
                        } else {
                            kept_div_or_unrel_evs = HashSet::new();
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
            OCOperatorType::ExclusiveChoice => {
                skippable = false;

                let mut unrelated_or_div_childs_pos: HashSet<usize> = HashSet::new();

                children_dfr
                    .iter()
                    .zip(&self.children)
                    .enumerate()
                    .for_each(
                        |(
                            pos,
                            ((start_evs_child, end_evs_child, dfr_evs_child, skip_child), child),
                        )| {
                            start_ev_types.extend(start_evs_child);
                            end_ev_types.extend(end_evs_child);
                            directly_follow_ev_types.extend(dfr_evs_child);
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

                self.children
                    .iter()
                    .enumerate()
                    .for_each(|(pos_1, child_1)| {
                        self.children
                            .iter()
                            .enumerate()
                            .for_each(|(pos_2, child_2)| {
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
            OCOperatorType::Concurrency => {
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

                children_dfr.iter().for_each(
                    |(start_evs_child, end_evs_child, dfr_evs_child, skip_child)| {
                        start_ev_types.extend(start_evs_child);
                        end_ev_types.extend(end_evs_child);
                        directly_follow_ev_types.extend(dfr_evs_child);

                        skippable &= skip_child;
                    },
                );

                let concurrent_dfrs: HashSet<_> = compute_shuffle_dfr_language(&child_alphabets);

                directly_follow_ev_types.extend(concurrent_dfrs);
            }
            OCOperatorType::Loop(_) => {
                children_dfr.iter().for_each(|(_, _, dfr_child, _)| {
                    directly_follow_ev_types.extend(dfr_child);
                });

                skippable = children_dfr.get(0).unwrap().3;
                let other_skippable = children_dfr
                    .iter()
                    .skip(1)
                    .find(|(_, _, _, skip_child)| *skip_child)
                    .is_some();

                children_dfr.get(0).unwrap().1.iter().for_each(|&end_ev| {
                    children_dfr
                        .iter()
                        .skip(1)
                        .for_each(|(start_evs, _, _, _)| {
                            start_evs.iter().for_each(|&start_ev| {
                                directly_follow_ev_types.insert((end_ev, start_ev));
                            })
                        })
                });

                children_dfr.get(0).unwrap().0.iter().for_each(|&start_ev| {
                    children_dfr.iter().skip(1).for_each(|(end_evs, _, _, _)| {
                        end_evs.iter().for_each(|&end_ev| {
                            directly_follow_ev_types.insert((end_ev, start_ev));
                        })
                    })
                });

                if skippable {
                    children_dfr.iter().skip(1).for_each(
                        |(start_evs_child, end_evs_child, _, _)| {
                            start_ev_types.extend(start_evs_child);
                            end_ev_types.extend(end_evs_child);
                        },
                    );

                    start_ev_types.iter().for_each(|&start_ev| {
                        end_ev_types.iter().for_each(|&end_ev| {
                            directly_follow_ev_types.insert((end_ev, start_ev));
                        })
                    })
                }

                start_ev_types.extend(&children_dfr.get(0).unwrap().0);
                end_ev_types.extend(&children_dfr.get(0).unwrap().1);

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

    pub fn compute_related<'a>(
        &'a self,
        rel_ob_types_per_node_ot: &mut HashMap<
            Uuid,
            HashMap<&'a EventType, HashSet<&'a ObjectType>>,
        >,
    ) {
        self.children.iter().for_each(|child| match child {
            OCProcessTreeNode::Operator(op) => {
                op.compute_related(rel_ob_types_per_node_ot);
            }
            OCProcessTreeNode::Leaf(leaf) => match &leaf.activity_label {
                OCLeafLabel::TreeActivity(leaf_label) => {
                    rel_ob_types_per_node_ot
                        .entry(leaf.uuid)
                        .or_insert(Default::default())
                        .insert(
                            leaf_label,
                            leaf.related_ob_types
                                .iter()
                                .map(|rel_ob_type| rel_ob_type)
                                .collect(),
                        );
                }
                OCLeafLabel::TreeTau => {}
            },
        });

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

    pub fn compute_div<'a>(
        &'a self,
        div_ob_types_per_node: &mut HashMap<Uuid, HashMap<&'a EventType, HashSet<&'a ObjectType>>>,
        rel_ob_types_per_node: &HashMap<Uuid, HashMap<&'a EventType, HashSet<&'a ObjectType>>>,
    ) {
        self.children.iter().for_each(|child| match child {
            OCProcessTreeNode::Operator(op) => {
                op.compute_div(div_ob_types_per_node, rel_ob_types_per_node);
            }
            OCProcessTreeNode::Leaf(leaf) => match &leaf.activity_label {
                OCLeafLabel::TreeActivity(leaf_label) => {
                    div_ob_types_per_node
                        .entry(leaf.uuid)
                        .or_insert(Default::default())
                        .insert(
                            leaf_label,
                            leaf.divergent_ob_types
                                .iter()
                                .map(|div_ob_type| div_ob_type)
                                .collect(),
                        );
                }
                OCLeafLabel::TreeTau => {}
            },
        });

        let mut result: HashMap<&EventType, HashSet<&ObjectType>> = self
            .children
            .iter()
            .flat_map(|child| {
                div_ob_types_per_node
                    .get(&child.get_uuid())
                    .unwrap()
                    .clone()
            })
            .collect();

        match self.operator_type {
            OCOperatorType::Loop(_) => self.children.iter().for_each(|child| {
                rel_ob_types_per_node
                    .get(&child.get_uuid())
                    .unwrap()
                    .iter()
                    .for_each(|(&ob_type, ev_types)| {
                        result
                            .entry(ob_type)
                            .or_insert(Default::default())
                            .extend(ev_types);
                    })
            }),
            _ => {}
        }

        div_ob_types_per_node.insert(self.uuid, result);
    }

    pub fn compute_opt<'a>(
        &'a self,
        opt_ob_types_per_node: &mut HashMap<Uuid, HashMap<&'a EventType, HashSet<&'a ObjectType>>>,
        rel_ob_types_per_node: &HashMap<Uuid, HashMap<&'a EventType, HashSet<&'a ObjectType>>>,
        mut ancestor_is_exclusive_choice: bool,
    ) {
        match self.operator_type {
            OCOperatorType::ExclusiveChoice => {
                ancestor_is_exclusive_choice = true;
            }
            _ => {}
        }

        self.children.iter().for_each(|child| match child {
            OCProcessTreeNode::Operator(op) => {
                op.compute_opt(
                    opt_ob_types_per_node,
                    rel_ob_types_per_node,
                    ancestor_is_exclusive_choice,
                );
            }
            OCProcessTreeNode::Leaf(leaf) => match &leaf.activity_label {
                OCLeafLabel::TreeActivity(leaf_label) => {
                    if ancestor_is_exclusive_choice {
                        opt_ob_types_per_node
                            .entry(leaf.uuid)
                            .or_insert(Default::default())
                            .entry(leaf_label)
                            .or_insert(Default::default())
                            .extend(&leaf.related_ob_types);
                    }
                }
                OCLeafLabel::TreeTau => {}
            },
        });

        let mut result = opt_ob_types_per_node.get(&self.uuid).unwrap().clone();
        self.children.iter().for_each(|child| {
            opt_ob_types_per_node
                .get(&child.get_uuid())
                .unwrap()
                .iter()
                .for_each(|(&ev_type, ob_types)| {
                    result
                        .entry(ev_type)
                        .or_insert(Default::default())
                        .extend(ob_types);
                })
        });
        opt_ob_types_per_node.insert(self.uuid, result);
    }

    pub fn compute_leaf_conv<'a>(
        &'a self,
        leaf_conv_ob_types_per_node: &mut HashMap<
            Uuid,
            HashMap<&'a EventType, HashSet<&'a ObjectType>>,
        >,
    ) {
        self.children.iter().for_each(|child| match child {
            OCProcessTreeNode::Operator(op) => {
                op.compute_leaf_conv(leaf_conv_ob_types_per_node);
            }
            OCProcessTreeNode::Leaf(leaf) => match &leaf.activity_label {
                OCLeafLabel::TreeActivity(leaf_label) => {
                    leaf_conv_ob_types_per_node
                        .entry(leaf.uuid)
                        .or_insert(Default::default())
                        .insert(
                            leaf_label,
                            leaf.convergent_ob_types
                                .iter()
                                .map(|ob_type| ob_type)
                                .collect(),
                        );
                }
                OCLeafLabel::TreeTau => {}
            },
        });

        let result = self
            .children
            .iter()
            .flat_map(|child| {
                leaf_conv_ob_types_per_node
                    .get(&child.get_uuid())
                    .unwrap()
                    .clone()
            })
            .collect::<HashMap<&EventType, HashSet<&ObjectType>>>();

        leaf_conv_ob_types_per_node.insert(self.uuid, result);
    }

    pub fn compute_leaf_def<'a>(
        &'a self,
        leaf_def_ob_types_per_node: &mut HashMap<
            Uuid,
            HashMap<&'a EventType, HashSet<&'a ObjectType>>,
        >,
    ) {
        self.children.iter().for_each(|child| match child {
            OCProcessTreeNode::Operator(op) => {
                op.compute_leaf_def(leaf_def_ob_types_per_node);
            }
            OCProcessTreeNode::Leaf(leaf) => match &leaf.activity_label {
                OCLeafLabel::TreeActivity(leaf_label) => {
                    leaf_def_ob_types_per_node
                        .entry(leaf.uuid)
                        .or_insert(Default::default())
                        .insert(
                            leaf_label,
                            leaf.deficient_ob_types
                                .iter()
                                .map(|ob_type| ob_type)
                                .collect(),
                        );
                }
                OCLeafLabel::TreeTau => {}
            },
        });

        let result = self
            .children
            .iter()
            .flat_map(|child| {
                leaf_def_ob_types_per_node
                    .get(&child.get_uuid())
                    .unwrap()
                    .clone()
            })
            .collect::<HashMap<&EventType, HashSet<&ObjectType>>>();

        leaf_def_ob_types_per_node.insert(self.uuid, result);
    }

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

        'outer_loop: for (ev_type, cand_ob_type) in candidates {
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
            competitors.remove(&cand_ob_type);

            for comp_ob_type in competitors {
                if !self.check_conv_subroutine_for_competitor(
                    cand_ob_type,
                    comp_ob_type,
                    root_opt_ob_types,
                ) {
                    continue 'outer_loop;
                }
            }

            result
                .entry(ev_type)
                .or_insert(Default::default())
                .insert(cand_ob_type);
        }

        result
    }

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

        'outer_loop: for (ev_type, cand_ob_type) in candidates {
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
            competitors.remove(&cand_ob_type);

            for comp_ob_type in competitors {
                if !self.check_def_subroutine_for_competitor(
                    cand_ob_type,
                    comp_ob_type,
                    root_opt_ob_types,
                ) {
                    continue 'outer_loop;
                }
            }

            result
                .entry(ev_type)
                .or_insert(Default::default())
                .insert(cand_ob_type);
        }

        result
    }

    pub fn check_conv_subroutine_for_competitor(
        &self,
        cand_ob_type: &ObjectType,
        comp_ob_type: &ObjectType,
        root_opt_ob_types: &HashMap<&EventType, HashSet<&ObjectType>>,
    ) -> bool {
        match self.operator_type {
            OCOperatorType::Sequence | OCOperatorType::Concurrency => {
                for child in self.children.iter() {
                    if !check_conv_subroutine_for_competitor(
                        child,
                        cand_ob_type,
                        comp_ob_type,
                        root_opt_ob_types,
                    ) {
                        return false;
                    }
                }
                true
            }
            OCOperatorType::ExclusiveChoice => {
                for child in self.children.iter() {
                    if check_conv_subroutine_for_competitor(
                        child,
                        cand_ob_type,
                        comp_ob_type,
                        root_opt_ob_types,
                    ) {
                        return true;
                    }
                }
                false
            }
            OCOperatorType::Loop(_) => check_conv_subroutine_for_competitor(
                self.children.get(0).unwrap(),
                cand_ob_type,
                comp_ob_type,
                root_opt_ob_types,
            ),
        }
    }

    pub fn check_def_subroutine_for_competitor(
        &self,
        cand_ob_type: &ObjectType,
        comp_ob_type: &ObjectType,
        root_opt_ob_types: &HashMap<&EventType, HashSet<&ObjectType>>,
    ) -> bool {
        match self.operator_type {
            OCOperatorType::Sequence | OCOperatorType::Concurrency => {
                for child in self.children.iter() {
                    if !check_def_subroutine_for_competitor(
                        child,
                        cand_ob_type,
                        comp_ob_type,
                        root_opt_ob_types,
                    ) {
                        return false;
                    }
                }
                true
            }
            OCOperatorType::ExclusiveChoice => {
                for child in self.children.iter() {
                    if check_def_subroutine_for_competitor(
                        child,
                        cand_ob_type,
                        comp_ob_type,
                        root_opt_ob_types,
                    ) {
                        return true;
                    }
                }
                false
            }
            OCOperatorType::Loop(_) => check_def_subroutine_for_competitor(
                self.children.get(0).unwrap(),
                cand_ob_type,
                comp_ob_type,
                root_opt_ob_types,
            ),
        }
    }
}

pub fn check_conv_subroutine_for_competitor(
    tree_node: &OCProcessTreeNode,
    cand_ob_type: &ObjectType,
    comp_ob_type: &ObjectType,
    root_opt_ob_types: &HashMap<&EventType, HashSet<&ObjectType>>,
) -> bool {
    match tree_node {
        OCProcessTreeNode::Operator(op) => {
            op.check_conv_subroutine_for_competitor(cand_ob_type, comp_ob_type, root_opt_ob_types)
        }
        OCProcessTreeNode::Leaf(leaf) => {
            leaf.check_conv_subroutine_for_competitor(cand_ob_type, comp_ob_type, root_opt_ob_types)
        }
    }
}

pub fn check_def_subroutine_for_competitor(
    tree_node: &OCProcessTreeNode,
    cand_ob_type: &ObjectType,
    comp_ob_type: &ObjectType,
    root_opt_ob_types: &HashMap<&EventType, HashSet<&ObjectType>>,
) -> bool {
    match tree_node {
        OCProcessTreeNode::Operator(op) => {
            op.check_def_subroutine_for_competitor(cand_ob_type, comp_ob_type, root_opt_ob_types)
        }
        OCProcessTreeNode::Leaf(leaf) => {
            leaf.check_def_subroutine_for_competitor(cand_ob_type, comp_ob_type, root_opt_ob_types)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OCProcessTreeLeaf {
    pub uuid: Uuid,
    pub activity_label: OCLeafLabel,
    pub related_ob_types: HashSet<ObjectType>,
    pub divergent_ob_types: HashSet<ObjectType>,
    pub convergent_ob_types: HashSet<ObjectType>,
    pub deficient_ob_types: HashSet<ObjectType>,
}

impl OCProcessTreeLeaf {
    pub fn new(leaf_label: Option<EventType>) -> Self {
        if leaf_label.is_some() {
            Self {
                uuid: Uuid::new_v4(),
                activity_label: OCLeafLabel::TreeActivity(leaf_label.unwrap().to_string()),
                related_ob_types: HashSet::new(),
                divergent_ob_types: HashSet::new(),
                convergent_ob_types: HashSet::new(),
                deficient_ob_types: HashSet::new(),
            }
        } else {
            Self {
                uuid: Uuid::new_v4(),
                activity_label: OCLeafLabel::TreeTau,
                related_ob_types: HashSet::new(),
                divergent_ob_types: HashSet::new(),
                convergent_ob_types: HashSet::new(),
                deficient_ob_types: HashSet::new(),
            }
        }
    }

    pub fn check_conv_subroutine_for_competitor(
        &self,
        cand_ob_type: &ObjectType,
        comp_ob_type: &ObjectType,
        root_opt_ob_types: &HashMap<&EventType, HashSet<&ObjectType>>,
    ) -> bool {
        match &self.activity_label {
            OCLeafLabel::TreeTau => true,
            OCLeafLabel::TreeActivity(leaf_label) => {
                leaf_label.is_empty()
                    || root_opt_ob_types
                        .get(leaf_label)
                        .unwrap_or(&HashSet::new())
                        .contains(cand_ob_type)
                    || self.convergent_ob_types.contains(cand_ob_type)
                    || self.divergent_ob_types.contains(comp_ob_type)
                    || self.deficient_ob_types.contains(comp_ob_type)
            }
        }
    }

    pub fn check_def_subroutine_for_competitor(
        &self,
        cand_ob_type: &ObjectType,
        comp_ob_type: &ObjectType,
        root_opt_ob_types: &HashMap<&EventType, HashSet<&ObjectType>>,
    ) -> bool {
        match &self.activity_label {
            OCLeafLabel::TreeTau => true,
            OCLeafLabel::TreeActivity(leaf_label) => {
                leaf_label.is_empty()
                    || root_opt_ob_types
                        .get(leaf_label)
                        .unwrap_or(&HashSet::new())
                        .contains(comp_ob_type)
                    || self.convergent_ob_types.contains(comp_ob_type)
                    || self.divergent_ob_types.contains(cand_ob_type)
                    || self.deficient_ob_types.contains(cand_ob_type)
            }
        }
    }

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
            OCLeafLabel::TreeActivity(activity_label) => {
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
            OCLeafLabel::TreeTau => {
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
