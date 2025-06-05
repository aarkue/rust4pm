use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

type NumberOfRepetitions = Option<u32>;
pub type ObjectType = String;
pub type EventType = String;

#[derive(Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum OCLeafLabel {
    TreeActivity(EventType),
    TreeTau,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum OCProcessTreeNode {
    Operator(OCProcessTreeOperator),
    Leaf(OCProcessTreeLeaf),
}

impl OCProcessTreeNode {
    pub fn get_uuid(&self) -> &Uuid {
        match self {
            OCProcessTreeNode::Operator(op) => &op.uuid,
            OCProcessTreeNode::Leaf(leaf) => &leaf.uuid,
        }
    }

    pub fn new_operator(op_type: OCOperatorType) -> Self {
        OCProcessTreeNode::Operator(OCProcessTreeOperator::new(op_type))
    }

    pub fn new_leaf(leaf_label: Option<EventType>) -> Self {
        OCProcessTreeNode::Leaf(OCProcessTreeLeaf::new(leaf_label))
    }

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

    pub fn add_convergent_ob_type(&mut self, ob_type: &ObjectType) {
        match self {
            OCProcessTreeNode::Operator(op) => {
                op.children
                    .iter_mut()
                    .for_each(|mut child| child.add_convergent_ob_type(ob_type));
            }
            OCProcessTreeNode::Leaf(ref mut leaf) => {
                leaf.convergent_ob_types.insert(ob_type.to_string());
            }
        }
    }

    pub fn add_deficient_ob_type(&mut self, ob_type: &ObjectType) {
        match self {
            OCProcessTreeNode::Operator(op) => {
                op.children
                    .iter_mut()
                    .for_each(|mut child| child.add_deficient_ob_type(ob_type));
            }
            OCProcessTreeNode::Leaf(ref mut leaf) => {
                leaf.deficient_ob_types.insert(ob_type.to_string());
            }
        }
    }

    pub fn add_divergent_ob_type(&mut self, ob_type: &ObjectType) {
        match self {
            OCProcessTreeNode::Operator(op) => {
                op.children
                    .iter_mut()
                    .for_each(|mut child| child.add_divergent_ob_type(ob_type));
            }
            OCProcessTreeNode::Leaf(ref mut leaf) => {
                leaf.divergent_ob_types.insert(ob_type.to_string());
            }
        }
    }

    pub fn add_related_ob_type(&mut self, ob_type: &ObjectType) {
        match self {
            OCProcessTreeNode::Operator(op) => {
                op.children
                    .iter_mut()
                    .for_each(|mut child| child.add_related_ob_type(ob_type));
            }
            OCProcessTreeNode::Leaf(ref mut leaf) => {
                leaf.related_ob_types.insert(ob_type.to_string());
            }
        }
    }

    fn check_unrelated_or_divergent(
        &self,
        ob_type: &ObjectType,
        rel_ob_types_per_node: &HashMap<Uuid, HashMap<&EventType, HashSet<&ObjectType>>>,
        div_ob_types_per_node: &HashMap<Uuid, HashMap<&EventType, HashSet<&ObjectType>>>,
    ) -> bool {
        let mut result = true;
        let childs_rel_ob_types_per_ev_type = rel_ob_types_per_node.get(self.get_uuid()).unwrap();
        let childs_div_ob_types_per_ev_type = div_ob_types_per_node.get(self.get_uuid()).unwrap();

        childs_rel_ob_types_per_ev_type
            .iter()
            .for_each(|(&ev_type, ob_types)| {
                if ob_types.contains(ob_type) {
                    if !childs_div_ob_types_per_ev_type
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

#[derive(Debug, Serialize, Deserialize)]
pub enum OCOperatorType {
    Sequence,
    ExclusiveChoice,
    Concurrency,
    Loop(NumberOfRepetitions),
}

#[derive(Debug, Serialize)]
pub struct OCProcessTree {
    pub root: OCProcessTreeNode,
}

impl OCProcessTree {
    pub fn new(root: OCProcessTreeNode) -> Self {
        Self { root }
    }

    pub fn is_valid(&self) -> bool {
        let mut prev_ocpt_node_ids: HashSet<Uuid> = HashSet::new();

        let mut ocpt_node_ids: HashSet<Uuid> = HashSet::new();
        ocpt_node_ids.insert(*self.root.get_uuid());
        let mut children_count: usize = 1;

        let mut curr_operators: Vec<&OCProcessTreeOperator> = Vec::new();
        match &self.root {
            OCProcessTreeNode::Operator(op) => {
                curr_operators.push(op);
                match op.operator_type {
                    OCOperatorType::Loop(_) => {
                        if op.children.len() < 2 {
                            return false;
                        }
                    }
                    _ => {
                        if op.children.is_empty() {
                            return false;
                        }
                    }
                }
            }
            OCProcessTreeNode::Leaf(_) => {}
        };

        let mut next_operators = Vec::new();

        while !prev_ocpt_node_ids.eq(&ocpt_node_ids) {
            curr_operators.iter().for_each(|op| {
                op.children.iter().for_each(|child| match child {
                    OCProcessTreeNode::Operator(op) => {
                        next_operators.push(op);
                        children_count += 1;
                        ocpt_node_ids.insert(op.uuid);
                    }
                    OCProcessTreeNode::Leaf(leaf) => {
                        children_count += 1;
                        ocpt_node_ids.insert(leaf.uuid);
                    }
                })
            });

            curr_operators = next_operators;
            next_operators = Vec::new();
            prev_ocpt_node_ids = ocpt_node_ids.clone();
        }

        children_count == ocpt_node_ids.len()
    }

    pub fn find_all_leaves(&self) -> Vec<&OCProcessTreeLeaf> {
        let mut result: Vec<&OCProcessTreeLeaf> = Vec::new();

        let mut curr_operators: Vec<&OCProcessTreeOperator> = Vec::new();
        match &self.root {
            OCProcessTreeNode::Operator(op) => {
                curr_operators.push(op);
            }
            OCProcessTreeNode::Leaf(leaf) => {
                result.push(leaf);
            }
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

    pub fn find_all_node_uuids(&self) -> Vec<&Uuid> {
        let mut result: Vec<&Uuid> = Vec::new();

        let mut curr_operators: Vec<&OCProcessTreeOperator> = Vec::new();
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

#[derive(Debug, Serialize, Deserialize)]
pub struct OCProcessTreeOperator {
    pub uuid: Uuid,
    pub operator_type: OCOperatorType,
    pub children: Vec<OCProcessTreeNode>,
}

impl OCProcessTreeOperator {
    pub fn new(operator_type: OCOperatorType) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            operator_type,
            children: Vec::new(),
        }
    }

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
        let mut start_ev_types = HashSet::new();
        let mut end_ev_types = HashSet::new();
        let mut skippable: bool;

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

        let mut directly_follow_ev_types: HashSet<_> = children_dfr
            .iter()
            .flat_map(|(_, _, dfr_evs_child, _)| dfr_evs_child.to_owned())
            .collect();

        match self.operator_type {
            OCOperatorType::Sequence => {
                skippable = true;

                let mut kept_end_evs = HashSet::new();
                let mut skip_forward = true;
                let mut kept_div_or_unrel_evs: HashSet<&EventType> = HashSet::new();

                // Iterate forward
                children_dfr.iter().zip(&self.children).for_each(
                    |((start_evs_child, end_evs_child, dfr_evs_child, skip_child), child)| {
                        directly_follow_ev_types.extend(dfr_evs_child);

                        if skip_forward {
                            start_ev_types.extend(start_evs_child);
                        }
                        skip_forward &= skip_child;

                        kept_end_evs.iter().for_each(|&previous_end_ev| {
                            start_evs_child.iter().for_each(|&curr_start_ev| {
                                directly_follow_ev_types.insert((previous_end_ev, curr_start_ev));
                            });
                        });

                        if *skip_child {
                            kept_end_evs.extend(end_evs_child);
                        } else {
                            kept_end_evs = end_evs_child.clone();
                        }

                        let is_unrelated_or_divergent = child.check_unrelated_or_divergent(
                            ob_type,
                            rel_ob_types_per_node,
                            div_ob_types_per_node,
                        );
                        skippable &= skip_child;

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

                let mut skip_backward = true;
                children_dfr.iter().zip(&self.children).rev().for_each(
                    |((_, end_evs_child, _, skip_child), child)| {
                        if skip_backward {
                            end_ev_types.extend(end_evs_child);
                            skip_backward &= skip_child;
                        }
                    },
                );
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

                let mut child_alphabets: Vec<HashSet<&EventType>> = self
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

                let concurrent_dfrs: HashSet<_> = (0..child_alphabets.len())
                    .flat_map(|i| Self::create_dfr_concurrent_alphabets(&child_alphabets, i))
                    .collect();

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

                    start_ev_types.iter().for_each(|(&start_ev)| {
                        end_ev_types.iter().for_each(|(&end_ev)| {
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

    pub fn create_dfr_concurrent_alphabets<'a>(
        alphabets: &Vec<HashSet<&'a EventType>>,
        basis_pos: usize,
    ) -> HashSet<(&'a EventType, &'a EventType)> {
        let basis_alphabet: &HashSet<&EventType> = alphabets.get(basis_pos).unwrap();
        let remainder_alphabet: HashSet<&EventType> = alphabets
            .iter()
            .enumerate()
            .flat_map(|(i, alphabet)| {
                if i != basis_pos {
                    alphabet.clone()
                } else {
                    HashSet::new()
                }
            })
            .collect();

        let mut result = HashSet::new();
        basis_alphabet.iter().for_each(|&from| {
            remainder_alphabet.iter().for_each(|&to| {
                result.insert((from, to));
            })
        });

        result
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
