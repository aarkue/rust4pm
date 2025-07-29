use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use uuid::Uuid;

///
/// Leaf in a process tree
///
#[derive(Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum LeafLabel {
    /// Non-silent activity leaf
    Activity(String),
    /// Silent activity leaf
    Tau,
}

///
/// Node in a process tree
///
#[derive(Debug, Serialize, Deserialize)]
pub enum Node {
    /// Operator node of a process tree
    Operator(Operator),
    /// Leaf node of a process tree
    Leaf(Leaf),
}

impl Node {
    ///
    /// Returns the identifier of a node in a process tree
    ///
    pub fn get_uuid(&self) -> &Uuid {
        match self {
            Node::Operator(op) => &op.uuid,
            Node::Leaf(leaf) => &leaf.uuid,
        }
    }

    ///
    /// Creates a new [`Node::Operator`] with the given [`OperatorType`]
    ///
    pub fn new_operator(op_type: OperatorType) -> Self {
        Node::Operator(Operator::new(op_type))
    }

    ///
    /// Creates a new non-silent or silent leaf [`Node`]
    ///
    pub fn new_leaf(leaf_label: Option<String>) -> Self {
        Node::Leaf(Leaf::new(leaf_label))
    }

    ///
    /// Adds a node as child if the node is an operator node
    ///
    pub fn add_child(&mut self, child: Node) {
        match self {
            Node::Operator(op) => {
                op.children.push(child);
            }
            Node::Leaf(_) => {
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
            Node::Operator(op) => match op.operator_type {
                OperatorType::Loop => op.children.len() >= 2,
                _ => !op.children.is_empty(),
            },
            Node::Leaf(_) => true,
        }
    }
}

///
/// Operator type enum for [`Operator`]
///
#[derive(Debug, Serialize, Deserialize)]
pub enum OperatorType {
    /// Sequence operator
    Sequence,
    /// Exclusive choice operator
    ExclusiveChoice,
    /// Concurrency operator
    Concurrency,
    /// Loop operator that, if given, restricts a given number of repetitions
    Loop,
}

///
/// Object-centric process tree struct that contains [`Node`] as root
///
#[derive(Debug, Serialize)]
pub struct ProcessTree {
    /// The root of the object-centric process tree
    pub root: Node,
}

impl ProcessTree {
    ///
    /// Initializes the object-centric process tree with the given node as root
    ///
    pub fn new(root: Node) -> Self {
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
        let mut prev_node_ids: HashSet<Uuid> = HashSet::new();
        let mut curr_node_ids: HashSet<Uuid> = HashSet::new();
        curr_node_ids.insert(*self.root.get_uuid());

        let mut curr_operators: Vec<&Operator> = Vec::new();
        match &self.root {
            Node::Operator(op) => {
                curr_operators.push(op);
            }
            Node::Leaf(_) => {}
        };

        // A child counter to check the tree to be acyclic
        let mut children_count: usize = 1;

        // Checking all nodes to have the right number of children
        let mut all_op_nodes_valid = true;

        // Iterate through the tree to count up the children, if a node is the child of many
        // operator nodes, the count computed here and the number of nodes in the process tree
        // disagree
        let mut next_operators = Vec::new();
        while !prev_node_ids.eq(&curr_node_ids) {
            curr_operators.iter().for_each(|op| {
                op.children.iter().for_each(|child| match child {
                    Node::Operator(op) => {
                        all_op_nodes_valid &= child.check_children_valid();

                        next_operators.push(op);
                        children_count += 1;
                        curr_node_ids.insert(op.uuid);
                    }
                    Node::Leaf(leaf) => {
                        children_count += 1;
                        curr_node_ids.insert(leaf.uuid);
                    }
                })
            });

            curr_operators = next_operators;
            next_operators = Vec::new();
            prev_node_ids = curr_node_ids.clone();
        }

        all_op_nodes_valid && (children_count == curr_node_ids.len())
    }

    ///
    /// Returns all descendant [`Leaf`]
    ///
    pub fn find_all_leaves(&self) -> Vec<&Leaf> {
        let mut result: Vec<&Leaf> = Vec::new();

        let mut curr_operators: Vec<&Operator> = Vec::new();
        match &self.root {
            Node::Operator(op) => curr_operators.push(op),
            Node::Leaf(leaf) => result.push(leaf),
        };

        let mut next_operators = Vec::new();

        while !curr_operators.is_empty() {
            curr_operators.iter().for_each(|op| {
                op.children.iter().for_each(|child| match child {
                    Node::Operator(op) => next_operators.push(op),
                    Node::Leaf(leaf) => result.push(leaf),
                })
            });

            curr_operators = next_operators;
            next_operators = Vec::new();
        }

        result
    }

    ///
    /// Returns all `Uuid` of all [`Operator`] in the tree
    ///
    pub fn find_all_node_uuids(&self) -> Vec<&Uuid> {
        let mut result: Vec<&Uuid> = Vec::new();

        let mut curr_operators: Vec<&Operator> = Vec::new();
        match &self.root {
            Node::Operator(op) => {
                curr_operators.push(op);
                result.push(&op.uuid);
            }
            Node::Leaf(leaf) => {
                result.push(&leaf.uuid);
            }
        };

        let mut next_operators = Vec::new();

        while !curr_operators.is_empty() {
            curr_operators.iter().for_each(|op| {
                op.children.iter().for_each(|child| match child {
                    Node::Operator(op) => {
                        next_operators.push(op);
                        result.push(&op.uuid);
                    }
                    Node::Leaf(leaf) => result.push(&leaf.uuid),
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
pub struct Operator {
    /// The node ID
    pub uuid: Uuid,
    /// The [`OperatorType`] of the tree itself
    pub operator_type: OperatorType,
    /// The children nodes of the operator node
    pub children: Vec<Node>,
}

impl Operator {
    ///
    /// A constructor for the struct that initializes with the given [`OperatorType`] and
    /// otherwise a fresh [`Uuid`] and an empty list of children
    ///
    pub fn new(operator_type: OperatorType) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            operator_type,
            children: Vec::new(),
        }
    }

    ///
    /// Returns all descendant [`Node`]'s Uuids
    ///
    pub fn find_all_descendants_uuids(&self) -> Vec<&Uuid> {
        let mut result: Vec<&Uuid> = Vec::new();

        self.children.iter().for_each(|child| match child {
            Node::Operator(op) => {
                result.push(child.get_uuid());
                op.find_all_descendants_uuids().iter().for_each(|&uuid| {
                    result.push(uuid);
                });
            }
            Node::Leaf(_) => {
                result.push(child.get_uuid());
            }
        });

        result
    }
}

#[derive(Debug, Serialize, Deserialize)]
///
/// A leaf in a process tree
///
pub struct Leaf {
    /// The identifier of the leaf
    pub uuid: Uuid,
    /// The silent or non-silent activity label [`LeafLabel`]
    pub activity_label: LeafLabel,
}

impl Leaf {
    ///
    /// Creates a new [`Leaf`] either by using a given label or making it silent if a label
    /// is missing
    ///
    pub fn new(leaf_label: Option<String>) -> Self {
        if let Some(leaf_label) = leaf_label {
            Self {
                uuid: Uuid::new_v4(),
                activity_label: LeafLabel::Activity(leaf_label),
            }
        } else {
            Self {
                uuid: Uuid::new_v4(),
                activity_label: LeafLabel::Tau,
            }
        }
    }
}
