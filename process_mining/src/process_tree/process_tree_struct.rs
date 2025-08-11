use serde::{Deserialize, Serialize};

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
    /// Returns `true` if all nodes have the right number of children and if all operators have
    /// eventually descendants that are leaves.
    ///
    pub fn is_valid(&self) -> bool {
        if !self.root.check_children_valid() {
            return false;
        }

        // Checking all nodes to have the right number of children
        let mut all_op_nodes_valid = true;

        let mut curr_operators: Vec<&Operator> = Vec::new();
        match &self.root {
            Node::Operator(op) => {
                curr_operators.push(op);
                all_op_nodes_valid &= self.root.check_children_valid();
            }
            Node::Leaf(_) => {}
        };

        // Iterate through the tree to check all children's number of children to be valid
        let mut next_operators = Vec::new();
        while !curr_operators.is_empty() {
            curr_operators.iter().for_each(|op| {
                op.children.iter().for_each(|child| {
                    all_op_nodes_valid &= child.check_children_valid();

                    match child {
                        Node::Operator(op) => {
                            next_operators.push(op);
                        }
                        Node::Leaf(_) => {}
                    }
                })
            });

            curr_operators = next_operators;
            next_operators = Vec::new();
        }

        all_op_nodes_valid
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

}

///
/// An operator node in a process tree
///
#[derive(Debug, Serialize, Deserialize)]
pub struct Operator {
    /// The [`OperatorType`] of the tree itself
    pub operator_type: OperatorType,
    /// The children nodes of the operator node
    pub children: Vec<Node>,
}

impl Operator {
    ///
    /// A constructor for the struct that initializes with the given [`OperatorType`]
    ///
    pub fn new(operator_type: OperatorType) -> Self {
        Self {
            operator_type,
            children: Vec::new(),
        }
    }

    ///
    /// Returns all descendant [`Node`]s
    ///
    pub fn find_all_descendant_nodes(&self) -> Vec<&Node> {
        let mut result: Vec<&Node> = Vec::new();

        self.children.iter().for_each(|child| {
            result.push(child);

            match child {
                Node::Operator(op) => {
                    op.find_all_descendant_nodes().iter().for_each(|&node| {
                        result.push(node);
                    });
                }
                Node::Leaf(_) => {}
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
                activity_label: LeafLabel::Activity(leaf_label),
            }
        } else {
            Self {
                activity_label: LeafLabel::Tau,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::process_tree::process_tree_struct::{
        Leaf, Node, Operator, OperatorType, ProcessTree,
    };

    #[test]
    fn is_valid_test() {
        // SEQ() is not valid
        let op = Operator::new(OperatorType::Sequence);
        let pt = ProcessTree::new(Node::Operator(op));
        assert!(!pt.is_valid());

        // SEQ(a) is valid
        let mut op = Operator::new(OperatorType::Sequence);
        let leaf = Leaf::new(Some("a".to_string()));
        op.children.push(Node::Leaf(leaf));
        let pt = ProcessTree::new(Node::Operator(op));
        assert!(pt.is_valid());

        // SEQ(a, b) is valid
        let mut op = Operator::new(OperatorType::Sequence);
        let leaf = Leaf::new(Some("a".to_string()));
        op.children.push(Node::Leaf(leaf));
        let leaf = Leaf::new(Some("b".to_string()));
        op.children.push(Node::Leaf(leaf));
        let pt = ProcessTree::new(Node::Operator(op));
        assert!(pt.is_valid());

        // LOOP(a) is not valid
        let mut op = Operator::new(OperatorType::Loop);
        let leaf = Leaf::new(Some("a".to_string()));
        op.children.push(Node::Leaf(leaf));
        let pt = ProcessTree::new(Node::Operator(op));
        assert!(!pt.is_valid());

        // LOOP(a, a) is valid
        let mut op = Operator::new(OperatorType::Loop);
        let leaf = Leaf::new(Some("a".to_string()));
        op.children.push(Node::Leaf(leaf));
        let leaf = Leaf::new(Some("a".to_string()));
        op.children.push(Node::Leaf(leaf));
        let pt = ProcessTree::new(Node::Operator(op));
        assert!(pt.is_valid());

        // SEQ(XOR(a,b), LOOP(c, d), Parallel(e, f, g)) is valid
        let mut xor_node = Operator::new(OperatorType::ExclusiveChoice);
        let mut loop_node = Operator::new(OperatorType::Loop);
        let mut parallel_node = Operator::new(OperatorType::Concurrency);

        let mut seq_node = Operator::new(OperatorType::Sequence);

        xor_node
            .children
            .push(Node::Leaf(Leaf::new(Some("a".to_string()))));
        xor_node
            .children
            .push(Node::Leaf(Leaf::new(Some("b".to_string()))));

        loop_node
            .children
            .push(Node::Leaf(Leaf::new(Some("c".to_string()))));
        loop_node
            .children
            .push(Node::Leaf(Leaf::new(Some("d".to_string()))));

        parallel_node
            .children
            .push(Node::Leaf(Leaf::new(Some("e".to_string()))));
        parallel_node
            .children
            .push(Node::Leaf(Leaf::new(Some("f".to_string()))));
        parallel_node
            .children
            .push(Node::Leaf(Leaf::new(Some("g".to_string()))));

        seq_node.children.push(Node::Operator(xor_node));
        seq_node.children.push(Node::Operator(loop_node));
        seq_node.children.push(Node::Operator(parallel_node));

        let pt = ProcessTree::new(Node::Operator(seq_node));
        assert!(pt.is_valid());
    }
}
