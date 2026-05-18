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
#[derive(Debug, Serialize, Deserialize, PartialEq)]
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

    /// Recursively folds this node by merging children that share the same
    /// associative operator into the current node.
    ///
    /// The fold is applied **bottom-up**: children are folded first, and only
    /// then the current node checks whether any of its (now-folded) children
    /// can be inlined.
    ///
    /// **Example** – `SEQ(SEQ(a, b), c)` becomes `SEQ(a, b, c)`.
    ///
    /// Leaf nodes are returned unchanged.
    pub fn fold(self) -> Self {
        match self {
            Node::Leaf(_) => self,
            Node::Operator(op) => {
                // Recursively fold all children first (bottom-up).
                let folded_children: Vec<Node> =
                    op.children.into_iter().map(|child| child.fold()).collect();

                // If the current operator is associative, inline any child
                // that carries the same operator type.
                let children = if op.operator_type.is_associative() {
                    let mut flattened = Vec::with_capacity(folded_children.len());
                    for child in folded_children {
                        match child {
                            Node::Operator(ref inner)
                                if inner.operator_type == op.operator_type =>
                            {
                                // Consume the child and move its children up.
                                if let Node::Operator(inner) = child {
                                    flattened.extend(inner.children);
                                }
                            }
                            other => flattened.push(other),
                        }
                    }
                    flattened
                } else {
                    folded_children
                };

                Node::Operator(Operator {
                    operator_type: op.operator_type,
                    children,
                })
            }
        }
    }
}

///
/// Operator type enum for [`Operator`]
///
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
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

impl OperatorType {
    /// Returns `true` if this operator is associative.
    ///
    /// The associative operators are [`Sequence`](OperatorType::Sequence),
    /// [`ExclusiveChoice`](OperatorType::ExclusiveChoice), and
    /// [`Concurrency`](OperatorType::Concurrency).  The [`Loop`](OperatorType::Loop)
    /// operator is **not** associative because its first child (the body) and
    /// subsequent children (the redo / exit branches) carry different semantic
    /// roles, so merging nested loops would change the language.
    pub fn is_associative(self) -> bool {
        matches!(
            self,
            OperatorType::Sequence
                | OperatorType::ExclusiveChoice
                | OperatorType::Concurrency
        )
    }
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

    /// Folds the process tree by merging nodes whose operator is associative.
    ///
    /// For the associative operators [`Sequence`](OperatorType::Sequence),
    /// [`ExclusiveChoice`](OperatorType::ExclusiveChoice), and
    /// [`Concurrency`](OperatorType::Concurrency) the following identity holds:
    ///
    /// ```text
    /// OP(OP(a, b), c)  ≡  OP(a, b, c)
    /// ```
    ///
    /// The fold is applied recursively bottom-up across the entire tree, so
    /// arbitrarily deep chains of the same associative operator are fully
    /// collapsed into a single flat node.
    ///
    /// [`Loop`](OperatorType::Loop) nodes are **not** folded because their
    /// child positions carry different semantic roles.
    ///
    /// # Returns
    /// A new [`ProcessTree`] with all associative operator chains collapsed.
    pub fn fold(self) -> Self {
        ProcessTree::new(self.root.fold())
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
#[derive(PartialEq)]
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
#[derive(PartialEq)]
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
    use crate::core::process_models::case_centric::process_tree::process_tree_struct::{
        Leaf, Node, Operator, OperatorType, ProcessTree,
    };

    // ── folding tests ────────────────────────────────────────────────────────

    #[test]
    fn fold_flat_sequence_unchanged() {
        // SEQ(a, b, c) has no nested SEQ — the tree must be returned as-is.
        let mut seq = Operator::new(OperatorType::Sequence);
        seq.children.push(Node::new_leaf(Some("a".into())));
        seq.children.push(Node::new_leaf(Some("b".into())));
        seq.children.push(Node::new_leaf(Some("c".into())));
        let pt = ProcessTree::new(Node::Operator(seq)).fold();

        let mut expected = Operator::new(OperatorType::Sequence);
        expected.children.push(Node::new_leaf(Some("a".into())));
        expected.children.push(Node::new_leaf(Some("b".into())));
        expected.children.push(Node::new_leaf(Some("c".into())));
        assert_eq!(pt.root, Node::Operator(expected));
    }

    #[test]
    fn fold_nested_sequence() {
        // SEQ(SEQ(a, b), c)  →  SEQ(a, b, c)
        let mut inner = Operator::new(OperatorType::Sequence);
        inner.children.push(Node::new_leaf(Some("a".into())));
        inner.children.push(Node::new_leaf(Some("b".into())));

        let mut outer = Operator::new(OperatorType::Sequence);
        outer.children.push(Node::Operator(inner));
        outer.children.push(Node::new_leaf(Some("c".into())));

        let pt = ProcessTree::new(Node::Operator(outer)).fold();

        let mut expected = Operator::new(OperatorType::Sequence);
        expected.children.push(Node::new_leaf(Some("a".into())));
        expected.children.push(Node::new_leaf(Some("b".into())));
        expected.children.push(Node::new_leaf(Some("c".into())));
        assert_eq!(pt.root, Node::Operator(expected));
    }

    #[test]
    fn fold_deeply_nested_sequence() {
        // SEQ(SEQ(SEQ(a, b), c), d)  →  SEQ(a, b, c, d)
        let mut innermost = Operator::new(OperatorType::Sequence);
        innermost.children.push(Node::new_leaf(Some("a".into())));
        innermost.children.push(Node::new_leaf(Some("b".into())));

        let mut middle = Operator::new(OperatorType::Sequence);
        middle.children.push(Node::Operator(innermost));
        middle.children.push(Node::new_leaf(Some("c".into())));

        let mut outer = Operator::new(OperatorType::Sequence);
        outer.children.push(Node::Operator(middle));
        outer.children.push(Node::new_leaf(Some("d".into())));

        let pt = ProcessTree::new(Node::Operator(outer)).fold();

        let mut expected = Operator::new(OperatorType::Sequence);
        expected.children.push(Node::new_leaf(Some("a".into())));
        expected.children.push(Node::new_leaf(Some("b".into())));
        expected.children.push(Node::new_leaf(Some("c".into())));
        expected.children.push(Node::new_leaf(Some("d".into())));
        assert_eq!(pt.root, Node::Operator(expected));
    }

    #[test]
    fn fold_xor_nested() {
        // XOR(XOR(a, b), c)  →  XOR(a, b, c)
        let mut inner = Operator::new(OperatorType::ExclusiveChoice);
        inner.children.push(Node::new_leaf(Some("a".into())));
        inner.children.push(Node::new_leaf(Some("b".into())));

        let mut outer = Operator::new(OperatorType::ExclusiveChoice);
        outer.children.push(Node::Operator(inner));
        outer.children.push(Node::new_leaf(Some("c".into())));

        let pt = ProcessTree::new(Node::Operator(outer)).fold();

        let mut expected = Operator::new(OperatorType::ExclusiveChoice);
        expected.children.push(Node::new_leaf(Some("a".into())));
        expected.children.push(Node::new_leaf(Some("b".into())));
        expected.children.push(Node::new_leaf(Some("c".into())));
        assert_eq!(pt.root, Node::Operator(expected));
    }

    #[test]
    fn fold_concurrency_nested() {
        // AND(AND(a, b), c)  →  AND(a, b, c)
        let mut inner = Operator::new(OperatorType::Concurrency);
        inner.children.push(Node::new_leaf(Some("a".into())));
        inner.children.push(Node::new_leaf(Some("b".into())));

        let mut outer = Operator::new(OperatorType::Concurrency);
        outer.children.push(Node::Operator(inner));
        outer.children.push(Node::new_leaf(Some("c".into())));

        let pt = ProcessTree::new(Node::Operator(outer)).fold();

        let mut expected = Operator::new(OperatorType::Concurrency);
        expected.children.push(Node::new_leaf(Some("a".into())));
        expected.children.push(Node::new_leaf(Some("b".into())));
        expected.children.push(Node::new_leaf(Some("c".into())));
        assert_eq!(pt.root, Node::Operator(expected));
    }

    #[test]
    fn fold_does_not_merge_different_operators() {
        // SEQ(XOR(a, b), c)  — different operator, must stay unchanged.
        // Build two identical XOR nodes: one for the input, one for expected.
        let make_inner = || {
            let mut xor = Operator::new(OperatorType::ExclusiveChoice);
            xor.children.push(Node::new_leaf(Some("a".into())));
            xor.children.push(Node::new_leaf(Some("b".into())));
            xor
        };

        let mut outer = Operator::new(OperatorType::Sequence);
        outer.children.push(Node::Operator(make_inner()));
        outer.children.push(Node::new_leaf(Some("c".into())));

        let pt = ProcessTree::new(Node::Operator(outer)).fold();

        let mut expected_outer = Operator::new(OperatorType::Sequence);
        expected_outer.children.push(Node::Operator(make_inner()));
        expected_outer.children.push(Node::new_leaf(Some("c".into())));
        assert_eq!(pt.root, Node::Operator(expected_outer));
    }

    #[test]
    fn fold_does_not_merge_loop() {
        // LOOP(LOOP(a, tau), tau)  — Loop is not associative, must stay unchanged.
        let make_inner = || {
            let mut lp = Operator::new(OperatorType::Loop);
            lp.children.push(Node::new_leaf(Some("a".into())));
            lp.children.push(Node::new_leaf(None));
            lp
        };

        let mut outer = Operator::new(OperatorType::Loop);
        outer.children.push(Node::Operator(make_inner()));
        outer.children.push(Node::new_leaf(None));

        let pt = ProcessTree::new(Node::Operator(outer)).fold();

        let mut expected = Operator::new(OperatorType::Loop);
        expected.children.push(Node::Operator(make_inner()));
        expected.children.push(Node::new_leaf(None));
        assert_eq!(pt.root, Node::Operator(expected));
    }

    #[test]
    fn fold_mixed_tree() {
        // SEQ( SEQ(a, b), LOOP(c, tau), SEQ(d, e) )
        // The two SEQ children get merged; the LOOP stays in place.
        // Result: SEQ(a, b, LOOP(c, tau), d, e)
        let make_loop = || {
            let mut lp = Operator::new(OperatorType::Loop);
            lp.children.push(Node::new_leaf(Some("c".into())));
            lp.children.push(Node::new_leaf(None));
            lp
        };

        let mut seq1 = Operator::new(OperatorType::Sequence);
        seq1.children.push(Node::new_leaf(Some("a".into())));
        seq1.children.push(Node::new_leaf(Some("b".into())));

        let mut seq2 = Operator::new(OperatorType::Sequence);
        seq2.children.push(Node::new_leaf(Some("d".into())));
        seq2.children.push(Node::new_leaf(Some("e".into())));

        let mut root = Operator::new(OperatorType::Sequence);
        root.children.push(Node::Operator(seq1));
        root.children.push(Node::Operator(make_loop()));
        root.children.push(Node::Operator(seq2));

        let pt = ProcessTree::new(Node::Operator(root)).fold();

        let mut expected = Operator::new(OperatorType::Sequence);
        expected.children.push(Node::new_leaf(Some("a".into())));
        expected.children.push(Node::new_leaf(Some("b".into())));
        expected.children.push(Node::Operator(make_loop()));
        expected.children.push(Node::new_leaf(Some("d".into())));
        expected.children.push(Node::new_leaf(Some("e".into())));
        assert_eq!(pt.root, Node::Operator(expected));
    }

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
