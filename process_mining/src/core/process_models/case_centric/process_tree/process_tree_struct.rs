use crate::core::process_models::petri_net::{ArcType, Marking, PlaceID};
use crate::PetriNet;
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
  
    ///
    /// Calls either [`Operator::add_to_petri_net`] or [`Leaf::add_to_petri_net`] depending on the
    /// [`Node`] type.
    /// Either takes given in and out places as the start and end of the inserted workflow net.
    /// Edits the given Petri net by inserting the corresponding places and transitions of the
    /// (sub)tree.
    /// Returns the start and end places of the workflow net.
    ///
    pub fn add_to_petri_net(
        &self,
        net: &mut PetriNet,
        in_place: Option<PlaceID>,
        out_place: Option<PlaceID>,
    ) -> (PlaceID, PlaceID) {
        match self {
            Node::Operator(op) => op.add_to_petri_net(net, in_place, out_place),
            Node::Leaf(leaf) => leaf.add_to_petri_net(net, in_place, out_place),
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
                let mut children = if op.operator_type.is_associative() {
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

                // XOR-specific: at most one tau (silent leaf) is semantically
                // meaningful as a direct child. Remove duplicates introduced
                // when EmptyTraces fallthrough shells are folded upward.
                if op.operator_type == OperatorType::ExclusiveChoice {
                    let tau = Node::Leaf(Leaf { activity_label: LeafLabel::Tau });
                    let mut tau_seen = false;
                    children.retain(|c| {
                        if *c == tau {
                            if tau_seen { return false; }
                            tau_seen = true;
                        }
                        true
                    });
                }

                Node::Operator(Operator {
                    operator_type: op.operator_type,
                    children,
                })
            }
        }
    }
}

impl std::fmt::Display for OperatorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperatorType::Sequence       => write!(f, "SEQ"),
            OperatorType::ExclusiveChoice => write!(f, "XOR"),
            OperatorType::Concurrency    => write!(f, "AND"),
            OperatorType::Loop           => write!(f, "LOOP"),
        }
    }
}

impl std::fmt::Display for LeafLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeafLabel::Activity(s) => write!(f, "{s}"),
            LeafLabel::Tau         => write!(f, "tau"),
        }
    }
}

impl std::fmt::Display for Leaf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.activity_label)
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Node::Leaf(leaf)     => write!(f, "{leaf}"),
            Node::Operator(op)   => write!(f, "{op}"),
        }
    }
}

impl std::fmt::Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}(", self.operator_type)?;
        for (i, child) in self.children.iter().enumerate() {
            if i > 0 { write!(f, ", ")?; }
            write!(f, "{child}")?;
        }
        write!(f, ")")
    }
}

impl std::fmt::Display for ProcessTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.root)
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
    /// The fold is applied recursively bottom-up across the entire tree, so
    /// arbitrarily deep chains of the same associative operator are fully
    /// collapsed into a single flat node.
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

    ///
    /// Transforms a [`ProcessTree`] into a [`PetriNet`] according to the rules defined in
    /// "Process Mining: Data Science in Action" by Wil van der Aalst.
    /// Returns a workflow net, consisting of the [`PetriNet`], its input, and output place's [`PlaceID`]
    ///
    pub fn to_petri_net(&self) -> PetriNet {
        let mut petri_net = PetriNet::new();

        let (start_place, end_place) = self.root.add_to_petri_net(&mut petri_net, None, None);

        petri_net.initial_marking = Some(Marking::from([(start_place, 1)]));

        let mut final_marking = Marking::new();
        final_marking.insert(end_place, 1);
        petri_net.final_markings = Some(vec![final_marking]);

        petri_net
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

    ///
    /// Unfolds an operator and its descendants into corresponding place, transitions, and arcs and
    /// adds them to the input [`PetriNet`]. This routine is executed recursively.
    /// Optionally, the input and output place of the inserted workflow net can be defined.
    ///
    pub fn add_to_petri_net(
        &self,
        net: &mut PetriNet,
        in_place: Option<PlaceID>,
        out_place: Option<PlaceID>,
    ) -> (PlaceID, PlaceID) {
        let in_place = in_place.unwrap_or_else(|| net.add_place(None));
        let out_place = out_place.unwrap_or_else(|| net.add_place(None));

        let num_of_children = self.children.len();

        match self.operator_type {
            // For a sequence operator, the workflow nets are sequentially connected using each
            // previous output place as the input place of the following workflow net.
            // The first and last children consider the operators input and output place as their
            // input and output place, respectively.
            OperatorType::Sequence => {
                let mut last_in_place = in_place;

                self.children.iter().enumerate().for_each(|(pos, child)| {
                    let curr_out_place = {
                        if pos == num_of_children - 1 {
                            out_place
                        } else {
                            net.add_place(None)
                        }
                    };

                    child.add_to_petri_net(net, Some(last_in_place), Some(curr_out_place));

                    last_in_place = curr_out_place;
                })
            }
            // Considers for each child the input and output place of the operator as their input
            // and output place
            OperatorType::ExclusiveChoice => self.children.iter().for_each(|child| {
                child.add_to_petri_net(net, Some(in_place), Some(out_place));
            }),
            // Inserts and connects additional silent transitions as start and end and each child
            // creates new input and output places that are then connected to the silent start and
            // silent end transition.
            OperatorType::Concurrency => {
                let tau_start_transition = net.add_transition(None, None);
                let tau_end_transition = net.add_transition(None, None);

                net.add_arc(
                    ArcType::place_to_transition(in_place, tau_start_transition),
                    None,
                );
                net.add_arc(
                    ArcType::transition_to_place(tau_end_transition, out_place),
                    None,
                );

                self.children.iter().for_each(|child| {
                    let (child_start, child_end) = child.add_to_petri_net(net, None, None);

                    net.add_arc(
                        ArcType::transition_to_place(tau_start_transition, child_start),
                        None,
                    );
                    net.add_arc(
                        ArcType::place_to_transition(child_end, tau_end_transition),
                        None,
                    );
                })
            }
            // Inserts silent transitions to put the workflow net of the loop operator in choice
            // if other operators are in choice. All workflow nets share the same input and output
            // places. However, only the first child models the do-part going from input to output
            // place and every other child going from output to input place modelling the redo-part.
            OperatorType::Loop => {
                let tau_start_transition = net.add_transition(None, None);
                let tau_end_transition = net.add_transition(None, None);

                net.add_arc(
                    ArcType::place_to_transition(in_place, tau_start_transition),
                    None,
                );
                net.add_arc(
                    ArcType::transition_to_place(tau_end_transition, out_place),
                    None,
                );

                let loop_start_place = net.add_place(None);
                let loop_end_place = net.add_place(None);

                net.add_arc(
                    ArcType::transition_to_place(tau_start_transition, loop_start_place),
                    None,
                );
                net.add_arc(
                    ArcType::place_to_transition(loop_end_place, tau_end_transition),
                    None,
                );

                self.children.iter().enumerate().for_each(|(pos, child)| {
                    let (child_start, child_end) = if pos == 0 {
                        (loop_start_place, loop_end_place)
                    } else {
                        (loop_end_place, loop_start_place)
                    };

                    child.add_to_petri_net(net, Some(child_start), Some(child_end));
                })
            }
        }

        (in_place, out_place)
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

    ///
    /// Adds a transition to represent the leaf of a tree. Optionally, input and output
    /// places can be given to connect the newly created (silent) transition to. The output is
    /// the [`PlaceID`] of the input and output place, each.
    ///
    pub fn add_to_petri_net(
        &self,
        net: &mut PetriNet,
        in_place: Option<PlaceID>,
        out_place: Option<PlaceID>,
    ) -> (PlaceID, PlaceID) {
        let in_place = in_place.unwrap_or_else(|| net.add_place(None));
        let out_place = out_place.unwrap_or_else(|| net.add_place(None));

        let leaf_transition = {
            match &self.activity_label {
                LeafLabel::Activity(label) => net.add_transition(Some(label.clone()), None),
                LeafLabel::Tau => net.add_transition(None, None),
            }
        };

        net.add_arc(
            ArcType::place_to_transition(in_place, leaf_transition),
            None,
        );
        net.add_arc(
            ArcType::transition_to_place(leaf_transition, out_place),
            None,
        );

        (in_place, out_place)
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

    // Checking Seq(a,b,c)
    #[test]
    fn sequence_test() {
        let mut seq = Operator::new(OperatorType::Sequence);

        let leaf_a = Leaf::new(Some("a".to_string()));
        let leaf_b = Leaf::new(Some("b".to_string()));
        let leaf_c = Leaf::new(Some("c".to_string()));

        seq.children.push(Node::Leaf(leaf_a));
        seq.children.push(Node::Leaf(leaf_b));
        seq.children.push(Node::Leaf(leaf_c));

        let tree = ProcessTree::new(Node::Operator(seq));

        let net = tree.to_petri_net();

        assert_eq!(4, net.places.len());
        assert_eq!(3, net.transitions.len());
        assert_eq!(6, net.arcs.len());
    }

    // Checking Conc(a,b,c)
    #[test]
    fn concurrency_test() {
        let mut conc = Operator::new(OperatorType::Concurrency);

        let leaf_a = Leaf::new(Some("a".to_string()));
        let leaf_b = Leaf::new(Some("b".to_string()));
        let leaf_c = Leaf::new(Some("c".to_string()));

        conc.children.push(Node::Leaf(leaf_a));
        conc.children.push(Node::Leaf(leaf_b));
        conc.children.push(Node::Leaf(leaf_c));

        let tree = ProcessTree::new(Node::Operator(conc));

        let net = tree.to_petri_net();

        assert_eq!(8, net.places.len());
        assert_eq!(5, net.transitions.len());
        assert_eq!(14, net.arcs.len());
    }

    // Checking Loop(a,b,c)
    #[test]
    fn loop_test() {
        let mut loop_op = Operator::new(OperatorType::Loop);

        let leaf_a = Leaf::new(Some("a".to_string()));
        let leaf_b = Leaf::new(Some("b".to_string()));
        let leaf_c = Leaf::new(Some("c".to_string()));

        loop_op.children.push(Node::Leaf(leaf_a));
        loop_op.children.push(Node::Leaf(leaf_b));
        loop_op.children.push(Node::Leaf(leaf_c));

        let tree = ProcessTree::new(Node::Operator(loop_op));

        let net = tree.to_petri_net();

        assert_eq!(4, net.places.len());
        assert_eq!(5, net.transitions.len());
        assert_eq!(10, net.arcs.len());
    }

    // Checking Xor(a,b,c)
    #[test]
    fn choice_test() {
        let mut choice = Operator::new(OperatorType::ExclusiveChoice);

        let leaf_a = Leaf::new(Some("a".to_string()));
        let leaf_b = Leaf::new(Some("b".to_string()));
        let leaf_c = Leaf::new(Some("c".to_string()));

        choice.children.push(Node::Leaf(leaf_a));
        choice.children.push(Node::Leaf(leaf_b));
        choice.children.push(Node::Leaf(leaf_c));

        let tree = ProcessTree::new(Node::Operator(choice));

        let net = tree.to_petri_net();

        assert_eq!(2, net.places.len());
        assert_eq!(3, net.transitions.len());
        assert_eq!(6, net.arcs.len());
    }

    // Checking tau
    #[test]
    fn silent_test() {
        let leaf_tau = Leaf::new(None);

        let tree = ProcessTree::new(Node::Leaf(leaf_tau));

        let net = tree.to_petri_net();

        assert_eq!(2, net.places.len());
        assert_eq!(1, net.transitions.len());
        assert_eq!(2, net.arcs.len());

        net.transitions
            .iter()
            .for_each(|(_, t)| assert!(t.label.is_none()));
    }

    // Checking a
    #[test]
    fn leaf_test() {
        let leaf_a = Leaf::new(Some("a".to_string()));

        let tree = ProcessTree::new(Node::Leaf(leaf_a));

        let net = tree.to_petri_net();

        assert_eq!(2, net.places.len());
        assert_eq!(1, net.transitions.len());
        assert_eq!(2, net.arcs.len());

        net.transitions
            .iter()
            .for_each(|(_, t)| assert_eq!("a", t.label.clone().unwrap()));
    }

    // Checking Seq(a, Loop(e, Conc(a,b), f, tau), Xor(b, c, d))
    #[test]
    fn all_op_test() {
        let mut seq = Operator::new(OperatorType::Sequence);
        let leaf_a = Leaf::new(Some("a".to_string()));
        seq.children.push(Node::Leaf(leaf_a));

        let mut conc = Operator::new(OperatorType::Concurrency);
        let leaf_a = Leaf::new(Some("a".to_string()));
        let leaf_b = Leaf::new(Some("b".to_string()));

        conc.children.push(Node::Leaf(leaf_a));
        conc.children.push(Node::Leaf(leaf_b));

        let mut loop_op = Operator::new(OperatorType::Loop);
        let leaf_e = Leaf::new(Some("e".to_string()));
        let leaf_f = Leaf::new(Some("f".to_string()));
        let leaf_silent = Leaf::new(None);

        loop_op.children.push(Node::Leaf(leaf_e));
        loop_op.children.push(Node::Operator(conc));
        loop_op.children.push(Node::Leaf(leaf_f));
        loop_op.children.push(Node::Leaf(leaf_silent));

        let mut choice = Operator::new(OperatorType::ExclusiveChoice);
        let leaf_b = Leaf::new(Some("b".to_string()));
        let leaf_c = Leaf::new(Some("c".to_string()));
        let leaf_d = Leaf::new(Some("d".to_string()));
        choice.children.push(Node::Leaf(leaf_b));
        choice.children.push(Node::Leaf(leaf_c));
        choice.children.push(Node::Leaf(leaf_d));

        seq.children.push(Node::Operator(loop_op));
        seq.children.push(Node::Operator(choice));
        let tree = ProcessTree::new(Node::Operator(seq));

        let net = tree.to_petri_net();

        assert_eq!(10, net.places.len());
        assert_eq!(13, net.transitions.len());
        assert_eq!(28, net.arcs.len());
    }
}
