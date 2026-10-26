//! Laying out and drawing a [`ProcessTree`] as a tidy tree, in one of two [`TreeLook`]s.

use graphlay::json::TreeRequest;
use graphlay::tree::{TreeEdgeStyle, TreeOptions};
use graphlay::{Direction, Edge, Graph, Node, Shape, Size};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::core::process_models::process_tree::{
    LeafLabel, Node as PtNode, OperatorType, ProcessTree,
};
use crate::visualization::engine::{tree_options_of, tree_request, VisualizationError};
use crate::visualization::graph_build::{
    leaf_size_with, GraphAcc, LabelWidth, LEAF_HEIGHT, OPERATOR_SIZE,
};
use crate::visualization::model::ModelLayoutRequest;
use crate::visualization::styling::BaseStyle;

/// A silent leaf in the classic look.
pub const SILENT_SIZE: Size = Size::new(24.0, 24.0);
/// Height of an operator pill in the color-coded look.
pub const PILL_HEIGHT: f64 = 26.0;

/// The symbol an operator is drawn with.
pub fn operator_symbol(operator: &OperatorType) -> &'static str {
    match operator {
        OperatorType::Sequence => "→",
        OperatorType::ExclusiveChoice => "×",
        OperatorType::Concurrency => "∧",
        OperatorType::Loop => "↺",
        OperatorType::Interleaving => "↔",
        OperatorType::InclusiveChoice => "∨",
    }
}

/// The word an operator is named with in the color-coded look.
pub fn operator_name(operator: &OperatorType) -> &'static str {
    match operator {
        OperatorType::Sequence => "sequence",
        OperatorType::ExclusiveChoice => "xor",
        OperatorType::Concurrency => "parallel",
        OperatorType::Loop => "loop",
        OperatorType::Interleaving => "interleaving",
        OperatorType::InclusiveChoice => "or",
    }
}

/// The text a leaf is drawn with: its activity, or tau for a silent leaf.
pub fn leaf_text(label: &LeafLabel) -> String {
    match label {
        LeafLabel::Activity(a) => a.clone(),
        LeafLabel::Tau => "τ".to_string(),
    }
}

/// How a process tree is drawn.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum TreeLook {
    /// Operator circles with their symbol, compact leaf boxes and straight lines.
    #[default]
    Classic,
    /// Named operator pills, one color per operator type, which its lines and leaves take.
    ColorCoded,
}

impl TreeLook {
    /// The tidy-tree options for this look, growing in `direction`, or top to bottom.
    pub fn tree_options(self, direction: Option<Direction>) -> TreeOptions {
        let (sibling_gap, subtree_gap, row_gap, edge_style) = match self {
            TreeLook::Classic => (14.0, 18.0, 46.0, TreeEdgeStyle::Straight),
            TreeLook::ColorCoded => (12.0, 20.0, 30.0, TreeEdgeStyle::Elbow),
        };
        TreeOptions {
            direction: direction.unwrap_or(Direction::TopBottom),
            sibling_gap,
            subtree_gap,
            row_gap,
            edge_style,
            ..TreeOptions::default()
        }
    }
}

/// A node of a [`ProcessTree`], by its path of child indices from the root, e.g.,
/// `{"kind": "leaf", "path": [0, 2]}`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "path", rename_all = "snake_case")]
pub enum ProcessTreeNodeRef {
    /// An operator node, by path from the root.
    Operator(Vec<usize>),
    /// A leaf, by path from the root.
    Leaf(Vec<usize>),
}

impl ProcessTreeNodeRef {
    /// The child indices leading from the root to this node.
    pub fn path(&self) -> &[usize] {
        match self {
            ProcessTreeNodeRef::Operator(path) | ProcessTreeNodeRef::Leaf(path) => path,
        }
    }
}

/// The node of `tree` at `path`, or `None` if the path leaves the tree.
pub fn node_at<'a>(tree: &'a ProcessTree, path: &[usize]) -> Option<&'a PtNode> {
    let mut node = &tree.root;
    for &step in path {
        match node {
            PtNode::Operator(op) => node = op.children.get(step)?,
            PtNode::Leaf(_) => return None,
        }
    }
    Some(node)
}

/// How a [`ProcessTree`] is turned into a [`Graph`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(JsonSchema))]
#[serde(default)]
pub struct ProcessTreeBuildOptions {
    /// Which way the tree grows. `None` means top to bottom.
    pub direction: Option<Direction>,
    /// How the tree is drawn. It also sets the node sizes and gaps.
    pub look: TreeLook,
    /// Fields of the tidy-tree options to change, as a partial [`TreeOptions`] object deep-merged
    /// over [`ProcessTreeBuildOptions::tree_options`], e.g., `{"row_gap": 60}`. `null` changes
    /// nothing.
    #[cfg_attr(feature = "bindings", schemars(with = "Option<TreeOptions>"))]
    pub layout_patch: Value,
}

impl ProcessTreeBuildOptions {
    /// The tidy-tree options for these build options, without
    /// [`ProcessTreeBuildOptions::layout_patch`].
    pub fn tree_options(&self) -> TreeOptions {
        self.look.tree_options(self.direction)
    }

    /// The tidy-tree request for `graph`: [`ProcessTreeBuildOptions::tree_options`], then
    /// [`ProcessTreeBuildOptions::layout_patch`].
    ///
    /// # Errors
    ///
    /// [`VisualizationError::InvalidOptions`] if the patch does not describe valid options.
    pub fn layout_request(&self, graph: Graph) -> Result<TreeRequest, VisualizationError> {
        tree_request(graph, self.tree_options(), &self.layout_patch)
    }

    /// The direction the tree grows in, after the patch.
    fn drawn_direction(&self) -> Result<Direction, VisualizationError> {
        Ok(tree_options_of(&self.layout_request(Graph::default())?)?.direction)
    }
}

/// The tidy-tree request of a [`ProcessTree`]. Each edge's ref is the child it leads to; its parent
/// is the node at that path without the last step.
pub type ProcessTreeLayoutRequest =
    ModelLayoutRequest<TreeRequest, ProcessTreeNodeRef, ProcessTreeNodeRef>;

/// The color of each operator type in the color-coded look.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct OperatorColors {
    /// Sequence.
    pub sequence: String,
    /// Exclusive choice.
    pub exclusive_choice: String,
    /// Concurrency.
    pub concurrency: String,
    /// Loop.
    pub loop_: String,
    /// Interleaving.
    pub interleaving: String,
    /// Inclusive choice.
    pub inclusive_choice: String,
}

impl OperatorColors {
    /// The color of `operator`.
    pub fn of(&self, operator: &OperatorType) -> &str {
        match operator {
            OperatorType::Sequence => &self.sequence,
            OperatorType::ExclusiveChoice => &self.exclusive_choice,
            OperatorType::Concurrency => &self.concurrency,
            OperatorType::Loop => &self.loop_,
            OperatorType::Interleaving => &self.interleaving,
            OperatorType::InclusiveChoice => &self.inclusive_choice,
        }
    }
}

impl Default for OperatorColors {
    fn default() -> Self {
        OperatorColors {
            sequence: "#2563eb".to_string(),
            exclusive_choice: "#ea580c".to_string(),
            concurrency: "#16a34a".to_string(),
            loop_: "#9333ea".to_string(),
            interleaving: "#db2777".to_string(),
            inclusive_choice: "#0891b2".to_string(),
        }
    }
}

/// Colors a process tree is drawn with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct ProcessTreeStyle {
    /// Colors and paddings shared with every other model.
    pub base: BaseStyle,
    /// Fill of an operator circle in the classic look.
    pub operator_fill: String,
    /// Color of the operator symbol in the classic look.
    pub operator_text: String,
    /// Point size of the operator symbol in the classic look.
    pub operator_symbol_size: f64,
    /// Fill of a leaf box in the classic look.
    pub leaf_fill: String,
    /// Fill of a silent leaf in the classic look.
    pub silent_fill: String,
    /// Color of a silent leaf's tau in the color-coded look.
    pub silent_text: String,
    /// The color of each operator type in the color-coded look.
    pub operator_colors: OperatorColors,
}

impl Default for ProcessTreeStyle {
    fn default() -> Self {
        ProcessTreeStyle {
            base: BaseStyle::default(),
            operator_fill: "#ffffff".to_string(),
            operator_text: "#1f2937".to_string(),
            operator_symbol_size: 18.0,
            leaf_fill: "#f8fafc".to_string(),
            silent_fill: "#111827".to_string(),
            silent_text: "#6b7280".to_string(),
            operator_colors: OperatorColors::default(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum TreeItem<'a> {
    Operator(OperatorType),
    Activity(&'a str),
    Silent,
}

impl<'a> TreeItem<'a> {
    fn of(node: &'a PtNode) -> Self {
        match node {
            PtNode::Operator(op) => TreeItem::Operator(op.operator_type),
            PtNode::Leaf(leaf) => match &leaf.activity_label {
                LeafLabel::Activity(a) => TreeItem::Activity(a),
                LeafLabel::Tau => TreeItem::Silent,
            },
        }
    }
}

fn pill_label(operator: &OperatorType) -> String {
    format!("{} {}", operator_symbol(operator), operator_name(operator))
}

fn tree_node(item: TreeItem<'_>, look: TreeLook, measure: LabelWidth<'_>) -> Node {
    let (size, shape) = match (item, look) {
        (TreeItem::Operator(..), TreeLook::Classic) => (OPERATOR_SIZE, Shape::Ellipse),
        (TreeItem::Operator(operator), TreeLook::ColorCoded) => (
            Size::new(measure(&pill_label(&operator)), PILL_HEIGHT),
            Shape::Box,
        ),
        (TreeItem::Activity(label), _) => (leaf_size_with(label, measure), Shape::Box),
        (TreeItem::Silent, TreeLook::Classic) => (SILENT_SIZE, Shape::Box),
        (TreeItem::Silent, TreeLook::ColorCoded) => {
            (Size::new(LEAF_HEIGHT, LEAF_HEIGHT), Shape::Box)
        }
    };
    Node {
        size,
        shape,
        ..Node::default()
    }
}

fn node_ref(node: &PtNode, path: &[usize]) -> ProcessTreeNodeRef {
    match node {
        PtNode::Operator(_) => ProcessTreeNodeRef::Operator(path.to_vec()),
        PtNode::Leaf(_) => ProcessTreeNodeRef::Leaf(path.to_vec()),
    }
}

fn add_node(
    acc: &mut GraphAcc<ProcessTreeNodeRef, ProcessTreeNodeRef>,
    node: &PtNode,
    path: &mut Vec<usize>,
    look: TreeLook,
    measure: LabelWidth<'_>,
) -> graphlay::NodeId {
    let key = path_key(path);
    let id = acc.add_node(key, tree_node(TreeItem::of(node), look, measure), || {
        node_ref(node, path)
    });
    if let PtNode::Operator(op) = node {
        for (index, child) in op.children.iter().enumerate() {
            path.push(index);
            let child_id = add_node(acc, child, path, look, measure);
            acc.add_edge(Edge::new(id, child_id), node_ref(child, path));
            path.pop();
        }
    }
    id
}

fn path_key(path: &[usize]) -> String {
    let mut key = String::from("n");
    for step in path {
        key.push('.');
        key.push_str(&step.to_string());
    }
    key
}

mod draw {
    use super::*;
    use graphlay::layout::{EdgeRoute, Placement};
    use std::collections::HashMap;

    use graphlay::svg::{
        MarkerRef, NodeContent, Point as SPoint, SceneEdge, SceneNode, Shape as SShape, Stroke,
        TextRun,
    };
    use graphlay::Layout;

    use crate::visualization::model::Visualize;
    use crate::visualization::styling::{leaf_runs, scene_node};

    // Without a stroke the renderer falls back to the theme's.
    fn no_border() -> Option<Stroke> {
        Some(Stroke::new("none", 0.0))
    }

    fn centered(text: &str, size: f64, weight: f64, color: &str) -> NodeContent {
        NodeContent::Text(TextRun {
            text: text.to_string(),
            size: Some(size),
            weight: Some(weight),
            color: Some(color.to_string()),
            ..TextRun::default()
        })
    }

    fn tree_scene_node(
        item: TreeItem<'_>,
        placement: &Placement,
        look: TreeLook,
        parent: Option<OperatorType>,
        style: &ProcessTreeStyle,
    ) -> SceneNode {
        let base = &style.base;
        let leaf_border = match (look, parent) {
            (TreeLook::ColorCoded, Some(operator)) => style.operator_colors.of(&operator),
            _ => &base.node_border,
        };
        let rounded = |mut n: SceneNode, radius: f64| {
            n.shape = SShape::Rect {
                corner_radius: radius,
            };
            n
        };
        match (item, look) {
            (TreeItem::Operator(operator), TreeLook::Classic) => {
                let mut n = scene_node(
                    placement,
                    Shape::Ellipse,
                    Some(style.operator_fill.clone()),
                    Some(Stroke::new(&base.node_border, 1.5)),
                    base,
                );
                n.content.push(centered(
                    operator_symbol(&operator),
                    style.operator_symbol_size,
                    600.0,
                    &style.operator_text,
                ));
                n
            }
            (TreeItem::Operator(operator), TreeLook::ColorCoded) => {
                let mut n = scene_node(
                    placement,
                    Shape::Box,
                    Some(style.operator_colors.of(&operator).to_string()),
                    no_border(),
                    base,
                );
                n.content.push(centered(
                    &pill_label(&operator),
                    12.0,
                    600.0,
                    &base.node_fill,
                ));
                rounded(n, placement.size.height / 2.0)
            }
            (TreeItem::Activity(label), TreeLook::Classic) => {
                let mut n = scene_node(
                    placement,
                    Shape::Box,
                    Some(style.leaf_fill.clone()),
                    Some(Stroke::new(&base.node_border, 1.25)),
                    base,
                );
                n.content.extend(leaf_runs(label, base, &base.text));
                rounded(n, 3.0)
            }
            (TreeItem::Activity(label), TreeLook::ColorCoded) => {
                let mut n = scene_node(
                    placement,
                    Shape::Box,
                    Some(base.node_fill.clone()),
                    Some(Stroke::new(leaf_border, 1.5)),
                    base,
                );
                n.content.extend(leaf_runs(label, base, &base.text));
                rounded(n, 6.0)
            }
            (TreeItem::Silent, TreeLook::Classic) => {
                let mut n = scene_node(
                    placement,
                    Shape::Box,
                    Some(style.silent_fill.clone()),
                    no_border(),
                    base,
                );
                n.content.push(centered("τ", 13.0, 600.0, &base.node_fill));
                rounded(n, 2.0)
            }
            (TreeItem::Silent, TreeLook::ColorCoded) => {
                let mut n = scene_node(
                    placement,
                    Shape::Box,
                    Some(base.node_fill.clone()),
                    Some(Stroke {
                        dash: Some("3 2".to_string()),
                        ..Stroke::new(leaf_border, 1.25)
                    }),
                    base,
                );
                n.content
                    .push(centered("τ", 13.0, 500.0, &style.silent_text));
                rounded(n, 6.0)
            }
        }
    }

    /// Where each row of a tree layout begins and ends, along the axis it grows.
    #[derive(Debug)]
    struct Rows {
        vertical: bool,
        bands: HashMap<i32, (f64, f64)>,
    }

    impl Rows {
        fn of(layout: &Layout, direction: Direction) -> Rows {
            let vertical = direction == Direction::TopBottom;
            let mut rows = Rows {
                vertical,
                bands: HashMap::new(),
            };
            for p in &layout.nodes {
                let (lo, hi) = rows.extent(p);
                let band = rows.bands.entry(p.rank.unwrap_or(0)).or_insert((lo, hi));
                *band = (band.0.min(lo), band.1.max(hi));
            }
            rows
        }

        fn extent(&self, p: &Placement) -> (f64, f64) {
            let (center, half) = if self.vertical {
                (p.center.y, p.size.height / 2.0)
            } else {
                (p.center.x, p.size.width / 2.0)
            };
            (center - half, center + half)
        }

        fn band(&self, p: &Placement) -> (f64, f64) {
            match p.rank.and_then(|rank| self.bands.get(&rank)) {
                Some(band) => *band,
                None => self.extent(p),
            }
        }

        fn point(&self, along: f64, across: f64) -> SPoint {
            if self.vertical {
                SPoint::new(across, along)
            } else {
                SPoint::new(along, across)
            }
        }

        /// Only the gap between two rows is crossed diagonally, so the line never runs over a node.
        fn classic_points(&self, from: &Placement, to: &Placement) -> Vec<SPoint> {
            let across = |p: &Placement| {
                if self.vertical {
                    p.center.x
                } else {
                    p.center.y
                }
            };
            let points = [
                self.point(self.extent(from).1, across(from)),
                self.point(self.band(from).1, across(from)),
                self.point(self.band(to).0, across(to)),
                self.point(self.extent(to).0, across(to)),
            ];
            let mut out: Vec<SPoint> = Vec::with_capacity(points.len());
            for p in points {
                if out
                    .last()
                    .is_none_or(|q| (q.x - p.x).abs() > 0.01 || (q.y - p.y).abs() > 0.01)
                {
                    out.push(p);
                }
            }
            out
        }
    }

    fn tree_scene_edge(
        route: &EdgeRoute,
        (from, to): (&Placement, &Placement),
        rows: &Rows,
        look: TreeLook,
        parent: Option<OperatorType>,
        style: &ProcessTreeStyle,
    ) -> Option<SceneEdge> {
        let (points, color, width, corner): (Vec<SPoint>, &str, f64, f64) = match look {
            TreeLook::Classic => (rows.classic_points(from, to), &style.base.edge, 1.5, 0.0),
            TreeLook::ColorCoded => (
                route.points.iter().map(|p| SPoint::new(p.x, p.y)).collect(),
                match parent {
                    Some(operator) => style.operator_colors.of(&operator),
                    None => &style.base.edge,
                },
                1.75,
                6.0,
            ),
        };
        (points.len() >= 2).then(|| SceneEdge {
            points,
            stroke: Stroke::new(color, width),
            corner_radius: corner,
            marker_end: MarkerRef::None,
            ..SceneEdge::default()
        })
    }

    /// The rows of the layout and where each tree node went, for drawing the edges.
    #[derive(Debug)]
    pub struct TreeContext {
        rows: Rows,
        index: HashMap<Vec<usize>, usize>,
        placements: Vec<Placement>,
    }

    fn operator_at(tree: &ProcessTree, path: &[usize]) -> Option<OperatorType> {
        match node_at(tree, path) {
            Some(PtNode::Operator(op)) => Some(op.operator_type),
            _ => None,
        }
    }

    fn parent_operator(tree: &ProcessTree, path: &[usize]) -> Option<OperatorType> {
        operator_at(tree, path.split_last()?.1)
    }

    impl Visualize for ProcessTree {
        type NodeRef = ProcessTreeNodeRef;
        type EdgeRef = ProcessTreeNodeRef;
        type Options = ProcessTreeBuildOptions;
        type Style = ProcessTreeStyle;
        type Request = TreeRequest;
        type Context = TreeContext;

        fn to_graph(
            &self,
            options: &ProcessTreeBuildOptions,
            measure: LabelWidth<'_>,
        ) -> (Graph, Vec<ProcessTreeNodeRef>, Vec<ProcessTreeNodeRef>) {
            let mut acc: GraphAcc<ProcessTreeNodeRef, ProcessTreeNodeRef> = GraphAcc::new();
            let mut path = Vec::new();
            add_node(&mut acc, &self.root, &mut path, options.look, measure);
            acc.finish()
        }

        fn request(
            graph: Graph,
            options: &ProcessTreeBuildOptions,
        ) -> Result<TreeRequest, VisualizationError> {
            options.layout_request(graph)
        }

        fn base(style: &ProcessTreeStyle) -> &BaseStyle {
            &style.base
        }

        fn context(
            &self,
            layout: &Layout,
            nodes: &[ProcessTreeNodeRef],
            options: &ProcessTreeBuildOptions,
        ) -> Result<TreeContext, VisualizationError> {
            Ok(TreeContext {
                rows: Rows::of(layout, options.drawn_direction()?),
                index: nodes
                    .iter()
                    .enumerate()
                    .map(|(i, node)| (node.path().to_vec(), i))
                    .collect(),
                placements: layout.nodes.clone(),
            })
        }

        fn draw_node(
            &self,
            node: &ProcessTreeNodeRef,
            placement: &Placement,
            _context: &TreeContext,
            options: &ProcessTreeBuildOptions,
            style: &ProcessTreeStyle,
        ) -> SceneNode {
            match node_at(self, node.path()) {
                Some(tree_node) => tree_scene_node(
                    TreeItem::of(tree_node),
                    placement,
                    options.look,
                    parent_operator(self, node.path()),
                    style,
                ),
                // A ref that does not match the tree.
                None => {
                    let base = &style.base;
                    scene_node(
                        placement,
                        Shape::Box,
                        Some(base.node_fill.clone()),
                        Some(Stroke::new(&base.node_border, base.border_width)),
                        base,
                    )
                }
            }
        }

        fn draw_edge(
            &self,
            child: &ProcessTreeNodeRef,
            _graph_edge: &Edge,
            route: &EdgeRoute,
            context: &TreeContext,
            options: &ProcessTreeBuildOptions,
            style: &ProcessTreeStyle,
        ) -> Option<SceneEdge> {
            let path = child.path();
            let parent = path.split_last()?.1;
            let ends = (
                context.placements.get(*context.index.get(parent)?)?,
                context.placements.get(*context.index.get(path)?)?,
            );
            let parent = operator_at(self, parent);
            tree_scene_edge(route, ends, &context.rows, options.look, parent, style)
        }
    }
}

pub use draw::TreeContext;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::process_models::process_tree::{Leaf, Operator};
    use crate::visualization::graph_build::heuristic_label_width;
    use crate::visualization::model::{self, Visualize};
    use crate::visualization::StyleOverrides;
    use graphlay::svg::{MarkerRef, Paint, Scene};

    /// Every operator type, with a silent leaf.
    fn tree() -> ProcessTree {
        let leaf = |label: Option<&str>| PtNode::Leaf(Leaf::new(label.map(str::to_string)));
        let op = |kind: OperatorType, children: Vec<PtNode>| {
            let mut op = Operator::new(kind);
            op.children = children;
            PtNode::Operator(op)
        };
        let conc = op(
            OperatorType::Concurrency,
            vec![leaf(Some("a")), leaf(Some("b"))],
        );
        let looped = op(OperatorType::Loop, vec![leaf(Some("e")), conc, leaf(None)]);
        let choice = op(
            OperatorType::ExclusiveChoice,
            vec![leaf(Some("c")), leaf(Some("d"))],
        );
        let inter = op(
            OperatorType::Interleaving,
            vec![leaf(Some("g")), leaf(Some("h"))],
        );
        let or = op(
            OperatorType::InclusiveChoice,
            vec![leaf(Some("i")), leaf(Some("j"))],
        );
        ProcessTree::new(op(
            OperatorType::Sequence,
            vec![leaf(Some("a")), looped, choice, inter, or],
        ))
    }

    fn scene_of(look: TreeLook) -> Scene {
        let (tree, options) = (
            tree(),
            ProcessTreeBuildOptions {
                look,
                ..Default::default()
            },
        );
        let layout = model::layout(&tree, &options).unwrap();
        let style = ProcessTreeStyle::default();
        model::scene_from_layout(&tree, &layout, &options, &style, &Default::default()).unwrap()
    }

    #[test]
    fn every_node_is_found_again_by_its_path() {
        let tree = tree();
        let (graph, nodes, edges) =
            tree.to_graph(&ProcessTreeBuildOptions::default(), &heuristic_label_width);
        assert_eq!(nodes[0].path(), &[] as &[usize]);
        for meta in &nodes {
            let leaf = matches!(node_at(&tree, meta.path()), Some(PtNode::Leaf(_)));
            assert_eq!(matches!(meta, ProcessTreeNodeRef::Leaf(_)), leaf);
        }
        for (edge, child) in graph.edges.iter().zip(&edges) {
            assert_eq!(&nodes[edge.target.index()], child);
            let (_, parent) = child.path().split_last().unwrap();
            assert_eq!(nodes[edge.source.index()].path(), parent);
        }
    }

    #[test]
    fn the_tree_request_takes_a_patch_that_the_drawing_follows() {
        use crate::visualization::engine::{layout_tree, tree_options_of};
        let tree = tree();
        let options = ProcessTreeBuildOptions::default();
        let request = model::layout_request(&tree, &options).unwrap();
        assert_eq!(
            tree_options_of(&request.request).unwrap(),
            options.tree_options()
        );
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(
            json["nodes"][0],
            serde_json::json!({"kind": "operator", "path": []})
        );
        assert_eq!(
            layout_tree(&request.request).unwrap(),
            model::layout(&tree, &options).unwrap()
        );

        let sideways = ProcessTreeBuildOptions {
            layout_patch: serde_json::json!({"direction": "leftright"}),
            ..ProcessTreeBuildOptions::default()
        };
        let layout = model::layout(&tree, &sideways).unwrap();
        assert!(layout.nodes[0].center.x < layout.nodes[1].center.x);
        let scene = model::scene_from_layout(
            &tree,
            &layout,
            &sideways,
            &ProcessTreeStyle::default(),
            &StyleOverrides::default(),
        )
        .unwrap();
        // A classic line leaves its parent on the side facing the children.
        let root = scene.nodes[0].rect;
        let first = scene.edges[0].points[0];
        assert!((first.x - root.max_x()).abs() < 0.01, "{first:?} {root:?}");
    }

    #[test]
    fn classic_lines_never_run_through_a_node() {
        let scene = scene_of(TreeLook::Classic);
        for seg in scene.edges.iter().flat_map(|e| e.points.windows(2)) {
            for t in (1..20).map(|i| i as f64 / 20.0) {
                let (x, y) = (
                    seg[0].x + (seg[1].x - seg[0].x) * t,
                    seg[0].y + (seg[1].y - seg[0].y) * t,
                );
                for r in scene.nodes.iter().map(|n| n.rect) {
                    let inside = x > r.x + 0.5
                        && x < r.max_x() - 0.5
                        && y > r.y + 0.5
                        && y < r.max_y() - 0.5;
                    assert!(!inside, "a line runs through the node at {r:?}");
                }
            }
        }
    }

    #[test]
    fn color_coded_edges_take_their_operators_color() {
        let scene = scene_of(TreeLook::ColorCoded);
        let sequence = ProcessTreeStyle::default().operator_colors.sequence;
        assert_eq!(scene.edges[0].stroke.paint, Paint::color(sequence));
        assert!(scene
            .edges
            .iter()
            .all(|e| matches!(e.marker_end, MarkerRef::None)));
    }

    #[test]
    fn writes_the_svg_exports() {
        for (look, name) in [
            (TreeLook::Classic, "process-tree.svg"),
            (TreeLook::ColorCoded, "process-tree-color-coded.svg"),
        ] {
            let options = ProcessTreeBuildOptions {
                look,
                ..Default::default()
            };
            let svg = model::svg(&tree(), &options, &ProcessTreeStyle::default()).unwrap();
            crate::test_utils::write_export(name, &svg);
        }
    }
}
