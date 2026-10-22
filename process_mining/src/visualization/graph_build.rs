//! Node sizes, edge weights, stroke widths and label measurement shared by the adapters.

use std::collections::HashMap;

use graphlay::{Edge, EdgeId, Graph, Node, NodeId, Size};

/// The drawn width of a node label in px, for a host with real font metrics.
pub type LabelWidth<'a> = &'a dyn Fn(&str) -> f64;

/// A Petri net place.
pub const PLACE_SIZE: Size = Size::new(52.0, 52.0);
/// A labeled Petri net transition.
pub const LABELED_TRANSITION_SIZE: Size = Size::new(120.0, 52.0);
/// A silent Petri net transition.
pub const SILENT_TRANSITION_SIZE: Size = Size::new(14.0, 52.0);
/// A START or END terminal.
pub const TERMINAL_SIZE: Size = Size::new(80.0, 36.0);
/// An OC-DECLARE activity box.
pub const DECLARE_NODE_SIZE: Size = Size::new(150.0, 58.0);
/// A process-tree operator in the classic look.
pub const OPERATOR_SIZE: Size = Size::new(34.0, 34.0);

/// Narrowest a DFG activity box gets.
pub const DFG_MIN_WIDTH: f64 = 120.0;
/// Widest a DFG activity box gets.
pub const DFG_MAX_WIDTH: f64 = 260.0;

/// Height of a leaf box with a one-line label.
pub const LEAF_HEIGHT: f64 = 30.0;
/// Height of a leaf box whose label wraps onto two lines.
pub const LEAF_HEIGHT_WRAPPED: f64 = 46.0;
/// Narrowest a leaf box gets.
pub const LEAF_MIN_WIDTH: f64 = 60.0;
/// Widest a leaf box gets.
pub const LEAF_MAX_WIDTH: f64 = 200.0;
/// Longest leaf label kept on one line. A longer one wraps onto two lines.
pub const LEAF_WRAP_CHARS: usize = 22;

/// `8 * chars + 24`: about 8px per character at 12.5px, plus the box padding.
pub fn heuristic_label_width(label: &str) -> f64 {
    8.0 * label.chars().count() as f64 + 24.0
}

/// The DFG / OC-DFG activity box width: `clamp(8 * chars + 24, 120, 260)`.
pub fn dfg_width(label: &str) -> f64 {
    dfg_width_with(label, &heuristic_label_width)
}

/// [`dfg_width`] against a caller's own label measurement.
pub fn dfg_width_with(label: &str, measure: LabelWidth<'_>) -> f64 {
    measure(label).clamp(DFG_MIN_WIDTH, DFG_MAX_WIDTH)
}

/// Height of a mid-edge label box, for 10.5px text on its chip.
pub const EDGE_LABEL_HEIGHT: f64 = 14.0;

/// The box a mid-edge label needs, from a node-label measurement: the node padding removed, the
/// text scaled from 12.5px to 10.5px, and the chip padding added.
pub fn edge_label_size_with(text: &str, measure: LabelWidth<'_>) -> Size {
    let glyphs = (measure(text) - NODE_LABEL_PADDING).max(0.0) * EDGE_LABEL_SIZE / NODE_LABEL_SIZE;
    Size::new(glyphs + 2.0 * CHIP_PADDING, EDGE_LABEL_HEIGHT)
}

/// [`edge_label_size_with`] under the default measurement.
pub fn edge_label_size(text: &str) -> Size {
    edge_label_size_with(text, &heuristic_label_width)
}

/// Horizontal padding [`heuristic_label_width`] keeps around a node label.
const NODE_LABEL_PADDING: f64 = 24.0;
/// Default `BaseStyle::label_size`.
const NODE_LABEL_SIZE: f64 = 12.5;
/// Default `BaseStyle::edge_label_size`.
const EDGE_LABEL_SIZE: f64 = 10.5;
/// Matches `graphlay::svg::Chip::default().padding`.
const CHIP_PADDING: f64 = 2.0;

/// `1 + ln(max(count, 1))`, so the busiest arc of a real log is not straightened at the cost of
/// every other.
pub fn weight_from_count(count: f64) -> f64 {
    1.0 + count.max(1.0).ln()
}

/// `1.5 + 4.5 * sqrt(count / max_count)` px. The square root keeps mid-range counts apart.
pub fn thickness_from_count(count: f64, max_count: f64) -> f64 {
    let max_count = max_count.max(1.0);
    1.5 + 4.5 * (count / max_count).clamp(0.0, 1.0).sqrt()
}

/// The one or two lines a leaf label is drawn on, split at the whitespace that makes the longer
/// line shortest.
///
/// ```
/// # use process_mining::visualization::graph_build::leaf_lines;
/// assert_eq!(leaf_lines("place order"), vec!["place order"]);
/// assert_eq!(leaf_lines("Create Fine Notification"), vec!["Create Fine", "Notification"]);
/// ```
pub fn leaf_lines(label: &str) -> Vec<String> {
    let chars: Vec<char> = label.chars().collect();
    if chars.len() <= LEAF_WRAP_CHARS {
        return vec![label.to_string()];
    }
    // Break at a whitespace run. Neither line keeps the whitespace.
    let mut best: Option<(usize, usize, usize)> = None; // (longest line, split at, end of gap)
    for (i, ch) in chars.iter().enumerate() {
        if !ch.is_whitespace() {
            continue;
        }
        let mut end = i + 1;
        while end < chars.len() && chars[end].is_whitespace() {
            end += 1;
        }
        if i == 0 || end >= chars.len() {
            continue;
        }
        let longest = i.max(chars.len() - end);
        if best.is_none_or(|(b, _, _)| longest < b) {
            best = Some((longest, i, end));
        }
    }
    let (left, right) = match best {
        Some((_, at, end)) => (&chars[..at], &chars[end..]),
        // No whitespace to break at, so split in the middle.
        None => chars.split_at(chars.len().div_ceil(2)),
    };
    vec![left.iter().collect(), right.iter().collect()]
}

/// The box a tree leaf or partial-order event is drawn in, as wide as its longest
/// [`leaf_lines`] line.
pub fn leaf_size(label: &str) -> Size {
    leaf_size_with(label, &heuristic_label_width)
}

/// [`leaf_size`] against a caller's own label measurement.
pub fn leaf_size_with(label: &str, measure: LabelWidth<'_>) -> Size {
    let lines = leaf_lines(label);
    let width = lines
        .iter()
        .map(|line| measure(line))
        .fold(0.0_f64, f64::max)
        .clamp(LEAF_MIN_WIDTH, LEAF_MAX_WIDTH);
    let height = if lines.len() > 1 {
        LEAF_HEIGHT_WRAPPED
    } else {
        LEAF_HEIGHT
    };
    Size::new(width, height)
}

/// Builds a [`Graph`] and its meta tables. Adding a node under an existing key returns that node,
/// which merges an OC-DFG's shared activities.
pub(crate) struct GraphAcc<N, E> {
    graph: Graph,
    node_meta: Vec<N>,
    edge_meta: Vec<E>,
    index: HashMap<String, NodeId>,
}

impl<N, E> GraphAcc<N, E> {
    pub(crate) fn new() -> Self {
        GraphAcc {
            graph: Graph::new(),
            node_meta: Vec::new(),
            edge_meta: Vec::new(),
            index: HashMap::new(),
        }
    }

    pub(crate) fn add_node(
        &mut self,
        key: impl Into<String>,
        node: Node,
        meta: impl FnOnce() -> N,
    ) -> NodeId {
        let key = key.into();
        if let Some(&id) = self.index.get(&key) {
            return id;
        }
        let id = NodeId(self.graph.nodes.len() as u32);
        self.graph.nodes.push(node);
        self.node_meta.push(meta());
        self.index.insert(key, id);
        id
    }

    pub(crate) fn node_id(&self, key: &str) -> Option<NodeId> {
        self.index.get(key).copied()
    }

    pub(crate) fn add_edge(&mut self, edge: Edge, meta: E) -> EdgeId {
        let id = EdgeId(self.graph.edges.len() as u32);
        self.graph.edges.push(edge);
        self.edge_meta.push(meta);
        id
    }

    pub(crate) fn finish(self) -> (Graph, Vec<N>, Vec<E>) {
        (self.graph, self.node_meta, self.edge_meta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_leaf_labels_wrap_onto_two_balanced_lines() {
        assert_eq!(
            leaf_lines("Insert Date Appeal to Prefecture"),
            ["Insert Date Appeal", "to Prefecture"]
        );
        assert_eq!(
            leaf_lines(&"A".repeat(24)),
            ["A".repeat(12), "A".repeat(12)]
        );
        let wrapped = leaf_size("Insert Date Appeal to Prefecture");
        assert_eq!(
            wrapped,
            Size::new(
                heuristic_label_width("Insert Date Appeal"),
                LEAF_HEIGHT_WRAPPED
            )
        );
    }

    #[test]
    fn an_edge_label_box_is_the_drawn_text() {
        // (8 * 4 + 24 - 24) * 10.5 / 12.5 + 4
        assert!((edge_label_size("1234").width - 30.88).abs() < 0.01);
        assert_eq!(edge_label_size("").width, 4.0);
    }
}
