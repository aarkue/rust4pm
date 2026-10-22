//! Prefix tree over the paths of a log, holding the markings consistent with each prefix
use super::{
    replay::{MarkingId, Replayer, Symbol},
    EscapingEdge, EscapingEdgesOptions, TraceEnds,
};

type NodeId = u32;
const ROOT: NodeId = 0;

#[derive(Debug)]
struct Node {
    parent: NodeId,
    symbol: Symbol,
    children: Vec<(Symbol, NodeId)>,
    /// Every marking the model can be in after this prefix
    markings: Box<[MarkingId]>,
    /// Weight of the paths that continue after this prefix
    through: u64,
    /// Weight of the paths that end here
    ends: u64,
}

impl Node {
    /// Whether a path continues with `symbol` here, or stops here if `symbol` is `end`
    fn executes(&self, symbol: Symbol, end: Option<Symbol>) -> bool {
        if Some(symbol) == end {
            return self.ends > 0;
        }
        self.children.iter().any(|&(s, _)| s == symbol)
    }
}

/// Whether a path was inserted completely
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Inserted {
    Complete,
    /// The model could not follow the path beyond some prefix
    Truncated,
}

/// Sums over all weighted states
#[derive(Debug, Default)]
pub(super) struct Totals {
    pub(super) available: u64,
    pub(super) escaping: u64,
    pub(super) states: usize,
}

#[derive(Debug)]
pub(super) struct PrefixTree {
    nodes: Vec<Node>,
}

impl PrefixTree {
    pub(super) fn new(replayer: &Replayer) -> Self {
        Self {
            nodes: vec![Node {
                parent: ROOT,
                symbol: 0,
                children: Vec::new(),
                markings: [replayer.initial()].into(),
                through: 0,
                ends: 0,
            }],
        }
    }

    /// Adds `weight` to every prefix of `path` the model can follow. `None` is an event the model
    /// does not know.
    pub(super) fn insert(
        &mut self,
        path: impl IntoIterator<Item = Option<Symbol>>,
        weight: u64,
        replayer: &mut Replayer,
    ) -> Inserted {
        let mut node = ROOT;
        for symbol in path {
            self.nodes[node as usize].through += weight;
            match symbol.and_then(|symbol| self.child(node, symbol, replayer)) {
                Some(child) => node = child,
                None => return Inserted::Truncated,
            }
        }
        self.nodes[node as usize].ends += weight;
        Inserted::Complete
    }

    /// The child of `node` reached by `symbol`, created on first use. `None` if the model cannot
    /// do `symbol` after this prefix.
    fn child(&mut self, node: NodeId, symbol: Symbol, replayer: &mut Replayer) -> Option<NodeId> {
        let parent = &self.nodes[node as usize];
        if let Some(&(_, child)) = parent.children.iter().find(|&&(s, _)| s == symbol) {
            return Some(child);
        }
        let markings = replayer.step(&parent.markings, symbol);
        if markings.is_empty() {
            return None;
        }
        let child = self.nodes.len() as NodeId;
        self.nodes.push(Node {
            parent: node,
            symbol,
            children: Vec::new(),
            markings,
            through: 0,
            ends: 0,
        });
        self.nodes[node as usize].children.push((symbol, child));
        Some(child)
    }

    pub(super) fn totals(&self, replayer: &mut Replayer, options: &EscapingEdgesOptions) -> Totals {
        let mut totals = Totals::default();
        let end = replayer.end_symbol();
        for (id, node, weight) in self.weighted_states(options) {
            let (available, executed) = if id == ROOT && !options.count_escaping_edges_at_start {
                let executed = node.children.len() + usize::from(end.is_some() && node.ends > 0);
                (executed as u64, executed as u64)
            } else {
                let available = replayer.available_in_any(&node.markings);
                let executed = available.iter().filter(|&s| node.executes(s, end)).count();
                (available.len() as u64, executed as u64)
            };
            totals.available += weight * available;
            totals.escaping += weight * (available - executed);
            totals.states += 1;
        }
        totals
    }

    /// Every escaping edge, sorted by state and then by what escapes
    pub(super) fn escaping_edges(
        &self,
        replayer: &mut Replayer,
        options: &EscapingEdgesOptions,
    ) -> Vec<EscapingEdge> {
        let mut edges = Vec::new();
        let end = replayer.end_symbol();
        for (id, node, weight) in self.weighted_states(options) {
            if id == ROOT && !options.count_escaping_edges_at_start {
                continue;
            }
            let escaping: Vec<Symbol> = replayer
                .available_in_any(&node.markings)
                .iter()
                .filter(|&s| !node.executes(s, end))
                .collect();
            if escaping.is_empty() {
                continue;
            }
            let prefix: Vec<_> = self
                .prefix(id)
                .into_iter()
                .map(|s| replayer.path_step(s))
                .collect();
            edges.extend(escaping.into_iter().map(|s| EscapingEdge {
                prefix: prefix.clone(),
                escaping: replayer.path_step(s),
                weight,
            }));
        }
        edges.sort_unstable();
        edges
    }

    fn weighted_states<'a>(
        &'a self,
        options: &EscapingEdgesOptions,
    ) -> impl Iterator<Item = (NodeId, &'a Node, u64)> {
        let count_ends = options.trace_ends != TraceEnds::Ignore;
        self.nodes.iter().enumerate().filter_map(move |(id, node)| {
            let weight = node.through + if count_ends { node.ends } else { 0 };
            (weight > 0).then_some((id as NodeId, node, weight))
        })
    }

    fn prefix(&self, mut node: NodeId) -> Vec<Symbol> {
        let mut prefix = Vec::new();
        while node != ROOT {
            let current = &self.nodes[node as usize];
            prefix.push(current.symbol);
            node = current.parent;
        }
        prefix.reverse();
        prefix
    }
}
