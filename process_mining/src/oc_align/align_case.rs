use crate::oc_case::case::{CaseGraph, Edge, Node};
use std::collections::HashMap;

trait Mappable {
    fn is_void(&self) -> bool;
    fn cost(&self) -> f64;
}

enum NodeMapping {
    RealNode(usize, usize), // (original_node_id, mapped_node_id)
    VoidNode(usize, usize), // (original_node_id, void_node_id)
}

enum EdgeMapping {
    RealEdge(usize, usize), // (original_edge_id, mapped_edge_id)
    VoidEdge(usize, usize), // (original_edge_id, void_edge_id)
}

impl Mappable for NodeMapping {
    fn is_void(&self) -> bool {
        matches!(self, NodeMapping::VoidNode(_, _))
    }

    fn cost(&self) -> f64 {
        match self {
            NodeMapping::RealNode(_, _) => 0.0,
            NodeMapping::VoidNode(_, _) => 1.0,
        }
    }
}

impl Mappable for EdgeMapping {
    fn is_void(&self) -> bool {
        matches!(self, EdgeMapping::VoidEdge(_, _))
    }

    fn cost(&self) -> f64 {
        match self {
            EdgeMapping::RealEdge(_, _) => 0.0,
            EdgeMapping::VoidEdge(_, _) => 1.0,
        }
    }
}

struct CaseAlignment<'a> {
    c1: &'a CaseGraph,
    c2: &'a CaseGraph,
    void_nodes: HashMap<usize, Node>, // id -> Node
    void_edges: HashMap<usize, Edge>, // id -> Edge
    node_mapping: HashMap<usize, NodeMapping>, // original_node_id -> NodeMapping
    edge_mapping: HashMap<usize, EdgeMapping>, // original_edge_id -> EdgeMapping
}

impl<'a> CaseAlignment<'a> {
    pub fn align(c1: &'a CaseGraph, c2: &'a CaseGraph) -> Self {
        CaseAlignment {
            c1,
            c2,
            void_nodes: HashMap::new(),
            void_edges: HashMap::new(),
            node_mapping: HashMap::new(),
            edge_mapping: HashMap::new(),
        }
    }

    pub fn align_worst_case(c1: &'a CaseGraph, c2: &'a CaseGraph) -> Self {
        let mut void_nodes = HashMap::new();
        let mut node_mapping = HashMap::new();

        for node in c1.nodes.values() {
            let void_node = node.clone();
            let void_id = void_node.id();
            void_nodes.insert(void_node.id(), void_node);
            node_mapping.insert(
                node.id(),
                NodeMapping::VoidNode(node.id(), void_id),
            );
        }

        let mut void_edges = HashMap::with_capacity(c1.edges.len());
        let mut edge_mapping = HashMap::with_capacity(c1.edges.len());

        for edge in c1.edges.values() {
            let void_edge = edge.clone();
            void_edges.insert(void_edge.id, void_edge);
            edge_mapping.insert(
                edge.id,
                EdgeMapping::VoidEdge(edge.id, void_edge.id),
            );
        }

        CaseAlignment {
            c1,
            c2,
            void_nodes,
            void_edges,
            node_mapping,
            edge_mapping,
        }
    }
}

fn get_void_node(node: &Node) -> Node {
    match node {
        Node::Event(event) => Node::Event(event.clone()),
        Node::Object(obj) => Node::Object(obj.clone()),
    }
}