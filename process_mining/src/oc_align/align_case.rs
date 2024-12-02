use std::collections::HashMap;
use crate::oc_case::case::{CaseGraph, Edge, Node};

trait Mappable {
    fn is_void(&self) -> bool;
    fn cost(&self) -> f64;
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

enum NodeMapping {
    RealNode(Node, Node),
    VoidNode(Node, Node),
}

enum EdgeMapping {
    RealEdge(Edge, Edge),
    VoidEdge(Edge, Edge),
}
struct CaseAlignment {
    c1: CaseGraph,
    c2: CaseGraph,
    void_nodes: Vec<Node>,
    void_edges: Vec<Edge>,
    node_mapping: HashMap<Node, NodeMapping>,
    edge_mapping: HashMap<Edge, EdgeMapping>,
}

impl CaseAlignment {
    pub fn align(c1: CaseGraph, c2: CaseGraph) -> Self {
        let void_nodes = Vec::new();
        let void_edges = Vec::new();
        let node_mapping = HashMap::new();
        let edge_mapping = HashMap::new();

        CaseAlignment {
            c1,
            c2,
            void_nodes,
            void_edges,
            node_mapping,
            edge_mapping,
        }
    }
    
    pub fn align_worst_case(c1: CaseGraph, c2: CaseGraph) -> Self {
        let void_nodes = c1.nodes.values().cloned().collect();
        let void_edges = c1.edges.values().cloned().collect();
        
        // first, create void nodes and edges for every node and edge in c1
        //  the worst case is to just map every node and edge to their exact void node and edge equivalent
        
        let mut node_mapping : HashMap<&Node, NodeMapping> = HashMap::new();
        for node in &void_nodes {
            node_mapping.insert(node, NodeMapping::VoidNode(node.clone(), node.clone()));
        }

        let mut edge_mapping: HashMap<&Edge, EdgeMapping> = HashMap::new();
        for edge in &void_edges {
            edge_mapping.insert(edge, EdgeMapping::VoidEdge(edge.clone(), edge.clone()));
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