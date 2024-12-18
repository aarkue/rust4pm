use crate::id_based_impls;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

// Define the Event struct
#[derive(Debug, Clone)]
pub struct Event {
    pub id: usize,
    pub event_type: String,
}
id_based_impls!(Event);

// Define the Object struct
#[derive(Debug, Clone)]
pub struct Object {
    pub id: usize,
    pub object_type: String,
}
id_based_impls!(Object);

// Define the Node enum which can be either an Event or an Object
#[derive(Debug, Clone)]
pub enum Node {
    Event(Event),
    Object(Object),
}

impl Node {
    pub fn id(&self) -> usize {
        match self {
            Node::Event(event) => event.id,
            Node::Object(object) => object.id,
        }
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl Eq for Node {}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

// Define the EdgeType enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EdgeType {
    DF,  // Event to Event
    O2O, // Object to Object
    E2O, // Event to Object
}

// Define the Edge struct with additional attributes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Edge {
    pub id: usize,
    pub from: usize,
    pub to: usize,
    pub edge_type: EdgeType,
    // Additional attributes can be added here
    // For example:
    // weight: f64,
    // label: String,
}

impl Edge {
    pub fn new(id: usize, from: usize, to: usize, edge_type: EdgeType) -> Self {
        Edge {
            id,
            from,
            to,
            edge_type,
            // Initialize additional attributes here
        }
    }
}

// Define the CaseGraph structure
#[derive(Debug, Clone)]
pub struct CaseGraph {
    pub nodes: HashMap<usize, Node>, // Keyed by node ID
    pub edges: HashMap<usize, Edge>, // Keyed by edge ID
    adjacency: HashMap<usize, Vec<usize>>, // from node ID -> Vec of edge IDs
    id_to_index: HashMap<usize, usize>, // Map from node ID to index (if needed)
}

impl CaseGraph {
    pub fn new() -> Self {
        CaseGraph {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            adjacency: HashMap::new(),
            id_to_index: HashMap::new(),
        }
    }

    // Add a node to the graph
    pub fn add_node(&mut self, node: Node) {
        let id = node.id();
        self.id_to_index.insert(id, self.nodes.len());
        self.nodes.insert(id, node);
    }

    // Add an edge to the graph with additional attributes
    pub fn add_edge(&mut self, edge: Edge) {
        let edge_id = edge.id;
        let from = edge.from;
        self.edges.insert(edge_id, edge);
        self.adjacency.entry(from).or_insert_with(Vec::new).push(edge_id);
    }

    // Retrieve node by id
    pub fn get_node(&self, id: usize) -> Option<&Node> {
        self.nodes.get(&id)
    }

    // Retrieve edge by id
    pub fn get_edge(&self, id: usize) -> Option<&Edge> {
        self.edges.get(&id)
    }

    // Retrieve outgoing edges from a node
    pub fn get_outgoing_edges(&self, from: usize) -> Option<&Vec<usize>> {
        self.adjacency.get(&from)
    }

    // Retrieve neighbors by edge type
    pub fn get_neighbors_by_edge_type(&self, from: usize, edge_type: EdgeType) -> Vec<usize> {
        match self.adjacency.get(&from) {
            Some(edge_ids) => edge_ids.iter()
                .filter_map(|eid| {
                    self.edges.get(eid).and_then(|edge| {
                        if edge.edge_type == edge_type {
                            Some(edge.to)
                        } else {
                            None
                        }
                    })
                })
                .collect(),
            None => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_nodes() {
        let mut graph = CaseGraph::new();
        let event1 = Event { id: 1, event_type: "A".to_string() };
        let object1 = Object { id: 2, object_type: "Person".to_string() };
        graph.add_node(Node::Event(event1.clone()));
        graph.add_node(Node::Object(object1.clone()));
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.get_node(1), Some(&Node::Event(event1)));
        assert_eq!(graph.get_node(2), Some(&Node::Object(object1)));
    }

    #[test]
    fn test_add_edges() {
        let mut graph = CaseGraph::new();
        // Add nodes
        let event1 = Event { id: 1, event_type: "A".to_string() };
        let event2 = Event { id: 2, event_type: "B".to_string() };
        let object1 = Object { id: 3, object_type: "Person".to_string() };
        let object2 = Object { id: 4, object_type: "Device".to_string() };
        graph.add_node(Node::Event(event1));
        graph.add_node(Node::Event(event2));
        graph.add_node(Node::Object(object1));
        graph.add_node(Node::Object(object2));
        // Add edges
        let edge1 = Edge::new(1, 1, 2, EdgeType::DF); // Event1 -> Event2
        let edge2 = Edge::new(2, 3, 4, EdgeType::O2O); // Object1 -> Object2
        let edge3 = Edge::new(3, 2, 3, EdgeType::E2O); // Event2 -> Object1
        graph.add_edge(edge1);
        graph.add_edge(edge2);
        graph.add_edge(edge3);
        // Verify DF edge
        let df_neighbors = graph.get_neighbors_by_edge_type(1, EdgeType::DF);
        assert_eq!(df_neighbors.len(), 1);
        assert_eq!(df_neighbors[0], 2);
        // Verify O2O edge
        let o2o_neighbors = graph.get_neighbors_by_edge_type(3, EdgeType::O2O);
        assert_eq!(o2o_neighbors.len(), 1);
        assert_eq!(o2o_neighbors[0], 4);
        // Verify E2O edge
        let e2o_neighbors = graph.get_neighbors_by_edge_type(2, EdgeType::E2O);
        assert_eq!(e2o_neighbors.len(), 1);
        assert_eq!(e2o_neighbors[0], 3);
    }

    #[test]
    fn test_get_neighbors_empty() {
        let graph = CaseGraph::new();
        // Attempt to get neighbors from an empty graph
        assert!(graph.get_neighbors_by_edge_type(1, EdgeType::DF).is_empty());
        assert!(graph.get_neighbors_by_edge_type(2, EdgeType::O2O).is_empty());
        assert!(graph.get_neighbors_by_edge_type(3, EdgeType::E2O).is_empty());
    }

    #[test]
    fn test_duplicate_edges() {
        let mut graph = CaseGraph::new();
        // Add nodes
        let event1 = Event { id: 1, event_type: "A".to_string() };
        let event2 = Event { id: 2, event_type: "B".to_string() };
        graph.add_node(Node::Event(event1));
        graph.add_node(Node::Event(event2));
        // Add duplicate DF edges
        let edge1 = Edge::new(1, 1, 2, EdgeType::DF);
        let edge2 = Edge::new(2, 1, 2, EdgeType::DF);
        graph.add_edge(edge1);
        graph.add_edge(edge2);
        let df_neighbors = graph.get_neighbors_by_edge_type(1, EdgeType::DF);
        assert_eq!(df_neighbors.len(), 2);
        assert_eq!(df_neighbors[0], 2);
        assert_eq!(df_neighbors[1], 2);
    }

    #[test]
    fn test_multiple_edge_types() {
        let mut graph = CaseGraph::new();
        // Add nodes
        let event1 = Event { id: 1, event_type: "A".to_string() };
        let event2 = Event { id: 2, event_type: "B".to_string() };
        let object1 = Object { id: 3, object_type: "Person".to_string() };
        graph.add_node(Node::Event(event1));
        graph.add_node(Node::Event(event2));
        graph.add_node(Node::Object(object1));
        // Add different types of edges from event1
        let edge1 = Edge::new(1, 1, 2, EdgeType::DF); // DF edge
        let edge2 = Edge::new(2, 1, 3, EdgeType::E2O); // E2O edge
        graph.add_edge(edge1);
        graph.add_edge(edge2);
        // Verify DF edge
        let df_neighbors = graph.get_neighbors_by_edge_type(1, EdgeType::DF);
        assert_eq!(df_neighbors.len(), 1);
        assert_eq!(df_neighbors[0], 2);
        // Verify E2O edge
        let e2o_neighbors = graph.get_neighbors_by_edge_type(1, EdgeType::E2O);
        assert_eq!(e2o_neighbors.len(), 1);
        assert_eq!(e2o_neighbors[0], 3);
        // Verify no O2O edges
        let o2o_neighbors = graph.get_neighbors_by_edge_type(1, EdgeType::O2O);
        assert!(o2o_neighbors.is_empty());
    }
}