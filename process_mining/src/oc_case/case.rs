use crate::id_based_impls;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

// Define the Event struct
#[derive(Debug, Clone)]
pub struct Event {
    id: usize,
    event_type: String,
}
id_based_impls!(Event);

// Define the Object struct
#[derive(Debug, Clone)]
pub struct Object {
    id: usize,
    object_type: String,
}
id_based_impls!(Object);

// Define the Node enum which can be either an Event or an Object
#[derive(Debug, Clone)]
pub enum Node {
    Event(Event),
    Object(Object),
}

impl Node {
    fn id(&self) -> usize {
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

// Define the Edge enum with associated target node ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Edge {
    DF(usize),  // Event to Event
    O2O(usize), // Object to Object
    E2O(usize), // Event to Object
}

#[derive(Debug)]
pub struct CaseGraph {
    nodes: Vec<Node>,
    // Adjacency lists storing Edge enums for each node
    edges: HashMap<usize, Vec<Edge>>,
    // Map from ID to node index for fast lookups
    id_to_index: HashMap<usize, usize>,
}

impl CaseGraph {
    fn new() -> Self {
        CaseGraph {
            nodes: Vec::new(),
            edges: HashMap::new(),
            id_to_index: HashMap::new(),
        }
    }

    // Add a node to the graph
    fn add_node(&mut self, node: Node) {
        let id = node.id();
        self.id_to_index.insert(id, self.nodes.len());
        self.nodes.push(node);
    }

    // Add a directed edge to the graph using the Edge enum
    fn add_edge(&mut self, from: usize, to: usize, edge_type: Edge) {
        self.edges.entry(from).or_insert_with(Vec::new).push(edge_type);
    }

    // Retrieve node by id
    fn get_node(&self, id: usize) -> Option<&Node> {
        self.id_to_index.get(&id).map(|&index| &self.nodes[index])
    }

    // Retrieve neighbors by edge type
    fn get_neighbors(&self, from: usize, desired_type: Edge) -> Option<Vec<usize>> {
        self.edges.get(&from).map(|edges| {
            edges.iter().filter_map(|edge| {
                match (edge, &desired_type) {
                    (Edge::DF(target), Edge::DF(_)) => Some(*target),
                    (Edge::O2O(target), Edge::O2O(_)) => Some(*target),
                    (Edge::E2O(target), Edge::E2O(_)) => Some(*target),
                    _ => None,
                }
            }).collect()
        })
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
        graph.add_edge(1, 2, Edge::DF(2)); // Event1 -> Event2
        graph.add_edge(3, 4, Edge::O2O(4)); // Object1 -> Object2
        graph.add_edge(2, 3, Edge::E2O(3)); // Event2 -> Object1
        // Verify DF edge
        let df_neighbors = graph.get_neighbors(1, Edge::DF(0)).unwrap();
        assert_eq!(df_neighbors.len(), 1);
        assert_eq!(df_neighbors[0], 2);
        // Verify O2O edge
        let o2o_neighbors = graph.get_neighbors(3, Edge::O2O(0)).unwrap();
        assert_eq!(o2o_neighbors.len(), 1);
        assert_eq!(o2o_neighbors[0], 4);
        // Verify E2O edge
        let e2o_neighbors = graph.get_neighbors(2, Edge::E2O(0)).unwrap();
        assert_eq!(e2o_neighbors.len(), 1);
        assert_eq!(e2o_neighbors[0], 3);
    }

    #[test]
    fn test_get_neighbors_empty() {
        let graph = CaseGraph::new();
        // Attempt to get neighbors from an empty graph
        assert!(graph.get_neighbors(1, Edge::DF(0)).is_none());
        assert!(graph.get_neighbors(2, Edge::O2O(0)).is_none());
        assert!(graph.get_neighbors(3, Edge::E2O(0)).is_none());
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
        graph.add_edge(1, 2, Edge::DF(2));
        graph.add_edge(1, 2, Edge::DF(2));
        let df_neighbors = graph.get_neighbors(1, Edge::DF(0)).unwrap();
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
        graph.add_edge(1, 2, Edge::DF(2)); // DF edge
        graph.add_edge(1, 3, Edge::E2O(3)); // E2O edge
        // Verify DF edge
        let df_neighbors = graph.get_neighbors(1, Edge::DF(0)).unwrap();
        assert_eq!(df_neighbors.len(), 1);
        assert_eq!(df_neighbors[0], 2);
        // Verify E2O edge
        let e2o_neighbors = graph.get_neighbors(1, Edge::E2O(0)).unwrap();
        assert_eq!(e2o_neighbors.len(), 1);
        assert_eq!(e2o_neighbors[0], 3);
        // Verify no O2O edges
        let o2o_neighbors = graph.get_neighbors(1, Edge::O2O(0));
        assert!(o2o_neighbors.is_none() || o2o_neighbors.unwrap().is_empty());
    }
}