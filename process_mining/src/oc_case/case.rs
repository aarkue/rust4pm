use crate::id_based_impls;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};


#[derive(Debug, Clone)]
pub struct Event {
    id: usize,
    event_type: String,
}

id_based_impls!(Event);

#[derive(Debug, Clone)]
pub struct Object {
    id: usize,
    object_type: String,
}

id_based_impls!(Object);

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EdgeType {
    DF,  // Event to Event
    O2O, // Object to Object
    E2O, // Event to Object
}

#[derive(Debug)]
pub struct CaseGraph {
    nodes: Vec<Node>,
    // Adjacency lists for each edge type
    edges_df: HashMap<usize, Vec<usize>>,  // DF: Event -> Event
    edges_o2o: HashMap<usize, Vec<usize>>, // O2O: Object -> Object
    edges_e2o: HashMap<usize, Vec<usize>>, // E2O: Event -> Object
    // Optional: Map from ID to node index for fast lookups
    id_to_index: HashMap<usize, usize>,
}

impl CaseGraph {
    fn new() -> Self {
        CaseGraph {
            nodes: Vec::new(),
            edges_df: HashMap::new(),
            edges_o2o: HashMap::new(),
            edges_e2o: HashMap::new(),
            id_to_index: HashMap::new(),
        }
    }

    // Add a node to the graph
    fn add_node(&mut self, node: Node) {
        let id = node.id();
        self.id_to_index.insert(id, self.nodes.len());
        self.nodes.push(node);
    }

    // Add a directed edge to the graph
    fn add_edge(&mut self, from: usize, to: usize, edge_type: EdgeType) {
        match edge_type {
            EdgeType::DF => {
                self.edges_df.entry(from).or_insert_with(Vec::new).push(to);
            }
            EdgeType::O2O => {
                self.edges_o2o.entry(from).or_insert_with(Vec::new).push(to);
            }
            EdgeType::E2O => {
                self.edges_e2o.entry(from).or_insert_with(Vec::new).push(to);
            }
        }
    }

    // Retrieve node by id
    fn get_node(&self, id: usize) -> Option<&Node> {
        self.id_to_index.get(&id).map(|&index| &self.nodes[index])
    }

    // Retrieve neighbors by edge type
    fn get_neighbors(&self, from: usize, edge_type: EdgeType) -> Option<&Vec<usize>> {
        match edge_type {
            EdgeType::DF => self.edges_df.get(&from),
            EdgeType::O2O => self.edges_o2o.get(&from),
            EdgeType::E2O => self.edges_e2o.get(&from),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_nodes() {
        let mut graph = CaseGraph::new();

        let event1 = Event {
            id: 1,
            event_type: "A".to_string(),
        };
        let object1 = Object {
            id: 2,
            object_type: "Person".to_string(),
        };

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
        let event1 = Event {
            id: 1,
            event_type: "A".to_string(),
        };
        let event2 = Event {
            id: 2,
            event_type: "B".to_string(),
        };
        let object1 = Object {
            id: 3,
            object_type: "Person".to_string(),
        };
        let object2 = Object {
            id: 4,
            object_type: "Device".to_string(),
        };

        graph.add_node(Node::Event(event1));
        graph.add_node(Node::Event(event2));
        graph.add_node(Node::Object(object1));
        graph.add_node(Node::Object(object2));

        // Add edges
        graph.add_edge(1, 2, EdgeType::DF); // Event1 -> Event2
        graph.add_edge(3, 4, EdgeType::O2O); // Object1 -> Object2
        graph.add_edge(2, 3, EdgeType::E2O); // Event2 -> Object1

        // Verify DF edge
        let df_neighbors = graph.get_neighbors(1, EdgeType::DF).unwrap();
        assert_eq!(df_neighbors.len(), 1);
        assert_eq!(df_neighbors[0], 2);

        // Verify O2O edge
        let o2o_neighbors = graph.get_neighbors(3, EdgeType::O2O).unwrap();
        assert_eq!(o2o_neighbors.len(), 1);
        assert_eq!(o2o_neighbors[0], 4);

        // Verify E2O edge
        let e2o_neighbors = graph.get_neighbors(2, EdgeType::E2O).unwrap();
        assert_eq!(e2o_neighbors.len(), 1);
        assert_eq!(e2o_neighbors[0], 3);
    }

    #[test]
    fn test_get_neighbors_empty() {
        let graph = CaseGraph::new();

        // Attempt to get neighbors from an empty graph
        assert!(graph.get_neighbors(1, EdgeType::DF).is_none());
        assert!(graph.get_neighbors(2, EdgeType::O2O).is_none());
        assert!(graph.get_neighbors(3, EdgeType::E2O).is_none());
    }

    #[test]
    fn test_duplicate_edges() {
        let mut graph = CaseGraph::new();

        // Add nodes
        let event1 = Event {
            id: 1,
            event_type: "A".to_string(),
        };
        let event2 = Event {
            id: 2,
            event_type: "B".to_string(),
        };

        graph.add_node(Node::Event(event1));
        graph.add_node(Node::Event(event2));

        // Add duplicate DF edges
        graph.add_edge(1, 2, EdgeType::DF);
        graph.add_edge(1, 2, EdgeType::DF);

        let df_neighbors = graph.get_neighbors(1, EdgeType::DF).unwrap();
        assert_eq!(df_neighbors.len(), 2);
        assert_eq!(df_neighbors[0], 2);
        assert_eq!(df_neighbors[1], 2);
    }

    #[test]
    fn test_multiple_edge_types() {
        let mut graph = CaseGraph::new();

        // Add nodes
        let event1 = Event {
            id: 1,
            event_type: "A".to_string(),
        };
        let event2 = Event {
            id: 2,
            event_type: "B".to_string(),
        };
        let object1 = Object {
            id: 3,
            object_type: "Person".to_string(),
        };

        graph.add_node(Node::Event(event1));
        graph.add_node(Node::Event(event2));
        graph.add_node(Node::Object(object1));

        // Add different types of edges from event1
        graph.add_edge(1, 2, EdgeType::DF); // DF edge
        graph.add_edge(1, 3, EdgeType::E2O); // E2O edge

        // Verify DF edge
        let df_neighbors = graph.get_neighbors(1, EdgeType::DF).unwrap();
        assert_eq!(df_neighbors.len(), 1);
        assert_eq!(df_neighbors[0], 2);

        // Verify E2O edge
        let e2o_neighbors = graph.get_neighbors(1, EdgeType::E2O).unwrap();
        assert_eq!(e2o_neighbors.len(), 1);
        assert_eq!(e2o_neighbors[0], 3);

        // Verify no O2O edges
        assert!(graph.get_neighbors(1, EdgeType::O2O).is_none());
    }
}
