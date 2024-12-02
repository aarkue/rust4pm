// Assuming the provided context code is in a module named `oc_case`
use crate::oc_case::case::{CaseGraph, Edge, EdgeType, Event, Node, Object};
use russcip::prelude::*;
use russcip::Variable;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

trait Mappable {
    fn is_void(&self) -> bool;
    fn cost(&self) -> f64;
}

enum NodeMapping {
    RealNode(usize, usize), // (c1_node, c2_node)
    VoidNode(usize, usize), // (c1_node, void_node_id)
}

enum EdgeMapping {
    RealEdge(usize, usize), // (c1_edge, c2_edge)
    VoidEdge(usize, usize), // (c1_edge, void_edge_id)
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
    void_nodes: HashMap<usize, Node>,          // id -> Node
    void_edges: HashMap<usize, Edge>,          // id -> Edge
    node_mapping: HashMap<usize, NodeMapping>, // c1_node_id -> mapping
    edge_mapping: HashMap<usize, EdgeMapping>, // c1_edge_id -> mapping
}

impl<'a> CaseAlignment<'a> {
    pub fn align_mip(c1: &'a CaseGraph, c2: &'a CaseGraph) -> Self {
        let mut model = Model::new()
            .hide_output()
            .include_default_plugins()
            .create_prob("CaseAlignment")
            .set_obj_sense(ObjSense::Minimize);

        // Variables
        // Node mapping variables: x_{i,j} = 1 if c1 node i maps to c2 node j
        let mut x_vars: HashMap<(usize, usize), Rc<Variable>> = HashMap::new();
        for n1 in c1.nodes.keys() {
            for n2 in c2.nodes.keys() {
                // Only map nodes of the same type
                let node1 = c1.nodes.get(n1).unwrap();
                let node2 = c2.nodes.get(n2).unwrap();
                if nodes_compatible(node1, node2) {
                    let var =
                        model.add_var(0., 1., 0., &format!("x_{}_{}", n1, n2), VarType::Binary);
                    x_vars.insert((*n1, *n2), var);
                }
            }
            // Option to map to a void node
            let var = model.add_var(0., 1., 1., &format!("x_void_{}", n1), VarType::Binary);
            x_vars.insert((*n1, 0), var); // Using 0 to denote void
        }

        // Each node in c1 must be mapped to exactly one node in c2 or to a void node
        for n1 in c1.nodes.keys() {
            let mut vars = Vec::new();
            let mut coeffs = Vec::new();
            for n2 in c2.nodes.keys() {
                if let Some(v) = x_vars.get(&(*n1, *n2)) {
                    vars.push(v.clone());
                    coeffs.push(1.0);
                }
            }
            if let Some(v) = x_vars.get(&(*n1, 0)) {
                vars.push(v.clone());
                coeffs.push(1.0);
            }
            model.add_cons(vars, &coeffs, 1.0, 1.0, &format!("map_node_{}", n1));
        }

        // Each node in c2 can be mapped to at most one node in c1
        for n2 in c2.nodes.keys() {
            let mut vars = Vec::new();
            let mut coeffs = Vec::new();
            for n1 in c1.nodes.keys() {
                if let Some(v) = x_vars.get(&(*n1, *n2)) {
                    vars.push(v.clone());
                    coeffs.push(1.0);
                }
            }
            model.add_cons(vars, &coeffs, 0.0, 1.0, &format!("c2_node_once_{}", n2));
        }

        // Create edge mapping variables similarly
        let mut y_vars: HashMap<(usize, usize), Rc<Variable>> = HashMap::new();
        for e1 in c1.edges.keys() {
            for e2 in c2.edges.keys() {
                let edge1 = c1.edges.get(e1).unwrap();
                let edge2 = c2.edges.get(e2).unwrap();
                if edges_compatible(edge1, edge2) {
                    let var =
                        model.add_var(0., 1., 0., &format!("y_{}_{}", e1, e2), VarType::Binary);
                    y_vars.insert((*e1, *e2), var);
                }
            }
            // Option to map to a void edge
            let var = model.add_var(0., 1., 1., &format!("y_void_{}", e1), VarType::Binary);
            y_vars.insert((*e1, 0), var); // Using 0 to denote void
        }

        // Each edge in c1 must be mapped to exactly one edge in c2 or to a void edge
        for e1 in c1.edges.keys() {
            let mut vars = Vec::new();
            let mut coeffs = Vec::new();
            for e2 in c2.edges.keys() {
                if let Some(v) = y_vars.get(&(*e1, *e2)) {
                    vars.push(v.clone());
                    coeffs.push(1.0);
                }
            }
            if let Some(v) = y_vars.get(&(*e1, 0)) {
                vars.push(v.clone());
                coeffs.push(1.0);
            }
            model.add_cons(vars, &coeffs, 1.0, 1.0, &format!("map_edge_{}", e1));
        }

        // Each edge in c2 can be mapped to at most one edge in c1
        for e2 in c2.edges.keys() {
            let mut vars = Vec::new();
            let mut coeffs = Vec::new();
            for e1 in c1.edges.keys() {
                if let Some(v) = y_vars.get(&(*e1, *e2)) {
                    vars.push(v.clone());
                    coeffs.push(1.0);
                }
            }
            model.add_cons(vars, &coeffs, 0.0, 1.0, &format!("c2_edge_once_{}", e2));
        }

        // Structure preservation: if two nodes are mapped, their edges should correspond
        for (&e1_id, e1) in &c1.edges {
            let from1 = e1.from;
            let to1 = e1.to;
            for (&e2_id, e2) in &c2.edges {
                // If e1 maps to e2, then from1 maps to e2.from and to1 maps to e2.to
                if let (Some(x_from), Some(x_to)) =
                    (x_vars.get(&(from1, e2.from)), x_vars.get(&(to1, e2.to)))
                {
                    if let Some(y_var) = y_vars.get(&(e1_id, e2_id)) {
                        // y >= x_from + x_to -1
                        model.add_cons(
                            vec![y_var.clone(), x_from.clone(), x_to.clone()],
                            &[1.0, -1.0, -1.0],
                            -f64::INFINITY,
                            0.0,
                            &format!("struct_pres_e{}_e{}", e1_id, e2_id),
                        );
                    }
                }
            }
        }

        // Objective: minimize number of void nodes and void edges plus unused nodes and edges in c2
        let mut obj_vars = Vec::new();
        let mut obj_coeffs = Vec::new();

        // Void nodes
        for n1 in c1.nodes.keys() {
            if let Some(v) = x_vars.get(&(*n1, 0)) {
                obj_vars.push(v.clone());
                obj_coeffs.push(1.0);
            }
        }

        // Void edges
        for e1 in c1.edges.keys() {
            if let Some(v) = y_vars.get(&(*e1, 0)) {
                obj_vars.push(v.clone());
                obj_coeffs.push(1.0);
            }
        }

        // Unused nodes in c2
        for n2 in c2.nodes.keys() {
            let var = model.add_var(0., 1., 1., &format!("unused_node_{}", n2), VarType::Binary);
            // If unused_node is 1, then no x_{i,j} can be 1 for this n2
            for n1 in c1.nodes.keys() {
                if let Some(x_var) = x_vars.get(&(*n1, *n2)) {
                    model.add_cons(
                        vec![x_var.clone(), var.clone()],
                        &[1.0, 1.0],
                        -f64::INFINITY,
                        1.0,
                        &format!("unused_node_def_{}", n2),
                    );
                }
            }
            obj_vars.push(var.clone());
            obj_coeffs.push(1.0);
        }

        // Unused edges in c2
        for e2 in c2.edges.keys() {
            let var = model.add_var(0., 1., 1., &format!("unused_edge_{}", e2), VarType::Binary);
            // If unused_edge is 1, then no y_{i,j} can be 1 for this e2
            for e1 in c1.edges.keys() {
                if let Some(y_var) = y_vars.get(&(*e1, *e2)) {
                    model.add_cons(
                        vec![y_var.clone(), var.clone()],
                        &[1.0, 1.0],
                        -f64::INFINITY,
                        1.0,
                        &format!("unused_edge_def_{}", e2),
                    );
                }
            }
            obj_vars.push(var.clone());
            obj_coeffs.push(1.0);
        }

        //model.set_obj(&obj_vars, &obj_coeffs);

        // Solve the model
        let solved_model = model.solve();

        let status = solved_model.status();
        println!("Solved with status {:?}", status);

        if solved_model.status() != Status::Optimal {
            panic!("No optimal solution found");
        }

        let obj_val = solved_model.obj_val();
        println!("Objective value: {}", obj_val);

        let sol = solved_model.best_sol().unwrap();

        // Extract node mappings
        let mut node_mapping = HashMap::new();
        for (&(n1, n2), var) in &x_vars {
            if sol.val(var.clone()) > 0.5 {
                if n2 == 0 {
                    // Mapped to void
                    node_mapping.insert(
                        n1,
                        NodeMapping::VoidNode(n1, n1), // Using n1 as void id
                    );
                } else {
                    node_mapping.insert(n1, NodeMapping::RealNode(n1, n2));
                }
            }
        }

        // Extract edge mappings
        let mut edge_mapping = HashMap::new();
        for (&(e1, e2), var) in &y_vars {
            if sol.val(var.clone()) > 0.5 {
                if e2 == 0 {
                    // Mapped to void
                    edge_mapping.insert(
                        e1,
                        EdgeMapping::VoidEdge(e1, e1), // Using e1 as void id
                    );
                } else {
                    edge_mapping.insert(e1, EdgeMapping::RealEdge(e1, e2));
                }
            }
        }

        // Collect void nodes and edges
        let mut void_nodes = HashMap::new();
        for (&n1, mapping) in &node_mapping {
            if mapping.is_void() {
                let node = c1.nodes.get(&n1).unwrap().clone();
                void_nodes.insert(n1, node);
            }
        }

        let mut void_edges = HashMap::new();
        for (&e1, mapping) in &edge_mapping {
            if mapping.is_void() {
                let edge = c1.edges.get(&e1).unwrap().clone();
                void_edges.insert(e1, edge);
            }
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

    /// Computes the total cost of the alignment.
    ///
    /// Returns:
    /// - Ok(total_cost) if the alignment is valid.
    /// - Err(error_message) if the alignment is invalid.
    fn total_cost(&self) -> Result<f64, String> {
        // 1. Validate that all nodes in c1 are mapped
        if self.node_mapping.len() != self.c1.nodes.len() {
            return Err("Not all nodes in c1 are mapped.".to_string());
        }

        // 2. Validate that all edges in c1 are mapped
        if self.edge_mapping.len() != self.c1.edges.len() {
            return Err("Not all edges in c1 are mapped.".to_string());
        }

        // 3. Ensure no node in c2 is mapped more than once
        let mut mapped_c2_nodes: HashSet<usize> = HashSet::new();
        for mapping in self.node_mapping.values() {
            if let NodeMapping::RealNode(_, c2_node_id) = mapping {
                if !mapped_c2_nodes.insert(*c2_node_id) {
                    return Err(format!(
                        "Node in c2 with ID {} is mapped more than once.",
                        c2_node_id
                    ));
                }
            }
        }

        // 4. Ensure no edge in c2 is mapped more than once
        let mut mapped_c2_edges: HashSet<usize> = HashSet::new();
        for mapping in self.edge_mapping.values() {
            if let EdgeMapping::RealEdge(_, c2_edge_id) = mapping {
                if !mapped_c2_edges.insert(*c2_edge_id) {
                    return Err(format!(
                        "Edge in c2 with ID {} is mapped more than once.",
                        c2_edge_id
                    ));
                }
            }
        }

        // 5. Calculate the total cost
        let mut total = 0.0;

        // 5a. Sum the costs of all node mappings
        for mapping in self.node_mapping.values() {
            total += mapping.cost();
        }

        // 5b. Sum the costs of all edge mappings
        for mapping in self.edge_mapping.values() {
            total += mapping.cost();
        }

        // 5c. Add cost for unmapped nodes in c2
        let mapped_c2_nodes_count = mapped_c2_nodes.len();
        let total_c2_nodes = self.c2.nodes.len();
        let unmapped_c2_nodes = (total_c2_nodes as isize - mapped_c2_nodes_count as isize).max(0) as f64;
        total += unmapped_c2_nodes;

        // 5d. Add cost for unmapped edges in c2
        let mapped_c2_edges_count = mapped_c2_edges.len();
        let total_c2_edges = self.c2.edges.len();
        let unmapped_c2_edges = (total_c2_edges as isize - mapped_c2_edges_count as isize).max(0) as f64;
        total += unmapped_c2_edges;

        Ok(total)
    }

    /// Prints the mappings of the alignment in a readable format.
    ///
    /// This includes:
    /// - How each node and edge in c1 is mapped to c2 or to a void.
    /// - Any nodes and edges in c2 that are not mapped.
    /// - Any nodes and edges in c1 that are not mapped (if alignment is invalid).
    fn print_mappings(&self) {
        println!("=== Node Mappings ===");
        // Track mapped c2 node IDs to identify unmapped nodes in c2 later
        let mut mapped_c2_nodes: HashSet<usize> = HashSet::new();

        for (c1_node_id, mapping) in &self.node_mapping {
            match mapping {
                NodeMapping::RealNode(_, c2_node_id) => {
                    println!("  c1 Node {} -> c2 Node {}", c1_node_id, c2_node_id);
                    mapped_c2_nodes.insert(*c2_node_id);
                }
                NodeMapping::VoidNode(_, void_node_id) => {
                    println!(
                        "  c1 Node {} -> VOID Node (Void ID: {})",
                        c1_node_id, void_node_id
                    );
                }
            }
        }

        // Identify and print any unmapped nodes in c1 (shouldn't exist if alignment is valid)
        let mapped_c1_nodes: HashSet<&usize> = self.node_mapping.keys().collect();
        let unmapped_c1_nodes: Vec<_> = self
            .c1
            .nodes
            .keys()
            .filter(|id| !mapped_c1_nodes.contains(id))
            .collect();

        if !unmapped_c1_nodes.is_empty() {
            println!("\n--- Unmapped c1 Nodes (Invalid Alignment) ---");
            for c1_node_id in unmapped_c1_nodes {
                println!("  c1 Node {} -> NOT MAPPED", c1_node_id);
            }
        }

        println!("\n--- Unmapped c2 Nodes ---");
        let unmapped_c2_nodes: Vec<_> = self
            .c2
            .nodes
            .keys()
            .filter(|id| !mapped_c2_nodes.contains(id))
            .collect();

        if unmapped_c2_nodes.is_empty() {
            println!("  All c2 nodes are mapped.");
        } else {
            for c2_node_id in unmapped_c2_nodes {
                println!("  c2 Node {} is UNMAPPED", c2_node_id);
            }
        }

        println!("\n=== Edge Mappings ===");
        // Track mapped c2 edge IDs to identify unmapped edges in c2 later
        let mut mapped_c2_edges: HashSet<usize> = HashSet::new();

        for (c1_edge_id, mapping) in &self.edge_mapping {
            match mapping {
                EdgeMapping::RealEdge(_, c2_edge_id) => {
                    println!("  c1 Edge {} -> c2 Edge {}", c1_edge_id, c2_edge_id);
                    mapped_c2_edges.insert(*c2_edge_id);
                }
                EdgeMapping::VoidEdge(_, void_edge_id) => {
                    println!(
                        "  c1 Edge {} -> VOID Edge (Void ID: {})",
                        c1_edge_id, void_edge_id
                    );
                }
            }
        }

        // Identify and print any unmapped edges in c1 (shouldn't exist if alignment is valid)
        let mapped_c1_edges: HashSet<&usize> = self.edge_mapping.keys().collect();
        let unmapped_c1_edges: Vec<_> = self
            .c1
            .edges
            .keys()
            .filter(|id| !mapped_c1_edges.contains(id))
            .collect();

        if !unmapped_c1_edges.is_empty() {
            println!("\n--- Unmapped c1 Edges (Invalid Alignment) ---");
            for c1_edge_id in unmapped_c1_edges {
                println!("  c1 Edge {} -> NOT MAPPED", c1_edge_id);
            }
        }

        println!("\n--- Unmapped c2 Edges ---");
        let unmapped_c2_edges: Vec<_> = self
            .c2
            .edges
            .keys()
            .filter(|id| !mapped_c2_edges.contains(id))
            .collect();

        if unmapped_c2_edges.is_empty() {
            println!("  All c2 edges are mapped.");
        } else {
            for c2_edge_id in unmapped_c2_edges {
                println!("  c2 Edge {} is UNMAPPED", c2_edge_id);
            }
        }

        // Optionally, print void nodes and edges details
        if !self.void_nodes.is_empty() {
            println!("\n--- Void Nodes in Alignment ---");
            for (void_id, node) in &self.void_nodes {
                println!("  Void Node ID {}: {:?}", void_id, node);
            }
        }

        if !self.void_edges.is_empty() {
            println!("\n--- Void Edges in Alignment ---");
            for (void_id, edge) in &self.void_edges {
                println!("  Void Edge ID {}: {:?}", void_id, edge);
            }
        }
    }
}

// Helper functions to check compatibility
fn nodes_compatible(n1: &Node, n2: &Node) -> bool {
    match (n1, n2) {
        (Node::Event(e1), Node::Event(e2)) => e1.event_type == e2.event_type,
        (Node::Object(o1), Node::Object(o2)) => o1.object_type == o2.object_type,
        _ => false,
    }
}

fn edges_compatible(e1: &Edge, e2: &Edge) -> bool {
    e1.edge_type == e2.edge_type
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_align() {
        // Example usage with test graphs
        let mut c1 = CaseGraph::new();
        let mut c2 = CaseGraph::new();

        // Populate c1
        let event1 = Node::Event(Event {
            id: 1,
            event_type: "A".to_string(),
        });
        let event2 = Node::Event(Event {
            id: 2,
            event_type: "B".to_string(),
        });
        let object1 = Node::Object(Object {
            id: 3,
            object_type: "Person".to_string(),
        });
        c1.add_node(event1);
        c1.add_node(event2);
        c1.add_node(object1);
        c1.add_edge(Edge::new(1, 1, 2, EdgeType::DF));
        c1.add_edge(Edge::new(2, 2, 3, EdgeType::E2O));

        // Populate c2
        let event3 = Node::Event(Event {
            id: 4,
            event_type: "A".to_string(),
        });
        let event4 = Node::Event(Event {
            id: 5,
            event_type: "B".to_string(),
        });
        let object2 = Node::Object(Object {
            id: 6,
            object_type: "Person".to_string(),
        });
        let object3 = Node::Object(Object {
            id: 7,
            object_type: "Device".to_string(),
        });
        c2.add_node(event3);
        c2.add_node(event4);
        c2.add_node(object2);
        c2.add_node(object3);
        c2.add_edge(Edge::new(3, 4, 5, EdgeType::DF));
        c2.add_edge(Edge::new(4, 5, 6, EdgeType::E2O));
        c2.add_edge(Edge::new(5, 5, 7, EdgeType::E2O));

        // Align using MIP
        let alignment = CaseAlignment::align_mip(&c1, &c2);

        // Print the alignment
        println!("Node Mappings:");
        for (&n1, mapping) in &alignment.node_mapping {
            match mapping {
                NodeMapping::RealNode(_, n2) => {
                    println!("c1 Node {} -> c2 Node {}", n1, n2);
                }
                NodeMapping::VoidNode(_, _) => {
                    println!("c1 Node {} -> Void", n1);
                }
            }
        }
        
        println!("\nEdge Mappings:");
        for (&e1, mapping) in &alignment.edge_mapping {
            match mapping {
                EdgeMapping::RealEdge(_, e2) => {
                    println!("c1 Edge {} -> c2 Edge {}", e1, e2);
                }
                EdgeMapping::VoidEdge(_, _) => {
                    println!("c1 Edge {} -> Void", e1);
                }
            }
        }


        // output the alignment total cost to console, if the alignment is valid else print the error message
        match alignment.total_cost() {
            Ok(cost) => println!("Total cost: {}", cost),
            Err(err) => println!("Error: {}", err),
        }
        
        alignment.print_mappings();
    }
}
