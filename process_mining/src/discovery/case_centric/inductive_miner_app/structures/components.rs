//! Component management utilities.
//!
//! This module provides a lightweight structure for maintaining connected components (partitions) of
//! a set of nodes. Each node belongs to exactly one component and components can be merged dynamically.
//!
//! The structure is primarily used to represent partitions of activities during algorithms for finding
//! cuts in event logs.
//!
//! # Implementation notes
//!
//! This is a port of the component structure implementation in the ProM framework ((`InductiveMiner`), originally written in Java.
//! - ProM source code:
//! https://github.com/promworkbench/InductiveMiner/blob/main/src/org/processmining/plugins/inductiveminer2/helperclasses/graphs/IntComponents.java

/// Port of the component structure implementation in
/// the ProM framework (`InductiveMiner`), originally written in Java.
///
/// - ProM source code:
/// https://github.com/promworkbench/InductiveMiner/blob/main/src/org/processmining/plugins/inductiveminer2/helperclasses/graphs/IntComponents.java


use std::borrow::Cow;
use std::collections::{HashMap, HashSet};


/// Maintains a partition of nodes into components.
///
/// Each node belongs to exactly one component. Components can be merged, queried and converted back
/// into explicit sets of nodes.
///
/// Internally, nodes are mapped to integer indices to allow for efficient component operations.
#[derive(Debug)]
pub struct Components<'a> {
    components: Vec<usize>,             // component index of each node, get node index from map
    node2index: HashMap<Cow<'a, str>, usize>, // index of every node in components
    number_of_components: usize,
}


impl<'a> Components<'a> {

    /// Creates a new component structure where each node initially forms its own component.
    pub fn new(nodes: &[Cow<'a, str>]) -> Self {
        let mut node2index = HashMap::new();
        // every node gets it own index in the beginning
        for (i, n) in nodes.iter().enumerate() {
            // clone is very cheap if cow is borrowed
            node2index.insert(n.clone(), i);
        }

        let len = nodes.len();
        Components {
            components: (0..len).collect(),
            node2index,
            number_of_components: len,
        }
    }


    /// Returns the component index of a given node.
    ///
    /// Panics if the node is not contained in the component structure.
    pub fn component_of(&self, node: &str) -> usize {
        self.components[self.node2index[node]]
    }

    /// Returns whether the nodes 'a' and 'b' are in the same component.
    pub fn same_component(&self, a: &str, b: &str) -> bool {
        self.component_of(a) == self.component_of(b)
    }

    /// Merges the components containing the nodes 'a' and 'b'.
    ///
    /// If both nodes already are in the same component, the structure remains unchanged.
    pub fn merge_components_of(&mut self, a: &str, b: &str) {
        let ca = self.component_of(a);
        let cb = self.component_of(b);
        self.merge_components(ca, cb);
    }

    /// Merge two components identified by their indices.
    ///
    /// All nodes belonging to the component 'ca' are reassigned to the component 'cb'.
    pub fn merge_components(&mut self, ca: usize, cb: usize) {
        if ca == cb {
            return;
        }
        let mut changed = false;
        for comp in self.components.iter_mut() {
            if *comp == ca {
                *comp = cb;
                changed = true;
            }
        }
        if changed {
            self.number_of_components -= 1;
        }
    }

    /// Returns the current partitioning of nodes as explicit sets.
    ///
    /// Each element of the returned vector represents a component containing the nodes belonging
    /// to that component.
    pub fn get_components(&self) -> Vec<HashSet<Cow<'a, str>>> {
        let mut result: Vec<HashSet<Cow<'a, str>>> = Vec::new();
        let mut map: HashMap<usize, usize> = HashMap::new();
        let mut next_idx = 0;

        // assign normalized indexes
        for comp in &self.components {
            if !map.contains_key(comp) {
                map.insert(*comp, next_idx);
                result.push(HashSet::new());
                next_idx += 1;
            }
        }

        // fill components
        for (node, idx) in &self.node2index {
            let comp = self.components[*idx];
            let part = map[&comp];
            result[part].insert(node.clone());
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_initial_components() {
        let nodes = vec!["A".into(), "B".into(), "C".into()];
        let c = Components::new(&nodes);

        assert_eq!(c.number_of_components, 3);
        assert!(!c.same_component("A", "B"));
        assert!(!c.same_component("B", "C"));
        assert!(!c.same_component("A", "C"));
    }

    #[test]
    fn test_simple_merge() {
        let nodes = vec!["A".into(), "B".into(), "C".into()];
        let mut c = Components::new(&nodes);

        c.merge_components_of("A", "B");

        assert!(c.same_component("A", "B"));
        assert!(!c.same_component("A", "C"));

        assert_eq!(c.number_of_components, 2);
    }

    #[test]
    fn test_chain_merge() {
        let nodes = vec!["A".into(), "B".into(), "C".into(), "D".into()];
        let mut c = Components::new(&nodes);

        c.merge_components_of("A", "B");
        c.merge_components_of("B", "C");

        // All A,B,C should be in the same component
        assert!(c.same_component("A", "C"));
        assert!(c.same_component("A", "B"));
        assert!(c.same_component("B", "C"));

        // D remains separate
        assert!(!c.same_component("A", "D"));

        assert_eq!(c.number_of_components, 2);
    }

    #[test]
    fn test_merge_same_component_does_not_decrease_count() {
        let nodes = vec!["A".into(), "B".into()];
        let mut c = Components::new(&nodes);

        c.merge_components_of("A", "B");
        assert_eq!(c.number_of_components, 1);

        // merging again should not decrease further
        c.merge_components_of("A", "B");
        assert_eq!(c.number_of_components, 1);
    }

    #[test]
    fn test_get_components() {
        let nodes = vec!["A".into(), "B".into(), "C".into(), "D".into()];
        let mut c = Components::new(&nodes);

        c.merge_components_of("A", "B");
        c.merge_components_of("C", "D");

        let comps = c.get_components();

        // each component should have 2 elements
        let mut sets: Vec<HashSet<Cow<'_, str>>> = comps.into_iter().collect();
        sets.sort_by_key(|s| s.len());

        assert_eq!(sets.len(), 2);

        let first = &sets[0];
        let second = &sets[1];

        assert!(first.contains("A") && first.contains("B") || first.contains("C") && first.contains("D"));
        assert!(second.contains("A") && second.contains("B") || second.contains("C") && second.contains("D"));
    }
}
