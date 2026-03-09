/// This implementation is inspired by the component structure in
/// the ProM framework (`InductiveMiner`), originally written in Java.
///
/// - ProM source code:
/// https://github.com/promworkbench/InductiveMiner/blob/main/src/org/processmining/plugins/inductiveminer2/helperclasses/graphs/IntComponents.java


use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct Components<'a> {
    components: Vec<usize>,             // component index of each node, get node index from map
    node2index: HashMap<Cow<'a, str>, usize>, // index of every node in components
    number_of_components: usize,
}


impl<'a> Components<'a> {
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

    pub fn from(partitions: &Vec<HashSet<Cow<'a, str>>>) -> Self {
        let mut node2index = HashMap::new();
        let mut node_number: usize = 0;

        for part in partitions.iter() {
            for act in part.iter() {
                node2index.insert(act.clone(), node_number);
                node_number += 1;
            }
        }

        let mut components = vec![0;node_number];


        let mut node_number: usize = 0;
        for (component_number, part) in partitions.iter().enumerate() {
            for _ in part.iter(){
                components[node_number] = component_number;
                node_number += 1;
            }
        }

        Self{components, node2index, number_of_components: partitions.len()}

    }


    pub fn component_of(&self, node: &str) -> usize {
        self.components[self.node2index[node]]
    }

    pub fn same_component(&self, a: &str, b: &str) -> bool {
        self.component_of(a) == self.component_of(b)
    }

    pub fn merge_components_of(&mut self, a: &str, b: &str) {
        let ca = self.component_of(a);
        let cb = self.component_of(b);
        self.merge_components(ca, cb);
    }

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
