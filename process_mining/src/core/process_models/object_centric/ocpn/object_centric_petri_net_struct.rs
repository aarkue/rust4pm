//! Struct for object-centric Petri nets

use std::collections::{HashMap, HashSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::process_models::case_centric::petri_net::petri_net_struct::PetriNet;

/// An object-centric Petri net, as described in van der Aalst and Berti, "Discovering
/// Object-Centric Petri Nets" (Fundamenta Informaticae, 2020).
///
/// Stored as one accepting [`PetriNet`] per object type, so each place has the object type of its
/// net. Transitions with the same non-silent label in different nets are the same transition of
/// the object-centric net. Silent transitions are local to their object type.
///
/// A variable arc consumes or produces a set of objects at once, e.g., when an activity handles
/// all items of an order together. An arc between a place of type `ot` and the transition of
/// activity `a` is variable exactly if [`variable_arcs`](ObjectCentricPetriNet::variable_arcs) lists `a` for `ot`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ObjectCentricPetriNet {
    /// The accepting Petri net discovered for each object type.
    pub nets: HashMap<String, PetriNet>,
    /// Per object type, the activities whose arcs to places of that type are variable.
    pub variable_arcs: HashMap<String, HashSet<String>>,
}

impl ObjectCentricPetriNet {
    /// Creates an object-centric Petri net without any object types.
    pub fn new() -> Self {
        Self::default()
    }

    /// The object types of the net, sorted.
    pub fn object_types(&self) -> Vec<&str> {
        let mut types: Vec<&str> = self.nets.keys().map(String::as_str).collect();
        types.sort_unstable();
        types
    }

    /// The activities of the net, i.e., all non-silent transition labels, sorted and deduplicated.
    pub fn activities(&self) -> Vec<&str> {
        let mut activities: Vec<&str> = self
            .nets
            .values()
            .flat_map(|net| net.transitions.values())
            .filter_map(|transition| transition.label.as_deref())
            .collect();
        activities.sort_unstable();
        activities.dedup();
        activities
    }

    /// The object types whose net contains a transition for the given activity.
    pub fn object_types_of(&self, activity: &str) -> Vec<&str> {
        let mut types: Vec<&str> = self
            .nets
            .iter()
            .filter(|(_, net)| {
                net.transitions
                    .values()
                    .any(|transition| transition.label.as_deref() == Some(activity))
            })
            .map(|(object_type, _)| object_type.as_str())
            .collect();
        types.sort_unstable();
        types
    }

    /// Returns `true` if the arcs between the given activity and places of the given object type
    /// are variable, i.e. the activity handles a varying number of objects of that type at once.
    pub fn is_variable_arc(&self, object_type: &str, activity: &str) -> bool {
        self.variable_arcs
            .get(object_type)
            .is_some_and(|activities| activities.contains(activity))
    }

    /// Total number of places over all object types.
    pub fn num_places(&self) -> usize {
        self.nets.values().map(|net| net.places.len()).sum()
    }

    /// Total number of transitions over all object types, counting a shared activity once per
    /// type.
    pub fn num_transitions(&self) -> usize {
        self.nets.values().map(|net| net.transitions.len()).sum()
    }

    /// Total number of arcs over all object types.
    pub fn num_arcs(&self) -> usize {
        self.nets.values().map(|net| net.arcs.len()).sum()
    }

    /// Serializes to a JSON string in the [flat form](super::json::ObjectCentricPetriNetJson).
    pub fn to_json(&self) -> String {
        serde_json::to_string(&self.to_json_form()).unwrap()
    }

    /// Reads a net back from [`to_json`](Self::to_json).
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        Ok(Self::from_json_form(&serde_json::from_str(json)?))
    }
}

#[cfg(test)]
mod test_object_centric_petri_net {
    use super::*;
    use crate::core::process_models::process_tree::{Node, OperatorType, ProcessTree};

    fn net_of(activities: &[&str]) -> PetriNet {
        let mut sequence = Node::new_operator(OperatorType::Sequence);
        for activity in activities {
            sequence.add_child(Node::new_leaf(Some((*activity).to_string())));
        }
        ProcessTree::new(sequence).to_petri_net()
    }

    fn example() -> ObjectCentricPetriNet {
        ObjectCentricPetriNet {
            nets: HashMap::from([
                ("order".to_string(), net_of(&["place order", "pay"])),
                ("item".to_string(), net_of(&["place order", "pick"])),
            ]),
            variable_arcs: HashMap::from([(
                "item".to_string(),
                HashSet::from(["place order".to_string()]),
            )]),
        }
    }

    #[test]
    fn test_types_activities_and_shared_transitions() {
        let net = example();
        assert_eq!(net.object_types(), vec!["item", "order"]);
        assert_eq!(net.activities(), vec!["pay", "pick", "place order"]);
        assert_eq!(net.object_types_of("place order"), vec!["item", "order"]);
        assert_eq!(net.object_types_of("pay"), vec!["order"]);
        assert!(net.object_types_of("nonexistent").is_empty());
        assert_eq!(net.num_transitions(), 4);
    }

    #[test]
    fn test_variable_arcs() {
        let net = example();
        assert!(net.is_variable_arc("item", "place order"));
        assert!(!net.is_variable_arc("order", "place order"));
        assert!(!net.is_variable_arc("item", "pick"));
        assert!(!net.is_variable_arc("nonexistent", "place order"));
    }
}
