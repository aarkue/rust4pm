//! A flat JSON form of an object-centric Petri net, for exchanging one with other tools.
//!
//! One list of places, transitions and arcs rather than one net per object type. A transition
//! appears once no matter how many types share it, and an arc takes its type from the place it
//! connects. Names are derived from the net structure rather than from UUIDs, so exporting the
//! same net twice gives the same file. Places and transitions without arcs are not exported.
//!
//! ```text
//! {
//!   "places":      [{"name": "orders p0", "object_type": "orders"}],
//!   "transitions": [{"name": "place order", "label": "place order"},
//!                   {"name": "orders tau0", "label": null}],
//!   "arcs":        [{"source": "orders p0", "target": "place order",
//!                    "type": "variable", "weight": 1}],
//!   "initial_marking": {"orders p0": 1},
//!   "final_marking":   {"orders p3": 1}
//! }
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use uuid::Uuid;

use super::object_centric_petri_net_struct::ObjectCentricPetriNet;
use crate::core::process_models::case_centric::petri_net::petri_net_struct::{
    ArcType, Marking, PlaceID, TransitionID,
};

/// An object-centric Petri net as a flat list of places, transitions and arcs.
///
/// See the [module documentation](self).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ObjectCentricPetriNetJson {
    /// The places, each belonging to one object type.
    pub places: Vec<JsonPlace>,
    /// The transitions, each appearing once even when several object types share it.
    pub transitions: Vec<JsonTransition>,
    /// The arcs, each between a place and a transition.
    pub arcs: Vec<JsonArc>,
    /// Tokens per place at the start, by place name.
    pub initial_marking: BTreeMap<String, u64>,
    /// Tokens per place at the end, by place name. Only the first final marking of each object
    /// type is exported.
    pub final_marking: BTreeMap<String, u64>,
}

/// A place of one object type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct JsonPlace {
    /// Unique among all places and transitions.
    pub name: String,
    /// The object type whose tokens this place holds.
    pub object_type: String,
}

/// A transition, silent if it has no label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct JsonTransition {
    /// Unique among all places and transitions. For a labelled transition, the activity itself.
    pub name: String,
    /// The activity, or `None` for a silent transition.
    pub label: Option<String>,
}

/// An arc from a place to a transition or the other way round.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct JsonArc {
    /// Name of the place or transition the arc leaves.
    pub source: String,
    /// Name of the place or transition the arc enters.
    pub target: String,
    /// Whether the arc moves one object or a whole set of them.
    #[serde(rename = "type", default)]
    pub arc_type: JsonArcType,
    /// How many tokens the arc moves.
    #[serde(default = "one")]
    pub weight: u32,
}

fn one() -> u32 {
    1
}

/// Whether an arc moves a single object or all of them at once.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum JsonArcType {
    /// Moves one object of the place's type.
    #[default]
    Normal,
    /// Moves a set of objects of the place's type at once.
    Variable,
}

impl ObjectCentricPetriNet {
    /// Converts to the [flat JSON form](ObjectCentricPetriNetJson).
    pub fn to_json_form(&self) -> ObjectCentricPetriNetJson {
        let mut json = ObjectCentricPetriNetJson::default();
        let mut taken: HashSet<String> =
            self.activities().into_iter().map(str::to_string).collect();
        let mut exported_activities: HashSet<&str> = HashSet::new();
        let mut names: HashMap<Uuid, String> = HashMap::new();

        for object_type in self.object_types() {
            let net = &self.nets[object_type];
            let (mut next_place, mut next_tau) = (0, 0);
            let mut name_of = |id: Uuid, json: &mut ObjectCentricPetriNetJson| -> String {
                if let Some(name) = names.get(&id) {
                    return name.clone();
                }
                let name = match net.transitions.get(&id).map(|t| t.label.as_deref()) {
                    // Nets sharing an activity agree on its name, so it is written out once.
                    Some(Some(activity)) => {
                        if exported_activities.insert(activity) {
                            json.transitions.push(JsonTransition {
                                name: activity.to_string(),
                                label: Some(activity.to_string()),
                            });
                        }
                        activity.to_string()
                    }
                    Some(None) => {
                        let name =
                            fresh_name(&format!("{object_type} tau"), &mut next_tau, &mut taken);
                        json.transitions.push(JsonTransition {
                            name: name.clone(),
                            label: None,
                        });
                        name
                    }
                    None => {
                        let name =
                            fresh_name(&format!("{object_type} p"), &mut next_place, &mut taken);
                        json.places.push(JsonPlace {
                            name: name.clone(),
                            object_type: object_type.to_string(),
                        });
                        name
                    }
                };
                names.insert(id, name.clone());
                name
            };

            // Arcs are stored in a fixed order, unlike places and transitions, so naming nodes
            // along them keeps the names reproducible.
            for arc in &net.arcs {
                let (from, to, transition) = match arc.from_to {
                    ArcType::PlaceTransition(place, transition) => (place, transition, transition),
                    ArcType::TransitionPlace(transition, place) => (transition, place, transition),
                };
                let variable = net
                    .transitions
                    .get(&transition)
                    .and_then(|t| t.label.as_deref())
                    .is_some_and(|activity| self.is_variable_arc(object_type, activity));
                let source = name_of(from, &mut json);
                let target = name_of(to, &mut json);
                json.arcs.push(JsonArc {
                    source,
                    target,
                    arc_type: if variable {
                        JsonArcType::Variable
                    } else {
                        JsonArcType::Normal
                    },
                    weight: arc.weight,
                });
            }

            for (place, tokens) in net.initial_marking.iter().flatten() {
                if let Some(name) = names.get(&place.0) {
                    json.initial_marking.insert(name.clone(), *tokens);
                }
            }
            for (place, tokens) in net.final_markings.iter().flatten().take(1).flatten() {
                if let Some(name) = names.get(&place.0) {
                    json.final_marking.insert(name.clone(), *tokens);
                }
            }
        }

        json
    }

    /// Rebuilds an object-centric Petri net from the [flat JSON form](ObjectCentricPetriNetJson).
    ///
    /// Places are grouped into one net per object type, and a transition joins every net that has
    /// an arc to it. Identifiers are new, so only the structure survives a round trip.
    pub fn from_json_form(json: &ObjectCentricPetriNetJson) -> Self {
        let type_of: HashMap<&str, &str> = json
            .places
            .iter()
            .map(|place| (place.name.as_str(), place.object_type.as_str()))
            .collect();
        let label_of: HashMap<&str, Option<&str>> = json
            .transitions
            .iter()
            .map(|transition| (transition.name.as_str(), transition.label.as_deref()))
            .collect();

        let mut ocpn = Self::new();
        let mut ids: HashMap<(&str, &str), Uuid> = HashMap::new();

        for arc in &json.arcs {
            let from_place = type_of.contains_key(arc.source.as_str());
            let (place, transition) = if from_place {
                (arc.source.as_str(), arc.target.as_str())
            } else {
                (arc.target.as_str(), arc.source.as_str())
            };
            let Some(&object_type) = type_of.get(place) else {
                continue;
            };
            let net = ocpn.nets.entry(object_type.to_string()).or_default();

            let place_id = PlaceID(
                *ids.entry((object_type, place))
                    .or_insert_with(|| net.add_place(None).0),
            );
            let label = label_of.get(transition).copied().flatten();
            let transition_id = TransitionID(
                *ids.entry((object_type, transition))
                    .or_insert_with(|| net.add_transition(label.map(str::to_string), None).0),
            );
            let from_to = if from_place {
                ArcType::place_to_transition(place_id, transition_id)
            } else {
                ArcType::transition_to_place(transition_id, place_id)
            };
            net.add_arc(from_to, Some(arc.weight));

            if let (JsonArcType::Variable, Some(activity)) = (arc.arc_type, label) {
                ocpn.variable_arcs
                    .entry(object_type.to_string())
                    .or_default()
                    .insert(activity.to_string());
            }
        }

        let markings_per_type = |marking: &BTreeMap<String, u64>| {
            let mut per_type: HashMap<&str, Marking> = HashMap::new();
            for (place, &tokens) in marking {
                let Some(&object_type) = type_of.get(place.as_str()) else {
                    continue;
                };
                if let Some(&id) = ids.get(&(object_type, place.as_str())) {
                    per_type
                        .entry(object_type)
                        .or_default()
                        .insert(PlaceID(id), tokens);
                }
            }
            per_type
        };
        for (object_type, marking) in markings_per_type(&json.initial_marking) {
            if let Some(net) = ocpn.nets.get_mut(object_type) {
                net.initial_marking = Some(marking);
            }
        }
        for (object_type, marking) in markings_per_type(&json.final_marking) {
            if let Some(net) = ocpn.nets.get_mut(object_type) {
                net.final_markings = Some(vec![marking]);
            }
        }

        ocpn
    }
}

/// The first name of the form `{prefix}{n}`, counting `n` up from `counter`, that is not taken yet.
fn fresh_name(prefix: &str, counter: &mut usize, taken: &mut HashSet<String>) -> String {
    loop {
        let name = format!("{prefix}{counter}");
        *counter += 1;
        if taken.insert(name.clone()) {
            return name;
        }
    }
}

#[cfg(test)]
mod test_json_form {
    use super::*;
    use crate::core::process_models::case_centric::petri_net::petri_net_struct::PetriNet;
    use crate::core::process_models::process_tree::{Node, OperatorType, ProcessTree};

    fn net_of(activities: &[&str]) -> PetriNet {
        let mut sequence = Node::new_operator(OperatorType::Sequence);
        for activity in activities {
            sequence.add_child(Node::new_leaf(Some(activity.to_string())));
        }
        ProcessTree::new(sequence).to_petri_net()
    }

    fn example() -> ObjectCentricPetriNet {
        let mut ocpn = ObjectCentricPetriNet::new();
        ocpn.nets
            .insert("orders".to_string(), net_of(&["place order", "pay order"]));
        ocpn.nets
            .insert("items".to_string(), net_of(&["place order", "pick item"]));
        ocpn.variable_arcs.insert(
            "items".to_string(),
            HashSet::from(["place order".to_string()]),
        );
        ocpn
    }

    #[test]
    fn test_shared_transitions_appear_once() {
        let json = example().to_json_form();

        // "place order" is in both nets but is one transition of the object-centric net.
        assert_eq!(
            json.transitions
                .iter()
                .filter(|t| t.name == "place order")
                .count(),
            1
        );
        assert_eq!(json.transitions.len(), 3);
        assert_eq!(json.places.len(), 6);
        assert_eq!(json.arcs.len(), 8);

        // Every arc of the items type at "place order" is variable, and no other arc is.
        let variable: Vec<&JsonArc> = json
            .arcs
            .iter()
            .filter(|arc| arc.arc_type == JsonArcType::Variable)
            .collect();
        assert_eq!(variable.len(), 2);
        assert!(variable
            .iter()
            .all(|arc| arc.source == "place order" || arc.target == "place order"));
    }

    #[test]
    fn test_export_is_reproducible() {
        assert_eq!(example().to_json_form(), example().to_json_form());
    }

    #[test]
    fn test_round_trip_keeps_the_structure() {
        let json = example().to_json_form();
        let rebuilt = ObjectCentricPetriNet::from_json_form(&json);

        assert_eq!(rebuilt.object_types(), example().object_types());
        assert_eq!(rebuilt.activities(), example().activities());
        assert_eq!(rebuilt.num_places(), example().num_places());
        assert_eq!(rebuilt.num_transitions(), example().num_transitions());
        assert_eq!(rebuilt.num_arcs(), example().num_arcs());
        assert_eq!(rebuilt.variable_arcs, example().variable_arcs);
        assert_eq!(rebuilt.to_json_form(), json);
    }
}
