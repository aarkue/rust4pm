use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
/// Place in a Petri net
pub struct Place {
    id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
/// Transition in a Petri net
pub struct Transition {
    /// Transition label (None if this transition is _invisible_)
    pub label: Option<String>,
    id: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]

/// Nodes (Places or Transitions) in a Petri net
pub enum PetriNetNodes {
    /// None
    None,
    /// List of places
    Places(Vec<PlaceID>),
    /// List of transitions
    Transitions(Vec<TransitionID>),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "nodes")]
/// Arc type in a Petri net
pub enum ArcType {
    /// From Place to Transition
    PlaceTransition(Uuid, Uuid),
    /// From Transition to Place
    TransitionPlace(Uuid, Uuid),
}

impl ArcType {
    /// Create new from place to transition
    pub fn place_to_transition(from: PlaceID, to: TransitionID) -> ArcType {
        ArcType::PlaceTransition(from.0, to.0)
    }
    /// Create new from transition to place
    pub fn transition_to_place(from: TransitionID, to: PlaceID) -> ArcType {
        ArcType::TransitionPlace(from.0, to.0)
    }
}

#[derive(Debug, Deserialize, Serialize)]
/// Arc in a Petri net
///
/// Connecting a transition and a place (or the other way around)
pub struct Arc {
    /// Source and target of Arc
    pub from_to: ArcType,
    /// Weight (i.e., how many tokens this arc moves)
    pub weight: u32,
}

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize, Eq, Hash)]
/// Place ID
pub struct PlaceID(pub Uuid);
impl PlaceID {
    /// Get UUID
    pub fn get_uuid(self) -> Uuid {
        self.0
    }
}
impl From<&Place> for PlaceID {
    fn from(value: &Place) -> Self {
        PlaceID(value.id)
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
/// Transition ID
pub struct TransitionID(pub Uuid);
impl From<&Transition> for TransitionID {
    fn from(value: &Transition) -> Self {
        TransitionID(value.id)
    }
}
impl TransitionID {
    /// Get  UUID
    pub fn get_uuid(self) -> Uuid {
        self.0
    }
}

/// Marking of a Petri net: Assigning [`PlaceID`]s to a number of tokens
pub type Marking = HashMap<PlaceID, u64>;

#[derive(Debug, Deserialize, Serialize)]
///
/// A Petri net of [`Place`]s and [`Transition`]s
///
/// Bipartite graph of [`Place`]s and [`Transition`]s with [`Arc`]s connecting them, as well as initial and final [`Marking`]s
pub struct PetriNet {
    /// Places
    pub places: HashMap<Uuid, Place>,
    /// Transitions
    pub transitions: HashMap<Uuid, Transition>,
    /// Arcs
    pub arcs: Vec<Arc>,
    /// Initial marking
    pub initial_marking: Option<Marking>,
    /// Final markings (any of them are accepted as a final marking)
    pub final_markings: Option<Vec<Marking>>,
}

impl Default for PetriNet {
    fn default() -> Self {
        Self::new()
    }
}
impl PetriNet {
    /// Create new [`PetriNet`] with no places or transitions
    pub fn new() -> Self {
        Self {
            places: HashMap::new(),
            transitions: HashMap::new(),
            arcs: Vec::new(),
            initial_marking: None,
            final_markings: None,
        }
    }
    /// Serialize to JSON string
    pub fn to_json(self) -> String {
        serde_json::to_string(&self).unwrap()
    }
    /// Add a place (with an optional passed UUID)
    ///
    /// If no ID is passed, a new UUID will be generated
    pub fn add_place(&mut self, place_id: Option<Uuid>) -> PlaceID {
        let place_id = place_id.unwrap_or(Uuid::new_v4());
        let place = Place { id: place_id };
        self.places.insert(place_id, place);
        PlaceID(place_id)
    }

    /// Add a transition with an label (and with an optional passed UUID)
    ///
    /// If no ID is passed, a new UUID will be generated
    pub fn add_transition(
        &mut self,
        label: Option<String>,
        transition_id: Option<Uuid>,
    ) -> TransitionID {
        let transition_id = transition_id.unwrap_or(Uuid::new_v4());
        let transition = Transition {
            id: transition_id,
            label,
        };
        self.transitions.insert(transition_id, transition);
        TransitionID(transition_id)
    }
    /// Add an arc
    pub fn add_arc(&mut self, from_to: ArcType, weight: Option<u32>) {
        self.arcs.push(Arc {
            from_to,
            weight: weight.unwrap_or(1),
        });
    }

    /// Get the preset of a [`PetriNet`] node referred to by passed id
    pub fn preset_of(&self, id: Uuid) -> PetriNetNodes {
        if self.places.contains_key(&id) {
            let p = self.places.get(&id).unwrap();
            PetriNetNodes::Transitions(self.preset_of_place(p.into()))
        } else if self.transitions.contains_key(&id) {
            let t = self.transitions.get(&id).unwrap();
            PetriNetNodes::Places(self.preset_of_transition(t.into()))
        } else {
            PetriNetNodes::None
        }
    }

    /// Get the preset of a [`PetriNet`] place
    pub fn preset_of_place(&self, p: PlaceID) -> Vec<TransitionID> {
        self.arcs
            .iter()
            .filter_map(|x: &Arc| match x.from_to {
                ArcType::TransitionPlace(from, to) if to == p.0 => Some(TransitionID(from)),
                _ => None,
            })
            .collect()
    }

    /// Get the preset of [`PetriNet`] transition referred to by passed id
    pub fn preset_of_transition(&self, t: TransitionID) -> Vec<PlaceID> {
        self.arcs
            .iter()
            .filter_map(|x: &Arc| match x.from_to {
                ArcType::PlaceTransition(from, to) if to == t.0 => Some(PlaceID(from)),
                _ => None,
            })
            .collect()
    }

    /// Get postset of [`PetriNet`] node referred to by passed id
    pub fn postset_of(&self, id: Uuid) -> PetriNetNodes {
        if self.places.contains_key(&id) {
            let p = self.places.get(&id).unwrap();
            PetriNetNodes::Transitions(self.postset_of_place(p.into()))
        } else if self.transitions.contains_key(&id) {
            let t = self.transitions.get(&id).unwrap();
            PetriNetNodes::Places(self.postset_of_transition(t.into()))
        } else {
            PetriNetNodes::None
        }
    }

    /// Get postset of [`PetriNet`] place referred to by passed id
    pub fn postset_of_place(&self, p: PlaceID) -> Vec<TransitionID> {
        self.arcs
            .iter()
            .filter_map(|x: &Arc| match x.from_to {
                ArcType::PlaceTransition(from, to) if from == p.0 => Some(TransitionID(to)),
                _ => None,
            })
            .collect()
    }

    /// Get postset of [`PetriNet`] transition referred to by passed id
    pub fn postset_of_transition(&self, t: TransitionID) -> Vec<PlaceID> {
        self.arcs
            .iter()
            .filter_map(|x: &Arc| match x.from_to {
                ArcType::TransitionPlace(from, to) if from == t.0 => Some(PlaceID(to)),
                _ => None,
            })
            .collect()
    }

    /// Check if place is in initial marking
    pub fn is_in_initial_marking(&self, p: &PlaceID) -> bool {
        self.initial_marking.is_some() && self.initial_marking.as_ref().unwrap().contains_key(p)
    }

    /// Check if place is in _any_ final marking
    pub fn is_in_a_final_marking(&self, p: &PlaceID) -> bool {
        self.final_markings.is_some()
            && self
                .final_markings
                .as_ref()
                .unwrap()
                .iter()
                .any(|m| m.contains_key(p))
    }

    #[cfg(feature = "graphviz-export")]
    /// Export Petri net as a PNG image
    ///
    /// The PNG file is written to the specified filepath
    ///
    /// _Note_: This is an export method for __visualizing__ the Petri net.
    /// The resulting PNG file cannot be imported as a Petri net again (for that functionality, see [`PetriNet::export_pnml`]).
    ///
    /// Only available with the `graphviz-export` feature.
    pub fn export_png(&self, path: &str) -> Result<(), std::io::Error> {
        super::image_export::export_petri_net_image_png(self, path)
    }

    #[cfg(feature = "graphviz-export")]
    /// Export Petri net as a SVG image
    ///
    /// The SVG file is written to the specified filepath
    ///
    /// _Note_: This is an export method for __visualizing__ the Petri net.
    /// The resulting SVG file cannot be imported as a Petri net again (for that functionality, see [`PetriNet::export_pnml`]).
    ///
    /// Only available with the `graphviz-export` feature.
    pub fn export_svg(&self, path: &str) -> Result<(), std::io::Error> {
        super::image_export::export_petri_net_image_svg(self, path)
    }

    /// Export Petri net to a PNML file
    ///
    /// The PNML file is written to the specified filepath
    ///
    /// _Note_: This is an export method for __saving__ the Petri net data.
    /// The resulting file can also be imported as a Petri net again (see [`PetriNet::import_pnml`]).
    pub fn export_pnml(&self, path: &str) {
        super::export_pnml::export_petri_net_to_pnml(self, path)
    }
    /// Import Petri net from a PNML file
    ///
    /// The PNML file is read from the specified filepath
    ///
    ///
    /// For the related export function, see [`PetriNet::export_pnml`])
    pub fn import_pnml(path: &str) -> Result<PetriNet, quick_xml::Error> {
        super::import_pnml::import_pnml(&mut quick_xml::Reader::from_file(path)?)
    }
}

#[cfg(test)]
mod tests {
    pub const SAMPLE_JSON_NET: &str = r#"
{
    "places": {
        "f20ded2a-d308-44d7-abb2-6d0acd30e43e": {
            "id": "f20ded2a-d308-44d7-abb2-6d0acd30e43e"
        },
        "25f9c84b-f220-4e7f-a86e-bb3f82676bb9": {
            "id": "25f9c84b-f220-4e7f-a86e-bb3f82676bb9"
        },
        "15810d3d-922c-43fc-bcd5-8d6e592ea537": {
            "id": "15810d3d-922c-43fc-bcd5-8d6e592ea537"
        },
        "a75faf03-731d-4c8c-9810-5a36c7e8c26b": {
            "id": "a75faf03-731d-4c8c-9810-5a36c7e8c26b"
        }
    },
    "transitions": {
        "0c768c77-6408-4f4f-88b8-13d9cc8fca20": {
            "id": "0c768c77-6408-4f4f-88b8-13d9cc8fca20",
            "label": "Inform User"
        },
        "54f78f93-523f-4e1e-a0f7-cd79e73dc473": {
            "id": "54f78f93-523f-4e1e-a0f7-cd79e73dc473",
            "label": "Register"
        },
        "f18e00b0-e90b-48f6-99b7-9ee526571213": {
            "id": "f18e00b0-e90b-48f6-99b7-9ee526571213",
            "label": "Archive Repair"
        },
        "97d666fc-a78b-481d-9a5a-0cad157682ca": {
            "id": "97d666fc-a78b-481d-9a5a-0cad157682ca",
            "label": "Analyze Defect"
        },
        "78266d34-8abf-43ab-99bc-69e5e93c24b1": {
            "id": "78266d34-8abf-43ab-99bc-69e5e93c24b1",
            "label": "Repair (Simple)"
        },
        "5e8f7aff-81d4-4822-a30f-875ecc0a06f0": {
            "id": "5e8f7aff-81d4-4822-a30f-875ecc0a06f0",
            "label": "Repair (Complex)"
        },
        "18915408-cc29-4a7c-ab93-8a33e78a277a": {
            "id": "18915408-cc29-4a7c-ab93-8a33e78a277a",
            "label": "Test Repair"
        },
        "2da04f6f-dacb-46ac-82fd-39d0dfe44c33": {
            "id": "2da04f6f-dacb-46ac-82fd-39d0dfe44c33",
            "label": "Restart Repair"
        }
    },
    "arcs": [
        {
            "from_to": {
                "type": "TransitionPlace",
                "nodes": [
                    "f18e00b0-e90b-48f6-99b7-9ee526571213",
                    "f20ded2a-d308-44d7-abb2-6d0acd30e43e"
                ]
            },
            "weight": 1
        },
        {
            "from_to": {
                "type": "TransitionPlace",
                "nodes": [
                    "0c768c77-6408-4f4f-88b8-13d9cc8fca20",
                    "a75faf03-731d-4c8c-9810-5a36c7e8c26b"
                ]
            },
            "weight": 1
        },
        {
            "from_to": {
                "type": "PlaceTransition",
                "nodes": [
                    "15810d3d-922c-43fc-bcd5-8d6e592ea537",
                    "54f78f93-523f-4e1e-a0f7-cd79e73dc473"
                ]
            },
            "weight": 1
        },
        {
            "from_to": {
                "type": "PlaceTransition",
                "nodes": [
                    "25f9c84b-f220-4e7f-a86e-bb3f82676bb9",
                    "f18e00b0-e90b-48f6-99b7-9ee526571213"
                ]
            },
            "weight": 1
        },
        {
            "from_to": {
                "type": "TransitionPlace",
                "nodes": [
                    "0c768c77-6408-4f4f-88b8-13d9cc8fca20",
                    "25f9c84b-f220-4e7f-a86e-bb3f82676bb9"
                ]
            },
            "weight": 1
        }
    ]
}
"#;
    use super::*;

    #[test]
    fn petri_nets() {
        let mut net = PetriNet::new();
        let p1 = net.add_place(None);
        let t1 = net.add_transition(Some("Have fun".into()), None);
        let t2 = net.add_transition(Some("Sleep".into()), None);
        net.add_arc(ArcType::place_to_transition(p1, t1), None);
        net.add_arc(ArcType::transition_to_place(t2, p1), None);

        assert!(net.postset_of_transition(t1).is_empty());
        assert!(net.preset_of_transition(t1) == vec![p1]);
        assert!(net.postset_of_place(p1) == vec![t1]);
        assert!(net.preset_of_place(p1) == vec![t2]);
        assert!(net.preset_of_transition(t2).is_empty());
    }

    #[test]
    fn deserialize_petri_net_test() {
        let pn: PetriNet = serde_json::from_str(SAMPLE_JSON_NET).unwrap();
        assert!(pn.places.len() == 4);
        assert!(
            pn.postset_of_transition(TransitionID(
                Uuid::parse_str("0c768c77-6408-4f4f-88b8-13d9cc8fca20").unwrap()
            ))
            .len()
                == 2
        );
    }
}
