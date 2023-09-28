use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Place {
    id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Transition {
    pub label: Option<String>,
    id: Uuid,
}

#[derive(Debug)]
pub enum PetriNetNodes {
    None,
    Places(Vec<PlaceID>),
    Transitions(Vec<TransitionID>),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "nodes")]
pub enum ArcType {
    PlaceTransition(Uuid, Uuid),
    TransitionPlace(Uuid, Uuid),
}

impl ArcType {
    pub fn place_to_transition(from: PlaceID, to: TransitionID) -> ArcType {
        return ArcType::PlaceTransition(from.0, to.0);
    }
    pub fn transition_to_place(from: TransitionID, to: PlaceID) -> ArcType {
        return ArcType::TransitionPlace(from.0, to.0);
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Arc {
    pub from_to: ArcType,
    pub weight: u32,
}

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize, Eq, Hash)]
pub struct PlaceID(Uuid);
impl PlaceID {
    pub fn get_uuid(self) -> Uuid {
        self.0
    }
}
impl From<&Place> for PlaceID {
    fn from(value: &Place) -> Self {
        PlaceID(value.id)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct TransitionID(Uuid);
impl From<&Transition> for TransitionID {
    fn from(value: &Transition) -> Self {
        TransitionID(value.id)
    }
}

pub type Marking = HashMap<PlaceID, u64>;


#[derive(Debug, Deserialize, Serialize)]
pub struct PetriNet {
    pub places: HashMap<Uuid, Place>,
    pub transitions: HashMap<Uuid, Transition>,
    pub arcs: Vec<Arc>,
    pub initial_marking: Option<Marking>,
    pub final_markings: Option<Vec<Marking>>,
}

impl PetriNet {
    pub fn new() -> Self {
        Self {
            places: HashMap::new(),
            transitions: HashMap::new(),
            arcs: Vec::new(),
            initial_marking: None,
            final_markings: None,
        }
    }

    pub fn to_json(self) -> String {
        serde_json::to_string(&self).unwrap()
    }
    pub fn add_place(&mut self, place_id: Option<Uuid>) -> PlaceID {
        let place_id = place_id.unwrap_or(Uuid::new_v4());
        let place = Place { id: place_id };
        self.places.insert(place_id, place);
        return PlaceID(place_id);
    }

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
        return TransitionID(transition_id);
    }
    pub fn add_arc(&mut self, from_to: ArcType, weight: Option<u32>) {
        self.arcs.push(Arc {
            from_to: from_to,
            weight: weight.unwrap_or(1),
        });
    }

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

    pub fn preset_of_place(&self, p: PlaceID) -> Vec<TransitionID> {
        self.arcs
            .iter()
            .filter_map(|x: &Arc| match x.from_to {
                ArcType::TransitionPlace(from, to) if to == p.0 => Some(TransitionID(from)),
                _ => None,
            })
            .collect()
    }

    pub fn preset_of_transition(&self, t: TransitionID) -> Vec<PlaceID> {
        self.arcs
            .iter()
            .filter_map(|x: &Arc| match x.from_to {
                ArcType::PlaceTransition(from, to) if to == t.0 => Some(PlaceID(from)),
                _ => None,
            })
            .collect()
    }

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

    pub fn postset_of_place(&self, p: PlaceID) -> Vec<TransitionID> {
        self.arcs
            .iter()
            .filter_map(|x: &Arc| match x.from_to {
                ArcType::PlaceTransition(from, to) if from == p.0 => Some(TransitionID(to)),
                _ => None,
            })
            .collect()
    }

    pub fn postset_of_transition(&self, t: TransitionID) -> Vec<PlaceID> {
        self.arcs
            .iter()
            .filter_map(|x: &Arc| match x.from_to {
                ArcType::TransitionPlace(from, to) if from == t.0 => Some(PlaceID(to)),
                _ => None,
            })
            .collect()
    }
}

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
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn petri_nets() {
        let mut net = PetriNet::new();
        let p1 = net.add_place(None);
        let t1 = net.add_transition(Some("Have fun".into()), None);
        let t2 = net.add_transition(Some("Sleep".into()), None);
        net.add_arc(ArcType::place_to_transition(p1, t1), None);
        net.add_arc(ArcType::transition_to_place(t2, p1), None);
        println!("{}", serde_json::to_string(&net).unwrap());

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
