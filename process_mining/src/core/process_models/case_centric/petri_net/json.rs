//! Loading Petri nets from JSON, in the current shape and in the shape of earlier versions
use std::collections::HashMap;

use itertools::Itertools;
use serde::{
    de::{Error as _, IgnoredAny},
    Deserialize, Deserializer,
};
use uuid::Uuid;

use super::{
    marking::Marking,
    net::{AcceptingPetriNet, ArcType, PetriNet, PlaceID, TransitionID},
};

/// The structure of a net, as lists (current shape) or as UUID-keyed maps with an arc list
/// (shape of earlier versions)
#[derive(Deserialize)]
struct StructureJson {
    places: Nodes<PlaceJson, IgnoredAny>,
    transitions: Nodes<TransitionJson, LegacyTransition>,
    #[serde(default)]
    arcs: Vec<LegacyArc>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Nodes<T, L> {
    Current(Vec<T>),
    Legacy(HashMap<Uuid, L>),
}

#[derive(Deserialize)]
struct PlaceJson {
    name: Option<String>,
}

#[derive(Deserialize)]
struct TransitionJson {
    label: Option<String>,
    name: Option<String>,
    #[serde(default)]
    inputs: Vec<(u32, u32)>,
    #[serde(default)]
    outputs: Vec<(u32, u32)>,
}

#[derive(Deserialize)]
struct LegacyTransition {
    label: Option<String>,
}

#[derive(Deserialize)]
struct LegacyArc {
    from_to: LegacyArcType,
    weight: u32,
}

#[derive(Deserialize)]
#[serde(tag = "type", content = "nodes")]
enum LegacyArcType {
    PlaceTransition(Uuid, Uuid),
    TransitionPlace(Uuid, Uuid),
}

#[derive(Deserialize)]
struct AcceptingJson {
    #[serde(flatten)]
    structure: StructureJson,
    initial_marking: Option<HashMap<String, u64>>,
    final_marking: Option<HashMap<String, u64>>,
    /// Earlier versions allowed several final markings
    final_markings: Option<Vec<HashMap<String, u64>>>,
}

/// How a document names places: by index, or by UUID in the legacy shape
enum PlaceKeys {
    Index(usize),
    Uuid(HashMap<Uuid, PlaceID>),
}

impl PlaceKeys {
    fn resolve(&self, key: &str) -> Result<PlaceID, String> {
        let place = match self {
            PlaceKeys::Index(num_places) => key
                .parse::<u32>()
                .ok()
                .filter(|index| (*index as usize) < *num_places)
                .map(PlaceID),
            PlaceKeys::Uuid(places) => key
                .parse::<Uuid>()
                .ok()
                .and_then(|uuid| places.get(&uuid).copied()),
        };
        place.ok_or_else(|| format!("a marking refers to place {key}, which is not in the net"))
    }

    fn marking(&self, tokens: HashMap<String, u64>) -> Result<Marking, String> {
        tokens
            .into_iter()
            .map(|(key, count)| {
                let count = u32::try_from(count)
                    .map_err(|_| format!("place {key} holds {count} tokens, more than u32::MAX"))?;
                Ok((self.resolve(&key)?, count))
            })
            .collect()
    }
}

impl StructureJson {
    fn load(self) -> Result<(PetriNet, PlaceKeys), String> {
        let mut net = PetriNet::new();
        let keys = match (self.places, self.transitions) {
            (Nodes::Current(places), Nodes::Current(transitions)) => {
                if !self.arcs.is_empty() {
                    return Err(
                        "`arcs` only exists in the legacy shape, where `places` and \
                                `transitions` are maps"
                            .into(),
                    );
                }
                for place in places {
                    net.add_place(place.name);
                }
                let num_places = net.num_places();
                for t in transitions {
                    let id = net.add_transition(t.label, t.name);
                    let place = |index: u32| {
                        if (index as usize) < num_places {
                            Ok(PlaceID(index))
                        } else {
                            Err(format!(
                                "transition {} has an arc to place {index}, which is not in the net",
                                id.index()
                            ))
                        }
                    };
                    for (input, weight) in t.inputs {
                        net.add_arc(ArcType::PlaceTransition(place(input)?, id), Some(weight));
                    }
                    for (output, weight) in t.outputs {
                        net.add_arc(ArcType::TransitionPlace(id, place(output)?), Some(weight));
                    }
                }
                PlaceKeys::Index(net.num_places())
            }
            (Nodes::Legacy(places), Nodes::Legacy(transitions)) => {
                // Sorted UUIDs give the order the algorithms of earlier versions used
                let place_ids: HashMap<Uuid, PlaceID> = places
                    .into_keys()
                    .sorted()
                    .map(|uuid| (uuid, net.add_place(Some(uuid.to_string()))))
                    .collect();
                let transition_ids: HashMap<Uuid, TransitionID> = transitions
                    .into_iter()
                    .sorted_by_key(|(uuid, _)| *uuid)
                    .map(|(uuid, t)| (uuid, net.add_transition(t.label, Some(uuid.to_string()))))
                    .collect();
                let unknown =
                    |uuid: &Uuid| format!("an arc refers to {uuid}, which is not in the net");
                let place =
                    |uuid: Uuid| place_ids.get(&uuid).copied().ok_or_else(|| unknown(&uuid));
                let transition = |uuid: Uuid| {
                    transition_ids
                        .get(&uuid)
                        .copied()
                        .ok_or_else(|| unknown(&uuid))
                };
                for arc in self.arcs {
                    let from_to = match arc.from_to {
                        LegacyArcType::PlaceTransition(p, t) => {
                            ArcType::PlaceTransition(place(p)?, transition(t)?)
                        }
                        LegacyArcType::TransitionPlace(t, p) => {
                            ArcType::TransitionPlace(transition(t)?, place(p)?)
                        }
                    };
                    net.add_arc(from_to, Some(arc.weight));
                }
                PlaceKeys::Uuid(place_ids)
            }
            _ => {
                return Err(
                    "`places` and `transitions` must both be lists (or, in the legacy \
                            shape, both maps)"
                        .into(),
                )
            }
        };
        Ok((net, keys))
    }
}

impl AcceptingJson {
    fn load(self) -> Result<AcceptingPetriNet, String> {
        let (net, keys) = self.structure.load()?;
        let initial_marking = self.initial_marking.ok_or("missing initial marking")?;
        let final_markings: Vec<_> = self
            .final_marking
            .into_iter()
            .chain(self.final_markings.into_iter().flatten())
            .collect();
        let [final_marking] = <[_; 1]>::try_from(final_markings).map_err(|found: Vec<_>| {
            format!("expected exactly one final marking, found {}", found.len())
        })?;
        Ok(AcceptingPetriNet::new(
            net,
            keys.marking(initial_marking)?,
            keys.marking(final_marking)?,
        ))
    }
}

/// Markings in the document are ignored
impl<'de> Deserialize<'de> for PetriNet {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (net, _) = StructureJson::deserialize(deserializer)?
            .load()
            .map_err(D::Error::custom)?;
        Ok(net)
    }
}

impl<'de> Deserialize<'de> for AcceptingPetriNet {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        AcceptingJson::deserialize(deserializer)?
            .load()
            .map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use crate::core::process_models::case_centric::petri_net::{
        marking::Marking,
        net::{AcceptingPetriNet, ArcType, PetriNet, PlaceID},
    };

    /// A net in the shape written by earlier versions (UUID-keyed maps and an arc list)
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

    const REGISTER_INPUT: &str = "15810d3d-922c-43fc-bcd5-8d6e592ea537";
    const INFORM_OUTPUT: &str = "a75faf03-731d-4c8c-9810-5a36c7e8c26b";

    fn legacy(edit: impl FnOnce(&mut Value)) -> String {
        let mut value: Value = serde_json::from_str(SAMPLE_JSON_NET).unwrap();
        edit(&mut value);
        value.to_string()
    }

    fn place_named(net: &PetriNet, name: &str) -> PlaceID {
        net.places()
            .find(|(_, p)| p.name.as_deref() == Some(name))
            .unwrap()
            .0
    }

    #[test]
    fn new_shape_is_pinned() {
        let mut net = PetriNet::new();
        let p = net.add_place(Some("p1".into()));
        let q = net.add_place(None);
        let t = net.add_transition(Some("a".into()), Some("t1".into()));
        net.add_arc(ArcType::place_to_transition(p, t), None);
        net.add_arc(ArcType::transition_to_place(t, q), Some(2));
        let accepting =
            AcceptingPetriNet::new(net, Marking::from([(p, 1)]), Marking::from([(q, 2)]));
        assert_eq!(
            serde_json::to_value(&accepting).unwrap(),
            json!({
                "places": [{"name": "p1"}, {}],
                "transitions": [{"label": "a", "name": "t1", "inputs": [[0, 1]], "outputs": [[1, 2]]}],
                "initial_marking": {"0": 1},
                "final_marking": {"1": 2}
            })
        );
        let back: AcceptingPetriNet = serde_json::from_str(&accepting.to_json()).unwrap();
        assert_eq!(back, accepting);
        let net_back: PetriNet = serde_json::from_str(&accepting.net.to_json()).unwrap();
        assert_eq!(net_back, accepting.net);
    }

    #[test]
    fn out_of_range_place_is_rejected() {
        let json = r#"{"places": [{}], "transitions": [{"label": null, "inputs": [[5, 1]], "outputs": []}]}"#;
        assert!(serde_json::from_str::<PetriNet>(json).is_err());
        let json = r#"{"places": [{}], "transitions": [], "initial_marking": {"5": 1}, "final_marking": {}}"#;
        let err = serde_json::from_str::<AcceptingPetriNet>(json).unwrap_err();
        assert!(
            err.to_string().contains("place 5, which is not in the net"),
            "{err}"
        );
    }

    #[test]
    fn zero_tokens_are_dropped_on_load() {
        let m: Marking = serde_json::from_str(r#"{"0": 0, "1": 2}"#).unwrap();
        assert_eq!(m, Marking::from([(PlaceID(1), 2)]));
    }

    #[test]
    fn legacy_json_loads_as_petri_net() {
        let net: PetriNet = serde_json::from_str(SAMPLE_JSON_NET).unwrap();
        assert_eq!((net.num_places(), net.num_transitions()), (4, 8));
        // The smallest UUID gets index 0
        let (first, t) = net.transitions().next().unwrap();
        assert_eq!(
            t.name.as_deref(),
            Some("0c768c77-6408-4f4f-88b8-13d9cc8fca20")
        );
        assert_eq!(net.postset_of_transition(first).len(), 2);
    }

    #[test]
    fn legacy_json_needs_markings_as_accepting_net() {
        let err = serde_json::from_str::<AcceptingPetriNet>(SAMPLE_JSON_NET).unwrap_err();
        assert!(err.to_string().contains("initial marking"), "{err}");
    }

    #[test]
    fn legacy_json_loads_as_accepting_net() {
        let json = legacy(|v| {
            v["initial_marking"] = json!({ REGISTER_INPUT: 1 });
            v["final_markings"] = json!([{ INFORM_OUTPUT: 1 }]);
        });
        let accepting: AcceptingPetriNet = serde_json::from_str(&json).unwrap();
        let net = &accepting.net;
        assert_eq!(
            accepting.initial_marking,
            Marking::from([(place_named(net, REGISTER_INPUT), 1)])
        );
        assert_eq!(
            accepting.final_marking,
            Marking::from([(place_named(net, INFORM_OUTPUT), 1)])
        );
    }

    #[test]
    fn legacy_json_with_two_final_markings_fails() {
        let json = legacy(|v| {
            v["initial_marking"] = json!({ REGISTER_INPUT: 1 });
            v["final_markings"] = json!([{ INFORM_OUTPUT: 1 }, { REGISTER_INPUT: 1 }]);
        });
        let err = serde_json::from_str::<AcceptingPetriNet>(&json).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("final marking") && message.contains('2'),
            "{message}"
        );
    }

    #[test]
    fn legacy_parallel_arcs_merge() {
        let json = legacy(|v| {
            let first = v["arcs"][0].clone();
            v["arcs"].as_array_mut().unwrap().push(first);
        });
        let net: PetriNet = serde_json::from_str(&json).unwrap();
        let archive = net
            .transitions()
            .find(|(_, t)| t.label.as_deref() == Some("Archive Repair"))
            .unwrap()
            .1;
        assert_eq!(archive.outputs().len(), 1);
        assert_eq!(archive.outputs()[0].1, 2);
    }

    #[test]
    fn legacy_arc_to_unknown_node_fails() {
        let json = legacy(|v| {
            v["arcs"][0]["from_to"]["nodes"][1] = json!("00000000-0000-0000-0000-000000000000");
        });
        assert!(serde_json::from_str::<PetriNet>(&json).is_err());
    }

    #[test]
    fn schema_describes_new_shape() {
        let schema = serde_json::to_value(schemars::schema_for!(AcceptingPetriNet)).unwrap();
        let properties = &schema["properties"];
        for key in ["places", "transitions", "initial_marking", "final_marking"] {
            assert!(properties.get(key).is_some(), "{key} missing in {schema}");
        }
        assert_eq!(properties["places"]["type"], "array");
    }
}
