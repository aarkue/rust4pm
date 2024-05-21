use std::{collections::HashMap, io::BufRead};

use quick_xml::Reader;
use uuid::Uuid;

use crate::PetriNet;

use super::petri_net_struct::{ArcType, Marking, PlaceID};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    None,
    Pnml,
    Net,
    Place,
    Transition,
    PlaceName,
    TransitionName,
    InitialMarking,
    FinalMarkings,
    FinalMarkingsMarking,
    FinalMarkingMarkingPlace,
    Arc,
    ArcInscription,
}

fn read_to_string(x: &mut &[u8]) -> String {
    String::from_utf8_lossy(x).to_string()
}

///
/// Import a PNML file from the given reader
/// 
/// Note, that this implementation (at least currently) is a best-effort parser for the basic types of Petri nets encountered commonly in Process Mining.
/// In particular, the following Petri net features are implemented:
///
/// - Places, Transitions
/// - Silent transitions (toolspecific tag)
/// - Basics arcs (i.e., not inhibitor arcs, etc.)
/// - Arc weights
/// - A single initial marking
/// - Multiple final markings
///
pub fn import_pnml<T>(reader: &mut Reader<T>) -> Result<PetriNet, quick_xml::Error>
where
    T: BufRead,
{
    reader.trim_text(true);
    reader.expand_empty_elements(true);
    let mut buf: Vec<u8> = Vec::new();

    let mut current_mode: Mode = Mode::None;

    let mut pn = PetriNet::new();
    let mut initial_marking: Marking = HashMap::new();
    let mut final_markings: Vec<Marking> = vec![];

    let mut id_map: HashMap<String, Uuid> = HashMap::new();

    let mut current_id: Option<Uuid> = None;

    let mut arcs: Vec<(String, String, u32)> = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            quick_xml::events::Event::Start(b) => match b.name().as_ref() {
                b"pnml" => {
                    current_mode = Mode::Pnml;
                }
                b"net" => {
                    if current_mode != Mode::Pnml {
                        eprintln!("Expected to be in Mode::PNML when encountering net");
                    }
                    current_mode = Mode::Net;
                }
                b"page" => {
                    if current_mode != Mode::Net {
                        eprintln!("Expected to be in Mode::Net when encountering page");
                    }
                    current_mode = Mode::Net;
                }
                b"place" => {
                    if current_mode == Mode::FinalMarkingsMarking {
                        // Final Marking place
                        let id_ref = read_to_string(
                            &mut b
                                .try_get_attribute("idref")
                                .unwrap_or_default()
                                .unwrap()
                                .value
                                .as_ref(),
                        );
                        current_mode = Mode::FinalMarkingMarkingPlace;
                        // Save idref as current_id (and use it when parsing the corresponding <text>1</text>)
                        current_id = id_map.get(&id_ref).cloned();
                    } else {
                        // Add place
                        current_mode = Mode::Place;
                        let place_id = b.try_get_attribute("id").unwrap_or_default().unwrap();
                        let place_id_str = read_to_string(&mut place_id.value.as_ref());
                        let uuid = Uuid::new_v4();
                        current_id = Some(uuid);
                        id_map.insert(place_id_str, uuid);
                        pn.add_place(Some(uuid));
                    }
                }
                b"transition" => {
                    current_mode = Mode::Transition;
                    let trans_id = b.try_get_attribute("id").unwrap_or_default().unwrap();
                    let trans_id_str = read_to_string(&mut trans_id.value.as_ref());
                    let uuid = Uuid::new_v4();
                    current_id = Some(uuid);
                    id_map.insert(trans_id_str, uuid);
                    pn.add_transition(Some(String::new()), Some(uuid));
                }
                b"arc" => {
                    let source_id = read_to_string(
                        &mut b
                            .try_get_attribute("source")
                            .unwrap_or_default()
                            .unwrap()
                            .value
                            .as_ref(),
                    );
                    let target_id = read_to_string(
                        &mut b
                            .try_get_attribute("target")
                            .unwrap_or_default()
                            .unwrap()
                            .value
                            .as_ref(),
                    );
                    // Only add arcs to vec here, and add them to the PetriNet only at the end
                    // bc. we do not know if the source/targets are encountered yet and we need to know which one is the transitions and which one the place
                    arcs.push((source_id, target_id, 1));
                    current_mode = Mode::Arc;
                }
                // Handle weighted arcs
                b"inscription" => {
                    if current_mode == Mode::Arc {
                        current_mode = Mode::ArcInscription;
                    }
                }
                // For handling silent transitions
                b"toolspecific" => {
                    if b.try_get_attribute("activity")
                        .unwrap_or_default()
                        .as_ref()
                        .unwrap()
                        .value
                        .as_ref()
                        == b"$invisible$"
                    {
                        if let Some(trans) = current_id.and_then(|id| pn.transitions.get_mut(&id)) {
                            // Set label to None (silent)
                            trans.label = None;
                        } else {
                            eprintln!("Can't find current transition when adding toolspecific!");
                        }
                    }
                }
                // For handling initial markings
                b"initialMarking" => {
                    current_mode = Mode::InitialMarking;
                }
                b"finalmarkings" => current_mode = Mode::FinalMarkings,
                b"marking" => {
                    if current_mode == Mode::FinalMarkings {
                        current_mode = Mode::FinalMarkingsMarking;
                        // Add new final marking
                        final_markings.push(HashMap::new());
                    }
                }
                b"name" => match current_mode {
                    Mode::Place => current_mode = Mode::PlaceName,
                    Mode::Transition => current_mode = Mode::TransitionName,
                    _ => {}
                },
                _ => {}
            },
            quick_xml::events::Event::End(b) => match b.name().as_ref() {
                b"place" => {
                    if current_mode == Mode::FinalMarkingMarkingPlace {
                        current_mode = Mode::FinalMarkingsMarking;
                        current_id = None;
                    } else {
                        current_mode = Mode::Net;
                        current_id = None;
                    }
                }
                b"transition" => {
                    current_mode = Mode::Net;
                    current_id = None;
                }
                b"initialMarking" => {
                    current_mode = Mode::Place;
                }
                b"finalmarkings" => current_mode = Mode::Net,
                b"marking" => current_mode = Mode::FinalMarkings,
                b"inscription" => {
                    if current_mode == Mode::ArcInscription {
                        current_mode = Mode::Arc
                    }
                }
                b"arc" => {
                    current_mode = Mode::Net;
                }
                b"name" => match current_mode {
                    Mode::PlaceName => current_mode = Mode::Place,
                    Mode::TransitionName => current_mode = Mode::Transition,
                    _ => {}
                },
                _ => {}
            },
            quick_xml::events::Event::Text(t) => {
                let text = read_to_string(&mut t.as_ref());
                match current_mode {
                    Mode::TransitionName => {
                        if let Some(trans) = current_id.and_then(|id| pn.transitions.get_mut(&id)) {
                            // Only overwrite label if it is set to Some(...)
                            // Because this is what we do initially
                            // Otherwise, silent transitions might get labeled by accident
                            if trans.label.is_some() {
                                trans.label = Some(text);
                            }
                        } else {
                            eprintln!("Can't find current transition when adding text!");
                        }
                    }
                    Mode::InitialMarking => {
                        if let Some(place) = current_id.and_then(|id| pn.places.get(&id)) {
                            initial_marking
                                .insert(place.into(), text.parse::<u64>().unwrap_or_default());
                        }
                    }
                    Mode::FinalMarkingMarkingPlace => {
                        if let Some(place_id) = current_id {
                            if let Some(fm) = final_markings.last_mut() {
                                fm.insert(
                                    PlaceID(place_id),
                                    text.parse::<u64>().unwrap_or_default(),
                                );
                            }
                        }
                    }
                    Mode::ArcInscription => {
                        if let Some(arc) = arcs.last_mut() {
                            arc.2 = text.parse::<u32>().unwrap_or(1);
                        }
                    }
                    _ => {}
                }
            }
            quick_xml::events::Event::Eof => break,
            _ => {}
        }
    }

    for (from, to, weight) in arcs {
        let from_uuid = id_map.get(&from);
        let to_uuid = id_map.get(&to);
        if let Some(from_uuid) = from_uuid {
            if let Some(to_uuid) = to_uuid {
                let mut from_to = None;
                // Option 1: Place -> Transition
                if let Some(place) = pn.places.get(from_uuid) {
                    if let Some(trans) = pn.transitions.get(to_uuid) {
                        from_to = Some(ArcType::place_to_transition(place.into(), trans.into()));
                    }
                // Option 2: Transition -> Place
                } else if let Some(trans) = pn.transitions.get(from_uuid) {
                    if let Some(place) = pn.places.get(to_uuid) {
                        from_to = Some(ArcType::transition_to_place(trans.into(), place.into()));
                    }
                }
                if let Some(from_to) = from_to {
                    pn.add_arc(from_to, Some(weight))
                }
            }
        }
    }
    if !initial_marking.is_empty() {
        pn.initial_marking = Some(initial_marking);
    }
    if !final_markings.is_empty() {
        pn.final_markings = Some(final_markings);
    }
    Ok(pn)
}

#[cfg(test)]
mod test {
    use quick_xml::Reader;

    use super::import_pnml;

    #[test]
    fn test_pnml_import() {
        let pnml_str = include_str!("./test_data/pn.pnml");
        let pn = import_pnml(&mut Reader::from_str(pnml_str)).unwrap();
        assert_eq!(pn.transitions.len(), 46);
        assert_eq!(pn.places.len(), 9);
        assert_eq!(pn.arcs.len(), 24);
        assert!(pn.initial_marking.is_some());
        assert!(pn.final_markings.is_some());
        assert!(pn.arcs.iter().any(|arc| arc.weight == 1337));
        println!("{:#?}", pn);
        #[cfg(feature = "graphviz-export")]
        {
            pn.export_svg("/tmp/pn.svg").unwrap();
            println!("Exported to: file:///tmp/pn.svg");
        }
    }
}
