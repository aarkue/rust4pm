//! Writing Petri nets as PNML
use std::{collections::HashSet, io::Write};

use quick_xml::{events::BytesText, Writer};
use uuid::Uuid;

use crate::core::{
    io::xml_chars,
    process_models::case_centric::petri_net::{
        io::PetriNetIOError,
        marking::Marking,
        net::{ArcType, PetriNet},
    },
};

const OK: Result<(), std::io::Error> = Ok(());
const NET_ID: &str = "net1";
const PAGE_ID: &str = "page1";

/// Whether `id` is an XML `NCName`, which PNML requires of ids
fn is_ncname(id: &str) -> bool {
    let mut chars = id.chars();
    chars.next().is_some_and(|c| c.is_alphabetic() || c == '_')
        && chars.all(|c| c.is_alphanumeric() || matches!(c, '.' | '-' | '_'))
}

/// The ids of the places, the transitions and the arcs
///
/// A node keeps its name as its id if the name is a valid `NCName` not taken by an earlier node.
/// Every other node gets a generated id (`p0`, `t0`, ...) that no name or other id uses.
fn ids(net: &PetriNet) -> (Vec<String>, Vec<String>, Vec<String>) {
    let arcs: Vec<String> = (0..net.arcs().count()).map(|i| format!("a{i}")).collect();
    let mut taken: HashSet<String> = [NET_ID, PAGE_ID].map(String::from).into();
    taken.extend(arcs.iter().cloned());
    let names: Vec<Option<&str>> = net
        .places()
        .map(|(_, p)| p.name.as_deref())
        .chain(net.transitions().map(|(_, t)| t.name.as_deref()))
        .collect();
    let kept: Vec<Option<String>> = names
        .iter()
        .map(|name| {
            name.filter(|n| is_ncname(n) && taken.insert(n.to_string()))
                .map(String::from)
        })
        .collect();
    let mut generate = |prefix: char, i: usize| {
        let mut id = format!("{prefix}{i}");
        while !taken.insert(id.clone()) {
            id.push('_');
        }
        id
    };
    let mut ids: Vec<String> = Vec::with_capacity(kept.len());
    for (i, id) in kept.into_iter().enumerate() {
        let id = match id {
            Some(id) => id,
            None if i < net.num_places() => generate('p', i),
            None => generate('t', i - net.num_places()),
        };
        ids.push(id);
    }
    let transitions = ids.split_off(net.num_places());
    (ids, transitions, arcs)
}

pub(crate) fn check_markings(
    net: &PetriNet,
    initial_marking: Option<&Marking>,
    final_marking: Option<&Marking>,
) -> Result<(), PetriNetIOError> {
    match [initial_marking, final_marking]
        .into_iter()
        .flatten()
        .find_map(|marking| net.place_outside(marking))
    {
        Some(place) => Err(PetriNetIOError::InvalidPlaceInMarking(place)),
        None => Ok(()),
    }
}

/// Export a [`PetriNet`] as indented PNML to `writer`, and flush it
///
/// Writes `<initialMarking>`s if `initial_marking` is given, and `ProM`'s `<finalmarkings>` with one
/// marking if `final_marking` is given. A marking with a place outside the net is an error, and
/// nothing is written then. A node's id is its name when that is a valid, unique PNML id, and
/// generated (`p0`, `t0`, ...) otherwise. The output only depends on the net.
pub fn export_pnml<W: Write>(
    mut writer: W,
    net: &PetriNet,
    initial_marking: Option<&Marking>,
    final_marking: Option<&Marking>,
) -> Result<(), PetriNetIOError> {
    check_markings(net, initial_marking, final_marking)?;
    write_pnml(
        &mut Writer::new_with_indent(&mut writer, b' ', 4),
        net,
        initial_marking,
        final_marking,
    )?;
    Ok(writer.flush()?)
}

fn write_pnml<W: Write>(
    writer: &mut Writer<W>,
    net: &PetriNet,
    initial_marking: Option<&Marking>,
    final_marking: Option<&Marking>,
) -> Result<(), std::io::Error> {
    let (place_ids, transition_ids, arc_ids) = ids(net);
    let text = |writer: &mut Writer<W>, element: &str, text: &str| {
        writer
            .create_element(element)
            .write_inner_content(|writer| {
                writer
                    .create_element("text")
                    .write_text_content(BytesText::new(&xml_chars(text)))?;
                OK
            })?;
        OK
    };
    writer
        .create_element("pnml")
        .with_attribute(("xmlns", "http://www.pnml.org/version-2009/grammar/pnml"))
        .write_inner_content(|writer| {
            writer
                .create_element("net")
                .with_attributes([
                    ("id", NET_ID),
                    ("type", "http://www.pnml.org/version-2009/grammar/ptnet"),
                ])
                .write_inner_content(|writer| {
                    writer
                        .create_element("page")
                        .with_attribute(("id", PAGE_ID))
                        .write_inner_content(|writer| {
                            for ((id, place), pnml_id) in net.places().zip(&place_ids) {
                                writer
                                    .create_element("place")
                                    .with_attribute(("id", pnml_id.as_str()))
                                    .write_inner_content(|writer| {
                                        text(
                                            writer,
                                            "name",
                                            place.name.as_deref().unwrap_or(pnml_id),
                                        )?;
                                        // An empty initial marking is written as 0 tokens
                                        // everywhere, so it reads back as present
                                        if let Some(marking) = initial_marking {
                                            let tokens = marking.tokens(id);
                                            if tokens > 0 || marking.is_empty() {
                                                text(
                                                    writer,
                                                    "initialMarking",
                                                    &tokens.to_string(),
                                                )?;
                                            }
                                        }
                                        OK
                                    })?;
                            }
                            for ((id, transition), pnml_id) in
                                net.transitions().zip(&transition_ids)
                            {
                                writer
                                    .create_element("transition")
                                    .with_attribute(("id", pnml_id.as_str()))
                                    .write_inner_content(|writer| {
                                        let name = transition
                                            .label
                                            .as_deref()
                                            .or(transition.name.as_deref())
                                            .unwrap_or("tau");
                                        text(writer, "name", name)?;
                                        if transition.label.is_none() {
                                            // ProM reads `localNodeID` as a UUID
                                            let local_id =
                                                Uuid::from_u128(id.index() as u128).to_string();
                                            writer
                                                .create_element("toolspecific")
                                                .with_attributes([
                                                    ("tool", "ProM"),
                                                    ("version", "6.4"),
                                                    ("activity", "$invisible$"),
                                                    ("localNodeID", local_id.as_str()),
                                                ])
                                                .write_empty()?;
                                        }
                                        OK
                                    })?;
                            }
                            for (arc, arc_id) in net.arcs().zip(&arc_ids) {
                                let (source, target) = match arc.from_to {
                                    ArcType::PlaceTransition(p, t) => {
                                        (&place_ids[p.index()], &transition_ids[t.index()])
                                    }
                                    ArcType::TransitionPlace(t, p) => {
                                        (&transition_ids[t.index()], &place_ids[p.index()])
                                    }
                                };
                                writer
                                    .create_element("arc")
                                    .with_attributes([
                                        ("id", arc_id.as_str()),
                                        ("source", source.as_str()),
                                        ("target", target.as_str()),
                                    ])
                                    .write_inner_content(|writer| {
                                        text(writer, "inscription", &arc.weight.to_string())
                                    })?;
                            }
                            OK
                        })?;
                    if let Some(final_marking) = final_marking {
                        writer
                            .create_element("finalmarkings")
                            .write_inner_content(|writer| {
                                writer
                                    .create_element("marking")
                                    .write_inner_content(|writer| {
                                        for (place, tokens) in final_marking.iter() {
                                            writer
                                                .create_element("place")
                                                .with_attribute((
                                                    "idref",
                                                    place_ids[place.index()].as_str(),
                                                ))
                                                .write_inner_content(|writer| {
                                                    writer
                                                        .create_element("text")
                                                        .write_text_content(BytesText::new(
                                                            &tokens.to_string(),
                                                        ))?;
                                                    OK
                                                })?;
                                        }
                                        OK
                                    })?;
                                OK
                            })?;
                    }
                    OK
                })?;
            OK
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{export_pnml, is_ncname};
    use crate::{
        core::io::Exportable,
        core::process_models::case_centric::petri_net::{
            io::PetriNetIOError,
            marking::Marking,
            net::{AcceptingPetriNet, ArcType, PetriNet, PlaceID},
            pnml::read::{accepting_net, import_pnml},
        },
        test_utils::get_test_data_path,
    };

    fn import(name: &str) -> (PetriNet, Option<Marking>, Option<Marking>) {
        let path = get_test_data_path().join("petri-net").join(name);
        import_pnml(std::io::BufReader::new(std::fs::File::open(path).unwrap())).unwrap()
    }

    fn export(
        net: &PetriNet,
        initial: Option<&Marking>,
        final_marking: Option<&Marking>,
    ) -> String {
        let mut out = Vec::new();
        export_pnml(&mut out, net, initial, final_marking).unwrap();
        String::from_utf8(out).unwrap()
    }

    fn ids(pnml: &str) -> Vec<&str> {
        pnml.split(" id=\"")
            .skip(1)
            .map(|rest| &rest[..rest.find('"').unwrap()])
            .collect()
    }

    #[test]
    fn round_trip_keeps_names_and_markings() {
        let (net, initial, final_marking) = import("sepsis-DISCovered.apnml");
        assert_eq!(net.places().next().unwrap().1.name.as_deref(), Some("i"));
        let pnml = export(&net, initial.as_ref(), final_marking.as_ref());
        // `i` and the transition ids are kept, names such as `pi_[0, 1]@1` get generated ids
        for id in ["i", "o", "n20", "p2"] {
            assert!(pnml.contains(&format!("id=\"{id}\"")), "{id}");
        }
        assert!(pnml.contains(r#"type="http://www.pnml.org/version-2009/grammar/ptnet""#));
        assert!(pnml.starts_with(r#"<pnml xmlns="http://www.pnml.org/version-2009/grammar/pnml">"#));
        assert!(pnml.contains("\n    <net "));
        let back = import_pnml(pnml.as_bytes()).unwrap();
        assert_eq!(back, (net, initial, final_marking));
    }

    #[test]
    fn invalid_xml_chars_are_dropped() {
        let mut net = PetriNet::new();
        net.add_place(Some("p\u{1}".into()));
        net.add_transition(Some("a\u{1}b\u{FFFE}".into()), None);
        let pnml = export(&net, None, None);
        crate::core::io::assert_xml_well_formed(pnml.as_bytes());
        let (back, _, _) = import_pnml(pnml.as_bytes()).unwrap();
        let labels: Vec<_> = back.transitions().map(|(_, t)| t.label.clone()).collect();
        assert_eq!(labels, [Some("ab".to_string())]);
    }

    #[test]
    fn round_trip_without_ncname_ids() {
        let (net, initial, final_marking) =
            import("BPI_Challenge_2019_sampled_3000cases_model_alphappp.pnml");
        let pnml = export(&net, initial.as_ref(), final_marking.as_ref());
        let ids = ids(&pnml);
        assert!(ids.iter().all(|id| is_ncname(id)), "{ids:?}");
        assert_eq!(ids.iter().collect::<HashSet<_>>().len(), ids.len());
        let (back, back_initial, back_final) = import_pnml(pnml.as_bytes()).unwrap();
        let labels = |net: &PetriNet| {
            net.transitions()
                .map(|(_, t)| t.label.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(labels(&back), labels(&net));
        assert_eq!(
            back.arcs().collect::<Vec<_>>(),
            net.arcs().collect::<Vec<_>>()
        );
        assert_eq!((back_initial, back_final), (initial, final_marking));
    }

    #[test]
    fn export_generates_ids_for_invalid_names() {
        let (net, initial, final_marking) = import("pn.pnml");
        let pnml = export(&net, initial.as_ref(), final_marking.as_ref());
        let ids = ids(&pnml);
        assert!(ids.contains(&"p0") && ids.contains(&"t2"), "{ids:?}");
        assert!(ids.iter().all(|id| is_ncname(id)), "{ids:?}");
        assert_eq!(ids.iter().collect::<HashSet<_>>().len(), ids.len());
    }

    #[test]
    fn export_generates_ids_on_collision() {
        let mut net = PetriNet::new();
        // `a0` is an arc id, `p1` is taken by the third place's name
        let p = net.add_place(Some("a0".into()));
        net.add_place(Some("bad name".into()));
        net.add_place(Some("p1".into()));
        let t = net.add_transition(Some("act".into()), Some("y".into()));
        net.add_arc(ArcType::place_to_transition(p, t), None);
        let pnml = export(&net, None, None);
        let ids = ids(&pnml);
        for id in ["p0", "p1_", "p1", "y"] {
            assert!(ids.contains(&id), "{ids:?}");
        }
        assert_eq!(ids.iter().collect::<HashSet<_>>().len(), ids.len());
    }

    #[test]
    fn empty_initial_marking_round_trips() {
        let mut net = PetriNet::new();
        let p = net.add_place(Some("p".into()));
        let t = net.add_transition(Some("a".into()), Some("t".into()));
        net.add_arc(ArcType::transition_to_place(t, p), None);
        let final_marking = Marking::from([(p, 1)]);
        let pnml = export(&net, Some(&Marking::new()), Some(&final_marking));
        let back = import_pnml(pnml.as_bytes()).unwrap();
        assert_eq!(
            back,
            (net.clone(), Some(Marking::new()), Some(final_marking))
        );
        assert!(accepting_net(back).is_ok());

        let pnml = export(&net, None, None);
        let (_, initial, _) = import_pnml(pnml.as_bytes()).unwrap();
        assert_eq!(initial, None);

        let empty = PetriNet::new();
        let pnml = export(&empty, Some(&Marking::new()), Some(&Marking::new()));
        let back = accepting_net(import_pnml(pnml.as_bytes()).unwrap()).unwrap();
        assert!(back.initial_marking.is_empty() && back.final_marking.is_empty());
    }

    #[test]
    fn export_is_deterministic() {
        let (net, initial, final_marking) = import("pn.pnml");
        assert_eq!(
            export(&net, initial.as_ref(), final_marking.as_ref()),
            export(&net, initial.as_ref(), final_marking.as_ref())
        );
    }

    #[test]
    fn markings_with_places_outside_the_net_are_an_error() {
        let (net, _, _) = import("sepsis-DISCovered.apnml");
        let foreign = Marking::from([(PlaceID(999), 1)]);
        let rejected = |result| {
            matches!(
                result,
                Err(PetriNetIOError::InvalidPlaceInMarking(PlaceID(999)))
            )
        };
        let mut out = Vec::new();
        assert!(rejected(export_pnml(&mut out, &net, Some(&foreign), None)));
        assert!(rejected(export_pnml(&mut out, &net, None, Some(&foreign))));
        assert!(out.is_empty(), "nothing is written for a rejected net");

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("foreign-marking.pnml");
        let accepting = AcceptingPetriNet::new(net, Marking::new(), foreign);
        assert!(rejected(accepting.export_to_path(&path)));
        assert!(!path.exists(), "no file is written for a rejected net");
    }

    #[test]
    fn plain_net_writes_no_markings() {
        let (net, _, _) = import("sepsis-DISCovered.apnml");
        let pnml = export(&net, None, None);
        assert!(!pnml.contains("initialMarking") && !pnml.contains("finalmarkings"));
    }

    /// Markings and arcs still resolve by PNML id when it differs from the name.
    #[test]
    fn place_names_round_trip() {
        let mut net = PetriNet::new();
        let named = net.add_place(Some("order placed".into()));
        let unnamed = net.add_place(None);
        let t = net.add_transition(Some("a".into()), Some("t".into()));
        net.add_arc(ArcType::place_to_transition(named, t), None);
        net.add_arc(ArcType::transition_to_place(t, unnamed), None);
        let initial = Marking::from([(named, 1)]);
        let final_marking = Marking::from([(unnamed, 1)]);
        let pnml = export(&net, Some(&initial), Some(&final_marking));
        let (back, back_initial, back_final) = import_pnml(pnml.as_bytes()).unwrap();
        let names: Vec<_> = back.places().map(|(_, p)| p.name.clone()).collect();
        // A place without a name is written with its generated id as its name.
        assert_eq!(
            names,
            [Some("order placed".to_string()), Some("p1".to_string())]
        );
        assert_eq!(
            back.arcs().collect::<Vec<_>>(),
            net.arcs().collect::<Vec<_>>()
        );
        assert_eq!(
            (back_initial, back_final),
            (Some(initial), Some(final_marking))
        );
    }
}
