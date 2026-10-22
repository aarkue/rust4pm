//! Writing Petri nets as PNML
use std::{collections::HashSet, fs::File, io::Write};

use quick_xml::{events::BytesText, Writer};
use uuid::Uuid;

use crate::{
    core::process_models::case_centric::petri_net::{
        io::PetriNetIOError,
        marking::Marking,
        net::{ArcType, PetriNet},
    },
    XMLWriterWrapper,
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
/// Nodes keep their names as ids if every node has one and the document's ids stay unique, valid
/// `NCNames`. Otherwise all nodes get generated ids, as mixing both could collide.
fn ids(net: &PetriNet) -> (Vec<String>, Vec<String>, Vec<String>) {
    let arcs: Vec<String> = (0..net.arcs().count()).map(|i| format!("a{i}")).collect();
    let names: Option<Vec<String>> = net
        .places()
        .map(|(_, p)| p.name.clone())
        .chain(net.transitions().map(|(_, t)| t.name.clone()))
        .collect();
    if let Some(mut names) = names {
        let mut seen: HashSet<&str> = HashSet::from([NET_ID, PAGE_ID]);
        seen.extend(arcs.iter().map(String::as_str));
        if names
            .iter()
            .all(|name| is_ncname(name) && seen.insert(name.as_str()))
        {
            let transitions = names.split_off(net.num_places());
            return (names, transitions, arcs);
        }
    }
    let places = (0..net.num_places()).map(|i| format!("p{i}")).collect();
    let transitions = (0..net.num_transitions())
        .map(|i| format!("t{i}"))
        .collect();
    (places, transitions, arcs)
}

fn check_markings(
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

///
/// Export a [`PetriNet`] to PNML and write it to the given writer ([`quick_xml::Writer`] or [`std::io::Write`])
///
/// Writes `<initialMarking>`s if `initial_marking` is given, and `ProM`'s `<finalmarkings>` with one
/// marking if `final_marking` is given; a marking with a place outside the net is an error. Node
/// ids are the nodes' names when those form valid, unique PNML ids, and generated (`p0`, `t0`, ...)
/// otherwise. The output only depends on the net.
pub fn export_petri_net_to_pnml<'a, W>(
    net: &PetriNet,
    initial_marking: Option<&Marking>,
    final_marking: Option<&Marking>,
    into_writer: impl Into<XMLWriterWrapper<'a, W>>,
) -> Result<(), PetriNetIOError>
where
    W: Write + 'a,
{
    check_markings(net, initial_marking, final_marking)?;
    Ok(write_pnml(
        net,
        initial_marking,
        final_marking,
        into_writer,
    )?)
}

fn write_pnml<'a, W>(
    net: &PetriNet,
    initial_marking: Option<&Marking>,
    final_marking: Option<&Marking>,
    into_writer: impl Into<XMLWriterWrapper<'a, W>>,
) -> Result<(), quick_xml::Error>
where
    W: Write + 'a,
{
    let (place_ids, transition_ids, arc_ids) = ids(net);
    let text = |writer: &mut Writer<W>, element: &str, text: &str| {
        writer
            .create_element(element)
            .write_inner_content(|writer| {
                writer
                    .create_element("text")
                    .write_text_content(BytesText::new(text))?;
                OK
            })?;
        OK
    };
    let mut xml_writer: XMLWriterWrapper<'_, W> = into_writer.into();
    let writer = xml_writer.to_xml_writer();
    writer
        .create_element("pnml")
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
                                        let tokens = initial_marking.map_or(0, |m| m.tokens(id));
                                        if tokens > 0 {
                                            text(writer, "initialMarking", &tokens.to_string())?;
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

/// Export a [`PetriNet`] to a `.pnml` file (specified through path)
///
/// See [`export_petri_net_to_pnml`] for what is written. Nothing is written if the markings refer
/// to places outside the net.
pub fn export_petri_net_to_pnml_path<P: AsRef<std::path::Path>>(
    net: &PetriNet,
    initial_marking: Option<&Marking>,
    final_marking: Option<&Marking>,
    path: P,
) -> Result<(), PetriNetIOError> {
    check_markings(net, initial_marking, final_marking)?;
    let mut writer = Writer::new_with_indent(File::create(path)?, b' ', 4);
    Ok(write_pnml(
        net,
        initial_marking,
        final_marking,
        &mut writer,
    )?)
}
#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{export_petri_net_to_pnml, export_petri_net_to_pnml_path, is_ncname};
    use crate::{
        core::process_models::case_centric::petri_net::{
            io::PetriNetIOError,
            marking::Marking,
            net::{ArcType, PetriNet, PlaceID},
            pnml::read::{import_pnml_from_path, import_pnml_reader},
        },
        test_utils::get_test_data_path,
    };

    fn import(name: &str) -> (PetriNet, Option<Marking>, Option<Marking>) {
        import_pnml_from_path(get_test_data_path().join("petri-net").join(name)).unwrap()
    }

    fn export(
        net: &PetriNet,
        initial: Option<&Marking>,
        final_marking: Option<&Marking>,
    ) -> String {
        let mut out = Vec::new();
        export_petri_net_to_pnml(net, initial, final_marking, &mut out).unwrap();
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
        let pnml = export(&net, initial.as_ref(), final_marking.as_ref());
        assert!(pnml.contains("id=\"n1\""));
        assert!(pnml.contains(r#"type="http://www.pnml.org/version-2009/grammar/ptnet""#));
        let back = import_pnml_reader(&mut pnml.as_bytes()).unwrap();
        assert_eq!(back, (net, initial, final_marking));
    }

    #[test]
    fn round_trip_without_ncname_ids() {
        let (net, initial, final_marking) =
            import("BPI_Challenge_2019_sampled_3000cases_model_alphappp.pnml");
        let pnml = export(&net, initial.as_ref(), final_marking.as_ref());
        let ids = ids(&pnml);
        assert!(ids.iter().all(|id| is_ncname(id)), "{ids:?}");
        assert_eq!(ids.iter().collect::<HashSet<_>>().len(), ids.len());
        let (back, back_initial, back_final) = import_pnml_reader(&mut pnml.as_bytes()).unwrap();
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
        assert!(ids.contains(&"p0") && ids.contains(&"t0"), "{ids:?}");
        assert!(ids.iter().all(|id| uuid::Uuid::parse_str(id).is_err()));
    }

    #[test]
    fn export_generates_ids_on_collision() {
        let mut net = PetriNet::new();
        let p = net.add_place(Some("a0".into()));
        net.add_place(Some("x".into()));
        let t = net.add_transition(Some("act".into()), Some("y".into()));
        net.add_arc(ArcType::place_to_transition(p, t), None);
        let pnml = export(&net, None, None);
        let ids = ids(&pnml);
        assert!(
            ids.contains(&"p0") && ids.contains(&"p1") && ids.contains(&"t0"),
            "{ids:?}"
        );
        assert!(!ids.contains(&"x"));
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
        assert!(rejected(export_petri_net_to_pnml(
            &net,
            Some(&foreign),
            None,
            &mut out
        )));
        assert!(rejected(export_petri_net_to_pnml(
            &net,
            None,
            Some(&foreign),
            &mut out
        )));
        let path = get_test_data_path()
            .join("export")
            .join("foreign-marking.pnml");
        let _ = std::fs::remove_file(&path);
        assert!(rejected(export_petri_net_to_pnml_path(
            &net,
            None,
            Some(&foreign),
            &path
        )));
        assert!(!path.exists(), "no file is written for a rejected net");
    }

    #[test]
    fn plain_net_writes_no_markings() {
        let (net, _, _) = import("sepsis-DISCovered.apnml");
        let pnml = export(&net, None, None);
        assert!(!pnml.contains("initialMarking") && !pnml.contains("finalmarkings"));
    }
}
