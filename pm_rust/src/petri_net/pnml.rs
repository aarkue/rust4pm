use std::fs::File;

use quick_xml::{events::BytesText, Writer};
use uuid::Uuid;

use super::petri_net_struct::PetriNet;

pub fn export_petri_net_to_pnml(pn: &PetriNet, path: &str) {
    let file = File::create(path).unwrap();
    // let mut writer = Writer::new_with_indent(Cursor::new(Vec::new()), b' ', 4);
    let mut writer = Writer::new_with_indent(file, b' ', 4);
    writer
        .create_element("pnml")
        .write_inner_content(|writer| {
            writer
                .create_element("net")
                .with_attributes(
                    vec![
                        ("id", "Rust PetriNet Export"),
                        (
                            "type",
                            "http://www.pnml.org/version-2009/grammar/pnmlcoremodel",
                        ),
                    ]
                    .into_iter(),
                )
                .write_inner_content(|writer| {
                    writer
                        .create_element("page")
                        .with_attribute(("id", "n0"))
                        .write_inner_content(|writer| {
                            pn.places.iter().for_each(|(id, _place)| {
                                writer
                                    .create_element("place")
                                    .with_attribute(("id", id.to_string().as_str()))
                                    .write_inner_content(|writer| {
                                        writer
                                            .create_element("name")
                                            .write_inner_content(|writer| {
                                                writer
                                                    .create_element("text")
                                                    .write_text_content(BytesText::new(
                                                        id.to_string().as_str(),
                                                    ))
                                                    .unwrap();
                                                Ok(())
                                            })
                                            .unwrap();
                                        Ok(())
                                    })
                                    .unwrap();
                            });
                            pn.transitions.iter().for_each(|(id, transition)| {
                                writer
                                    .create_element("transition")
                                    .with_attribute(("id", id.to_string().as_str()))
                                    .write_inner_content(|writer| {
                                        writer
                                            .create_element("name")
                                            .write_inner_content(|writer| {
                                                writer
                                                    .create_element("text")
                                                    .write_text_content(BytesText::new(
                                                        transition
                                                            .label
                                                            .clone()
                                                            .unwrap_or("Tau".to_string())
                                                            .as_str(),
                                                    ))
                                                    .unwrap();
                                                Ok(())
                                            })
                                            .unwrap();
                                        if transition.label.is_none() {
                                            // TODO: Add  something like <toolspecific tool="ProM" version="6.4" activity="$invisible$" localNodeID="..."/>
                                            writer
                                                .create_element("toolspecific")
                                                .with_attributes(
                                                    vec![
                                                        ("tool", "ProM"),
                                                        ("version", "6.4"),
                                                        ("activity", "$invisible$"),
                                                        (
                                                            "localNodeID",
                                                            Uuid::new_v4().to_string().as_str(),
                                                        ),
                                                    ]
                                                    .into_iter(),
                                                )
                                                .write_empty()
                                                .unwrap();
                                        }
                                        Ok(())
                                    })
                                    .unwrap();
                            });
                            pn.arcs.iter().for_each(|arc| {
                                let (source_id, target_id) = match arc.from_to {
                                    super::petri_net_struct::ArcType::PlaceTransition(from, to) => {
                                        (from, to)
                                    }
                                    super::petri_net_struct::ArcType::TransitionPlace(from, to) => {
                                        (from, to)
                                    }
                                };
                                writer
                                    .create_element("arc")
                                    .with_attribute((
                                        "id",
                                        (source_id.to_string() + target_id.to_string().as_str())
                                            .as_str(),
                                    ))
                                    .with_attribute(("source", source_id.to_string().as_str()))
                                    .with_attribute(("target", target_id.to_string().as_str()))
                                    .write_empty()
                                    .unwrap();
                            });
                            Ok(())
                        })
                        .unwrap();
                    Ok(())
                })
                .unwrap();
            Ok(())
        })
        .unwrap();
    // String::from_utf8(writer.into_inner().into_inner()).unwrap()
}
