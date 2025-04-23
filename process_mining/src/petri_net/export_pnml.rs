use super::petri_net_struct::PetriNet;
use crate::utils::xml_utils::XMLWriterWrapper;
use quick_xml::{events::BytesText, Error, Writer};
use std::{fs::File, io::Write};
use uuid::Uuid;

///
/// Export a [`PetriNet`] to the PNML format and write the result to the provided writer which implements into [`quick_xml::Writer`] / [`std::io::Write`]
///
pub fn export_petri_net_to_pnml<'a, W>(
    pn: &PetriNet,
    into_writer: impl Into<XMLWriterWrapper<'a, W>>,
) -> Result<(), Error>
where
    W: Write + 'a,
{
    let mut xml_writer: XMLWriterWrapper<'_, W> = into_writer.into();
    let writer = xml_writer.to_xml_writer();
    writer
        .create_element("pnml")
        .write_inner_content(|writer| {
            writer
                .create_element("net")
                .with_attributes(vec![
                    ("id", "Rust PetriNet Export"),
                    (
                        "type",
                        "http://www.pnml.org/version-2009/grammar/pnmlcoremodel",
                    ),
                ])
                .write_inner_content(|writer| {
                    writer
                        .create_element("page")
                        .with_attribute(("id", "n0"))
                        .write_inner_content(|writer| {
                            pn.places.iter().for_each(|(id, place)| {
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
                                        if let Some(initial_marking) = pn.initial_marking.clone() {
                                            if initial_marking.contains_key(&place.into()) {
                                                let tokens =
                                                    initial_marking.get(&place.into()).unwrap();
                                                writer
                                                    .create_element("initialMarking")
                                                    .write_inner_content(|writer| {
                                                        writer
                                                            .create_element("text")
                                                            .write_text_content(BytesText::new(
                                                                tokens.to_string().as_str(),
                                                            ))
                                                            .unwrap();
                                                        Ok(())
                                                    })
                                                    .unwrap();
                                            }
                                        }

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
                                                .with_attributes(vec![
                                                    ("tool", "ProM"),
                                                    ("version", "6.4"),
                                                    ("activity", "$invisible$"),
                                                    (
                                                        "localNodeID",
                                                        Uuid::new_v4().to_string().as_str(),
                                                    ),
                                                ])
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
                                    .write_inner_content(|w| {
                                        w.create_element("inscription")
                                            .write_inner_content(|w| {
                                                w.create_element("text")
                                                    .write_text_content(BytesText::new(
                                                        arc.weight.to_string().as_str(),
                                                    ))
                                                    .unwrap();
                                                Ok(())
                                            })
                                            .unwrap();
                                        Ok(())
                                    })
                                    .unwrap();
                            });
                            Ok(())
                        })
                        .unwrap();

                    if let Some(final_markings) = pn.final_markings.clone() {
                        writer
                            .create_element("finalmarkings")
                            .write_inner_content(|writer| {
                                final_markings.iter().for_each(|marking| {
                                    writer
                                        .create_element("marking")
                                        .write_inner_content(|writer| {
                                            marking.iter().for_each(|(place_id, tokens)| {
                                                writer
                                                    .create_element("place")
                                                    .with_attribute((
                                                        "idref",
                                                        place_id.get_uuid().to_string().as_str(),
                                                    ))
                                                    .write_inner_content(|writer| {
                                                        writer
                                                            .create_element("text")
                                                            .write_text_content(BytesText::new(
                                                                tokens.to_string().as_str(),
                                                            ))
                                                            .unwrap();
                                                        Ok(())
                                                    })
                                                    .unwrap();
                                            });
                                            Ok(())
                                        })
                                        .unwrap();
                                });
                                Ok(())
                            })
                            .unwrap();
                    }

                    // </net>
                    Ok(())
                })
                .unwrap();
            Ok(())
        })?;
    Ok(())
}

/// Export a [`PetriNet`] to a `.pnml` file (specified through path)
///
/// Also consider using [`PetriNet::export_pnml`] for convenience or [`export_petri_net_to_pnml`] for more control.
pub fn export_petri_net_to_pnml_path<P: AsRef<std::path::Path>>(
    pn: &PetriNet,
    path: P,
) -> Result<(), Error> {
    let file = File::create(path)?;
    let mut writer = Writer::new_with_indent(file, b' ', 4);
    export_petri_net_to_pnml(pn, &mut writer)
}

#[cfg(test)]
mod test {
    use super::export_petri_net_to_pnml_path;
    use crate::{
        import_xes_file, petri_net::export_pnml::export_petri_net_to_pnml,
        utils::test_utils::get_test_data_path, XESImportOptions,
    };
    use std::{fs::File, io::BufWriter};

    #[test]
    fn test_export_pnml() {
        let path = get_test_data_path().join("xes").join("AN1-example.xes");
        let log = import_xes_file(&path, XESImportOptions::default()).unwrap();
        let (_, mut pn) = crate::alphappp::auto_parameters::alphappp_discover_with_auto_parameters(
            &(&log).into(),
        );
        pn.arcs.last_mut().unwrap().weight = 1337;
        let export_path = get_test_data_path().join("export").join("pnml-export.pnml");
        export_petri_net_to_pnml_path(&pn, &export_path).unwrap();
        println!("file:///{}", export_path.to_string_lossy());
    }

    #[test]
    fn test_export_pnml_to_writer() -> Result<(), quick_xml::Error> {
        let path = get_test_data_path().join("xes").join("AN1-example.xes");
        let log = import_xes_file(&path, XESImportOptions::default()).unwrap();
        let (_, mut pn) = crate::alphappp::auto_parameters::alphappp_discover_with_auto_parameters(
            &(&log).into(),
        );
        pn.arcs.last_mut().unwrap().weight = 1337;
        let export_path = get_test_data_path().join("export").join("pnml-export.pnml");
        let file = File::create(&export_path)?;
        let mut writer = BufWriter::new(file);
        export_petri_net_to_pnml(&pn, &mut writer)?;
        println!("file:///{}", export_path.to_string_lossy());
        Ok(())
    }
}
