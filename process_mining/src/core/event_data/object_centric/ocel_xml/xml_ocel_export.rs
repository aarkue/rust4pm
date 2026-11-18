use chrono::DateTime;
use quick_xml::{
    events::{BytesDecl, BytesText, Event},
    Writer,
};
use std::io::Write;

use crate::core::{
    event_data::object_centric::{
        ocel_struct::{OCELAttributeValue, OCELRelationship, OCELTypeAttribute},
        readable::ReadableOCEL,
    },
    io::xml_chars,
};
const OK: Result<(), std::io::Error> = Ok(());

/// Export an OCEL as XML to `writer`
pub(crate) fn export_ocel_xml<O: ReadableOCEL + ?Sized>(
    writer: impl Write,
    ocel: &O,
) -> Result<(), std::io::Error> {
    let mut writer = Writer::new(writer);
    writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;
    writer.create_element("log").write_inner_content(|w| {
        w.create_element("object-types").write_inner_content(|w| {
            for ot in ocel.object_types() {
                w.create_element("object-type")
                    .with_attributes(vec![("name", xml_chars(&ot.name).as_ref())])
                    .write_inner_content(|w| {
                        write_ocel_type_attrs(&ot.attributes, w)?;
                        OK
                    })?;
            }
            OK
        })?;
        w.create_element("event-types").write_inner_content(|w| {
            for et in ocel.event_types() {
                w.create_element("event-type")
                    .with_attributes(vec![("name", xml_chars(&et.name).as_ref())])
                    .write_inner_content(|w| {
                        write_ocel_type_attrs(&et.attributes, w)?;
                        OK
                    })?;
            }
            OK
        })?;
        w.create_element("objects").write_inner_content(|w| {
            for o in ocel.iter_objects() {
                w.create_element("object")
                    .with_attribute(("id", xml_chars(&o.id).as_ref()))
                    .with_attribute(("type", xml_chars(&o.object_type).as_ref()))
                    .write_inner_content(|w| {
                        w.create_element("attributes").write_inner_content(|w| {
                            // A `Null` initial value is left out: an empty element is read back
                            // as `""` for a string attribute. A change to `Null` cannot be left
                            // out without losing it, so it is written as an empty element.
                            for oa in o.attributes.iter().filter(|oa| {
                                !(oa.value == OCELAttributeValue::Null
                                    && oa.time == DateTime::UNIX_EPOCH)
                            }) {
                                w.create_element("attribute")
                                    .with_attribute(("name", xml_chars(&oa.name).as_ref()))
                                    .with_attribute(("time", oa.time.to_rfc3339().as_str()))
                                    .write_inner_content(|w| {
                                        w.write_event(Event::Text(BytesText::new(&xml_chars(
                                            &oa.value.to_string(),
                                        ))))?;
                                        OK
                                    })?;
                            }
                            OK
                        })?;
                        write_ocel_relationships(&o.relationships, w)?;
                        OK
                    })?;
            }
            OK
        })?;
        w.create_element("events").write_inner_content(|w| {
            for e in ocel.iter_events() {
                w.create_element("event")
                    .with_attribute(("id", xml_chars(&e.id).as_ref()))
                    .with_attribute(("type", xml_chars(&e.event_type).as_ref()))
                    .with_attribute(("time", e.time.to_rfc3339().as_str()))
                    .write_inner_content(|w| {
                        w.create_element("attributes").write_inner_content(|w| {
                            // Left out like `Null` initial object values above.
                            for ea in e
                                .attributes
                                .iter()
                                .filter(|ea| ea.value != OCELAttributeValue::Null)
                            {
                                w.create_element("attribute")
                                    .with_attribute(("name", xml_chars(&ea.name).as_ref()))
                                    .write_inner_content(|w| {
                                        w.write_event(Event::Text(BytesText::new(&xml_chars(
                                            &ea.value.to_string(),
                                        ))))?;
                                        OK
                                    })?;
                            }
                            OK
                        })?;
                        write_ocel_relationships(&e.relationships, w)?;
                        OK
                    })?;
            }
            OK
        })?;

        OK
    })?;
    Ok(())
}

fn write_ocel_type_attrs<W: Write>(
    attrs: &[OCELTypeAttribute],
    w: &mut Writer<W>,
) -> Result<(), std::io::Error> {
    w.create_element("attributes").write_inner_content(|w| {
        for at in attrs {
            w.create_element("attribute")
                .with_attributes(vec![
                    ("name", xml_chars(&at.name).as_ref()),
                    ("type", xml_chars(&at.value_type).as_ref()),
                ])
                .write_empty()?;
        }
        OK
    })?;
    OK
}

fn write_ocel_relationships<W: Write>(
    rels: &[OCELRelationship],
    w: &mut Writer<W>,
) -> Result<(), std::io::Error> {
    w.create_element("objects").write_inner_content(|w| {
        for r in rels {
            w.create_element("relationship")
                .with_attribute(("object-id", xml_chars(&r.object_id).as_ref()))
                .with_attribute(("qualifier", xml_chars(&r.qualifier).as_ref()))
                .write_empty()?;
        }
        OK
    })?;
    OK
}

#[cfg(test)]
mod ocel_xml_export_test {
    use std::time::Instant;

    use crate::{test_utils::get_test_data_path, Exportable, Importable, OCEL};

    #[test]
    fn invalid_xml_chars_are_dropped() {
        use crate::core::event_data::object_centric::ocel_struct::{
            OCELAttributeValue, OCELEvent, OCELEventAttribute, OCELObject, OCELObjectAttribute,
            OCELRelationship, OCELType, OCELTypeAttribute,
        };
        use crate::core::event_data::object_centric::ocel_xml::xml_ocel_export::export_ocel_xml;
        let log = |x: &str| {
            let string_type = |name: String| OCELType {
                name,
                attributes: vec![OCELTypeAttribute {
                    name: format!("a{x}"),
                    value_type: "string".into(),
                }],
            };
            OCEL {
                event_types: vec![string_type(format!("et{x}"))],
                object_types: vec![string_type(format!("ot{x}"))],
                events: vec![OCELEvent {
                    id: format!("e{x}"),
                    event_type: format!("et{x}"),
                    time: "2024-01-01T10:00:00+00:00".parse().unwrap(),
                    attributes: vec![OCELEventAttribute {
                        name: format!("a{x}"),
                        value: OCELAttributeValue::String(format!("v{x}")),
                    }],
                    relationships: vec![OCELRelationship {
                        object_id: format!("o{x}"),
                        qualifier: format!("q{x}"),
                    }],
                }],
                objects: vec![OCELObject {
                    id: format!("o{x}"),
                    object_type: format!("ot{x}"),
                    attributes: vec![OCELObjectAttribute {
                        name: format!("a{x}"),
                        value: OCELAttributeValue::String(format!("v{x}")),
                        time: "2024-01-02T10:00:00+00:00".parse().unwrap(),
                    }],
                    relationships: vec![],
                }],
            }
        };
        let mut data = Vec::new();
        export_ocel_xml(&mut data, &log("\u{1}\u{FFFE}")).unwrap();
        crate::core::io::assert_xml_well_formed(&data);
        assert_eq!(OCEL::import_from_bytes(&data, "xml").unwrap(), log(""));
    }

    #[test]
    fn export_round_trip_order_management() {
        let path = get_test_data_path()
            .join("ocel")
            .join("order-management.xml");
        let mut now = Instant::now();
        let ocel = OCEL::import_from_path(&path).unwrap();
        let obj = ocel.objects.first().unwrap();
        println!("{obj:?}");
        println!(
            "Imported OCEL with {} objects and {} events in {:#?}",
            ocel.objects.len(),
            ocel.events.len(),
            now.elapsed()
        );
        assert_eq!(ocel.objects.len(), 10840);
        assert_eq!(ocel.events.len(), 21008);

        assert_eq!(ocel.object_types.len(), 6);
        assert_eq!(ocel.event_types.len(), 11);

        let export_path = get_test_data_path()
            .join("export")
            .join("order-mangement-export.xml");
        now = Instant::now();
        ocel.export_to_path(&export_path).unwrap();
        println!(
            "Exported OCEL with {} objects and {} events in {:#?}",
            ocel.objects.len(),
            ocel.events.len(),
            now.elapsed()
        );
        now = Instant::now();
        let ocel2 = OCEL::import_from_path(&export_path).unwrap();
        println!(
            "Imported OCEL AGAIN with {} objects and {} events in {:#?}",
            ocel.objects.len(),
            ocel.events.len(),
            now.elapsed()
        );

        assert_eq!(ocel2.objects.len(), 10840);
        assert_eq!(ocel2.events.len(), 21008);

        // Do not use assert_eq!(...) because this will flood stdout with full OCEL if not true
        assert!(ocel == ocel2);
    }

    #[test]
    fn export_round_trip_p2p() {
        let path = get_test_data_path().join("ocel").join("ocel2-p2p.xml");
        let mut now = Instant::now();
        let ocel = OCEL::import_from_path(&path).unwrap();
        let obj = ocel.objects.first().unwrap();
        println!("{obj:?}");
        println!(
            "Imported OCEL with {} objects and {} events in {:#?}",
            ocel.objects.len(),
            ocel.events.len(),
            now.elapsed()
        );
        assert_eq!(ocel.objects.len(), 9543);
        assert_eq!(ocel.events.len(), 14671);

        let export_path = get_test_data_path()
            .join("export")
            .join("ocel2-p2p-export.xml");
        now = Instant::now();
        ocel.export_to_path(&export_path).unwrap();
        println!(
            "Exported OCEL with {} objects and {} events in {:#?}",
            ocel.objects.len(),
            ocel.events.len(),
            now.elapsed()
        );
        now = Instant::now();
        let ocel2 = OCEL::import_from_path(&export_path).unwrap();
        println!(
            "Imported OCEL AGAIN with {} objects and {} events in {:#?}",
            ocel.objects.len(),
            ocel.events.len(),
            now.elapsed()
        );
        assert_eq!(ocel2.objects.len(), 9543);
        assert_eq!(ocel2.events.len(), 14671);

        assert_eq!(ocel.event_types, ocel2.event_types);
        assert_eq!(ocel.object_types, ocel2.object_types);
        assert_eq!(ocel.events, ocel2.events);

        // Do not use assert_eq!(...) because this will flood stdout with full OCEL if not true
        assert!(ocel == ocel2);
    }
}
