use std::{
    fs::File,
    io::{BufWriter, Write},
};

use flate2::{write::GzEncoder, Compression};
use quick_xml::{
    events::{BytesDecl, BytesText, Event},
    Writer,
};

use crate::{utils::xml_utils::XMLWriterWrapper, OCEL};

use super::ocel_struct::{OCELRelationship, OCELTypeAttribute};

const OK: Result<(), quick_xml::Error> = Ok::<(), quick_xml::Error>(());

///
/// Export OCEL 2.0 to XML Writer
///
pub fn export_ocel_xml<'a, 'b, W>(
    writer: impl Into<XMLWriterWrapper<'b, W>>,
    ocel: &'a OCEL,
) -> Result<(), quick_xml::Error>
where
    W: Write + 'b,
{
    let mut xml_writer = writer.into();
    let writer: &mut quick_xml::Writer<W> = xml_writer.to_xml_writer();
    writer
        .write_event(quick_xml::events::Event::Decl(BytesDecl::new(
            "1.0",
            Some("UTF-8"),
            None,
        )))
        .unwrap();
    writer.create_element("log").write_inner_content(|w| {
        // Write Object Types
        w.create_element("object-types").write_inner_content(|w| {
            for ot in &ocel.object_types {
                w.create_element("object-type")
                    .with_attributes(vec![("name", ot.name.as_str())])
                    .write_inner_content(|w| {
                        write_ocel_type_attrs(&ot.attributes, w)?;
                        OK
                    })?;
            }
            OK
        })?;
        // Write Event Types
        w.create_element("event-types").write_inner_content(|w| {
            for et in &ocel.event_types {
                w.create_element("event-type")
                    .with_attributes(vec![("name", et.name.as_str())])
                    .write_inner_content(|w| {
                        write_ocel_type_attrs(&et.attributes, w)?;
                        OK
                    })?;
            }
            OK
        })?;
        // Write Objects
        w.create_element("objects").write_inner_content(|w| {
            for o in &ocel.objects {
                w.create_element("object")
                    .with_attribute(("id", o.id.as_str()))
                    .with_attribute(("type", o.object_type.as_str()))
                    .write_inner_content(|w| {
                        // Write Attributes
                        w.create_element("attributes").write_inner_content(|w| {
                            for oa in &o.attributes {
                                w.create_element("attribute")
                                    .with_attribute(("name", oa.name.as_str()))
                                    .with_attribute(("time", oa.time.to_rfc3339().as_str()))
                                    .write_inner_content(|w| {
                                        w.write_event(Event::Text(BytesText::new(
                                            &oa.value.to_string(),
                                        )))?;
                                        OK
                                    })?;
                            }
                            OK
                        })?;
                        // Write Relationships
                        write_ocel_relationships(&o.relationships, w)?;
                        OK
                    })?;
            }
            OK
        })?;

        // Write Events
        w.create_element("events").write_inner_content(|w| {
            for e in &ocel.events {
                w.create_element("event")
                    .with_attribute(("id", e.id.as_str()))
                    .with_attribute(("type", e.event_type.as_str()))
                    .with_attribute(("time", e.time.to_rfc3339().as_str()))
                    .write_inner_content(|w| {
                        // Write Attributes
                        w.create_element("attributes").write_inner_content(|w| {
                            for ea in &e.attributes {
                                w.create_element("attribute")
                                    .with_attribute(("name", ea.name.as_str()))
                                    .write_inner_content(|w| {
                                        w.write_event(Event::Text(BytesText::new(
                                            &ea.value.to_string(),
                                        )))?;
                                        OK
                                    })?;
                            }
                            OK
                        })?;
                        // Write Relationships
                        write_ocel_relationships(&e.relationships, w)?;
                        OK
                    })?;
            }
            OK
        })?;

        OK
    })?;
    OK
}

fn write_ocel_type_attrs<W: std::io::Write>(
    attrs: &Vec<OCELTypeAttribute>,
    w: &mut quick_xml::Writer<W>,
) -> Result<(), quick_xml::Error> {
    w.create_element("attributes").write_inner_content(|w| {
        for at in attrs {
            w.create_element("attribute")
                .with_attributes(vec![
                    ("name", at.name.as_str()),
                    ("type", at.value_type.as_str()),
                ])
                .write_empty()?;
        }
        OK
    })?;
    OK
}

fn write_ocel_relationships<W: std::io::Write>(
    rels: &Vec<OCELRelationship>,
    w: &mut quick_xml::Writer<W>,
) -> Result<(), quick_xml::Error> {
    w.create_element("objects").write_inner_content(|w| {
        for r in rels {
            w.create_element("relationship")
                .with_attribute(("object-id", r.object_id.as_str()))
                .with_attribute(("qualifier", r.qualifier.as_str()))
                .write_empty()?;
        }
        OK
    })?;
    OK
}

/// Export [`OCEL`] to a [`File`]
pub fn export_ocel_to_xml_file(
    ocel: &OCEL,
    file: File,
    compress_gz: bool,
) -> Result<(), quick_xml::Error> {
    if compress_gz {
        let encoder = GzEncoder::new(BufWriter::new(file), Compression::fast());
        return export_ocel_xml(&mut Writer::new(BufWriter::new(encoder)), ocel);
    }
    export_ocel_xml(&mut Writer::new(BufWriter::new(file)), ocel)
}

#[cfg(test)]
mod ocel_xml_export_test {
    use std::{fs::File, time::Instant};

    use crate::{
        import_ocel_xml_file, import_ocel_xml_slice, ocel::xml_ocel_export::export_ocel_to_xml_file,
    };

    #[test]
    fn export_round_trip_order_management() {
        let log_bytes = include_bytes!("../tests/test_data/order-management.xml");
        let mut now = Instant::now();
        let ocel = import_ocel_xml_slice(log_bytes);
        let obj = ocel.objects.first().unwrap();
        println!("{:?}", obj);
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

        let export_path = "/tmp/order-mangement-export.xml";
        now = Instant::now();
        export_ocel_to_xml_file(&ocel, File::create(export_path).unwrap(), false).unwrap();
        println!(
            "Exported OCEL with {} objects and {} events in {:#?}",
            ocel.objects.len(),
            ocel.events.len(),
            now.elapsed()
        );
        now = Instant::now();
        let ocel2 = import_ocel_xml_file(&export_path);
        println!(
            "Imported OCEL AGAIN with {} objects and {} events in {:#?}",
            ocel.objects.len(),
            ocel.events.len(),
            now.elapsed()
        );

        std::fs::remove_file(export_path).unwrap();
        assert_eq!(ocel2.objects.len(), 10840);
        assert_eq!(ocel2.events.len(), 21008);

        // Do not use assert_eq!(...) because this will flood stdout with full OCEL if not true
        assert!(ocel == ocel2);
    }

    #[test]
    fn export_round_trip_p2p() {
        let log_bytes = include_bytes!("../tests/test_data/ocel2-p2p.xml");
        let mut now = Instant::now();
        let ocel = import_ocel_xml_slice(log_bytes);
        let obj = ocel.objects.first().unwrap();
        println!("{:?}", obj);
        println!(
            "Imported OCEL with {} objects and {} events in {:#?}",
            ocel.objects.len(),
            ocel.events.len(),
            now.elapsed()
        );
        assert_eq!(ocel.objects.len(), 9543);
        assert_eq!(ocel.events.len(), 14671);

        let export_path = "/tmp/ocel2-p2p-export.xml";
        now = Instant::now();
        export_ocel_to_xml_file(&ocel, File::create(export_path).unwrap(), false).unwrap();
        println!(
            "Exported OCEL with {} objects and {} events in {:#?}",
            ocel.objects.len(),
            ocel.events.len(),
            now.elapsed()
        );
        now = Instant::now();
        let ocel2 = import_ocel_xml_file(&export_path);
        println!(
            "Imported OCEL AGAIN with {} objects and {} events in {:#?}",
            ocel.objects.len(),
            ocel.events.len(),
            now.elapsed()
        );
        std::fs::remove_file(export_path).unwrap();
        assert_eq!(ocel2.objects.len(), 9543);
        assert_eq!(ocel2.events.len(), 14671);

        assert_eq!(ocel.event_types, ocel2.event_types);
        assert_eq!(ocel.object_types, ocel2.object_types);
        assert_eq!(ocel.events, ocel2.events);

        // Do not use assert_eq!(...) because this will flood stdout with full OCEL if not true
        assert!(ocel == ocel2);

        // for (o1,o2) in ocel.objects.iter().zip(ocel2.objects.iter()) {
        //     for (a1,a2) in o1.attributes.iter().zip(o2.attributes.iter()) {
        //         assert_eq!(a1,a2);
        //     }
        // }
    }
}
