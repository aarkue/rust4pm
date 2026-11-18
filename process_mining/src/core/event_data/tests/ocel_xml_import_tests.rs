use std::{
    fs::File,
    io::{BufWriter, Read},
    time::Instant,
};

use crate::{
    core::event_data::object_centric::{
        io::OCELIOError,
        linked_ocel::{LinkedOCELAccess, SlimLinkedOCEL},
    },
    test_utils::get_test_data_path,
    Exportable, Importable, OCEL,
};

fn get_ocel_file_bytes(name: &str) -> Vec<u8> {
    let path = get_test_data_path().join("ocel").join(name);
    let mut bytes = Vec::new();
    File::open(&path).unwrap().read_to_end(&mut bytes).unwrap();
    bytes
}

#[test]
fn test_ocel_xml_import() {
    let log_bytes = &get_ocel_file_bytes("order-management.xml");
    let now = Instant::now();
    let ocel = OCEL::import_from_bytes(log_bytes, "xml").unwrap();
    let obj = ocel.objects.first().unwrap();
    println!("{obj:?}");
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
    assert_eq!(ocel.events.len(), 21008);
    assert_eq!(ocel.objects.len(), 10840);
    let export_path = get_test_data_path()
        .join("export")
        .join("order-management-export.xml");
    ocel.export_to_path_as(&export_path, "xml", Default::default())
        .unwrap();
}

#[test]
fn test_ocel_xml_import_json_ground_truth() {
    let log_bytes = &get_ocel_file_bytes("order-management.xml");
    let ocel = OCEL::import_from_bytes(log_bytes, "xml").unwrap();
    let locel = SlimLinkedOCEL::from_ocel(ocel);
    let json_path = get_test_data_path()
        .join("ocel")
        .join("order-management.json");
    let locel_json = SlimLinkedOCEL::import_from_path(&json_path).unwrap();
    let xml_path = get_test_data_path()
        .join("ocel")
        .join("order-management.xml");
    let locel_xml = SlimLinkedOCEL::import_from_path(&xml_path).unwrap();
    for ob in locel.get_all_obs() {
        let ob_id = locel.get_ob_id(ob);
        let ob_json = locel_json.get_ob_by_id(ob_id).unwrap();
        let ob_xml = locel_xml.get_ob_by_id(ob_id).unwrap();
        let full_ob = locel.get_full_ob(ob).into_owned();
        let full_ob_json = locel_json.get_full_ob(ob_json).into_owned();
        let full_ob_xml = locel_xml.get_full_ob(ob_xml).into_owned();
        println!("Comparing object {ob_id}");
        assert_eq!(full_ob, full_ob_xml);
        // Currently the JSON import does not necessarily parse attributes as the same type (e.g., float instead of int)
        assert_eq!(full_ob.relationships, full_ob_json.relationships);
        assert_eq!(full_ob.object_type, full_ob_json.object_type);
    }
    for ev in locel.get_all_evs() {
        let ev_id = locel.get_ev_id(ev);
        let ev_json = locel_json.get_ev_by_id(ev_id).unwrap();
        let ev_xml = locel_xml.get_ev_by_id(ev_id).unwrap();
        let full_ev = locel.get_full_ev(ev).into_owned();
        let full_ev_json = locel_json.get_full_ev(ev_json).into_owned();
        let full_ev_xml = locel_xml.get_full_ev(ev_xml).into_owned();
        println!("Comparing event {ev_id}");
        assert_eq!(full_ev, full_ev_xml);
        assert_eq!(full_ev, full_ev_json);
    }
}

#[test]
fn test_xes_as_ocel_xml_import() {
    let xes_path = get_test_data_path().join("xes").join("small-example.xes");
    let ocel = OCEL::import_from_bytes(&std::fs::read(xes_path).unwrap(), "xml");
    assert!(matches!(ocel, Result::Err(OCELIOError::Invalid(_))));
}

#[test]
fn test_order_ocel_json_import() {
    let log_bytes = &get_ocel_file_bytes("order-management.json");
    let now = Instant::now();
    let ocel = OCEL::import_from_bytes(log_bytes, "json").unwrap();
    let obj = ocel.objects.first().unwrap();
    println!("{obj:?}");
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
    assert_eq!(ocel.events.len(), 21008);
    assert_eq!(ocel.objects.len(), 10840);
}

#[test]
fn test_ocel_p2p_xml_import() {
    let log_bytes = &get_ocel_file_bytes("ocel2-p2p.xml");
    let now = Instant::now();
    let ocel = OCEL::import_from_bytes(log_bytes, "xml").unwrap();
    let obj = ocel.objects.first().unwrap();
    println!("{obj:?}");
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
    assert_eq!(ocel.events.len(), 14671);
    assert_eq!(ocel.objects.len(), 9543);
}

#[test]
fn test_ocel_p2p_json_import() {
    let log_bytes = &get_ocel_file_bytes("ocel2-p2p.json");
    let now = Instant::now();
    let ocel = OCEL::import_from_bytes(log_bytes, "json").unwrap();
    let obj = ocel.objects.first().unwrap();
    println!("{obj:?}");
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
    assert_eq!(ocel.events.len(), 14671);
    assert_eq!(ocel.objects.len(), 9543);
}

#[test]
fn test_ocel_logistics_xml_import() {
    let log_bytes = &get_ocel_file_bytes("ContainerLogistics.xml");
    let now = Instant::now();
    let ocel = OCEL::import_from_bytes(log_bytes, "xml").unwrap();
    let obj = ocel.objects.first().unwrap();
    println!("{obj:?}");
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
    assert_eq!(ocel.events.len(), 35413); //35761
    assert_eq!(ocel.objects.len(), 13910); //14013
    let export_path = get_test_data_path()
        .join("export")
        .join("ContainerLogistics-EXPORT.xml");
    ocel.export_to_path_as(&export_path, "xml", Default::default())
        .unwrap();
}

#[test]
fn test_ocel_logistics_json_import() {
    let log_bytes = &get_ocel_file_bytes("ContainerLogistics.json");
    let now = Instant::now();
    let ocel = OCEL::import_from_bytes(log_bytes, "json").unwrap();
    let obj = ocel.objects.first().unwrap();
    println!("{obj:?}");
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
    assert_eq!(ocel.events.len(), 35413); //35761
    assert_eq!(ocel.objects.len(), 13910); //14013
}

#[test]
fn test_ocel_pm4py_log() {
    let log_bytes = &get_ocel_file_bytes("pm4py-ocel20_example.xmlocel");
    let now = Instant::now();
    let ocel = OCEL::import_from_bytes(log_bytes, "xml").unwrap();
    let obj = ocel.objects.first().unwrap();
    println!("{obj:?}");
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
}

#[test]
fn test_slim_xml_streaming_matches_via_ocel() {
    let log_bytes = get_ocel_file_bytes("order-management.xml");
    let via_ocel = SlimLinkedOCEL::from_ocel(OCEL::import_from_bytes(&log_bytes, "xml").unwrap());
    let streamed = SlimLinkedOCEL::import_from_reader(log_bytes.as_slice(), "xml").unwrap();
    assert_eq!(via_ocel.get_num_evs(), streamed.get_num_evs());
    assert_eq!(via_ocel.get_num_obs(), streamed.get_num_obs());

    // Compare reconstructed OCELs end-to-end so that relationships, attributes,
    // qualifiers, types, and ordering are all checked, not just counts.
    use crate::test_utils::sort_ocel_for_equality_compare;
    use std::collections::HashMap;

    let mut via = via_ocel.construct_ocel();
    let mut sm = streamed.construct_ocel();
    sort_ocel_for_equality_compare(&mut via);
    sort_ocel_for_equality_compare(&mut sm);
    assert_eq!(via.event_types, sm.event_types);
    assert_eq!(via.object_types, sm.object_types);
    let via_evs: HashMap<&str, _> = via.events.iter().map(|e| (e.id.as_str(), e)).collect();
    let sm_evs: HashMap<&str, _> = sm.events.iter().map(|e| (e.id.as_str(), e)).collect();
    assert_eq!(via_evs, sm_evs);
    let via_obs: HashMap<&str, _> = via.objects.iter().map(|o| (o.id.as_str(), o)).collect();
    let sm_obs: HashMap<&str, _> = sm.objects.iter().map(|o| (o.id.as_str(), o)).collect();
    assert_eq!(via_obs, sm_obs);
}

#[test]
fn test_ocel_pm4py_log_json() {
    let now = Instant::now();
    let log_bytes = &get_ocel_file_bytes("pm4py-ocel20_example.jsonocel");
    let ocel = OCEL::import_from_bytes(log_bytes, "json").unwrap();
    let obj = ocel.objects.first().unwrap();
    println!("{obj:?}");
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
}

#[test]
fn test_ocel_order_mangement_log_json() {
    let now = Instant::now();
    let log_bytes = &get_ocel_file_bytes("order-management.json");
    let ocel = OCEL::import_from_bytes(log_bytes, "json").unwrap();
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

    let ocel_export_path = get_test_data_path()
        .join("export")
        .join("order-management-export-EXPORT2.json");
    let writer = BufWriter::new(File::create(&ocel_export_path).unwrap());
    serde_json::to_writer(writer, &ocel).unwrap();

    let ocel2 = OCEL::import_from_path(&ocel_export_path).unwrap();

    assert_eq!(ocel2.objects.len(), 10840);
    assert_eq!(ocel2.events.len(), 21008);

    assert!(ocel == ocel2);
}

#[test]
fn test_ocel_xml_text_outside_attributes_is_ignored() {
    use crate::core::event_data::object_centric::ocel_struct::OCELAttributeValue;
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<log>
  <object-types>
    <object-type name="o">
      <attributes>
        <attribute name="s" type="string"/>
        <attribute name="n" type="integer"/>
      </attributes>
    </object-type>
  </object-types>
  <event-types>
    <event-type name="e">
      <attributes>
        <attribute name="s" type="string"/>
      </attributes>
    </event-type>
  </event-types>
  <objects>
    <object id="o1" type="o">stray
      <attributes>
        <attribute name="s" time="1970-01-01T00:00:00Z"> A </attribute>
        <attribute name="n" time="1970-01-01T00:00:00Z"> 42
        </attribute>after
        <attribute name="s" time="not a time">skipped</attribute>
      </attributes>
    </object>
  </objects>
  <events>
    <event id="e1" type="e" time="1970-01-01T00:00:00Z">hello
      <attributes>
        <attribute name="s">  </attribute>
      </attributes>
    </event>
    <event id="e2" type="e" time="1970-01-01T00:00:00Z">hello</event>
  </events>
</log>"#;
    let ocel = OCEL::import_from_bytes(xml.as_bytes(), "xml").unwrap();
    let values: Vec<_> = ocel.objects[0]
        .attributes
        .iter()
        .map(|a| a.value.clone())
        .collect();
    assert_eq!(
        values,
        [
            OCELAttributeValue::String(" A ".to_string()),
            OCELAttributeValue::Integer(42),
        ]
    );
    assert_eq!(
        ocel.events[0].attributes[0].value,
        OCELAttributeValue::String("  ".to_string())
    );
    assert!(ocel.events[1].attributes.is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn test_ocel_export_reports_write_errors_at_flush() {
    // Small enough to sit in the write buffer until the final flush; writes to /dev/full fail
    // with ENOSPC
    let ocel = crate::OCEL {
        event_types: vec![],
        object_types: vec![],
        events: vec![],
        objects: vec![],
    };
    for format in ["json", "xml", "json.gz"] {
        assert!(ocel
            .export_to_path_as("/dev/full", format, Default::default())
            .is_err());
    }
}

#[test]
fn test_ocel_failing_xml() {
    let log_bytes = &get_ocel_file_bytes("ocel-failure.xml");
    let now = Instant::now();
    let ocel = OCEL::import_from_bytes(log_bytes, "xml").unwrap();
    let obj = ocel.objects.first().unwrap();
    println!("{obj:?}");
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
}
