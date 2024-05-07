use std::time::Instant;

use crate::{import_ocel_json_from_slice, import_ocel_xml_slice};

#[test]
fn test_ocel_xml_import() {
    let log_bytes = include_bytes!("test_data/order-management.xml");
    let now = Instant::now();
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
}

#[test]
fn test_ocel_p2p_xml_import() {
    let log_bytes = include_bytes!("test_data/ocel2-p2p.xml");
    let now = Instant::now();
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
}

#[test]
fn test_ocel_pm4py_log() {
    let log_bytes = include_bytes!("test_data/pm4py-ocel20_example.xmlocel");
    let now = Instant::now();
    let ocel = import_ocel_xml_slice(log_bytes);
    let obj = ocel.objects.first().unwrap();
    println!("{:?}", obj);
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
}

#[test]
fn test_ocel_pm4py_log_json() {
    let now = Instant::now();
    let log_bytes = include_bytes!("test_data/pm4py-ocel20_example.jsonocel");
    let ocel = import_ocel_json_from_slice(log_bytes).unwrap();
    let obj = ocel.objects.first().unwrap();
    println!("{:?}", obj);
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
}

#[test]
fn test_ocel_failing_xml() {
    let log_bytes = include_bytes!("test_data/ocel-failure.xml");
    let now = Instant::now();
    let ocel = import_ocel_xml_slice(log_bytes);
    let obj = ocel.objects.first().unwrap();
    println!("{:?}", obj);
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
}
