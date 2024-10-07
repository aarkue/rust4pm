use std::{fs::File, io::BufWriter, path::PathBuf, time::Instant};

use crate::{
    export_ocel_json_path, import_ocel_json_from_path, import_ocel_json_from_slice,
    import_ocel_xml_file, import_ocel_xml_slice, ocel::xml_ocel_export::export_ocel_xml_path,
};

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
    assert_eq!(ocel.events.len(), 21008);
    assert_eq!(ocel.objects.len(), 10840);
    export_ocel_xml_path(&ocel, "order-management-export.xml").unwrap();
}

#[test]
fn test_order_ocel_json_import() {
    let log_bytes = include_bytes!("test_data/order-management.json");
    let now = Instant::now();
    let ocel = import_ocel_json_from_slice(log_bytes).unwrap();
    let obj = ocel.objects.first().unwrap();
    println!("{:?}", obj);
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
    assert_eq!(ocel.events.len(), 14671);
    assert_eq!(ocel.objects.len(), 9543);
}

#[test]
fn test_ocel_p2p_json_import() {
    let log_bytes = include_bytes!("test_data/ocel2-p2p.json");
    let now = Instant::now();
    let ocel = import_ocel_json_from_slice(log_bytes).unwrap();
    let obj = ocel.objects.first().unwrap();
    println!("{:?}", obj);
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
    let log_bytes = include_bytes!("test_data/ContainerLogistics.xml");
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
    assert_eq!(ocel.events.len(), 35413); //35761
    assert_eq!(ocel.objects.len(), 13910); //14013
    export_ocel_xml_path(&ocel, "ContainerLogistics-EXPORT.xml").unwrap();
}

#[test]
fn test_ocel_logistics_json_import() {
    let log_bytes = include_bytes!("test_data/ContainerLogistics.json");
    let now = Instant::now();
    let ocel = import_ocel_json_from_slice(log_bytes).unwrap();
    let obj = ocel.objects.first().unwrap();
    println!("{:?}", obj);
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
fn test_ocel_angular_xml_import() {
    // Use PathBuf instead of including bytes because the file is very large
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("src");
    path.push("event_log");
    path.push("tests");
    path.push("test_data");
    path.push("angular_github_commits_ocel.xml");
    let now = Instant::now();
    let ocel = import_ocel_xml_file(&path);
    let obj = ocel.objects.first().unwrap();
    println!("{:?}", obj);
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
    assert_eq!(ocel.events.len(), 27847);
    assert_eq!(ocel.objects.len(), 28317); // 35392
    export_ocel_xml_path(&ocel, "angular_github_commits_ocel-EXPORT.xml").unwrap();
    export_ocel_json_path(&ocel, "angular_github_commits_ocel-EXPORT.json").unwrap();
}

#[test]
fn test_ocel_angular_json_import() {
    // Use PathBuf instead of including bytes because the file is very large
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("src");
    path.push("event_log");
    path.push("tests");
    path.push("test_data");
    path.push("angular_github_commits_ocel-EXPORT.json");
    let now = Instant::now();
    let ocel = import_ocel_json_from_path(path).unwrap();
    let obj = ocel.objects.first().unwrap();
    println!("{:?}", obj);
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
    assert_eq!(ocel.events.len(), 27847);
    assert_eq!(ocel.objects.len(), 28317); // 35392
    export_ocel_xml_path(&ocel, "angular_github_commits_ocel-EXPORT.xml").unwrap();
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
fn test_ocel_order_mangement_log_json() {
    let now = Instant::now();
    let log_bytes = include_bytes!("test_data/order-management.json");
    let ocel = import_ocel_json_from_slice(log_bytes).unwrap();
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

    let ocel_export_path = "/tmp/order-management-export.json";
    let writer = BufWriter::new(File::create(ocel_export_path).unwrap());
    serde_json::to_writer(writer, &ocel).unwrap();

    let ocel2 = import_ocel_json_from_path(ocel_export_path).unwrap();

    assert_eq!(ocel2.objects.len(), 10840);
    assert_eq!(ocel2.events.len(), 21008);

    assert!(ocel == ocel2);
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
