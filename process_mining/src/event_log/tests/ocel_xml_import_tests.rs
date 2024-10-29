use std::{
    fs::File,
    io::{BufWriter, Read},
    time::Instant,
};

use crate::{
    import_ocel_json_from_path, import_ocel_json_from_slice, import_ocel_xml_slice,
    ocel::xml_ocel_export::export_ocel_xml_path, utils::test_utils::get_test_data_path,
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
    let export_path = get_test_data_path()
        .join("export")
        .join("order-management-export.xml");
    export_ocel_xml_path(&ocel, &export_path).unwrap();
}

#[test]
fn test_order_ocel_json_import() {
    let log_bytes = &get_ocel_file_bytes("order-management.json");
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
    let log_bytes = &get_ocel_file_bytes("ocel2-p2p.xml");
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
    let log_bytes = &get_ocel_file_bytes("ocel2-p2p.json");
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
    let log_bytes = &get_ocel_file_bytes("ContainerLogistics.xml");
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
    let export_path = get_test_data_path()
        .join("export")
        .join("ContainerLogistics-EXPORT.xml");
    export_ocel_xml_path(&ocel, &export_path).unwrap();
}

#[test]
fn test_ocel_logistics_json_import() {
    let log_bytes = &get_ocel_file_bytes("ContainerLogistics.json");
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

// #[test]
// fn test_ocel_angular_xml_import() {
//     // Use PathBuf instead of including bytes because the file is very large
//     let path = get_test_data_path()
//         .join("ocel")
//         .join("angular_github_commits_ocel.xml");
//     let now = Instant::now();
//     let ocel = import_ocel_xml_file(&path);
//     let obj = ocel.objects.first().unwrap();
//     println!("{:?}", obj);
//     println!(
//         "Imported OCEL with {} objects and {} events in {:#?}",
//         ocel.objects.len(),
//         ocel.events.len(),
//         now.elapsed()
//     );
//     assert_eq!(ocel.events.len(), 27847);
//     assert_eq!(ocel.objects.len(), 28317); // 35392
//     export_ocel_xml_path(&ocel, "angular_github_commits_ocel-EXPORT.xml").unwrap();
//     export_ocel_json_path(&ocel, "angular_github_commits_ocel-EXPORT.json").unwrap();
// }

// #[test]
// fn test_ocel_angular_json_import() {
//     // Use PathBuf instead of including bytes because the file is very large
//     let path = get_test_data_path()
//         .join("ocel")
//         .join("angular_github_commits_ocel-EXPORT.json");
//     let now = Instant::now();
//     let ocel = import_ocel_json_from_path(path).unwrap();
//     let obj = ocel.objects.first().unwrap();
//     println!("{:?}", obj);
//     println!(
//         "Imported OCEL with {} objects and {} events in {:#?}",
//         ocel.objects.len(),
//         ocel.events.len(),
//         now.elapsed()
//     );
//     assert_eq!(ocel.events.len(), 27847);
//     assert_eq!(ocel.objects.len(), 28317); // 35392
//     export_ocel_xml_path(&ocel, "angular_github_commits_ocel-EXPORT.xml").unwrap();
// }

#[test]
fn test_ocel_pm4py_log() {
    let log_bytes = &get_ocel_file_bytes("pm4py-ocel20_example.xmlocel");
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
    let log_bytes = &get_ocel_file_bytes("pm4py-ocel20_example.jsonocel");
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
    let log_bytes = &get_ocel_file_bytes("order-management.json");
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

    let ocel_export_path = get_test_data_path()
    .join("export")
    .join("order-management-export-EXPORT2.json");
    let writer = BufWriter::new(File::create(&ocel_export_path).unwrap());
    serde_json::to_writer(writer, &ocel).unwrap();

    let ocel2 = import_ocel_json_from_path(&ocel_export_path).unwrap();

    assert_eq!(ocel2.objects.len(), 10840);
    assert_eq!(ocel2.events.len(), 21008);

    assert!(ocel == ocel2);
}

#[test]
fn test_ocel_failing_xml() {
    let log_bytes = &get_ocel_file_bytes("ocel-failure.xml");
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
