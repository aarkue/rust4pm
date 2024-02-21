#[test]
fn test_ocel_xml_import() {
    let log_bytes = include_bytes!("test_data/order-management.xml");
    let now = Instant::now();
    let ocel = import_ocel_xml_slice(&log_bytes);
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
}
