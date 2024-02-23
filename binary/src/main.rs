use std::{
    fs::File, io::BufReader, time::Instant
};

use process_mining::{
    event_log::{
        activity_projection::EventLogActivityProjection,
        constants::ACTIVITY_NAME,
        import_xes::{build_ignore_attributes, XESImportOptions},
        stream_xes::stream_xes_from_path,
    },
    import_ocel_xml_file, import_xes_file, OCEL,
};

fn main() {
    let xes_path = "../../../dow/event_data/BPI Challenge 2018.xes.gz";

    // Default XES parsing
    let now = Instant::now();
    let log = import_xes_file(xes_path, XESImportOptions::default()).unwrap();
    println!(
        "Parsed XES with {} cases in {:#?}",
        log.traces.len(),
        now.elapsed()
    );

    // Streaming XES Parsing (constructing a primitive [EventLogActivityProjection])
    // This demonstrates how streaming can enable very low memory consumption and faster processing
    let now = Instant::now();
    let st_res = stream_xes_from_path(
        xes_path,
        XESImportOptions {
            ignore_event_attributes_except: Some(build_ignore_attributes(vec![ACTIVITY_NAME])),
            ignore_trace_attributes_except: Some(build_ignore_attributes(Vec::<&str>::new())),
            ignore_log_attributes_except: Some(build_ignore_attributes(Vec::<&str>::new())),
            ..XESImportOptions::default()
        },
    );
    match st_res {
        Ok((mut st,_log_data)) => {
            let projection: EventLogActivityProjection = (&mut st).into();
            if let Some(e) = st.check_for_errors() {
                eprintln!("Error: {}",e);
            }
            println!(
                "Streamed XES into Activity Projection with {} variants in {:#?}",
                projection.traces.len(),
                now.elapsed()
            );
        }
        Err(e) => {
            eprintln!("Error while streaming parsing: {}", e);
        }
    }

    // Parsing XML OCEL files:
    let now = Instant::now();
    let ocel = import_ocel_xml_file("../../../dow/event_data/order-management.xml");
    println!(
        "Imported OCEL2 XML with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );

    // Parsing JSON OCEL files
    let now = Instant::now();
    let ocel: OCEL = serde_json::from_reader(BufReader::new(
        File::open("../../../dow/event_data/order-management.json").unwrap(),
    ))
    .unwrap();
    println!(
        "Imported OCEL2 JSON with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
}
