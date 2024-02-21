use std::time::Instant;

use chrono::DateTime;
use process_mining::{
    event_log::{
        import_xes::{build_ignore_attributes, XESImportOptions},
        ocel::xml_ocel_import::import_ocel_xml_file,
    },
    import_xes_file,
};

fn main() {
    let now = Instant::now();

    // XES
    let log = import_xes_file(
        "log.xes",
        XESImportOptions {
                ignore_event_attributes_except: Some(build_ignore_attributes(vec![
                    "concept:name",
                    "time:timestamp",
                ])),
                ignore_trace_attributes_except: Some(build_ignore_attributes(vec!["concept:name"])),
            ..Default::default()
        },
    )
    .unwrap();

    println!(
        "Imported XES with {} traces in {:#?}",
        log.traces.len(),
        now.elapsed()
    );

    // OCEL:

    // println!("{:?}",log.traces.first().unwrap());
    // let ocel = import_ocel_xml_file("/home/aarkue/dow/order-management(2).xml");
    // println!(
    //     "Imported OCEL with {} objects and {} events in {:#?}",
    //     ocel.objects.len(),
    //     ocel.events.len(),
    //     now.elapsed()
    // );
}
