use std::time::Instant;

use chrono::DateTime;
use process_mining::{
    event_log::{
        import_xes::{build_ignore_attributes, XESImportOptions},
        ocel::xml_ocel_import::import_ocel_xml_file,
        xes_streaming::{stream_xes_slice, stream_xes_slice_gz},
    },
    import_xes_file, import_xes_slice,
};

fn main() {
    
    // let x = include_bytes!("../../process_mining/src/event_log/tests/test_data/BPI Challenge 2018.xes.gz");
    // let now = Instant::now();
    // let s = stream_xes_slice_gz(x, XESImportOptions::default());
    // let count= s.count();
    // println!(
    //     "Streamed XES with {} cases in {:#?}",
    //     count,
    //     now.elapsed()
    // );
    // // XES
    // let res = import_xes_slice(x, true, XESImportOptions::default()).unwrap();
    
    // println!(
    //     "Parsed XES with {} cases in {:#?}",
    //     res.traces.len(),
    //     now.elapsed()
    // );
    // let log = import_xes_file(
    //     "log.xes",
    //     XESImportOptions {
    //             ignore_event_attributes_except: Some(build_ignore_attributes(vec![
    //                 "concept:name",
    //                 "time:timestamp",
    //             ])),
    //             ignore_trace_attributes_except: Some(build_ignore_attributes(vec!["concept:name"])),
    //         ..Default::default()
    //     },
    // )
    // .unwrap();

    // println!(
    //     "Imported XES with {} traces in {:#?}",
    //     log.traces.len(),
    //     now.elapsed()
    // );

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
