use std::{fs::File, time::Instant};


use process_mining::{
    event_log::{
        import_xes::XESImportOptions,
        xes_streaming::{stream_xes_file, stream_xes_file_gz},
    },
    import_xes_file,
};

fn main() {
    
    let path = "/home/aarkue/doc/sciebo/alpha-revisit/BPI_Challenge_2018.xes";
    let now = Instant::now();
    let file = File::open(path).unwrap();
    let s = if path.ends_with(".gz"){
        stream_xes_file_gz(&file, XESImportOptions::default())
    }else {
        stream_xes_file(&file, XESImportOptions::default())
    };
    // let s = stream_xes_file_gz(&file, XESImportOptions::default());
    // let s = stream_xes_file(path, XESImportOptions::default());
    let count= s.count();
    println!(
        "Streamed XES with {} cases in {:#?}",
        count,
        now.elapsed()
    );

    let now = Instant::now();
    // // XES
    let res = import_xes_file(path, XESImportOptions::default()).unwrap();
    println!(
        "Parsed XES with {} cases in {:#?}",
        res.traces.len(),
        now.elapsed()
    );
    
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
