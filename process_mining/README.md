# process_mining

This crate contains basic data structures, functions and utilities for Process Mining.

Full documentation of the modules, structs and functions of this crate is available at **[docs.rs/process_mining/](https://docs.rs/process_mining/)**.

## Features

- Event Logs
    - Event Log struct
    - Fast XES Parsing (also includes _Streaming XES Import_, which has a very low memory footprint)
    - XES Export (also with stream support)
- Object-Centric Event Logs (OCEL 2.0)
    - OCEL struct
    - Fast OCEL XML & JSON Parsing
- Petri Nets
- Alpha+++ Process Discovery

## Getting Started

To get started, you can try importing an XES event log using the following code snippet:

```rust
use std::{fs::File, io::BufReader, time::Instant};

use process_mining::{
    event_log::{
        activity_projection::EventLogActivityProjection,
        constants::ACTIVITY_NAME,
        export_xes::{export_xes_event_log_to_file_path, export_xes_trace_stream_to_file},
        import_xes::{build_ignore_attributes, XESImportOptions},
        stream_xes::stream_xes_from_path,
    },
    import_ocel_xml_file, import_xes_file, OCEL,
};

fn main() {
    let xes_path = "./src/event_log/tests/test_data/BPI Challenge 2018.xes.gz";

    // Parsing XES
    println!("==Parsing XES==");

    // Default XES parsing
    let now = Instant::now();
    let log = import_xes_file(xes_path, XESImportOptions::default()).unwrap();
    println!(
        "Parsed XES with {} cases in {:#?}",
        log.traces.len(),
        now.elapsed()
    );

    // Streaming XES Parsing (only counting number of traces)
    // Streaming enables very low memory consumption and sometimes also faster processing
    let now = Instant::now();
    let (mut trace_stream, _log_data) =
        stream_xes_from_path(xes_path, XESImportOptions::default()).unwrap();
    println!(
        "Streamed XES counting {} traces in {:#?} ",
        trace_stream.count(),
        now.elapsed()
    );

    // Streaming XES Parsing (constructing a primitive [EventLogActivityProjection])
    // Streaming enables very low memory consumption and sometimes also faster processing
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
        Ok((mut st, _log_data)) => {
            let projection: EventLogActivityProjection = (&mut st).into();
            if let Some(e) = st.check_for_errors() {
                eprintln!("Error: {}", e);
            }
            println!(
                "Streamed XES into Activity Projection ({} variants) in {:#?} (Only parsing concept:name event attributes)",
                projection.traces.len(),
                now.elapsed()
            );
        }
        Err(e) => {
            eprintln!("Error while streaming parsing: {}", e);
        }
    }

    // Writing XES
    println!("\n==Writing XES==");

    // Streaming: Stream-parsing XES and stream-writing XES to .xes.gz (with very low memory footprint!)
    let now = Instant::now();
    let (mut stream, log_data) =
        stream_xes_from_path(xes_path, XESImportOptions::default()).unwrap();
    let file = File::create("/tmp/streaming-export.xes.gz").unwrap();
    export_xes_trace_stream_to_file(stream.into_iter(), log_data, file, true).unwrap();
    println!("Streamed from .xes to .xes.gz in {:?}", now.elapsed());
    std::fs::remove_file("/tmp/streaming-export.xes.gz").unwrap();

    // First Parsing XES completely, then writing XES to .xes.gz file
    let now = Instant::now();
    let log = import_xes_file(xes_path, XESImportOptions::default()).unwrap();
    export_xes_event_log_to_file_path(&log, "/tmp/non-streaming-export.xes.gz").unwrap();
    println!("Read .xes & Wrote to .xes.gz in {:?} total", now.elapsed());
    std::fs::remove_file("/tmp/non-streaming-export.xes.gz").unwrap();


    // Parsing XML OCEL files:
    println!("\n==Parsing XML OCEL==");

    let now = Instant::now();
    let ocel = import_ocel_xml_file("./src/event_log/tests/test_data/order-management.xml");
    println!(
        "Imported OCEL2 XML with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );

    // Parsing JSON OCEL files
    println!("\n==Parsing JSON OCEL==");

    let now = Instant::now();
    let ocel: OCEL = serde_json::from_reader(BufReader::new(
        File::open("./src/event_log/tests/test_data/order-management.json").unwrap(),
    ))
    .unwrap();
    println!(
        "Imported OCEL2 JSON with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
}
```

Example output:

```plain
==Parsing XES==
Parsed XES with 43809 cases in 12.891360353s
Streamed XES counting 43809 traces in 12.109210673s 
Streamed XES into Activity Projection (28457 variants) in 6.244971889s (Only parsing concept:name event attributes)

==Writing XES==
Streamed from .xes to .xes.gz in 22.264394114s
Read .xes & Wrote to .xes.gz in 21.56858199s total

==Parsing XML OCEL==
Imported OCEL2 XML with 10840 objects and 21008 events in 91.707868ms

==Parsing JSON OCEL==
Imported OCEL2 JSON with 10840 objects and 21008 events in 101.915743ms
```