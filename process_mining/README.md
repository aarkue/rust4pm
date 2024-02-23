# process_mining

This crate contains basic data structures, functions and utilities for Process Mining.

Full documentation of the modules, structs and functions of this crate is available at **[docs.rs/process_mining/](https://docs.rs/process_mining/)**.

## Modules

Currently, the following modules are implemented:

- `event_log` (Event Logs, traditional and object-centric)
- `petri_net` (Petri nets)
- `alphappp` (Alpha+++ discovery algorithm)

## Getting Started

To get started, you can try importing an XES event log using the following code snippet:

```rust
use process_mining::{import_xes_file, XESImportOptions};

let log_res = import_xes_file("log.xes", XESImportOptions::default());

match log_res {
    Ok(log) => {
      println!("Imported event log with {} traces", log.traces.len())
    },
    Err(e) => {
      eprintln!("XES Import failed: {:?}", e)
    },
}
```


## Examples
```rust
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
    let xes_path = "./src/event_log/tests/test_data/AN1-example.xes";

    // // Default XES parsing
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
    let ocel = import_ocel_xml_file("./src/event_log/tests/test_data/order-management.xml");
    println!(
        "Imported OCEL2 XML with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );

    // Parsing JSON OCEL files
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

```
# Parsed XES with 43809 cases in 12.334751875s
# Streamed XES into Activity Projection with 28457 variants in 6.078319582s
# Imported OCEL2 XML with 10840 objects and 21008 events in 85.719414ms
# Imported OCEL2 JSON with 10840 objects and 21008 events in 102.26467ms
```