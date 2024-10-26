# `process_mining`

This crate contains basic data structures, functions and utilities for Process Mining.

Full documentation of the modules, structs and functions of this crate is available at **[docs.rs/process_mining/](https://docs.rs/process_mining/)**.

_As this crate is still in very active development, expect larger API changes also in minor (or even patch) version updates._


## Features

- Event Logs
  - Event Log struct ([`EventLog`])
  - Fast XES Parsing (also includes _Streaming XES Import_, which has a very low memory footprint)
    - See [`import_xes_file`] or [`stream_xes_from_path`]
  - XES Export (also with streaming support)
    - See [`export_xes_event_log_to_file_path`] or [`export_xes_trace_stream_to_file`]
- Object-Centric Event Logs (OCEL 2.0)
  - OCEL struct
  - OCEL import from all available formats (XML, JSON, and `SQLite`)
  - OCEL export to all available formats (XML, JSON, and `SQLite`)
- Petri Nets
  - PNML Export
  - PNML Import
  - Image Export (SVG, PNG, ...; Requires the `graphviz-export` feature)
- Alpha+++ Process Discovery

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

## Additional Information
<details>
<summary>

__Full Code Example Showcase__

</summary>

```rust,no_run
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
    let xes_path = "BPI Challenge 2018.xes.gz";

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
Parsed XES with 43809 cases in 12.643408724s
Streamed XES counting 43809 traces in 11.814082231s 
Streamed XES into Activity Projection (28457 variants) in 7.366106006s (Only parsing concept:name event attributes)

==Writing XES==
Streamed from .xes to .xes.gz in 22.778810621s
Read .xes & Wrote to .xes.gz in 20.944550225s total

==Parsing XML OCEL==
Imported OCEL2 XML with 10840 objects and 21008 events in 101.156023ms

==Parsing JSON OCEL==
Imported OCEL2 JSON with 10840 objects and 21008 events in 108.422759ms
```
</details>

<details>
<summary>

__XES Import/Export: Normal vs. Streaming__

</summary>

For the import/export of event logs, either the normal API (e.g., [`import_xes_file`]) or the streaming API (e.g., [`stream_xes_from_path`]) can be used.
Here, _streaming_ refers to supporting __iterators over traces__.

Internally, the XES import and export functionality is only implemented as a streaming version.
The normal API uses the streaming implementation under the hood to provide convenient wrappers for common situations (i.e., simply importing an XES file as a complete [`EventLog`] struct into memory).

When parsing, only a part of the input XES file containing log-level information will be parsed initially (specifically: parsing will stop before the first trace).
The rest is wrapped behind an iterator and only lazily parses until the next trace is available.

In most situations, there is no large performance difference between the normal and streaming API.
There is, however, a significant difference in memory consumption when importing large data: The streaming import/export functions only use the memory required for log-level information and the information of one trace (i.e., the _one_ trace which was last parsed).
Thus, the streaming methods also allow reading and writing XES event logs which do not fit into the system memory.

For example, if you want to transform trace attributes of an event log read from a large XES file and export the result as a file again, you can either:

1. Import the full event log, transform it, export it
2. Stream the full event log, transform the streamed traces in place, export the resulting trace stream

In such situations, streaming is clearly a better choice, as traces can easily be transformed individually.

__Difference in Memory Consumption__

For the [`BPI Challenge 2018`](https://data.4tu.nl/articles/dataset/BPI_Challenge_2018/12688355) Event Log XES file (`150.9 MiB` as a gzipped `.xes.gz` or `1.8 GB` as a plain `.xes`), parsing the log completely and then exporting it to a `.xes.gz` file uses up to `3.3 GB` of memory at peak.
When using the streaming functions for the XES import and export instead, the memory consumption peaks at only `5.7 MiB`


| Memory Usage without Streaming (`3.3GB`) | Memory Usage with Streaming (`5.7MiB`) 
:-------------------------:|:-------------------------:
[![Plot of Heap usage with a peak at 3.3GB](https://github.com/aarkue/rust-bridge-process-mining/assets/20766652/9ae3deb7-c28b-42e8-b22a-5901c70f505e)](https://github.com/aarkue/rust-bridge-process-mining/assets/20766652/9ae3deb7-c28b-42e8-b22a-5901c70f505e) | [![Plot of Heap usage with a peak at 5.7MiB](https://github.com/aarkue/rust-bridge-process-mining/assets/20766652/466dac4c-263f-4e6f-b9a3-db355dd4e603)](https://github.com/aarkue/rust-bridge-process-mining/assets/20766652/466dac4c-263f-4e6f-b9a3-db355dd4e603) 


</details>


## Contributing

### Test Data

The data (OCEL2, XES, etc. files) used for the tests of this crate are available for download at https://rwth-aachen.sciebo.de/s/4cvtTU3lLOgtxt1.
Simply download this zip and extract it into the `test_data` folder.