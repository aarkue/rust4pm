# process_mining

This crate contains basic data structures, functions and utilities for Process Mining.

Visit **[docs.rs/process_mining/](https://docs.rs/process_mining/)** to view the full documentation of this crate.

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
