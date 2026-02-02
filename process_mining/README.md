# `process_mining`

A Rust library for Process Mining, providing efficient data structures and algorithms for working with event data.

[![Crates.io](https://img.shields.io/crates/v/process_mining.svg)](https://crates.io/crates/process_mining)
[![Docs.rs](https://img.shields.io/badge/docs.rs-process_mining-blue)](https://docs.rs/process_mining/)
[![Docs.rs](https://img.shields.io/badge/GitHub-Rust4PM-teal)](https://github.com/aarkue/rust4pm)

## Module Structure

The library is organized into the following main modules:

- **`core`**: Fundamental data structures (e.g., `EventLog`, `OCEL`, `PetriNet`) and I/O traits.
- **`discovery`**: Algorithms for discovering process models from event data (e.g., Alpha+++, DFG).
- **`conformance`**: Techniques for checking conformance between data and models (e.g., Token-based replay).

## Examples

You can find various usage examples in the [`examples/`](examples/) directory, covering:
- Importing and analyzing XES event logs (`event_log_stats.rs`)
- Working with OCEL 2.0 data (`ocel_stats.rs`)
- Process discovery (`process_discovery.rs`)
- Exporting to DuckDB/KuzuDB (`ocel_duckdb_export.rs`, `ocel_kuzudb_export.rs`)

To run an example:
```bash
cargo run --example event_log_stats -- <path_to_log.xes>
```

For more details, see the [Examples README](examples/README.md).

## Features

- **Event Data Support**:
  - **XES**: Import and export of IEEE XES event logs.
  - **OCEL 2.0**: Full support for Object-Centric Event Logs (`JSON`, `XML`, `SQLite`, and also an unofficial `DuckDB` format).
- **Process Discovery**:
  - Directly-Follows Graphs (DFG)
  - Alpha Miner
  - Object-Centric DFG
  - OC-DECLARE Constraints with Synchronization
- **Process Models**:
  - Petri Nets (import/export PNML, export to SVG/PNG via Graphviz)
  - (Object-Centric) Process Trees
  - OC-DECLARE Models
- **Performance**: Built with Rust for high performance and memory safety.

## Documentation

Full API documentation is available at [docs.rs/process_mining](https://docs.rs/process_mining/).

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
