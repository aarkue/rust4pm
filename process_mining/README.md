# `process_mining`

A Rust library for Process Mining, providing efficient data structures and algorithms for working with event data.

[![Crates.io](https://img.shields.io/crates/v/process_mining.svg)](https://crates.io/crates/process_mining)
[![Docs.rs](https://img.shields.io/badge/docs.rs-process_mining-blue)](https://docs.rs/process_mining/)
[![Docs.rs](https://img.shields.io/badge/GitHub-Rust4PM-teal)](https://github.com/aarkue/rust4pm)

## Quick start

Event data and models are read and written through the `Importable` and `Exportable` traits,
which pick the format from the file extension:

```rust,no_run
use process_mining::conformance::alignments::{align_variants, compute_fitness, AlignmentOptions};
use process_mining::core::event_data::case_centric::utils::activity_projection::EventLogActivityProjection;
use process_mining::discovery::case_centric::inductive_miner::{
    discover_inductive_net, InductiveMinerPreset, InductiveMinerVariant,
};
use process_mining::{EventLog, Exportable, Importable, OCEL};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let log = EventLog::import_from_path("log.xes.gz")?;
    let projection = EventLogActivityProjection::from(&log);

    let net = discover_inductive_net(&projection, InductiveMinerVariant::Imf.options(InductiveMinerPreset::default(), 0.2));
    net.export_to_path("model.pnml")?;

    let options = AlignmentOptions::default();
    let alignments = align_variants(&net, &projection, &options);
    let fitness = compute_fitness(&net, &alignments, &options)?;
    println!("Log fitness: {}", fitness.log_fitness);

    // JSON, XML and CSV out of the box, SQLite, DuckDB and the bundle format with their features
    let ocel = OCEL::import_from_path("order-management.json")?;
    println!("{} events, {} objects", ocel.events.len(), ocel.objects.len());
    Ok(())
}
```

## Module Structure

The library is organized into the following main modules:

- **`core`**: Fundamental data structures (e.g., `EventLog`, `OCEL`, `PetriNet`) and I/O traits.
- **`discovery`**: Algorithms for discovering process models from event data (e.g., Alpha+++, Inductive Miner, DFG, OC-DECLARE).
- **`conformance`**: Techniques for checking conformance between data and models (e.g., alignments, escaping-edges precision, token-based replay).
- **`analysis`**: Statistics and views on event data (e.g., dotted charts, object attribute changes, path schemas).
- **`visualization`**: Layouts and SVG drawings of models (feature `visualization`).
- **`bindings`**: A registry of functions callable by name with JSON arguments, for other languages and the `r4pm` CLI (feature `bindings`).
- **`utils`**: Shared helpers (e.g., the Dijkstra search behind alignments).

## Examples

You can find various usage examples in the [`examples/`](https://github.com/aarkue/rust4pm/tree/main/process_mining/examples) directory, covering:
- Importing and analyzing XES event logs (`event_log_stats.rs`)
- Working with OCEL 2.0 data (`ocel_stats.rs`)
- Process discovery (`process_discovery.rs`, `inductive_miner.rs`, `oc_declare.rs`)
- Alignments and fitness (`calculate_alignments.rs`)
- Exporting to `DuckDB` (`ocel_duckdb_export.rs`, `ocel_stream_to_duckdb.rs`)

To run an example:
```bash
cargo run --example event_log_stats -- <path_to_log.xes>
```

For more details, see the [Examples README](https://github.com/aarkue/rust4pm/blob/main/process_mining/examples/README.md).

## Features

- **Event Data Support**:
  - **XES**: Import and export of IEEE XES event logs.
  - **OCEL 2.0**: Full support for Object-Centric Event Logs (`JSON`, `XML`, `SQLite`, and also an unofficial `DuckDB` format).
  - **OCEL 2.0 CSV and bundle**: A single-file CSV format for OCEL 2.0, and the bundled CSV/Parquet format (`.ocel.zip` archive or directory, features `ocel-bundle` and `ocel-bundle-parquet`).
  - **Extraction**: Extract `OCEL` data from relational sources (`SQLite`, CSV, Parquet, `PostgreSQL`, `DuckDB`) with extraction blueprints (feature `extraction-blueprint` and the `extraction-dbcon*` features).
- **Process Discovery**:
  - Directly-Follows Graphs (DFG)
  - Alpha Miner
  - Inductive Miner (IM, `IMf`, `IMa` and the DFG variants `IMd` and `IMfd`)
  - Object-Centric DFG
  - Object-Centric Petri Nets
  - OC-DECLARE Constraints with Synchronization
- **Conformance Checking**:
  - Alignments (bidirectional Dijkstra, with memory limits) and alignment-based fitness
  - Escaping-edges precision
  - Token-based replay (feature `token-based-replay`)
  - OC-DECLARE conformance
- **Process Models**:
  - Petri Nets (import/export PNML)
  - (Object-Centric) Process Trees
  - Object-Centric Petri Nets
  - OC-DECLARE Models
- **Visualization**: Layout and SVG export for every model above except object-centric process
  trees, for OCEL type graphs and for partial order traces, in pure Rust through
  [graphlay](https://github.com/aarkue/graphlay) (module and feature `visualization`, on by
  default). Hosts that do not draw can leave it out with `default-features = false`.
- **Warnings**: Importers report recoverable problems (e.g., a value that does not parse) to
  `Control::on_warning`, or through the [`log`](https://docs.rs/log) crate when nothing listens,
  and print nothing themselves.
- **Performance**: Built with Rust for high performance and memory safety.

## Documentation

Full API documentation is available at [docs.rs/process_mining](https://docs.rs/process_mining/).

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
