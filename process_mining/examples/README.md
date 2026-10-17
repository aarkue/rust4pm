# Examples

This folder contains example usages of the `process_mining` crate.

## Basic Usage

- **`event_log_stats.rs`**: Imports an XES event log and prints basic statistics (trace count, event count, etc.).
  ```bash
  cargo run --example event_log_stats -- <path_to_log.xes>
  ```

- **`process_discovery.rs`**: Imports an XES event log, discovers a Petri net using the Alpha+++ algorithm, and exports it to PNML.
  ```bash
  cargo run --example process_discovery -- <path_to_log.xes> <output_model.pnml>
  ```

- **`petri_net_import_export.rs`**: Imports a Petri net from PNML, prints stats, and exports it again.
  ```bash
  cargo run --example petri_net_import_export -- <input_model.pnml> <output_model.pnml>
  ```

## Conformance Checking

- **`calculate_alignments.rs`**: Imports an XES event log and a Petri net, computes optimal alignments for every trace variant, and prints the resulting fitness.
  ```bash
  cargo run --release --example calculate_alignments -- <path_to_log.xes> <path_to_model.pnml>
  ```

- **`align_bench.rs`**: Measures alignment time and peak memory (Linux), either for all trace variants or a chosen subset, e.g., the most expensive ones.
  ```bash
  cargo run --release --example align_bench -- discover <log.xes> <model.pnml> <cap>
  cargo run --release --example align_bench -- run <log.xes> <model.pnml> <cap> <all|k1,k2,...>
  ```

## Object-Centric Process Mining

- **`ocel_stats.rs`**: Imports an OCEL and prints basic statistics.
  ```bash
  cargo run --example ocel_stats -- <path_to_ocel.xml>
  ```

- **`ocel_csv_export.rs`**: Imports an OCEL and exports it to CSV format.
  ```bash
  cargo run --example ocel_csv_export -- <path_to_ocel.xml> [output.ocel.csv]
  ```

- **`ocel_duckdb_export.rs`**: Imports an OCEL and exports it to a DuckDB database.
  ```bash
  cargo run --example ocel_duckdb_export -- <path_to_ocel.xml>
  ```

