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

## Object-Centric Process Mining (OCEL)

- **`ocel_stats.rs`**: Imports an OCEL (XML/JSON) and prints basic statistics.
  ```bash
  cargo run --example ocel_stats -- <path_to_ocel.xml>
  ```

- **`ocel_duckdb_export.rs`**: Imports an OCEL and exports it to a DuckDB database.
  ```bash
  cargo run --example ocel_duckdb_export -- <path_to_ocel.xml>
  ```

- **`ocel_kuzudb_export.rs`**: Imports an OCEL and exports it to a KuzuDB graph database.
  ```bash
  cargo run --example ocel_kuzudb_export -- <path_to_folder_containing_ocel_files>
  ```
