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

- **`inductive_miner.rs`**: Discovers a process tree with the Inductive Miner (IM, IMf, or IMd/IMfd with `--dfg`), optionally exporting it as PNML.
  ```bash
  cargo run --release --example inductive_miner -- <path_to_log.xes> [noise threshold] [output.pnml]
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

- **`object_centric_discovery.rs`**: Discovers an object-centric Petri net with the Inductive Miner, optionally for some object types only.
  ```bash
  cargo run --release --example object_centric_discovery -- <path_to_ocel> [noise threshold] [variable arc tolerance] [--types a,b,c | --exclude a,b,c]
  ```

- **`ocel_stats.rs`**: Imports an OCEL and prints basic statistics.
  ```bash
  cargo run --example ocel_stats -- <path_to_ocel.xml>
  ```

- **`ocel_csv_export.rs`**: Imports an OCEL and exports it to CSV format.
  ```bash
  cargo run --example ocel_csv_export -- <path_to_ocel> [output.ocel.csv]
  ```

- **`ocel_duckdb_export.rs`**: Imports an OCEL and exports it to a DuckDB database.
  ```bash
  cargo run --example ocel_duckdb_export --features ocel-duckdb -- <path_to_ocel.xml>
  ```

- **`ocel_stream_to_duckdb.rs`**: Streams an OCEL file in any OCEL format into a DuckDB database (consolidated schema). JSON, XML and bundles stream without loading the whole log into memory. An existing output file is replaced.
  ```bash
  cargo run --release --example ocel_stream_to_duckdb --features ocel-duckdb -- <src> <out.duckdb>
  ```

  `ocel-duckdb` links against an installed `libduckdb`. Without one, use `--features ocel-duckdb-bundled`, which compiles DuckDB from source.

- **`oc_declare.rs`**: Imports an OCEL and discovers OC-DECLARE constraints with the default options.
  ```bash
  cargo run --release --example oc_declare -- <path_to_ocel>
  ```

- **`oc_declare_evaluation.rs`**: Runs the OC-DECLARE discovery evaluation on a fixed set of OCEL 2.0 logs (Logistics, P2P, O2C, BPIC 2017, 2014 and 2019) that are expected under the given directory.
  ```bash
  cargo run --release --example oc_declare_evaluation -- <directory_with_logs>
  ```

- **`ocel_dataset_crosscheck.rs`**: Reads every re-exported form of a log in a directory of datasets back and compares it against the source log it was written from. Exits with an error code when any comparison fails.
  ```bash
  cargo run --release --example ocel_dataset_crosscheck --features ocel-bundle-parquet,ocel-sqlite -- --root <root>
  ```

