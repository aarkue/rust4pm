# Changelog

## `process_mining` 0.4.2
- **New OCEL CSV Format**
  - Added CSV format support for OCEL:
  - Added Importer/Exporter for CSV OCEL file format
  - Added CSV file format to OCEL io trait + known formats (as `.ocel.csv`)
- **Bindings Improvements**:
  - Exposed OC-DECLARE conformance function (`oc_declare_conformace`) to bindings
  - Renamed `discover_oc-declare` binding to `discover_oc_declare` (**Breaking for Bindings**)
  - Renamed `discover_dfg_from_locel` to `discover_dfg_from_ocel` (**Breaking**)
  - Added `SlimLinkedOCEL` <-> `OCEL` conversion support in bindings
  - Implemented `LinkedOCELAccess` trait support in bindings macro for more generic functions
  - Added `ocel_type_stats` binding to compute event/object type statistics
  - Exposed `flatten_ocel_on` function to bindings for flattening OCEL on object types
  - Exposed `add_init_exit_events_to_ocel` function to bindings
- **Other Fixes and Improvements**:
  - Fixed SQLite/DuckDB export to remove existing file before export (prevents UNIQUE constraint errors)
  - Combined/Deduped timestamp-related parsing functionality across files
  - Implemented `Null` as default `OCELAttributeValue`
- **Internal Improvements**:
  - Updated `rusqlite` and related dependencies
  - Improved CLI in `r4pm`

### Breaking Changes / Migration Guide
- The `From<OCELAttributeValue>` implementation for `OCELAttributeType` was removed. Instead, use the `get_type` function on `OCELAttributeValue` to retrieve its type.
- Updates related to io module for CSV parsing (e.g., new error variant in `OCELIOError`)
- Renamed binding `discover_oc-declare` to `discover_oc_declare`

## `process_mining` 0.4.1
- Added `verbose` option to `XESImportOptions`, defaulting to true
  - Note: Technically this is a breaking change, however the recommended way to use `XESImportOptions` is non-exhaustive with default fallback:
    - e.g., ```XESImportOptions {verbose: false, ..Default::default()}```

## `process_mining` 0.4.0

### Restructuring (Current)
- **Unified IO Traits**: Introduced `Importable` and `Exportable` traits in `process_mining::core::io` to standardize import and export operations across different data structures.
- **EventLog IO**: Implemented `Importable` and `Exportable` for `EventLog`, supporting JSON (`.json`), XES (`.xes`), and Gzipped XES (`.xes.gz`) formats.
- **PetriNet IO**: Implemented `Importable` and `Exportable` for `PetriNet`, supporting PNML (`.pnml`) format.
- **OCEL IO**: Implemented `Importable` and `Exportable` for Object-Centric Event Logs (OCEL), including support for SQLite and DuckDB (if features enabled).
- **Format Inference**: Added automatic format inference based on file extensions (e.g., `.xes`, `.xes.gz`, `.pnml`).
- **Auto-Bindings**: Added auto-binding functionality to facilitate Python bindings generation.
- **Module Restructuring**:
    - Moved Alpha+++ discovery to `process_mining::discovery`.
    - Moved Petri nets to `process_mining::core::process_models`.
    - Moved DFG discovery to `process_mining::discovery`.
- **API Simplification**: Users can now use generic `import_from_path` and `export_to_path` methods. These methods now strictly rely on file extension for format inference, removing the optional format argument.

### Features (Unreleased on crates.io)
- **KuzuDB Support**: Added initial support for OCEL export to KuzuDB.
- **DuckDB Support**: Added example for OCEL export to DuckDB.
- **Polars Export**: Added OCEL to Polars DataFrame export.
- **Object-Centric Process Trees**: Added implementation of object-centric process trees and abstraction-based conformance checking.
- **Token-Based Replay**: Implemented token-based replay on Petri nets.
- **Incidence Matrices**: Added incidence matrices for Petri nets.
- **Event Log Macros**: Implemented macros for easier event log creation.
- **OC-DECLARE**: Object-centric declarative process models, with discovery and conformance checking.

### Changed
- **Exposed Fields**: Exposed `OCLanguageAbstraction` fields.

### Migration Guide
- **Importing Event Logs**:
  - Old: `import_xes_file("log.xes")`
  - New: `EventLog::import_from_path("log.xes")`
- **Exporting Event Logs**:
  - New: `log.export_to_path("log.xes")`
- **Traits**: Ensure `process_mining::Importable` and `process_mining::Exportable` are in scope if you need to use the traits generically.
- **Format Specification**: If you need to specify a format explicitly (e.g., reading from a stream or non-standard extension), use `import_from_reader` or `export_to_writer` which still accept a format string.
