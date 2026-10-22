# Changelog

## Unreleased
- Alignments now search from both ends at once (`utils::dijkstra_search::search_bidirectional` + `ReversibleSearchProblem`), and detect unreachable final markings up front
- `search` takes `SearchLimits` (states visited, memory) instead of `Option<usize>` and reports `SearchError::MemoryLimitReached` when its memory budget runs out (**Breaking**)
- `SearchProblem::expand` takes the search's `MemoryBudget`, through which buffers that grow during the search must reserve, and `SearchProblem` requires `allocated_bytes` (**Breaking**)
- `AlignmentOptions::max_memory_mib` (default 6 GiB) bounds the memory of all alignment searches of one call, split evenly across the searches running at once; `align_variants_with_retry` aligns the variants that ran out again, one at a time with the whole budget. `max_states` now defaults to `None` instead of 100,000 (**Breaking**)
- Fix parallel arcs between the same place and transition counting separately (previously their weights were not summed, which could underflow a place's token count)
- Arc weights and markings exceeding `TokenCount::MAX` now error with `SyncProdNetConstructionError::{ArcWeightTooLarge, MarkingTooLarge}` instead of being truncated (**Breaking**)
- Traces with more events than `TracePos::MAX` now error with `SyncProdNetConstructionError::TraceTooLong` instead of being truncated (**Breaking**)
- Alignments break ties between equally optimal alignments the same way on every run, in transition order (creation or file order). Precision values that depend on the choice among optimal alignments (e.g., `prom()` on Sepsis) change accordingly
- All search buffers reused per thread are given back once a later search needs far less, so one large trace does not keep that memory for the rest of the run
- Added escaping-edges precision (`conformance::case_centric::precision`): `escaping_edges_precision` aligns the log, `escaping_edges_precision_from_alignments` reuses alignments (e.g., from fitness)
  - `EscapingEdgesOptions` choose between aligned paths (optionally aligning variants that ran out of memory again, see `align_variants_with_retry`) and replaying until the first deviation, and whether silent transitions are skipped or count as activities
  - Presets `prom()` and `pm4py()` follow ProM's "Check Precision using Escaping Edges" and `pm4py.precision_alignments`. `prom()` comes close to ProM (silent transitions are moved to where they are needed, while ProM's value depends on where its aligner places them). `pm4py()` gives pm4py's result, except where pm4py's search over silent transitions misses available activities
  - `exclude_dead_ends` (off by default) only counts activities after which the final marking can still be reached, so the dead ends of unsound nets are not counted as allowed behavior
  - `TraceEnds::AsActivity` (off by default) makes stopping an activity of its own, so a model that may stop where the log never does is penalized
  - Optionally returns every escaping edge for diagnostics
- Redesigned `PetriNet` (**Breaking**)
  - Places and transitions have dense ids (`PlaceID`, `TransitionID`) in creation order instead of random UUIDs, and everything (iteration, exports, alignments) follows that order
  - `PetriNet` holds only places, transitions and arcs. `AcceptingPetriNet` adds exactly one initial and one final marking, and is what discovery returns and alignments, precision and token-based replay take
  - Arcs live on their transitions: `Transition::inputs`/`outputs`, presets and postsets are lookups, and `arcs()` lists them. Arcs between the same nodes merge into one with the summed weight, and an arc of weight 0 adds nothing
  - `Marking` is a value type (places to `u32` tokens), with the token game on the net: `is_enabled`, `fire`, `enabled_transitions`
  - `NodeID` wrapper (a place or a transition) for Petri nets as graphs: `nodes`, `preset_of` and `postset_of` replace the UUID-based `preset_of`/`postset_of` and `PetriNetNodes`
  - `remove_node`, `retain_nodes` and `retain_transitions` remove nodes with their arcs; the kept nodes are renumbered in order, so the ids above a removed node drop by one. On `AcceptingPetriNet`, `remove_node` and `retain_nodes` also update the markings
  - `AcceptingPetriNet::is_in_initial_marking`/`is_in_final_marking` replace `is_in_initial_marking`/`is_in_a_final_marking`
  - Removed `create_vector_dictionary` (ids are indices, see `index()`), `get_uuid` and the `id` fields of `Place` and `Transition`. A transition's arcs are read with `inputs`/`outputs` and written only by `add_arc`
  - `ProcessTree::to_petri_net` and `From<ProcessTree>` give an `AcceptingPetriNet`; `marking_to_vector` takes the vector length instead of a dictionary
  - New JSON shape (places and transitions as lists, markings keyed by place index); JSON of earlier versions still loads
- PNML (**Breaking**)
  - `pnml::import_pnml*` return the net with its initial and final marking as options. `AcceptingPetriNet::import_pnml` requires both markings, `PetriNet::import_pnml` reads the structure only; `Importable`/`Exportable` work the same way for both types
  - A node's PNML id becomes its name, and ids follow file order, so importing a file twice gives the same net. A transition without `<name>` is labelled with its PNML id. More than one final marking, a token count that is not a number, two nodes with the same id, a reference to an unknown node, or an arc between two places or two transitions is an error
  - Export writes unique, valid PNML ids (the node names where possible, otherwise `p0`, `t0`, ...), declares the place/transition net type (`ptnet`, as it writes markings and arc weights), and the same net always gives the same file. The export functions return `PetriNetIOError`, which reports a marking with a place outside the net as `InvalidPlaceInMarking` (and then writes no file). `PNMLParseError` loses the unused `InvalidMode` and `InvalidKeyValue`
- Alignment moves carry the transition's label (**Breaking**)
- `SyncProdNetConstructionError` and `TokenBasedReplayError` lose their missing-marking variants, as the types now guarantee the markings; token-based replay reports a marked place outside the net as `TokenBasedReplayError::InvalidPlaceInMarking` (**Breaking**)
- Escaping-edges precision rejects arc weights and token counts above `TokenCount::MAX` instead of truncating them
- Token-based replay and the incidence matrices use arc weights (before, every arc counted as weight 1); `create_*_incidence_matrix` return `i64` matrices indexed by place and transition id and no longer take a dictionary (**Breaking**)
- **Breaking:** Replaced the Graphviz image export with [graphlay](https://github.com/aarkue/graphlay), which needs no locally installed binary
  - The `graphviz-export` feature and the `graphviz-rust` dependency are removed, and `visualization` takes their place (enabled by default)
  - `export_svg` on `PetriNet`, `AcceptingPetriNet` (with its markings), `ProcessTree`, `DirectlyFollowsGraph` and `PartialOrderTrace` now lays the model out with graphlay
  - `export_png` and the `image_export` modules (`export_*_image*`, `export_*_to_dot_graph`) are removed
- Added the `visualization` module (feature `visualization`): positions and SVG drawings for Petri nets, process trees, DFGs, OC-DFGs, OC-DECLARE models and partial order traces, and registered bindings for all but the partial order traces
  - Process trees come in a classic look (operator circles, straight lines) and a color-coded one (named operator pills, one color per operator type), chosen with `TreeLook`

## 0.6.2
- `stream_ocel_file_to_duckdb` / `_with` infer the format the way `OCEL` does and accept every format it imports from, bundles included
  - `.json`/`.xml` (and `.gz`), bundles, and a `DuckDB` source in the consolidated schema stream into the database, holding no log in memory
  - `.sqlite`, `.ocel.csv` and a `.duckdb` in the OCEL 2.0 per-type layout are materialized first (no streaming support yet)
  - Importing onto the source's own path is now an error instead of deleting the source
- `Container::read_into` reads a bundle into any `AppendableOCEL` sink, skipping the intermediate `OCEL`. `Container::read` and `SlimLinkedOCEL`'s bundle import both go through it
- A bundle manifest path pointing outside the container is rejected before any table is read
- `OCEL`'s format inference recognizes `.sqlite3` as SQLite, alongside `.sqlite` and `.db`

## 0.6.1
- **Breaking:** Removed `KuzuDB` export (the `kuzudb` feature, the `core::event_data::object_centric::graph_db` module and the `ocel_kuzudb_export` example). Kuzu is no longer maintained upstream.
- Added extraction blueprints (`extraction-blueprint`): A declarative model for building an OCEL from relational data, with a row executor, a SQL-view compiler, a validator, and in-memory and `DuckDB` sinks
  - Feature `extraction-dbcon` adds a `RowProvider` over SQLite, CSV and Parquet via `dbcon`, all readable from bytes and buildable for `wasm32`
  - Feature `extraction-dbcon-postgres` adds PostgreSQL, which needs `sqlx` and is native-only
- Added the OCEL 2.0 bundled CSV/Parquet format (`ocel-bundle`, plus `ocel-bundle-parquet` for Parquet storage): An `.ocel.zip` archive or a directory with the same layout, read and written through `Importable`/`Exportable` like any other OCEL format
  - `Exportable::export_to_path_as` writes to a path in an explicitly named format, for paths that cannot carry one (e.g., a directory)
  - Snappy-compressed Parquet is now readable, which is what most other Parquet writers emit by default
- OCEL 2.0 CSV tweaks
  - An event attribute column is written under its plain name; the `ea:` prefix is no longer produced on export, and any column that is not `id`, `activity`, `timestamp` or `ot:<X>` is an event attribute. Import still strips a leading `ea:` so older files read unchanged
  - An object id or qualifier containing `/`, `#`, `{` or `\` is escaped with a backslash on export and read back on import
  - An `ot:<X>` header names the object type exactly, and an event attribute value is kept as written; both were trimmed before
  - A value is only read as a number or an instant when its text is the canonical spelling of one; `007`, `+7`, `1e3`, an integer too large for `i64`, and a timestamp with no timezone stay strings
  - `strict` rejects an o2o row whose source object has no declared type; the check sat inside a `verbose` branch, so `strict` alone dropped the row silently
- Added the `ocel_dataset_crosscheck` example: walks a directory of dataset folders, each holding the log it started from under `source/` and its re-exports (`.ocel.zip`, `.ocel.csv`, `.json`, `.xml`, `.sqlite`) beside it, reads every re-export back and reports where it disagrees with the source, separating differences in values from ones only in the attribute variant or recorded time
- `ocel_sql` exposes the `DuckDB` consolidated schema: `DuckDbLinkedOCEL`, `stream_ocel_file_to_duckdb` / `_with`, `DuckDbImportOptions`, and `read_consolidated_ocel_from_duckdb_path` / `read_consolidated_slim_ocel_from_duckdb_path`
- `SqlOcelImportOptions` with `import_ocel_sqlite_from_con_with_options` / `import_ocel_sqlite_from_path_with_options` / `import_ocel_duckdb_from_con_with_options`
- An object-type table without its `ocel_changed_field` column is now read as initial state (configurable with `allow_missing_changed_field: false`)
- `StreamImportOCEL` streams a reader or a path into any `AppendableOCEL` and finalizes it; `is_streaming_format` reports which formats it covers
- `TabularSource` holds the bytes of a tabular file (`SQLite`, CSV, Parquet, workbook) in the registry, so a binding can name a dropped file by id where there is no filesystem
- `OCELAttributeType::coalesce`: the narrowest type covering two others, used by CSV type inference
- `OCELEvent.time` accepts the non-RFC3339 timestamp formats the rest of the crate parses
- `AggregatedEventTimestamps::bin_width_ms` (new field) makes the bin width explicit: the spacing of the bin centers, and `0` when there are no bins; before it was implicit and a caller re-deriving it could disagree with the centers (**Breaking**)

- Fixed a regression in OC-DECLARE discovery/conformance runtime performance:
  - Now builds and construct a reverse-E2O index grouped by event type
  - OC-DECLARE internals are no longer public; their arguments could only be produced by other internals (**Breaking**)
    - `conformance::oc_declare::{get_evs_with_objs_perf, get_for_ev_perf, get_for_all_evs_perf, get_for_all_evs_perf_thresh}`
    - `discovery::object_centric::oc_declare::{get_oi_labels, combine_constraints}`
    - Per-arc checking is unchanged: `oc_declare_conformance`, `OCDeclareArc::violation_fraction`, `OCDeclareArc::satisfies_threshold`
  - `OCDeclareArc::get_for_all_evs_perf` -> `violation_fraction`, `OCDeclareArc::get_for_all_evs_perf_thresh` -> `satisfies_threshold` (**Breaking**)
  - Fixed `ANY` object involvements counting a target event once per referenced object instead of once

## 0.6.0

- Optimal Petri net alignments (`conformance::alignments`):
  - `align_log` / `align_projection` / `align_trace` / `align_empty_trace` compute cost-optimal alignments via Dijkstra over a synchronous product net
  - `compute_fitness` derives log/trace fitness for pre-computed alignments
  - Configurable `AlignmentOptions` (`CostFunction`, `max_states`)
  - Exposed as bindings
- Generic, reusable state-space search in `utils::dijkstra_search` (`SearchProblem` trait + `search`)
- `register_binding` macro accepts slice arguments (`&[T]` extracted as `Vec<T>`)
- New `ReadableOCEL` and `AppendableOCEL` traits; OCEL exporters (CSV, XML, SQL, JSON) and the JSON importer are generic over them
- `SlimLinkedOCEL` implements `AppendableOCEL`, with auto-declare types, auto-grow attributes, and value coercion via new `OCELAttributeValue::try_coerce_to`; misordered streams are buffered and resolved on `finalize`
- `import_ocel_json_into` and `import_ocel_xml_into` stream JSON / XML directly into any `AppendableOCEL` (e.g., `SlimLinkedOCEL`) without materializing an `OCEL` first; `SlimLinkedOCEL::import_from_*` uses the streaming paths automatically
- `ReadableOCEL::iter_events_of_type` / `iter_objects_of_type` (default filters; `SlimLinkedOCEL` overrides via per-type indices, avoiding a full scan per type in SQL export)
- New `OCELAttributeType::as_type_str` returns `&'static str` (cheaper than `to_type_string`)
- `EventIndex` / `ObjectIndex` are now `u32`-backed; `into_inner` returns `u32` (**Breaking**)
- `SlimLinkedOCEL`, `SlimOCELEvent`, `SlimOCELObject` no longer derive `Deserialize` (still serialize); construct via `Importable` or `AppendableOCEL` (**Breaking**)
- `SlimOCELEvent::relationships` and `SlimOCELObject::relationships` are now `Vec<(QualifierIdx, ObjectIndex)>` (was `Vec<(String, ObjectIndex)>`); resolve qualifier strings via `SlimLinkedOCEL::qualifier_str` (**Breaking**)
- CSV exporter streams rows instead of buffering all rows; tracks `(time, object_id)` pairs in a `HashSet` to skip redundant object-attribute rows
- `SlimLinkedOCEL::from_ocel` now goes through `AppendableOCEL`, so it shares attribute coercion, schema-grow, and duplicate-id detection with the streaming import paths. Behavior changes: events/objects with attribute names not in the declared type schema grow the schema (before they were silently dropped); duplicate event/object ids are skipped with a warning (before they were kept but unreachable); references to undeclared types auto-create the type (before they were dropped with warning)
- `try_json_to_ocel` returning `Result<OCEL, serde_json::Error>` added alongside the existing panicking `json_to_ocel`
- `SlimLinkedOCEL::get_o2o_rev_obs_of_obtype` / `get_e2o_rev_evs_of_evtype`: qualifier-optional reverse-relation getters filtered by type
- New public object-centric analysis functions (also exposed as bindings): per-event sojourn and synchronization times with optional `top_k` (`analysis::object_centric::oc_performance`), E2O `(event_type, object_type)` counts and `source -> target` conversion rate (`analysis::object_centric::oc_statistics`), and per-object-type directly-follows graph and activity-trace variants (`discovery::object_centric::dfg` / `variants`)
- Fix SQL export/import of floats and timestamps: floats are written as `DOUBLE PRECISION` (full f64 precision) and timestamps as naive UTC (avoids a double-applied timezone offset); import maps `DOUBLE` / `DOUBLE PRECISION` columns back to float, so round-trips no longer drop float attributes
- New direct dependency on `hashbrown` for the slim per-id hash tables

## 0.5.6

- Translate a `ProcessTree` into a `PetriNet`:
  - `ProcessTree::to_petri_net` returns a workflow net with initial and final marking set
  - `add_to_petri_net` on `Node` / `Operator` / `Leaf` for recursive insertion into an existing net
  - `From<ProcessTree> for PetriNet`
- Bump MSRV to 1.88
- Update dependencies (pin `cxx-build` to match `kuzu`'s `cxx`, refresh `cargo deny` license allowlist)
- Add Criterion + dhat benchmarks for event log import (+ DataFrame conversion)

## 0.5.5

- `SlimLinkedOCEL` and bindings:
  - `add_event` / `add_object` pad or truncate `attributes` to the declared length with a warning, instead of causing out-of-bounds panics later
  - `add_e2o` / `add_o2o` / `delete_e2o` / `delete_o2o` return `bool`; invalid indices warn and return `false` instead of panicking
  - Multiple qualifiers between the same `(event, object)` or `(from, to)` pair are kept, and reverse lookups return every qualifier
  - `fat_ev` / `fat_ob` fall back to `Null` / empty attribute on missing positional values
  - Drop `unwrap()`s on unknown type names in reverse-type lookups
  - Expanded docstrings on `slim_ocel_bindings` for the auto-generated Python API docs
- `Importable` / `Exportable` for `SlimLinkedOCEL` aligned with the `OCEL` versions
- OCEL import/export supports `.gz` for all formats
- Better defaults for OCEL 2.0 CSV export

## 0.5.4

- Add `stream_xes_bufread` function for streaming XES traces from a `BufRead` (supports gzipped input)
- Remove noisy `println!` in OCEL XML import for extended OCELs

## 0.5.3

- Parse XES version from log element
- Fix some missing unescapes in XML-based imports (XES, PNML)

## 0.5.2

- New `analysis` module with reusable analysis functions (also exposed as bindings ;))
  - `analysis::case_centric::dotted_chart`: Configurable multi-axis dotted chart generation (`DottedChartOptions`)
  - `analysis::case_centric::event_timestamp_histogram`: Aggregate event timestamps into bins grouped by activity (`EventTimestampOptions`)
  - `analysis::object_centric::object_attribute_changes`: Extract time-stamped attribute change history for an OCEL object

## 0.5.1
- Rename bindings function for SlimLinkedOCEL bindings (not breaking, as 0.5.0 bindings were not published yet)

## 0.5.0
- Fix SlimLinkedOCEL addObject function (previously did not correctly expand the reverse E2O/O2O reference array)
- Change error type of OCEL XML import to `OCELIOError` 
(**Breaking**), return error if XML does not contain any event or object types
  - Added related test (`test_xes_as_ocel_xml_import`) to ensure xes files are not correctly imported as OCEL
- Implement `Default` for SlimLinkedOCEL, add `new` function for SlimLinkedOCEL
- Expose SlimLinkedOCEL binding functions (e.g., for adding events/objects, getting relations, etc.)
- Implement From<...> for (bi-directional) conversion between (XES) `AttributeValue`s to `OCELAttributeValue`s
- Remove `Hash` derive from `OCELType` (**Breaking**)

## 0.4.4
- Fix version mismatch in macros crate

## 0.4.3
- Fix: typo in function name: `oc_declare_conformace` -> `oc_declare_conformance` (**Breaking**)

## 0.4.2
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

## 0.4.1
- Added `verbose` option to `XESImportOptions`, defaulting to true
  - Note: Technically this is a breaking change, however the recommended way to use `XESImportOptions` is non-exhaustive with default fallback:
    - e.g., ```XESImportOptions {verbose: false, ..Default::default()}```

## 0.4.0

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
