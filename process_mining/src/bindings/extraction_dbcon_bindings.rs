//! The extraction bindings that open a real connection, behind the `extraction-dbcon` feature.
//!
//! The sibling `extraction_bindings` module holds the pure ones (`extraction_validate`,
//! `extraction_compile`) and the ones reading registry-held `SQLite` bytes, which need no
//! connector.
//!
//! Every function here takes `connections`, a map from the `source_id` a `Blueprint`'s nodes name
//! to a `dbcon` connection string (`postgres://...`, `sqlite:...`, or a `.csv` path), as a
//! separate argument. A blueprint never carries connection details, so the same blueprint can run
//! against staging and then production unedited.
//!
//! [`extraction_run`] cannot return `(SlimLinkedOCEL, ExtractionReport)` directly, since
//! `#[register_binding]` recognises a big type by matching the whole return type's name against a
//! fixed list and a tuple's rendered name never matches. It instead takes an empty
//! `SlimLinkedOCEL` handle (from `locel_new`) as a `&mut` argument and fills it.

// See `object_centric::mod`: `ExtractionError` is deliberately descriptive.
#![allow(clippy::result_large_err)]

use std::collections::HashMap;

use macros_process_mining::register_binding;

use crate::bindings::extraction_bindings::merge_into;
#[cfg(not(feature = "ocel-sqlite"))]
use crate::bindings::extraction_bindings::report_error_message;
#[cfg(not(feature = "ocel-sqlite"))]
use crate::bindings::{RegistryItem, StateRef};
use crate::core::event_data::object_centric::extraction::{
    discover_catalog, extract, Blueprint, Catalog, DbconProviderError, DbconRowProvider,
    ExtractionCatalog, ExtractionError, ExtractionReport, ExtractionSink, ExtractionTiming,
    ProviderError, RowProvider, SlimOcelSink, TablePreview,
};
use crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;
#[cfg(not(feature = "ocel-sqlite"))]
use crate::core::tabular_source::TabularReader;

/// Failure discovering a catalog, connecting to a source, or running an extraction against one.
///
/// Never crosses a bindings boundary as a typed value: `#[register_binding(stringify_error)]`
/// converts it to a plain `String`, via [`Display`](std::fmt::Display), at the call boundary.
#[derive(Debug)]
enum ExtractionRunError {
    /// A blueprint node, or a direct call, named a `source_id` with no entry in `connections`.
    UnknownSource(String),
    /// A direct call named a table the connected source does not have.
    UnknownTable { source_id: String, table: String },
    /// A query against an open connection failed.
    Provider(ProviderError),
    /// Connecting to a source, or discovering its schema, failed.
    Connect(DbconProviderError),
    /// The extraction itself failed.
    Extract(ExtractionError),
    /// Opening the `DuckDB` output file failed.
    #[cfg(feature = "ocel-duckdb")]
    Sink(crate::core::event_data::object_centric::extraction::SinkError),
}

impl std::fmt::Display for ExtractionRunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownSource(id) => write!(f, "no connection given for source '{id}'"),
            Self::UnknownTable { source_id, table } => {
                write!(f, "source '{source_id}' has no table '{table}'")
            }
            Self::Provider(e) => write!(f, "{e}"),
            Self::Connect(e) => write!(f, "{e}"),
            Self::Extract(e) => write!(f, "{e}"),
            #[cfg(feature = "ocel-duckdb")]
            Self::Sink(e) => write!(f, "{e}"),
        }
    }
}

impl From<DbconProviderError> for ExtractionRunError {
    fn from(e: DbconProviderError) -> Self {
        Self::Connect(e)
    }
}

impl From<ProviderError> for ExtractionRunError {
    fn from(e: ProviderError) -> Self {
        Self::Provider(e)
    }
}

impl From<ExtractionError> for ExtractionRunError {
    fn from(e: ExtractionError) -> Self {
        Self::Extract(e)
    }
}

/// Discover the schema of every source in `connections` and merge the results into one
/// [`ExtractionCatalog`], keyed by the same source ids.
fn discover_catalog_from_connections(
    connections: &HashMap<String, String>,
) -> Result<ExtractionCatalog, ExtractionRunError> {
    let mut catalog = ExtractionCatalog::new();
    for (source_id, connection_string) in connections {
        merge_into(
            &mut catalog,
            discover_catalog(source_id, connection_string)?,
        );
    }
    Ok(catalog)
}

/// Open a fast, non-discovering connection for every entry in `connections`, keyed the same way.
fn open_providers(
    connections: &HashMap<String, String>,
) -> Result<HashMap<String, DbconRowProvider>, ExtractionRunError> {
    let mut providers = HashMap::with_capacity(connections.len());
    for (source_id, connection_string) in connections {
        providers.insert(
            source_id.clone(),
            DbconRowProvider::connect(source_id, connection_string)?,
        );
    }
    Ok(providers)
}

/// Discover `connections`' schema, open a provider for each, and run `blueprint` into `sink`.
///
/// Shared by every connected binding below, so `extraction_run` and `extraction_run_to_duckdb`
/// differ only in which `ExtractionSink` they construct.
fn run_extraction(
    blueprint: &Blueprint,
    connections: &HashMap<String, String>,
    catalog: Option<ExtractionCatalog>,
    sink: &mut dyn ExtractionSink,
) -> Result<ExtractionReport, ExtractionRunError> {
    let started = std::time::Instant::now();
    // Discovery is a fixed cost paid before a single row is read, and an editor that has already
    // discovered a catalog to validate against is holding the very thing this would recompute.
    // Taking it as an argument turns that into a skipped phase rather than a repeated one.
    let catalog = match catalog {
        Some(c) => c,
        None => discover_catalog_from_connections(connections)?,
    };
    let providers = open_providers(connections)?;
    let discovery_ms = started.elapsed().as_millis() as u64;

    let extraction_started = std::time::Instant::now();
    let provider_refs: HashMap<String, &dyn RowProvider> = providers
        .iter()
        .map(|(source_id, provider)| (source_id.clone(), provider as &dyn RowProvider))
        .collect();
    let mut report = extract(blueprint, &catalog, &provider_refs, sink)?;
    report.timing = Some(ExtractionTiming {
        discovery_ms,
        extraction_ms: extraction_started.elapsed().as_millis() as u64,
    });
    Ok(report)
}

/// The source kinds *this build's* connector can open, as `dbcon` backend ids (`"csv"`,
/// `"parquet"`, `"xlsx"`, `"sqlite"`, `"duckdb"`, `"postgres"`). Lets a UI offer exactly the
/// kinds the running binary supports instead of hardcoding a per-build list.
///
/// Asked of `dbcon` itself rather than derived from this crate's `extraction-dbcon*` features:
/// under Cargo feature unification another crate in the graph can enable a `dbcon` backend
/// those features do not name.
#[register_binding]
fn extraction_connection_kinds() -> Vec<String> {
    dbcon::enabled_backends()
        .iter()
        .map(|s| (*s).to_string())
        .collect()
}

/// Connect to every source in `connections` and discover its schema, returning one merged
/// [`ExtractionCatalog`] a caller can `extraction_validate` or `extraction_compile` a blueprint
/// against, or edit further with `ExtractionCatalog::with_domain` entries from
/// [`extraction_column_domain`].
///
/// `connections` maps each source id a blueprint's nodes name to a `dbcon` connection string
/// (`postgres://...`, `postgresql://...`, `sqlite:...`, or a bare path ending in `.csv`).
#[register_binding(stringify_error)]
fn extraction_discover_catalog(
    connections: HashMap<String, String>,
) -> Result<ExtractionCatalog, ExtractionRunError> {
    discover_catalog_from_connections(&connections)
}

/// The distinct values `table.column` holds in the source named `source_id`, for compiling a
/// dynamic type name (one read from a column rather than written in the blueprint) to SQL. See
/// `ExtractionCatalog::with_domain`.
///
/// `connections` must contain an entry for `source_id`. Every other entry is ignored.
#[register_binding(stringify_error)]
fn extraction_column_domain(
    connections: HashMap<String, String>,
    source_id: String,
    table: String,
    column: String,
) -> Result<Vec<String>, ExtractionRunError> {
    let connection_string = connections
        .get(&source_id)
        .ok_or_else(|| ExtractionRunError::UnknownSource(source_id.clone()))?;
    let provider = DbconRowProvider::connect(&source_id, connection_string)?;
    Ok(provider.distinct_values(&table, &column)?)
}

/// The first `limit` rows of `table` in the source named `source_id`, for showing a person what
/// the data looks like while they build a blueprint against it.
///
/// Not a substitute for [`extraction_column_domain`]: a preview is incomplete, so it must never
/// be used where the compiler needs a column's full domain.
///
/// `connections` must contain an entry for `source_id`. Every other entry is ignored.
#[register_binding(stringify_error)]
fn extraction_table_preview(
    connections: HashMap<String, String>,
    source_id: String,
    table: String,
    #[bind(default)] limit: Option<usize>,
) -> Result<TablePreview, ExtractionRunError> {
    let connection_string = connections
        .get(&source_id)
        .ok_or_else(|| ExtractionRunError::UnknownSource(source_id.clone()))?;
    let provider = DbconRowProvider::connect(&source_id, connection_string)?;
    // Taking the columns from the schema keeps them in the source's declared order.
    let catalog = discover_catalog(&source_id, connection_string)?;
    let schema =
        catalog
            .table(&source_id, &table)
            .ok_or_else(|| ExtractionRunError::UnknownTable {
                source_id: source_id.clone(),
                table: table.clone(),
            })?;
    let columns: Vec<&str> = schema.columns.keys().map(String::as_str).collect();
    Ok(provider.table_preview(&table, &columns, limit.unwrap_or(DEFAULT_PREVIEW_ROWS))?)
}

const DEFAULT_PREVIEW_ROWS: usize = 5;

/// Run `blueprint` against `connections`, filling `ocel` in place and returning the
/// `ExtractionReport`.
///
/// `ocel` must be an empty log (get one from `locel_new`), since this overwrites it wholesale
/// rather than merging into whatever it already held. `connections` maps each source id
/// `blueprint`'s nodes name to a `dbcon` connection string. The blueprint itself carries no
/// connection details, so the same blueprint can run against different connections (a staging
/// database, then production) with no edit.
#[register_binding(stringify_error)]
fn extraction_run(
    ocel: &mut SlimLinkedOCEL,
    blueprint: Blueprint,
    connections: HashMap<String, String>,
    #[bind(default)] catalog: Option<ExtractionCatalog>,
) -> Result<ExtractionReport, ExtractionRunError> {
    let mut sink = SlimOcelSink::new();
    let report = run_extraction(&blueprint, &connections, catalog, &mut sink)?;
    *ocel = sink.into_ocel();
    Ok(report)
}

/// Run `blueprint` against `connections`, streaming straight to a fresh `DuckDB` file at
/// `target_path` (an existing file there is replaced) instead of holding the log in memory.
///
/// The right choice for a source too large to fit in RAM. See [`extraction_run`] for a log kept
/// as an in-process handle instead. The written file is read back with, for example,
/// `read_ocel_from_duckdb`.
#[cfg(feature = "ocel-duckdb")]
#[register_binding(stringify_error)]
fn extraction_run_to_duckdb(
    blueprint: Blueprint,
    connections: HashMap<String, String>,
    target_path: impl AsRef<std::path::Path>,
    #[bind(default)] catalog: Option<ExtractionCatalog>,
) -> Result<ExtractionReport, ExtractionRunError> {
    let mut sink =
        crate::core::event_data::object_centric::extraction::DuckDbSink::new(target_path)
            .map_err(ExtractionRunError::Sink)?;
    run_extraction(&blueprint, &connections, catalog, &mut sink)
}

/// Discover the schema of sources held in the registry as bytes, for the formats `dbcon` reads
/// from memory: CSV, TSV, Parquet and XLSX.
///
/// Behind `not(ocel-sqlite)`, since `extraction_discover_catalog_items` covers strictly more:
/// this route goes through [`DbconRowProvider::from_bytes`], which cannot read a `SQLite` file at
/// all (`dbcon` opens `SQLite` by path, and a registry item is bytes).
///
/// `sources` maps each `source_id` a blueprint names to the registry id of an imported file.
#[cfg(not(feature = "ocel-sqlite"))]
#[register_binding(stringify_error)]
fn extraction_discover_catalog_items_dbcon(
    #[bind(state)] state: StateRef<'_>,
    sources: HashMap<String, String>,
) -> Result<ExtractionCatalog, String> {
    let mut catalog = ExtractionCatalog::new();
    for (source_ids, reader) in open_items(state, &sources)? {
        for source_id in source_ids {
            merge_into(&mut catalog, reader.get().discover_catalog(&source_id));
        }
    }
    Ok(catalog)
}

/// Run `blueprint` against registry-held sources, returning the resulting log.
///
/// Returns a fresh log rather than filling one given by `&mut`, because a binding cannot both take
/// a `&mut` big type and read the registry (see `StateRef`). The `ExtractionReport` has nowhere to
/// go for the same reason, so a run whose report carries errors fails with them.
///
/// Behind `not(ocel-sqlite)` for the reason [`extraction_discover_catalog_items_dbcon`] gives.
#[cfg(not(feature = "ocel-sqlite"))]
#[register_binding(stringify_error)]
fn extraction_run_items_dbcon(
    #[bind(state)] state: StateRef<'_>,
    blueprint: Blueprint,
    sources: HashMap<String, String>,
    #[bind(default)] catalog: Option<ExtractionCatalog>,
) -> Result<SlimLinkedOCEL, String> {
    let opened = open_items(state, &sources)?;
    let catalog = match catalog {
        Some(c) => c,
        None => {
            let mut discovered = ExtractionCatalog::new();
            for (source_ids, reader) in &opened {
                for source_id in source_ids {
                    merge_into(&mut discovered, reader.get().discover_catalog(source_id));
                }
            }
            discovered
        }
    };
    let provider_refs: HashMap<String, &dyn RowProvider> = opened
        .iter()
        .flat_map(|(source_ids, reader)| {
            let provider = reader.get() as &dyn RowProvider;
            source_ids.iter().map(move |id| (id.clone(), provider))
        })
        .collect();
    let mut sink = SlimOcelSink::new();
    let report =
        extract(&blueprint, &catalog, &provider_refs, &mut sink).map_err(|e| e.to_string())?;
    if let Some(msg) = report_error_message(&report) {
        return Err(msg);
    }
    Ok(sink.into_ocel())
}

/// Borrow every source in `sources` as an opened provider, keyed the same way.
///
/// [`TabularSource::reader`](crate::core::tabular_source::TabularSource::reader) caches the
/// opened source on the item, so a second discovery or run does not reparse the whole file.
///
/// Returns the lock guards: the reader is not `Sync`, so it may only be touched while its guard is
/// held. Two `source_id`s naming the same item share one guard, since locking the same mutex twice
/// would deadlock, and items are opened in registry-id order rather than in `sources` iteration
/// order, so two concurrent calls cannot take the same locks in opposite orders.
#[cfg(not(feature = "ocel-sqlite"))]
fn open_items<'a>(
    state: StateRef<'a>,
    sources: &HashMap<String, String>,
) -> Result<Vec<(Vec<String>, TabularReader<'a, DbconRowProvider>)>, String> {
    let mut by_item: Vec<(&str, Vec<String>)> = Vec::new();
    for (source_id, item_id) in sources {
        match by_item.iter_mut().find(|(id, _)| *id == item_id.as_str()) {
            Some((_, ids)) => ids.push(source_id.clone()),
            None => by_item.push((item_id.as_str(), vec![source_id.clone()])),
        }
    }
    by_item.sort_unstable();

    let mut out = Vec::with_capacity(by_item.len());
    for (item_id, source_ids) in by_item {
        let named = source_ids.join(", ");
        let Some(item) = state.get(item_id) else {
            return Err(format!("no item '{item_id}' for source '{named}'"));
        };
        let RegistryItem::TabularSource(src) = item else {
            return Err(format!("item '{item_id}' is not a data source"));
        };
        let format = src.format().to_string();
        let reader = src
            .reader(|bytes| {
                DbconRowProvider::from_bytes(&named, &format, std::sync::Arc::from(bytes))
            })
            .map_err(|e| format!("source '{named}': {e}"))?;
        out.push((source_ids, reader));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bindings::{call, list_functions, AppState, RegistryItem};
    #[cfg(feature = "ocel-sqlite")]
    use crate::core::event_data::object_centric::extraction::{AttributeMapping, FlatEventTable};
    use crate::core::event_data::object_centric::extraction::{Catalog, TableSchema};

    /// A tiny flat-event-table blueprint reading a table `events(case_id, activity, ts)` from
    /// source `db`, built through `Blueprint::from_flat_event_table` so it carries only a
    /// `source_id` string, never a connection string, matching what a real caller sends over the
    /// bindings boundary.
    #[cfg(feature = "ocel-sqlite")]
    fn flat_blueprint() -> Blueprint {
        Blueprint::from_flat_event_table(FlatEventTable {
            source_id: "db".to_string(),
            table: "events".to_string(),
            case_id: "case_id".to_string(),
            activity: "activity".to_string(),
            timestamp: "ts".to_string(),
            case_object_type: "Case".to_string(),
            case_attributes: Vec::<AttributeMapping>::new(),
            event_attributes: Vec::<AttributeMapping>::new(),
        })
    }

    #[cfg(feature = "ocel-sqlite")]
    fn write_fixture_sqlite() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("fixture.sqlite");
        let con = rusqlite::Connection::open(&path).expect("open sqlite file");
        con.execute_batch("CREATE TABLE events (case_id TEXT, activity TEXT, ts TEXT);")
            .expect("create table");
        let rows = [
            ("A", "create", "2020-01-01T00:00:00Z"),
            ("A", "close", "2020-01-02T00:00:00Z"),
            ("B", "create", "2020-01-01T00:00:00Z"),
        ];
        for (case_id, activity, ts) in rows {
            con.execute(
                "INSERT INTO events (case_id, activity, ts) VALUES (?1, ?2, ?3)",
                rusqlite::params![case_id, activity, ts],
            )
            .expect("insert row");
        }
        drop(con);
        (dir, path)
    }

    /// Every binding this module contributes is registered, with non-empty schemas.
    #[test]
    fn every_connected_binding_has_non_empty_schemas() {
        let expected = [
            "extraction_discover_catalog",
            "extraction_column_domain",
            "extraction_run",
            #[cfg(feature = "ocel-duckdb")]
            "extraction_run_to_duckdb",
        ];
        let registered = list_functions();
        for name in expected {
            let binding = registered
                .iter()
                .find(|b| b.name == name)
                .unwrap_or_else(|| panic!("{name} is registered"));
            assert!(
                !(binding.args)().is_empty(),
                "{name} should declare at least one argument"
            );
            for (arg_name, schema) in (binding.args)() {
                assert!(
                    schema.is_object(),
                    "{name}'s argument '{arg_name}' should have a non-empty JSON schema"
                );
            }
            let return_schema = (binding.return_type)();
            assert!(
                return_schema.is_object(),
                "{name} should have a non-empty return schema"
            );
        }
    }

    /// Discover, validate and extract a real `SQLite` fixture entirely through the registry, as a
    /// Python or `TypeScript` caller does, and check the returned handle resolves to a
    /// `SlimLinkedOCEL` with the expected contents.
    ///
    /// Also pins that a blueprint carries no connection details: `bp`, built by
    /// [`flat_blueprint`], is serialized to JSON and back before use and only ever carries the
    /// source id `"db"`, with the connection string supplied separately in `connections`.
    #[cfg(feature = "ocel-sqlite")]
    #[test]
    fn discover_validate_and_extract_a_sqlite_fixture_through_the_registry() {
        let (_dir, path) = write_fixture_sqlite();
        let connection_string = format!("sqlite:{}", path.display());

        // The blueprint a caller would hold carries no connection string at all. Round-trip it
        // through JSON, as a caller relaying it between processes would.
        let bp_json = serde_json::to_value(flat_blueprint()).expect("serialize blueprint");
        assert!(
            !serde_json::to_string(&bp_json)
                .expect("stringify")
                .to_ascii_lowercase()
                .contains("sqlite:"),
            "a blueprint must carry no connection string, even after a JSON round trip"
        );

        let state = AppState::default();
        let connections = serde_json::json!({ "db": connection_string });

        let discover = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_discover_catalog")
            .expect("extraction_discover_catalog registered");
        let catalog_bytes = call(
            discover,
            &serde_json::json!({ "connections": connections }),
            &state,
        )
        .expect("discover_catalog succeeds");
        let catalog: ExtractionCatalog =
            serde_json::from_slice(&catalog_bytes).expect("catalog deserializes");
        assert!(catalog.table("db", "events").is_some());

        let validate_fn = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_validate")
            .expect("extraction_validate registered by process_mining");
        let validate_bytes = call(
            validate_fn,
            &serde_json::json!({ "blueprint": bp_json, "catalog": catalog }),
            &state,
        )
        .expect("validate succeeds");
        let errors: serde_json::Value =
            serde_json::from_slice(&validate_bytes).expect("errors deserialize");
        assert_eq!(
            errors.as_array().map(Vec::len),
            Some(0),
            "the fixture blueprint should validate: {errors:?}"
        );

        // `locel_new` first, exactly as a real caller would, to get the handle `extraction_run`
        // fills in place.
        let locel_new = list_functions()
            .into_iter()
            .find(|b| b.name == "locel_new")
            .expect("locel_new registered");
        let handle_bytes = call(locel_new, &serde_json::json!({}), &state).expect("locel_new");
        let handle: String = serde_json::from_slice(&handle_bytes).expect("handle deserializes");

        let run_binding = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_run")
            .expect("extraction_run registered");
        let run_args = serde_json::json!({
            "ocel": handle,
            "blueprint": bp_json,
            "connections": connections,
        });
        let report_bytes = call(run_binding, &run_args, &state).expect("extraction_run succeeds");
        // `ExtractionReport` derives `Serialize` but not `Deserialize` (some `ExtractionError`
        // variants carry `&'static str`, which no deserializer can manufacture), so a caller
        // reads this outbound-only value as JSON, not as the Rust struct.
        let report: serde_json::Value =
            serde_json::from_slice(&report_bytes).expect("report deserializes as JSON");
        let errors = report
            .get("errors")
            .and_then(|e| e.as_array())
            .expect("an 'errors' array");
        assert!(errors.is_empty(), "no errors expected: {errors:?}");
        let rows_read: u64 = report
            .get("per_mapping")
            .and_then(|v| v.as_array())
            .expect("a 'per_mapping' array")
            .iter()
            .map(|m| {
                m.get("rows_read")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0)
            })
            .sum();
        assert_eq!(rows_read, 3, "the fixture table has 3 rows");

        let items = state.items.read().unwrap();
        let stored = items.get(&handle).expect("handle resolves in the registry");
        let RegistryItem::SlimLinkedOCEL(locel) = stored else {
            panic!("handle resolved to {stored:?}, not a SlimLinkedOCEL");
        };
        assert_eq!(locel.get_evs_of_type("create").count(), 2);
        assert_eq!(locel.get_evs_of_type("close").count(), 1);
        assert_eq!(locel.get_obs_of_type("Case").count(), 2);
    }

    #[cfg(feature = "ocel-sqlite")]
    #[test]
    fn extraction_column_domain_reports_distinct_values() {
        let (_dir, path) = write_fixture_sqlite();
        let connection_string = format!("sqlite:{}", path.display());
        let state = AppState::default();

        let binding = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_column_domain")
            .expect("extraction_column_domain registered");
        let args = serde_json::json!({
            "connections": { "db": connection_string },
            "source_id": "db",
            "table": "events",
            "column": "activity",
        });
        let bytes = call(binding, &args, &state).expect("call succeeds");
        let mut values: Vec<String> = serde_json::from_slice(&bytes).expect("deserializes");
        values.sort();
        assert_eq!(values, vec!["close".to_string(), "create".to_string()]);
    }

    /// The functional half of `extraction_run_to_duckdb`: called through the registry, it writes a
    /// real `DuckDB` file that reads back as the log the fixture describes.
    /// `every_connected_binding_has_non_empty_schemas` above checks the binding's schema, which
    /// is satisfied by a binding that writes nothing at all.
    ///
    /// Reads the file back through
    /// [`read_consolidated_ocel_from_duckdb_path`](crate::core::event_data::object_centric::ocel_sql::read_consolidated_ocel_from_duckdb_path)
    /// rather than opening a `duckdb::Connection` here.
    #[cfg(all(feature = "ocel-duckdb", feature = "ocel-sqlite"))]
    #[test]
    fn extraction_run_to_duckdb_writes_a_readable_file() {
        use crate::core::event_data::object_centric::ocel_sql::read_consolidated_ocel_from_duckdb_path;

        let (_dir, path) = write_fixture_sqlite();
        let connection_string = format!("sqlite:{}", path.display());
        let out_dir = tempfile::tempdir().expect("tempdir");
        let out_path = out_dir.path().join("out.duckdb");
        let state = AppState::default();

        let binding = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_run_to_duckdb")
            .expect("extraction_run_to_duckdb registered");
        let args = serde_json::json!({
            "blueprint": flat_blueprint(),
            "connections": { "db": connection_string },
            "target_path": out_path.to_str().unwrap(),
        });
        let bytes = call(binding, &args, &state).expect("call succeeds");
        let report: serde_json::Value =
            serde_json::from_slice(&bytes).expect("report deserializes as JSON");
        let errors = report
            .get("errors")
            .and_then(|e| e.as_array())
            .expect("an 'errors' array");
        assert!(errors.is_empty(), "no errors expected: {errors:?}");
        assert!(
            out_path.exists(),
            "the DuckDB file should have been written"
        );

        let ocel = read_consolidated_ocel_from_duckdb_path(&out_path).expect("read duckdb back");
        assert_eq!(ocel.events.len(), 3);
        assert_eq!(ocel.objects.len(), 2);
    }

    /// Pins that `extraction_run`'s `ocel` argument is declared as a `SlimLinkedOCEL` registry
    /// reference, not a plain string: the schema shape `resolve_argument` needs to accept the
    /// live handle `locel_new` hands back, exercised end-to-end by the registry test above.
    #[test]
    fn extraction_run_declares_ocel_as_a_registry_reference() {
        let binding = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_run")
            .expect("extraction_run registered");
        let (_, schema) = (binding.args)()
            .into_iter()
            .find(|(name, _)| name == "ocel")
            .expect("an 'ocel' argument");
        assert_eq!(
            schema.get("x-registry-ref").and_then(|v| v.as_str()),
            Some("SlimLinkedOCEL")
        );
    }

    /// The `TableSchema` route is what a caller building a catalog by hand uses, rather than
    /// discovering one from a live connection.
    #[test]
    fn a_catalog_can_be_built_by_hand() {
        let catalog = ExtractionCatalog::new().with_table(
            "db",
            TableSchema::new("events", [("case_id", "TEXT", false)]),
        );
        assert!(catalog.table("db", "events").is_some());
    }
}
