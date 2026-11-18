//! The extraction bindings that open a real connection, behind the `extraction-dbcon` feature.
//!
//! The sibling `extraction_bindings` module holds the pure ones (`extraction_validate`,
//! `extraction_compile`) and the `extraction_*_items` ones, which read source bytes held in the
//! registry and open no connection.
//!
//! Every function here takes `connections`, a map from the `source_id` a `Blueprint`'s nodes name
//! to a `dbcon` connection string (`postgres://...`, `sqlite:...`, or a `.csv` path), as a
//! separate argument. A blueprint never carries connection details, so the same blueprint can run
//! against staging and then production unedited.

// See `object_centric::mod`: `ExtractionError` is deliberately descriptive.
#![allow(clippy::result_large_err)]

use std::collections::HashMap;

use macros_process_mining::register_binding;

use crate::bindings::extraction_bindings::merge_into;
use crate::core::event_data::object_centric::extraction::{
    discover_catalog, extract, Blueprint, Catalog, DbconProviderError, DbconRowProvider,
    ExtractionCatalog, ExtractionError, ExtractionReport, ExtractionSink, ExtractionTiming,
    ProviderError, RowProvider, SlimOcelSink, TablePreview,
};
use crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;
#[cfg(feature = "ocel-duckdb")]
use crate::core::event_data::object_centric::ocel_sql::DuckDbWriteOptions;

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
    /// Opening the `DuckDB` output file failed, or it exists and `overwrite` was not set.
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
    // Columns come from the source's own schema rather than from the caller, so a preview is
    // always aligned to something the catalog agrees exists. `TableSchema::columns` is a
    // `BTreeMap`, so that order is alphabetical: stable for a table, but not its declared order,
    // and `TablePreview::columns` is what says which is which.
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

/// Run `blueprint` against `connections`, storing the extracted log under `output_id` and
/// returning the `ExtractionReport`.
///
/// An item already stored under `output_id` is replaced. `connections` maps each source id
/// `blueprint`'s nodes name to a `dbcon` connection string. The blueprint itself carries no
/// connection details, so the same blueprint can run against different connections (a staging
/// database, then production) with no edit.
#[register_binding(stringify_error)]
fn extraction_run(
    blueprint: Blueprint,
    connections: HashMap<String, String>,
    #[bind(default)] catalog: Option<ExtractionCatalog>,
) -> Result<(SlimLinkedOCEL, ExtractionReport), ExtractionRunError> {
    let mut sink = SlimOcelSink::new();
    let report = run_extraction(&blueprint, &connections, catalog, &mut sink)?;
    Ok((sink.into_ocel(), report))
}

/// Run `blueprint` against `connections`, streaming straight to a new `DuckDB` file at
/// `db_path` instead of holding the log in memory.
///
/// `options` apply as in [`write_ocel_to_duckdb`]: without `overwrite`, an existing `db_path` is
/// an error, and a failed run removes the new file again.
///
/// The right choice for a source too large to fit in RAM. See [`extraction_run`] for a log kept
/// as an in-process handle instead. Read the written file back by importing its `.duckdb` path
/// as an `OCEL` or `SlimLinkedOCEL` (`Importable`, or the registry).
///
/// [`write_ocel_to_duckdb`]: crate::core::event_data::object_centric::ocel_sql::write_ocel_to_duckdb
#[cfg(feature = "ocel-duckdb")]
#[register_binding(stringify_error)]
fn extraction_run_to_duckdb(
    blueprint: Blueprint,
    connections: HashMap<String, String>,
    db_path: impl AsRef<std::path::Path>,
    #[bind(default)] catalog: Option<ExtractionCatalog>,
    #[bind(default)] options: &DuckDbWriteOptions,
) -> Result<ExtractionReport, ExtractionRunError> {
    use crate::core::event_data::object_centric::extraction::DuckDbSink;
    use crate::core::event_data::object_centric::ocel_sql::duckdb::schema::stream::remove_database;

    let db_path = db_path.as_ref();
    let mut sink = DuckDbSink::new(db_path, options).map_err(ExtractionRunError::Sink)?;
    let result = run_extraction(&blueprint, &connections, catalog, &mut sink);
    drop(sink);
    if result.is_err() {
        remove_database(db_path);
    }
    result
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
    /// Python or `TypeScript` caller does, and check the log stored under `output_id` is a
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

        let handle = "extracted";
        let run_binding = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_run")
            .expect("extraction_run registered");
        let run_args = serde_json::json!({
            "blueprint": bp_json,
            "connections": connections,
            "output_id": handle,
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
        let stored = items
            .get(handle)
            .expect("the log is stored under output_id");
        let RegistryItem::SlimLinkedOCEL(locel) = stored else {
            panic!("output_id holds {stored:?}, not a SlimLinkedOCEL");
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
    /// Reads the file back through [`OCEL::import_from_path`](crate::core::io::Importable), which
    /// detects the consolidated schema.
    #[cfg(all(feature = "ocel-duckdb", feature = "ocel-sqlite"))]
    #[test]
    fn extraction_run_to_duckdb_writes_a_readable_file() {
        use crate::core::event_data::object_centric::OCEL;
        use crate::core::io::Importable;

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
            "db_path": out_path.to_str().unwrap(),
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

        let ocel = OCEL::import_from_path(&out_path).expect("read duckdb back");
        assert_eq!(ocel.events.len(), 3);
        assert_eq!(ocel.objects.len(), 2);
    }

    #[cfg(all(feature = "ocel-duckdb", feature = "ocel-sqlite"))]
    #[test]
    fn extraction_run_to_duckdb_refuses_to_overwrite_by_default() {
        let (_dir, path) = write_fixture_sqlite();
        let out_dir = tempfile::tempdir().expect("tempdir");
        let out_path = out_dir.path().join("out.duckdb");
        std::fs::write(&out_path, b"keep").unwrap();
        let state = AppState::default();
        let binding = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_run_to_duckdb")
            .expect("extraction_run_to_duckdb registered");
        let mut args = serde_json::json!({
            "blueprint": flat_blueprint(),
            "connections": { "db": format!("sqlite:{}", path.display()) },
            "db_path": out_path.to_str().unwrap(),
        });

        let err = call(binding, &args, &state).unwrap_err();
        assert!(err.contains("already exists"), "{err}");
        assert_eq!(std::fs::read(&out_path).unwrap(), b"keep");

        args["options"] = serde_json::json!({ "overwrite": true });
        call(binding, &args, &state).expect("call succeeds");
        assert_ne!(std::fs::read(&out_path).unwrap(), b"keep");
    }

    #[cfg(all(feature = "ocel-duckdb", feature = "ocel-sqlite"))]
    #[test]
    fn extraction_run_to_duckdb_removes_the_file_after_a_failed_run() {
        let out_dir = tempfile::tempdir().expect("tempdir");
        let out_path = out_dir.path().join("out.duckdb");
        let missing = out_dir.path().join("missing.sqlite");
        let binding = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_run_to_duckdb")
            .expect("extraction_run_to_duckdb registered");
        let args = serde_json::json!({
            "blueprint": flat_blueprint(),
            "connections": { "db": format!("sqlite:{}", missing.display()) },
            "db_path": out_path.to_str().unwrap(),
        });
        call(binding, &args, &AppState::default()).unwrap_err();
        assert!(!out_path.exists());
    }

    /// The caller names the id the log is stored under, since the call returns the report.
    #[test]
    fn extraction_run_requires_output_id_for_the_log() {
        let binding = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_run")
            .expect("extraction_run registered");
        assert!((binding.required_args)().iter().any(|a| a == "output_id"));
        let (_, schema) = (binding.args)()
            .into_iter()
            .find(|(name, _)| name == "output_id")
            .expect("an 'output_id' argument");
        assert_eq!(
            schema.get("x-registry-out").and_then(|v| v.as_str()),
            Some("SlimLinkedOCEL")
        );
        assert!(!(binding.args)().iter().any(|(name, _)| name == "ocel"));
        assert!((binding.return_type)().get("x-registry-ref").is_none());
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
