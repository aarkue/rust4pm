//! Binding wrappers for the relational-to-OCEL extraction blueprint subsystem.
//!
//! A [`Blueprint`] describes how to build an OCEL out of relational tables. It carries no
//! connection details and no schema snapshot, so it can be saved, shared and sent to a browser
//! without leaking credentials. Bindings that reach a real data source take the connection strings
//! as a separate argument.
//!
//! [`extraction_validate`] and [`extraction_compile`] are pure: they take a [`Blueprint`] and an
//! already-discovered [`ExtractionCatalog`], open no connection and read no row. The
//! `extraction_*_items` pair reads sources held in the registry as bytes, which needs
//! `ocel-sqlite` but still no connector.
//!
//! `extraction_discover_catalog`, `extraction_column_domain`, `extraction_run` and
//! `extraction_run_to_duckdb` open a real connection through `dbcon` and live in the sibling
//! `extraction_dbcon_bindings` module behind the `extraction-dbcon` feature.

use macros_process_mining::register_binding;

#[cfg(feature = "ocel-sqlite")]
use crate::bindings::{RegistryItem, StateRef};
#[cfg(all(feature = "ocel-sqlite", feature = "extraction-dbcon"))]
use crate::core::event_data::object_centric::extraction::DbconRowProvider;
use crate::core::event_data::object_centric::extraction::{
    compile, validate, Blueprint, CompiledOcel, EmissionShape, ExtractionCatalog, ExtractionReport,
    SqlDialect, ValidationError,
};
#[cfg(feature = "ocel-sqlite")]
use crate::core::event_data::object_centric::extraction::{
    extract, provider::distinct_column_values, provider::preview_rows, Catalog, RowProvider,
    SlimOcelSink, SqliteRowProvider, TablePreview,
};
#[cfg(feature = "ocel-sqlite")]
use crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;
#[cfg(feature = "ocel-sqlite")]
use crate::core::tabular_source::TabularReader;
#[cfg(feature = "ocel-sqlite")]
use std::collections::HashMap;

/// Check `blueprint` against `catalog` for problems decidable from the schema alone, such as an
/// unknown source or table, a node graph cycle, or a type-rendering rule the blueprint's own
/// `id_rendering` setting cannot satisfy.
///
/// An empty result means `blueprint` is safe to pass to [`extraction_run`],
/// [`extraction_run_to_duckdb`] or [`extraction_compile`]. All three call this internally and
/// refuse to run an invalid blueprint, so calling it first is for surfacing errors to a user
/// while they edit, not a required step before the others.
///
/// The two report a refusal differently, because they fail differently: the running bindings
/// return an error string, while [`extraction_compile`] always returns a [`CompiledOcel`] and
/// puts one entry per validation error in its `errors` array, with no relations emitted. This is
/// also the only place a blueprint's `version` is checked: the bindings deserialize a
/// [`Blueprint`] with plain serde, not `Blueprint::from_json`, so a blueprint from a newer model
/// version is caught here and nowhere else.
#[register_binding]
fn extraction_validate(blueprint: Blueprint, catalog: ExtractionCatalog) -> Vec<ValidationError> {
    validate(&blueprint, &catalog)
}

/// Compile `blueprint` into SQL views presenting the OCEL 2.0 surface over `catalog`'s tables,
/// with no connection opened and no row read.
///
/// `shape` picks the emitted layout: [`EmissionShape::PerType`] (one view per declared event/
/// object type, the layout external OCEL 2.0 tooling reads) or [`EmissionShape::Consolidated`]
/// (a single wide `events`/`objects`/... layout with the type as a column value). A mapping the
/// emitter cannot reproduce exactly is skipped and recorded in the result rather than failing the
/// whole compile.
#[register_binding]
fn extraction_compile(
    blueprint: Blueprint,
    catalog: ExtractionCatalog,
    shape: EmissionShape,
    #[bind(default)] dialect: SqlDialect,
) -> CompiledOcel {
    compile(&blueprint, &catalog, dialect, shape)
}

/// Borrow every source in `sources` as an opened [`RowProvider`], keyed the same way.
///
/// `sources` maps a blueprint's `source_id` to the registry id of a [`TabularSource`], i.e. the
/// bytes of a file someone dropped. Nothing here touches the filesystem, which is what makes it
/// the only extraction path available on `wasm32`.
///
/// Each source is opened once and cached on the item, so a second call reuses the open `SQLite`
/// database rather than copying the file in again.
///
/// Returns the lock guards: the reader is not `Sync`, so it may only be touched while its guard is
/// held. Two `source_id`s naming the same item share one guard, since locking the same mutex twice
/// would deadlock, and items are opened in registry-id order rather than in `sources` iteration
/// order, so two concurrent calls cannot take the same locks in opposite orders.
#[cfg(feature = "ocel-sqlite")]
fn open_sources<'a>(
    state: StateRef<'a>,
    sources: &HashMap<String, String>,
) -> Result<Vec<(Vec<String>, OpenedSource<'a>)>, String> {
    let mut by_item: Vec<(&str, Vec<String>)> = Vec::new();
    for (source_id, item_id) in sources {
        match by_item.iter_mut().find(|(id, _)| *id == item_id.as_str()) {
            Some((_, ids)) => ids.push(source_id.clone()),
            None => by_item.push((item_id.as_str(), vec![source_id.clone()])),
        }
    }
    by_item.sort_unstable();

    let mut out: Vec<(Vec<String>, OpenedSource<'a>)> = Vec::with_capacity(by_item.len());
    for (item_id, source_ids) in by_item {
        let named = source_ids.join(", ");
        let Some(item) = state.get(item_id) else {
            return Err(format!("no item '{item_id}' for source '{named}'"));
        };
        let RegistryItem::TabularSource(src) = item else {
            return Err(format!("item '{item_id}' is not a data source"));
        };
        let opened = match src.format() {
            "sqlite" | "sqlite3" | "db" => src
                .reader(SqliteRowProvider::from_slice)
                .map(OpenedSource::Sqlite),
            // Bytes, not a path: the route a browser takes, where a dropped file is all there
            // is. Feature-gated rather than absent, so a build without the connector still says
            // which formats it can manage.
            #[cfg(feature = "extraction-dbcon")]
            format @ ("csv" | "tsv" | "parquet" | "xlsx") => {
                let format = format.to_string();
                let name = named.clone();
                src.reader(move |bytes| {
                    DbconRowProvider::from_bytes(&name, &format, std::sync::Arc::from(bytes))
                })
                .map(OpenedSource::Dbcon)
            }
            other => Err(format!("this build cannot read '{other}' from memory")),
        };
        out.push((
            source_ids,
            opened.map_err(|e| format!("source '{named}': {e}"))?,
        ));
    }
    Ok(out)
}

/// The entry holding one `source_id`, or an error naming it.
#[cfg(feature = "ocel-sqlite")]
fn opened_for<'a, 'b>(
    opened: &'b [(Vec<String>, OpenedSource<'a>)],
    source_id: &str,
) -> Result<&'b OpenedSource<'a>, String> {
    opened
        .iter()
        .find(|(ids, _)| ids.iter().any(|id| id == source_id))
        .map(|(_, o)| o)
        .ok_or_else(|| format!("no source '{source_id}'"))
}

/// Merge `from` into `into`, keyed by `source_id` so neither source can clobber the other.
pub(super) fn merge_into(into: &mut ExtractionCatalog, from: ExtractionCatalog) {
    into.tables.extend(from.tables);
    into.domains.extend(from.domains);
    into.previews.extend(from.previews);
}

/// The message for a run whose report carries per-mapping failures, or `None` for a clean run.
///
/// A binding returning only a log handle has nowhere to put the [`ExtractionReport`], so a run
/// that dropped every row of a mapping would otherwise look like a clean success.
pub(super) fn report_error_message(report: &ExtractionReport) -> Option<String> {
    if report.errors.is_empty() {
        return None;
    }
    let listed = report
        .errors
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" | ");
    let total = report.errors.len() as u64 + report.errors_suppressed;
    let mut msg = format!("extraction reported {total} errors: {listed}");
    if report.errors_suppressed > 0 {
        msg.push_str(&format!(
            " (and {} further errors, not kept)",
            report.errors_suppressed
        ));
    }
    Some(msg)
}

/// Every source's schema, merged into one catalog under its own `source_id`.
#[cfg(feature = "ocel-sqlite")]
fn discover_all(opened: &[(Vec<String>, OpenedSource<'_>)]) -> Result<ExtractionCatalog, String> {
    let mut catalog = ExtractionCatalog::new();
    for (source_ids, reader) in opened {
        for source_id in source_ids {
            merge_into(&mut catalog, reader.catalog(source_id)?);
        }
    }
    Ok(catalog)
}

/// A source opened as whichever reader its format calls for.
#[cfg(feature = "ocel-sqlite")]
enum OpenedSource<'a> {
    Sqlite(TabularReader<'a, SqliteRowProvider>),
    /// CSV, TSV or Parquet, which need `dbcon`'s readers. `SQLite` does not go here: `dbcon`
    /// opens a `SQLite` database by path, and these sources are bytes.
    #[cfg(feature = "extraction-dbcon")]
    Dbcon(TabularReader<'a, DbconRowProvider>),
}

#[cfg(feature = "ocel-sqlite")]
impl OpenedSource<'_> {
    fn provider(&self) -> &dyn RowProvider {
        match self {
            Self::Sqlite(r) => r.get(),
            #[cfg(feature = "extraction-dbcon")]
            Self::Dbcon(r) => r.get(),
        }
    }

    fn catalog(&self, source_id: &str) -> Result<ExtractionCatalog, String> {
        match self {
            Self::Sqlite(r) => r
                .get()
                .discover_catalog(source_id)
                .map_err(|e| format!("source '{source_id}': {e}")),
            #[cfg(feature = "extraction-dbcon")]
            Self::Dbcon(r) => Ok(r.get().discover_catalog(source_id)),
        }
    }
}

/// The `source_id -> provider` map [`extract`] wants, borrowed from held guards.
#[cfg(feature = "ocel-sqlite")]
fn provider_refs<'a>(
    opened: &'a [(Vec<String>, OpenedSource<'a>)],
) -> HashMap<String, &'a dyn RowProvider> {
    opened
        .iter()
        .flat_map(|(source_ids, reader)| {
            let provider = reader.provider();
            source_ids.iter().map(move |id| (id.clone(), provider))
        })
        .collect()
}

/// Discover the schema of every source in `sources`, merged into one [`ExtractionCatalog`].
///
/// The in-memory counterpart of `extraction_discover_catalog`: that one takes connection strings
/// and needs a database connector, this one reads files already in the registry and needs only
/// `ocel-sqlite`. `sources` maps each `source_id` a blueprint names to the registry id of an
/// imported source file.
#[cfg(feature = "ocel-sqlite")]
#[register_binding(stringify_error)]
fn extraction_discover_catalog_items(
    #[bind(state)] state: StateRef<'_>,
    sources: HashMap<String, String>,
) -> Result<ExtractionCatalog, String> {
    discover_all(&open_sources(state, &sources)?)
}

/// Every distinct value of `table.column` in a source held in the registry.
///
/// The registry counterpart of `extraction_column_domain`: same answer, for a source whose bytes
/// the host already holds rather than one reachable by connection string. Without it a dropped
/// file could be extracted from but never inspected, so the editor could not offer the example
/// values a dynamic type name needs, and on wasm, where every source is byte-held, not at all.
///
/// `sources` maps source id to registry item id. Every entry other than `source_id` is ignored.
#[cfg(feature = "ocel-sqlite")]
#[register_binding(stringify_error)]
fn extraction_column_domain_items(
    #[bind(state)] state: StateRef<'_>,
    sources: HashMap<String, String>,
    source_id: String,
    table: String,
    column: String,
) -> Result<Vec<String>, String> {
    let opened = open_sources(state, &sources)?;
    let provider = opened_for(&opened, &source_id)?.provider();
    distinct_column_values(provider, &table, &column)
        .map_err(|e| format!("source '{source_id}': {e}"))
}

/// The first `limit` rows of `table` in a source held in the registry.
///
/// See [`extraction_column_domain_items`] for why this exists separately from the connection-string
/// route. Not a substitute for a domain: a preview is incomplete, so it must never be used where
/// the compiler needs a column's full set of values.
///
/// `sources` maps source id to registry item id. Every entry other than `source_id` is ignored.
#[cfg(feature = "ocel-sqlite")]
#[register_binding(stringify_error)]
fn extraction_table_preview_items(
    #[bind(state)] state: StateRef<'_>,
    sources: HashMap<String, String>,
    source_id: String,
    table: String,
    #[bind(default)] limit: Option<usize>,
) -> Result<TablePreview, String> {
    let opened = open_sources(state, &sources)?;
    let source = opened_for(&opened, &source_id)?;
    // Columns come from the source's own schema rather than from the caller, so a preview is
    // always aligned to something the catalog agrees exists. `TableSchema::columns` is a
    // `BTreeMap`, so that order is alphabetical: stable for a table, but not its declared order,
    // and `TablePreview::columns` is what says which is which.
    let catalog = source.catalog(&source_id)?;
    let schema = catalog
        .table(&source_id, &table)
        .ok_or_else(|| format!("source '{source_id}' has no table '{table}'"))?;
    let columns: Vec<&str> = schema.columns.keys().map(String::as_str).collect();
    preview_rows(source.provider(), &table, &columns, limit.unwrap_or(20))
        .map_err(|e| format!("source '{source_id}': {e}"))
}

/// Run `blueprint` against sources held in the registry, returning the resulting log.
///
/// Returns a fresh `SlimLinkedOCEL` rather than filling one given by `&mut`, because a binding
/// cannot both take a `&mut` big type and read the registry: the `&mut` borrow of the state guard
/// is live across the call, so lending the same guard out as a [`StateRef`] would not borrow-check.
///
/// The [`ExtractionReport`] (drop reasons, per-mapping counts) is not returned, because a binding
/// returns either a big type or plain data, never both. A run whose report carries errors
/// therefore fails with them rather than handing back a log that silently lost rows. Validate
/// first with [`extraction_validate`] to catch what a report would otherwise tell you.
#[cfg(feature = "ocel-sqlite")]
#[register_binding(stringify_error)]
fn extraction_run_items(
    #[bind(state)] state: StateRef<'_>,
    blueprint: Blueprint,
    sources: HashMap<String, String>,
    #[bind(default)] catalog: Option<ExtractionCatalog>,
) -> Result<SlimLinkedOCEL, String> {
    let providers = open_sources(state, &sources)?;
    let catalog = match catalog {
        Some(c) => c,
        None => discover_all(&providers)?,
    };
    let mut sink = SlimOcelSink::new();
    let report = extract(&blueprint, &catalog, &provider_refs(&providers), &mut sink)
        .map_err(|e| e.to_string())?;
    if let Some(msg) = report_error_message(&report) {
        return Err(msg);
    }
    Ok(sink.into_ocel())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bindings::{call, list_functions, AppState};
    use crate::core::event_data::object_centric::extraction::{AttributeMapping, FlatEventTable};

    /// A tiny flat-event-table blueprint reading a table `events(case_id, activity, ts)` from
    /// source `db`, built through
    /// [`Blueprint::from_flat_event_table`](crate::core::event_data::object_centric::extraction::Blueprint::from_flat_event_table)
    /// so it carries only a `source_id` string, never a connection string, matching what a real
    /// caller sends over the bindings boundary.
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

    fn flat_catalog() -> ExtractionCatalog {
        ExtractionCatalog::new().with_table(
            "db",
            crate::core::event_data::object_centric::extraction::TableSchema::new(
                "events",
                [
                    ("case_id", "TEXT", false),
                    ("activity", "TEXT", false),
                    ("ts", "TEXT", false),
                ],
            ),
        )
    }

    /// Registry-held sources answer `extraction_column_domain_items` the same way a connection
    /// string answers `extraction_column_domain`. Without this the blueprint editor could extract
    /// from a dropped file but never show what is in it, and on `wasm32`, where every source is
    /// byte-held, there would be no example values at all.
    #[cfg(feature = "ocel-sqlite")]
    #[test]
    fn a_registry_source_reports_its_column_domain_and_a_preview() {
        use crate::core::tabular_source::TabularSource;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("shop.sqlite");
        {
            let con = rusqlite::Connection::open(&path).expect("open");
            con.execute_batch(
                "CREATE TABLE orders (kind TEXT, amount TEXT);
                 INSERT INTO orders VALUES ('web', '10'), ('phone', '20'), ('web', '30');",
            )
            .expect("seed");
        }
        let state = AppState::default();
        state.add(
            "src1",
            RegistryItem::TabularSource(TabularSource::new(
                std::fs::read(&path).expect("read"),
                "sqlite",
            )),
        );

        let domain = list_functions()
            .into_iter()
            .find(|b| b.name.ends_with("extraction_column_domain_items"))
            .expect("domain binding is registered");
        let args = serde_json::json!({
            "sources": { "shop": "src1" }, "source_id": "shop",
            "table": "orders", "column": "kind",
        });
        let out = call(domain, &args, &state).expect("domain succeeds");
        let mut values: Vec<String> = serde_json::from_slice(&out).expect("deserializes");
        values.sort();
        // Distinct, not one entry per row: 'web' appears twice in the table.
        assert_eq!(values, vec!["phone".to_string(), "web".to_string()]);

        let preview = list_functions()
            .into_iter()
            .find(|b| b.name.ends_with("extraction_table_preview_items"))
            .expect("preview binding is registered");
        let args = serde_json::json!({
            "sources": { "shop": "src1" }, "source_id": "shop", "table": "orders", "limit": 2,
        });
        let out = call(preview, &args, &state).expect("preview succeeds");
        let preview: TablePreview = serde_json::from_slice(&out).expect("deserializes");
        // Alphabetical, because the schema keys columns in a `BTreeMap`. The rows are aligned to
        // exactly that, which is why the header is returned alongside them.
        assert_eq!(
            preview.columns,
            vec!["amount".to_string(), "kind".to_string()]
        );
        assert_eq!(preview.rows.len(), 2, "limit is respected");
        assert_eq!(
            preview.rows[0],
            vec![Some("10".to_string()), Some("web".to_string())]
        );
    }

    /// The same two bindings over a CSV, which reaches them through `dbcon` rather than through
    /// `SqliteRowProvider`, the arm a dropped `.csv` or `.parquet` takes in a browser.
    #[cfg(all(feature = "ocel-sqlite", feature = "extraction-dbcon"))]
    #[test]
    fn a_registry_csv_reports_its_column_domain_too() {
        use crate::core::tabular_source::TabularSource;

        let state = AppState::default();
        state.add(
            "src1",
            RegistryItem::TabularSource(TabularSource::new(
                b"kind,amount\nweb,10\nphone,20\nweb,30\n".to_vec(),
                "csv",
            )),
        );
        let domain = list_functions()
            .into_iter()
            .find(|b| b.name.ends_with("extraction_column_domain_items"))
            .expect("domain binding is registered");
        let args = serde_json::json!({
            "sources": { "rows": "src1" }, "source_id": "rows",
            "table": "rows", "column": "kind",
        });
        let out = call(domain, &args, &state).expect("domain succeeds");
        let mut values: Vec<String> = serde_json::from_slice(&out).expect("deserializes");
        values.sort();
        assert_eq!(values, vec!["phone".to_string(), "web".to_string()]);
    }

    /// The whole in-memory path, through `call`: bytes -> registry -> catalog -> log, with no
    /// filesystem and no database connector. This is the only route available on `wasm32`.
    #[cfg(feature = "ocel-sqlite")]
    #[test]
    fn an_extraction_reads_a_source_held_in_the_registry() {
        use crate::core::tabular_source::TabularSource;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("shop.sqlite");
        {
            let con = rusqlite::Connection::open(&path).expect("open");
            con.execute_batch(
                "CREATE TABLE orders (order_id TEXT, placed_at TEXT);
                 INSERT INTO orders VALUES ('o1', '2024-01-02T03:04:05Z'),
                                           ('o2', '2024-01-03T03:04:05Z');",
            )
            .expect("seed");
        }
        let bytes = std::fs::read(&path).expect("read");

        let state = AppState::default();
        state.add(
            "src1",
            RegistryItem::TabularSource(TabularSource::new(bytes, "sqlite")),
        );

        // Discovery finds the table without ever naming a path.
        let discover = list_functions()
            .into_iter()
            .find(|b| b.name.ends_with("extraction_discover_catalog_items"))
            .expect("discover binding is registered");
        let args = serde_json::json!({ "sources": { "shop": "src1" } });
        let out = call(discover, &args, &state).expect("discovery succeeds");
        let catalog: serde_json::Value = serde_json::from_slice(&out).expect("deserializes");
        assert!(
            catalog["tables"]["shop"]["orders"].is_object(),
            "discovered the table: {catalog}"
        );

        // ..and a run over the same source produces a log handle.
        let run = list_functions()
            .into_iter()
            .find(|b| b.name.ends_with("extraction_run_items"))
            .expect("run binding is registered");
        let blueprint = serde_json::json!({
            "version": 1,
            "nodes": [{ "id": "n1", "op": { "type": "source", "source_id": "shop", "table": "orders" } }],
            "mappings": [{
                "type": "single",
                "node": "n1",
                "target": {
                    "type": "event",
                    "event_type": { "type": "constant", "value": "placed" },
                    "id": { "type": "column", "column": "order_id" },
                    "timestamp": { "type": "value", "source": { "type": "column", "column": "placed_at" } }
                }
            }]
        });
        let args = serde_json::json!({ "blueprint": blueprint, "sources": { "shop": "src1" } });
        let out = call(run, &args, &state).expect("run succeeds");
        let handle: String = serde_json::from_slice(&out).expect("a handle id");
        let items = state.items.read().expect("lock");
        let Some(RegistryItem::SlimLinkedOCEL(log)) = items.get(&handle) else {
            panic!("the run should store a log under its returned handle");
        };
        use crate::core::event_data::object_centric::linked_ocel::LinkedOCELAccess;
        assert_eq!(log.get_ev_types().count(), 1);
        assert_eq!(log.get_all_evs().count(), 2);
    }

    /// Two sources discovered together keep their own entries: the merge is keyed by `source_id`,
    /// so neither can overwrite the other's tables.
    #[cfg(feature = "ocel-sqlite")]
    #[test]
    fn two_sources_merge_into_one_catalog_without_clobbering_each_other() {
        use crate::core::event_data::object_centric::extraction::Catalog;
        use crate::core::tabular_source::TabularSource;

        let dir = tempfile::tempdir().expect("tempdir");
        let mut state = AppState::default();
        for (item, table) in [("srcA", "alpha"), ("srcB", "beta")] {
            let path = dir.path().join(format!("{item}.sqlite"));
            {
                let con = rusqlite::Connection::open(&path).expect("open");
                con.execute_batch(&format!("CREATE TABLE {table} (id TEXT);"))
                    .expect("seed");
            }
            state = {
                state.add(
                    item,
                    RegistryItem::TabularSource(TabularSource::new(
                        std::fs::read(&path).expect("read"),
                        "sqlite",
                    )),
                );
                state
            };
        }

        let discover = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_discover_catalog_items")
            .expect("registered");
        let args = serde_json::json!({ "sources": { "a": "srcA", "b": "srcB" } });
        let out = call(discover, &args, &state).expect("discovery succeeds");
        let catalog: ExtractionCatalog = serde_json::from_slice(&out).expect("deserializes");
        assert!(catalog.table("a", "alpha").is_some(), "{catalog:?}");
        assert!(catalog.table("b", "beta").is_some(), "{catalog:?}");
    }

    /// The bytes route end to end for a workbook: registered as a `TabularSource`, opened by
    /// `extraction_discover_catalog_items`, and every sheet reported as a table.
    #[cfg(all(feature = "ocel-sqlite", feature = "extraction-dbcon"))]
    #[test]
    fn a_workbook_registered_as_bytes_discovers_one_table_per_sheet() {
        use crate::core::io::Importable;
        use crate::core::tabular_source::TabularSource;

        assert!(
            <TabularSource as Importable>::known_import_formats()
                .iter()
                .any(|f| f.extension == "xlsx"),
            "xlsx must be an advertised import format or nothing can register one"
        );

        let state = AppState::default();
        state.add(
            "book",
            RegistryItem::TabularSource(TabularSource::new(minimal_xlsx(), "xlsx")),
        );
        let discover = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_discover_catalog_items")
            .expect("registered");
        let args = serde_json::json!({ "sources": { "s": "book" } });
        let out = call(discover, &args, &state).expect("the workbook opens from memory");
        let catalog: serde_json::Value = serde_json::from_slice(&out).expect("catalog is JSON");
        let tables = &catalog["tables"]["s"];
        assert!(
            tables.get("orders").is_some(),
            "expected a table per sheet, got {tables}"
        );
        assert!(
            tables["orders"]["columns"].get("id").is_some(),
            "expected the header row as columns, got {}",
            tables["orders"]["columns"]
        );
    }

    /// A one-sheet workbook (`orders`, header `id,total`, one row), as the smallest OOXML a
    /// reader accepts. Inline base64 so the test needs no filesystem.
    #[cfg(all(feature = "ocel-sqlite", feature = "extraction-dbcon"))]
    fn minimal_xlsx() -> Vec<u8> {
        const MINIMAL_XLSX_BASE64: &str = concat!(
        "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",
        "AABZBAAAAAA=",
        );
        super::super::decode_base64(MINIMAL_XLSX_BASE64).expect("the fixture is valid base64")
    }

    /// A source that cannot be opened is reported against the source id that named it, so a
    /// caller working with several of them can tell them apart.
    #[cfg(feature = "ocel-sqlite")]
    #[test]
    fn a_source_that_cannot_be_opened_is_named_in_the_error() {
        use crate::core::tabular_source::TabularSource;

        let state = AppState::default();
        // Parquet is advertised as a source format but has no in-memory reader yet.
        state.add(
            "parquet1",
            RegistryItem::TabularSource(TabularSource::new(b"PAR1".to_vec(), "parquet")),
        );
        state.add(
            "junk",
            RegistryItem::TabularSource(TabularSource::new(vec![0u8; 4096], "sqlite")),
        );
        state.add(
            "notasource",
            RegistryItem::SlimLinkedOCEL(SlimLinkedOCEL::new()),
        );

        let discover = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_discover_catalog_items")
            .expect("registered");
        let err_for = |id: &str| {
            let args = serde_json::json!({ "sources": { "s": id } });
            call(discover, &args, &state).expect_err("must not open")
        };
        assert!(
            err_for("missing").contains("no item 'missing'"),
            "{}",
            err_for("missing")
        );
        assert!(
            err_for("notasource").contains("is not a data source"),
            "{}",
            err_for("notasource")
        );
        // Whether a Parquet source can be opened at all is what `extraction-dbcon` decides. With
        // it, these four bytes are opened and rejected as malformed Parquet; without it, the
        // format itself is unreadable. Either way the source is named rather than silently
        // yielding an empty catalog.
        let parquet = err_for("parquet1");
        assert!(parquet.contains("source 's'"), "{parquet}");
        #[cfg(feature = "extraction-dbcon")]
        assert!(parquet.to_lowercase().contains("parquet file"), "{parquet}");
        #[cfg(not(feature = "extraction-dbcon"))]
        assert!(parquet.contains("cannot read 'parquet'"), "{parquet}");
        // Bytes that are not a database fail against the source, not against a table name.
        let junk = err_for("junk");
        assert!(
            junk.contains("source 's'") && junk.contains("not a SQLite database"),
            "{junk}"
        );
    }

    /// A `#[bind(state)]` argument is not a JSON argument, so it must be absent from both the
    /// schema and the required-arguments list. Leaving it in the latter tells every host (the CLI
    /// checks exactly this list) to demand an argument it is given no schema for and can never
    /// supply, which makes the binding unreachable.
    #[cfg(feature = "ocel-sqlite")]
    #[test]
    fn a_state_argument_is_not_a_required_json_argument() {
        for name in ["extraction_discover_catalog_items", "extraction_run_items"] {
            let binding = list_functions()
                .into_iter()
                .find(|b| b.name == name)
                .unwrap_or_else(|| panic!("{name} is registered"));
            let declared: Vec<String> = (binding.args)().into_iter().map(|(n, _)| n).collect();
            let required = (binding.required_args)();
            assert!(
                !declared.contains(&"state".to_string()),
                "{name} must not declare a schema for its state argument: {declared:?}"
            );
            for req in &required {
                assert!(
                    declared.contains(req),
                    "{name} requires '{req}' but declares no schema for it: {declared:?}"
                );
            }
            assert!(
                required.contains(&"sources".to_string()),
                "{name} still requires its real arguments: {required:?}"
            );
        }
    }

    #[test]
    fn extraction_validate_round_trips_through_the_registry() {
        let binding = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_validate")
            .expect("extraction_validate registered");
        let args = serde_json::json!({
            "blueprint": flat_blueprint(),
            "catalog": flat_catalog(),
        });
        let state = AppState::default();
        let bytes = call(binding, &args, &state).expect("call succeeds");
        let errors: Vec<ValidationError> =
            serde_json::from_slice(&bytes).expect("result deserializes");
        assert!(
            errors.is_empty(),
            "a valid blueprint validates clean: {errors:?}"
        );
    }

    #[test]
    fn extraction_validate_reports_an_unknown_source() {
        let binding = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_validate")
            .expect("extraction_validate registered");
        let args = serde_json::json!({
            "blueprint": flat_blueprint(),
            "catalog": ExtractionCatalog::new(),
        });
        let state = AppState::default();
        let bytes = call(binding, &args, &state).expect("call succeeds");
        let errors: Vec<ValidationError> =
            serde_json::from_slice(&bytes).expect("result deserializes");
        assert!(
            !errors.is_empty(),
            "an empty catalog cannot satisfy a blueprint reading table 'events'"
        );
    }

    #[test]
    fn extraction_compile_round_trips_through_the_registry() {
        let binding = list_functions()
            .into_iter()
            .find(|b| b.name == "extraction_compile")
            .expect("extraction_compile registered");
        let args = serde_json::json!({
            "blueprint": flat_blueprint(),
            "catalog": flat_catalog(),
            "shape": "PerType",
        });
        let state = AppState::default();
        let bytes = call(binding, &args, &state).expect("call succeeds");
        let compiled: serde_json::Value =
            serde_json::from_slice(&bytes).expect("result deserializes");
        let views = compiled
            .get("views")
            .and_then(|v| v.as_array())
            .expect("a 'views' array");
        assert!(!views.is_empty(), "compiling a valid blueprint emits views");
    }

    #[test]
    fn every_extraction_binding_has_non_empty_schemas() {
        // The connected bindings (`extraction_run` and friends) are behind `extraction-dbcon`
        // and covered by that module's own registry test.
        let expected = ["extraction_validate", "extraction_compile"];
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
}
