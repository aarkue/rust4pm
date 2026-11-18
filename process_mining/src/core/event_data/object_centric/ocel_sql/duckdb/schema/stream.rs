//! Public entry points: own the connection, create schema, import inside one transaction, finalize.
use std::path::Path;

use duckdb::Connection;

use crate::core::event_data::object_centric::appendable::{AppendableOCEL, StreamImportOCEL};
use crate::core::event_data::object_centric::io::OCELIOError;
use crate::core::event_data::object_centric::io::OCELImportOptions;
use crate::core::event_data::object_centric::readable::ReadableOCEL;
use crate::utils::control::Control;
use macros_process_mining::{controlled, register_binding};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::sink::DuckDbOcelSink;
use super::tables::{create_indexes, create_schema};

/// Open a new database at `db_path`, replacing an existing file only if `overwrite` is set.
pub(crate) fn open_fresh(db_path: &Path, overwrite: bool) -> Result<Connection, OCELIOError> {
    if db_path.exists() {
        if !overwrite {
            return Err(OCELIOError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "{} already exists; set `overwrite` to replace it",
                    db_path.display()
                ),
            )));
        }
        std::fs::remove_file(db_path)?;
    }
    // A crashed earlier run leaves a write-ahead log beside its file, which `DuckDB` would replay
    // into the new database.
    let wal = wal_path(db_path);
    if wal.exists() {
        std::fs::remove_file(wal)?;
    }
    Ok(Connection::open(db_path)?)
}

/// The write-ahead log `DuckDB` keeps beside `db_path`: its path with `.wal` appended.
fn wal_path(db_path: &Path) -> std::path::PathBuf {
    let mut wal = db_path.as_os_str().to_owned();
    wal.push(".wal");
    wal.into()
}

/// Remove a database [`open_fresh`] created, with its write-ahead log, after a failed import.
pub(crate) fn remove_database(db_path: &Path) {
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(wal_path(db_path));
}

/// Applies [`DuckDbWriteOptions::compression`] to `con`, before anything is written.
pub(crate) fn apply_compression(
    con: &Connection,
    options: &DuckDbWriteOptions,
) -> Result<(), duckdb::Error> {
    if !options.compression {
        con.execute_batch("PRAGMA force_compression='uncompressed';")?;
    }
    Ok(())
}

/// Options controlling how an OCEL is written into a `DuckDB` database.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct DuckDbWriteOptions {
    /// Whether `DuckDB` compresses columns. `true` (default) = smaller, faster-to-query
    /// file; `false` = ~20% faster import, larger file.
    pub compression: bool,
    /// Whether to rewrite tables clustered by key after import. `true` (default) reorders
    /// rows (events by type+time, relations by key) so similar values group, giving better
    /// compression and range scans. Drops then rebuilds indexes/PKs. `false` skips it.
    pub optimize_filesize: bool,
    /// Whether to replace an existing file at the destination. `false` (default) fails instead.
    pub overwrite: bool,
}

impl Default for DuckDbWriteOptions {
    fn default() -> Self {
        Self {
            compression: true,
            optimize_filesize: true,
            overwrite: false,
        }
    }
}

/// Run `import`, which appends into the sink and finalizes it, inside one transaction. A failed
/// import removes the new database again.
pub(super) fn run_import<F>(
    db_path: &Path,
    options: &DuckDbWriteOptions,
    import: F,
) -> Result<(), OCELIOError>
where
    F: FnOnce(&mut DuckDbOcelSink<'_>) -> Result<(), OCELIOError>,
{
    let con = open_fresh(db_path, options.overwrite)?;
    let result = fill(&con, options, import);
    drop(con);
    if result.is_err() {
        remove_database(db_path);
    }
    result
}

fn fill<F>(con: &Connection, options: &DuckDbWriteOptions, import: F) -> Result<(), OCELIOError>
where
    F: FnOnce(&mut DuckDbOcelSink<'_>) -> Result<(), OCELIOError>,
{
    apply_compression(con, options)?;
    create_schema(con)?;
    con.execute_batch("BEGIN TRANSACTION")?;
    {
        let mut sink = DuckDbOcelSink::new(con)?;
        import(&mut sink)?;
    }
    optimize_and_index(con, options)?;
    con.execute_batch("COMMIT")?;
    con.execute_batch("CHECKPOINT")?;
    con.execute_batch("VACUUM")?;
    Ok(())
}

/// Applies [`DuckDbWriteOptions::optimize_filesize`] to the filled tables, then builds the
/// secondary indexes.
pub(crate) fn optimize_and_index(
    con: &Connection,
    options: &DuckDbWriteOptions,
) -> Result<(), duckdb::Error> {
    if options.optimize_filesize {
        // Rewrite tables clustered by key for better compression + scan locality. The
        // `CREATE TABLE AS SELECT` rewrite drops indexes and primary keys; both are
        // rebuilt below (PKs here, secondary indexes in `create_indexes`).
        con.execute_batch("
            -- events: by type, then time
            CREATE TABLE events_new AS SELECT * FROM events ORDER BY ocel_type, time;
            DROP TABLE events;
            ALTER TABLE events_new RENAME TO events;
            ALTER TABLE events ADD PRIMARY KEY (id);

            -- e2o: by event, then qualifier
            CREATE TABLE e2o_new AS SELECT * FROM e2o ORDER BY event_id, qualifier;
            DROP TABLE e2o;
            ALTER TABLE e2o_new RENAME TO e2o;

            -- objects: by type
            CREATE TABLE objects_new AS SELECT * FROM objects ORDER BY ocel_type;
            DROP TABLE objects;
            ALTER TABLE objects_new RENAME TO objects;
            ALTER TABLE objects ADD PRIMARY KEY (id);

            -- object attribute changes (EAV): by attribute name, then value type
            CREATE TABLE oac_new AS SELECT * FROM object_attribute_changes ORDER BY name, value_type;
            DROP TABLE object_attribute_changes;
            ALTER TABLE oac_new RENAME TO object_attribute_changes;

            -- o2o: by qualifier
            CREATE TABLE o2o_new AS SELECT * FROM o2o ORDER BY qualifier;
            DROP TABLE o2o;
            ALTER TABLE o2o_new RENAME TO o2o;
        ")?;
    }
    // Build secondary indexes last, so the optimize rewrite above cannot drop them.
    create_indexes(con)
}

/// Write an in-memory OCEL into a new `DuckDB` database in the consolidated schema.
///
/// For writing the OCEL 2.0 standard per-type layout instead, see
/// [`OCEL::export_to_path`](crate::core::io::Exportable::export_to_path) with a `.duckdb` path.
#[register_binding(stringify_error)]
pub fn write_ocel_to_duckdb(
    ocel: &(impl ReadableOCEL + ?Sized),
    db_path: impl AsRef<Path>,
    #[bind(default)] options: &DuckDbWriteOptions,
) -> Result<(), OCELIOError> {
    run_import(db_path.as_ref(), options, |sink| {
        for et in ocel.event_types() {
            sink.declare_event_type(et.clone())?;
        }
        for ot in ocel.object_types() {
            sink.declare_object_type(ot.clone())?;
        }
        for e in ocel.iter_events() {
            let e = e.into_owned();
            sink.append_event(e.id, &e.event_type, e.time, e.attributes, e.relationships)?;
        }
        for o in ocel.iter_objects() {
            let o = o.into_owned();
            sink.append_object(o.id, &o.object_type, o.attributes, o.relationships)?;
        }
        Ok(sink.finalize()?)
    })
}

/// Stream an OCEL file into a new `DuckDB` database in the consolidated schema.
///
/// Accepts every format [`OCEL`](crate::OCEL) imports from, read as
/// [`StreamImportOCEL::stream_ocel_from_path`] does: `.json`, `.xml` (and their `.gz` variants),
/// bundles and a `.duckdb` in this schema stream into the database, the other formats are
/// materialized into an [`OCEL`](crate::OCEL) first.
///
/// `import_options` apply to reading the source. Called on a [`Control`], reading reports
/// progress and honors cancellation. A failed or cancelled import removes the new database again.
///
/// Read the result back with
/// [`import_ocel_duckdb`](crate::core::event_data::object_centric::ocel_sql::import_ocel_duckdb) or
/// by importing the `.duckdb` path, e.g. with [`SlimLinkedOCEL`]'s
/// [`Importable::import_from_path`](crate::core::io::Importable::import_from_path).
///
/// [`SlimLinkedOCEL`]: crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL
///
/// # Timestamps
///
/// Stored as `TIMESTAMPTZ` holding the UTC instant. The source's UTC offset is not
/// preserved: `2023-01-01T10:00:00+02:00` reads back as `2023-01-01T08:00:00+00:00`.
///
/// # Event-attribute columns
///
/// Columns on the wide `events` table come from the event-type declarations, and are typed
/// accordingly. An attribute that is never declared still gets a column, added on the fly
/// as `VARCHAR`; no attribute is dropped.
#[controlled]
#[register_binding(stringify_error)]
pub fn stream_ocel_file_to_duckdb(
    src_path: impl AsRef<Path>,
    db_path: impl AsRef<Path>,
    #[bind(default)] options: &DuckDbWriteOptions,
    #[bind(default)] import_options: &OCELImportOptions,
    ctl: &Control<'_>,
) -> Result<(), OCELIOError> {
    let src = src_path.as_ref();
    let db_path = db_path.as_ref();
    // The destination is recreated from scratch, so a source at the same path would be deleted
    // out from under the read.
    if let (Ok(from), Ok(to)) = (src.canonicalize(), db_path.canonicalize()) {
        if from == to {
            return Err(OCELIOError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "the source and the destination are the same file: {}",
                    db_path.display()
                ),
            )));
        }
    }
    run_import(db_path, options, |sink| {
        sink.stream_ocel_from_path(src, import_options.clone(), ctl)
    })
}

#[cfg(test)]
mod tests {
    use super::super::reader::is_consolidated_schema;
    use super::*;
    use crate::test_utils::get_test_data_path;
    use crate::{Importable, OCEL};

    /// Event and object counts in `db_path`.
    fn counts(db_path: &Path) -> (usize, usize) {
        let con = Connection::open(db_path).unwrap();
        let n_ev: i64 = con
            .query_row("SELECT count(*) FROM events", [], |r| r.get(0))
            .unwrap();
        let n_ob: i64 = con
            .query_row("SELECT count(*) FROM objects", [], |r| r.get(0))
            .unwrap();
        (n_ev as usize, n_ob as usize)
    }

    #[test]
    #[cfg(feature = "ocel-sqlite")]
    fn sqlite_stream_roundtrip_counts() {
        let src = get_test_data_path()
            .join("ocel")
            .join("order-management.sqlite");
        let reference = OCEL::import_from_path(&src).unwrap();
        let out = get_test_data_path()
            .join("export")
            .join("stream-from-sqlite.duckdb");
        let _ = std::fs::remove_file(&out);
        stream_ocel_file_to_duckdb(
            &src,
            &out,
            &DuckDbWriteOptions {
                overwrite: true,
                ..Default::default()
            },
            &Default::default(),
        )
        .unwrap();
        assert_eq!(
            counts(&out),
            (reference.events.len(), reference.objects.len())
        );
    }

    /// A source in this schema goes through `read_from_duckdb`, so it must arrive whole.
    #[test]
    fn consolidated_duckdb_source_reads_into_the_sink() {
        let src = get_test_data_path()
            .join("ocel")
            .join("order-management.json");
        let reference = OCEL::import_from_path(&src).unwrap();
        let first = get_test_data_path()
            .join("export")
            .join("consolidated-source.duckdb");
        let second = get_test_data_path()
            .join("export")
            .join("consolidated-copy.duckdb");
        let _ = std::fs::remove_file(&first);
        let _ = std::fs::remove_file(&second);
        stream_ocel_file_to_duckdb(
            &src,
            &first,
            &DuckDbWriteOptions {
                overwrite: true,
                ..Default::default()
            },
            &Default::default(),
        )
        .unwrap();

        assert!(is_consolidated_schema(&Connection::open(&first).unwrap()).unwrap());
        stream_ocel_file_to_duckdb(
            &first,
            &second,
            &DuckDbWriteOptions {
                overwrite: true,
                ..Default::default()
            },
            &Default::default(),
        )
        .unwrap();
        assert_eq!(
            counts(&second),
            (reference.events.len(), reference.objects.len())
        );

        // The import recreates the destination, so a source at that path would be gone before
        // the read.
        let err = stream_ocel_file_to_duckdb(
            &second,
            &second,
            &DuckDbWriteOptions {
                overwrite: true,
                ..Default::default()
            },
            &Default::default(),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("the same file"),
            "{err}, and {} still exists: {}",
            second.display(),
            second.exists()
        );
    }

    /// Reaching a bundle at all depends on the format inference. A directory has no extension,
    /// and the manifest's `.json` names a file that is not an OCEL JSON.
    #[test]
    #[cfg(feature = "ocel-bundle")]
    fn bundle_dir_manifest_and_archive_all_import() {
        use crate::core::event_data::object_centric::ocel_bundle::{
            export_ocel_bundle, BundleExportOptions, ContainerLayout, StorageFormat, META_FILE_NAME,
        };

        let reference = OCEL::import_from_path(
            get_test_data_path()
                .join("ocel")
                .join("order-management.json"),
        )
        .unwrap();
        let expected = (reference.events.len(), reference.objects.len());

        let storages = [
            StorageFormat::Csv,
            #[cfg(feature = "ocel-bundle-parquet")]
            StorageFormat::Parquet,
        ];
        for storage in storages {
            let work = get_test_data_path()
                .join("export")
                .join(format!("bundle-to-duckdb-{storage:?}"));
            let _ = std::fs::remove_dir_all(&work);
            let as_dir = work.join("container");
            std::fs::create_dir_all(&as_dir).unwrap();
            export_ocel_bundle(
                &reference,
                &as_dir,
                BundleExportOptions {
                    layout: ContainerLayout::Directory,
                    storage,
                },
            )
            .unwrap();
            let as_zip = work.join("container.ocel.zip");
            export_ocel_bundle(
                &reference,
                &as_zip,
                BundleExportOptions {
                    layout: ContainerLayout::Archive,
                    storage,
                },
            )
            .unwrap();

            for src in [as_dir.clone(), as_dir.join(META_FILE_NAME), as_zip] {
                let out = work.join("out.duckdb");
                let _ = std::fs::remove_file(&out);
                stream_ocel_file_to_duckdb(
                    &src,
                    &out,
                    &DuckDbWriteOptions {
                        overwrite: true,
                        ..Default::default()
                    },
                    &Default::default(),
                )
                .unwrap_or_else(|e| panic!("{storage:?} {}: {e}", src.display()));
                assert_eq!(counts(&out), expected, "{storage:?} {}", src.display());
            }
        }
    }

    #[test]
    fn json_stream_roundtrip_counts() {
        let src = get_test_data_path()
            .join("ocel")
            .join("order-management.json");
        let reference = OCEL::import_from_path(&src).unwrap();

        let out = get_test_data_path()
            .join("export")
            .join("stream-order-mgmt.duckdb");
        let _ = std::fs::remove_file(&out);
        stream_ocel_file_to_duckdb(
            &src,
            &out,
            &DuckDbWriteOptions {
                overwrite: true,
                ..Default::default()
            },
            &Default::default(),
        )
        .unwrap();

        let con = Connection::open(&out).unwrap();
        let n_ev: i64 = con
            .query_row("SELECT count(*) FROM events", [], |r| r.get(0))
            .unwrap();
        let n_ob: i64 = con
            .query_row("SELECT count(*) FROM objects", [], |r| r.get(0))
            .unwrap();
        let n_types: i64 = con
            .query_row("SELECT count(DISTINCT ocel_type) FROM events", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(n_ev as usize, reference.events.len());
        assert_eq!(n_ob as usize, reference.objects.len());
        assert_eq!(n_types as usize, reference.event_types.len());
    }

    #[test]
    fn uncompressed_option_roundtrips() {
        let src = get_test_data_path()
            .join("ocel")
            .join("order-management.json");
        let reference = OCEL::import_from_path(&src).unwrap();
        let out = get_test_data_path()
            .join("export")
            .join("stream-uncompressed.duckdb");
        let _ = std::fs::remove_file(&out);
        stream_ocel_file_to_duckdb(
            &src,
            &out,
            &DuckDbWriteOptions {
                compression: false,
                ..Default::default()
            },
            &Default::default(),
        )
        .unwrap();
        let con = Connection::open(&out).unwrap();
        let n_ev: i64 = con
            .query_row("SELECT count(*) FROM events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n_ev as usize, reference.events.len());
    }

    #[test]
    fn gz_stream_roundtrip_counts() {
        use std::io::Write;
        // Proves the `.gz` path: gzip a JSON fixture, then stream the `.json.gz` in. Format
        // (incl. the `.gz` layer) is resolved centrally by `StreamImportOCEL`.
        let src = get_test_data_path()
            .join("ocel")
            .join("order-management.json");
        let reference = OCEL::import_from_path(&src).unwrap();
        let raw = std::fs::read(&src).unwrap();

        let gz_path = get_test_data_path()
            .join("export")
            .join("order-management.json.gz");
        {
            let f = std::fs::File::create(&gz_path).unwrap();
            let mut enc = flate2::write::GzEncoder::new(f, flate2::Compression::default());
            enc.write_all(&raw).unwrap();
            enc.finish().unwrap();
        }

        let out = get_test_data_path().join("export").join("stream-gz.duckdb");
        let _ = std::fs::remove_file(&out);
        stream_ocel_file_to_duckdb(
            &gz_path,
            &out,
            &DuckDbWriteOptions {
                overwrite: true,
                ..Default::default()
            },
            &Default::default(),
        )
        .unwrap();

        let con = Connection::open(&out).unwrap();
        let n_ev: i64 = con
            .query_row("SELECT count(*) FROM events", [], |r| r.get(0))
            .unwrap();
        let n_ob: i64 = con
            .query_row("SELECT count(*) FROM objects", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n_ev as usize, reference.events.len());
        assert_eq!(n_ob as usize, reference.objects.len());
    }

    #[test]
    fn default_import_has_indexes_and_pks() {
        // The default import runs the optimize_filesize rewrite, which drops indexes and
        // PKs; run_import must rebuild all of them. Guards the D1 regression.
        let src = get_test_data_path()
            .join("ocel")
            .join("order-management.json");
        let out = get_test_data_path()
            .join("export")
            .join("stream-index-check.duckdb");
        let _ = std::fs::remove_file(&out);
        stream_ocel_file_to_duckdb(
            &src,
            &out,
            &DuckDbWriteOptions {
                overwrite: true,
                ..Default::default()
            },
            &Default::default(),
        )
        .unwrap();
        let con = Connection::open(&out).unwrap();

        let mut idx: Vec<String> = con
            .prepare("SELECT index_name FROM duckdb_indexes()")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        idx.sort();
        for expected in [
            "e2o_event",
            "e2o_object",
            "o2o_source",
            "o2o_target",
            "object_attribute_changes_id",
        ] {
            assert!(
                idx.iter().any(|i| i == expected),
                "missing index {expected}; have {idx:?}"
            );
        }

        let n_pk: i64 = con
            .query_row(
                "SELECT count(*) FROM duckdb_constraints() \
                 WHERE constraint_type = 'PRIMARY KEY' AND table_name IN ('events','objects')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n_pk, 2, "expected PKs on events + objects");
    }

    #[test]
    fn xml_stream_roundtrip_counts() {
        let src = get_test_data_path()
            .join("ocel")
            .join("order-management.xml");
        let reference = OCEL::import_from_path(&src).unwrap();

        let out = get_test_data_path()
            .join("export")
            .join("stream-order-mgmt-xml.duckdb");
        let _ = std::fs::remove_file(&out);
        stream_ocel_file_to_duckdb(
            &src,
            &out,
            &DuckDbWriteOptions {
                overwrite: true,
                ..Default::default()
            },
            &Default::default(),
        )
        .unwrap();

        let con = Connection::open(&out).unwrap();
        let n_ev: i64 = con
            .query_row("SELECT count(*) FROM events", [], |r| r.get(0))
            .unwrap();
        let n_ob: i64 = con
            .query_row("SELECT count(*) FROM objects", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n_ev as usize, reference.events.len());
        assert_eq!(n_ob as usize, reference.objects.len());
    }

    #[test]
    fn xml_stream_roundtrip_counts_ck() {
        let src = get_test_data_path()
            .join("ocel")
            .join("ContainerLogistics.xml");
        let reference = OCEL::import_from_path(&src).unwrap();

        let out = get_test_data_path()
            .join("export")
            .join("stream-container-logistics-xml.duckdb");
        let _ = std::fs::remove_file(&out);
        stream_ocel_file_to_duckdb(
            &src,
            &out,
            &DuckDbWriteOptions {
                overwrite: true,
                ..Default::default()
            },
            &Default::default(),
        )
        .unwrap();

        let con = Connection::open(&out).unwrap();
        let n_ev: i64 = con
            .query_row("SELECT count(*) FROM events", [], |r| r.get(0))
            .unwrap();
        let n_ob: i64 = con
            .query_row("SELECT count(*) FROM objects", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n_ev as usize, reference.events.len());
        assert_eq!(n_ob as usize, reference.objects.len());
    }

    #[test]
    fn json_stream_attribute_and_relationship_fidelity() {
        use super::super::value::duck_value_to_ocel;
        use crate::core::event_data::object_centric::ocel_sql::quoted_ident;

        // order-management.json has zero events with any attributes (verified against
        // the fixture); ocel2-p2p.json has events with both attributes and
        // relationships, so it exercises the round-trip this test targets.
        let src = get_test_data_path().join("ocel").join("ocel2-p2p.json");
        let reference = OCEL::import_from_path(&src).unwrap();
        let out = get_test_data_path()
            .join("export")
            .join("stream-fidelity.duckdb");
        let _ = std::fs::remove_file(&out);
        stream_ocel_file_to_duckdb(
            &src,
            &out,
            &DuckDbWriteOptions {
                overwrite: true,
                ..Default::default()
            },
            &Default::default(),
        )
        .unwrap();
        let con = Connection::open(&out).unwrap();

        // Pick one event that has at least one attribute and one relationship.
        let ev = reference
            .events
            .iter()
            .find(|e| !e.attributes.is_empty() && !e.relationships.is_empty())
            .expect("an event with attrs + rels");

        // Attribute value round-trips via the typed wide column.
        let a = &ev.attributes[0];
        let value: duckdb::types::Value = con
            .query_row(
                &format!("SELECT {} FROM events WHERE id = ?", quoted_ident(&a.name)),
                duckdb::params![ev.id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(duck_value_to_ocel(value), a.value);

        // Relationship count matches.
        let n_rel: i64 = con
            .query_row(
                "SELECT count(*) FROM e2o WHERE event_id = ?",
                duckdb::params![ev.id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n_rel as usize, ev.relationships.len());
    }

    /// Reading neither creates a missing database nor writes to an existing one.
    #[test]
    fn readers_open_read_only() {
        use crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;
        use crate::core::event_data::object_centric::ocel_sql::export_ocel_duckdb_to_path;

        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.duckdb");
        assert!(OCEL::import_from_path(&missing).is_err());
        assert!(SlimLinkedOCEL::import_from_path(&missing).is_err());
        assert!(stream_ocel_file_to_duckdb(
            &missing,
            dir.path().join("out.duckdb"),
            &DuckDbWriteOptions {
                overwrite: true,
                ..Default::default()
            },
            &Default::default()
        )
        .is_err());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

        let ocel = OCEL::import_from_path(
            get_test_data_path()
                .join("ocel")
                .join("order-management.json"),
        )
        .unwrap();
        let consolidated = dir.path().join("consolidated.duckdb");
        let per_type = dir.path().join("per-type.duckdb");
        write_ocel_to_duckdb(
            &ocel,
            &consolidated,
            &DuckDbWriteOptions {
                overwrite: true,
                ..Default::default()
            },
        )
        .unwrap();
        export_ocel_duckdb_to_path(&ocel, &per_type).unwrap();
        let snapshot = |p: &Path| {
            let modified = std::fs::metadata(p).unwrap().modified().unwrap();
            (std::fs::read(p).unwrap(), modified)
        };
        let before = [snapshot(&consolidated), snapshot(&per_type)];

        OCEL::import_from_path(&consolidated).unwrap();
        SlimLinkedOCEL::import_from_path(&consolidated).unwrap();
        OCEL::import_from_path(&per_type).unwrap();
        stream_ocel_file_to_duckdb(
            &consolidated,
            dir.path().join("copy-1.duckdb"),
            &DuckDbWriteOptions {
                overwrite: true,
                ..Default::default()
            },
            &Default::default(),
        )
        .unwrap();
        stream_ocel_file_to_duckdb(
            &per_type,
            dir.path().join("copy-2.duckdb"),
            &DuckDbWriteOptions {
                overwrite: true,
                ..Default::default()
            },
            &Default::default(),
        )
        .unwrap();

        assert!(before == [snapshot(&consolidated), snapshot(&per_type)]);
        assert!(!dir.path().join("consolidated.duckdb.wal").exists());
        assert!(!dir.path().join("per-type.duckdb.wal").exists());
    }

    #[test]
    #[cfg(feature = "bindings")]
    fn stream_binding_refuses_to_overwrite_by_default() {
        use crate::bindings::{call, list_functions, AppState};

        let src = get_test_data_path()
            .join("ocel")
            .join("order-management.json");
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.duckdb");
        std::fs::write(&out, b"keep").unwrap();
        let binding = list_functions()
            .into_iter()
            .find(|b| b.name == "stream_ocel_file_to_duckdb")
            .expect("stream_ocel_file_to_duckdb registered");
        let state = AppState::default();

        let args = serde_json::json!({ "src_path": src, "db_path": out });
        let err = call(binding, &args, &state).unwrap_err();
        assert!(err.contains("already exists"), "{err}");
        assert_eq!(std::fs::read(&out).unwrap(), b"keep");

        let args = serde_json::json!({ "src_path": src, "db_path": out, "options": { "overwrite": true } });
        call(binding, &args, &state).unwrap();
        let reference = OCEL::import_from_path(&src).unwrap();
        assert_eq!(
            counts(&out),
            (reference.events.len(), reference.objects.len())
        );
    }

    #[test]
    fn stream_reports_progress_and_a_cancel_removes_the_database() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Mutex;

        let src = get_test_data_path()
            .join("ocel")
            .join("order-management.json");
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.duckdb");

        let stages = Mutex::new(Vec::new());
        let ctl = Control::new().on_progress(|p| stages.lock().unwrap().push(p.stage));
        ctl.stream_ocel_file_to_duckdb(&src, &out, &Default::default(), &Default::default())
            .unwrap();
        drop(ctl);
        assert!(!stages.into_inner().unwrap().is_empty());

        let flag = AtomicBool::new(false);
        let ctl = Control::new()
            .on_progress(|_| flag.store(true, Ordering::Relaxed))
            .cancel_flag(&flag);
        let options = DuckDbWriteOptions {
            overwrite: true,
            ..Default::default()
        };
        let err = ctl
            .stream_ocel_file_to_duckdb(&src, &out, &options, &Default::default())
            .unwrap_err();
        assert!(matches!(err, OCELIOError::Cancelled), "{err}");
        assert!(!out.exists());
    }
}
