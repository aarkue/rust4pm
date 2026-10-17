//! `SQLite`-backed [`RowProvider`].
#![cfg(feature = "ocel-sqlite")]

use std::ops::ControlFlow;
use std::path::Path;

use chrono::{DateTime, FixedOffset, NaiveDateTime};
use rusqlite::types::ValueRef;
use rusqlite::Connection;

use super::catalog::{ExtractionCatalog, TableSchema};
use super::provider::{ProviderError, RowProvider};
use super::value::Value;

/// Streams rows out of a `SQLite` database via `rusqlite`.
///
/// One `SELECT <columns> FROM <table>` per [`RowProvider::scan`] call, read through `rusqlite`'s
/// own row iterator, never collected into a `Vec` first, so a `Source -> Filter` chain over this
/// provider streams.
pub struct SqliteRowProvider {
    con: Connection,
}

impl std::fmt::Debug for SqliteRowProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteRowProvider").finish_non_exhaustive()
    }
}

impl SqliteRowProvider {
    /// Open a `SQLite` database file.
    ///
    /// # Errors
    /// Returns the underlying `rusqlite::Error` if the file cannot be opened.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, rusqlite::Error> {
        Ok(Self {
            con: Connection::open(path)?,
        })
    }

    /// Wrap an already-open connection.
    #[must_use]
    pub fn from_connection(con: Connection) -> Self {
        Self { con }
    }

    /// Read a `SQLite` database held in memory.
    ///
    /// Needs no filesystem: the bytes go straight to `sqlite3_deserialize`, which is what makes
    /// extraction work on `wasm32` and on a browser `File`.
    ///
    /// # Errors
    /// Returns `SQLITE_NOTADB` if `bytes` do not begin with a `SQLite` file header, and the
    /// underlying `rusqlite::Error` if the handover fails. Corruption past the header surfaces on
    /// the first read, as it does for a file on disk.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, rusqlite::Error> {
        let mut con = Connection::open_in_memory()?;
        deserialize_slice(&mut con, bytes)?;
        Ok(Self { con })
    }

    /// Every table in this database, as an [`ExtractionCatalog`] under `source_id`.
    ///
    /// Views are included: a blueprint reads them exactly as it reads a table. `sqlite_*` internal
    /// tables are not.
    ///
    /// # Errors
    /// Returns the underlying `rusqlite::Error` if the schema cannot be read.
    pub fn discover_catalog(&self, source_id: &str) -> Result<ExtractionCatalog, rusqlite::Error> {
        let mut catalog = ExtractionCatalog::new();
        let mut tables = self.con.prepare(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') \
             AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )?;
        let names: Vec<String> = tables
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<Result<_, _>>()?;
        drop(tables);

        for name in names {
            // `PRAGMA table_info` takes no bind parameter, and the name comes from sqlite_master
            // rather than from a caller, so it cannot carry anything a bind would protect against.
            let mut cols = self.con.prepare(&format!(
                "PRAGMA table_info('{}')",
                name.replace('\'', "''")
            ))?;
            let columns: Vec<(String, String, bool)> = cols
                .query_map([], |r| {
                    let col: String = r.get(1)?;
                    let decl: String = r.get(2).unwrap_or_default();
                    let notnull: i64 = r.get(3)?;
                    Ok((col, decl, notnull == 0))
                })?
                .collect::<Result<_, _>>()?;
            drop(cols);
            catalog = catalog.with_table(source_id, TableSchema::new(&name, columns));
        }
        Ok(catalog)
    }
}

/// The magic every `SQLite` database file starts with, trailing NUL included.
const SQLITE_HEADER_MAGIC: &[u8] = b"SQLite format 3\0";
/// A `SQLite` file header is 100 bytes, so anything shorter cannot be a database.
const SQLITE_HEADER_LEN: usize = 100;
/// Offsets of the file-format write and read version bytes within that header.
const WRITE_VERSION_OFFSET: usize = 18;
const READ_VERSION_OFFSET: usize = 19;
/// The value those bytes carry for a write-ahead log, and for a rollback journal.
const FORMAT_WAL: u8 = 2;
const FORMAT_ROLLBACK: u8 = 1;

/// Load `data` into `con` as its `main` database, via `sqlite3_deserialize`.
fn deserialize_slice(con: &mut Connection, data: &[u8]) -> Result<(), rusqlite::Error> {
    let schema = std::ffi::CString::new("main")?;
    // Checked here rather than left to the first query. `sqlite3_deserialize` accepts any bytes
    // and only reports a bad database when something reads one, which attributes the failure to
    // whichever table happened to be scanned first instead of to the file that is not a database.
    if data.len() < SQLITE_HEADER_LEN || !data.starts_with(SQLITE_HEADER_MAGIC) {
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_NOTADB),
            Some("not a SQLite database: the file header is missing or truncated".to_string()),
        ));
    }
    let sz = i64::try_from(data.len()).map_err(|_| {
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_TOOBIG),
            Some("database is too large to deserialize".to_string()),
        )
    })?;
    // SQLite takes ownership of a buffer it may resize, so it must be one sqlite allocated.
    let buf = unsafe { rusqlite::ffi::sqlite3_malloc64(sz as u64) }.cast::<u8>();
    if buf.is_null() {
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_NOMEM),
            Some("sqlite3_malloc64 failed".to_string()),
        ));
    }
    unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf, data.len()) };
    // A deserialized database has no `-wal` sidecar to open, so SQLite answers every read of one
    // whose header claims WAL mode with SQLITE_CANTOPEN, so `from_slice` would appear to succeed
    // and then fail on the first scan. `sqlite3_deserialize`'s own documented workaround is to
    // set the file-format version bytes to 1 before handing the buffer over. The buffer is this
    // function's private copy, so the caller's bytes and the file they came from are untouched;
    // what is read is the database as of its last checkpoint, which is all that is present.
    unsafe {
        for offset in [WRITE_VERSION_OFFSET, READ_VERSION_OFFSET] {
            let byte = buf.add(offset);
            if byte.read() == FORMAT_WAL {
                byte.write(FORMAT_ROLLBACK);
            }
        }
    }
    let rc = unsafe {
        rusqlite::ffi::sqlite3_deserialize(
            con.handle(),
            schema.as_ptr(),
            buf,
            sz,
            sz,
            rusqlite::ffi::SQLITE_DESERIALIZE_RESIZEABLE
                | rusqlite::ffi::SQLITE_DESERIALIZE_FREEONCLOSE,
        )
    };
    // No `sqlite3_free(buf)` on this path: with SQLITE_DESERIALIZE_FREEONCLOSE set,
    // `sqlite3_deserialize` frees the buffer itself before returning a failure, and freeing it
    // again here would be a double free.
    if rc != rusqlite::ffi::SQLITE_OK {
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rc),
            Some("sqlite3_deserialize failed".to_string()),
        ));
    }
    Ok(())
}

impl RowProvider for SqliteRowProvider {
    fn scan(
        &self,
        table: &str,
        columns: &[&str],
        f: &mut dyn FnMut(&[Value]) -> ControlFlow<()>,
    ) -> Result<(), ProviderError> {
        let quoted_table = quote_ident(table);
        // An empty projection means "no columns, one callback per row", not "every column": see
        // `RowProvider::scan`. `SELECT 1` gives exactly that row count with nothing to read.
        let column_list = if columns.is_empty() {
            "1".to_string()
        } else {
            columns
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let sql = format!("SELECT {column_list} FROM {quoted_table}");

        let mut stmt = self.con.prepare(&sql).map_err(|e| map_err(table, &e))?;
        // SQLite stores a type per cell, so a TIMESTAMP column arrives as Text and its declared
        // type is the only thing saying what that text means. Read once, before the scan, and use
        // it to re-tag: otherwise every row pays the multi-format chrono cascade to rediscover
        // what the schema already stated.
        let declared: Vec<Option<DeclaredKind>> = stmt
            .columns()
            .iter()
            .map(|c| c.decl_type().and_then(declared_kind))
            .collect();
        let mut rows = stmt.query([]).map_err(|e| map_err(table, &e))?;

        let mut buf = vec![Value::Null; columns.len()];
        while let Some(row) = rows.next().map_err(|e| map_err(table, &e))? {
            for (i, slot) in buf.iter_mut().enumerate() {
                let value_ref = row.get_ref(i).map_err(|e| map_err(table, &e))?;
                *slot = convert(value_ref, declared[i]);
            }
            if f(&buf).is_break() {
                return Ok(());
            }
        }
        Ok(())
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// What a column's declared type says its cells mean, where the storage class alone is ambiguous.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeclaredKind {
    Timestamp,
    Boolean,
}

/// `SQLite` type affinity is by name, so this matches by name too. Only the two kinds whose
/// storage class is ambiguous are worth recovering; an INTEGER column already arrives as an
/// integer.
///
/// Matched per word, not by substring: `SQLite` accepts any declared type, so `CANDIDATE`
/// contains "date" and `RUNTIME_MS` contains "time" while neither holds an instant. Splitting on
/// non-alphanumerics still keeps `TIMESTAMP(3) WITHOUT TIME ZONE`, `TIMESTAMPTZ`, `DATETIME2` and
/// `SMALLDATETIME`.
fn declared_kind(decl: &str) -> Option<DeclaredKind> {
    let d = decl.to_ascii_lowercase();
    let words = || d.split(|c: char| !c.is_ascii_alphanumeric());
    if words()
        .any(|w| w == "date" || w == "time" || w.contains("timestamp") || w.contains("datetime"))
    {
        Some(DeclaredKind::Timestamp)
    } else if words().any(|w| w.contains("bool")) {
        Some(DeclaredKind::Boolean)
    } else {
        None
    }
}

fn convert(v: ValueRef<'_>, declared: Option<DeclaredKind>) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        // 0/1 in a BOOLEAN column is the value SQLite writes for a bool; anything else is a
        // number that happens to live there, and is left alone.
        ValueRef::Integer(i) if declared == Some(DeclaredKind::Boolean) && (i == 0 || i == 1) => {
            Value::Boolean(i == 1)
        }
        ValueRef::Integer(i) => Value::Integer(i),
        ValueRef::Real(f) => Value::Float(f),
        ValueRef::Text(t) => {
            let text = String::from_utf8_lossy(t).into_owned();
            if declared == Some(DeclaredKind::Timestamp) {
                // Parsed here once, or left as text for the timestamp cascade to try, but never
                // silently turned into a wrong instant.
                if let Some(ts) = parse_declared_timestamp(&text) {
                    return Value::Timestamp(ts);
                }
            }
            Value::Text(text)
        }
        // Not text, and lossy UTF-8 would invent characters. A blob has no place in a value the
        // model can render, so it is absent rather than corrupt.
        ValueRef::Blob(_) => Value::Null,
    }
}

/// The two spellings `SQLite` itself writes for a timestamp, plus RFC 3339. Deliberately narrow:
/// anything else stays `Text` and reaches the timestamp parser's full cascade, which is where
/// format guessing belongs.
fn parse_declared_timestamp(text: &str) -> Option<DateTime<FixedOffset>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
        return Some(dt);
    }
    let utc = FixedOffset::east_opt(0)?;
    for fmt in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(text, fmt) {
            return Some(DateTime::from_naive_utc_and_offset(naive, utc));
        }
    }
    None
}

/// Translate a `rusqlite` error into a [`ProviderError`], recognising the two cases `SQLite`'s
/// own error text names literally (`"no such table"`, `"no such column"`) and falling back to
/// [`ProviderError::Backend`] for everything else.
fn map_err(table: &str, e: &rusqlite::Error) -> ProviderError {
    let message = e.to_string();
    super::provider::sqlite_message_error(table, &message).unwrap_or(ProviderError::Backend {
        table: table.to_string(),
        message,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::extraction::Catalog;

    /// `SQLite` accepts any word at all as a declared type, so matching "date" or "time" anywhere
    /// in one claims columns that hold no instant: `CANDIDATE` and `RUNTIME_MS` are both legal
    /// declarations, and a text cell in either would have been re-tagged as a timestamp.
    #[test]
    fn a_declared_type_that_merely_spells_date_or_time_is_not_a_timestamp() {
        for decl in [
            "CANDIDATE",
            "UPDATE_COUNT",
            "RUNTIME_MS INTEGER",
            "VALIDATED",
            "MANDATE",
            "TEXT",
            "VARCHAR(45)",
        ] {
            assert_eq!(declared_kind(decl), None, "{decl} is not an instant");
        }
        for decl in [
            "TIMESTAMP",
            "timestamptz",
            "TIMESTAMP(3) WITHOUT TIME ZONE",
            "DATETIME",
            "datetime2",
            "SMALLDATETIME",
            "DATE",
            "TIME",
        ] {
            assert_eq!(
                declared_kind(decl),
                Some(DeclaredKind::Timestamp),
                "{decl} is an instant"
            );
        }
        assert_eq!(declared_kind("BOOLEAN"), Some(DeclaredKind::Boolean));
        assert_eq!(declared_kind("bool"), Some(DeclaredKind::Boolean));
    }

    /// The same rule end to end: a date-shaped string in a `CANDIDATE` column stays text.
    #[test]
    fn a_date_shaped_string_in_a_candidate_column_stays_text() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("candidate.sqlite");
        {
            let con = Connection::open(&path).expect("open");
            con.execute_batch(
                "CREATE TABLE t (c CANDIDATE); INSERT INTO t VALUES ('2024-01-02 03:04:05');",
            )
            .expect("seed");
        }
        let provider =
            SqliteRowProvider::from_slice(&std::fs::read(&path).expect("read")).expect("load");
        let mut seen = Vec::new();
        provider
            .scan("t", &["c"], &mut |row| {
                seen = row.to_vec();
                ControlFlow::Continue(())
            })
            .expect("scan");
        assert_eq!(seen[0], Value::Text("2024-01-02 03:04:05".to_string()));
    }

    /// A deserialized database has no `-wal` sidecar, so one whose header claims WAL mode is
    /// unreadable unless the header handed to `sqlite3_deserialize` says rollback journal. Every
    /// database left in WAL mode, which is the default for many applications, lands here and used
    /// to load and then fail every scan with "unable to open database file".
    #[test]
    fn a_database_left_in_wal_mode_still_reads() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wal.sqlite");
        {
            let con = Connection::open(&path).expect("open");
            con.pragma_update(None, "journal_mode", "WAL")
                .expect("switch to WAL");
            con.execute_batch("CREATE TABLE t (a TEXT); INSERT INTO t VALUES ('x'), ('y');")
                .expect("seed");
        }
        let bytes = std::fs::read(&path).expect("read");
        assert_eq!(
            (bytes[WRITE_VERSION_OFFSET], bytes[READ_VERSION_OFFSET]),
            (FORMAT_WAL, FORMAT_WAL),
            "the fixture must really be a WAL-mode file for this to test anything"
        );

        let provider = SqliteRowProvider::from_slice(&bytes).expect("load");
        assert!(
            provider
                .discover_catalog("db")
                .expect("discover")
                .table("db", "t")
                .is_some(),
            "a WAL-mode database's schema is readable"
        );
        let mut rows = 0;
        provider
            .scan("t", &["a"], &mut |_| {
                rows += 1;
                ControlFlow::Continue(())
            })
            .expect("scan");
        assert_eq!(rows, 2);
        // The caller's bytes are its own; only the private copy was rewritten.
        assert_eq!(bytes[WRITE_VERSION_OFFSET], FORMAT_WAL);
    }

    /// Bytes that are not a database are rejected where they are handed over, not on whichever
    /// table is scanned first. `sqlite3_deserialize` accepts anything, so this is checked here.
    #[test]
    fn bytes_that_are_not_a_database_are_rejected_up_front() {
        for bad in [
            Vec::new(),
            b"not a database at all".to_vec(),
            vec![0u8; 4096],
            // A valid header, truncated below the 100 bytes one occupies.
            SQLITE_HEADER_MAGIC.to_vec(),
        ] {
            let err = SqliteRowProvider::from_slice(&bad)
                .expect_err("bytes that are not a database must not load");
            assert!(
                err.to_string().contains("not a SQLite database"),
                "unhelpful error for {} bytes: {err}",
                bad.len()
            );
        }
    }

    /// `SQLite` stores a type per cell, so only the declared type says what a `TIMESTAMP` column's
    /// text means. Recovering it here is what keeps every row from re-running the format cascade.
    #[test]
    fn declared_types_recover_what_the_storage_class_loses() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("typed.sqlite");
        {
            let con = Connection::open(&path).expect("open");
            con.execute_batch(
                "CREATE TABLE t (at TIMESTAMP, flag BOOLEAN, n INTEGER, label TEXT, raw BLOB);
                 INSERT INTO t VALUES ('2024-01-02 03:04:05', 1, 7, '2024-01-02 03:04:05', x'00ff');",
            )
            .expect("seed");
        }
        let provider =
            SqliteRowProvider::from_slice(&std::fs::read(&path).expect("read")).expect("load");

        let mut seen = Vec::new();
        provider
            .scan("t", &["at", "flag", "n", "label", "raw"], &mut |row| {
                seen = row.to_vec();
                ControlFlow::Continue(())
            })
            .expect("scan");

        assert!(
            matches!(seen[0], Value::Timestamp(_)),
            "a TIMESTAMP column is an instant, not text: {:?}",
            seen[0]
        );
        assert_eq!(seen[1], Value::Boolean(true));
        assert_eq!(seen[2], Value::Integer(7));
        // Identical text, undeclared: still text, so the timestamp cascade decides later.
        assert_eq!(seen[3], Value::Text("2024-01-02 03:04:05".to_string()));
        // A blob is absent rather than lossy-decoded into invented characters.
        assert_eq!(seen[4], Value::Null);
    }

    /// A number in a BOOLEAN column that is not 0/1 is a number, not a bool.
    #[test]
    fn only_zero_and_one_read_as_boolean() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("odd.sqlite");
        {
            let con = Connection::open(&path).expect("open");
            con.execute_batch("CREATE TABLE t (flag BOOLEAN); INSERT INTO t VALUES (42);")
                .expect("seed");
        }
        let provider =
            SqliteRowProvider::from_slice(&std::fs::read(&path).expect("read")).expect("load");
        let mut seen = Vec::new();
        provider
            .scan("t", &["flag"], &mut |row| {
                seen = row.to_vec();
                ControlFlow::Continue(())
            })
            .expect("scan");
        assert_eq!(seen[0], Value::Integer(42));
    }

    #[test]
    fn a_database_held_in_memory_scans_and_discovers() {
        // Built on disk, then read as bytes: only `from_slice` is under test, not how the bytes
        // were produced.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("fixture.sqlite");
        {
            let con = Connection::open(&path).expect("open");
            con.execute_batch(
                "CREATE TABLE actor (actor_id numeric NOT NULL, first_name VARCHAR(45));
                 INSERT INTO actor VALUES (1, 'PENELOPE'), (2, 'NICK');
                 CREATE VIEW recent AS SELECT * FROM actor;",
            )
            .expect("seed");
        }
        let bytes = std::fs::read(&path).expect("read bytes");

        let provider = SqliteRowProvider::from_slice(&bytes).expect("load from bytes");

        let catalog = provider.discover_catalog("db").expect("discover");
        let actor = catalog.table("db", "actor").expect("actor table");
        assert_eq!(actor.columns["actor_id"].col_type, "numeric");
        assert!(!actor.columns["actor_id"].nullable);
        assert!(actor.columns["first_name"].nullable);
        // A view is readable exactly as a table is, so it belongs in the catalog.
        assert!(catalog.table("db", "recent").is_some());
        // Internal bookkeeping is not something a blueprint maps.
        assert!(catalog.tables["db"]
            .keys()
            .all(|t| !t.starts_with("sqlite_")));

        let mut seen = Vec::new();
        provider
            .scan("actor", &["actor_id"], &mut |row| {
                seen.push(row[0].canonical_string());
                ControlFlow::Continue(())
            })
            .expect("scan");
        // `numeric` decodes as Float; a whole one is still an identity.
        assert_eq!(seen, vec![Some("1".to_string()), Some("2".to_string())]);
    }
}
