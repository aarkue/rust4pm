//! Streaming [`ExtractionSink`] writing directly to a `DuckDB` file, reusing the schema and value
//! conversions from `ocel_sql::duckdb::schema`.
//!
//! Relation endpoints are not resolved eagerly: streaming to disk leaves no id index to look them
//! up in. Every [`resolve_event`](ExtractionSink::resolve_event) /
//! [`resolve_object`](ExtractionSink::resolve_object) answers [`Resolution::Deferred`], relations
//! are written unresolved, and [`finalize`](ExtractionSink::finalize) settles them with set-based
//! joins. This is safe because `e2o`/`o2o` carry no foreign key onto `events`/`objects`.
//! Under [`MissingEndpointPolicy::Create`] the object endpoints are additionally staged, since
//! that policy needs to know the type an id was named under.
//!
//! `on_missing_endpoint` behaves like in the eager sink, but is applied at finalize instead of at
//! the call site, and unresolved counts are reported in [`FinalizeReport::unresolved_endpoints`]
//! instead of per-mapping [`DropReason::UnresolvedEndpoint`](super::report::DropReason).
//!
//! Duplicate ids are not deferred: `events.id` and `objects.id` are `PRIMARY KEY`, so `DuckDB`
//! rejects repeats and they are reported as [`SinkError::DuplicateEvent`]/
//! [`SinkError::DuplicateObject`], matching the in-memory sink.
//!
//! Two divergences from the eager sink remain:
//!
//! * Attribute columns of the wide `events` table are typed from the
//!   [`declare_event_type`](ExtractionSink::declare_event_type) calls seen before the first
//!   [`add_event`](ExtractionSink::add_event). Later declarations add columns on demand, but
//!   cannot re-type a frozen one.
//! * Relation endpoints are not type-checked at add time, so under
//!   [`IdRendering::Raw`](super::blueprint::IdRendering::Raw) an id shared by two object types
//!   resolves to whichever type already holds it, where the eager sink reports a collision.
//!   [`IdRendering::TypePrefixed`](super::blueprint::IdRendering::TypePrefixed) avoids this.

use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, FixedOffset};
use duckdb::{types::Value as DuckValue, Connection, ToSql};

use crate::core::event_data::object_centric::ocel_sql::duckdb::schema::tables::{
    build_events_table, create_indexes, create_schema, event_attr_sql_type, quote_ident, T_E2O,
    T_EVENTS, T_EVENT_ATTR_META, T_O2O, T_OBJECTS, T_OBJECT_ATTR_CHANGES, T_OBJECT_ATTR_META,
};
use crate::core::event_data::object_centric::ocel_sql::duckdb::schema::value::{
    datetime_to_duck_timestamp, ocel_value_to_duck, to_sql_value,
};
use crate::core::event_data::object_centric::ocel_struct::OCELAttributeType;
use crate::core::event_data::object_centric::{OCELAttributeValue, OCELTypeAttribute};

use super::blueprint::MissingEndpointPolicy;
use super::sink::{EventRef, ExtractionSink, FinalizeReport, ObjectRef, Resolution, SinkError};

/// Staging table for every endpoint this sink was asked to resolve and deferred:
/// `(id, ocel_type, seq)`, one row per ask. `seq` is the order the asks arrived in, so
/// [`DuckDbSink::resolve_deferred`] can pick the first type an id was named under, as the eager
/// sink does, but only among the asks [`T_ENDPOINT_GATE`] does not rule out. Dropped again by
/// [`DuckDbSink::finalize`], so it never reaches a reader.
const T_DEFERRED: &str = "_extraction_deferred_objects";

/// Which [`T_DEFERRED`] asks were made for a relation row whose other endpoint might not
/// exist: `(seq, gate_id, gate_kind)`, where `gate_kind` is `'event'` for an
/// [`add_e2o`](DuckDbSink::add_e2o) and `'object'` for an [`add_o2o`](DuckDbSink::add_o2o).
///
/// This sink cannot fail [`resolve_event`](ExtractionSink::resolve_event) or
/// [`resolve_object`](ExtractionSink::resolve_object) the way an eager sink does, so it stages a
/// relation's object endpoint even on a row the eager path abandons earlier:
///
/// - `run_e2o` gives up before looking at the object endpoint when the event endpoint does not
///   resolve, so an `E2O` object ask on a row with no such event is one the eager path never
///   makes.
/// - `run_o2o` never reaches a row's targets when its source cannot be resolved or created, which
///   happens under [`MissingEndpointPolicy::Create`] whenever the source endpoint's `object_type`
///   expression yields nothing on that row.
///
/// Left in [`T_DEFERRED`]'s `arg_min` unfiltered, such an ask could win the type for an id that is
/// reachable through a different, real row, picking whichever ask merely arrived first rather than
/// the one the eager path would have made. [`DuckDbSink::resolve_deferred`] excludes an ask whose
/// gate row names an entity that neither exists nor is about to be created. See the two strata
/// there for why an `'object'` gate cannot simply test `objects`.
///
/// The gate's predicate is satisfied by any id present in `objects`, whatever type that row
/// carries, so it lets through the target ask of an `O2O` whose source id is taken by another
/// type. That is the same blind spot the module docs describe for relation endpoints, and
/// `IdRendering::TypePrefixed` removes it.
///
/// Populated by `add_e2o`/`add_o2o` reading [`DuckDbSink::last_object_ask_seq`], which
/// `resolve_object` sets to the seq it just staged. This relies on the calling contracts on
/// [`ExtractionSink::add_e2o`]/[`add_o2o`](ExtractionSink::add_o2o). An `O2O`'s source is not
/// gated: the eager path resolves it before looking at any target, so a source ask is always one
/// the eager path also makes.
const T_ENDPOINT_GATE: &str = "_extraction_deferred_endpoint_gate";

/// Unique index on `object_attribute_changes (id, name, "time")`, which is how this sink keeps the
/// first value written at an instant and silently discards a repeat, as
/// [`add_object_attribute`](ExtractionSink::add_object_attribute) requires.
///
/// An index rather than a check: reading the table back per row scans a table that grows with the
/// run, and deleting the duplicates at finalize would write a row per repeat to disk first.
/// `DuckDB` rejects the insert against its ART index in log time instead. Left in the finished
/// database, since one value per `(id, attribute, time)` is a property of a well-formed OCEL.
const I_OBJECT_ATTR_ONCE: &str = "_extraction_object_attribute_once";

/// Streaming [`ExtractionSink`] that writes directly to a `DuckDB` file in the consolidated
/// schema. See the module docs for how it resolves relation endpoints without an in-memory OCEL.
///
/// Handles it hands out are always [`EventRef::Id`]/[`ObjectRef::Id`], echoing the id it was
/// given, since it has no index to hand back instead.
///
/// Construct with [`DuckDbSink::new`], run an [`extract`](super::extract::extract) against it,
/// then call [`DuckDbSink::finalize`] before reading the file back (with
/// [`read_ocel_from_duckdb`](crate::core::event_data::object_centric::ocel_sql::read_ocel_from_duckdb),
/// for instance).
#[derive(Debug)]
pub struct DuckDbSink {
    con: Connection,
    declared_event_types: std::collections::HashSet<String>,
    declared_object_types: std::collections::HashSet<String>,
    /// What [`ExtractionSink::finalize`] applies to the endpoints this sink deferred.
    on_missing_endpoint: MissingEndpointPolicy,
    /// `finalize` is idempotent. This is how.
    finalized: bool,
    /// Event-attribute schema accumulated over every `declare_event_type` call, widened via
    /// [`OCELAttributeType::coalesce`] when two declarations disagree on a name's type.
    ev_attr_types: HashMap<String, OCELAttributeType>,
    /// `ev_attr_types` frozen into ordered, typed wide columns on the first `add_event`, then
    /// extended (typed from `ev_attr_types` when known) by any later new name.
    ev_columns: Vec<(String, OCELAttributeType)>,
    /// name -> index into `ev_columns`.
    ev_col_index: HashMap<String, usize>,
    events_created: bool,
    /// Next `seq` for [`T_DEFERRED`]: one counter, not one entry per id.
    deferred_seq: i64,
    /// The `seq` [`resolve_object`](ExtractionSink::resolve_object) most recently staged into
    /// [`T_DEFERRED`], consumed by [`add_e2o`](ExtractionSink::add_e2o) to populate
    /// [`T_ENDPOINT_GATE`]. See that constant's docs for the calling-order invariant this relies on.
    last_object_ask_seq: Option<i64>,
    /// `e2o` rows not yet appended. See [`RELATION_BUFFER`].
    e2o_buffer: Vec<(String, String, String)>,
    /// `o2o` rows not yet appended. See [`RELATION_BUFFER`].
    o2o_buffer: Vec<(String, String, String)>,
}

/// How many relation rows are held before an appender is opened for them.
///
/// Neither `e2o` nor `o2o` carries a constraint, so there is nothing for an immediate flush to
/// surface, unlike `events`/`objects`, whose primary key is how this sink reports a repeated id at
/// the call site.
const RELATION_BUFFER: usize = 4096;

fn backend(e: duckdb::Error) -> SinkError {
    SinkError::Backend(e.to_string())
}

/// Append one row through a short-lived appender and flush it, so a constraint violation surfaces
/// here rather than at some later, unrelated flush.
fn append_one(
    con: &Connection,
    table: &str,
    write: impl FnOnce(&mut duckdb::Appender<'_>) -> Result<(), duckdb::Error>,
) -> Result<(), duckdb::Error> {
    let mut ap = con.appender(table)?;
    write(&mut ap)?;
    ap.flush()
}

/// The write-ahead log `DuckDB` keeps beside a database file: the database's own path with
/// `.wal` appended, not with its extension replaced.
fn wal_sidecar(path: &Path) -> std::path::PathBuf {
    let mut wal = path.as_os_str().to_owned();
    wal.push(".wal");
    std::path::PathBuf::from(wal)
}

/// Whether a `DuckDB` error is a primary-key violation. Matched on the message: the driver
/// exposes no error code for it.
fn is_duplicate_key(e: &duckdb::Error) -> bool {
    let message = e.to_string().to_ascii_lowercase();
    message.contains("duplicate key") || message.contains("primary key")
}

impl DuckDbSink {
    /// Open a fresh `DuckDB` database at `path` (an existing file at that path is replaced) and
    /// create the consolidated schema's static tables.
    ///
    /// # Errors
    /// Returns [`SinkError::Backend`] if the existing file cannot be removed, the database cannot
    /// be opened, or schema creation fails.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, SinkError> {
        let path = path.as_ref();
        if path.exists() {
            std::fs::remove_file(path).map_err(|e| SinkError::Backend(e.to_string()))?;
        }
        // A crashed earlier run leaves a write-ahead log beside the file it belonged to, which
        // `DuckDB` would replay into this fresh database.
        let wal = wal_sidecar(path);
        if wal.exists() {
            std::fs::remove_file(&wal).map_err(|e| SinkError::Backend(e.to_string()))?;
        }
        let con = Connection::open(path).map_err(backend)?;
        create_schema(&con).map_err(backend)?;
        con.execute_batch(&format!(
            "CREATE TABLE {T_DEFERRED} (id TEXT, ocel_type TEXT, seq BIGINT)"
        ))
        .map_err(backend)?;
        con.execute_batch(&format!(
            "CREATE TABLE {T_ENDPOINT_GATE} (seq BIGINT, gate_id TEXT, gate_kind TEXT)"
        ))
        .map_err(backend)?;
        con.execute_batch(&format!(
            "CREATE UNIQUE INDEX {I_OBJECT_ATTR_ONCE} \
             ON {T_OBJECT_ATTR_CHANGES} (id, name, \"time\")"
        ))
        .map_err(backend)?;
        // Deliberately not wrapped in a transaction, however much a failed run would benefit from
        // one: `DuckDB` aborts a transaction on a constraint violation ("Current transaction is
        // aborted"), and this sink swallows two of those by design: the duplicate key that
        // reports a repeated id, and the [`I_OBJECT_ATTR_ONCE`] rejection enforcing first-wins on
        // `(id, name, time)`. Under one transaction the first static object attribute written
        // twice would fail every statement after it.
        Ok(Self {
            con,
            declared_event_types: std::collections::HashSet::new(),
            declared_object_types: std::collections::HashSet::new(),
            on_missing_endpoint: MissingEndpointPolicy::default(),
            finalized: false,
            ev_attr_types: HashMap::new(),
            ev_columns: Vec::new(),
            ev_col_index: HashMap::new(),
            events_created: false,
            deferred_seq: 0,
            last_object_ask_seq: None,
            e2o_buffer: Vec::new(),
            o2o_buffer: Vec::new(),
        })
    }

    /// Whether this run needs [`T_DEFERRED`] and [`T_ENDPOINT_GATE`] at all.
    ///
    /// [`Self::resolve_deferred`] reads them only to synthesise missing objects, so under any
    /// other policy every staged row is written and then dropped unread, costing two appender
    /// create-and-flush cycles per relation endpoint.
    fn stages_endpoints(&self) -> bool {
        self.on_missing_endpoint == MissingEndpointPolicy::Create
    }

    /// Count the rows one aggregate query returns.
    fn count(&self, sql: &str) -> Result<u64, duckdb::Error> {
        let n: i64 = self.con.query_row(sql, [], |r| r.get(0))?;
        Ok(u64::try_from(n).unwrap_or(0))
    }

    /// The `WHERE` fragment selecting `e2o`/`o2o` rows with an endpoint that does not exist.
    const UNRESOLVED_E2O: &'static str = "SELECT count(*) FROM e2o \
         WHERE event_id NOT IN (SELECT id FROM events) \
            OR object_id NOT IN (SELECT id FROM objects)";
    /// See [`Self::UNRESOLVED_E2O`].
    const UNRESOLVED_O2O: &'static str = "SELECT count(*) FROM o2o \
         WHERE source_id NOT IN (SELECT id FROM objects) \
            OR target_id NOT IN (SELECT id FROM objects)";

    /// Settle every endpoint this sink deferred, applying `on_missing_endpoint`.
    fn resolve_deferred(&mut self) -> Result<FinalizeReport, duckdb::Error> {
        let mut report = FinalizeReport::default();

        if self.on_missing_endpoint == MissingEndpointPolicy::Create {
            // One object per still-missing id, typed from the first reachable ask, matching the
            // eager path where the first ask creates the object and a later ask under another type
            // is an `IdTypeCollision` rather than a retype.
            //
            // "Reachable" matters here: this sink was asked about every endpoint a row named,
            // including ones the eager path never touches, since it defers everything and cannot
            // tell in advance. An unreachable ask must not win `arg_min` merely for having a
            // smaller `seq`, so `T_ENDPOINT_GATE` rules those asks out before a type is picked.
            // The final `WHERE` still confines creation to ids a surviving relation actually
            // names.
            //
            // The two gate kinds are tested against different sets, and in two strata, because an
            // `O2O` source is one of the objects this very statement creates:
            //
            // - `'event'` tests `events`, which this statement does not write, so it is final.
            // - `'object'` tests the post-creation object set, not `objects`: under `Create` a
            //   relation-only source is created by this `INSERT`, so testing the pre-creation
            //   `objects` would rule out the target ask of every synthesised source, and the
            //   target would then never be created and the relation deleted for want of it.
            //
            // The post-creation set is `available` below (`objects UNION creatable`), but the gate
            // cannot reference it: `available` derives from `creatable`, which derives from
            // `reachable`, which is what the gate filters, and SQL will not evaluate that cycle.
            // It is stratified instead. An `O2O`'s source ask is never gated (see
            // `T_ENDPOINT_GATE`), so a source's own creatability is settled by the event stratum
            // alone: `gate_available` (`objects` plus every id with an event-reachable typed ask)
            // answers the object gate without needing `creatable`, and the strata terminate.
            //
            // This over-approximates in one place: an id whose only typed ask belongs to another
            // relation counts as available, even though that ask might itself be gated out. The
            // eager path is order-dependent on exactly that shape, so no set-based rule matches it
            // row for row. Erring towards available keeps `Create` working when a source is itself
            // relation-only, which is what this stratification exists for.
            let created = self.con.execute(
                &format!(
                    "INSERT INTO objects (id, ocel_type) \
                     WITH event_reachable AS ( \
                         SELECT d.id AS id, d.ocel_type AS ocel_type, d.seq AS seq \
                         FROM {T_DEFERRED} d \
                         WHERE d.ocel_type IS NOT NULL \
                           AND d.seq NOT IN ( \
                               SELECT g.seq FROM {T_ENDPOINT_GATE} g \
                               WHERE g.gate_kind = 'event' \
                                 AND g.gate_id NOT IN (SELECT id FROM events) \
                           ) \
                     ), gate_available AS ( \
                         SELECT id FROM objects UNION ALL SELECT id FROM event_reachable \
                     ), reachable AS ( \
                         SELECT r.id AS id, r.ocel_type AS ocel_type, r.seq AS seq \
                         FROM event_reachable r \
                         WHERE r.seq NOT IN ( \
                               SELECT g.seq FROM {T_ENDPOINT_GATE} g \
                               WHERE g.gate_kind = 'object' \
                                 AND g.gate_id NOT IN (SELECT id FROM gate_available) \
                           ) \
                     ), creatable AS ( \
                         SELECT id, arg_min(ocel_type, seq) AS ocel_type \
                         FROM reachable \
                         WHERE id NOT IN (SELECT id FROM objects) \
                         GROUP BY id \
                     ), available AS ( \
                         SELECT id FROM objects UNION ALL SELECT id FROM creatable \
                     ) \
                     SELECT c.id, c.ocel_type FROM creatable c \
                     WHERE c.id IN (SELECT object_id FROM e2o \
                                    WHERE event_id IN (SELECT id FROM events)) \
                        OR c.id IN (SELECT source_id FROM o2o) \
                        OR c.id IN (SELECT target_id FROM o2o \
                                    WHERE source_id IN (SELECT id FROM available))"
                ),
                [],
            )?;
            report.objects_created = created as u64;
        }

        report.unresolved_endpoints =
            self.count(Self::UNRESOLVED_E2O)? + self.count(Self::UNRESOLVED_O2O)?;
        self.con.execute_batch(
            "DELETE FROM e2o WHERE event_id NOT IN (SELECT id FROM events) \
                                OR object_id NOT IN (SELECT id FROM objects); \
             DELETE FROM o2o WHERE source_id NOT IN (SELECT id FROM objects) \
                                OR target_id NOT IN (SELECT id FROM objects);",
        )?;
        report.resolved_relations =
            self.count("SELECT count(*) FROM e2o")? + self.count("SELECT count(*) FROM o2o")?;

        self.con.execute_batch(&format!(
            "DROP TABLE {T_DEFERRED}; DROP TABLE {T_ENDPOINT_GATE}"
        ))?;
        Ok(report)
    }

    fn event_id(r: &EventRef) -> Result<&str, SinkError> {
        match r {
            EventRef::Id(s) => Ok(s.as_str()),
            EventRef::Index(_) => Err(SinkError::InvalidRef),
        }
    }

    fn object_id(r: &ObjectRef) -> Result<&str, SinkError> {
        match r {
            ObjectRef::Id(s) => Ok(s.as_str()),
            ObjectRef::Index(_) => Err(SinkError::InvalidRef),
        }
    }

    /// Freeze `ev_attr_types` into `ev_columns`/`ev_col_index` and create the wide `events`
    /// table. Idempotent, and called on the first `add_event` and, as a safety net for an
    /// event-less run, from `finalize`.
    fn ensure_events_created(&mut self) -> Result<(), duckdb::Error> {
        if self.events_created {
            return Ok(());
        }
        let mut names: Vec<String> = self.ev_attr_types.keys().cloned().collect();
        names.sort();
        self.ev_columns = names
            .into_iter()
            .map(|n| {
                let ty = self.ev_attr_types[&n];
                (n, ty)
            })
            .collect();
        self.ev_col_index = self
            .ev_columns
            .iter()
            .enumerate()
            .map(|(i, (n, _))| (n.clone(), i))
            .collect();
        self.con
            .execute_batch(&build_events_table(&self.ev_columns))?;
        self.events_created = true;
        Ok(())
    }

    /// Add a column for an attribute name the wide `events` table does not yet have. Typed from
    /// `ev_attr_types` when a declaration already named it (the normal case: see the module docs
    /// on why a dynamically-named event type's later-arriving column still lands here typed, not
    /// as text). Falls back to `VARCHAR` only if reached with no declaration at all, which does
    /// not happen through [`extract`](super::extract::extract), where every name `add_event` is
    /// called with came from the same attribute list its `declare_event_type` call used.
    fn add_event_column(&mut self, name: &str) -> Result<(), duckdb::Error> {
        let ty = self
            .ev_attr_types
            .get(name)
            .copied()
            .unwrap_or(OCELAttributeType::String);
        self.con.execute_batch(&format!(
            "ALTER TABLE {T_EVENTS} ADD COLUMN {} {}",
            quote_ident(name),
            event_attr_sql_type(ty)
        ))?;
        self.ev_col_index
            .insert(name.to_string(), self.ev_columns.len());
        self.ev_columns.push((name.to_string(), ty));
        Ok(())
    }

    /// Turn an `objects.id` primary-key violation into the right [`SinkError`]. `DuckDB`'s error
    /// says only that `id` repeats, not under what type, so this looks the existing row up. The
    /// same type is an ordinary [`SinkError::DuplicateObject`]. A different type (or, defensively,
    /// a lookup that somehow finds no row at all) is [`SinkError::IdTypeCollision`], which
    /// [`mapping_exec`](super::mapping_exec) must not treat as "the id already exists, append".
    /// A lookup that fails is neither, and is reported as the backend error it is.
    ///
    /// This is the one place `resolve_object`'s inability to check a declared type against the
    /// object's actual one (see the module docs) is recovered from: one query, only on an actual
    /// conflict, rather than a per-id structure held for the whole run.
    fn classify_duplicate_object(&self, id: &str, object_type: &str) -> SinkError {
        let existing_type: Option<String> = match self.con.query_row(
            &format!("SELECT ocel_type FROM {T_OBJECTS} WHERE id = ?"),
            [id],
            |r| r.get(0),
        ) {
            Ok(t) => Some(t),
            Err(duckdb::Error::QueryReturnedNoRows) => None,
            // A query that failed says nothing about the existing row's type. Reporting a
            // collision here would have the extractor drop the row as two entities colliding,
            // losing data to what may be a transient backend problem.
            Err(e) => return backend(e),
        };
        if existing_type.as_deref() == Some(object_type) {
            SinkError::DuplicateObject { id: id.to_string() }
        } else {
            SinkError::IdTypeCollision { id: id.to_string() }
        }
    }

    /// The `(value, value_type)` pair one object attribute is stored as.
    ///
    /// [`to_sql_value`] alone is not enough: it renders [`OCELAttributeValue::Null`] as
    /// `("", "string")`, which `from_sql_value` reads back as `String("")`, where
    /// [`SlimOcelSink`](super::slim_sink::SlimOcelSink) still holds `Null`. That is an accepted
    /// caveat of `write_ocel_to_duckdb`'s own round trip, but a divergence between the two
    /// extraction sinks, which must agree. `"null"` is
    /// [`OCELAttributeType::Null`]'s own type string, and `from_sql_value` maps it straight back
    /// to `OCELAttributeValue::Null` regardless of the stored text.
    fn object_attr_sql_value(v: &OCELAttributeValue) -> (std::borrow::Cow<'_, str>, &'static str) {
        match v {
            OCELAttributeValue::Null => (std::borrow::Cow::Borrowed(""), "null"),
            other => to_sql_value(other),
        }
    }

    /// Write one `object_attribute_changes` row, keeping whatever was already recorded at this
    /// `(id, name, time)`. A rejection from [`I_OBJECT_ATTR_ONCE`] means a value is there already,
    /// which is the rule rather than a failure, see
    /// [`add_object_attribute`](ExtractionSink::add_object_attribute). Any other backend error is
    /// still reported.
    fn write_object_attribute(
        &self,
        id: &str,
        name: &str,
        time: DateTime<FixedOffset>,
        value: &OCELAttributeValue,
    ) -> Result<(), SinkError> {
        let (value_str, value_type) = Self::object_attr_sql_value(value);
        let t = datetime_to_duck_timestamp(time);
        match append_one(&self.con, T_OBJECT_ATTR_CHANGES, |ap| {
            ap.append_row((id, name, &t, value_str.as_ref(), value_type))
        }) {
            Ok(()) => Ok(()),
            Err(e) if is_duplicate_key(&e) => Ok(()),
            Err(e) => Err(backend(e)),
        }
    }

    /// Record that the object ask [`resolve_object`](ExtractionSink::resolve_object) most
    /// recently staged was made on behalf of a relation row gated by `gate_id`: an event id for
    /// an `E2O`, the source object id for an `O2O`. Consumes the pending seq, so a second relation
    /// written without an intervening ask gates nothing.
    ///
    /// A failure to record the gate row is swallowed, but not harmless: an ungated ask can win
    /// `arg_min` for an id a real row also names, so `Create` synthesises that object under a type
    /// the eager path would never have used, and both sinks then hold the same id under different
    /// types. The relation itself survives either way, only its type moves; the opposite mistake,
    /// an over-eager gate, is what costs a relation (see `resolve_deferred`). It is swallowed
    /// because failing the whole extraction over an appender error here would be worse, and
    /// nothing this sink can do at this point would repair the ask.
    fn gate_last_ask(&mut self, gate_kind: &'static str, gate_id: &str) {
        let Some(seq) = self.last_object_ask_seq.take() else {
            return;
        };
        if let Ok(mut gate) = self.con.appender(T_ENDPOINT_GATE) {
            let _ = gate.append_row(duckdb::params![seq, gate_id, gate_kind]);
            let _ = gate.flush();
        }
    }

    /// Append every buffered relation row of one kind and empty the buffer.
    fn drain_relations(
        con: &Connection,
        table: &str,
        buffer: &mut Vec<(String, String, String)>,
    ) -> Result<(), SinkError> {
        if buffer.is_empty() {
            return Ok(());
        }
        append_one(con, table, |ap| {
            for (a, b, qualifier) in buffer.iter() {
                ap.append_row([a.as_str(), b.as_str(), qualifier.as_str()])?;
            }
            Ok(())
        })
        .map_err(backend)?;
        buffer.clear();
        Ok(())
    }

    /// Append both relation buffers, so a reader of `e2o`/`o2o` sees every row written so far.
    fn drain_all_relations(&mut self) -> Result<(), SinkError> {
        Self::drain_relations(&self.con, T_E2O, &mut self.e2o_buffer)?;
        Self::drain_relations(&self.con, T_O2O, &mut self.o2o_buffer)
    }
}

impl ExtractionSink for DuckDbSink {
    fn declare_event_type(
        &mut self,
        name: &str,
        attrs: &[OCELTypeAttribute],
    ) -> Result<(), SinkError> {
        self.declared_event_types.insert(name.to_string());
        if attrs.is_empty() {
            return Ok(());
        }
        // one row per `(event_type, attr_name)`, last declaration winning. `SlimOcelSink`
        // overwrites a redeclared type wholesale (`locel.add_event_type`), while the reader's
        // `collect_types` keeps the first row per pair, from a `SELECT` with no `ORDER BY`, so
        // "first" was not even stable. Deleting the pair before appending collapses both rules
        // onto the same answer.
        let mut stmt = self
            .con
            .prepare(&format!(
                "DELETE FROM {T_EVENT_ATTR_META} WHERE event_type = ? AND attr_name = ?"
            ))
            .map_err(backend)?;
        for a in attrs {
            stmt.execute(duckdb::params![name, a.name.as_str()])
                .map_err(backend)?;
        }
        drop(stmt);

        let mut ap = self.con.appender(T_EVENT_ATTR_META).map_err(backend)?;
        for a in attrs {
            let ty = OCELAttributeType::from_type_str(&a.value_type);
            self.ev_attr_types
                .entry(a.name.clone())
                .and_modify(|existing| *existing = existing.coalesce(ty))
                .or_insert(ty);
            ap.append_row([name, a.name.as_str(), a.value_type.as_str()])
                .map_err(backend)?;
        }
        ap.flush().map_err(backend)?;
        Ok(())
    }

    /// Records the declaration in `T_OBJECT_ATTR_META`, the object-side mirror of
    /// `T_EVENT_ATTR_META`.
    ///
    /// Object attribute values are EAV (`object_attribute_changes`) rather than typed wide
    /// columns, so unlike events there is nothing to declare structurally. The declaration is
    /// still persisted, because it is the only record of an attribute no row ever wrote, and of a
    /// type whose mapping matched nothing, which [`extract`](super::extract::extract) declares up
    /// front on purpose. Rebuilding either from the observed change rows loses both, and
    /// splits one attribute observed once as `Null` and once as an integer into two entries of
    /// the same name.
    ///
    /// A type that declares no attributes at all still leaves no trace, exactly as an event type
    /// with no attributes and no events does: the type list itself lives in `objects`/`events`.
    fn declare_object_type(
        &mut self,
        name: &str,
        attrs: &[OCELTypeAttribute],
    ) -> Result<(), SinkError> {
        self.declared_object_types.insert(name.to_string());
        if attrs.is_empty() {
            return Ok(());
        }
        // One row per `(object_type, attr_name)`, last declaration winning. See
        // `declare_event_type` for why the pair is deleted before it is appended.
        let mut stmt = self
            .con
            .prepare(&format!(
                "DELETE FROM {T_OBJECT_ATTR_META} WHERE object_type = ? AND attr_name = ?"
            ))
            .map_err(backend)?;
        for a in attrs {
            stmt.execute(duckdb::params![name, a.name.as_str()])
                .map_err(backend)?;
        }
        drop(stmt);

        let mut ap = self.con.appender(T_OBJECT_ATTR_META).map_err(backend)?;
        for a in attrs {
            ap.append_row([name, a.name.as_str(), a.value_type.as_str()])
                .map_err(backend)?;
        }
        ap.flush().map_err(backend)?;
        Ok(())
    }

    fn add_event(
        &mut self,
        event_type: &str,
        time: DateTime<FixedOffset>,
        id: &str,
        attributes: &[(String, OCELAttributeValue)],
    ) -> Result<EventRef, SinkError> {
        if !self.declared_event_types.contains(event_type) {
            return Err(SinkError::UnknownType {
                kind: "event",
                name: event_type.to_string(),
            });
        }
        self.ensure_events_created().map_err(backend)?;
        for (name, _) in attributes {
            if !self.ev_col_index.contains_key(name) {
                self.add_event_column(name).map_err(backend)?;
            }
        }

        let mut row: Vec<DuckValue> = Vec::with_capacity(3 + self.ev_columns.len());
        row.push(DuckValue::Text(id.to_string()));
        row.push(DuckValue::Text(event_type.to_string()));
        row.push(datetime_to_duck_timestamp(time));
        row.extend(std::iter::repeat_n(DuckValue::Null, self.ev_columns.len()));
        for (name, value) in attributes {
            let idx = self.ev_col_index[name];
            row[3 + idx] = ocel_value_to_duck(value, self.ev_columns[idx].1);
        }
        let params: Vec<&dyn ToSql> = row.iter().map(|v| v as &dyn ToSql).collect();
        // `events.id` is a PRIMARY KEY, so a repeat is rejected here rather than by a set this
        // sink would otherwise have to hold in memory. See the module docs.
        append_one(&self.con, T_EVENTS, |ap| ap.append_row(params.as_slice())).map_err(|e| {
            if is_duplicate_key(&e) {
                SinkError::DuplicateEvent { id: id.to_string() }
            } else {
                backend(e)
            }
        })?;
        Ok(EventRef::Id(id.to_string()))
    }

    fn add_object(
        &mut self,
        object_type: &str,
        id: &str,
        attributes: &[(String, DateTime<FixedOffset>, OCELAttributeValue)],
    ) -> Result<ObjectRef, SinkError> {
        if !self.declared_object_types.contains(object_type) {
            return Err(SinkError::UnknownType {
                kind: "object",
                name: object_type.to_string(),
            });
        }
        // See `add_event`: `objects.id` is a PRIMARY KEY.
        append_one(&self.con, T_OBJECTS, |ap| ap.append_row([id, object_type])).map_err(|e| {
            if is_duplicate_key(&e) {
                self.classify_duplicate_object(id, object_type)
            } else {
                backend(e)
            }
        })?;

        for (name, time, value) in attributes {
            self.write_object_attribute(id, name, *time, value)?;
        }

        Ok(ObjectRef::Id(id.to_string()))
    }

    /// First-wins on `(id, name, time)`, enforced by `I_OBJECT_ATTR_ONCE`.
    fn add_object_attribute(
        &mut self,
        object: &ObjectRef,
        name: &str,
        time: DateTime<FixedOffset>,
        value: OCELAttributeValue,
    ) -> Result<(), SinkError> {
        let id = Self::object_id(object)?;
        self.write_object_attribute(id, name, time, &value)
    }

    fn set_missing_endpoint_policy(
        &mut self,
        policy: MissingEndpointPolicy,
    ) -> Result<(), SinkError> {
        self.on_missing_endpoint = policy;
        Ok(())
    }

    /// Always [`Resolution::Deferred`]: answering would need an id index this sink deliberately
    /// does not keep. See the module docs.
    fn resolve_event(&mut self, id: &str, _event_type: Option<&str>) -> Resolution<EventRef> {
        Resolution::Deferred(EventRef::Id(id.to_string()))
    }

    /// Always [`Resolution::Deferred`], recording `object_type` so
    /// [`finalize`](ExtractionSink::finalize) can honour `on_missing_endpoint: Create` for an
    /// endpoint that turns out not to exist.
    fn resolve_object(&mut self, id: &str, object_type: Option<&str>) -> Resolution<ObjectRef> {
        // Nothing reads what would be staged unless the policy is `Create`.
        if !self.stages_endpoints() {
            return Resolution::Deferred(ObjectRef::Id(id.to_string()));
        }
        // A failure here would only cost `Create` an object it could have synthesised. The
        // relation itself is still written and still resolved (or dropped) at finalize, so this
        // is deliberately not turned into a resolution failure.
        let seq = self.deferred_seq;
        self.deferred_seq += 1;
        if let Ok(mut ap) = self.con.appender(T_DEFERRED) {
            let _ = ap.append_row(duckdb::params![id, object_type, seq]);
            let _ = ap.flush();
        }
        self.last_object_ask_seq = Some(seq);
        Resolution::Deferred(ObjectRef::Id(id.to_string()))
    }

    fn finalize(&mut self) -> Result<FinalizeReport, SinkError> {
        if self.finalized {
            return Ok(FinalizeReport::default());
        }
        self.drain_all_relations()?;
        self.ensure_events_created().map_err(backend)?;
        let report = self.resolve_deferred().map_err(backend)?;
        create_indexes(&self.con).map_err(backend)?;
        self.con.execute_batch("CHECKPOINT").map_err(backend)?;
        self.finalized = true;
        Ok(report)
    }

    fn add_e2o(
        &mut self,
        event: &EventRef,
        object: &ObjectRef,
        qualifier: &str,
    ) -> Result<(), SinkError> {
        let e = Self::event_id(event)?.to_string();
        let o = Self::object_id(object)?.to_string();
        self.e2o_buffer.push((e.clone(), o, qualifier.to_string()));
        if self.e2o_buffer.len() >= RELATION_BUFFER {
            Self::drain_relations(&self.con, T_E2O, &mut self.e2o_buffer)?;
        }
        // See `T_ENDPOINT_GATE`'s docs: this links the object ask just staged (if any) to the
        // event this row names, so `resolve_deferred` can tell a real ask from one made on behalf
        // of a row whose event never resolves.
        self.gate_last_ask("event", &e);
        Ok(())
    }

    fn add_o2o(
        &mut self,
        source: &ObjectRef,
        target: &ObjectRef,
        qualifier: &str,
    ) -> Result<(), SinkError> {
        let s = Self::object_id(source)?.to_string();
        let t = Self::object_id(target)?.to_string();
        self.o2o_buffer.push((s.clone(), t, qualifier.to_string()));
        if self.o2o_buffer.len() >= RELATION_BUFFER {
            Self::drain_relations(&self.con, T_O2O, &mut self.o2o_buffer)?;
        }
        // The target ask is the immediately preceding `resolve_object` (see `add_o2o`'s contract
        // on `ExtractionSink`), so the same gate that saves `E2O` covers `O2O` too, keyed on the
        // source, which is what the eager path gives up on before it reaches any target.
        self.gate_last_ask("object", &s);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::core::event_data::object_centric::ocel_sql::read_ocel_from_duckdb;

    fn open_sink() -> (tempfile::TempDir, DuckDbSink) {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("sink.duckdb");
        let sink = DuckDbSink::new(&path).expect("open sink");
        (dir, sink)
    }

    #[test]
    fn unknown_type_is_rejected_for_events_and_objects() {
        let (_dir, mut sink) = open_sink();
        let t = chrono::Utc::now().fixed_offset();
        assert!(matches!(
            sink.add_event("Nope", t, "e1", &[]),
            Err(SinkError::UnknownType { kind: "event", .. })
        ));
        assert!(matches!(
            sink.add_object("Nope", "o1", &[]),
            Err(SinkError::UnknownType { kind: "object", .. })
        ));
    }

    #[test]
    fn duplicate_event_and_object_ids_are_rejected() {
        let (_dir, mut sink) = open_sink();
        sink.declare_event_type("Pay", &[]).unwrap();
        sink.declare_object_type("Order", &[]).unwrap();
        let t = chrono::Utc::now().fixed_offset();
        sink.add_event("Pay", t, "e1", &[]).unwrap();
        sink.add_object("Order", "o1", &[]).unwrap();

        assert_eq!(
            sink.add_event("Pay", t, "e1", &[]),
            Err(SinkError::DuplicateEvent { id: "e1".into() })
        );
        assert_eq!(
            sink.add_object("Order", "o1", &[]),
            Err(SinkError::DuplicateObject { id: "o1".into() })
        );
    }

    /// This sink never answers `Exists`/`Missing`: it keeps no id index, by design, and settles
    /// every endpoint at `finalize` instead.
    #[test]
    fn every_resolution_is_deferred() {
        let (_dir, mut sink) = open_sink();
        assert!(matches!(
            sink.resolve_event("e1", None),
            Resolution::Deferred(EventRef::Id(_))
        ));
        assert!(matches!(
            sink.resolve_object("o1", Some("Order")),
            Resolution::Deferred(ObjectRef::Id(_))
        ));
    }

    /// A relation written against a deferred endpoint that never materialises is deleted at
    /// finalize under `Drop`, and counted as the same loss an eager sink reports per mapping via
    /// `DropReason::UnresolvedEndpoint`.
    #[test]
    fn finalize_drops_relations_whose_deferred_endpoint_never_appeared() {
        let (dir, mut sink) = open_sink();
        sink.set_missing_endpoint_policy(MissingEndpointPolicy::Drop)
            .unwrap();
        sink.declare_event_type("Pay", &[]).unwrap();
        sink.declare_object_type("Order", &[]).unwrap();
        let t = chrono::Utc::now().fixed_offset();
        sink.add_event("Pay", t, "e1", &[]).unwrap();
        sink.add_object("Order", "o1", &[]).unwrap();

        let ev = sink.resolve_event("e1", None).into_ref().unwrap();
        let good = sink.resolve_object("o1", Some("Order")).into_ref().unwrap();
        let ghost = sink
            .resolve_object("nope", Some("Order"))
            .into_ref()
            .unwrap();
        sink.add_e2o(&ev, &good, "q").unwrap();
        sink.add_e2o(&ev, &ghost, "q").unwrap();

        let report = ExtractionSink::finalize(&mut sink).unwrap();
        assert_eq!(report.unresolved_endpoints, 1);
        assert_eq!(report.resolved_relations, 1);
        assert_eq!(report.objects_created, 0);

        let con = duckdb::Connection::open(dir.path().join("sink.duckdb")).unwrap();
        let ocel = read_ocel_from_duckdb(&con).unwrap();
        let e1 = ocel.events.iter().find(|e| e.id == "e1").unwrap();
        assert_eq!(e1.relationships.len(), 1, "the dangling relation is gone");
    }

    /// `on_missing_endpoint: Create` is honoured at finalize, from the type the endpoint
    /// declared, which is why `resolve_object` is given it.
    #[test]
    fn finalize_creates_missing_objects_under_the_create_policy() {
        let (dir, mut sink) = open_sink();
        sink.set_missing_endpoint_policy(MissingEndpointPolicy::Create)
            .unwrap();
        sink.declare_event_type("Pay", &[]).unwrap();
        sink.declare_object_type("Order", &[]).unwrap();
        let t = chrono::Utc::now().fixed_offset();
        sink.add_event("Pay", t, "e1", &[]).unwrap();
        let ev = sink.resolve_event("e1", None).into_ref().unwrap();
        let ghost = sink
            .resolve_object("o-new", Some("Order"))
            .into_ref()
            .unwrap();
        sink.add_e2o(&ev, &ghost, "q").unwrap();

        let report = ExtractionSink::finalize(&mut sink).unwrap();
        assert_eq!(report.objects_created, 1);
        assert_eq!(report.unresolved_endpoints, 0);
        assert_eq!(report.resolved_relations, 1);

        let con = duckdb::Connection::open(dir.path().join("sink.duckdb")).unwrap();
        let ocel = read_ocel_from_duckdb(&con).unwrap();
        let created = ocel.objects.iter().find(|o| o.id == "o-new").unwrap();
        assert_eq!(created.object_type, "Order");
    }

    /// A second event type declaring an attribute name under a genuinely different type than an
    /// earlier declaration widens the accumulated declaration ([`OCELAttributeType::coalesce`]),
    /// regardless of whether the wide `events` table already exists. Exercises the "declared
    /// after the freeze, but still declared" path documented on [`DuckDbSink::add_event_column`].
    #[test]
    fn a_column_declared_after_the_freeze_still_gets_its_declared_type() {
        let (dir, mut sink) = open_sink();
        let t = chrono::Utc::now().fixed_offset();
        sink.declare_event_type(
            "A",
            &[OCELTypeAttribute {
                name: "n".into(),
                value_type: "integer".into(),
            }],
        )
        .unwrap();
        // Freezes the wide table with "n" as BIGINT.
        sink.add_event(
            "A",
            t,
            "e1",
            &[("n".to_string(), OCELAttributeValue::Integer(1))],
        )
        .unwrap();
        // A second, later-declared type gives "n" an unrelated but compatible attribute name
        // "m", exercising the on-demand ALTER path rather than "n" itself (widening an existing
        // column's type is a separate, inherited limitation documented on the sink).
        sink.declare_event_type(
            "B",
            &[OCELTypeAttribute {
                name: "m".into(),
                value_type: "float".into(),
            }],
        )
        .unwrap();
        sink.add_event(
            "B",
            t,
            "e2",
            &[("m".to_string(), OCELAttributeValue::Float(2.5))],
        )
        .unwrap();

        let path = dir.path().join("sink.duckdb");
        ExtractionSink::finalize(&mut sink).unwrap();
        let con = duckdb::Connection::open(&path).unwrap();
        let ty: String = con
            .query_row(
                "SELECT data_type FROM information_schema.columns \
                 WHERE table_name = 'events' AND column_name = 'm'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            ty, "DOUBLE",
            "late column keeps its declared type, not VARCHAR"
        );

        let ocel = read_ocel_from_duckdb(&con).unwrap();
        let e2 = ocel.events.iter().find(|e| e.id == "e2").unwrap();
        assert_eq!(
            e2.attributes.first().map(|a| &a.value),
            Some(&OCELAttributeValue::Float(2.5))
        );
    }

    /// `SlimOcelSink` keeps the last declaration for a `(type, attribute)`, since
    /// `locel.add_event_type` overwrites, while the `DuckDB` reader's `collect_types` keeps the
    /// first row per `(type, name)`, from a `SELECT` with no `ORDER BY`. Appending a row per
    /// `declare_event_type` call therefore made the two sinks disagree, and made "first" not even
    /// stable across reads. One row per `(type, name)` makes first-wins and last-wins the same
    /// answer.
    #[test]
    fn a_redeclared_event_attribute_type_reads_back_as_the_last_declaration() {
        let (dir, mut sink) = open_sink();
        let declare = |sink: &mut DuckDbSink, value_type: &str| {
            sink.declare_event_type(
                "A",
                &[OCELTypeAttribute {
                    name: "n".into(),
                    value_type: value_type.into(),
                }],
            )
            .unwrap();
        };
        declare(&mut sink, "integer");
        declare(&mut sink, "string");
        ExtractionSink::finalize(&mut sink).unwrap();

        let con = duckdb::Connection::open(dir.path().join("sink.duckdb")).unwrap();
        let rows: i64 = con
            .query_row(
                &format!("SELECT count(*) FROM {T_EVENT_ATTR_META} WHERE attr_name = 'n'"),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            rows, 1,
            "one row per (type, attribute), so first-wins and last-wins coincide"
        );
        let stored: String = con
            .query_row(
                &format!("SELECT attr_type FROM {T_EVENT_ATTR_META} WHERE attr_name = 'n'"),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            stored, "string",
            "the last declaration wins, as in SlimOcelSink"
        );
    }

    /// `to_sql_value` renders `OCELAttributeValue::Null` as `("", "string")`, so the reader
    /// hands back `String("")` where `SlimOcelSink` holds `Null`. The `value_type` column is the
    /// sink's to choose, and `"null"` round-trips through `from_sql_value` exactly.
    #[test]
    fn a_null_object_attribute_round_trips_as_null_not_an_empty_string() {
        let (dir, mut sink) = open_sink();
        sink.declare_object_type("Order", &[]).unwrap();
        let t = chrono::Utc::now().fixed_offset();
        sink.add_object(
            "Order",
            "o1",
            &[("note".to_string(), t, OCELAttributeValue::Null)],
        )
        .unwrap();
        // The same value through the other write path.
        let o = ObjectRef::Id("o1".to_string());
        sink.add_object_attribute(&o, "memo", t, OCELAttributeValue::Null)
            .unwrap();
        ExtractionSink::finalize(&mut sink).unwrap();

        let con = duckdb::Connection::open(dir.path().join("sink.duckdb")).unwrap();
        let ocel = read_ocel_from_duckdb(&con).unwrap();
        let o1 = ocel.objects.iter().find(|o| o.id == "o1").unwrap();
        for name in ["note", "memo"] {
            let a = o1
                .attributes
                .iter()
                .find(|a| a.name == name)
                .unwrap_or_else(|| panic!("attribute {name} present"));
            assert_eq!(
                a.value,
                OCELAttributeValue::Null,
                "a Null object attribute must not read back as String(\"\")"
            );
        }
    }

    // The `O2O` half, that a target ask staged for a row whose source never resolves must not win
    // `arg_min`, is covered by `case_11_an_o2o_target_ask_for_an_unresolvable_source_does_not_win_the_type`
    // in this module's `tests` sibling, which compares against the eager sink rather than pinning
    // this sink's own output.

    #[test]
    fn finalize_creates_events_table_even_with_zero_events() {
        let (dir, mut sink) = open_sink();
        let path = dir.path().join("sink.duckdb");
        ExtractionSink::finalize(&mut sink).unwrap();
        let con = duckdb::Connection::open(&path).unwrap();
        let n: i64 = con
            .query_row("SELECT count(*) FROM events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }
}
