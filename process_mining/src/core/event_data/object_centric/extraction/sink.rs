//! Where extracted entities and relations go.

use std::fmt::Debug;

use chrono::{DateTime, FixedOffset};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::blueprint::MissingEndpointPolicy;
use crate::core::event_data::object_centric::{OCELAttributeValue, OCELTypeAttribute};

/// A handle to an event, passed back to [`ExtractionSink::add_e2o`].
///
/// Opaque to the extractor, which only ever stores and replays it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventRef {
    /// An in-memory sink's own event index.
    Index(u32),
    /// The id a streaming sink was given, echoed back as its handle.
    Id(String),
}

/// A handle to an object. Mirrors [`EventRef`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectRef {
    /// An in-memory sink's own object index.
    Index(u32),
    /// The id a streaming sink was given, echoed back as its handle.
    Id(String),
}

/// What a sink can say about an entity id a relation names.
///
/// [`Resolution::Deferred`] is what lets a sink avoid keeping a full in-memory id index: it may
/// decline to answer now and resolve the relation in bulk at
/// [`finalize`](ExtractionSink::finalize). `on_missing_endpoint` decides the same fate for a
/// relation either way, but a deferring sink can only report a count, not which row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution<R> {
    /// The entity exists. Here is its handle.
    Exists(R),
    /// The entity does not exist. The caller applies `on_missing_endpoint` now.
    Missing,
    /// The sink cannot say, and has undertaken to resolve it at
    /// [`finalize`](ExtractionSink::finalize). The handle is valid for writing a relation
    /// against, but carries no promise that the entity exists: if it turns out not to,
    /// `finalize` applies `on_missing_endpoint` to the relation then.
    Deferred(R),
}

impl<R> Resolution<R> {
    /// The handle, for `Exists` and `Deferred` alike. `None` for `Missing`.
    #[must_use]
    pub fn into_ref(self) -> Option<R> {
        match self {
            Resolution::Exists(r) | Resolution::Deferred(r) => Some(r),
            Resolution::Missing => None,
        }
    }
}

/// What a sink did during [`ExtractionSink::finalize`], folded into
/// [`ExtractionReport::finalize`](super::report::ExtractionReport::finalize).
///
/// All zero for a sink that resolves eagerly. Nonzero entries carry a deferring sink's share of
/// the per-mapping counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FinalizeReport {
    /// Relations written against a [`Resolution::Deferred`] endpoint that resolved to a real
    /// entity at finalize.
    pub resolved_relations: u64,
    /// Relations whose deferred endpoint did not resolve. An eager sink counts these per mapping
    /// as [`DropReason::UnresolvedEndpoint`](super::report::DropReason::UnresolvedEndpoint).
    pub unresolved_endpoints: u64,
    /// Objects synthesised at finalize under `on_missing_endpoint: Create`, for deferred
    /// endpoints that turned out not to exist.
    pub objects_created: u64,
    /// Repeated entity ids removed at finalize: a deferring sink's share of
    /// [`MappingStats::deduplicated`](super::report::MappingStats::deduplicated).
    pub duplicates_removed: u64,
}

/// Why an [`ExtractionSink`] call failed.
///
/// Serializable but not deserializable: [`SinkError::UnknownType`] carries a `&'static str` (a
/// borrow no deserializer can manufacture), so this only ever crosses a bindings boundary
/// outbound, nested inside [`ExtractionError::Sink`](super::report::ExtractionError::Sink).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub enum SinkError {
    /// [`ExtractionSink::add_event`] was called with an id already present.
    DuplicateEvent {
        /// The repeated id.
        id: String,
    },
    /// [`ExtractionSink::add_object`] was called with an id already present.
    DuplicateObject {
        /// The repeated id.
        id: String,
    },
    /// [`ExtractionSink::add_object`] was called for `id`, but `id` is already present under a
    /// different object type, i.e. two distinct entities collided on one id rather than the plain
    /// repeat [`SinkError::DuplicateObject`] reports. Only reachable under
    /// [`IdRendering::Raw`](super::blueprint::IdRendering::Raw). A deferring sink cannot make that
    /// distinction at [`resolve_object`](ExtractionSink::resolve_object) time and reports it
    /// here.
    IdTypeCollision {
        /// The contested id.
        id: String,
    },
    /// An entity was added, or a relation named an endpoint, under a type that was never
    /// declared via [`ExtractionSink::declare_event_type`] / [`declare_object_type`](ExtractionSink::declare_object_type).
    UnknownType {
        /// `"event"` or `"object"`.
        kind: &'static str,
        /// The undeclared type name.
        name: String,
    },
    /// A relation named a ref this sink instance did not itself hand out. Cannot happen through
    /// [`extract`](super::extract::extract).
    InvalidRef,
    /// The backend failed (I/O, driver error), as its message.
    Backend(String),
}

impl std::fmt::Display for SinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkError::DuplicateEvent { id } => write!(f, "duplicate event id '{id}'"),
            SinkError::DuplicateObject { id } => write!(f, "duplicate object id '{id}'"),
            SinkError::IdTypeCollision { id } => {
                write!(f, "id '{id}' is already taken by an object of another type")
            }
            SinkError::UnknownType { kind, name } => {
                write!(f, "undeclared {kind} type '{name}'")
            }
            SinkError::InvalidRef => write!(f, "ref not recognised by this sink"),
            SinkError::Backend(message) => write!(f, "sink backend error: {message}"),
        }
    }
}

impl std::error::Error for SinkError {}

/// Where [`extract`](super::extract::extract) sends the entities and relations it produces.
///
/// Two implementations ship with this crate: [`SlimOcelSink`](super::slim_sink::SlimOcelSink)
/// (in memory, always available) and a `DuckDB`-backed streaming sink (feature `ocel-duckdb`).
pub trait ExtractionSink: Debug {
    /// Declare an event type with these attributes, merging with any earlier declaration of
    /// the same name (new attribute names are added, already-declared ones are left alone).
    /// Idempotent: declaring the same type again is not an error.
    ///
    /// # Errors
    /// Returns [`SinkError`] if the backend fails to record the declaration.
    fn declare_event_type(
        &mut self,
        name: &str,
        attrs: &[OCELTypeAttribute],
    ) -> Result<(), SinkError>;

    /// Declare an object type. See [`declare_event_type`](ExtractionSink::declare_event_type).
    ///
    /// # Errors
    /// Returns [`SinkError`] if the backend fails to record the declaration.
    fn declare_object_type(
        &mut self,
        name: &str,
        attrs: &[OCELTypeAttribute],
    ) -> Result<(), SinkError>;

    /// Add an event of a previously declared `event_type`, whose declaration must already name
    /// every attribute in `attributes`.
    ///
    /// # Errors
    /// Returns [`SinkError::UnknownType`] if `event_type` was never declared,
    /// [`SinkError::DuplicateEvent`] if `id` is already present, or [`SinkError::Backend`] if an
    /// attribute name is not one the type declares (as
    /// [`add_object_attribute`](ExtractionSink::add_object_attribute) reports the same condition)
    /// or the backend failed.
    fn add_event(
        &mut self,
        event_type: &str,
        time: DateTime<FixedOffset>,
        id: &str,
        attributes: &[(String, OCELAttributeValue)],
    ) -> Result<EventRef, SinkError>;

    /// Add an object of a previously declared `object_type`, with its initial timed attribute
    /// values, subject to the same one-value-per-`(id, name, time)` rule
    /// [`add_object_attribute`](ExtractionSink::add_object_attribute) describes.
    ///
    /// # Errors
    /// Returns [`SinkError::UnknownType`] if `object_type` was never declared,
    /// [`SinkError::DuplicateObject`] if `id` is already present under this same type,
    /// [`SinkError::IdTypeCollision`] if `id` is already present under a different type, or
    /// [`SinkError::Backend`] if an attribute name is not one the type declares (as
    /// [`add_object_attribute`](ExtractionSink::add_object_attribute) reports the same condition)
    /// or the backend failed.
    fn add_object(
        &mut self,
        object_type: &str,
        id: &str,
        attributes: &[(String, DateTime<FixedOffset>, OCELAttributeValue)],
    ) -> Result<ObjectRef, SinkError>;

    /// Append one more timed value to an already-added object's named attribute. This is how a
    /// change-tracked [`Target::Object`](super::blueprint::Target::Object) (`timestamp: Some`)
    /// records a later row naming the same object id.
    ///
    /// # At most one value per `(id, name, time)`, and the first one wins
    ///
    /// A repeated `(object, name, time)` is not an error and does not append a second
    /// entry. The same rule applies to [`add_object`](ExtractionSink::add_object)'s initial
    /// values, so an object's history does not depend on whether the row that wrote an attribute
    /// also created the object. It is what makes a static object attribute single-valued: a
    /// `timestamp: None` [`Target::Object`](super::blueprint::Target::Object) writes every row's
    /// value at the epoch, so a case-level column repeated across a case's events is stored once.
    ///
    /// Where two writes at one `(id, name, time)` carry different values, scan order decides
    /// which survives, and [`extract`](super::extract::extract) issues no `ORDER BY`.
    ///
    /// # Errors
    /// Returns [`SinkError::InvalidRef`] if `object` was not handed out by this sink, or
    /// [`SinkError::Backend`] on a backend failure.
    fn add_object_attribute(
        &mut self,
        object: &ObjectRef,
        name: &str,
        time: DateTime<FixedOffset>,
        value: OCELAttributeValue,
    ) -> Result<(), SinkError>;

    /// Announce the `on_missing_endpoint` policy this run uses, before any entity is added.
    ///
    /// Only a sink that answers [`Resolution::Deferred`] needs this: it applies the policy itself
    /// at [`finalize`](ExtractionSink::finalize). For an eager sink the extractor applies it at
    /// the call site, so the default is to ignore it.
    ///
    /// # Errors
    /// Returns [`SinkError`] if the backend fails to record it.
    fn set_missing_endpoint_policy(
        &mut self,
        policy: MissingEndpointPolicy,
    ) -> Result<(), SinkError> {
        let _ = policy;
        Ok(())
    }

    /// Resolve an event id a relation names, without adding anything.
    ///
    /// `event_type`, when the endpoint declares one, is the type the caller expects: a sink that
    /// can check it must answer [`Resolution::Missing`] when an event of that id exists under a
    /// different type. Otherwise, under
    /// [`IdRendering::Raw`](super::blueprint::IdRendering::Raw), two types collide on one id and
    /// are silently merged.
    ///
    /// Takes `&mut self` so a sink may record that it owes an answer, see
    /// [`Resolution::Deferred`].
    fn resolve_event(&mut self, id: &str, event_type: Option<&str>) -> Resolution<EventRef>;

    /// Resolve an object id. See [`resolve_event`](ExtractionSink::resolve_event).
    fn resolve_object(&mut self, id: &str, object_type: Option<&str>) -> Resolution<ObjectRef>;

    /// Finish the run: resolve everything this sink deferred, and leave the backing store in a
    /// readable state. Called exactly once by [`extract`](super::extract::extract), after the
    /// last row.
    ///
    /// # Errors
    /// Returns [`SinkError`] if the backend fails. Fatal to the run: the log is incomplete.
    fn finalize(&mut self) -> Result<FinalizeReport, SinkError>;

    /// Add an event-to-object relation.
    ///
    /// # Calling contract: one `resolve_object` immediately before, for this row's own object
    ///
    /// Every `add_e2o` call must be immediately preceded by exactly one
    /// [`resolve_object`](ExtractionSink::resolve_object) call for the very `object` argument
    /// passed here, with no other `resolve_object` call in between.
    /// [`extract`](super::extract::extract) satisfies this by construction. A third-party driver
    /// that batches its resolutions does not, and a deferring sink cannot detect the violation.
    ///
    /// The adjacency is how a deferring sink links each staged ask to the `add_e2o` that consumed
    /// it, so it can discard an ask belonging to a row whose event never materialised. Violate it
    /// and, under [`MissingEndpointPolicy::Create`],
    /// an object id can be synthesised under the wrong declared type, silently.
    ///
    /// # Errors
    /// Returns [`SinkError::InvalidRef`] if either ref was not handed out by this sink, or
    /// [`SinkError::Backend`] on a backend failure.
    fn add_e2o(
        &mut self,
        event: &EventRef,
        object: &ObjectRef,
        qualifier: &str,
    ) -> Result<(), SinkError>;

    /// Add an object-to-object relation, from `source` to `target`.
    ///
    /// # Calling contract: one `resolve_object` immediately before, for this row's own target
    ///
    /// The same adjacency [`add_e2o`](ExtractionSink::add_e2o) requires, on the target side.
    /// The `source` carries no such requirement: one resolved source may back several targets,
    /// which is how [`extract`](super::extract::extract) drives it.
    ///
    /// It lets a deferring sink discard a target ask staged for a row whose source never
    /// resolved. See `add_e2o` for what a violation costs.
    ///
    /// # Errors
    /// Returns [`SinkError::InvalidRef`] if either ref was not handed out by this sink, or
    /// [`SinkError::Backend`] on a backend failure.
    fn add_o2o(
        &mut self,
        source: &ObjectRef,
        target: &ObjectRef,
        qualifier: &str,
    ) -> Result<(), SinkError>;
}
