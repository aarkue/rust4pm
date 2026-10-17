//! What [`extract`](super::extract::extract) produces alongside the OCEL: what ran, and every
//! row it could not use.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::compile::RejectReason;
use super::provider::ProviderError;
use super::sink::{FinalizeReport, SinkError};
use super::validate::ValidationError;
use crate::core::event_data::object_centric::OCELAttributeType;

/// Points a diagnostic back at the mapping it came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MappingRef {
    /// Position in the desugared, flattened mapping list this run executed.
    pub index: usize,
    /// The mapping's own label, if it has one.
    pub label: Option<String>,
    /// The JSON path of the authored entry this mapping came from (see `desugar_with_paths`), so
    /// a diagnostic points at what the author wrote rather than a position in the flattened
    /// list.
    pub path: String,
    /// What this mapping produces, derived from its target: `event "appoint officer"`,
    /// `event -> object relation`, and so on. Present whether or not a `label` was typed.
    pub describes: String,
}

impl MappingRef {
    /// Build a reference to `mapping`, describing it from its target.
    #[must_use]
    pub(crate) fn new(index: usize, path: String, mapping: &super::blueprint::Mapping) -> Self {
        Self {
            index,
            label: mapping.label.clone(),
            path,
            describes: describe_target(&mapping.target),
        }
    }

    /// The author's label if there is one, else the derived description. Prefer this over `path`
    /// when rendering a diagnostic.
    #[must_use]
    pub fn title(&self) -> &str {
        self.label.as_deref().unwrap_or(&self.describes)
    }
}

fn describe_target(target: &super::blueprint::Target) -> String {
    use super::blueprint::Target;
    use super::expr::ValueExpression;
    // Only a constant names a type the reader can match against the canvas.
    fn named(e: &ValueExpression) -> Option<&str> {
        match e {
            ValueExpression::Constant { value } if !value.is_empty() => Some(value.as_str()),
            _ => None,
        }
    }
    match target {
        Target::Event { event_type, .. } => {
            named(event_type).map_or_else(|| "event".to_string(), |t| format!("event \"{t}\""))
        }
        Target::Object { object_type, .. } => {
            named(object_type).map_or_else(|| "object".to_string(), |t| format!("object \"{t}\""))
        }
        Target::E2O { object, .. } => object.object_type.as_ref().and_then(named).map_or_else(
            || "event -> object relation".to_string(),
            |t| format!("event -> \"{t}\" relation"),
        ),
        Target::O2O { source, target, .. } => match (
            source.object_type.as_ref().and_then(named),
            target.object_type.as_ref().and_then(named),
        ) {
            (Some(s), Some(t)) => format!("\"{s}\" -> \"{t}\" relation"),
            _ => "object -> object relation".to_string(),
        },
    }
}

/// Why one row a mapping read produced nothing.
///
/// Does not include a repeated object id at event grain, see [`MappingStats::deduplicated`].
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
pub enum DropReason {
    /// A relation named an event or object id that could not be resolved, under
    /// [`MissingEndpointPolicy::Drop`](super::blueprint::MissingEndpointPolicy::Drop).
    UnresolvedEndpoint,
    /// A timestamp expression produced text, and that text did not parse: the format is wrong.
    UnparseableTimestamp,
    /// The row had no timestamp to parse: the column was `NULL`, or the expression produced
    /// nothing (or only blank text).
    ///
    /// For a [`TimestampSource::Components`](super::expr::TimestampSource::Components) pair the
    /// date side decides, whatever the time side holds: a time of day with no date does not
    /// name an instant.
    MissingTimestamp,
    /// An id expression evaluated to `Null` or to a value with no
    /// [`canonical_string`](super::value::Value::canonical_string).
    NullOrUnrenderableId,
    /// The mapping's `when` excluded the row.
    PredicateExcluded,
    /// The row named an entity whose id is already taken by an entity of a different type.
    /// Only reachable under [`IdRendering::Raw`](super::blueprint::IdRendering::Raw), where two
    /// types can render the same id. `TypePrefixed` makes it impossible. Deliberately not
    /// [`MappingStats::deduplicated`]: nothing was deduplicated, two distinct entities collided.
    IdTypeCollision,
    /// The row named an object the sink already had and
    /// [`DuplicateObjectPolicy::Error`](super::blueprint::DuplicateObjectPolicy::Error) is in
    /// force, so none of the row's attributes were written onto it. The same repeat counts as
    /// [`MappingStats::deduplicated`] under `FirstWins`, where it is not a loss.
    DuplicateObjectRejected,
}

/// Counts for one mapping's run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MappingStats {
    /// Which mapping.
    pub mapping: MappingRef,
    /// Rows the mapping's node produced, before `when` was applied.
    pub rows_read: u64,
    /// Entities or relations this mapping handed to the sink, which is not the same as
    /// "survived the run" for a sink that defers resolution.
    ///
    /// An eager sink refuses a dangling relation at the call site, so it lands in
    /// [`DropReason::UnresolvedEndpoint`] and never here. A deferring sink writes it, counts it
    /// here, and deletes it at [`finalize`](super::sink::ExtractionSink::finalize), reporting it
    /// in [`FinalizeReport::unresolved_endpoints`](super::sink::FinalizeReport) instead. To count
    /// what a run produced, subtract `ExtractionReport::finalize.unresolved_endpoints` from the
    /// total.
    pub entities_emitted: u64,
    /// Rows that tried to create an entity the sink already had. Not a loss: an object mapping
    /// at event grain names the same object on every row by design. See
    /// [`DuplicateObjectPolicy::Error`](super::blueprint::DuplicateObjectPolicy::Error) for what
    /// turns a repeat into a loss instead.
    ///
    /// One increment per row whose entity-creating call found the entity already present, across
    /// mappings, since the sink is what answers.
    ///
    /// Resolving a relation endpoint is never counted, so an `E2O`/`O2O` mapping reports zero
    /// however often its rows repeat an id: finding an existing endpoint is the normal successful
    /// case. A blueprint that wants its inline references' repeats counted can name the objects
    /// with their own [`Target::Object`](super::blueprint::Target::Object) mapping.
    pub deduplicated: u64,
    /// Rows dropped, by reason. A row that matches several reasons at once (rare) is counted
    /// once, under the first one detected.
    pub dropped: BTreeMap<DropReason, u64>,
    /// Attribute values that would not convert to their attribute's declared type, stored as
    /// `Null`. Not a dropped row: the entity was written, with one of its attributes empty.
    #[serde(default)]
    pub uncoercible_attributes: u64,
}

impl MappingStats {
    /// Zeroed stats for `mapping`.
    #[must_use]
    pub(crate) fn new(mapping: MappingRef) -> Self {
        Self {
            mapping,
            rows_read: 0,
            entities_emitted: 0,
            deduplicated: 0,
            dropped: BTreeMap::new(),
            uncoercible_attributes: 0,
        }
    }

    /// Increment `reason`'s count by one.
    pub(crate) fn drop(&mut self, reason: DropReason) {
        *self.dropped.entry(reason).or_insert(0) += 1;
    }
}

/// The most [`ExtractionError`]s one run keeps. Past this only `ErrorLog::suppressed` grows.
///
/// Capped because a policy configured to error reports one error per offending row, making this
/// the only per-run structure whose size is a function of the data rather than of the blueprint.
pub const MAX_REPORTED_ERRORS: usize = 1000;

/// A bounded [`ExtractionError`] collector: the first [`MAX_REPORTED_ERRORS`] are kept in full,
/// the rest only counted.
#[derive(Debug, Default)]
pub(crate) struct ErrorLog {
    errors: Vec<ExtractionError>,
    suppressed: u64,
}

impl ErrorLog {
    /// An empty log.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record `error`, or count it as suppressed once the cap is reached.
    pub(crate) fn push(&mut self, error: ExtractionError) {
        if self.errors.len() < MAX_REPORTED_ERRORS {
            self.errors.push(error);
        } else {
            self.suppressed += 1;
        }
    }

    /// The errors kept, and how many were not.
    pub(crate) fn into_parts(self) -> (Vec<ExtractionError>, u64) {
        (self.errors, self.suppressed)
    }
}

/// One node the compiler refused to push down to its source, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct PushdownDeclined {
    /// The node id, as the blueprint names it.
    pub node: String,
    /// What the compiler objected to.
    pub reason: RejectReason,
}

/// What [`extract`](super::extract::extract) produced, beyond the OCEL itself.
///
/// Serializable but not deserializable: [`ExtractionError`] carries `&'static str` fields (a
/// borrow no deserializer can manufacture), so this only ever crosses a bindings boundary
/// outbound, as a `#[register_binding]` return value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct ExtractionReport {
    /// One entry per mapping executed, in desugared blueprint order: the order the author wrote
    /// the mappings in, with each ordered group expanded in place. Not execution order,
    /// which is multi-pass and grouped by node. Each entry's [`MappingRef::path`] names the
    /// authored entry.
    pub per_mapping: Vec<MappingStats>,
    /// Non-fatal problems collected while running: a policy configured to error
    /// (`on_duplicate_object: Error`, `on_missing_endpoint: Error`), or an attribute type two
    /// mappings disagreed on. Extraction continues past these. See [`ExtractionError`] for what
    /// aborts it instead.
    ///
    /// Capped at [`MAX_REPORTED_ERRORS`], with the remainder counted in
    /// [`errors_suppressed`](Self::errors_suppressed).
    pub errors: Vec<ExtractionError>,
    /// Non-fatal problems the run hit past [`MAX_REPORTED_ERRORS`], which
    /// [`errors`](Self::errors) therefore does not name. Zero for every run under the cap.
    pub errors_suppressed: u64,
    /// Rows every `Join`/`Union` materialisation this run performed produced, summed across
    /// materialisations rather than peaked: an upper bound on peak buffered rows. A cached
    /// materialisation is counted once, when computed.
    ///
    /// Zero when no mapping's node graph contains a `Join` or `Union`, since a pure
    /// `Source -> Filter` chain streams. Zero is therefore a witness that the run streamed.
    pub rows_materialized: u64,
    /// Nodes whose source could have executed the whole node but the compiler declined to build
    /// the query for, paired with the reason, and deduplicated per node.
    ///
    /// Always safe, since the executor runs the node itself. Reported because falling back on a
    /// `Join` is the one execution path whose memory grows with the data, so this explains a
    /// non-zero [`rows_materialized`](Self::rows_materialized).
    pub pushdown_declined: Vec<PushdownDeclined>,
    /// What the sink did at [`ExtractionSink::finalize`](super::sink::ExtractionSink::finalize).
    ///
    /// All zero for a sink that resolves relation endpoints eagerly, which reports everything
    /// through [`per_mapping`](Self::per_mapping) instead. See
    /// [`Resolution`](super::sink::Resolution).
    pub finalize: FinalizeReport,
    /// Where the run's wall-clock time went.
    ///
    /// `None` from [`extract`](super::extract::extract) itself, which is handed open providers
    /// and cannot know what they cost to obtain. The runner that owns the connections fills this
    /// in, as the `extraction-dbcon` bindings do. Also kept out of `extract` because
    /// `std::time::Instant` panics on `wasm32-unknown-unknown`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timing: Option<ExtractionTiming>,
}

/// How long a run spent, split by phase, in milliseconds. Schema discovery is a fixed cost a
/// caller holding a catalog can skip, so it is reported apart from the row reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, JsonSchema)]
pub struct ExtractionTiming {
    /// Connecting to each source and reading its schema. Zero when the caller supplied a catalog.
    pub discovery_ms: u64,
    /// Reading rows and emitting entities: `extract` itself.
    pub extraction_ms: u64,
}

/// Why [`extract`](super::extract::extract) could not run at all, or a non-fatal problem
/// recorded in [`ExtractionReport::errors`] while it did.
///
/// Serializable but not deserializable: some variants carry `&'static str` fields (a borrow no
/// deserializer can manufacture), so this only ever crosses a bindings boundary outbound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub enum ExtractionError {
    /// The blueprint failed [`validate`](super::validate::validate), so `extract` refuses to run
    /// it. Fatal.
    Invalid(Vec<ValidationError>),
    /// A `Source` node names a `source_id` with no entry in the `providers` map `extract` was
    /// given. Fatal.
    MissingProvider {
        /// The missing source id.
        source_id: String,
    },
    /// A predicate's or split's regular expression failed to compile. Fatal. `validate` checks
    /// this, so it should not happen when `extract` is called on a validated blueprint.
    InvalidRegex {
        /// The offending pattern.
        pattern: String,
        /// The compiler's message.
        message: String,
    },
    /// A `Join`'s `on` clause named a key column that its input's rows do not carry. Fatal:
    /// dropping the column from the key instead would shorten it, silently turning the join into
    /// a partial cross product.
    JoinKeyColumnMissing {
        /// The `Join` node.
        node: String,
        /// `"left"` or `"right"`.
        side: &'static str,
        /// The key column, as named on that side.
        column: String,
    },
    /// A [`RowProvider`](super::provider::RowProvider) call failed. Fatal: the rows a mapping
    /// needs are simply not available.
    Provider {
        /// The node being read when the failure happened.
        node: String,
        /// The underlying error.
        source: ProviderError,
    },
    /// An [`ExtractionSink`](super::sink::ExtractionSink) call failed for a reason other than a
    /// policy below. Fatal: this is the storage layer failing, and carrying on would leave a
    /// half-written OCEL that reports success.
    Sink {
        /// What was being added when the failure happened.
        context: String,
        /// The underlying error.
        source: SinkError,
    },
    /// Two mappings, or two rows, produced the same rendered id under two different types.
    /// Only reachable under [`IdRendering::Raw`](super::blueprint::IdRendering::Raw). Non-fatal:
    /// the row is dropped (see [`DropReason::IdTypeCollision`]) and the run continues, since
    /// merging the two would fold two distinct entities into one.
    IdTypeCollision {
        /// The mapping whose row collided.
        mapping: MappingRef,
        /// The contested id.
        id: String,
        /// The type this row wanted the id for. The type that already holds it is whatever the
        /// sink reports for that id.
        requested_type: String,
    },
    /// Two mappings declared the same attribute of the same entity type under different value
    /// types. Non-fatal: the declaration is widened to a type covering both (see
    /// [`OCELAttributeType::coalesce`](crate::core::event_data::object_centric::OCELAttributeType::coalesce))
    /// and every row converted to it. Reported because the resulting type is then decided by a
    /// coincidence of two mappings rather than by either author's intent. Names no mapping: the
    /// conflict is a property of the pair.
    ConflictingAttributeType {
        /// `"event"` or `"object"`.
        kind: &'static str,
        /// The entity type.
        type_name: String,
        /// The attribute.
        attribute: String,
        /// The type it was declared with first.
        declared: OCELAttributeType,
        /// The type the later declaration gave it.
        conflicting: OCELAttributeType,
    },
    /// `on_duplicate_object: Error` fired: `id` had already been added by `mapping`. Non-fatal.
    DuplicateObject {
        /// The mapping whose row named the repeat.
        mapping: MappingRef,
        /// The repeated id.
        id: String,
    },
    /// `on_missing_endpoint: Error` fired: `endpoint` named `id`, which could not be resolved.
    /// Non-fatal.
    MissingEndpoint {
        /// The mapping whose row named the endpoint.
        mapping: MappingRef,
        /// Which endpoint (`"event"`, `"object"`, `"source"`, `"target"`, ...).
        endpoint: &'static str,
        /// The unresolved id.
        id: String,
    },
    /// `on_missing_endpoint: Error` fired for a sink that answered
    /// [`Resolution::Deferred`](super::sink::Resolution::Deferred): the same policy violation as
    /// [`MissingEndpoint`](Self::MissingEndpoint), reported once for the whole run instead of
    /// once per endpoint. Non-fatal.
    ///
    /// Such a sink detects the violation at [`finalize`](super::sink::ExtractionSink::finalize),
    /// where the mapping and row that named the endpoint are gone, so this names neither.
    MissingEndpointsAtFinalize {
        /// How many relations the sink could not resolve. Usually equals an eager sink's
        /// [`MissingEndpoint`](Self::MissingEndpoint) count for the same run, but can exceed it:
        /// a deferring sink also stages the inline references of a `Target::Event` whose event
        /// this run dropped, which an eager sink never asks about.
        count: u64,
    },
}

impl std::fmt::Display for ExtractionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExtractionError::Invalid(errs) => write!(f, "blueprint did not validate: {errs:?}"),
            ExtractionError::MissingProvider { source_id } => {
                write!(f, "no provider registered for source '{source_id}'")
            }
            ExtractionError::InvalidRegex { pattern, message } => {
                write!(f, "invalid regular expression '{pattern}': {message}")
            }
            ExtractionError::JoinKeyColumnMissing { node, side, column } => write!(
                f,
                "join '{node}': {side} input has no key column '{column}'"
            ),
            ExtractionError::Provider { node, source } => {
                write!(f, "reading node '{node}' failed: {source}")
            }
            ExtractionError::Sink { context, source } => {
                write!(f, "{context}: {source}")
            }
            ExtractionError::ConflictingAttributeType {
                kind,
                type_name,
                attribute,
                declared,
                conflicting,
            } => write!(
                f,
                "{kind} type '{type_name}': attribute '{attribute}' declared as '{}' and as '{}'",
                declared.to_type_string(),
                conflicting.to_type_string()
            ),
            ExtractionError::IdTypeCollision {
                mapping,
                id,
                requested_type,
            } => write!(
                f,
                "mapping {}: id '{id}' is already taken by an entity of another type, \
                 so no '{requested_type}' could take it",
                mapping.title()
            ),
            ExtractionError::DuplicateObject { mapping, id } => {
                write!(f, "mapping {}: duplicate object id '{id}'", mapping.title())
            }
            ExtractionError::MissingEndpoint {
                mapping,
                endpoint,
                id,
            } => write!(
                f,
                "mapping {}: unresolved {endpoint} '{id}'",
                mapping.title()
            ),
            ExtractionError::MissingEndpointsAtFinalize { count } => {
                write!(f, "{count} relation(s) had an endpoint that never resolved")
            }
        }
    }
}

impl std::error::Error for ExtractionError {}
