//! The blueprint itself: a node graph producing rows, and mappings turning rows into entities.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::expr::{AttributeMapping, SplitSpec, TimestampSource, ValueExpression};
use super::predicate::Predicate;
use super::MODEL_VERSION;

/// How entity ids are rendered, for events and objects alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum IdRendering {
    /// Use the id expression's value verbatim.
    #[default]
    Raw,
    /// Prefix the id with its type name, so ids from different types cannot collide.
    ///
    /// Under this setting every relation endpoint must declare its type, since otherwise the
    /// prefixed id cannot be rebuilt. Validation enforces that.
    TypePrefixed,
}

/// What to do when a relation names an entity that does not exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum MissingEndpointPolicy {
    /// Skip the relation.
    #[default]
    Drop,
    /// Create the object. Requires the endpoint to declare its type.
    Create,
    /// Record an error.
    Error,
}

/// What to do when an object id is produced more than once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum DuplicateObjectPolicy {
    /// Keep the first, and count the rest as deduplicated rather than lost.
    #[default]
    FirstWins,
    /// Record an error.
    Error,
}

/// One operation in the row graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum NodeOp {
    /// Read a table from a source.
    Source {
        /// Source id, resolved to a connection at execution time.
        source_id: String,
        /// Table name.
        table: String,
    },
    /// Keep the rows of `input` satisfying `condition`.
    Filter {
        /// Input node id.
        input: String,
        /// The condition.
        condition: Predicate,
    },
    /// Inner-join two nodes on the given column pairs.
    Join {
        /// Left input node id.
        left: String,
        /// Right input node id.
        right: String,
        /// Column pairs, as `(left column, right column)`.
        on: Vec<(String, String)>,
    },
    /// Concatenate the rows of several nodes, aligning columns by name.
    ///
    /// `UNION ALL`, not `UNION`: dropping duplicates would drop the entities they produce. Output
    /// columns are the union of the inputs' column names, and an input lacking one contributes
    /// `Null`. Both are model semantics a compiler must reproduce.
    Union {
        /// Input node ids.
        inputs: Vec<String>,
    },
}

/// A node in the row graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Node {
    /// Unique id, referenced by other nodes and by mappings.
    pub id: String,
    /// Display label. No semantic role.
    pub label: Option<String>,
    /// The operation.
    pub op: NodeOp,
}

/// A reference to an object, used at every position where one is named.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ObjectEndpoint {
    /// The object's id.
    pub id: ValueExpression,
    /// The object's type. Required under [`IdRendering::TypePrefixed`] and under
    /// [`MissingEndpointPolicy::Create`].
    pub object_type: Option<ValueExpression>,
    /// Split the id cell into several ids, producing one relation per part.
    pub split: Option<SplitSpec>,
}

/// A reference to an event. Mirrors [`ObjectEndpoint`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EventEndpoint {
    /// The event's id.
    pub id: ValueExpression,
    /// The event's type. Required under [`IdRendering::TypePrefixed`].
    pub event_type: Option<ValueExpression>,
}

/// An object related to an event declared by the same mapping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct InlineObjectRef {
    /// The object.
    pub object: ObjectEndpoint,
    /// Relation qualifier.
    pub qualifier: Option<ValueExpression>,
}

/// What a mapping produces from a row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Target {
    /// An event.
    Event {
        /// Event type.
        event_type: ValueExpression,
        /// Event id. `None` assigns a UUID, which is not reproducible across runs and cannot
        /// be compiled to a view.
        ///
        /// It is also what coalesces a fan-out: over a join of orders and their items, a `None`
        /// id makes one event per item, an id naming the order one event per order, still related
        /// to every item. The repeated rows count as
        /// [`MappingStats::deduplicated`](super::report::MappingStats::deduplicated) while
        /// `objects` below is emitted for each.
        id: Option<ValueExpression>,
        /// When it happened.
        timestamp: TimestampSource,
        /// Event attributes.
        #[serde(default)]
        attributes: Vec<AttributeMapping>,
        /// Objects related to this event.
        #[serde(default)]
        objects: Vec<InlineObjectRef>,
    },
    /// An object.
    Object {
        /// Object type.
        object_type: ValueExpression,
        /// Object id.
        id: ValueExpression,
        /// When the attribute values below were observed. `None` records them as static
        /// values stamped at the Unix epoch.
        #[serde(default)]
        timestamp: Option<TimestampSource>,
        /// Object attributes.
        #[serde(default)]
        attributes: Vec<AttributeMapping>,
    },
    /// An event-to-object relation.
    #[serde(rename = "e2o")]
    E2O {
        /// The event.
        event: EventEndpoint,
        /// The object.
        object: ObjectEndpoint,
        /// Relation qualifier.
        qualifier: Option<ValueExpression>,
    },
    /// An object-to-object relation.
    #[serde(rename = "o2o")]
    O2O {
        /// The source object.
        source: ObjectEndpoint,
        /// The target object.
        target: ObjectEndpoint,
        /// Relation qualifier.
        qualifier: Option<ValueExpression>,
    },
}

/// One mapping from a node's rows to entities.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Mapping {
    /// The node whose rows this reads.
    pub node: String,
    /// Display label, also used to name this mapping in diagnostics.
    pub label: Option<String>,
    /// Only rows satisfying this produce anything. `None` accepts every row.
    pub when: Option<Predicate>,
    /// What to produce.
    pub target: Target,
}

/// A mapping, or an ordered group of them.
///
/// `Single` is not boxed: it is the overwhelmingly common case, and callers construct and match
/// it directly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[allow(clippy::large_enum_variant)]
pub enum MappingEntry {
    /// One independent mapping.
    Single(Mapping),
    /// Mappings tried in order, where the first match wins.
    ///
    /// Surface sugar: desugaring rewrites each guard to exclude the earlier ones, so nothing
    /// downstream of validation sees this variant.
    Ordered {
        /// Mappings, in priority order.
        mappings: Vec<Mapping>,
    },
}

/// A declarative mapping from relational rows to an OCEL.
///
/// Carries no connection details and no schema snapshot: both are supplied by the caller, which
/// keeps a blueprint portable, shareable and free of secrets.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Blueprint {
    /// Schema version. Checked against [`super::MODEL_VERSION`] during validation.
    pub version: u32,
    /// How entity ids are rendered.
    #[serde(default)]
    pub id_rendering: IdRendering,
    /// The row graph.
    pub nodes: Vec<Node>,
    /// The mappings.
    pub mappings: Vec<MappingEntry>,
    /// What to do about relations naming a missing entity.
    #[serde(default)]
    pub on_missing_endpoint: MissingEndpointPolicy,
    /// What to do about a repeated object id.
    #[serde(default)]
    pub on_duplicate_object: DuplicateObjectPolicy,
}

impl Blueprint {
    /// The node with this id, if any.
    #[must_use]
    pub fn node(&self, id: &str) -> Option<&Node> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Parse a blueprint from JSON, checking its declared `version` before attempting to parse
    /// the body.
    ///
    /// Checking `version` first is what turns a document from a newer build into an "unsupported
    /// version" message rather than serde's "unknown variant" naming a construct the caller can do
    /// nothing about. An unknown variant within a supported version still fails.
    ///
    /// # Errors
    /// Returns [`BlueprintParseError::UnsupportedVersion`] when `version` exceeds
    /// [`MODEL_VERSION`], or [`BlueprintParseError::Malformed`] when `input` is not valid JSON,
    /// has no `version` field, or otherwise does not parse as a `Blueprint`.
    pub fn from_json(input: &str) -> Result<Blueprint, BlueprintParseError> {
        // Probed off the parsed tree rather than by a second `from_str`, so the common case does
        // not pay a full extra parse.
        let document: serde_json::Value =
            serde_json::from_str(input).map_err(BlueprintParseError::Malformed)?;
        if let Some(found) = document.get("version").and_then(serde_json::Value::as_u64) {
            if found > u64::from(MODEL_VERSION) {
                return Err(BlueprintParseError::UnsupportedVersion {
                    found: u32::try_from(found).unwrap_or(u32::MAX),
                    supported: MODEL_VERSION,
                });
            }
        }
        serde_json::from_value(document).map_err(BlueprintParseError::Malformed)
    }
}

/// Why [`Blueprint::from_json`] failed.
#[derive(Debug)]
pub enum BlueprintParseError {
    /// The document's `version` exceeds what this build supports. Returned before the body is
    /// parsed, so an unknown construct in the body never masks this as a generic serde error.
    UnsupportedVersion {
        /// The document's `version`.
        found: u32,
        /// The newest version this build reads, [`MODEL_VERSION`].
        supported: u32,
    },
    /// `input` was not valid JSON, had no `version` field, or otherwise did not parse as a
    /// `Blueprint`. An unknown variant within a supported version lands here too: that is a parse
    /// failure, not something the version check is meant to catch.
    Malformed(serde_json::Error),
}

impl std::fmt::Display for BlueprintParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlueprintParseError::UnsupportedVersion { found, supported } => write!(
                f,
                "blueprint version {found} is newer than the supported version {supported}"
            ),
            BlueprintParseError::Malformed(e) => write!(f, "malformed blueprint: {e}"),
        }
    }
}

impl std::error::Error for BlueprintParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BlueprintParseError::UnsupportedVersion { .. } => None,
            BlueprintParseError::Malformed(e) => Some(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ACCOUNT_MOVE: &str = r#"{
      "version": 1,
      "id_rendering": "type-prefixed",
      "on_missing_endpoint": "create",
      "on_duplicate_object": "first-wins",
      "nodes": [
        { "id": "account_move",
          "op": { "type": "source", "source_id": "odoo", "table": "account_move" } }
      ],
      "mappings": [
        { "type": "single", "node": "account_move",
          "when": { "type": "compare",
                    "left": { "type": "column", "column": "move_type" },
                    "op": "eq",
                    "right": { "type": "literal", "value": "out_invoice" } },
          "target": { "type": "object",
                      "object_type": { "type": "constant", "value": "customer_invoice" },
                      "id": { "type": "column", "column": "id" },
                      "attributes": [] } }
      ]
    }"#;

    #[test]
    fn parses_the_discriminated_table_example_from_the_spec() {
        let bp: Blueprint = serde_json::from_str(ACCOUNT_MOVE).expect("parse");
        assert_eq!(bp.version, 1);
        assert_eq!(bp.id_rendering, IdRendering::TypePrefixed);
        assert_eq!(bp.nodes.len(), 1);
        assert!(bp.node("account_move").is_some());
        assert!(bp.node("nope").is_none());
    }

    #[test]
    fn round_trips_through_json_unchanged() {
        let bp: Blueprint = serde_json::from_str(ACCOUNT_MOVE).expect("parse");
        let again: Blueprint =
            serde_json::from_str(&serde_json::to_string(&bp).expect("serialize")).expect("reparse");
        assert_eq!(bp, again);
    }

    #[test]
    fn an_unknown_field_is_ignored_rather_than_rejected() {
        // Forward compatibility: an older build must degrade, not refuse.
        let json = ACCOUNT_MOVE.replace(r#""version": 1,"#, r#""version": 1, "future_field": 42,"#);
        assert!(serde_json::from_str::<Blueprint>(&json).is_ok());
    }

    #[test]
    fn from_json_parses_a_valid_v1_document() {
        let bp = Blueprint::from_json(ACCOUNT_MOVE).expect("parse");
        assert_eq!(bp.version, 1);
        assert!(bp.node("account_move").is_some());
    }

    #[test]
    fn from_json_reports_an_unsupported_version_before_touching_the_body() {
        // The body uses a node op this build does not implement at all: plain
        // serde_json::from_str::<Blueprint> would fail with "unknown variant", masking the
        // actionable "unsupported version" message from_json exists to give instead.
        let json = r#"{
          "version": 999,
          "nodes": [
            { "id": "a", "op": { "type": "a-future-node-op-this-build-does-not-know" } }
          ],
          "mappings": []
        }"#;
        let err = Blueprint::from_json(json).expect_err("must reject a future version");
        assert!(
            matches!(
                err,
                BlueprintParseError::UnsupportedVersion {
                    found: 999,
                    supported: 1
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn from_json_still_reports_an_unknown_variant_within_a_supported_version() {
        // A supported-version document using a construct this build does not implement must
        // still fail loudly, naming the offending variant. This build cannot execute what it
        // does not implement, and serde's message is the right diagnostic.
        let json = r#"{
          "version": 1,
          "nodes": [
            { "id": "a", "op": { "type": "a-future-node-op-this-build-does-not-know" } }
          ],
          "mappings": []
        }"#;
        let err = Blueprint::from_json(json).expect_err("must reject an unknown variant");
        let BlueprintParseError::Malformed(inner) = &err else {
            panic!("expected Malformed, got {err:?}");
        };
        assert!(
            inner
                .to_string()
                .contains("a-future-node-op-this-build-does-not-know"),
            "error should name the unknown variant: {inner}"
        );
    }
}
