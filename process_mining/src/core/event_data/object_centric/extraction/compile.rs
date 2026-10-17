//! Compile a [`Blueprint`] into SQL presenting the OCEL 2.0 surface over the untouched source
//! tables, so a log can be queried without ever being materialised.
//!
//! [`extract`](super::extract::extract) is the reference semantics. A mapping the emitter cannot
//! reproduce becomes a [`CompileError`] naming the mapping and the reason, and the rest of the
//! blueprint still compiles.
//!
//! Each relation is stored as a bare `SELECT` body ([`ViewDef`]), which [`CompiledOcel`] emits
//! three ways: [`CompiledOcel::ddl`] wraps them in `CREATE VIEW`, [`CompiledOcel::with_prelude`]
//! inlines them as a `WITH` prelude in front of an analysis query (needs no DDL right, so it runs
//! against a read-only database), and [`CompiledOcel::materialize_ddl`] emits `CREATE TABLE ... AS`
//! for callers that have write rights.
//!
//! # Preconditions
//!
//! * The catalog describes the kinds the source's values actually have. The extractor decides
//!   literal coercion, identity rendering and join-key matching from the runtime
//!   [`Value`](super::value::Value) in each cell, while the compiler only has
//!   [`ColumnSchema::declared_kind`](super::catalog::ColumnSchema::declared_kind). Those agree
//!   under a statically-typed engine but not under `SQLite`, which stores a type per cell.
//! * Every source table is reachable under its bare name. The emitted SQL names only the table, so
//!   a multi-source blueprint requires the caller to attach or alias the sources into one
//!   namespace.
//! * Text semantics are the engine's, not Rust's, and the two differ at the edges: regular
//!   expressions are handed over verbatim, where Rust's `regex`, RE2 (`DuckDB`) and POSIX AREs
//!   (`PostgreSQL`) disagree on some constructs; and `trim` strips ASCII space where `str::trim`
//!   strips all Unicode whitespace. Either can keep different rows than an extraction.
//! * The compiler is a pure function of blueprint plus catalog: it opens no connection and reads no
//!   row. Domains that have to be measured arrive through [`Catalog::column_domain`].

pub(crate) mod dialect;
pub(crate) mod emit;

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "ocel-duckdb"))]
mod differential;

use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use dialect::SqlDialect;

use emit::{
    attribute_sql, identity_sql, predicate_sql, split_sql, timestamp_sql, Emitter, ROW_ALIAS,
};

use super::blueprint::{
    Blueprint, EventEndpoint, IdRendering, InlineObjectRef, Mapping, MissingEndpointPolicy, NodeOp,
    ObjectEndpoint, Target,
};
use super::catalog::{Catalog, ColumnSchema, TableSchema};
use super::desugar::desugar_with_paths;
use super::expr::{AttributeMapping, ValueExpression};
use super::report::MappingRef;
use super::schema::full_node_schemas;
use crate::core::event_data::object_centric::OCELAttributeType;

/// How many distinct values a column domain may have before a per-type emission refuses it.
///
/// A per-type shape emits one view per type name, so an open-ended column would emit thousands.
/// Exceeding the cap is an error naming the column, never a silent truncation. The consolidated
/// shape carries the type as a column value and has no such limit.
pub const MAX_TYPE_DOMAIN: usize = 512;

/// Relation names the emitter reserves, so a type named after one of them is reported rather
/// than silently overwriting the relation it collides with.
const RESERVED_RELATIONS: &[&str] = &[
    "event",
    "object",
    "event_object",
    "object_object",
    "event_map_type",
    "object_map_type",
];

/// Which OCEL surface the compiler emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub enum EmissionShape {
    /// One `event_<T>` / `object_<T>` view per declared type, plus `event`, `object`,
    /// `event_object`, `object_object` and the two type maps: the OCEL 2.0 layout external tooling
    /// reads.
    #[default]
    PerType,
    /// The `events`/`objects`/`e2o`/`o2o`/`object_attribute_changes`/`event_attr_meta` layout
    /// `rust4pm`'s own reader (`DuckDbLinkedOCEL`) consumes. A type is stored as a column value
    /// rather than encoded into a view name, so no
    /// [`Catalog::column_domain`] lookup is needed and
    /// [`RejectReason::DynamicTypeName`] never fires.
    Consolidated,
}

/// One compiled relation: a name and the bare `SELECT` that defines it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ViewDef {
    /// The relation's name, unquoted.
    pub name: String,
    /// A bare `SELECT` body with no `CREATE` wrapper, so the same text serves a view, a CTE and
    /// a `CREATE TABLE AS`.
    pub body: String,
}

/// Why a mapping could not be compiled to a view. Every variant names something the emitter
/// refused to guess at.
///
/// Serializable but not deserializable: several variants carry `&'static str` fields, so this only
/// ever crosses a bindings boundary outbound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[non_exhaustive]
pub enum RejectReason {
    /// A `Target::Event` with no `id` expression: the extractor mints a fresh `UUID` per row, a
    /// nondeterministic side effect with no relational denotation.
    SynthesizedId {
        /// The absent field.
        field: &'static str,
    },
    /// A type name is read from the data and the catalog supplies no domain for the column it
    /// comes from, so there is no name to put in `CREATE VIEW event_<T>`.
    DynamicTypeName {
        /// The position whose type is dynamic.
        field: &'static str,
        /// Why no domain was available.
        detail: String,
    },
    /// A column domain has more distinct values than [`MAX_TYPE_DOMAIN`], so a per-type shape
    /// would emit one view per value.
    TypeDomainTooLarge {
        /// The column the domain came from.
        column: String,
        /// How many values it has.
        size: usize,
        /// The cap.
        cap: usize,
    },
    /// A type name collides with one of the relations the emitter itself defines.
    ReservedTypeName {
        /// The offending type name.
        name: String,
    },
    /// A mapping reads a node that is not declared.
    UnknownNode {
        /// The node id.
        node: String,
    },
    /// A node's column shape could not be resolved, typically because of an unknown source table.
    UnresolvedNodeSchema {
        /// The node id.
        node: String,
    },
    /// The node graph contains a cycle, so there is no order in which to emit it.
    NodeCycle {
        /// A node id taking part in the cycle.
        node: String,
    },
    /// A node has no columns, and SQL has no zero-column `SELECT`.
    EmptyProjection {
        /// The node id.
        node: String,
    },
    /// A `Union` node with no inputs.
    EmptyUnion {
        /// The node id.
        node: String,
    },
    /// An expression reads a column the node does not have.
    UnknownColumn {
        /// The column name.
        column: String,
        /// Which position referenced it.
        field: &'static str,
    },
    /// The catalog's `col_type` for a column maps to no
    /// [`ValueKind`](super::value::ValueKind), so the rule the extractor applies to that
    /// column's values cannot be decided at compile time.
    UndeclaredColumnKind {
        /// The column name.
        column: String,
        /// The catalog's own type string.
        col_type: String,
        /// Which position referenced it.
        field: &'static str,
    },
    /// A `Float` or `Timestamp` column, which has no
    /// [`Value::canonical_string`](super::value::Value::canonical_string), is used at an identity
    /// position, where it makes the expression `None` for every row.
    UnstableIdentityRendering {
        /// The column name.
        column: String,
        /// The catalog's own type string.
        col_type: String,
        /// Which position referenced it.
        field: &'static str,
    },
    /// A column whose [`Value::display_string`](super::value::Value::display_string) a SQL cast
    /// does not reproduce feeds a position that reads it as text. Rust writes `1` where `DuckDB`
    /// writes `1.0`, and keeps a timestamp's original offset.
    UnstableDisplayRendering {
        /// The column name.
        column: String,
        /// The catalog's own type string.
        col_type: String,
        /// Which position referenced it.
        field: &'static str,
    },
    /// A timestamp is parsed by a `chrono` cascade that is not translated to SQL.
    ResidualTimestamp {
        /// What about it is residual.
        detail: String,
    },
    /// A join key column's declared kind is unknown, so whether the extractor's kind-tagged keys
    /// can match is not decidable at compile time.
    UndecidableJoinKey {
        /// The join node's id.
        node: String,
        /// Which side the column is on.
        side: &'static str,
        /// The column name.
        column: String,
        /// The catalog's own type string.
        col_type: String,
    },
    /// A regular expression does not compile at all.
    InvalidRegex {
        /// The pattern.
        pattern: String,
        /// The compiler's message.
        message: String,
    },
    /// A `Template` has an unterminated or empty placeholder, which makes it `None` for every
    /// row.
    InvalidTemplate {
        /// The template text.
        template: String,
        /// What is wrong with it.
        reason: String,
    },
    /// An attribute's declared type is not the source column's kind, and the coercion
    /// `attribute_value` applies is not one a typed SQL column can reproduce: its fallback on
    /// failure is the cell's own natural value, which has a different type.
    AttributeCoercion {
        /// The attribute name.
        attribute: String,
        /// The source column.
        column: String,
        /// The catalog's own type string.
        col_type: String,
        /// The declared OCEL attribute type.
        declared: &'static str,
    },
    /// A mapping whose type name comes from the data declares an attribute that another mapping
    /// declares under a different type. A statically-named type has its declarations reconciled
    /// before the first row. A data-named one declares lazily, one row at a time, so which type
    /// wins depends on row order.
    DynamicTypeAttributeConflict {
        /// The attribute name.
        attribute: String,
    },
    /// A relation view's dependencies on other relation views form a cycle, so no order exists
    /// in which its `CREATE VIEW` (or CTE, or `CREATE TABLE ... AS`) could run. Only reachable
    /// from a blueprint that skipped [`validate`](super::validate()).
    ViewCycle {
        /// The relation's name.
        view: String,
    },
    /// The blueprint does not [`validate`](super::validate::validate) against the catalog, so
    /// nothing was compiled. One per
    /// [`ValidationError`](super::validate::ValidationError), rendered through its `Display`.
    Invalid {
        /// The rendered validation error.
        detail: String,
    },
}

impl std::fmt::Display for RejectReason {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RejectReason::SynthesizedId { field } => write!(
                f,
                "'{field}' is absent, so ids would be random UUIDs that differ every run. Point \
                 '{field}' at a column that identifies the row."
            ),
            RejectReason::DynamicTypeName { field, detail } => write!(
                f,
                "'{field}' is read from the data and no column domain is available ({detail}), \
                 so the per-type view name is unknown at compile time"
            ),
            RejectReason::TypeDomainTooLarge { column, size, cap } => write!(
                f,
                "the domain of '{column}' has {size} values, above the per-type cap of {cap}"
            ),
            RejectReason::ReservedTypeName { name } => write!(
                f,
                "type name '{name}' collides with a relation the compiler defines"
            ),
            RejectReason::UnknownNode { node } => write!(f, "no node '{node}' is declared"),
            RejectReason::UnresolvedNodeSchema { node } => {
                write!(f, "the column shape of node '{node}' could not be resolved")
            }
            RejectReason::NodeCycle { node } => {
                write!(f, "node '{node}' takes part in a cycle")
            }
            RejectReason::EmptyProjection { node } => write!(
                f,
                "node '{node}' has no columns, and SQL has no zero-column SELECT"
            ),
            RejectReason::EmptyUnion { node } => {
                write!(f, "union node '{node}' has no inputs")
            }
            RejectReason::UnknownColumn { column, field } => write!(
                f,
                "column '{column}' used by '{field}' is not declared for this node"
            ),
            RejectReason::UndeclaredColumnKind {
                column,
                col_type,
                field,
            } => write!(
                f,
                "column '{column}' is declared '{col_type}', which maps to no value kind, so \
                 '{field}' cannot be decided without reading the data"
            ),
            RejectReason::UnstableIdentityRendering {
                column,
                col_type,
                field,
            } => write!(
                f,
                "column '{column}' ({col_type}) has no canonical identity rendering, so \
                 '{field}' is None for every row"
            ),
            RejectReason::UnstableDisplayRendering {
                column,
                col_type,
                field,
            } => write!(
                f,
                "column '{column}' ({col_type}) feeds '{field}' through a text rendering a SQL \
                 cast does not reproduce"
            ),
            RejectReason::ResidualTimestamp { detail } => {
                write!(f, "timestamp is residual: {detail}")
            }
            RejectReason::UndecidableJoinKey {
                node,
                side,
                column,
                col_type,
            } => write!(
                f,
                "join '{node}': the {side} key '{column}' is declared '{col_type}', which maps \
                 to no value kind, so whether the extractor's kind-tagged keys can match is not \
                 decidable at compile time"
            ),
            RejectReason::InvalidRegex { pattern, message } => {
                write!(f, "invalid regular expression '{pattern}': {message}")
            }
            RejectReason::InvalidTemplate { template, reason } => {
                write!(f, "invalid template '{template}': {reason}")
            }
            RejectReason::AttributeCoercion {
                attribute,
                column,
                col_type,
                declared,
            } => write!(
                f,
                "attribute '{attribute}' reads column '{column}' ({col_type}) as '{declared}', a \
                 coercion whose fallback value has a different type than the column it would be \
                 stored in"
            ),
            RejectReason::DynamicTypeAttributeConflict { attribute } => write!(
                f,
                "attribute '{attribute}' is declared under conflicting types, and this mapping's \
                 type name comes from the data, so which declaration wins depends on row order"
            ),
            RejectReason::ViewCycle { view } => write!(
                f,
                "relation '{view}' could not be ordered: its dependencies on other relations \
                 form a cycle"
            ),
            RejectReason::Invalid { detail } => {
                write!(f, "the blueprint does not validate: {detail}")
            }
        }
    }
}

impl std::error::Error for RejectReason {}

/// A mapping that produced no view, and why.
///
/// Compilation never fails wholesale: an uncompilable mapping is skipped and recorded here, and
/// everything else still compiles.
///
/// Serializable but not deserializable: [`RejectReason`] is not, so neither is this.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct CompileError {
    /// The mapping this is about, or `None` for a blueprint-level problem.
    pub mapping: Option<MappingRef>,
    /// Why it could not be compiled.
    pub reason: RejectReason,
}

impl std::fmt::Display for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.mapping {
            Some(m) => write!(f, "{}: {}", m.path, self.reason),
            None => write!(f, "{}", self.reason),
        }
    }
}

impl std::error::Error for CompileError {}

/// What a [`Probe`] guards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub enum ProbeKind {
    /// Two objects claim one id under different types. The extractor keeps the first and reports
    /// the collision, and the views keep both.
    AmbiguousObjectIdentity,
    /// Two events claim one id. See [`Self::AmbiguousObjectIdentity`].
    AmbiguousEventIdentity,
    /// Two rows of one mapping give one object id different static attribute values. The
    /// extractor writes the mapping's first row for an id and ignores later repeats. SQL rows
    /// are unordered, so the views agree only when the repeats carry the same values.
    AmbiguousStaticObjectAttributes,
    /// A type name read from a column has a value outside the domain the catalog supplied, so
    /// the compiled view set is missing the entities carrying it.
    StaleTypeDomain {
        /// The column the domain came from.
        column: String,
    },
}

impl std::fmt::Display for ProbeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProbeKind::AmbiguousObjectIdentity => {
                write!(f, "one object id carries more than one type")
            }
            ProbeKind::AmbiguousEventIdentity => {
                write!(f, "one event id carries more than one event")
            }
            ProbeKind::AmbiguousStaticObjectAttributes => write!(
                f,
                "one object id is given different static attribute values by one mapping"
            ),
            ProbeKind::StaleTypeDomain { column } => write!(
                f,
                "column '{column}' holds a value outside the domain this compile pinned"
            ),
        }
    }
}

/// A data-dependent assumption the compiled relations make, as SQL that returns zero rows when
/// the assumption holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Probe {
    /// The mapping this is about, or `None` for a whole-log check.
    pub mapping: Option<MappingRef>,
    /// What it guards.
    pub kind: ProbeKind,
    /// The check itself, as a `SELECT` returning zero rows when the guard holds.
    pub sql: String,
}

/// A blueprint compiled to SQL.
///
/// Serializable but not deserializable: [`Self::errors`] holds [`CompileError`], which is not, so
/// neither is this. Crosses a bindings boundary outbound only, as a compile binding's return
/// value.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct CompiledOcel {
    dialect: SqlDialect,
    shape: EmissionShape,
    views: Vec<ViewDef>,
    probes: Vec<Probe>,
    errors: Vec<CompileError>,
}

impl CompiledOcel {
    /// Which shape this was compiled for.
    #[must_use]
    pub fn shape(&self) -> EmissionShape {
        self.shape
    }

    /// The relation bodies, in dependency order: a relation another one's body `EXISTS`-checks
    /// against comes first, so every emission path can write this list top to bottom. A relation
    /// whose dependencies could not be ordered is dropped and reported in [`Self::errors`] as
    /// [`RejectReason::ViewCycle`], which only a blueprint that skipped
    /// [`validate`](super::validate()) can reach.
    #[must_use]
    pub fn relations(&self) -> &[ViewDef] {
        &self.views
    }

    /// The probes. Each must return zero rows for the relations to agree with an extraction.
    #[must_use]
    pub fn probes(&self) -> &[Probe] {
        &self.probes
    }

    /// The mappings that produced no view, and why. Never a reason to discard the rest.
    #[must_use]
    pub fn errors(&self) -> &[CompileError] {
        &self.errors
    }

    /// Every relation as a `CREATE VIEW`, in dependency order, each terminated by the dialect's
    /// statement separator.
    #[must_use]
    pub fn ddl(&self) -> String {
        self.statements(|d, v| d.create_view(&v.name, &v.body))
    }

    /// Every relation as a `CREATE TABLE ... AS`, so a relation referenced many times is
    /// computed once instead of re-inlined. Needs write rights, unlike [`Self::with_prelude`].
    #[must_use]
    pub fn materialize_ddl(&self) -> String {
        self.statements(|d, v| d.create_table_as(&v.name, &v.body))
    }

    fn statements(&self, render: impl Fn(SqlDialect, &ViewDef) -> String) -> String {
        self.views
            .iter()
            .map(|v| {
                format!(
                    "{}{}",
                    render(self.dialect, v),
                    self.dialect.statement_separator()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// `analysis_sql` with every relation bound as a `WITH` CTE in dependency order, so an
    /// analysis query can name `event`, `object`, `event_object` and friends without any view
    /// existing. Needs no DDL right, so it runs against a read-only database.
    ///
    /// A relation referenced many times is re-inlined by the engine each time. Prefer
    /// [`Self::ddl`] or [`Self::materialize_ddl`] where that matters.
    #[must_use]
    pub fn with_prelude(&self, analysis_sql: &str) -> String {
        if self.views.is_empty() {
            return analysis_sql.to_string();
        }
        let ctes: Vec<String> = self
            .views
            .iter()
            .map(|v| format!("{} AS (\n{}\n)", self.dialect.quote_ident(&v.name), v.body))
            .collect();
        format!("WITH {}\n{analysis_sql}", ctes.join(",\n"))
    }

    /// Each probe rewritten to run with no views present, by prepending the relation CTEs.
    #[must_use]
    pub fn probe_statements(&self) -> Vec<String> {
        self.probes
            .iter()
            .map(|p| self.with_prelude(&p.sql))
            .collect()
    }
}

/// Compile `blueprint` against `catalog` into SQL.
///
/// Pure: no connection is opened and no row is read. A mapping the emitter cannot reproduce
/// exactly is skipped and recorded in [`CompiledOcel::errors`], and the rest still compiles.
///
/// `catalog` supplies both the column types every rule here is decided from and, for a type name
/// read from a column, the [`column_domain`](super::catalog::Catalog::column_domain) the
/// per-type view names come from.
///
/// [`validate`](super::validate::validate) runs first, as it does in
/// [`extract`](super::extract::extract): a blueprint that does not pass it compiles to no
/// relations and one [`RejectReason::Invalid`] per validation error. Three of `validate`'s rules
/// matter here in particular: the version check (which lives only there), duplicate node ids
/// (which would make the emitter read a node's op and its columns from two different
/// declarations), and node cycles or unknown tables (which would otherwise degrade to a silently
/// empty log).
#[must_use]
pub fn compile(
    blueprint: &Blueprint,
    catalog: &dyn Catalog,
    dialect: SqlDialect,
    shape: EmissionShape,
) -> CompiledOcel {
    let mut out = CompiledOcel {
        dialect,
        shape,
        views: Vec::new(),
        probes: Vec::new(),
        errors: Vec::new(),
    };

    let validation_errors = super::validate::validate(blueprint, catalog);
    if !validation_errors.is_empty() {
        out.errors = validation_errors
            .iter()
            .map(|e| CompileError {
                mapping: None,
                reason: RejectReason::Invalid {
                    detail: e.to_string(),
                },
            })
            .collect();
        return out;
    }

    let full = full_node_schemas(blueprint, catalog);
    let emitter = Emitter::from_schemas(blueprint, &full, dialect);
    let desugared = desugar_with_paths(blueprint);
    let mappings: Vec<(MappingRef, Mapping)> = desugared
        .into_iter()
        .enumerate()
        .map(|(index, (path, m))| (MappingRef::new(index, path, &m), m))
        .collect();

    let attrs = AttributePlan::build(blueprint, catalog, &emitter, &mappings, shape);
    let mut acc = Accumulator::default();

    for (mapping_ref, mapping) in &mappings {
        let mut ctx = MappingCtx {
            dialect,
            shape,
            blueprint,
            catalog,
            emitter: &emitter,
            attrs: &attrs,
            mapping_ref,
            mapping,
            probes: Vec::new(),
        };
        match ctx.compile(&mut acc) {
            Ok(()) => out.probes.append(&mut ctx.probes),
            Err(reason) => out.errors.push(CompileError {
                mapping: Some(mapping_ref.clone()),
                reason,
            }),
        }
    }

    match shape {
        EmissionShape::PerType => assemble(dialect, &attrs, &acc, &mut out),
        EmissionShape::Consolidated => assemble_consolidated(dialect, &attrs, &acc, &mut out),
    }
    out
}

/// The declared attribute type of every `(kind, type name, attribute)` the blueprint produces,
/// reconciled across mappings as [`mapping_exec`](super::mapping_exec) does at extraction time:
/// conflicting declarations widen via [`OCELAttributeType::coalesce`], and every mapping's rows
/// convert to the widened type.
#[derive(Debug, Default)]
struct AttributePlan {
    declared: BTreeMap<(&'static str, String, String), OCELAttributeType>,
    conflicted: BTreeSet<(&'static str, String, String)>,
    /// Event attributes of a mapping whose type name is read from the data under
    /// [`EmissionShape::Consolidated`], where there is no name to key them under. Without them
    /// [`Self::event_columns_wide`] would drop every dynamically-typed event's attributes.
    dynamic_event_attrs: BTreeMap<String, OCELAttributeType>,
}

impl AttributePlan {
    /// Reconcile every mapping's declarations.
    ///
    /// A mapping whose node schema does not resolve, or whose type-name expression [`type_names`]
    /// refuses, is skipped without a [`RejectReason`]: [`MappingCtx::compile`] re-derives the
    /// identical reason and is the only place one is recorded.
    fn build(
        blueprint: &Blueprint,
        catalog: &dyn Catalog,
        emitter: &Emitter<'_, '_>,
        mappings: &[(MappingRef, Mapping)],
        shape: EmissionShape,
    ) -> Self {
        let mut plan = Self::default();
        for (_, m) in mappings {
            let Some(schema) = emitter.schema_of(&m.node) else {
                continue;
            };
            let (kind, type_expr, attributes) = match &m.target {
                Target::Event {
                    event_type,
                    attributes,
                    ..
                } => ("event", event_type, attributes),
                Target::Object {
                    object_type,
                    attributes,
                    ..
                } => ("object", object_type, attributes),
                Target::E2O { .. } | Target::O2O { .. } => continue,
            };
            // `PerType` needs every name a domain enumerates, since each becomes a view. Under
            // `Consolidated` the type is a column value, so only a statically-known (`Constant`)
            // name is ever reconciled here.
            let names = match shape {
                EmissionShape::PerType => {
                    type_names(blueprint, catalog, emitter, &m.node, type_expr)
                        .map(|t| t.names)
                        .unwrap_or_default()
                }
                EmissionShape::Consolidated => static_type_names(type_expr),
            };
            if names.is_empty() && shape == EmissionShape::Consolidated && kind == "event" {
                for a in attributes {
                    let declared = resolve_attribute_type(a, schema);
                    plan.dynamic_event_attrs
                        .entry(a.name.clone())
                        .and_modify(|prev| *prev = prev.coalesce(declared))
                        .or_insert(declared);
                }
            }
            for name in names {
                for a in attributes {
                    let declared = resolve_attribute_type(a, schema);
                    let key = (kind, name.clone(), a.name.clone());
                    match plan.declared.get(&key).copied() {
                        Some(prev) if prev != declared => {
                            plan.declared.insert(key.clone(), prev.coalesce(declared));
                            plan.conflicted.insert(key);
                        }
                        Some(_) => {}
                        None => {
                            plan.declared.insert(key, declared);
                        }
                    }
                }
            }
        }
        plan
    }

    /// The reconciled type for one attribute, falling back to this mapping's own resolution when
    /// nothing was recorded (which only happens for a type name no plan pass saw).
    fn type_of(
        &self,
        kind: &'static str,
        type_name: &str,
        a: &AttributeMapping,
        schema: &TableSchema,
    ) -> OCELAttributeType {
        self.declared
            .get(&(kind, type_name.to_string(), a.name.clone()))
            .copied()
            .unwrap_or_else(|| resolve_attribute_type(a, schema))
    }

    fn is_conflicted(&self, kind: &'static str, type_name: &str, attribute: &str) -> bool {
        self.conflicted
            .contains(&(kind, type_name.to_string(), attribute.to_string()))
    }

    /// Every attribute name declared for `type_name`, sorted: the column list of its per-type
    /// view.
    fn columns_of(&self, kind: &'static str, type_name: &str) -> Vec<(String, OCELAttributeType)> {
        self.declared
            .iter()
            .filter(|((k, t, _), _)| *k == kind && t == type_name)
            .map(|((_, _, a), ty)| (a.clone(), *ty))
            .collect()
    }

    /// The wide `events` table's attribute columns under [`EmissionShape::Consolidated`]: every
    /// declared event attribute name, widened across every event type that declares it (not
    /// merely within one, as [`Self::columns_of`] does), since that table is one row per event.
    /// Mirrors the streaming `DuckDB` sink's `ev_attr_types`.
    fn event_columns_wide(&self) -> Vec<(String, OCELAttributeType)> {
        let mut widened: BTreeMap<String, OCELAttributeType> = self.dynamic_event_attrs.clone();
        for ((kind, _type_name, attribute), ty) in &self.declared {
            if *kind != "event" {
                continue;
            }
            widened
                .entry(attribute.clone())
                .and_modify(|existing| *existing = existing.coalesce(*ty))
                .or_insert(*ty);
        }
        widened.into_iter().collect()
    }
}

/// The statically-known names a type expression can take without consulting a column domain: one
/// for a `Constant`, none for anything else.
fn static_type_names(expr: &ValueExpression) -> Vec<String> {
    match expr {
        ValueExpression::Constant { value } => vec![value.clone()],
        _ => Vec::new(),
    }
}

/// An attribute's declared type: `value_type` if given, else the source column's declared kind,
/// else `String`. Mirrors `resolve_attribute_type` in `mapping_exec`.
fn resolve_attribute_type(a: &AttributeMapping, schema: &TableSchema) -> OCELAttributeType {
    if let Some(t) = a.value_type {
        return t;
    }
    schema
        .columns
        .get(&a.source_column)
        .and_then(ColumnSchema::declared_kind)
        .map(|k| match k {
            super::value::ValueKind::Text => OCELAttributeType::String,
            super::value::ValueKind::Integer => OCELAttributeType::Integer,
            super::value::ValueKind::Float => OCELAttributeType::Float,
            super::value::ValueKind::Boolean => OCELAttributeType::Boolean,
            super::value::ValueKind::Timestamp => OCELAttributeType::Time,
        })
        .unwrap_or(OCELAttributeType::String)
}

/// A type-name expression resolved to the set of names it can take, plus the SQL producing it.
#[derive(Debug)]
struct TypeNames {
    sql: String,
    names: Vec<String>,
    /// `Some(column)` when the names came from a catalog domain rather than a constant, so the
    /// caller can attach the staleness probe.
    domain_column: Option<String>,
}

/// Resolve a type-name expression.
///
/// A `Constant` names one type. Anything else is read from the data, and per-type emission needs
/// the set of names up front, which
/// [`Catalog::column_domain`](super::catalog::Catalog::column_domain) supplies for a `Column`
/// expression tracing back to a single `Source`. The pinned set can go stale, which a
/// [`ProbeKind::StaleTypeDomain`] probe detects.
fn type_names(
    blueprint: &Blueprint,
    catalog: &dyn Catalog,
    emitter: &Emitter<'_, '_>,
    node_id: &str,
    expr: &ValueExpression,
) -> Result<TypeNames, RejectReason> {
    if let ValueExpression::Constant { value } = expr {
        return Ok(TypeNames {
            sql: emitter.dialect.string_literal(value),
            names: vec![value.clone()],
            domain_column: None,
        });
    }
    let ValueExpression::Column { column } = expr else {
        return Err(RejectReason::DynamicTypeName {
            field: "type",
            detail: "only a plain column expression can be given a domain".to_string(),
        });
    };
    let Some((source_id, table)) = source_of(blueprint, node_id) else {
        return Err(RejectReason::DynamicTypeName {
            field: "type",
            detail: format!("node '{node_id}' is not a source or a chain of filters over one"),
        });
    };
    let Some(domain) = catalog.column_domain(&source_id, &table, column) else {
        return Err(RejectReason::DynamicTypeName {
            field: "type",
            detail: format!("no domain for '{table}'.'{column}' in source '{source_id}'"),
        });
    };
    if domain.is_empty() {
        // An empty domain names no view, so every entity of this mapping would vanish while the
        // compile reported nothing.
        return Err(RejectReason::DynamicTypeName {
            field: "type",
            detail: format!(
                "the domain recorded for '{table}'.'{column}' in source '{source_id}' is empty"
            ),
        });
    }
    if domain.len() > MAX_TYPE_DOMAIN {
        return Err(RejectReason::TypeDomainTooLarge {
            column: column.clone(),
            size: domain.len(),
            cap: MAX_TYPE_DOMAIN,
        });
    }
    let schema = emitter
        .schema_of(node_id)
        .ok_or_else(|| RejectReason::UnresolvedNodeSchema {
            node: node_id.to_string(),
        })?;
    Ok(TypeNames {
        sql: identity_sql(emitter.dialect, expr, schema, ROW_ALIAS, "type")?,
        names: domain.iter().cloned().collect(),
        domain_column: Some(column.clone()),
    })
}

/// The `(source_id, table)` a node's rows ultimately come from, following `Filter` chains. A
/// `Join` or `Union` has no single one.
fn source_of(blueprint: &Blueprint, node_id: &str) -> Option<(String, String)> {
    let mut current = node_id.to_string();
    for _ in 0..blueprint.nodes.len().max(1) {
        match &blueprint.node(&current)?.op {
            NodeOp::Source { source_id, table } => return Some((source_id.clone(), table.clone())),
            NodeOp::Filter { input, .. } => current = input.clone(),
            NodeOp::Join { .. } | NodeOp::Union { .. } => return None,
        }
    }
    None
}

/// One event's projection into `event_<T>`.
#[derive(Debug)]
struct EventBranch {
    type_sql: String,
    id_sql: String,
    time_sql: String,
    from: String,
    filters: Vec<String>,
    /// Attribute name to the SQL producing its value on this branch.
    attributes: BTreeMap<String, String>,
    /// Attribute name to the declared type [`Self::attributes`]'s SQL was rendered at, so
    /// [`EmissionShape::Consolidated`]'s wide `events` table can coerce it to a further-widened
    /// column type.
    attribute_types: BTreeMap<String, OCELAttributeType>,
    types: Vec<String>,
}

/// One object's projection into `object`.
#[derive(Debug)]
struct ObjectBranch {
    type_sql: String,
    id_sql: String,
    from: String,
    filters: Vec<String>,
    types: Vec<String>,
    /// Relation names this branch's `filters` semi-join against via `EXISTS`, so the `object`
    /// view built from it is emitted after those relations rather than before.
    depends_on: BTreeSet<String>,
}

/// One `(object, attribute)` observation's projection into `object_<T>`.
#[derive(Debug)]
struct ObjectAttrBranch {
    type_sql: String,
    id_sql: String,
    time_sql: String,
    attribute: String,
    value_sql: String,
    /// The declared type [`Self::value_sql`] was rendered at, so
    /// [`EmissionShape::Consolidated`]'s EAV `object_attribute_changes` can render `value` as text
    /// and record `value_type` alongside it.
    value_type: OCELAttributeType,
    from: String,
    filters: Vec<String>,
    types: Vec<String>,
}

/// One relation's projection into `event_object` or `object_object`.
#[derive(Debug)]
struct RelBranch {
    left_sql: String,
    right_sql: String,
    qualifier_sql: String,
    from: String,
    filters: Vec<String>,
    /// Relation names this branch's `filters` semi-join against via `EXISTS`, so the relation
    /// view built from it is emitted after those relations rather than before.
    depends_on: BTreeSet<String>,
}

/// What an identity relation (`PerType`'s `object` and `event`, `Consolidated`'s `objects`) reads
/// off one branch, so one body builder serves both branch types.
trait EntityBranch {
    fn id_sql(&self) -> &str;
    fn type_sql(&self) -> &str;
    fn from(&self) -> &str;
    fn filters(&self) -> &[String];
}

impl EntityBranch for ObjectBranch {
    fn id_sql(&self) -> &str {
        &self.id_sql
    }
    fn type_sql(&self) -> &str {
        &self.type_sql
    }
    fn from(&self) -> &str {
        &self.from
    }
    fn filters(&self) -> &[String] {
        &self.filters
    }
}

impl EntityBranch for EventBranch {
    fn id_sql(&self) -> &str {
        &self.id_sql
    }
    fn type_sql(&self) -> &str {
        &self.type_sql
    }
    fn from(&self) -> &str {
        &self.from
    }
    fn filters(&self) -> &[String] {
        &self.filters
    }
}

/// A branch that can semi-join against another relation, so whatever is built from it has to be
/// emitted after that relation.
trait BranchDependencies {
    fn depends_on(&self) -> &BTreeSet<String>;
}

impl BranchDependencies for ObjectBranch {
    fn depends_on(&self) -> &BTreeSet<String> {
        &self.depends_on
    }
}

impl BranchDependencies for RelBranch {
    fn depends_on(&self) -> &BTreeSet<String> {
        &self.depends_on
    }
}

#[derive(Debug, Default)]
struct Accumulator {
    events: Vec<EventBranch>,
    objects: Vec<ObjectBranch>,
    object_attrs: Vec<ObjectAttrBranch>,
    e2o: Vec<RelBranch>,
    o2o: Vec<RelBranch>,
    /// Set when at least one object mapping writes static attributes, so the assembler knows the
    /// ambiguity probe is worth emitting.
    static_attr_probe_needed: bool,
}

struct MappingCtx<'a> {
    dialect: SqlDialect,
    shape: EmissionShape,
    blueprint: &'a Blueprint,
    catalog: &'a dyn Catalog,
    emitter: &'a Emitter<'a, 'a>,
    attrs: &'a AttributePlan,
    mapping_ref: &'a MappingRef,
    mapping: &'a Mapping,
    probes: Vec<Probe>,
}

impl MappingCtx<'_> {
    fn compile(&mut self, acc: &mut Accumulator) -> Result<(), RejectReason> {
        let node = &self.mapping.node;
        let schema = self
            .emitter
            .schema_of(node)
            .ok_or_else(|| RejectReason::UnresolvedNodeSchema { node: node.clone() })?
            .clone();
        let from = self
            .dialect
            .derived_table(&self.emitter.node_sql(node)?, ROW_ALIAS);

        let mut base_filters = Vec::new();
        if let Some(when) = &self.mapping.when {
            base_filters.push(predicate_sql(self.dialect, when, &schema, ROW_ALIAS)?);
        }

        match &self.mapping.target {
            Target::Event {
                event_type,
                id,
                timestamp,
                attributes,
                objects,
            } => {
                let id = id
                    .as_ref()
                    .ok_or(RejectReason::SynthesizedId { field: "id" })?;
                self.compile_event(
                    acc,
                    &schema,
                    &from,
                    base_filters,
                    event_type,
                    id,
                    timestamp,
                    attributes,
                    objects,
                )
            }
            Target::Object {
                object_type,
                id,
                timestamp,
                attributes,
            } => self.compile_object(
                acc,
                &schema,
                &from,
                base_filters,
                object_type,
                id,
                timestamp.as_ref(),
                attributes,
            ),
            Target::E2O {
                event,
                object,
                qualifier,
            } => self.compile_e2o(acc, &schema, &from, base_filters, event, object, qualifier),
            Target::O2O {
                source,
                target,
                qualifier,
            } => self.compile_o2o(acc, &schema, &from, base_filters, source, target, qualifier),
        }
    }

    /// The relation an object-existence check (an `E2O`/`O2O`/inline-object endpoint guard) runs
    /// `EXISTS` against, and the column its id lives under: `object`/`ocel_id` for
    /// [`EmissionShape::PerType`], `objects`/`id` for [`EmissionShape::Consolidated`].
    fn object_relation(&self) -> (&'static str, &'static str) {
        match self.shape {
            EmissionShape::PerType => ("object", "ocel_id"),
            EmissionShape::Consolidated => ("objects", "id"),
        }
    }

    /// [`Self::object_relation`] for an `E2O`'s event endpoint.
    fn event_relation(&self) -> (&'static str, &'static str) {
        match self.shape {
            EmissionShape::PerType => ("event", "ocel_id"),
            EmissionShape::Consolidated => ("events", "id"),
        }
    }

    /// [`type_names`] for [`EmissionShape::PerType`]. Under [`EmissionShape::Consolidated`] the
    /// type is a column value with no per-type view to name, so this never calls
    /// [`Catalog::column_domain`] and anything but a `Constant` resolves to SQL with no names.
    fn resolve_type_names(&self, expr: &ValueExpression) -> Result<TypeNames, RejectReason> {
        if self.shape != EmissionShape::Consolidated {
            return type_names(
                self.blueprint,
                self.catalog,
                self.emitter,
                &self.mapping.node,
                expr,
            );
        }
        if let ValueExpression::Constant { value } = expr {
            return Ok(TypeNames {
                sql: self.dialect.string_literal(value),
                names: vec![value.clone()],
                domain_column: None,
            });
        }
        let schema = self.emitter.schema_of(&self.mapping.node).ok_or_else(|| {
            RejectReason::UnresolvedNodeSchema {
                node: self.mapping.node.clone(),
            }
        })?;
        Ok(TypeNames {
            sql: identity_sql(self.dialect, expr, schema, ROW_ALIAS, "type")?,
            names: Vec::new(),
            domain_column: None,
        })
    }

    fn types_of(
        &self,
        kind: &'static str,
        expr: &ValueExpression,
    ) -> Result<TypeNames, RejectReason> {
        let resolved = self.resolve_type_names(expr)?;
        if self.shape == EmissionShape::Consolidated {
            // The type is a column value, not a view name: no collision is possible, and with
            // no domain (see `resolve_type_names`) there is nothing that can go stale.
            return Ok(resolved);
        }
        check_reserved(kind, &resolved.names)?;
        Ok(resolved)
    }

    /// The staleness probe for a type set that came from a catalog domain.
    ///
    /// `filters` are the mapping's final row-level guards, not merely its `when`: a row dropped
    /// for a null id or an unparseable timestamp is absent from the views either way, so
    /// reporting it as stale would fail over rows neither side emits.
    fn push_stale_type_probe(&mut self, types: &TypeNames, from: &str, filters: &[String]) {
        let Some(column) = &types.domain_column else {
            return;
        };
        let listed: Vec<String> = types
            .names
            .iter()
            .map(|n| self.dialect.string_literal(n))
            .collect();
        let mut probe_filters = filters.to_vec();
        probe_filters.push(format!("{} IS NOT NULL", types.sql));
        probe_filters.push(format!("{} NOT IN ({})", types.sql, listed.join(", ")));
        self.probes.push(Probe {
            mapping: Some(self.mapping_ref.clone()),
            kind: ProbeKind::StaleTypeDomain {
                column: column.clone(),
            },
            sql: format!(
                "SELECT DISTINCT {} AS ocel_type FROM {from} WHERE {}",
                types.sql,
                probe_filters.join(" AND ")
            ),
        });
    }

    /// Each attribute's reconciled declared type and the SQL producing its value, in the order
    /// `attributes` gives them. `kind` is `"event"` or `"object"`.
    fn attribute_values(
        &self,
        kind: &'static str,
        types: &TypeNames,
        attributes: &[AttributeMapping],
        schema: &TableSchema,
    ) -> Result<Vec<(OCELAttributeType, String)>, RejectReason> {
        attributes
            .iter()
            .map(|a| {
                for type_name in &types.names {
                    if types.domain_column.is_some()
                        && self.attrs.is_conflicted(kind, type_name, &a.name)
                    {
                        return Err(RejectReason::DynamicTypeAttributeConflict {
                            attribute: a.name.clone(),
                        });
                    }
                }
                // Every name this branch can take shares one reconciled declaration. Picking the
                // first is only ambiguous when they conflict, which the check above rules out.
                let declared = self.attrs.type_of(
                    kind,
                    types.names.first().map_or("", String::as_str),
                    a,
                    schema,
                );
                let sql = attribute_sql(
                    self.dialect,
                    &a.source_column,
                    &a.name,
                    declared,
                    schema,
                    ROW_ALIAS,
                )?;
                Ok((declared, sql))
            })
            .collect()
    }

    /// `render_id`: the raw identity verbatim, or `<type>-<raw>` under
    /// [`IdRendering::TypePrefixed`].
    fn render_id(&self, type_sql: &str, raw_sql: &str) -> String {
        match self.blueprint.id_rendering {
            IdRendering::Raw => raw_sql.to_string(),
            IdRendering::TypePrefixed => self.dialect.concat(&[
                type_sql.to_string(),
                self.dialect.string_literal("-"),
                raw_sql.to_string(),
            ]),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn compile_event(
        &mut self,
        acc: &mut Accumulator,
        schema: &TableSchema,
        from: &str,
        mut filters: Vec<String>,
        event_type: &ValueExpression,
        id: &ValueExpression,
        timestamp: &super::expr::TimestampSource,
        attributes: &[AttributeMapping],
        objects: &[InlineObjectRef],
    ) -> Result<(), RejectReason> {
        let types = self.types_of("event", event_type)?;
        let raw_id = identity_sql(self.dialect, id, schema, ROW_ALIAS, "id")?;
        let time = timestamp_sql(self.dialect, timestamp, schema, ROW_ALIAS)?;

        // `run_event` drops a row whose type does not render, whose id is None or empty, or whose
        // timestamp does not parse.
        filters.push(format!("{} IS NOT NULL", types.sql));
        filters.push(format!("{raw_id} IS NOT NULL"));
        filters.push(format!("{raw_id} <> ''"));
        filters.extend(time.filter(self.dialect));
        self.push_stale_type_probe(&types, from, &filters);

        let id_sql = self.render_id(&types.sql, &raw_id);

        let mut attribute_sqls = BTreeMap::new();
        let mut attribute_types = BTreeMap::new();
        for (a, (declared, sql)) in attributes
            .iter()
            .zip(self.attribute_values("event", &types, attributes, schema)?)
        {
            attribute_sqls.insert(a.name.clone(), sql);
            attribute_types.insert(a.name.clone(), declared);
        }

        for inline in objects {
            self.compile_inline_object(acc, schema, from, &filters, &id_sql, inline)?;
        }

        acc.events.push(EventBranch {
            type_sql: types.sql,
            id_sql,
            time_sql: time.sql(self.dialect),
            from: from.to_string(),
            filters,
            attributes: attribute_sqls,
            attribute_types,
            types: types.names,
        });
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn compile_object(
        &mut self,
        acc: &mut Accumulator,
        schema: &TableSchema,
        from: &str,
        mut filters: Vec<String>,
        object_type: &ValueExpression,
        id: &ValueExpression,
        timestamp: Option<&super::expr::TimestampSource>,
        attributes: &[AttributeMapping],
    ) -> Result<(), RejectReason> {
        let types = self.types_of("object", object_type)?;
        let raw_id = identity_sql(self.dialect, id, schema, ROW_ALIAS, "id")?;
        filters.push(format!("{} IS NOT NULL", types.sql));
        filters.push(format!("{raw_id} IS NOT NULL"));
        filters.push(format!("{raw_id} <> ''"));

        // A change-tracked object stamps each observation with the row's own timestamp. A static
        // one stamps a single observation at the Unix epoch.
        let time_sql = match timestamp {
            Some(ts) => {
                let time = timestamp_sql(self.dialect, ts, schema, ROW_ALIAS)?;
                filters.extend(time.filter(self.dialect));
                time.sql(self.dialect)
            }
            None => self.dialect.epoch_timestamp(),
        };
        self.push_stale_type_probe(&types, from, &filters);
        let id_sql = self.render_id(&types.sql, &raw_id);

        let values = self.attribute_values("object", &types, attributes, schema)?;
        for (a, (declared, value_sql)) in attributes.iter().zip(&values) {
            acc.object_attrs.push(ObjectAttrBranch {
                type_sql: types.sql.clone(),
                id_sql: id_sql.clone(),
                time_sql: time_sql.clone(),
                attribute: a.name.clone(),
                value_sql: value_sql.clone(),
                value_type: *declared,
                from: from.to_string(),
                filters: filters.clone(),
                types: types.names.clone(),
            });
        }

        if timestamp.is_none() && !attributes.is_empty() {
            // Static attributes are written on this mapping's first row for an id and never
            // again. SQL rows are unordered, so the views agree only when the repeats carry the
            // same values.
            acc.static_attr_probe_needed = true;
            let projections: Vec<String> = attributes
                .iter()
                .zip(&values)
                .map(|(a, (_, value_sql))| {
                    format!("{value_sql} AS {}", self.dialect.quote_ident(&a.name))
                })
                .collect();
            let inner = format!(
                "SELECT DISTINCT {id_sql} AS ocel_id, {} FROM {from} WHERE {}",
                projections.join(", "),
                filters.join(" AND ")
            );
            self.probes.push(Probe {
                mapping: Some(self.mapping_ref.clone()),
                kind: ProbeKind::AmbiguousStaticObjectAttributes,
                sql: format!(
                    "SELECT ocel_id FROM {} GROUP BY ocel_id HAVING COUNT(*) > 1",
                    self.dialect.derived_table(&inner, "static_attrs")
                ),
            });
        }

        acc.objects.push(ObjectBranch {
            type_sql: types.sql,
            id_sql,
            from: from.to_string(),
            filters,
            types: types.names,
            // This mapping's own filters are type/id/timestamp checks over its own row, never
            // an `EXISTS` against another relation.
            depends_on: BTreeSet::new(),
        });
        Ok(())
    }

    /// An object endpoint: the rendered id, the type SQL, the `FROM` the split (if any) needs,
    /// and the filters that keep exactly the parts the extractor keeps.
    fn endpoint(
        &mut self,
        schema: &TableSchema,
        from: &str,
        endpoint: &ObjectEndpoint,
        part_column: &str,
        field: &'static str,
    ) -> Result<Endpoint, RejectReason> {
        let raw = identity_sql(self.dialect, &endpoint.id, schema, ROW_ALIAS, field)?;
        let type_sql = match &endpoint.object_type {
            Some(e) => Some(identity_sql(self.dialect, e, schema, ROW_ALIAS, field)?),
            None => None,
        };
        if self.shape != EmissionShape::Consolidated {
            if let Some(t) = &type_sql {
                // Under `Create` and `PerType` this endpoint's type becomes a per-type view of
                // its own. `Consolidated` has no such view to collide with.
                let names = constant_names(t);
                if names.is_empty()
                    && self.blueprint.on_missing_endpoint == MissingEndpointPolicy::Create
                {
                    // `validate` asks only that the type is declared, so it may be read from
                    // the data. A created object would then reach `object` carrying a type no
                    // `object_<T>` view and no `object_map_type` row names.
                    return Err(RejectReason::DynamicTypeName {
                        field,
                        detail: "the missing-endpoint policy creates this object, and a per-type \
                                 shape has no name for the view it would need"
                            .to_string(),
                    });
                }
                check_reserved("object", &names)?;
            }
        }
        let split = split_sql(
            self.dialect,
            from,
            &raw,
            endpoint.split.as_ref(),
            part_column,
        )?;
        let mut id_filters = vec![format!("{raw} IS NOT NULL"), format!("{raw} <> ''")];
        id_filters.extend(split.filters);
        let mut type_filters = Vec::new();
        let id_sql = self.render_endpoint_id(type_sql.as_deref(), &split.part, &mut type_filters);
        Ok(Endpoint {
            from: split.from,
            id_sql,
            type_sql,
            id_filters,
            type_filters,
        })
    }

    /// An endpoint's id: the raw identity verbatim, or `<type>-<raw>` under
    /// [`IdRendering::TypePrefixed`], which needs the type to render and pushes the guard saying
    /// so onto `type_filters`.
    fn render_endpoint_id(
        &self,
        type_sql: Option<&str>,
        raw_sql: &str,
        type_filters: &mut Vec<String>,
    ) -> String {
        match self.blueprint.id_rendering {
            IdRendering::Raw => raw_sql.to_string(),
            IdRendering::TypePrefixed => {
                // `resolve_object_endpoint` drops the row outright when the type is None here.
                let t = type_sql.map_or_else(|| self.dialect.null_text(), str::to_string);
                type_filters.push(format!("{t} IS NOT NULL"));
                self.dialect
                    .concat(&[t, self.dialect.string_literal("-"), raw_sql.to_string()])
            }
        }
    }

    /// The semi-join that drops a relation whose endpoint the extractor's own lookup would have
    /// rejected, plus, under [`MissingEndpointPolicy::Create`], the object branch that creates it
    /// instead. An endpoint that declares a type semi-joins on `(id, type)`, one that does not on
    /// the id alone, matching `resolve_object`.
    ///
    /// `extra_depends` names the relations `extra_filters` already semi-joins against, so a
    /// created object branch inherits them too. `create_from` is the `FROM` such a branch reads,
    /// which is the endpoint's own unless `extra_filters` names a column only a later endpoint's
    /// `FROM` carries.
    fn endpoint_guard(
        &self,
        acc: &mut Accumulator,
        endpoint: &Endpoint,
        create_from: &str,
        extra_filters: &[String],
        extra_depends: &BTreeSet<String>,
    ) -> Option<String> {
        if self.blueprint.on_missing_endpoint == MissingEndpointPolicy::Create {
            // `validate` guarantees a declared type under this policy, so the object always
            // exists once it is created here.
            if let Some(type_sql) = &endpoint.type_sql {
                let mut filters = extra_filters.to_vec();
                filters.extend(endpoint.filters().cloned());
                filters.push(format!("{type_sql} IS NOT NULL"));
                acc.objects.push(ObjectBranch {
                    type_sql: type_sql.clone(),
                    id_sql: endpoint.id_sql.clone(),
                    from: create_from.to_string(),
                    filters,
                    // Recorded so a constant type's view exists even with no entity mapping.
                    // `Self::endpoint` has already refused a dynamic one under a per-type shape.
                    types: constant_names(type_sql),
                    // An endpoint's own filters never carry an `EXISTS`, so the created branch
                    // depends on exactly what `extra_filters` already did.
                    depends_on: extra_depends.clone(),
                });
                return None;
            }
        }
        let (relation, id_column) = self.object_relation();
        Some(self.exists_guard(
            relation,
            id_column,
            &endpoint.id_sql,
            endpoint.type_sql.as_deref(),
        ))
    }

    /// The semi-join that keeps a row only when the relation carrying entity identity already
    /// has this id, and, when the endpoint declares a type, only under that type.
    fn exists_guard(
        &self,
        relation: &str,
        id_column: &str,
        id_sql: &str,
        type_sql: Option<&str>,
    ) -> String {
        let type_column = self.dialect.quote_ident("ocel_type");
        let type_test = match type_sql {
            Some(t) => format!(" AND ({t} IS NULL OR e.{type_column} = {t})"),
            None => String::new(),
        };
        format!(
            "EXISTS (SELECT 1 FROM {} AS e WHERE e.{} = {id_sql}{type_test})",
            self.dialect.quote_ident(relation),
            self.dialect.quote_ident(id_column)
        )
    }

    fn event_endpoint(
        &mut self,
        schema: &TableSchema,
        endpoint: &EventEndpoint,
        field: &'static str,
    ) -> Result<(String, Vec<String>), RejectReason> {
        let raw = identity_sql(self.dialect, &endpoint.id, schema, ROW_ALIAS, field)?;
        let type_sql = match &endpoint.event_type {
            Some(e) => Some(identity_sql(self.dialect, e, schema, ROW_ALIAS, field)?),
            None => None,
        };
        let mut filters = vec![format!("{raw} IS NOT NULL"), format!("{raw} <> ''")];
        let id_sql = self.render_endpoint_id(type_sql.as_deref(), &raw, &mut filters);
        let (relation, id_column) = self.event_relation();
        filters.push(self.exists_guard(relation, id_column, &id_sql, type_sql.as_deref()));
        Ok((id_sql, filters))
    }

    fn compile_inline_object(
        &mut self,
        acc: &mut Accumulator,
        schema: &TableSchema,
        from: &str,
        event_filters: &[String],
        event_id_sql: &str,
        inline: &InlineObjectRef,
    ) -> Result<(), RejectReason> {
        let endpoint = self.endpoint(schema, from, &inline.object, "__part0", "inline object")?;
        let qualifier = self.qualifier_sql(inline.qualifier.as_ref(), schema)?;
        let mut filters = event_filters.to_vec();
        filters.extend(endpoint.filters().cloned());
        // The event's own filters are type/id/timestamp checks over its own row, never an
        // `EXISTS`, so this branch starts with no dependency.
        let mut depends_on = BTreeSet::new();
        if let Some(guard) =
            self.endpoint_guard(acc, &endpoint, &endpoint.from, event_filters, &depends_on)
        {
            filters.push(guard);
            depends_on.insert(self.object_relation().0.to_string());
        }
        acc.e2o.push(RelBranch {
            left_sql: event_id_sql.to_string(),
            right_sql: endpoint.id_sql,
            qualifier_sql: qualifier,
            from: endpoint.from,
            filters,
            depends_on,
        });
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn compile_e2o(
        &mut self,
        acc: &mut Accumulator,
        schema: &TableSchema,
        from: &str,
        mut filters: Vec<String>,
        event: &EventEndpoint,
        object: &ObjectEndpoint,
        qualifier: &Option<ValueExpression>,
    ) -> Result<(), RejectReason> {
        let (event_id, event_filters) = self.event_endpoint(schema, event, "event")?;
        filters.extend(event_filters);
        // `event_endpoint` always emits an `EXISTS` against the event relation, unconditionally
        // of policy.
        let mut depends_on: BTreeSet<String> =
            BTreeSet::from([self.event_relation().0.to_string()]);
        let endpoint = self.endpoint(schema, from, object, "__part0", "object")?;
        let qualifier_sql = self.qualifier_sql(qualifier.as_ref(), schema)?;
        // The event is resolved first: an unresolved one drops the row before the object is
        // even looked at, so a created object must inherit the event's filters too.
        let created_filters = filters.clone();
        let created_depends = depends_on.clone();
        filters.extend(endpoint.filters().cloned());
        if let Some(guard) = self.endpoint_guard(
            acc,
            &endpoint,
            &endpoint.from,
            &created_filters,
            &created_depends,
        ) {
            filters.push(guard);
            depends_on.insert(self.object_relation().0.to_string());
        }
        acc.e2o.push(RelBranch {
            left_sql: event_id,
            right_sql: endpoint.id_sql,
            qualifier_sql,
            from: endpoint.from,
            filters,
            depends_on,
        });
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn compile_o2o(
        &mut self,
        acc: &mut Accumulator,
        schema: &TableSchema,
        from: &str,
        mut filters: Vec<String>,
        source: &ObjectEndpoint,
        target: &ObjectEndpoint,
        qualifier: &Option<ValueExpression>,
    ) -> Result<(), RejectReason> {
        let src = self.endpoint(schema, from, source, "__part0", "source")?;
        // Nesting the target's split over the source's reproduces the extractor's nested loops
        // as a cross product.
        let tgt = self.endpoint(schema, &src.from, target, "__part1", "target")?;
        let qualifier_sql = self.qualifier_sql(qualifier.as_ref(), schema)?;
        // Neither the mapping's own filters nor an endpoint's split filters ever carry an
        // `EXISTS` of their own; only a guard below can add one.
        let mut depends_on: BTreeSet<String> = BTreeSet::new();
        // `run_o2o` returns before it resolves either endpoint when the target's id is absent
        // or splits to nothing, so an object created for the source inherits the target's id
        // guards, but not its type guard: the source is created before the target's type is ever
        // rendered. The created branch reads the target's `FROM`, the only one those guards can be
        // evaluated against.
        let mut source_stage = filters.clone();
        source_stage.extend(tgt.id_filters.iter().cloned());
        filters.extend(src.filters().cloned());
        filters.extend(tgt.filters().cloned());
        if let Some(guard) = self.endpoint_guard(acc, &src, &tgt.from, &source_stage, &depends_on) {
            filters.push(guard);
            depends_on.insert(self.object_relation().0.to_string());
        }
        if let Some(guard) = self.endpoint_guard(acc, &tgt, &tgt.from, &filters, &depends_on) {
            filters.push(guard);
            depends_on.insert(self.object_relation().0.to_string());
        }
        acc.o2o.push(RelBranch {
            left_sql: src.id_sql,
            right_sql: tgt.id_sql,
            qualifier_sql,
            from: tgt.from,
            filters,
            depends_on,
        });
        Ok(())
    }

    /// A missing qualifier is `unwrap_or_default()`, so `''`. A present one that evaluates to
    /// `None` is the same.
    fn qualifier_sql(
        &self,
        qualifier: Option<&ValueExpression>,
        schema: &TableSchema,
    ) -> Result<String, RejectReason> {
        match qualifier {
            None => Ok(self.dialect.string_literal("")),
            Some(e) => {
                let sql = identity_sql(self.dialect, e, schema, ROW_ALIAS, "qualifier")?;
                Ok(self
                    .dialect
                    .coalesce(&[sql, self.dialect.string_literal("")]))
            }
        }
    }
}

/// A resolved object endpoint.
#[derive(Debug)]
struct Endpoint {
    from: String,
    id_sql: String,
    type_sql: Option<String>,
    /// The guards on the raw id and on the split parts.
    id_filters: Vec<String>,
    /// The guard that the type renders, under [`IdRendering::TypePrefixed`] only. Kept apart from
    /// [`Self::id_filters`] because `resolve_object_endpoint` applies it only after the id is in
    /// hand, which a caller reproducing an earlier endpoint's state has to reproduce too.
    type_filters: Vec<String>,
}

impl Endpoint {
    /// Every guard, id before type, in the order `resolve_object_endpoint` applies them.
    fn filters(&self) -> impl Iterator<Item = &String> {
        self.id_filters.iter().chain(&self.type_filters)
    }
}

/// Reject a type name whose per-type view name would collide with a relation the emitter defines,
/// e.g. `object_map_type` is `object_` plus a type literally named `map_type`.
fn check_reserved(kind: &'static str, names: &[String]) -> Result<(), RejectReason> {
    for name in names {
        let view = format!("{kind}_{name}");
        if RESERVED_RELATIONS.contains(&view.as_str()) {
            return Err(RejectReason::ReservedTypeName { name: name.clone() });
        }
    }
    Ok(())
}

/// The single type name a type expression that compiled to a plain string literal denotes.
/// Anything else contributes no statically known name.
fn constant_names(type_sql: &str) -> Vec<String> {
    let trimmed = type_sql
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''));
    match trimmed {
        // A literal's embedded quotes arrive doubled (`string_literal`); a lone quote means
        // this is not a plain string literal after all.
        Some(inner) if !inner.replace("''", "").contains('\'') => {
            vec![inner.replace("''", "'")]
        }
        _ => Vec::new(),
    }
}

fn select(projections: &[String], from: &str, filters: &[String]) -> String {
    let mut sql = format!("SELECT {} FROM {from}", projections.join(", "));
    if !filters.is_empty() {
        sql.push_str(&format!(" WHERE {}", filters.join(" AND ")));
    }
    sql
}

/// A relation's body: `branches` deduplicated under `header`, or a typed empty relation when
/// there is no branch at all, so a downstream reference still type-checks.
fn union_body(
    dialect: SqlDialect,
    header: &[String],
    empty: &[(&str, String)],
    alias: &str,
    branches: &[String],
) -> String {
    if branches.is_empty() {
        return empty_relation(dialect, empty);
    }
    dialect.distinct_select(
        header,
        &dialect.derived_table(&dialect.union_all(branches), alias),
    )
}

/// The `(<id column>, ocel_type)` projection of every entity branch.
fn identity_projections<B: EntityBranch>(branches: &[B], id_column: &str) -> Vec<String> {
    branches
        .iter()
        .map(|b| {
            select(
                &[
                    format!("{} AS {id_column}", b.id_sql()),
                    format!("{} AS ocel_type", b.type_sql()),
                ],
                b.from(),
                b.filters(),
            )
        })
        .collect()
}

/// The two columns every identity relation starts with, as its header and as the typed `NULL`s
/// an empty one stands in with.
fn identity_columns(dialect: SqlDialect, id_column: &str) -> (Vec<String>, Vec<(&str, String)>) {
    (
        vec![id_column.to_string(), "ocel_type".to_string()],
        vec![
            (id_column, dialect.null_text()),
            ("ocel_type", dialect.null_text()),
        ],
    )
}

/// Every relation the branches semi-join against.
fn depends_of<B: BranchDependencies>(branches: &[B]) -> BTreeSet<String> {
    branches
        .iter()
        .flat_map(|b| b.depends_on().iter().cloned())
        .collect()
}

/// The probe saying two entities claim one id, which the extractor answers by dropping one and
/// the views by keeping both.
fn push_ambiguity_probe(
    out: &mut CompiledOcel,
    dialect: SqlDialect,
    relation: &str,
    id_column: &str,
    kind: ProbeKind,
) {
    out.probes.push(Probe {
        mapping: None,
        kind,
        sql: format!(
            "SELECT {id_column} FROM {} GROUP BY {id_column} HAVING COUNT(*) > 1",
            dialect.quote_ident(relation)
        ),
    });
}

fn assemble(dialect: SqlDialect, attrs: &AttributePlan, acc: &Accumulator, out: &mut CompiledOcel) {
    let mut event_types: BTreeSet<String> = BTreeSet::new();
    for b in &acc.events {
        event_types.extend(b.types.iter().cloned());
    }
    let mut object_types: BTreeSet<String> = BTreeSet::new();
    for b in &acc.objects {
        object_types.extend(b.types.iter().cloned());
    }

    // Every view goes here with the relation names its own body semi-joins against, and is
    // reordered into `out.views` by dependency once the whole set is known.
    let mut pending: Vec<(ViewDef, BTreeSet<String>)> = Vec::new();

    let (header, empty) = identity_columns(dialect, "ocel_id");
    pending.push((
        ViewDef {
            name: "object".to_string(),
            body: union_body(
                dialect,
                &header,
                &empty,
                "object_union",
                &identity_projections(&acc.objects, "ocel_id"),
            ),
        },
        depends_of(&acc.objects),
    ));
    if !acc.objects.is_empty() {
        push_ambiguity_probe(
            out,
            dialect,
            "object",
            "ocel_id",
            ProbeKind::AmbiguousObjectIdentity,
        );
    }

    for t in &object_types {
        // `object_type_view` always reads straight off `object`, so this dependency is fixed
        // rather than gathered from a branch.
        pending.push((
            ViewDef {
                name: format!("object_{t}"),
                body: object_type_view(dialect, attrs, acc, t),
            },
            BTreeSet::from(["object".to_string()]),
        ));
    }

    // `Target::Event` never guards against another relation, so `event` never depends on
    // anything. Only `object` can, through the `Create`-policy branches `compile_e2o` leaves
    // depending on `event`.
    pending.push((
        ViewDef {
            name: "event".to_string(),
            body: union_body(
                dialect,
                &header,
                &empty,
                "event_union",
                &identity_projections(&acc.events, "ocel_id"),
            ),
        },
        BTreeSet::new(),
    ));
    if !acc.events.is_empty() {
        push_ambiguity_probe(
            out,
            dialect,
            "event",
            "ocel_id",
            ProbeKind::AmbiguousEventIdentity,
        );
    }

    for t in &event_types {
        let (body, has_rows) = event_type_view(dialect, attrs, acc, t);
        pending.push((
            ViewDef {
                name: format!("event_{t}"),
                body,
            },
            BTreeSet::new(),
        ));
        if has_rows {
            push_ambiguity_probe(
                out,
                dialect,
                &format!("event_{t}"),
                "ocel_id",
                ProbeKind::AmbiguousEventIdentity,
            );
        }
    }

    pending.push((
        ViewDef {
            name: "event_map_type".to_string(),
            body: type_map(dialect, &event_types),
        },
        BTreeSet::new(),
    ));
    pending.push((
        ViewDef {
            name: "object_map_type".to_string(),
            body: type_map(dialect, &object_types),
        },
        BTreeSet::new(),
    ));

    pending.push((
        ViewDef {
            name: "event_object".to_string(),
            body: relation_view(
                dialect,
                &acc.e2o,
                ("ocel_event_id", "ocel_object_id"),
                "ocel_qualifier",
            ),
        },
        depends_of(&acc.e2o),
    ));
    pending.push((
        ViewDef {
            name: "object_object".to_string(),
            body: relation_view(
                dialect,
                &acc.o2o,
                ("ocel_source_id", "ocel_target_id"),
                "ocel_qualifier",
            ),
        },
        depends_of(&acc.o2o),
    ));

    out.views = order_views(pending, &mut out.errors);
}

/// Assemble [`EmissionShape::Consolidated`]'s six relations (`events`, `objects`,
/// `object_attribute_changes`, `e2o`, `o2o` and `event_attr_meta`) from the same `Accumulator`
/// [`assemble`] reads for [`EmissionShape::PerType`]. Every branch was already compiled
/// shape-aware, so this only reshapes already-correct SQL.
#[allow(clippy::too_many_lines)]
fn assemble_consolidated(
    dialect: SqlDialect,
    attrs: &AttributePlan,
    acc: &Accumulator,
    out: &mut CompiledOcel,
) {
    let mut event_types: BTreeSet<String> = BTreeSet::new();
    for b in &acc.events {
        event_types.extend(b.types.iter().cloned());
    }

    let mut pending: Vec<(ViewDef, BTreeSet<String>)> = Vec::new();

    // `objects(id, ocel_type)`.
    let (header, empty) = identity_columns(dialect, "id");
    pending.push((
        ViewDef {
            name: "objects".to_string(),
            body: union_body(
                dialect,
                &header,
                &empty,
                "object_union",
                &identity_projections(&acc.objects, "id"),
            ),
        },
        depends_of(&acc.objects),
    ));
    if !acc.objects.is_empty() {
        push_ambiguity_probe(
            out,
            dialect,
            "objects",
            "id",
            ProbeKind::AmbiguousObjectIdentity,
        );
    }

    // `events(id, ocel_type, "time", <one column per declared event attribute>)`, wide across
    // every event type at once, unlike `PerType`'s bare `event`, which carries no attributes.
    let wide_cols = attrs.event_columns_wide();
    let time_col = dialect.quote_ident("time");
    let mut events_header = vec!["id".to_string(), "ocel_type".to_string(), time_col.clone()];
    events_header.extend(wide_cols.iter().map(|(n, _)| dialect.quote_ident(n)));

    let mut events_empty: Vec<(&str, String)> = vec![
        ("id", dialect.null_text()),
        ("ocel_type", dialect.null_text()),
        ("time", dialect.null_timestamp()),
    ];
    let owned: Vec<(String, String)> = wide_cols
        .iter()
        .map(|(n, t)| (n.clone(), dialect.null_attribute(*t)))
        .collect();
    events_empty.extend(owned.iter().map(|(n, v)| (n.as_str(), v.clone())));

    let event_branches: Vec<String> = acc
        .events
        .iter()
        .map(|b| {
            let mut projections = vec![
                format!("{} AS id", b.id_sql),
                format!("{} AS ocel_type", b.type_sql),
                format!("{} AS {time_col}", b.time_sql),
            ];
            for (name, target_ty) in &wide_cols {
                let quoted = dialect.quote_ident(name);
                // A branch that never declared `name` at all contributes a typed `NULL`,
                // matching a `UNION ALL` branch missing a column elsewhere in this module.
                let value = match (b.attributes.get(name), b.attribute_types.get(name)) {
                    (Some(sql), Some(&from_ty)) => {
                        coerce_attr_sql(dialect, sql, from_ty, *target_ty)
                    }
                    _ => dialect.null_attribute(*target_ty),
                };
                projections.push(format!("{value} AS {quoted}"));
            }
            select(&projections, &b.from, &b.filters)
        })
        .collect();
    pending.push((
        ViewDef {
            name: "events".to_string(),
            body: union_body(
                dialect,
                &events_header,
                &events_empty,
                "event_union",
                &event_branches,
            ),
        },
        BTreeSet::new(),
    ));
    if !acc.events.is_empty() {
        push_ambiguity_probe(
            out,
            dialect,
            "events",
            "id",
            ProbeKind::AmbiguousEventIdentity,
        );
    }

    // `event_attr_meta(event_type, attr_name, attr_type)`, best-effort: only a statically known
    // event type contributes a row. A dynamically-typed mapping's attribute values still land in
    // `events`, only this metadata is incomplete for it.
    let mut meta_rows: Vec<String> = Vec::new();
    for t in &event_types {
        for (name, ty) in attrs.columns_of("event", t) {
            meta_rows.push(format!(
                "SELECT {} AS event_type, {} AS attr_name, {} AS attr_type",
                dialect.string_literal(t),
                dialect.string_literal(&name),
                dialect.string_literal(ty.as_type_str())
            ));
        }
    }
    let event_attr_meta_body = if meta_rows.is_empty() {
        empty_relation(
            dialect,
            &[
                ("event_type", dialect.null_text()),
                ("attr_name", dialect.null_text()),
                ("attr_type", dialect.null_text()),
            ],
        )
    } else {
        dialect.union_all(&meta_rows)
    };
    pending.push((
        ViewDef {
            name: "event_attr_meta".to_string(),
            body: event_attr_meta_body,
        },
        BTreeSet::new(),
    ));

    // `object_attribute_changes(id, name, "time", value, value_type)`, one row per attribute
    // observation, exactly `acc.object_attrs`. Unlike `PerType`'s `object_<T>` there is no
    // existence row: `objects` alone already carries identity.
    let value_col = dialect.quote_ident("value");
    let value_type_col = dialect.quote_ident("value_type");
    let name_col = dialect.quote_ident("name");
    let object_attr_branches: Vec<String> = acc
        .object_attrs
        .iter()
        .map(|b| {
            let text = attr_value_as_text(dialect, &b.value_sql, b.value_type);
            let empty = dialect.string_literal("");
            // A `NULL` cell is a recorded observation of `Null`, not an absent attribute.
            // `'null'` is outside `OCELAttributeType::as_type_str`'s outputs, so `from_sql_value`
            // reconstructs `OCELAttributeValue::Null` regardless of `value`, never conflating a
            // `Null` observation with an empty-string one.
            let value_type_sql = format!(
                "CASE WHEN {} IS NULL THEN {} ELSE {} END",
                b.value_sql,
                dialect.string_literal("null"),
                dialect.string_literal(b.value_type.as_type_str())
            );
            let projections = vec![
                format!("{} AS id", b.id_sql),
                format!("{} AS {name_col}", dialect.string_literal(&b.attribute)),
                format!("{} AS {time_col}", b.time_sql),
                format!("COALESCE({text}, {empty}) AS {value_col}"),
                format!("{value_type_sql} AS {value_type_col}"),
            ];
            select(&projections, &b.from, &b.filters)
        })
        .collect();
    pending.push((
        ViewDef {
            name: "object_attribute_changes".to_string(),
            body: union_body(
                dialect,
                &[
                    "id".to_string(),
                    name_col,
                    time_col,
                    value_col,
                    value_type_col,
                ],
                &[
                    ("id", dialect.null_text()),
                    ("name", dialect.null_text()),
                    ("time", dialect.null_timestamp()),
                    ("value", dialect.null_text()),
                    ("value_type", dialect.null_text()),
                ],
                "object_attr_union",
                &object_attr_branches,
            ),
        },
        BTreeSet::new(),
    ));

    // `e2o(event_id, object_id, qualifier)` / `o2o(source_id, target_id, qualifier)`.
    pending.push((
        ViewDef {
            name: "e2o".to_string(),
            body: relation_view(dialect, &acc.e2o, ("event_id", "object_id"), "qualifier"),
        },
        depends_of(&acc.e2o),
    ));
    pending.push((
        ViewDef {
            name: "o2o".to_string(),
            body: relation_view(dialect, &acc.o2o, ("source_id", "target_id"), "qualifier"),
        },
        depends_of(&acc.o2o),
    ));

    out.views = order_views(pending, &mut out.errors);
}

/// Reorder `pending` so every view comes after each view named in its own dependency set, using
/// Kahn's algorithm in the same style as the cycle sweep `validate.rs` runs over the node graph,
/// but building an order instead of only detecting a cycle.
///
/// A cycle cannot arise from the emission rules alone. Only a blueprint that skipped
/// [`validate`](super::validate) can reach one, by omitting the endpoint type declaration
/// [`MissingEndpointPolicy::Create`] requires and so making `object` depend on itself.
/// Unorderable views are reported as [`RejectReason::ViewCycle`] rather than emitted out of order.
fn order_views(
    pending: Vec<(ViewDef, BTreeSet<String>)>,
    errors: &mut Vec<CompileError>,
) -> Vec<ViewDef> {
    let mut remaining = pending;
    let mut ordered: Vec<ViewDef> = Vec::with_capacity(remaining.len());
    let mut ordered_names: BTreeSet<String> = BTreeSet::new();
    loop {
        let (ready, blocked): (Vec<_>, Vec<_>) = remaining
            .into_iter()
            .partition(|(_, deps)| deps.iter().all(|d| ordered_names.contains(d)));
        if ready.is_empty() {
            remaining = blocked;
            break;
        }
        for (view, _) in ready {
            ordered_names.insert(view.name.clone());
            ordered.push(view);
        }
        remaining = blocked;
    }
    for (view, _) in remaining {
        errors.push(CompileError {
            mapping: None,
            reason: RejectReason::ViewCycle { view: view.name },
        });
    }
    ordered
}

/// `event_<T>(ocel_id, ocel_time, <one column per declared attribute>)`, returning whether any
/// branch actually contributes rows.
fn event_type_view(
    dialect: SqlDialect,
    attrs: &AttributePlan,
    acc: &Accumulator,
    type_name: &str,
) -> (String, bool) {
    let columns = attrs.columns_of("event", type_name);
    let mut header: Vec<String> = vec!["ocel_id".to_string(), "ocel_time".to_string()];
    header.extend(columns.iter().map(|(n, _)| dialect.quote_ident(n)));

    let branches: Vec<String> = acc
        .events
        .iter()
        .filter(|b| b.types.iter().any(|t| t == type_name))
        .map(|b| {
            let mut projections = vec![
                format!("{} AS ocel_id", b.id_sql),
                format!("{} AS ocel_time", b.time_sql),
            ];
            for (name, ty) in &columns {
                let quoted = dialect.quote_ident(name);
                let value = b
                    .attributes
                    .get(name)
                    .cloned()
                    .unwrap_or_else(|| dialect.null_attribute(*ty));
                projections.push(format!("{value} AS {quoted}"));
            }
            let mut filters = b.filters.clone();
            // A branch whose type is read from a column contributes to several views.
            filters.push(format!(
                "{} = {}",
                b.type_sql,
                dialect.string_literal(type_name)
            ));
            select(&projections, &b.from, &filters)
        })
        .collect();

    let mut empty: Vec<(&str, String)> = vec![
        ("ocel_id", dialect.null_text()),
        ("ocel_time", dialect.null_timestamp()),
    ];
    let owned: Vec<(String, String)> = columns
        .iter()
        .map(|(n, t)| (n.clone(), dialect.null_attribute(*t)))
        .collect();
    empty.extend(owned.iter().map(|(n, v)| (n.as_str(), v.clone())));

    let has_rows = !branches.is_empty();
    (
        union_body(dialect, &header, &empty, "event_type_union", &branches),
        has_rows,
    )
}

/// `object_<T>(ocel_id, ocel_time, ocel_changed_field, <one column per declared attribute>)`.
///
/// Every attribute observation is a row naming its attribute in `ocel_changed_field`, static and
/// change-tracked alike, plus one `ocel_changed_field IS NULL` row per object carrying its
/// existence. An OCEL 2.0 exporter would instead put a static object's values on the `NULL` row,
/// conflating "observed as `Null`" with "never declared", a distinction the extractor draws.
fn object_type_view(
    dialect: SqlDialect,
    attrs: &AttributePlan,
    acc: &Accumulator,
    type_name: &str,
) -> String {
    let columns = attrs.columns_of("object", type_name);
    let mut header: Vec<String> = vec![
        "ocel_id".to_string(),
        "ocel_time".to_string(),
        "ocel_changed_field".to_string(),
    ];
    header.extend(columns.iter().map(|(n, _)| dialect.quote_ident(n)));

    let mut projections = vec![
        "ocel_id".to_string(),
        format!("{} AS ocel_time", dialect.epoch_timestamp()),
        format!("{} AS ocel_changed_field", dialect.null_text()),
    ];
    for (name, ty) in &columns {
        projections.push(format!(
            "{} AS {}",
            dialect.null_attribute(*ty),
            dialect.quote_ident(name)
        ));
    }
    let mut branches = vec![select(
        &projections,
        &dialect.quote_ident("object"),
        &[format!("ocel_type = {}", dialect.string_literal(type_name))],
    )];

    for b in acc
        .object_attrs
        .iter()
        .filter(|b| b.types.iter().any(|t| t == type_name))
    {
        let mut projections = vec![
            format!("{} AS ocel_id", b.id_sql),
            format!("{} AS ocel_time", b.time_sql),
            format!(
                "{} AS ocel_changed_field",
                dialect.string_literal(&b.attribute)
            ),
        ];
        for (name, ty) in &columns {
            let value = if *name == b.attribute {
                b.value_sql.clone()
            } else {
                dialect.null_attribute(*ty)
            };
            projections.push(format!("{value} AS {}", dialect.quote_ident(name)));
        }
        let mut filters = b.filters.clone();
        filters.push(format!(
            "{} = {}",
            b.type_sql,
            dialect.string_literal(type_name)
        ));
        branches.push(select(&projections, &b.from, &filters));
    }

    dialect.distinct_select(
        &header,
        &dialect.derived_table(&dialect.union_all(&branches), "object_type_union"),
    )
}

fn relation_view(
    dialect: SqlDialect,
    rows: &[RelBranch],
    cols: (&str, &str),
    qualifier_col: &str,
) -> String {
    let (left, right) = cols;
    let (left_col, right_col) = (dialect.quote_ident(left), dialect.quote_ident(right));
    let qualifier = dialect.quote_ident(qualifier_col);
    let branches: Vec<String> = rows
        .iter()
        .map(|r| {
            select(
                &[
                    format!("{} AS {left_col}", r.left_sql),
                    format!("{} AS {right_col}", r.right_sql),
                    format!("{} AS {qualifier}", r.qualifier_sql),
                ],
                &r.from,
                &r.filters,
            )
        })
        .collect();
    union_body(
        dialect,
        &[left_col, right_col, qualifier],
        &[
            (left, dialect.null_text()),
            (right, dialect.null_text()),
            (qualifier_col, dialect.null_text()),
        ],
        "rel_union",
        &branches,
    )
}

fn type_map(dialect: SqlDialect, types: &BTreeSet<String>) -> String {
    if types.is_empty() {
        return empty_relation(
            dialect,
            &[
                ("ocel_type", dialect.null_text()),
                ("ocel_type_map", dialect.null_text()),
            ],
        );
    }
    let rows: Vec<String> = types
        .iter()
        .map(|t| {
            format!(
                "SELECT {0} AS ocel_type, {0} AS ocel_type_map",
                dialect.string_literal(t)
            )
        })
        .collect();
    dialect.union_all(&rows)
}

/// An always-empty relation with typed placeholder columns, so a downstream reference still
/// type-checks.
fn empty_relation(dialect: SqlDialect, cols: &[(&str, String)]) -> String {
    let projections: Vec<String> = cols
        .iter()
        .map(|(name, null_literal)| format!("{null_literal} AS {}", dialect.quote_ident(name)))
        .collect();
    format!(
        "SELECT {} WHERE {}",
        projections.join(", "),
        dialect.false_predicate()
    )
}

/// Render a `declared`-typed SQL expression as the text `to_sql_value` would produce, so that
/// `from_sql_value`, which
/// `DuckDbLinkedOCEL`'s
/// reader uses, parses it back to the identical value.
///
/// `Integer`/`Float` use the engine's round-trip-safe `CAST(.. AS VARCHAR)`, which need not match
/// Rust's `Display` digit-for-digit, only parse back to the identical value.
fn attr_value_as_text(dialect: SqlDialect, expr: &str, declared: OCELAttributeType) -> String {
    use OCELAttributeType as A;
    match declared {
        A::String | A::Null => expr.to_string(),
        A::Integer | A::Float => dialect.cast_to_text(expr),
        A::Boolean => dialect.bool_to_text(expr),
        A::Time => dialect.timestamptz_to_iso_text(expr),
    }
}

/// Coerce a `from`-typed SQL expression to `to`, mirroring [`OCELAttributeType::coalesce`]:
/// identical types are a no-op, `Integer` widens into `Float`, anything else widens into `String`
/// via [`attr_value_as_text`]. Used for [`EmissionShape::Consolidated`]'s wide `events` columns,
/// which may hold values several event types declared under different-but-reconciled types.
fn coerce_attr_sql(
    dialect: SqlDialect,
    expr: &str,
    from: OCELAttributeType,
    to: OCELAttributeType,
) -> String {
    use OCELAttributeType as A;
    if from == to {
        return expr.to_string();
    }
    if from == A::Integer && to == A::Float {
        return format!("CAST({expr} AS DOUBLE)");
    }
    debug_assert_eq!(
        to,
        A::String,
        "OCELAttributeType::coalesce widens any mismatch other than Integer/Float to String"
    );
    attr_value_as_text(dialect, expr, from)
}
