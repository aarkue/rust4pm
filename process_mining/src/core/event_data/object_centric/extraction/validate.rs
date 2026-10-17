//! Check a blueprint against a catalog before anything runs.
//!
//! Every rule here is decidable from the blueprint plus declared schema, with no data access.
//! Catching these up front is what lets the extractor and the compiler agree: a blueprint the
//! two would interpret differently is rejected rather than run.

use std::collections::{HashMap, HashSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::blueprint::{
    Blueprint, IdRendering, Mapping, MissingEndpointPolicy, Node, NodeOp, ObjectEndpoint, Target,
};
use super::catalog::{Catalog, ColumnSchema, TableSchema};
use super::desugar::desugar_with_paths;
use super::expr::{SplitKind, TimestampSource, ValueExpression};
use super::predicate::{Literal, Operand, Predicate};
use super::value::ValueKind;
use super::MODEL_VERSION;

/// A reason a blueprint cannot be executed or compiled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ValidationError {
    /// The blueprint's `version` is newer than this build understands.
    UnsupportedVersion {
        /// The blueprint's version.
        found: u32,
        /// The newest version this build reads.
        supported: u32,
    },
    /// Two nodes share an id.
    DuplicateNodeId {
        /// The repeated id.
        id: String,
    },
    /// A node or mapping names a node that does not exist.
    UnknownNodeRef {
        /// Who referred to it.
        from: String,
        /// The missing id.
        id: String,
    },
    /// The node graph contains a cycle, so no evaluation order exists.
    NodeCycle {
        /// One node id participating in the cycle.
        id: String,
    },
    /// A source node names a source with no entry in the catalog.
    UnknownSource {
        /// The source id.
        source_id: String,
    },
    /// A source node names a table with no schema in the catalog.
    UnknownTable {
        /// The source id.
        source_id: String,
        /// The table name.
        table: String,
    },
    /// An expression reads a column absent from the declared schema.
    UnknownColumn {
        /// The node whose rows were being read.
        node: String,
        /// The column name.
        column: String,
    },
    /// Type prefixing is on, but a relation endpoint does not declare its type.
    MissingTypeForPrefixing {
        /// Which mapping, by label or index.
        mapping: String,
        /// Which endpoint.
        endpoint: String,
    },
    /// Missing endpoints are created, but an object endpoint does not declare its type.
    MissingTypeForCreate {
        /// Which mapping, by label or index.
        mapping: String,
        /// Which endpoint.
        endpoint: String,
    },
    /// A union has no inputs, so it has no columns to project.
    EmptyUnion {
        /// The node id.
        node: String,
    },
    /// A regular expression does not compile.
    InvalidRegex {
        /// The pattern.
        pattern: String,
        /// The compiler's message.
        message: String,
    },
    /// A `Template` expression has a placeholder that is unterminated or empty, either of which
    /// drops every row instead of the intended substitution.
    InvalidTemplate {
        /// The template text.
        template: String,
        /// What is wrong with it.
        reason: String,
    },
    /// A `Join` output column resolves to no single input column: the left input has a column
    /// literally named `right_<name>` and the right input's `<name>` is renamed onto it.
    AmbiguousJoinColumn {
        /// The `Join` node.
        node: String,
        /// The contested output column name.
        column: String,
    },
    /// A comparison literal cannot be read as the type its column declares, so the comparison
    /// matches no row at all, however the data looks.
    ///
    /// Raised only where the column's declared type names a kind: an unrecognised `col_type` turns
    /// coercion off on purpose, and then the literal is compared as authored.
    UncoercibleLiteral {
        /// The mapping, by label or authored path, or the `Filter` node, the comparison sits in.
        location: String,
        /// The column compared against.
        column: String,
        /// That column's declared type, verbatim from the catalog.
        col_type: String,
        /// The literal, as authored.
        literal: String,
        /// The kind `col_type` names: `text`, `integer`, `float`, `boolean` or `timestamp`.
        expected: String,
    },
    /// A comparison's two operands (both columns, or both literals) have different, statically
    /// known kinds that [`Value::compare`](super::value::Value::compare) has no rule for (only
    /// `integer`/`float` compare across kinds), so it matches no row at all.
    IncomparableCompare {
        /// The mapping, by label or authored path, or the `Filter` node, the comparison sits in.
        location: String,
        /// The left operand's declared kind: `text`, `integer`, `float`, `boolean` or `timestamp`.
        left_kind: String,
        /// The right operand's declared kind.
        right_kind: String,
    },
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::UnsupportedVersion { found, supported } => {
                write!(
                    f,
                    "blueprint version {found} is newer than the supported version {supported}"
                )
            }
            ValidationError::DuplicateNodeId { id } => write!(f, "duplicate node id '{id}'"),
            ValidationError::UnknownNodeRef { from, id } => {
                write!(f, "'{from}' refers to unknown node '{id}'")
            }
            ValidationError::NodeCycle { id } => {
                write!(f, "node '{id}' takes part in a cycle")
            }
            ValidationError::UnknownSource { source_id } => {
                write!(f, "no catalog entry for source '{source_id}'")
            }
            ValidationError::UnknownTable { source_id, table } => {
                write!(f, "no schema for table '{table}' in source '{source_id}'")
            }
            ValidationError::UnknownColumn { node, column } => {
                write!(f, "node '{node}' has no column '{column}'")
            }
            ValidationError::MissingTypeForPrefixing { mapping, endpoint } => write!(
                f,
                "mapping {mapping}: {endpoint} needs a declared type because ids are type-prefixed"
            ),
            ValidationError::MissingTypeForCreate { mapping, endpoint } => write!(
                f,
                "mapping {mapping}: {endpoint} needs a declared type because missing endpoints are created"
            ),
            ValidationError::EmptyUnion { node } => write!(f, "union node '{node}' has no inputs"),
            ValidationError::InvalidRegex { pattern, message } => {
                write!(f, "invalid regular expression '{pattern}': {message}")
            }
            ValidationError::InvalidTemplate { template, reason } => {
                write!(f, "invalid template '{template}': {reason}")
            }
            ValidationError::AmbiguousJoinColumn { node, column } => write!(
                f,
                "join '{node}': column '{column}' could be the left input's own column or the \
                 renamed right-hand one"
            ),
            ValidationError::UncoercibleLiteral {
                location,
                column,
                col_type,
                literal,
                expected,
            } => write!(
                f,
                "{location}: '{literal}' cannot be read as {expected}, which is what column \
                 '{column}' declares ({col_type}), so the comparison matches nothing"
            ),
            ValidationError::IncomparableCompare {
                location,
                left_kind,
                right_kind,
            } => write!(
                f,
                "{location}: comparing {left_kind} to {right_kind} matches nothing, since only \
                 integer and float compare across kinds"
            ),
        }
    }
}

impl std::error::Error for ValidationError {}

/// Check a blueprint against a catalog, returning every problem found.
///
/// An empty result means the blueprint is executable and compilable. Errors are collected rather
/// than short-circuited so an editor can show all of them at once.
#[must_use]
pub fn validate(blueprint: &Blueprint, catalog: &dyn Catalog) -> Vec<ValidationError> {
    let mut errors = Vec::new();

    if blueprint.version > MODEL_VERSION {
        errors.push(ValidationError::UnsupportedVersion {
            found: blueprint.version,
            supported: MODEL_VERSION,
        });
        return errors;
    }

    let mut seen: HashSet<&str> = HashSet::new();
    for node in &blueprint.nodes {
        if !seen.insert(node.id.as_str()) {
            errors.push(ValidationError::DuplicateNodeId {
                id: node.id.clone(),
            });
        }
    }

    for node in &blueprint.nodes {
        for input in node_inputs(&node.op) {
            if blueprint.node(input).is_none() {
                errors.push(ValidationError::UnknownNodeRef {
                    from: node.id.clone(),
                    id: input.to_string(),
                });
            }
        }
        match &node.op {
            NodeOp::Source { source_id, table } => {
                if !catalog.has_source(source_id) {
                    errors.push(ValidationError::UnknownSource {
                        source_id: source_id.clone(),
                    });
                } else if catalog.table(source_id, table).is_none() {
                    errors.push(ValidationError::UnknownTable {
                        source_id: source_id.clone(),
                        table: table.clone(),
                    });
                }
            }
            NodeOp::Union { inputs } if inputs.is_empty() => {
                errors.push(ValidationError::EmptyUnion {
                    node: node.id.clone(),
                });
            }
            _ => {}
        }
    }

    errors.extend(cycles(blueprint));

    let schemas = super::schema::full_node_schemas(blueprint, catalog);
    let columns = node_columns(&schemas);
    for node in &blueprint.nodes {
        errors.extend(check_filter_columns(node, &columns));
        errors.extend(check_join_columns(node, &columns));
        errors.extend(check_join_ambiguity(node, &schemas));
        if let NodeOp::Filter { input, condition } = &node.op {
            let location = format!("filter '{}'", node.id);
            let input_schema = schemas.get(input.as_str());
            errors.extend(check_literal_kinds(&location, condition, input_schema));
            errors.extend(check_compare_kind_mismatch(
                &location,
                condition,
                input_schema,
            ));
        }
    }

    let desugared = desugar_with_paths(blueprint);
    for pattern in all_regexes(blueprint, &desugared) {
        if let Err(e) = regex::Regex::new(pattern) {
            errors.push(ValidationError::InvalidRegex {
                pattern: pattern.to_string(),
                message: e.to_string(),
            });
        }
    }

    for (path, mapping) in &desugared {
        errors.extend(check_mapping(blueprint, mapping, path, &columns));
        if let Some(when) = &mapping.when {
            let name = mapping.label.clone().unwrap_or_else(|| path.to_string());
            let location = format!("mapping {name}");
            let node_schema = schemas.get(mapping.node.as_str());
            errors.extend(check_literal_kinds(&location, when, node_schema));
            errors.extend(check_compare_kind_mismatch(&location, when, node_schema));
        }
    }

    errors
}

/// The node ids an operation reads from.
fn node_inputs(op: &NodeOp) -> Vec<&str> {
    match op {
        NodeOp::Source { .. } => Vec::new(),
        NodeOp::Filter { input, .. } => vec![input.as_str()],
        NodeOp::Join { left, right, .. } => vec![left.as_str(), right.as_str()],
        NodeOp::Union { inputs } => inputs.iter().map(String::as_str).collect(),
    }
}

/// Report every node on a cycle, but not the ones merely downstream of one: fixing the cycle
/// fixes those too.
fn cycles(blueprint: &Blueprint) -> Vec<ValidationError> {
    let deps: HashMap<&str, Vec<&str>> = blueprint
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), node_inputs(&n.op)))
        .collect();

    // Kahn's algorithm: whatever is left once nothing more can be ordered is in or downstream
    // of a cycle, but the two are not yet told apart.
    let mut remaining = deps.clone();
    loop {
        let ready: Vec<&str> = remaining
            .iter()
            .filter(|(_, ds)| ds.iter().all(|d| !remaining.contains_key(d)))
            .map(|(id, _)| *id)
            .collect();
        if ready.is_empty() {
            break;
        }
        for id in ready {
            remaining.remove(id);
        }
    }

    let mut ids: Vec<&str> = remaining
        .keys()
        .copied()
        .filter(|id| reaches_itself(id, &deps))
        .collect();
    ids.sort_unstable();
    ids.into_iter()
        .map(|id| ValidationError::NodeCycle { id: id.to_string() })
        .collect()
}

/// Whether following dependency edges from `start` leads back to `start`, i.e. whether `start` is
/// itself on a cycle rather than merely reading from one.
fn reaches_itself(start: &str, deps: &HashMap<&str, Vec<&str>>) -> bool {
    let mut stack: Vec<&str> = deps.get(start).cloned().unwrap_or_default();
    let mut seen: HashSet<&str> = HashSet::new();
    while let Some(cur) = stack.pop() {
        if cur == start {
            return true;
        }
        if seen.insert(cur) {
            stack.extend(deps.get(cur).into_iter().flatten().copied());
        }
    }
    false
}

/// The columns each node produces, as far as the catalog can say, taken from
/// [`full_node_schemas`](super::schema::full_node_schemas) so that validation and execution cannot
/// disagree about a `Join`'s `right_<name>` renaming.
///
/// A node with no entry has unknown columns, and column checks against it are skipped.
fn node_columns<'a>(schemas: &HashMap<&'a str, TableSchema>) -> HashMap<&'a str, HashSet<String>> {
    schemas
        .iter()
        .map(|(id, schema)| (*id, schema.columns.keys().cloned().collect()))
        .collect()
}

/// Report every output column of a `Join` that resolves to neither input. See
/// [`join_column_source`](super::schema::join_column_source), which the executor uses for the same
/// decision.
///
/// Reported whether or not anything reads the column, so the run cannot fail part way through on a
/// name the author never suspected.
fn check_join_ambiguity(node: &Node, schemas: &HashMap<&str, TableSchema>) -> Vec<ValidationError> {
    let NodeOp::Join { left, right, .. } = &node.op else {
        return Vec::new();
    };
    let (Some(l), Some(r), Some(joined)) = (
        schemas.get(left.as_str()),
        schemas.get(right.as_str()),
        schemas.get(node.id.as_str()),
    ) else {
        return Vec::new();
    };
    joined
        .columns
        .keys()
        .filter(|name| super::schema::join_column_source(name, l, r).is_none())
        .map(|column| ValidationError::AmbiguousJoinColumn {
            node: node.id.clone(),
            column: column.clone(),
        })
        .collect()
}

/// Report every literal a predicate compares against a column whose declared kind cannot hold it.
///
/// [`Predicate::prepare`] falls back to the literal as authored when coercion fails, and that
/// never equals a cell of the column's kind. Such a comparison is empty on every row, which no
/// runtime signal distinguishes from a genuinely empty result. The shape this catches is
/// `created_at > "2019-01-01"` against a `DATE` column, since
/// [`Value::coerce_to`](super::value::Value::coerce_to) reads a timestamp as strict RFC 3339 only.
fn check_literal_kinds(
    location: &str,
    predicate: &Predicate,
    schema: Option<&TableSchema>,
) -> Vec<ValidationError> {
    let Some(schema) = schema else {
        return Vec::new();
    };
    let mut compared = Vec::new();
    collect_compared_literals(predicate, &mut compared);
    compared
        .into_iter()
        .filter_map(|(column, literal)| {
            let col = schema.columns.get(column)?;
            // No declared kind means coercion is off for this column, and the literal is compared
            // exactly as authored.
            let kind = col.declared_kind()?;
            let value = literal.as_value();
            if value.coerce_to(kind).is_some() {
                return None;
            }
            Some(ValidationError::UncoercibleLiteral {
                location: location.to_string(),
                column: column.to_string(),
                col_type: col.col_type.clone(),
                literal: value.display_string().unwrap_or_default(),
                expected: kind_name(kind).to_string(),
            })
        })
        .collect()
}

/// Report every `Compare` whose two operands are both columns, or both literals, with different
/// statically known kinds that do not coerce (only `Integer`/`Float` do). Unlike
/// [`check_literal_kinds`], a mismatched `Column`/`Literal` pair is not reported here: `prepare`
/// coerces the literal to the column's kind there, and [`check_literal_kinds`] already reports
/// when that coercion fails.
fn check_compare_kind_mismatch(
    location: &str,
    predicate: &Predicate,
    schema: Option<&TableSchema>,
) -> Vec<ValidationError> {
    let mut out = Vec::new();
    collect_compare_kind_mismatches(location, predicate, schema, &mut out);
    out
}

fn collect_compare_kind_mismatches(
    location: &str,
    predicate: &Predicate,
    schema: Option<&TableSchema>,
    out: &mut Vec<ValidationError>,
) {
    match predicate {
        Predicate::And { conditions } | Predicate::Or { conditions } => {
            for c in conditions {
                collect_compare_kind_mismatches(location, c, schema, out);
            }
        }
        Predicate::Not { condition } => {
            collect_compare_kind_mismatches(location, condition, schema, out);
        }
        Predicate::Compare { left, right, .. } => {
            let kinds = match (left, right) {
                (Operand::Column { column: l }, Operand::Column { column: r }) => Some((
                    schema
                        .and_then(|s| s.columns.get(l))
                        .and_then(ColumnSchema::declared_kind),
                    schema
                        .and_then(|s| s.columns.get(r))
                        .and_then(ColumnSchema::declared_kind),
                )),
                (Operand::Literal { value: l }, Operand::Literal { value: r }) => {
                    Some((l.as_value().kind(), r.as_value().kind()))
                }
                _ => None,
            };
            if let Some((Some(l), Some(r))) = kinds {
                if l != r
                    && !matches!(
                        (l, r),
                        (ValueKind::Integer, ValueKind::Float)
                            | (ValueKind::Float, ValueKind::Integer)
                    )
                {
                    out.push(ValidationError::IncomparableCompare {
                        location: location.to_string(),
                        left_kind: kind_name(l).to_string(),
                        right_kind: kind_name(r).to_string(),
                    });
                }
            }
        }
        Predicate::In { .. }
        | Predicate::IsNull { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::Matches { .. } => {}
    }
}

/// Every `(column, literal)` pair a predicate compares, at any depth under `And`/`Or`/`Not`.
fn collect_compared_literals<'a>(predicate: &'a Predicate, out: &mut Vec<(&'a str, &'a Literal)>) {
    match predicate {
        Predicate::And { conditions } | Predicate::Or { conditions } => {
            for c in conditions {
                collect_compared_literals(c, out);
            }
        }
        Predicate::Not { condition } => collect_compared_literals(condition, out),
        Predicate::Compare { left, right, .. } => match (left, right) {
            (Operand::Column { column }, Operand::Literal { value })
            | (Operand::Literal { value }, Operand::Column { column }) => {
                out.push((column.as_str(), value));
            }
            _ => {}
        },
        Predicate::In { column, values } => {
            for value in values {
                out.push((column.as_str(), value));
            }
        }
        Predicate::IsNull { .. } | Predicate::IsEmpty { .. } | Predicate::Matches { .. } => {}
    }
}

/// The name [`ValidationError::UncoercibleLiteral`] reports a [`ValueKind`] under.
fn kind_name(kind: ValueKind) -> &'static str {
    match kind {
        ValueKind::Text => "text",
        ValueKind::Integer => "integer",
        ValueKind::Float => "float",
        ValueKind::Boolean => "boolean",
        ValueKind::Timestamp => "timestamp",
    }
}

/// One [`ValidationError::UnknownColumn`] per name in `referenced` that `node`'s columns do not
/// have, in sorted order and once each. The one place that comparison is made.
fn missing_columns<'a>(
    node: &str,
    referenced: impl IntoIterator<Item = &'a str>,
    available: Option<&HashSet<String>>,
) -> Vec<ValidationError> {
    let Some(available) = available else {
        return Vec::new();
    };
    let mut missing: Vec<&str> = referenced
        .into_iter()
        .filter(|c| !available.contains(*c))
        .collect();
    missing.sort_unstable();
    missing.dedup();
    missing
        .into_iter()
        .map(|column| ValidationError::UnknownColumn {
            node: node.to_string(),
            column: column.to_string(),
        })
        .collect()
}

/// Check a `Join` node's key column pairs against each side's columns.
fn check_join_columns(
    node: &Node,
    columns: &HashMap<&str, HashSet<String>>,
) -> Vec<ValidationError> {
    let NodeOp::Join { left, right, on } = &node.op else {
        return Vec::new();
    };
    let mut errors = missing_columns(
        left,
        on.iter().map(|(l, _)| l.as_str()),
        columns.get(left.as_str()),
    );
    errors.extend(missing_columns(
        right,
        on.iter().map(|(_, r)| r.as_str()),
        columns.get(right.as_str()),
    ));
    errors
}

/// Check one desugared mapping.
///
/// Only the column check needs the referenced node to exist, so it alone is skipped when it does
/// not. A bad node reference does not stop the regex, template or endpoint-type checks from
/// running too, so a mapping with several independent problems reports all of them.
fn check_mapping(
    blueprint: &Blueprint,
    mapping: &Mapping,
    path: &str,
    columns: &HashMap<&str, HashSet<String>>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    let name = mapping.label.clone().unwrap_or_else(|| path.to_string());

    let node_exists = blueprint.node(&mapping.node).is_some();
    if !node_exists {
        errors.push(ValidationError::UnknownNodeRef {
            from: name.clone(),
            id: mapping.node.clone(),
        });
    }

    if node_exists {
        let mut referenced: HashSet<&str> = HashSet::new();
        if let Some(when) = &mapping.when {
            when.referenced_columns(&mut referenced);
        }
        collect_target_columns(&mapping.target, &mut referenced);
        errors.extend(missing_columns(
            &mapping.node,
            referenced,
            columns.get(mapping.node.as_str()),
        ));
    }

    errors.extend(template_problems(&mapping.target));
    errors.extend(endpoint_rules(blueprint, mapping, &name));
    errors
}

/// Check a `Filter` node's condition against its input node's columns, exactly as a mapping's
/// referenced columns are checked against the node it reads.
fn check_filter_columns(
    node: &Node,
    columns: &HashMap<&str, HashSet<String>>,
) -> Vec<ValidationError> {
    let NodeOp::Filter { input, condition } = &node.op else {
        return Vec::new();
    };
    let mut referenced: HashSet<&str> = HashSet::new();
    condition.referenced_columns(&mut referenced);
    missing_columns(input, referenced, columns.get(input.as_str()))
}

/// The `ObjectEndpoint`s a target names, in every position one can appear.
///
/// `pub(crate)`: the executor's `mapping_exec` walks a target's endpoints in this exact order
/// when preparing (and later executing) each one's `Split`, so the two must agree on order.
pub(crate) fn target_object_endpoints(target: &Target) -> Vec<&ObjectEndpoint> {
    match target {
        Target::Event { objects, .. } => objects.iter().map(|o| &o.object).collect(),
        Target::Object { .. } => Vec::new(),
        Target::E2O { object, .. } => vec![object],
        Target::O2O { source, target, .. } => vec![source, target],
    }
}

/// Every [`ValueExpression`] a [`TimestampSource`]'s parts read from.
fn timestamp_expressions(timestamp: &TimestampSource) -> Vec<&ValueExpression> {
    match timestamp {
        TimestampSource::Value(part) => vec![&part.source],
        TimestampSource::Components { date, time } => [date, time]
            .into_iter()
            .flatten()
            .map(|p| &p.source)
            .collect(),
    }
}

/// Every [`ValueExpression`] position a target names (id, type, qualifier, timestamp), without
/// recursing into `Coalesce`'s parts. Callers needing to look inside one call the expression's own
/// methods.
fn target_value_expressions(target: &Target) -> Vec<&ValueExpression> {
    let mut out = Vec::new();
    for endpoint in target_object_endpoints(target) {
        out.push(&endpoint.id);
        if let Some(t) = &endpoint.object_type {
            out.push(t);
        }
    }
    match target {
        Target::Event {
            event_type,
            id,
            timestamp,
            objects,
            ..
        } => {
            out.push(event_type);
            if let Some(id) = id {
                out.push(id);
            }
            out.extend(timestamp_expressions(timestamp));
            for o in objects {
                if let Some(q) = &o.qualifier {
                    out.push(q);
                }
            }
        }
        Target::Object {
            object_type,
            id,
            timestamp,
            ..
        } => {
            out.push(object_type);
            out.push(id);
            if let Some(ts) = timestamp {
                out.extend(timestamp_expressions(ts));
            }
        }
        Target::E2O {
            event, qualifier, ..
        } => {
            out.push(&event.id);
            if let Some(t) = &event.event_type {
                out.push(t);
            }
            if let Some(q) = qualifier {
                out.push(q);
            }
        }
        Target::O2O { qualifier, .. } => {
            if let Some(q) = qualifier {
                out.push(q);
            }
        }
    }
    out
}

/// Every `Template` defect in a target: an unterminated or an empty placeholder, both decidable
/// without row data.
fn template_problems(target: &Target) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    for expr in target_value_expressions(target) {
        collect_template_errors(expr, &mut errors);
    }
    errors
}

/// Collect `Template` defects from `expr`, recursing into `Coalesce`'s parts.
fn collect_template_errors(expr: &ValueExpression, out: &mut Vec<ValidationError>) {
    match expr {
        ValueExpression::Template { template } => {
            if let Some(reason) = template_defect(template) {
                out.push(ValidationError::InvalidTemplate {
                    template: template.clone(),
                    reason: reason.to_string(),
                });
            }
        }
        ValueExpression::Coalesce { parts } => {
            for p in parts {
                collect_template_errors(p, out);
            }
        }
        ValueExpression::Column { .. } | ValueExpression::Constant { .. } => {}
    }
}

/// Whether `template` has an unterminated or an empty placeholder.
fn template_defect(template: &str) -> Option<&'static str> {
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            return Some("unterminated placeholder");
        };
        if after[..close].is_empty() {
            return Some("empty placeholder");
        }
        rest = &after[close + 1..];
    }
    None
}

/// Every regular expression a predicate contains: a `Matches` pattern, at any depth under
/// `And`/`Or`/`Not`.
fn collect_predicate_regexes<'a>(predicate: &'a Predicate, out: &mut Vec<&'a str>) {
    match predicate {
        Predicate::And { conditions } | Predicate::Or { conditions } => {
            for c in conditions {
                collect_predicate_regexes(c, out);
            }
        }
        Predicate::Not { condition } => collect_predicate_regexes(condition, out),
        Predicate::Matches { regex, .. } => out.push(regex.as_str()),
        Predicate::Compare { .. }
        | Predicate::IsNull { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::In { .. } => {}
    }
}

/// Every regular expression in the blueprint that must compile to run it: a `Matches` pattern in
/// a node's `Filter` condition or a mapping's `when`, and a `Regex` split on any `ObjectEndpoint`.
/// One traversal driven from both node ops and mappings, so a new regex-bearing position only has
/// to be added here to be checked everywhere, rather than risking a second
/// hand-rolled walk that forgets a site the first one covers.
fn all_regexes<'a>(blueprint: &'a Blueprint, desugared: &'a [(String, Mapping)]) -> Vec<&'a str> {
    let mut out = Vec::new();
    for node in &blueprint.nodes {
        if let NodeOp::Filter { condition, .. } = &node.op {
            collect_predicate_regexes(condition, &mut out);
        }
    }
    for (_, m) in desugared {
        if let Some(when) = &m.when {
            collect_predicate_regexes(when, &mut out);
        }
        for endpoint in target_object_endpoints(&m.target) {
            if let Some(split) = &endpoint.split {
                if let SplitKind::Regex { pattern } = &split.kind {
                    out.push(pattern.as_str());
                }
            }
        }
    }
    out
}

/// Collect every column a target reads into `out`.
///
/// `pub(crate)`: also used by the executor's demand analysis (`schema::demanded_columns`), so a
/// `Source` node only ever projects the columns a mapping's target actually reads.
pub(crate) fn collect_target_columns<'a>(target: &'a Target, out: &mut HashSet<&'a str>) {
    let endpoint = |e: &'a ObjectEndpoint, out: &mut HashSet<&'a str>| {
        e.id.referenced_columns(out);
        if let Some(t) = &e.object_type {
            t.referenced_columns(out);
        }
    };
    match target {
        Target::Event {
            event_type,
            id,
            timestamp,
            attributes,
            objects,
        } => {
            event_type.referenced_columns(out);
            if let Some(id) = id {
                id.referenced_columns(out);
            }
            timestamp.referenced_columns(out);
            for a in attributes {
                out.insert(a.source_column.as_str());
            }
            for o in objects {
                endpoint(&o.object, out);
                if let Some(q) = &o.qualifier {
                    q.referenced_columns(out);
                }
            }
        }
        Target::Object {
            object_type,
            id,
            timestamp,
            attributes,
        } => {
            object_type.referenced_columns(out);
            id.referenced_columns(out);
            if let Some(ts) = timestamp {
                ts.referenced_columns(out);
            }
            for a in attributes {
                out.insert(a.source_column.as_str());
            }
        }
        Target::E2O {
            event,
            object,
            qualifier,
        } => {
            event.id.referenced_columns(out);
            if let Some(t) = &event.event_type {
                t.referenced_columns(out);
            }
            endpoint(object, out);
            if let Some(q) = qualifier {
                q.referenced_columns(out);
            }
        }
        Target::O2O {
            source,
            target: tgt,
            qualifier,
        } => {
            endpoint(source, out);
            endpoint(tgt, out);
            if let Some(q) = qualifier {
                q.referenced_columns(out);
            }
        }
    }
}

/// Endpoints must declare their type when the id rendering or the missing-endpoint policy
/// needs it. Both rules are decidable from the blueprint alone, which is what replaced the
/// order-dependent `prefixed_types` state the original extractor carried.
fn endpoint_rules(blueprint: &Blueprint, mapping: &Mapping, name: &str) -> Vec<ValidationError> {
    let prefixing = blueprint.id_rendering == IdRendering::TypePrefixed;
    let creating = blueprint.on_missing_endpoint == MissingEndpointPolicy::Create;
    let mut errors = Vec::new();

    let object = |e: &ObjectEndpoint, label: &'static str, errors: &mut Vec<ValidationError>| {
        if e.object_type.is_some() {
            return;
        }
        if prefixing {
            errors.push(ValidationError::MissingTypeForPrefixing {
                mapping: name.to_string(),
                endpoint: label.to_string(),
            });
        }
        if creating {
            errors.push(ValidationError::MissingTypeForCreate {
                mapping: name.to_string(),
                endpoint: label.to_string(),
            });
        }
    };

    match &mapping.target {
        Target::E2O {
            event, object: obj, ..
        } => {
            if prefixing && event.event_type.is_none() {
                errors.push(ValidationError::MissingTypeForPrefixing {
                    mapping: name.to_string(),
                    endpoint: "event".to_string(),
                });
            }
            object(obj, "object", &mut errors);
        }
        Target::O2O { source, target, .. } => {
            object(source, "source", &mut errors);
            object(target, "target", &mut errors);
        }
        Target::Event { objects, .. } => {
            for o in objects {
                object(&o.object, "inline object", &mut errors);
            }
        }
        Target::Object { .. } => {}
    }
    errors
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::extraction::blueprint::*;
    use crate::core::event_data::object_centric::extraction::catalog::{
        ExtractionCatalog, TableSchema,
    };
    use crate::core::event_data::object_centric::extraction::expr::{
        SplitKind, SplitSpec, ValueExpression,
    };
    use crate::core::event_data::object_centric::extraction::predicate::{
        CompareOp, Literal, Operand, Predicate,
    };

    fn source(id: &str, table: &str) -> Node {
        Node {
            id: id.into(),
            label: None,
            op: NodeOp::Source {
                source_id: "erp".into(),
                table: table.into(),
            },
        }
    }

    fn object_mapping(node: &str, object_type: Option<&str>) -> MappingEntry {
        MappingEntry::Single(Mapping {
            node: node.into(),
            label: None,
            when: None,
            target: Target::Object {
                object_type: ValueExpression::Constant {
                    value: object_type.unwrap_or("Order").to_string(),
                },
                id: ValueExpression::Column {
                    column: "id".into(),
                },
                timestamp: None,
                attributes: vec![],
            },
        })
    }

    fn bp(nodes: Vec<Node>, mappings: Vec<MappingEntry>) -> Blueprint {
        Blueprint {
            version: 1,
            id_rendering: IdRendering::Raw,
            nodes,
            mappings,
            on_missing_endpoint: MissingEndpointPolicy::Drop,
            on_duplicate_object: DuplicateObjectPolicy::FirstWins,
        }
    }

    fn catalog() -> ExtractionCatalog {
        ExtractionCatalog::new().with_table(
            "erp",
            TableSchema::new(
                "orders",
                [("id", "INTEGER", false), ("state", "TEXT", true)],
            ),
        )
    }

    #[test]
    fn a_valid_blueprint_reports_nothing() {
        let b = bp(vec![source("o", "orders")], vec![object_mapping("o", None)]);
        assert_eq!(validate(&b, &catalog()), vec![]);
    }

    #[test]
    fn rejects_a_future_version() {
        let mut b = bp(vec![], vec![]);
        b.version = 999;
        assert!(matches!(
            validate(&b, &catalog())[0],
            ValidationError::UnsupportedVersion { .. }
        ));
    }

    #[test]
    fn rejects_duplicate_node_ids() {
        let b = bp(vec![source("o", "orders"), source("o", "orders")], vec![]);
        assert!(matches!(
            validate(&b, &catalog())[0],
            ValidationError::DuplicateNodeId { .. }
        ));
    }

    #[test]
    fn rejects_a_mapping_naming_an_unknown_node() {
        let b = bp(
            vec![source("o", "orders")],
            vec![object_mapping("nope", None)],
        );
        assert!(matches!(
            validate(&b, &catalog())[0],
            ValidationError::UnknownNodeRef { .. }
        ));
    }

    #[test]
    fn rejects_a_cycle_in_the_node_graph() {
        let a = Node {
            id: "a".into(),
            label: None,
            op: NodeOp::Filter {
                input: "b".into(),
                condition: Predicate::And { conditions: vec![] },
            },
        };
        let b_node = Node {
            id: "b".into(),
            label: None,
            op: NodeOp::Filter {
                input: "a".into(),
                condition: Predicate::And { conditions: vec![] },
            },
        };
        let b = bp(vec![a, b_node], vec![]);
        assert!(validate(&b, &catalog())
            .iter()
            .any(|e| matches!(e, ValidationError::NodeCycle { .. })));
    }

    #[test]
    fn rejects_a_column_absent_from_the_catalog() {
        let m = MappingEntry::Single(Mapping {
            node: "o".into(),
            label: None,
            when: None,
            target: Target::Object {
                object_type: ValueExpression::Constant {
                    value: "Order".into(),
                },
                id: ValueExpression::Column {
                    column: "not_a_column".into(),
                },
                timestamp: None,
                attributes: vec![],
            },
        });
        let b = bp(vec![source("o", "orders")], vec![m]);
        assert!(validate(&b, &catalog())
            .iter()
            .any(|e| matches!(e, ValidationError::UnknownColumn { .. })));
    }

    #[test]
    fn type_prefixing_requires_every_relation_endpoint_to_declare_its_type() {
        let m = MappingEntry::Single(Mapping {
            node: "o".into(),
            label: None,
            when: None,
            target: Target::E2O {
                event: EventEndpoint {
                    id: ValueExpression::Column {
                        column: "id".into(),
                    },
                    event_type: None,
                },
                object: ObjectEndpoint {
                    id: ValueExpression::Column {
                        column: "id".into(),
                    },
                    object_type: None,
                    split: None,
                },
                qualifier: None,
            },
        });
        let mut b = bp(vec![source("o", "orders")], vec![m]);
        assert_eq!(validate(&b, &catalog()), vec![]);
        b.id_rendering = IdRendering::TypePrefixed;
        let errs = validate(&b, &catalog());
        assert_eq!(
            errs.iter()
                .filter(|e| matches!(e, ValidationError::MissingTypeForPrefixing { .. }))
                .count(),
            2,
            "both the event and the object endpoint must be reported"
        );
    }

    #[test]
    fn create_policy_requires_object_endpoints_to_declare_their_type() {
        let m = MappingEntry::Single(Mapping {
            node: "o".into(),
            label: None,
            when: None,
            target: Target::E2O {
                event: EventEndpoint {
                    id: ValueExpression::Column {
                        column: "id".into(),
                    },
                    event_type: None,
                },
                object: ObjectEndpoint {
                    id: ValueExpression::Column {
                        column: "id".into(),
                    },
                    object_type: None,
                    split: None,
                },
                qualifier: None,
            },
        });
        let mut b = bp(vec![source("o", "orders")], vec![m]);
        b.on_missing_endpoint = MissingEndpointPolicy::Create;
        assert!(validate(&b, &catalog())
            .iter()
            .any(|e| matches!(e, ValidationError::MissingTypeForCreate { .. })));
    }

    #[test]
    fn rejects_an_uncompilable_regex_before_execution() {
        let m = MappingEntry::Single(Mapping {
            node: "o".into(),
            label: None,
            when: Some(Predicate::Matches {
                column: "state".into(),
                regex: "([".into(),
            }),
            target: Target::Object {
                object_type: ValueExpression::Constant {
                    value: "Order".into(),
                },
                id: ValueExpression::Column {
                    column: "id".into(),
                },
                timestamp: None,
                attributes: vec![],
            },
        });
        let b = bp(vec![source("o", "orders")], vec![m]);
        assert!(validate(&b, &catalog())
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidRegex { .. })));
    }

    #[test]
    fn a_bad_column_inside_a_filter_condition_is_reported() {
        // Regression: validate() used to match only Source and Union in its node loop, so a
        // Filter's condition was never checked against its input's columns.
        let filter = Node {
            id: "recent".into(),
            label: None,
            op: NodeOp::Filter {
                input: "o".into(),
                condition: Predicate::Compare {
                    left: Operand::Column {
                        column: "stat".into(), // typo for "state"
                    },
                    op: CompareOp::Eq,
                    right: Operand::Literal {
                        value: Literal::Text("open".into()),
                    },
                },
            },
        };
        let b = bp(
            vec![source("o", "orders"), filter],
            vec![object_mapping("recent", None)],
        );
        assert!(validate(&b, &catalog()).iter().any(
            |e| matches!(e, ValidationError::UnknownColumn { column, .. } if column == "stat")
        ));
    }

    #[test]
    fn a_bad_regex_inside_a_filter_condition_is_the_only_error() {
        // Regression: bad_regexes was called only on mapping.when, never on a Filter condition.
        let filter = Node {
            id: "orders_filtered".into(),
            label: None,
            op: NodeOp::Filter {
                input: "o".into(),
                condition: Predicate::Matches {
                    column: "state".into(),
                    regex: "([".into(),
                },
            },
        };
        let b = bp(
            vec![source("o", "orders"), filter],
            vec![object_mapping("orders_filtered", None)],
        );
        let errs = validate(&b, &catalog());
        assert_eq!(errs.len(), 1, "got {errs:?}");
        assert!(matches!(errs[0], ValidationError::InvalidRegex { .. }));
    }

    #[test]
    fn a_bad_regex_inside_an_object_endpoint_split_is_the_only_error() {
        // Regression: an uncompilable SplitSpec::Regex pattern inside an ObjectEndpoint.split
        // was never visited by bad_regexes nor by collect_target_columns.
        let m = MappingEntry::Single(Mapping {
            node: "o".into(),
            label: None,
            when: None,
            target: Target::E2O {
                event: EventEndpoint {
                    id: ValueExpression::Column {
                        column: "id".into(),
                    },
                    event_type: None,
                },
                object: ObjectEndpoint {
                    id: ValueExpression::Column {
                        column: "id".into(),
                    },
                    object_type: None,
                    split: Some(SplitSpec {
                        kind: SplitKind::Regex {
                            pattern: "([".into(),
                        },
                        trim: true,
                    }),
                },
                qualifier: None,
            },
        });
        let b = bp(vec![source("o", "orders")], vec![m]);
        let errs = validate(&b, &catalog());
        assert_eq!(errs.len(), 1, "got {errs:?}");
        assert!(matches!(errs[0], ValidationError::InvalidRegex { .. }));
    }

    #[test]
    fn an_unknown_node_reference_does_not_suppress_other_errors_on_the_same_mapping() {
        // D1: a mapping with both a bad node name and a missing endpoint type used to report
        // only the first, because check_mapping returned early.
        let m = MappingEntry::Single(Mapping {
            node: "nope".into(),
            label: None,
            when: None,
            target: Target::E2O {
                event: EventEndpoint {
                    id: ValueExpression::Column {
                        column: "id".into(),
                    },
                    event_type: None,
                },
                object: ObjectEndpoint {
                    id: ValueExpression::Column {
                        column: "id".into(),
                    },
                    object_type: None,
                    split: None,
                },
                qualifier: None,
            },
        });
        let mut b = bp(vec![source("o", "orders")], vec![m]);
        b.id_rendering = IdRendering::TypePrefixed;
        let errs = validate(&b, &catalog());
        assert!(
            errs.iter()
                .any(|e| matches!(e, ValidationError::UnknownNodeRef { .. })),
            "got {errs:?}"
        );
        assert!(
            errs.iter()
                .any(|e| matches!(e, ValidationError::MissingTypeForPrefixing { .. })),
            "endpoint-type checks must run even when the node reference is bad: got {errs:?}"
        );
    }

    #[test]
    fn a_node_only_downstream_of_a_cycle_is_not_reported_as_being_in_one() {
        // D2: Kahn's algorithm leaves every node downstream of a cycle unordered too, so the
        // old code reported c as "taking part in a cycle" despite only reading from it.
        let a = Node {
            id: "a".into(),
            label: None,
            op: NodeOp::Filter {
                input: "b".into(),
                condition: Predicate::And { conditions: vec![] },
            },
        };
        let b_node = Node {
            id: "b".into(),
            label: None,
            op: NodeOp::Filter {
                input: "a".into(),
                condition: Predicate::And { conditions: vec![] },
            },
        };
        let c = Node {
            id: "c".into(),
            label: None,
            op: NodeOp::Filter {
                input: "a".into(),
                condition: Predicate::And { conditions: vec![] },
            },
        };
        let bp = bp(vec![a, b_node, c], vec![]);
        let errs = validate(&bp, &catalog());
        let cycle_nodes: Vec<&str> = errs
            .iter()
            .filter_map(|e| match e {
                ValidationError::NodeCycle { id } => Some(id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(cycle_nodes, vec!["a", "b"], "got {errs:?}");
    }

    #[test]
    fn a_source_id_typo_reports_unknown_source_not_unknown_table() {
        // D3: Catalog::table alone cannot tell a source-id typo apart from a known source with
        // an unknown table.
        let b = bp(
            vec![Node {
                id: "o".into(),
                label: None,
                op: NodeOp::Source {
                    source_id: "nope".into(),
                    table: "orders".into(),
                },
            }],
            vec![],
        );
        let errs = validate(&b, &catalog());
        assert!(matches!(
            errs[0],
            ValidationError::UnknownSource { ref source_id } if source_id == "nope"
        ));
    }

    #[test]
    fn a_known_source_with_an_unknown_table_still_reports_unknown_table() {
        let b = bp(
            vec![Node {
                id: "o".into(),
                label: None,
                op: NodeOp::Source {
                    source_id: "erp".into(),
                    table: "missing".into(),
                },
            }],
            vec![],
        );
        let errs = validate(&b, &catalog());
        assert!(matches!(errs[0], ValidationError::UnknownTable { .. }));
    }

    #[test]
    fn a_colliding_right_join_column_is_reachable_as_right_prefixed() {
        // node_columns must model the same right_<name> prefix rule Join applies at run
        // time, or a mapping reading the documented right_id spuriously fails validation.
        let left = source("o", "orders");
        let right = Node {
            id: "same_ids".into(),
            label: None,
            op: NodeOp::Source {
                source_id: "erp".into(),
                table: "orders".into(), // also has an "id" column: collides with the left side
            },
        };
        let join = Node {
            id: "joined".into(),
            label: None,
            op: NodeOp::Join {
                left: "o".into(),
                right: "same_ids".into(),
                on: vec![("id".into(), "id".into())],
            },
        };
        let m = MappingEntry::Single(Mapping {
            node: "joined".into(),
            label: None,
            when: None,
            target: Target::Object {
                object_type: ValueExpression::Constant {
                    value: "Order".into(),
                },
                id: ValueExpression::Column {
                    column: "right_id".into(),
                },
                timestamp: None,
                attributes: vec![],
            },
        });
        let b = bp(vec![left, right, join], vec![m]);
        assert_eq!(validate(&b, &catalog()), vec![]);
    }

    #[test]
    fn a_join_column_claimed_by_both_the_rename_and_a_real_left_column_is_reported() {
        // The left side already has a column literally named `right_id`, and the right side's
        // own `id` renames onto it. The executor resolves that to neither, so validation must
        // reject it rather than let the run fail on a name the author never wrote.
        let catalog = ExtractionCatalog::new()
            .with_table(
                "erp",
                TableSchema::new(
                    "left_rows",
                    [("id", "INTEGER", false), ("right_id", "INTEGER", false)],
                ),
            )
            .with_table(
                "erp",
                TableSchema::new("right_rows", [("id", "INTEGER", false)]),
            );
        let nodes = vec![
            source("l", "left_rows"),
            Node {
                id: "r".into(),
                label: None,
                op: NodeOp::Source {
                    source_id: "erp".into(),
                    table: "right_rows".into(),
                },
            },
            Node {
                id: "joined".into(),
                label: None,
                op: NodeOp::Join {
                    left: "l".into(),
                    right: "r".into(),
                    on: vec![("id".into(), "id".into())],
                },
            },
        ];
        let b = bp(nodes, vec![]);
        assert!(validate(&b, &catalog).iter().any(|e| matches!(
            e,
            ValidationError::AmbiguousJoinColumn { node, column }
                if node == "joined" && column == "right_id"
        )));
    }

    #[test]
    fn a_literal_that_cannot_be_read_as_its_column_s_type_is_reported() {
        // `Value::coerce_to(Timestamp)` takes strict RFC 3339, so this comparison matches no row
        // whatever the data holds, and at run time that is indistinguishable from an empty
        // result.
        let catalog = ExtractionCatalog::new().with_table(
            "erp",
            TableSchema::new(
                "orders",
                [("id", "INTEGER", false), ("created_at", "DATE", false)],
            ),
        );
        let guarded = |value: Literal| {
            bp(
                vec![source("o", "orders")],
                vec![MappingEntry::Single(Mapping {
                    node: "o".into(),
                    label: Some("orders".into()),
                    when: Some(Predicate::Compare {
                        left: Operand::Column {
                            column: "created_at".into(),
                        },
                        op: CompareOp::Gt,
                        right: Operand::Literal { value },
                    }),
                    target: Target::Object {
                        object_type: ValueExpression::Constant {
                            value: "Order".into(),
                        },
                        id: ValueExpression::Column {
                            column: "id".into(),
                        },
                        timestamp: None,
                        attributes: vec![],
                    },
                })],
            )
        };

        let errors = validate(&guarded(Literal::Text("2019-01-01".into())), &catalog);
        assert!(
            errors.iter().any(|e| matches!(
                e,
                ValidationError::UncoercibleLiteral { column, literal, expected, .. }
                    if column == "created_at" && literal == "2019-01-01" && expected == "timestamp"
            )),
            "{errors:?}"
        );

        // The same comparison written so it does coerce is accepted.
        assert_eq!(
            validate(
                &guarded(Literal::Text("2019-01-01T00:00:00Z".into())),
                &catalog
            ),
            vec![]
        );
    }

    #[test]
    fn comparing_two_columns_of_incomparable_kinds_is_reported() {
        // Neither operand is a literal, so `Predicate::prepare` never coerces either side: at
        // run time `Value::compare` sees a `Text` and an `Integer` and returns `None` (false) on
        // every row.
        let catalog = ExtractionCatalog::new().with_table(
            "erp",
            TableSchema::new(
                "orders",
                [
                    ("id", "INTEGER", false),
                    ("code", "TEXT", false),
                    ("amount", "INTEGER", false),
                    ("price", "DOUBLE", false),
                ],
            ),
        );
        let guarded = |left: &str, right: &str| {
            bp(
                vec![source("o", "orders")],
                vec![MappingEntry::Single(Mapping {
                    node: "o".into(),
                    label: Some("orders".into()),
                    when: Some(Predicate::Compare {
                        left: Operand::Column {
                            column: left.into(),
                        },
                        op: CompareOp::Eq,
                        right: Operand::Column {
                            column: right.into(),
                        },
                    }),
                    target: Target::Object {
                        object_type: ValueExpression::Constant {
                            value: "Order".into(),
                        },
                        id: ValueExpression::Column {
                            column: "id".into(),
                        },
                        timestamp: None,
                        attributes: vec![],
                    },
                })],
            )
        };

        let errors = validate(&guarded("code", "amount"), &catalog);
        assert!(
            errors.iter().any(|e| matches!(
                e,
                ValidationError::IncomparableCompare { left_kind, right_kind, .. }
                    if left_kind == "text" && right_kind == "integer"
            )),
            "{errors:?}"
        );

        // Integer/Float compare across kinds, so this is not reported.
        assert_eq!(validate(&guarded("amount", "price"), &catalog), vec![]);
    }

    #[test]
    fn a_literal_against_a_column_of_unrecognised_type_is_left_alone() {
        // No declared kind means coercion is deliberately off for that column, so the literal is
        // compared as authored and there is nothing to report.
        let catalog = ExtractionCatalog::new().with_table(
            "erp",
            TableSchema::new(
                "orders",
                [("id", "INTEGER", false), ("payload", "GEOMETRY", false)],
            ),
        );
        let b = bp(
            vec![source("o", "orders")],
            vec![MappingEntry::Single(Mapping {
                node: "o".into(),
                label: None,
                when: Some(Predicate::In {
                    column: "payload".into(),
                    values: vec![Literal::Text("anything".into())],
                }),
                target: Target::Object {
                    object_type: ValueExpression::Constant {
                        value: "Order".into(),
                    },
                    id: ValueExpression::Column {
                        column: "id".into(),
                    },
                    timestamp: None,
                    attributes: vec![],
                },
            })],
        );
        assert_eq!(validate(&b, &catalog), vec![]);
    }

    #[test]
    fn a_typo_in_a_join_key_is_reported() {
        // Join.on column pairs were never checked against either input's schema.
        let join = Node {
            id: "joined".into(),
            label: None,
            op: NodeOp::Join {
                left: "o".into(),
                right: "o2".into(),
                on: vec![("not_a_column".into(), "id".into())],
            },
        };
        let b = bp(
            vec![
                source("o", "orders"),
                Node {
                    id: "o2".into(),
                    label: None,
                    op: NodeOp::Source {
                        source_id: "erp".into(),
                        table: "orders".into(),
                    },
                },
                join,
            ],
            vec![],
        );
        assert!(validate(&b, &catalog()).iter().any(
            |e| matches!(e, ValidationError::UnknownColumn { column, .. } if column == "not_a_column")
        ));
    }

    #[test]
    fn an_unlabelled_mapping_error_names_the_authored_path_not_the_flattened_index() {
        // With mappings = [Ordered{3 members}, Single], the Single is at flattened index 3 but
        // its authored JSON path is mappings[1]. Comparing the complete set of (from, id) pairs
        // rules out a flattened-index naming that would satisfy a bare `from == "mappings[1]"`
        // check for the wrong reason.
        let ordered = MappingEntry::Ordered {
            mappings: vec![
                object_mapping_value("a", Some(Predicate::And { conditions: vec![] })),
                object_mapping_value("b", Some(Predicate::And { conditions: vec![] })),
                object_mapping_value("c", None),
            ],
        };
        let single = MappingEntry::Single(Mapping {
            node: "nope".into(),
            label: None,
            when: None,
            target: Target::Object {
                object_type: ValueExpression::Constant {
                    value: "Order".into(),
                },
                id: ValueExpression::Column {
                    column: "id".into(),
                },
                timestamp: None,
                attributes: vec![],
            },
        });
        let b = bp(vec![source("o", "orders")], vec![ordered, single]);
        let errs = validate(&b, &catalog());
        let mut got: Vec<(&str, &str)> = errs
            .iter()
            .filter_map(|e| match e {
                ValidationError::UnknownNodeRef { from, id } => Some((from.as_str(), id.as_str())),
                _ => None,
            })
            .collect();
        got.sort_unstable();
        let mut expected = vec![
            ("mappings[0].mappings[0]", "a"),
            ("mappings[0].mappings[1]", "b"),
            ("mappings[0].mappings[2]", "c"),
            ("mappings[1]", "nope"),
        ];
        expected.sort_unstable();
        assert_eq!(got, expected, "got {errs:?}");
    }

    fn object_mapping_value(node: &str, when: Option<Predicate>) -> Mapping {
        Mapping {
            node: node.into(),
            label: None,
            when,
            target: Target::Object {
                object_type: ValueExpression::Constant {
                    value: "Order".into(),
                },
                id: ValueExpression::Column {
                    column: "id".into(),
                },
                timestamp: None,
                attributes: vec![],
            },
        }
    }

    #[test]
    fn an_unterminated_template_placeholder_is_reported() {
        let m = MappingEntry::Single(Mapping {
            node: "o".into(),
            label: None,
            when: None,
            target: Target::Object {
                object_type: ValueExpression::Template {
                    template: "{a}-{b".into(),
                },
                id: ValueExpression::Column {
                    column: "id".into(),
                },
                timestamp: None,
                attributes: vec![],
            },
        });
        let b = bp(vec![source("o", "orders")], vec![m]);
        assert!(validate(&b, &catalog())
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidTemplate { .. })));
    }

    #[test]
    fn an_empty_template_placeholder_is_reported() {
        // Regression: an empty placeholder used to also collect "" as a referenced column,
        // so validation additionally reported a spurious `UnknownColumn { column: "" }`
        // alongside the `InvalidTemplate` that actually names the defect. Only the latter is
        // the right diagnostic here, so the full error set is asserted, not just `.any()`.
        let m = MappingEntry::Single(Mapping {
            node: "o".into(),
            label: None,
            when: None,
            target: Target::Object {
                object_type: ValueExpression::Template {
                    template: "{}".into(),
                },
                id: ValueExpression::Column {
                    column: "id".into(),
                },
                timestamp: None,
                attributes: vec![],
            },
        });
        let b = bp(vec![source("o", "orders")], vec![m]);
        assert_eq!(
            validate(&b, &catalog()),
            vec![ValidationError::InvalidTemplate {
                template: "{}".into(),
                reason: "empty placeholder".into(),
            }]
        );
    }

    #[test]
    fn validation_errors_round_trip_through_json_externally_tagged_kebab_case() {
        // a binding boundary sends `extraction_validate(..) -> Vec<ValidationError>` through
        // serde_json::to_vec, which needs Serialize; and needs to render like every sibling enum
        // in this module rather than the default PascalCase.
        let err = ValidationError::MissingTypeForPrefixing {
            mapping: "mappings[0]".into(),
            endpoint: "object".into(),
        };
        let json = serde_json::to_value(&err).expect("serialize");
        assert_eq!(json["type"], "missing-type-for-prefixing");
        assert_eq!(json["endpoint"], "object");
        let back: ValidationError = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back, err);
    }
}
