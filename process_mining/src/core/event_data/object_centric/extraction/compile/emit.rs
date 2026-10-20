//! Row-level emission: a node's rows as SQL, and one row's expressions, predicates, timestamps
//! and splits as SQL fragments.
//!
//! Everything here mirrors a specific piece of the extractor, named in the implementing function's
//! doc comment. Where the two could disagree the function returns a [`RejectReason`] rather than
//! guessing.
//!
//! The extractor decides literal coercion, identity rendering and join-key matching from the
//! runtime [`Value`] in a cell, while a compiler only has [`ColumnSchema::declared_kind`]. Every
//! kind-dependent rule below therefore assumes the catalog describes the kinds a source's values
//! actually have, which holds for statically typed engines but not for `SQLite`.

use std::collections::HashMap;

use super::dialect::SqlDialect;
use super::RejectReason;
use crate::core::event_data::object_centric::extraction::blueprint::{Blueprint, NodeOp};
use crate::core::event_data::object_centric::extraction::catalog::{ColumnSchema, TableSchema};
use crate::core::event_data::object_centric::extraction::expr::{
    SplitKind, SplitSpec, TimestampFormat, TimestampSource, ValueExpression,
};
use crate::core::event_data::object_centric::extraction::predicate::{
    prepare_literal, CompareOp, Literal, Operand, Predicate,
};
use crate::core::event_data::object_centric::extraction::row::{build_column_index, Row};
use crate::core::event_data::object_centric::extraction::schema::{join_column_source, JoinSide};
use crate::core::event_data::object_centric::extraction::value::ValueKind;

/// The alias every emitted fragment qualifies its column references with.
pub(crate) const ROW_ALIAS: &str = "src";

/// A node's rows as SQL, plus the declared shape those rows have.
///
/// Holds the same per-node column resolution the extractor uses
/// ([`full_node_schemas`](crate::core::event_data::object_centric::extraction::schema::full_node_schemas)),
/// so a `Join`'s `right_<name>` columns and a `Union`'s null-filled
/// ones carry the identical names on both sides.
#[derive(Debug)]
pub(crate) struct Emitter<'a, 'b> {
    pub(crate) dialect: SqlDialect,
    blueprint: &'a Blueprint,
    full: &'b HashMap<&'a str, TableSchema>,
}

impl<'a, 'b> Emitter<'a, 'b> {
    /// Borrow a caller's already-resolved
    /// [`full_node_schemas`](crate::core::event_data::object_centric::extraction::schema::full_node_schemas)
    /// map. Borrowed rather than owned because the push-down path builds one emitter per join per
    /// consumer per phase, and the map holds every node's whole column list.
    pub(crate) fn from_schemas(
        blueprint: &'a Blueprint,
        full: &'b HashMap<&'a str, TableSchema>,
        dialect: SqlDialect,
    ) -> Self {
        Self {
            dialect,
            blueprint,
            full,
        }
    }

    /// The declared shape of `node_id`'s rows, or `None` when the node does not exist or its
    /// schema could not be resolved (an unknown source table, say).
    pub(crate) fn schema_of(&self, node_id: &str) -> Option<&TableSchema> {
        self.full.get(node_id)
    }

    /// A bare `SELECT` producing every column of `node_id`, in the same order and under the same
    /// names the extractor's own row layout uses.
    pub(crate) fn node_sql(&self, node_id: &str) -> Result<String, RejectReason> {
        self.node_sql_inner(node_id, &mut Vec::new())
    }

    fn node_sql_inner(
        &self,
        node_id: &str,
        visiting: &mut Vec<String>,
    ) -> Result<String, RejectReason> {
        if visiting.iter().any(|v| v == node_id) {
            return Err(RejectReason::NodeCycle {
                node: node_id.to_string(),
            });
        }
        let node = self
            .blueprint
            .node(node_id)
            .ok_or_else(|| RejectReason::UnknownNode {
                node: node_id.to_string(),
            })?;
        let schema = self
            .schema_of(node_id)
            .ok_or_else(|| RejectReason::UnresolvedNodeSchema {
                node: node_id.to_string(),
            })?;
        let columns: Vec<&str> = schema.columns.keys().map(String::as_str).collect();
        if columns.is_empty() {
            return Err(RejectReason::EmptyProjection {
                node: node_id.to_string(),
            });
        }

        visiting.push(node_id.to_string());
        let sql = match &node.op {
            NodeOp::Source { table, .. } => {
                let cols: Vec<String> = columns
                    .iter()
                    .map(|c| self.dialect.quote_ident(c))
                    .collect();
                Ok(format!(
                    "SELECT {} FROM {}",
                    cols.join(", "),
                    self.dialect.quote_ident(table)
                ))
            }
            NodeOp::Filter { input, condition } => self.filter_sql(input, condition, visiting),
            NodeOp::Union { inputs } => self.union_sql(node_id, inputs, &columns, visiting),
            NodeOp::Join { left, right, on } => {
                self.join_sql(node_id, left, right, on, &columns, visiting)
            }
        };
        visiting.pop();
        sql
    }

    /// A `Filter` narrows rows and never columns, exactly as `WHERE` does.
    fn filter_sql(
        &self,
        input: &str,
        condition: &Predicate,
        visiting: &mut Vec<String>,
    ) -> Result<String, RejectReason> {
        let inner = self.node_sql_inner(input, visiting)?;
        let input_schema =
            self.schema_of(input)
                .ok_or_else(|| RejectReason::UnresolvedNodeSchema {
                    node: input.to_string(),
                })?;
        let cols: Vec<String> = input_schema
            .columns
            .keys()
            .map(|c| format!("{ROW_ALIAS}.{}", self.dialect.quote_ident(c)))
            .collect();
        let where_sql = predicate_sql(self.dialect, condition, input_schema, ROW_ALIAS)?;
        Ok(format!(
            "SELECT {} FROM {} WHERE {where_sql}",
            cols.join(", "),
            self.dialect.derived_table(&inner, ROW_ALIAS)
        ))
    }

    /// `UNION ALL` with the absent columns explicitly null-filled, as [`NodeOp::Union`] specifies.
    fn union_sql(
        &self,
        node_id: &str,
        inputs: &[String],
        columns: &[&str],
        visiting: &mut Vec<String>,
    ) -> Result<String, RejectReason> {
        if inputs.is_empty() {
            return Err(RejectReason::EmptyUnion {
                node: node_id.to_string(),
            });
        }
        let out_schema = self.schema_of(node_id).expect("checked by the caller");
        let mut branches = Vec::with_capacity(inputs.len());
        for input in inputs {
            let inner = self.node_sql_inner(input, visiting)?;
            let input_schema =
                self.schema_of(input)
                    .ok_or_else(|| RejectReason::UnresolvedNodeSchema {
                        node: input.to_string(),
                    })?;
            let cols: Vec<String> = columns
                .iter()
                .map(|c| {
                    let quoted = self.dialect.quote_ident(c);
                    if input_schema.columns.contains_key(*c) {
                        format!("{ROW_ALIAS}.{quoted} AS {quoted}")
                    } else {
                        let kind = out_schema
                            .columns
                            .get(*c)
                            .and_then(ColumnSchema::declared_kind);
                        match kind {
                            Some(k) => format!(
                                "CAST(NULL AS {}) AS {quoted}",
                                self.dialect.kind_sql_type(k)
                            ),
                            None => format!("NULL AS {quoted}"),
                        }
                    }
                })
                .collect();
            branches.push(format!(
                "SELECT {} FROM {}",
                cols.join(", "),
                self.dialect.derived_table(&inner, ROW_ALIAS)
            ));
        }
        Ok(self.dialect.union_all(&branches))
    }

    /// An inner join whose output columns are routed by [`join_column_source`], the same rule the
    /// extractor's own `GraphExecutor` uses, so `right_<name>` cannot mean two things.
    ///
    /// The key comparison is not a bare `l.k = r.k`: [`Value::join_key_part`] tags each key with
    /// its kind, so a `Text` `"1"` never matches an `Integer` `1` where `DuckDB` would
    /// implicit-cast and join them. Under the module's catalog precondition, two declared kinds
    /// that are equal or both numeric compile to an equality, any other pair to a constant false,
    /// and an undeclared kind is rejected.
    fn join_sql(
        &self,
        node_id: &str,
        left: &str,
        right: &str,
        on: &[(String, String)],
        columns: &[&str],
        visiting: &mut Vec<String>,
    ) -> Result<String, RejectReason> {
        let left_sql = self.node_sql_inner(left, visiting)?;
        let right_sql = self.node_sql_inner(right, visiting)?;
        let l_schema = self
            .schema_of(left)
            .ok_or_else(|| RejectReason::UnresolvedNodeSchema {
                node: left.to_string(),
            })?;
        let r_schema = self
            .schema_of(right)
            .ok_or_else(|| RejectReason::UnresolvedNodeSchema {
                node: right.to_string(),
            })?;

        let mut conds: Vec<String> = Vec::with_capacity(on.len());
        for (l, r) in on {
            let (l_col, lk) = key_column(l_schema, l, node_id, "left")?;
            let (r_col, rk) = key_column(r_schema, r, node_id, "right")?;
            if lk == rk || (is_numeric(lk) && is_numeric(rk)) {
                conds.push(format!(
                    "{} = {}",
                    column_read_sql(self.dialect, l_col, l, "l"),
                    column_read_sql(self.dialect, r_col, r, "r")
                ));
            } else {
                conds.push(self.dialect.false_predicate().to_string());
            }
        }
        if conds.is_empty() {
            // An empty `on` gives every left row the empty key, which every right row also has:
            // a full cross product in the extractor, and `ON TRUE` here.
            conds.push(self.dialect.true_predicate().to_string());
        }

        let mut select_cols = Vec::with_capacity(columns.len());
        for c in columns {
            let quoted = self.dialect.quote_ident(c);
            let projection = match join_column_source(c, l_schema, r_schema) {
                Some((JoinSide::Left, source)) => {
                    format!("l.{} AS {quoted}", self.dialect.quote_ident(source))
                }
                Some((JoinSide::Right, source)) => {
                    format!("r.{} AS {quoted}", self.dialect.quote_ident(source))
                }
                // The executor raises a hard error here, so a null-filled column would be a view
                // carrying rows no extraction can produce.
                None => {
                    return Err(RejectReason::UnknownColumn {
                        column: (*c).to_string(),
                        field: "join output column",
                    })
                }
            };
            select_cols.push(projection);
        }

        Ok(format!(
            "SELECT {} FROM {} INNER JOIN {} ON {}",
            select_cols.join(", "),
            self.dialect.derived_table(&left_sql, "l"),
            self.dialect.derived_table(&right_sql, "r"),
            conds.join(" AND ")
        ))
    }
}

fn is_numeric(k: ValueKind) -> bool {
    matches!(k, ValueKind::Integer | ValueKind::Float)
}

fn key_column<'s>(
    schema: &'s TableSchema,
    column: &str,
    node: &str,
    side: &'static str,
) -> Result<(&'s ColumnSchema, ValueKind), RejectReason> {
    let col = schema
        .columns
        .get(column)
        .ok_or_else(|| RejectReason::UnknownColumn {
            column: column.to_string(),
            field: "join key",
        })?;
    let kind = col
        .declared_kind()
        .ok_or_else(|| RejectReason::UndecidableJoinKey {
            node: node.to_string(),
            side,
            column: column.to_string(),
            col_type: col.col_type.clone(),
        })?;
    Ok((col, kind))
}

/// Whether a timestamp column carries no offset of its own (`TIMESTAMP`, `DATE`, `DATETIME`) and
/// so needs the explicit UTC anchor [`SqlDialect::timestamp_column`] applies.
fn is_naive_timestamp(col: &ColumnSchema) -> bool {
    let lowered = col.col_type.to_ascii_lowercase();
    !(lowered.contains("tz") || lowered.contains("with time zone"))
}

/// One column read as the extractor's providers report it.
///
/// Everything except a timestamp is the bare qualified reference. A timestamp is an instant to
/// the extractor, so an offset-less column is anchored rather than compared as a bare `TIMESTAMP`,
/// which `DuckDB` promotes using the session time zone as soon as the other side is a
/// `TIMESTAMPTZ`.
fn column_read_sql(dialect: SqlDialect, col: &ColumnSchema, column: &str, alias: &str) -> String {
    let q = format!("{alias}.{}", dialect.quote_ident(column));
    if col.declared_kind() == Some(ValueKind::Timestamp) {
        dialect.timestamp_column(&q, is_naive_timestamp(col))
    } else {
        q
    }
}

/// A [`Predicate`] as a SQL boolean that is never `NULL`.
///
/// The extractor evaluates predicates in two-valued logic: an unresolvable comparison is `false`,
/// not "unknown". SQL is three-valued, so every leaf that can yield `NULL` is wrapped through
/// [`SqlDialect::total_bool`] before `AND`/`OR`/`NOT` ever see it.
pub(crate) fn predicate_sql(
    dialect: SqlDialect,
    predicate: &Predicate,
    schema: &TableSchema,
    alias: &str,
) -> Result<String, RejectReason> {
    match predicate {
        Predicate::And { conditions } => {
            if conditions.is_empty() {
                return Ok(dialect.true_predicate().to_string());
            }
            let parts = conditions
                .iter()
                .map(|c| predicate_sql(dialect, c, schema, alias))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", parts.join(" AND ")))
        }
        Predicate::Or { conditions } => {
            if conditions.is_empty() {
                return Ok(dialect.false_predicate().to_string());
            }
            let parts = conditions
                .iter()
                .map(|c| predicate_sql(dialect, c, schema, alias))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", parts.join(" OR ")))
        }
        // Safe because the operand is total: see this function's docs.
        Predicate::Not { condition } => Ok(format!(
            "(NOT {})",
            predicate_sql(dialect, condition, schema, alias)?
        )),
        Predicate::Compare { left, op, right } => {
            compare_sql(dialect, left, *op, right, schema, alias)
        }
        Predicate::IsNull { column } => match schema.columns.get(column) {
            // A column the row does not carry reads as `None`, which `IsNull` calls true.
            None => Ok(dialect.true_predicate().to_string()),
            Some(_) => Ok(format!("({alias}.{} IS NULL)", dialect.quote_ident(column))),
        },
        Predicate::IsEmpty { column } => is_empty_sql(dialect, column, schema, alias),
        Predicate::Matches { column, regex } => matches_sql(dialect, column, regex, schema, alias),
        Predicate::In { column, values } => in_sql(dialect, column, values, schema, alias),
    }
}

/// `IsEmpty` is `NULL`, absent, or a [`Value::canonical_string`] of `""`. Only `Text` has a
/// canonical rendering that can be empty, which makes every other kind exactly "is null".
fn is_empty_sql(
    dialect: SqlDialect,
    column: &str,
    schema: &TableSchema,
    alias: &str,
) -> Result<String, RejectReason> {
    let Some(col) = schema.columns.get(column) else {
        return Ok(dialect.true_predicate().to_string());
    };
    let kind = col
        .declared_kind()
        .ok_or_else(|| RejectReason::UndeclaredColumnKind {
            column: column.to_string(),
            col_type: col.col_type.clone(),
            field: "is-empty",
        })?;
    let q = format!("{alias}.{}", dialect.quote_ident(column));
    Ok(match kind {
        ValueKind::Text => format!("({q} IS NULL OR {q} = '')"),
        _ => format!("({q} IS NULL)"),
    })
}

/// `Matches` reads the column through [`Value::display_string`].
///
/// That rendering is only reproducible in SQL for `Text`, `Integer` and `Boolean`: Rust's
/// `f64::to_string` writes `1` where `DuckDB` writes `1.0`, and `DateTime::to_rfc3339` keeps the
/// original offset where a SQL cast does not.
fn matches_sql(
    dialect: SqlDialect,
    column: &str,
    regex: &str,
    schema: &TableSchema,
    alias: &str,
) -> Result<String, RejectReason> {
    let Some(col) = schema.columns.get(column) else {
        // `row.get` is `None`, so `is_some_and` is false for every row.
        return Ok(dialect.false_predicate().to_string());
    };
    let kind = col
        .declared_kind()
        .ok_or_else(|| RejectReason::UndeclaredColumnKind {
            column: column.to_string(),
            col_type: col.col_type.clone(),
            field: "matches",
        })?;
    let q = format!("{alias}.{}", dialect.quote_ident(column));
    let text = match kind {
        ValueKind::Text => q,
        ValueKind::Integer => dialect.cast_to_text(&q),
        ValueKind::Boolean => dialect.bool_to_text(&q),
        ValueKind::Float | ValueKind::Timestamp => {
            return Err(RejectReason::UnstableDisplayRendering {
                column: column.to_string(),
                col_type: col.col_type.clone(),
                field: "matches",
            })
        }
    };
    Ok(dialect.total_bool(&dialect.regex_match(&text, regex)))
}

/// `In` coerces every literal to the column's declared kind independently, exactly as
/// [`Predicate::prepare`] does, and drops the ones that then cannot compare equal to a value of
/// that kind. An uncoercible literal matches nothing there either.
fn in_sql(
    dialect: SqlDialect,
    column: &str,
    values: &[Literal],
    schema: &TableSchema,
    alias: &str,
) -> Result<String, RejectReason> {
    let Some(col) = schema.columns.get(column) else {
        return Ok(dialect.false_predicate().to_string());
    };
    let kind = col
        .declared_kind()
        .ok_or_else(|| RejectReason::UndeclaredColumnKind {
            column: column.to_string(),
            col_type: col.col_type.clone(),
            field: "in",
        })?;
    let literals: Vec<String> = values
        .iter()
        .map(|l| prepare_literal(l, Some(kind)))
        .filter(|v| comparable(v.kind(), Some(kind)))
        .filter_map(|v| dialect.value_literal(&v))
        .collect();
    if literals.is_empty() {
        return Ok(dialect.false_predicate().to_string());
    }
    let q = column_read_sql(dialect, col, column, alias);
    Ok(dialect.total_bool(&format!("{q} IN ({})", literals.join(", "))))
}

/// Whether [`Value::compare`] can order these two kinds: identical kinds, or both numeric.
/// Anything else, and anything involving `Null`, makes the comparing predicate false.
fn comparable(a: Option<ValueKind>, b: Option<ValueKind>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a == b || (is_numeric(a) && is_numeric(b)),
        _ => false,
    }
}

fn op_sql(op: CompareOp) -> &'static str {
    match op {
        CompareOp::Eq => "=",
        CompareOp::Ne => "<>",
        CompareOp::Lt => "<",
        CompareOp::Le => "<=",
        CompareOp::Gt => ">",
        CompareOp::Ge => ">=",
    }
}

fn apply_op(op: CompareOp, ord: std::cmp::Ordering) -> bool {
    match op {
        CompareOp::Eq => ord.is_eq(),
        CompareOp::Ne => ord.is_ne(),
        CompareOp::Lt => ord.is_lt(),
        CompareOp::Le => ord.is_le(),
        CompareOp::Gt => ord.is_gt(),
        CompareOp::Ge => ord.is_ge(),
    }
}

/// A typed comparison, reproducing [`Predicate::prepare`]'s coercion and
/// [`Value::compare`]'s "mismatched kinds do not order, so the predicate is false" rule.
fn compare_sql(
    dialect: SqlDialect,
    left: &Operand,
    op: CompareOp,
    right: &Operand,
    schema: &TableSchema,
    alias: &str,
) -> Result<String, RejectReason> {
    let lk = operand_kind(left, schema)?;
    let rk = operand_kind(right, schema)?;
    match (left, right) {
        // Two literals fold at compile time: no column is read, so the answer is the same on
        // every row.
        (Operand::Literal { value: l }, Operand::Literal { value: r }) => {
            let answer = l
                .as_value()
                .compare(&r.as_value())
                .is_some_and(|ord| apply_op(op, ord));
            Ok(if answer {
                dialect.true_predicate().to_string()
            } else {
                dialect.false_predicate().to_string()
            })
        }
        _ => {
            // `prepare` coerces each literal against the other side's declared column kind.
            let (l_kind, r_kind) = (
                operand_value_kind(left, rk, schema),
                operand_value_kind(right, lk, schema),
            );
            if !comparable(l_kind, r_kind) {
                // `Value::compare` returns `None`, and `CompareOp` turns that into false. Decided
                // before either side is rendered, so a column the node does not carry degrades to
                // a false predicate rather than refusing the whole mapping.
                return Ok(dialect.false_predicate().to_string());
            }
            let l_sql = operand_sql(dialect, left, rk, schema, alias)?;
            let r_sql = operand_sql(dialect, right, lk, schema, alias)?;
            Ok(dialect.total_bool(&format!("{l_sql} {} {r_sql}", op_sql(op))))
        }
    }
}

/// A column operand's declared kind, or `None` for a literal. Mirrors `column_kind` in
/// `predicate.rs`, which decides whether the other side's literal is coerced.
fn operand_kind(
    operand: &Operand,
    schema: &TableSchema,
) -> Result<Option<ValueKind>, RejectReason> {
    match operand {
        Operand::Literal { .. } => Ok(None),
        Operand::Column { column } => {
            match schema.columns.get(column) {
                None => Ok(None),
                Some(col) => col.declared_kind().map(Some).ok_or_else(|| {
                    RejectReason::UndeclaredColumnKind {
                        column: column.to_string(),
                        col_type: col.col_type.clone(),
                        field: "compare",
                    }
                }),
            }
        }
    }
}

/// The kind the value on this side has once coercion has run.
fn operand_value_kind(
    operand: &Operand,
    other_side_kind: Option<ValueKind>,
    schema: &TableSchema,
) -> Option<ValueKind> {
    match operand {
        Operand::Column { column } => schema
            .columns
            .get(column)
            .and_then(ColumnSchema::declared_kind),
        Operand::Literal { value } => prepare_literal(value, other_side_kind).kind(),
    }
}

fn operand_sql(
    dialect: SqlDialect,
    operand: &Operand,
    other_side_kind: Option<ValueKind>,
    schema: &TableSchema,
    alias: &str,
) -> Result<String, RejectReason> {
    match operand {
        Operand::Column { column } => {
            let col = schema
                .columns
                .get(column)
                .ok_or_else(|| RejectReason::UnknownColumn {
                    column: column.clone(),
                    field: "compare",
                })?;
            Ok(column_read_sql(dialect, col, column, alias))
        }
        Operand::Literal { value } => {
            let v = prepare_literal(value, other_side_kind);
            Ok(dialect
                .value_literal(&v)
                .unwrap_or_else(|| "NULL".to_string()))
        }
    }
}

/// A [`ValueExpression`] as text, at an identity position.
///
/// Mirrors [`ValueExpression::evaluate`], which reads every column through
/// [`Value::canonical_string`]: only `Text`, `Integer` and `Boolean` have one, and every variant
/// propagates absence. `NULL` propagation through `||` and `COALESCE` reproduces that exactly, so
/// the expression is `NULL` on precisely the rows the extractor drops. The caller adds the
/// `IS NOT NULL` filter.
pub(crate) fn identity_sql(
    dialect: SqlDialect,
    expr: &ValueExpression,
    schema: &TableSchema,
    alias: &str,
    field: &'static str,
) -> Result<String, RejectReason> {
    match expr {
        ValueExpression::Constant { value } => Ok(dialect.string_literal(value)),
        ValueExpression::Column { column } => {
            column_identity_sql(dialect, column, schema, alias, field)
        }
        ValueExpression::Template { template } => {
            template_sql(dialect, template, schema, alias, field)
        }
        ValueExpression::Coalesce { parts } => {
            if parts.is_empty() {
                // `find_map` over nothing is `None`.
                return Ok(dialect.null_text());
            }
            let rendered = parts
                .iter()
                .map(|p| identity_sql(dialect, p, schema, alias, field))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(dialect.coalesce(&rendered))
        }
    }
}

fn column_identity_sql(
    dialect: SqlDialect,
    column: &str,
    schema: &TableSchema,
    alias: &str,
    field: &'static str,
) -> Result<String, RejectReason> {
    let col = schema
        .columns
        .get(column)
        .ok_or_else(|| RejectReason::UnknownColumn {
            column: column.to_string(),
            field,
        })?;
    let kind = col
        .declared_kind()
        .ok_or_else(|| RejectReason::UndeclaredColumnKind {
            column: column.to_string(),
            col_type: col.col_type.clone(),
            field,
        })?;
    let q = format!("{alias}.{}", dialect.quote_ident(column));
    match kind {
        ValueKind::Text => Ok(q),
        ValueKind::Integer => Ok(dialect.cast_to_text(&q)),
        ValueKind::Boolean => Ok(dialect.bool_to_text(&q)),
        // The extractor renders a whole-number Float as its integer and drops a fractional one,
        // which no single SQL expression reproduces. Timestamp offset rendering varies the same
        // way.
        ValueKind::Float | ValueKind::Timestamp => Err(RejectReason::UnstableIdentityRendering {
            column: column.to_string(),
            col_type: col.col_type.clone(),
            field,
        }),
    }
}

/// `render_template` scans the template, substituting each `{name}` and returning `None` as
/// soon as one placeholder has no [`Value::canonical_string`]. `||` propagates `NULL` the same
/// way, so no per-placeholder fallback is emitted.
fn template_sql(
    dialect: SqlDialect,
    template: &str,
    schema: &TableSchema,
    alias: &str,
    field: &'static str,
) -> Result<String, RejectReason> {
    let mut parts: Vec<String> = Vec::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let literal = &rest[..open];
        if !literal.is_empty() {
            parts.push(dialect.string_literal(literal));
        }
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            // `render_template`'s `?` on the missing '}': the whole expression is always `None`.
            return Err(RejectReason::InvalidTemplate {
                template: template.to_string(),
                reason: "unterminated placeholder".to_string(),
            });
        };
        let name = &after[..close];
        if name.is_empty() {
            return Err(RejectReason::InvalidTemplate {
                template: template.to_string(),
                reason: "empty placeholder".to_string(),
            });
        }
        parts.push(column_identity_sql(dialect, name, schema, alias, field)?);
        rest = &after[close + 1..];
    }
    if !rest.is_empty() {
        parts.push(dialect.string_literal(rest));
    }
    match parts.len() {
        0 => Ok(dialect.string_literal("")),
        1 => Ok(parts.remove(0)),
        _ => Ok(dialect.concat(&parts)),
    }
}

/// A compiled timestamp, carrying whether the extractor can still drop the row.
#[derive(Debug)]
pub(crate) enum TimeSql {
    /// A constant folded at compile time: present on every row.
    Literal(String),
    /// A native timestamp column: `NULL` parses to `None`, dropping the row.
    Column(String),
    /// A constant that did not parse, so no row survives.
    Never,
}

impl TimeSql {
    pub(crate) fn sql(&self, dialect: SqlDialect) -> String {
        match self {
            TimeSql::Literal(s) | TimeSql::Column(s) => s.clone(),
            TimeSql::Never => dialect.null_timestamp(),
        }
    }

    /// The filter that keeps exactly the rows whose timestamp parsed.
    pub(crate) fn filter(&self, dialect: SqlDialect) -> Option<String> {
        match self {
            TimeSql::Literal(_) => None,
            TimeSql::Column(s) => Some(format!("{s} IS NOT NULL")),
            TimeSql::Never => Some(dialect.false_predicate().to_string()),
        }
    }
}

fn reads_no_column(expr: &ValueExpression) -> bool {
    let mut columns = std::collections::HashSet::new();
    expr.referenced_columns(&mut columns);
    columns.is_empty()
}

/// [`TimestampSource::parse`] as SQL.
///
/// Two shapes compile, both inside [`TimestampSource::Value`]. A source that reads nothing from
/// the row folds at compile time, running the whole chrono cascade here for any format. A plain
/// `Column` whose declared type is already a timestamp is read directly, because `parse`
/// short-circuits on a [`Value::Timestamp`] before any string parsing.
///
/// Everything else is a [`RejectReason::ResidualTimestamp`]: chrono's format cascade has no
/// SQL translation that can be proved identical.
pub(crate) fn timestamp_sql(
    dialect: SqlDialect,
    ts: &TimestampSource,
    schema: &TableSchema,
    alias: &str,
) -> Result<TimeSql, RejectReason> {
    match ts {
        // Reads no column, so every row gets the same instant: fold it now.
        TimestampSource::Value(part) if reads_no_column(&part.source) => {
            let index = build_column_index(&[]);
            let row = Row {
                values: &[],
                index: &index,
            };
            Ok(match ts.parse(&row) {
                Some(folded) => TimeSql::Literal(dialect.timestamp_literal(&folded)),
                None => TimeSql::Never,
            })
        }
        TimestampSource::Value(part) => {
            let ValueExpression::Column { column } = &part.source else {
                return Err(RejectReason::ResidualTimestamp {
                    detail: "a timestamp composed from columns is parsed by chrono, not SQL"
                        .to_string(),
                });
            };
            let col = schema
                .columns
                .get(column)
                .ok_or_else(|| RejectReason::UnknownColumn {
                    column: column.to_string(),
                    field: "timestamp",
                })?;
            if col.declared_kind() == Some(ValueKind::Timestamp) {
                return Ok(TimeSql::Column(column_read_sql(
                    dialect, col, column, alias,
                )));
            }
            let format = part.format.as_ref().unwrap_or(&TimestampFormat::Auto);
            Err(RejectReason::ResidualTimestamp {
                detail: format!(
                    "column '{column}' is declared {} rather than a timestamp, so the value goes \
                     through chrono's {format:?} string parsing",
                    col.col_type
                ),
            })
        }
        TimestampSource::Components { .. } => Err(RejectReason::ResidualTimestamp {
            detail: "Components tries three chrono strategies in order".to_string(),
        }),
    }
}

/// One endpoint's split, as a `FROM` target that yields one row per part plus the expression
/// naming that part.
#[derive(Debug)]
pub(crate) struct SplitSql {
    /// The `FROM` target replacing the mapping's own, already aliased.
    pub(crate) from: String,
    /// The expression for one part, valid against `from`.
    pub(crate) part: String,
    /// Extra filters the split needs, in addition to the caller's own.
    pub(crate) filters: Vec<String>,
}

/// `split_or_single` as SQL: the split parts, or the raw cell itself when there is no split.
///
/// The caller has already filtered the raw cell to non-`NULL` and non-empty. The filters
/// returned here are the per-part ones on top. The `unnest` goes into a derived table so the
/// per-part column can be trimmed, filtered and referenced more than once, and so a second split
/// can nest over the first.
pub(crate) fn split_sql(
    dialect: SqlDialect,
    from: &str,
    raw_expr: &str,
    split: Option<&SplitSpec>,
    part_column: &str,
) -> Result<SplitSql, RejectReason> {
    let Some(split) = split else {
        return Ok(SplitSql {
            from: from.to_string(),
            part: raw_expr.to_string(),
            filters: Vec::new(),
        });
    };
    let ident = dialect.quote_ident(part_column);
    let expanded = match &split.kind {
        SplitKind::Delimiter { delimiter } => {
            if delimiter.is_empty() {
                // `PreparedSplit::split` returns the whole raw value for an empty delimiter.
                let part = maybe_trim(dialect, raw_expr, split.trim);
                return Ok(SplitSql {
                    from: from.to_string(),
                    filters: vec![format!("{part} <> ''")],
                    part,
                });
            }
            dialect.split_to_rows(raw_expr, delimiter)
        }
        SplitKind::Regex { pattern } => {
            let compiled = regex::Regex::new(pattern).map_err(|e| RejectReason::InvalidRegex {
                pattern: pattern.clone(),
                message: e.to_string(),
            })?;
            // `captures_len` counts the implicit whole-match group, which `split` only reads
            // when the pattern has no other one.
            let groups = compiled.captures_len().saturating_sub(1);
            dialect.regex_split_to_rows(raw_expr, pattern, groups)
        }
    };
    let part = maybe_trim(dialect, &format!("{ROW_ALIAS}.{ident}"), split.trim);
    Ok(SplitSql {
        // Reusing the input's own alias keeps a nested second split naming `src` too.
        from: dialect.derived_table(
            &format!("SELECT {ROW_ALIAS}.*, {expanded} AS {ident} FROM {from}"),
            ROW_ALIAS,
        ),
        // `regexp_extract_all` yields `[NULL]` for a group that did not participate, where
        // Rust's `caps.get(i)` yields nothing; and `split` drops empty parts after trimming.
        filters: vec![
            format!("{ROW_ALIAS}.{ident} IS NOT NULL"),
            format!("{part} <> ''"),
        ],
        part,
    })
}

fn maybe_trim(dialect: SqlDialect, expr: &str, trim: bool) -> String {
    if trim {
        dialect.trim(expr)
    } else {
        expr.to_string()
    }
}

/// A source column read as an attribute value of declared type `declared`.
///
/// `attribute_value` falls back to the cell's natural rendering when coercion fails, which a
/// typed SQL column cannot hold. So only the two combinations where coercion is provably a no-op
/// compile: the column already has the declared kind, or an `Integer` column widening to `Float`.
pub(crate) fn attribute_sql(
    dialect: SqlDialect,
    source_column: &str,
    attribute: &str,
    declared: crate::core::event_data::object_centric::OCELAttributeType,
    schema: &TableSchema,
    alias: &str,
) -> Result<String, RejectReason> {
    use crate::core::event_data::object_centric::OCELAttributeType as A;
    let Some(col) = schema.columns.get(source_column) else {
        // A column the row does not carry is `OCELAttributeValue::Null` on every row.
        return Ok(dialect.null_attribute(declared));
    };
    let kind = col
        .declared_kind()
        .ok_or_else(|| RejectReason::UndeclaredColumnKind {
            column: source_column.to_string(),
            col_type: col.col_type.clone(),
            field: "attribute",
        })?;
    let q = format!("{alias}.{}", dialect.quote_ident(source_column));
    let ok = matches!(
        (kind, declared),
        (ValueKind::Text, A::String)
            | (ValueKind::Integer, A::Integer)
            | (ValueKind::Float, A::Float)
            | (ValueKind::Boolean, A::Boolean)
            | (ValueKind::Timestamp, A::Time)
            | (ValueKind::Integer, A::Float)
    );
    if !ok {
        return Err(RejectReason::AttributeCoercion {
            attribute: attribute.to_string(),
            column: source_column.to_string(),
            col_type: col.col_type.clone(),
            declared: declared.as_type_str(),
        });
    }
    Ok(match (kind, declared) {
        (ValueKind::Integer, A::Float) => format!("CAST({q} AS DOUBLE)"),
        (ValueKind::Timestamp, A::Time) => dialect.timestamp_column(&q, is_naive_timestamp(col)),
        _ => q,
    })
}
