//! Row predicates, used both to filter a node's rows and as a mapping's `when` guard.

use std::collections::HashSet;

use chrono::{DateTime, FixedOffset};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::catalog::{ColumnSchema, TableSchema};
use super::row::Row;
use super::value::{Value, ValueKind};

/// A literal in a comparison or membership test.
///
/// Untagged in JSON, so `true`, `5`, `5.0` and `"x"` deserialize to the matching variant.
/// Order matters for `Boolean`, `Integer`, `Float` and `Text`, whose JSON shapes overlap only
/// with each other (a bare JSON boolean, number or string): boolean before integer before float
/// before text.
///
/// `Timestamp` is exempt from that ordering: it is a single-field object (`{"timestamp": "..."}`)
/// rather than a bare string, so its position does not matter and no ordinary string that happens
/// to parse as RFC 3339 is silently reclassified. A plain string against a timestamp column works
/// without it, through `Predicate::prepare`'s literal coercion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum Literal {
    /// A boolean.
    Boolean(bool),
    /// An integer.
    Integer(i64),
    /// A float.
    Float(f64),
    /// Text.
    Text(String),
    /// An instant with a fixed UTC offset, given as RFC 3339 text.
    Timestamp {
        /// The instant.
        timestamp: DateTime<FixedOffset>,
    },
}

impl Literal {
    /// The [`Value`] this literal denotes, with no coercion applied.
    ///
    /// See `Predicate::prepare` for the coercion `Compare` applies on top of this when a
    /// column's declared type is known.
    #[must_use]
    pub fn as_value(&self) -> Value {
        match self {
            Literal::Boolean(b) => Value::Boolean(*b),
            Literal::Integer(i) => Value::Integer(*i),
            Literal::Float(f) => Value::Float(*f),
            Literal::Text(s) => Value::Text(s.clone()),
            Literal::Timestamp { timestamp } => Value::Timestamp(*timestamp),
        }
    }
}

/// One side of a comparison.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Operand {
    /// The value in a column of the current row.
    Column {
        /// Column name.
        column: String,
    },
    /// A fixed value.
    Literal {
        /// The literal.
        value: Literal,
    },
}

/// Comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum CompareOp {
    /// Equal.
    Eq,
    /// Not equal.
    Ne,
    /// Less than.
    Lt,
    /// Less than or equal.
    Le,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    Ge,
}

/// A boolean test over one row.
///
/// Comparisons are typed: numbers compare numerically, so `amount > 0` means what it says
/// rather than comparing text. Any comparison involving `NULL` is false, including `NULL = NULL`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Predicate {
    /// All conditions hold. An empty list is true.
    And {
        /// Conditions.
        conditions: Vec<Predicate>,
    },
    /// Any condition holds. An empty list is false.
    Or {
        /// Conditions.
        conditions: Vec<Predicate>,
    },
    /// The condition does not hold.
    Not {
        /// The negated condition.
        condition: Box<Predicate>,
    },
    /// Compare two operands.
    Compare {
        /// Left side.
        left: Operand,
        /// Operator.
        op: CompareOp,
        /// Right side.
        right: Operand,
    },
    /// The column is `NULL` or absent.
    IsNull {
        /// Column name.
        column: String,
    },
    /// The column is `NULL`, absent, or renders as the empty string.
    IsEmpty {
        /// Column name.
        column: String,
    },
    /// The column's text (see [`Value::display_string`]) matches the regular expression.
    Matches {
        /// Column name.
        column: String,
        /// Regular expression.
        regex: String,
    },
    /// The column equals one of the listed literals.
    In {
        /// Column name.
        column: String,
        /// Accepted values.
        values: Vec<Literal>,
    },
}

/// A [`Predicate`] with its regular expressions compiled, ready for repeated evaluation.
#[derive(Debug)]
pub(crate) struct PreparedPredicate {
    kind: PreparedKind,
}

#[derive(Debug)]
enum PreparedKind {
    And(Vec<PreparedPredicate>),
    Or(Vec<PreparedPredicate>),
    Not(Box<PreparedPredicate>),
    Compare {
        left: PreparedOperand,
        op: CompareOp,
        right: PreparedOperand,
    },
    IsNull {
        column: String,
    },
    IsEmpty {
        column: String,
    },
    Matches {
        column: String,
        regex: regex::Regex,
    },
    In {
        column: String,
        values: Vec<Value>,
    },
}

/// An [`Operand`] resolved once at [`Predicate::prepare`] time: a literal is coerced then, so
/// [`PreparedPredicate::evaluate`] only ever clones an already-typed [`Value`].
#[derive(Debug)]
enum PreparedOperand {
    Column(String),
    Literal(Value),
}

impl Predicate {
    /// Compile this predicate's regular expressions once, ahead of row evaluation, and coerce
    /// every literal (in a `Compare` or an `In`) to its column's declared type where `schema`
    /// names one.
    ///
    /// # Literal coercion
    ///
    /// A blueprint's `Literal` is untagged JSON, so an editor's text input for `docstatus = 1`
    /// emits `Literal::Text("1")`, which matches zero rows of an `INTEGER` column as-is. Each
    /// literal position is therefore reinterpreted as the column's [`ValueKind`] via
    /// [`Value::coerce_to`], once here instead of per row. A literal that does not parse as the
    /// target kind is left as authored and never matches, as is every literal when `schema` is
    /// `None`.
    ///
    /// This is part of the model's semantics: a SQL compiler must reproduce it at every literal
    /// position, casting the literal to the column's SQL type or emitting an always-false
    /// predicate when the cast is impossible. Otherwise the compiled view and this evaluator
    /// disagree about which rows match.
    ///
    /// # Errors
    /// Returns the underlying [`regex::Error`] if a `Matches` pattern does not compile.
    pub(crate) fn prepare(
        &self,
        schema: Option<&TableSchema>,
    ) -> Result<PreparedPredicate, regex::Error> {
        let kind = match self {
            Predicate::And { conditions } => PreparedKind::And(
                conditions
                    .iter()
                    .map(|c| c.prepare(schema))
                    .collect::<Result<_, _>>()?,
            ),
            Predicate::Or { conditions } => PreparedKind::Or(
                conditions
                    .iter()
                    .map(|c| c.prepare(schema))
                    .collect::<Result<_, _>>()?,
            ),
            Predicate::Not { condition } => PreparedKind::Not(Box::new(condition.prepare(schema)?)),
            Predicate::Compare { left, op, right } => PreparedKind::Compare {
                left: prepare_operand(left, column_kind(right, schema)),
                op: *op,
                right: prepare_operand(right, column_kind(left, schema)),
            },
            Predicate::IsNull { column } => PreparedKind::IsNull {
                column: column.clone(),
            },
            Predicate::IsEmpty { column } => PreparedKind::IsEmpty {
                column: column.clone(),
            },
            Predicate::Matches { column, regex } => PreparedKind::Matches {
                column: column.clone(),
                regex: regex::Regex::new(regex)?,
            },
            Predicate::In { column, values } => {
                let kind = schema
                    .and_then(|s| s.columns.get(column))
                    .and_then(ColumnSchema::declared_kind);
                PreparedKind::In {
                    column: column.clone(),
                    values: values.iter().map(|v| prepare_literal(v, kind)).collect(),
                }
            }
        };
        Ok(PreparedPredicate { kind })
    }

    /// Collect every column name this predicate reads into `out`.
    pub fn referenced_columns<'a>(&'a self, out: &mut HashSet<&'a str>) {
        match self {
            Predicate::And { conditions } | Predicate::Or { conditions } => {
                for c in conditions {
                    c.referenced_columns(out);
                }
            }
            Predicate::Not { condition } => condition.referenced_columns(out),
            Predicate::Compare { left, right, .. } => {
                for side in [left, right] {
                    if let Operand::Column { column } = side {
                        out.insert(column);
                    }
                }
            }
            Predicate::IsNull { column }
            | Predicate::IsEmpty { column }
            | Predicate::Matches { column, .. }
            | Predicate::In { column, .. } => {
                out.insert(column);
            }
        }
    }
}

impl PreparedPredicate {
    /// Evaluate against one row.
    pub(crate) fn evaluate(&self, row: &Row<'_>) -> bool {
        match &self.kind {
            PreparedKind::And(cs) => cs.iter().all(|c| c.evaluate(row)),
            PreparedKind::Or(cs) => cs.iter().any(|c| c.evaluate(row)),
            PreparedKind::Not(c) => !c.evaluate(row),
            PreparedKind::Compare { left, op, right } => {
                let (Some(l), Some(r)) = (resolve(left, row), resolve(right, row)) else {
                    return false;
                };
                match l.compare(&r) {
                    Some(ord) => match op {
                        CompareOp::Eq => ord.is_eq(),
                        CompareOp::Ne => ord.is_ne(),
                        CompareOp::Lt => ord.is_lt(),
                        CompareOp::Le => ord.is_le(),
                        CompareOp::Gt => ord.is_gt(),
                        CompareOp::Ge => ord.is_ge(),
                    },
                    None => false,
                }
            }
            PreparedKind::IsNull { column } => row.get(column).is_none_or(Value::is_null),
            PreparedKind::IsEmpty { column } => match row.get(column) {
                None | Some(Value::Null) => true,
                Some(v) => v.canonical_string().is_some_and(|s| s.is_empty()),
            },
            PreparedKind::Matches { column, regex } => row
                .get(column)
                .and_then(Value::display_string)
                .is_some_and(|s| regex.is_match(&s)),
            PreparedKind::In { column, values } => match row.get(column) {
                Some(v) => values
                    .iter()
                    .any(|c| v.compare(c).is_some_and(std::cmp::Ordering::is_eq)),
                None => false,
            },
        }
    }
}

/// Resolve a prepared operand against a row. A column absent from the row yields `None`.
fn resolve(operand: &PreparedOperand, row: &Row<'_>) -> Option<Value> {
    match operand {
        PreparedOperand::Column(column) => row.get(column).cloned(),
        PreparedOperand::Literal(value) => Some(value.clone()),
    }
}

/// The declared [`ValueKind`] of `operand`, if it is an `Operand::Column` with an entry in
/// `schema`. `None` for a `Literal` operand, or for a `Column` whose type is not known.
fn column_kind(operand: &Operand, schema: Option<&TableSchema>) -> Option<ValueKind> {
    match operand {
        Operand::Column { column } => schema
            .and_then(|s| s.columns.get(column))
            .and_then(ColumnSchema::declared_kind),
        Operand::Literal { .. } => None,
    }
}

/// Prepare one side of a `Compare`, coercing a `Literal` to `other_side_kind` (the other side's
/// declared column kind, if any) when it parses cleanly. See [`Predicate::prepare`] for the full
/// rule.
fn prepare_operand(operand: &Operand, other_side_kind: Option<ValueKind>) -> PreparedOperand {
    match operand {
        Operand::Column { column } => PreparedOperand::Column(column.clone()),
        Operand::Literal { value } => {
            PreparedOperand::Literal(prepare_literal(value, other_side_kind))
        }
    }
}

/// Coerce `literal` to `kind`, when given, if it parses cleanly. Otherwise leave it exactly as
/// authored.
///
/// The coercion rule shared by every literal position, in this evaluator and in the SQL a
/// compiled view carries. See [`Predicate::prepare`] for the full rule.
pub(crate) fn prepare_literal(literal: &Literal, kind: Option<ValueKind>) -> Value {
    let natural = literal.as_value();
    let coerced = kind.and_then(|kind| natural.coerce_to(kind));
    coerced.unwrap_or(natural)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::extraction::row::with_row;
    use crate::core::event_data::object_centric::extraction::value::Value;

    fn col(name: &str) -> Operand {
        Operand::Column {
            column: name.to_string(),
        }
    }

    fn schema_of(columns: &[(&str, &str)]) -> TableSchema {
        TableSchema::new(
            "t",
            columns
                .iter()
                .map(|&(name, col_type)| (name, col_type, true)),
        )
    }

    #[test]
    fn a_text_literal_is_coerced_to_match_an_integer_column() {
        // The regression this replaces: a text-input editor emits Literal::Text("1"), which
        // used to leave docstatus = 1 matching zero rows against an INTEGER column.
        let schema = schema_of(&[("docstatus", "INTEGER")]);
        let p = Predicate::Compare {
            left: col("docstatus"),
            op: CompareOp::Eq,
            right: Operand::Literal {
                value: Literal::Text("1".into()),
            },
        };
        let prepared = p.prepare(Some(&schema)).unwrap();
        with_row(&[("docstatus", Value::Integer(1))], |row| {
            assert!(prepared.evaluate(row));
        });
    }

    #[test]
    fn a_text_iso8601_literal_is_coerced_and_orders_against_a_timestamp_column() {
        let schema = schema_of(&[("created_at", "TIMESTAMPTZ")]);
        let p = Predicate::Compare {
            left: col("created_at"),
            op: CompareOp::Gt,
            right: Operand::Literal {
                value: Literal::Text("2019-01-01T00:00:00Z".into()),
            },
        };
        let prepared = p.prepare(Some(&schema)).unwrap();
        let ts = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap();
        with_row(&[("created_at", Value::Timestamp(ts))], |row| {
            assert!(prepared.evaluate(row));
        });
    }

    #[test]
    fn an_uncoercible_literal_matches_nothing_without_panicking() {
        let schema = schema_of(&[("docstatus", "INTEGER")]);
        let p = Predicate::Compare {
            left: col("docstatus"),
            op: CompareOp::Eq,
            right: Operand::Literal {
                value: Literal::Text("abc".into()),
            },
        };
        let prepared = p.prepare(Some(&schema)).unwrap();
        with_row(&[("docstatus", Value::Integer(1))], |row| {
            assert!(!prepared.evaluate(row));
        });
    }

    #[test]
    fn an_unrecognised_col_type_leaves_the_literal_untouched() {
        let schema = schema_of(&[("shape", "GEOMETRY")]);
        let p = Predicate::Compare {
            left: col("shape"),
            op: CompareOp::Eq,
            right: Operand::Literal {
                value: Literal::Text("1".into()),
            },
        };
        let prepared = p.prepare(Some(&schema)).unwrap();
        // Left uncoerced, the literal stays Text and never matches an Integer column value --
        // same outcome as an uncoercible literal, which is the point: an unknown col_type must
        // not be guessed at.
        with_row(&[("shape", Value::Integer(1))], |row| {
            assert!(!prepared.evaluate(row));
        });
        with_row(&[("shape", Value::Text("1".into()))], |row| {
            assert!(prepared.evaluate(row));
        });
    }

    #[test]
    fn a_timestamp_literal_compares_directly_without_needing_coercion() {
        let ts = chrono::DateTime::parse_from_rfc3339("2020-06-15T00:00:00Z").unwrap();
        let p = Predicate::Compare {
            left: col("created_at"),
            op: CompareOp::Eq,
            right: Operand::Literal {
                value: Literal::Timestamp { timestamp: ts },
            },
        };
        let prepared = p.prepare(None).unwrap();
        with_row(&[("created_at", Value::Timestamp(ts))], |row| {
            assert!(prepared.evaluate(row));
        });
    }

    #[test]
    fn a_bare_json_string_is_not_swallowed_by_the_timestamp_variant() {
        // Literal::Timestamp's object shape ({"timestamp": ...}) must never match a bare JSON
        // string, regardless of where it sits among the untagged variants.
        let text: Literal = serde_json::from_str(r#""2020-01-01T00:00:00Z""#).unwrap();
        assert_eq!(text, Literal::Text("2020-01-01T00:00:00Z".into()));

        let ts = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap();
        let json = serde_json::to_string(&Literal::Timestamp { timestamp: ts }).unwrap();
        let back: Literal = serde_json::from_str(&json).unwrap();
        assert_eq!(back, Literal::Timestamp { timestamp: ts });
    }

    #[test]
    fn compares_numbers_numerically_not_as_text() {
        // "9" > "10" lexicographically; 9 < 10 numerically. The typed path must win.
        let p = Predicate::Compare {
            left: col("n"),
            op: CompareOp::Gt,
            right: Operand::Literal {
                value: Literal::Integer(10),
            },
        };
        with_row(&[("n", Value::Integer(9))], |row| {
            assert!(!p.prepare(None).unwrap().evaluate(row));
        });
    }

    #[test]
    fn compares_two_columns() {
        // The change-log case: old <> new.
        let p = Predicate::Compare {
            left: col("old"),
            op: CompareOp::Ne,
            right: col("new"),
        };
        let prepared = p.prepare(None).unwrap();
        with_row(
            &[
                ("old", Value::Text("A".into())),
                ("new", Value::Text("B".into())),
            ],
            |row| {
                assert!(prepared.evaluate(row));
            },
        );
        with_row(
            &[
                ("old", Value::Text("A".into())),
                ("new", Value::Text("A".into())),
            ],
            |row| {
                assert!(!prepared.evaluate(row));
            },
        );
    }

    #[test]
    fn null_never_compares_equal_even_to_null() {
        let p = Predicate::Compare {
            left: col("a"),
            op: CompareOp::Eq,
            right: col("b"),
        };
        with_row(&[("a", Value::Null), ("b", Value::Null)], |row| {
            assert!(!p.prepare(None).unwrap().evaluate(row));
        });
    }

    #[test]
    fn not_negates_and_empty_and_or_are_identity_and_absorbing() {
        let t = Predicate::And { conditions: vec![] };
        let f = Predicate::Or { conditions: vec![] };
        with_row(&[("a", Value::Integer(1))], |row| {
            assert!(t.prepare(None).unwrap().evaluate(row));
            assert!(!f.prepare(None).unwrap().evaluate(row));
            let n = Predicate::Not {
                condition: Box::new(f.clone()),
            };
            assert!(n.prepare(None).unwrap().evaluate(row));
        });
    }

    #[test]
    fn is_null_and_is_empty_are_different_questions() {
        let is_null = Predicate::IsNull { column: "a".into() };
        let is_empty = Predicate::IsEmpty { column: "a".into() };
        with_row(&[("a", Value::Null)], |row| {
            assert!(is_null.prepare(None).unwrap().evaluate(row));
            assert!(is_empty.prepare(None).unwrap().evaluate(row));
        });
        with_row(&[("a", Value::Text(String::new()))], |row| {
            assert!(!is_null.prepare(None).unwrap().evaluate(row));
            assert!(is_empty.prepare(None).unwrap().evaluate(row));
        });
    }

    #[test]
    fn in_matches_any_listed_literal() {
        let p = Predicate::In {
            column: "t".into(),
            values: vec![
                Literal::Text("out_invoice".into()),
                Literal::Text("in_invoice".into()),
            ],
        };
        let prepared = p.prepare(None).unwrap();
        with_row(&[("t", Value::Text("in_invoice".into()))], |row| {
            assert!(prepared.evaluate(row))
        });
        with_row(&[("t", Value::Text("entry".into()))], |row| {
            assert!(!prepared.evaluate(row))
        });
    }

    #[test]
    fn in_coerces_a_text_literal_to_match_an_integer_column() {
        // Same regression as `a_text_literal_is_coerced_to_match_an_integer_column`, but for
        // `In`: `docstatus IN ["1"]` authored the same way as `docstatus = 1` must behave the
        // same way, not silently match zero rows.
        let schema = schema_of(&[("docstatus", "INTEGER")]);
        let p = Predicate::In {
            column: "docstatus".into(),
            values: vec![Literal::Text("1".into())],
        };
        let prepared = p.prepare(Some(&schema)).unwrap();
        with_row(&[("docstatus", Value::Integer(1))], |row| {
            assert!(prepared.evaluate(row));
        });
    }

    #[test]
    fn in_with_an_uncoercible_literal_matches_nothing_without_panicking() {
        let schema = schema_of(&[("docstatus", "INTEGER")]);
        let p = Predicate::In {
            column: "docstatus".into(),
            values: vec![Literal::Text("abc".into())],
        };
        let prepared = p.prepare(Some(&schema)).unwrap();
        with_row(&[("docstatus", Value::Integer(1))], |row| {
            assert!(!prepared.evaluate(row));
        });
    }

    #[test]
    fn in_coerces_each_literal_independently() {
        // One uncoercible value in the list must not stop a coercible sibling from matching.
        let schema = schema_of(&[("docstatus", "INTEGER")]);
        let p = Predicate::In {
            column: "docstatus".into(),
            values: vec![Literal::Text("1".into()), Literal::Text("abc".into())],
        };
        let prepared = p.prepare(Some(&schema)).unwrap();
        with_row(&[("docstatus", Value::Integer(1))], |row| {
            assert!(prepared.evaluate(row), "the coercible literal must match");
        });
        with_row(&[("docstatus", Value::Integer(2))], |row| {
            assert!(!prepared.evaluate(row), "neither literal matches 2");
        });
    }

    /// `"NaN".parse::<f64>()` succeeds, so a plain-JSON `Literal::Text("NaN")` against a
    /// `DOUBLE` column coerces to `Value::Float(NaN)` and the emitter renders
    /// `CAST('NaN' AS DOUBLE)`. `DuckDB` gives floats a total order, so `col = 'NaN'`,
    /// `col > 1.0` and `col IN ('NaN')` are all true there, so `Value::compare` has to agree or
    /// the extractor drops every row a compiled view keeps.
    #[test]
    fn compare_and_in_follow_sql_s_total_float_order_for_nan() {
        let schema = schema_of(&[("amount", "DOUBLE")]);
        let cmp = |op: CompareOp, lit: &str| {
            Predicate::Compare {
                left: Operand::Column {
                    column: "amount".into(),
                },
                op,
                right: Operand::Literal {
                    value: Literal::Text(lit.into()),
                },
            }
            .prepare(Some(&schema))
            .unwrap()
        };
        with_row(&[("amount", Value::Float(f64::NAN))], |row| {
            assert!(cmp(CompareOp::Eq, "NaN").evaluate(row), "NaN = NaN");
            assert!(!cmp(CompareOp::Ne, "NaN").evaluate(row), "NaN <> NaN");
            assert!(cmp(CompareOp::Gt, "1.0").evaluate(row), "NaN > 1.0");
            assert!(!cmp(CompareOp::Lt, "1.0").evaluate(row), "NaN < 1.0");

            let in_nan = Predicate::In {
                column: "amount".into(),
                values: vec![Literal::Text("NaN".into())],
            }
            .prepare(Some(&schema))
            .unwrap();
            assert!(in_nan.evaluate(row), "NaN IN (NaN)");
        });
        with_row(&[("amount", Value::Float(1.0))], |row| {
            assert!(cmp(CompareOp::Lt, "NaN").evaluate(row), "1.0 < NaN");
            assert!(!cmp(CompareOp::Gt, "NaN").evaluate(row), "1.0 > NaN");
        });
    }

    #[test]
    fn matches_reads_a_timestamp_or_float_column_instead_of_matching_nothing() {
        // `canonical_string` is `None` for `Float` and `Timestamp`, so reusing it here would make
        // a `Matches` against those columns false for every row.
        let ts = chrono::DateTime::parse_from_rfc3339("2020-02-03T04:05:06+02:00").unwrap();
        let p = Predicate::Matches {
            column: "created_at".into(),
            regex: "^2020".into(),
        }
        .prepare(None)
        .unwrap();
        with_row(&[("created_at", Value::Timestamp(ts))], |row| {
            assert!(p.evaluate(row));
        });

        let p = Predicate::Matches {
            column: "amount".into(),
            regex: r"^1\.5$".into(),
        }
        .prepare(None)
        .unwrap();
        with_row(&[("amount", Value::Float(1.5))], |row| {
            assert!(p.evaluate(row));
        });
    }

    #[test]
    fn prepare_reports_an_invalid_regex_instead_of_panicking() {
        let p = Predicate::Matches {
            column: "a".into(),
            regex: "([".into(),
        };
        assert!(p.prepare(None).is_err());
    }

    #[test]
    fn referenced_columns_collects_from_every_variant() {
        let p = Predicate::And {
            conditions: vec![
                Predicate::Compare {
                    left: col("a"),
                    op: CompareOp::Eq,
                    right: col("b"),
                },
                Predicate::Not {
                    condition: Box::new(Predicate::IsNull { column: "c".into() }),
                },
                Predicate::In {
                    column: "d".into(),
                    values: vec![],
                },
                Predicate::Matches {
                    column: "e".into(),
                    regex: ".".into(),
                },
            ],
        };
        let mut cols = HashSet::new();
        p.referenced_columns(&mut cols);
        let mut got: Vec<&str> = cols.into_iter().collect();
        got.sort_unstable();
        assert_eq!(got, vec!["a", "b", "c", "d", "e"]);
    }

    #[test]
    fn an_empty_column_name_is_not_swallowed() {
        // A dropped guard here used to hide `{"type":"column","column":""}`, which drops every
        // row at evaluation time, from validation.
        let p = Predicate::IsNull {
            column: String::new(),
        };
        let mut cols = HashSet::new();
        p.referenced_columns(&mut cols);
        assert_eq!(cols, HashSet::from([""]));
    }
}
