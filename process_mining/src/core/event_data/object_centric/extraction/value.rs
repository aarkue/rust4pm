//! The value type rows carry, and its canonical rendering.
//!
//! Floats follow SQL's total order rather than IEEE's: `DuckDB` and `PostgreSQL` both sort `NaN`
//! above everything else and treat `NaN = NaN` and `-0.0 = 0.0` as true, where `partial_cmp`
//! answers `None`. [`Value::compare`] and [`Value::join_key_part`] follow the engines, so that
//! `Ne` does not go false for every operand and `-0.0`/`0.0` do not get separate join keys.
//!
//! Join keys carry a kind tag, so text never joins with numbers. The engines disagree here:
//! `DuckDB` casts and joins `'1'` with `1`, `PostgreSQL` rejects `text = integer`. Refusing the
//! join is the only total engine-independent option, so a compiler must not emit a bare
//! `l.k = r.k` across kinds.
//!
//! The tag is the value's runtime kind, which a compiler cannot see: it only has
//! [`ColumnSchema::declared_kind`](super::catalog::ColumnSchema::declared_kind). Reproducing the
//! rule in SQL is sound exactly when the catalog matches the values' actual kinds, which holds for
//! statically typed engines but not for `SQLite`. That is a catalog precondition rather than
//! something a blueprint can be checked for, so [`validate`](super::validate::validate) does not
//! reject such a join.

use std::cmp::Ordering;
use std::fmt::Write;

use chrono::{DateTime, FixedOffset};

/// A single cell of a row, normalised across data sources.
///
/// Owned by this crate rather than taken from a driver, so the model and compiler need no
/// connector dependency. Not `Serialize`: a row's values never cross a bindings boundary.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// SQL `NULL` or a missing cell.
    Null,
    /// Text.
    Text(String),
    /// A 64-bit signed integer.
    Integer(i64),
    /// A double-precision float.
    Float(f64),
    /// A boolean.
    Boolean(bool),
    /// An instant with a fixed UTC offset.
    Timestamp(DateTime<FixedOffset>),
}

/// A [`Value`]'s kind, independent of any particular value.
///
/// This is the vocabulary a source's declared column type ([`ColumnSchema::col_type`], mapped by
/// [`ColumnSchema::declared_kind`](super::catalog::ColumnSchema::declared_kind)) is translated
/// into, and the target of [`Value::coerce_to`].
///
/// [`ColumnSchema::col_type`]: super::catalog::ColumnSchema::col_type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    /// [`Value::Text`].
    Text,
    /// [`Value::Integer`].
    Integer,
    /// [`Value::Float`].
    Float,
    /// [`Value::Boolean`].
    Boolean,
    /// [`Value::Timestamp`].
    Timestamp,
}

/// `f` as an `i64` when it holds a whole number exactly representable as one, else `None`.
///
/// `f64` can represent integers beyond `i64::MAX` only approximately, so the range check keeps a
/// value that would round on conversion out of an identity. The upper bound is strict because
/// `i64::MAX as f64` rounds up to 2^63, which `as i64` then saturates back to `i64::MAX`.
fn whole_number(f: f64) -> Option<i64> {
    (f.fract() == 0.0 && f >= i64::MIN as f64 && f < i64::MAX as f64).then_some(f as i64)
}

impl Value {
    /// This value's [`ValueKind`], or `None` for [`Value::Null`], which has no kind of its own.
    #[must_use]
    pub fn kind(&self) -> Option<ValueKind> {
        match self {
            Value::Null => None,
            Value::Text(_) => Some(ValueKind::Text),
            Value::Integer(_) => Some(ValueKind::Integer),
            Value::Float(_) => Some(ValueKind::Float),
            Value::Boolean(_) => Some(ValueKind::Boolean),
            Value::Timestamp(_) => Some(ValueKind::Timestamp),
        }
    }

    /// Reinterpret this value as `kind`, when that parses cleanly. `None` otherwise.
    ///
    /// Used by `Predicate::prepare` to coerce a
    /// `Compare` literal to its column's declared type. Part of the model's semantics, not an
    /// implementation detail: [`compile`](super::compile()) reproduces exactly this rule in SQL.
    ///
    /// A value already of `kind` is returned unchanged, and one number converts to the other
    /// numeric kind directly. Everything else reads [`Value::display_string`] and parses it as
    /// `kind`:
    ///
    /// - `Integer`, `Float`: [`str::parse`], so `"1"` and `"1.5"` succeed and `"abc"` does not.
    /// - `Boolean`: exactly `"true"` or `"false"`.
    /// - `Timestamp`: strict RFC 3339 only (via [`DateTime::parse_from_rfc3339`]), not the
    ///   lenient cascade `TimestampSource::parse` uses.
    ///   RFC 3339 is what every mainstream engine's timestamp cast accepts, which is what keeps
    ///   the evaluator and a compiled view in agreement.
    /// - `Text`: always succeeds, since every non-`Null` value has a `display_string`.
    ///
    /// `Null` never coerces to anything.
    #[must_use]
    pub fn coerce_to(&self, kind: ValueKind) -> Option<Value> {
        if self.kind() == Some(kind) {
            return Some(self.clone());
        }
        // Same answer as the text round trip below, without an allocation per value.
        match (self, kind) {
            #[allow(clippy::cast_precision_loss)]
            (Value::Integer(i), ValueKind::Float) => return Some(Value::Float(*i as f64)),
            (Value::Float(f), ValueKind::Integer) => return whole_number(*f).map(Value::Integer),
            _ => {}
        }
        let s = self.display_string()?;
        match kind {
            ValueKind::Text => Some(Value::Text(s)),
            ValueKind::Integer => s.parse::<i64>().ok().map(Value::Integer),
            ValueKind::Float => s.parse::<f64>().ok().map(Value::Float),
            ValueKind::Boolean => match s.as_str() {
                "true" => Some(Value::Boolean(true)),
                "false" => Some(Value::Boolean(false)),
                _ => None,
            },
            ValueKind::Timestamp => DateTime::parse_from_rfc3339(&s).ok().map(Value::Timestamp),
        }
    }

    /// The canonical text form used wherever a value becomes an identity: entity ids,
    /// relation endpoints, type names and template placeholders.
    ///
    /// Only `Text`, `Integer`, `Boolean` and a `Float` holding a whole number have one.
    /// Fractional `Float` formatting is not stable across engines, `Timestamp` offset rendering
    /// varies, and `Null` has no identity at all, so those return `None` and the caller drops the
    /// row rather than inventing an id.
    ///
    /// A whole-number `Float` renders as that integer, because `numeric`/`decimal` columns decode
    /// as `Float` and an integer key in one would otherwise have no identity at all.
    ///
    /// Not the rendering used to match a regex or reparse a value. See
    /// [`Value::display_string`].
    #[must_use]
    pub fn canonical_string(&self) -> Option<String> {
        match self {
            Value::Text(s) => Some(s.clone()),
            Value::Integer(i) => Some(i.to_string()),
            Value::Boolean(b) => Some(if *b { "true" } else { "false" }.to_string()),
            Value::Float(f) => whole_number(*f).map(|i| i.to_string()),
            Value::Timestamp(_) | Value::Null => None,
        }
    }

    /// The key one cell contributes to a [`Join`](super::blueprint::NodeOp::Join)'s equality
    /// test, or `None` when this value never joins.
    ///
    /// Not [`Value::canonical_string`], which is `None` for `Float` and `Timestamp` and would
    /// make a join on such a column silently yield zero rows where SQL joins them. Join keys get
    /// a total rendering instead:
    ///
    /// - `Null` is the one value with no key: SQL `NULL` never equals anything.
    /// - `Integer` and `Float` share one key space (`1` joins `1.0`, as in SQL).
    /// - Floats follow SQL's total order, not IEEE's: `NaN` joins `NaN` and `-0.0` joins `0.0`,
    ///   as `DuckDB` and `PostgreSQL` both define. Infinities are ordinary values.
    /// - `Timestamp` is normalised to UTC first, so one instant written with two offsets joins.
    /// - Every other kind is tagged with its kind, so a `Text` `"1"` does not join `Integer` `1`.
    ///
    /// The kind tag diverges from every engine, so a compiler has to reproduce it rather than
    /// inherit it. See this module's docs.
    #[must_use]
    pub fn join_key_part(&self) -> Option<String> {
        let mut out = String::new();
        self.write_join_key_part(&mut out).then_some(out)
    }

    /// [`Value::join_key_part`] appended to `out`, reporting `false` for `Null`, the one value
    /// with no key.
    ///
    /// The allocation-free spelling, for a hash join that keys every row of both its inputs.
    pub(crate) fn write_join_key_part(&self, out: &mut String) -> bool {
        let written = match self {
            Value::Null => return false,
            Value::Text(s) => {
                out.push_str("s:");
                out.push_str(s);
                Ok(())
            }
            Value::Integer(i) => write!(out, "n:{i}"),
            // `-0.0 == 0.0` in Rust, so this maps the negative zero onto the positive one and
            // leaves every other value (`NaN` included) alone.
            Value::Float(f) => write!(out, "n:{}", if *f == 0.0 { 0.0 } else { *f }),
            Value::Boolean(b) => write!(out, "b:{b}"),
            Value::Timestamp(t) => write!(out, "t:{}", t.to_utc().to_rfc3339()),
        };
        written.is_ok()
    }

    /// The text form used wherever a value is being read rather than turned into an identity:
    /// the input to a `Matches` regex, or the fallback input to
    /// `TimestampSource::parse` when the column is not
    /// already a `Timestamp`.
    ///
    /// Unlike [`Value::canonical_string`], `Float` and `Timestamp` render here: reading a value
    /// needs some text, and cross-engine stability only matters for an identity. Only `Null`
    /// has no rendering.
    #[must_use]
    pub fn display_string(&self) -> Option<String> {
        match self {
            Value::Text(s) => Some(s.clone()),
            Value::Integer(i) => Some(i.to_string()),
            Value::Float(f) => Some(f.to_string()),
            Value::Boolean(b) => Some(if *b { "true" } else { "false" }.to_string()),
            Value::Timestamp(ts) => Some(ts.to_rfc3339()),
            Value::Null => None,
        }
    }

    /// Whether this is [`Value::Null`].
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Typed ordering, used by comparison predicates.
    ///
    /// Integers and floats are one ordering class, so `2 < 2.5` holds. Any other pair of
    /// different kinds returns `None`, as does any comparison involving `Null`, `Null` against
    /// `Null` included, since SQL `NULL` is not equal to itself. A `None` result makes the
    /// comparing predicate false.
    ///
    /// Floats follow SQL's total order rather than IEEE's, see this module's docs.
    #[must_use]
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => None,
            (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => Some(sql_float_cmp(*a, *b)),
            #[allow(clippy::cast_precision_loss)]
            (Value::Integer(a), Value::Float(b)) => Some(sql_float_cmp(*a as f64, *b)),
            #[allow(clippy::cast_precision_loss)]
            (Value::Float(a), Value::Integer(b)) => Some(sql_float_cmp(*a, *b as f64)),
            (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
            (Value::Boolean(a), Value::Boolean(b)) => Some(a.cmp(b)),
            (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

/// Compare two floats the way `DuckDB` and `PostgreSQL` do: a total order in which `NaN` equals
/// itself and sorts above everything else, and `-0.0` equals `0.0`.
///
/// Not [`f64::total_cmp`], which separates `-0.0` from `0.0` and distinguishes `NaN` payloads and
/// signs. SQL does neither.
fn sql_float_cmp(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn canonical_string_renders_only_stable_types() {
        assert_eq!(
            Value::Text("a".into()).canonical_string().as_deref(),
            Some("a")
        );
        assert_eq!(Value::Integer(-7).canonical_string().as_deref(), Some("-7"));
        assert_eq!(
            Value::Boolean(true).canonical_string().as_deref(),
            Some("true")
        );
        assert_eq!(
            Value::Boolean(false).canonical_string().as_deref(),
            Some("false")
        );
        // A whole-number float is an identity: `numeric`/`decimal` columns decode as Float, and an
        // integer key in one (Sakila's `actor_id numeric`) must not drop every row it keys.
        assert_eq!(Value::Float(1.0).canonical_string().as_deref(), Some("1"));
        assert_eq!(
            Value::Float(200.0).canonical_string().as_deref(),
            Some("200")
        );
        // A fractional float has no stable rendering across engines, so it still has no identity.
        assert!(Value::Float(1.5).canonical_string().is_none());
        assert!(Value::Float(f64::NAN).canonical_string().is_none());
        assert!(Value::Float(f64::INFINITY).canonical_string().is_none());
        // Beyond i64 an f64 only approximates whole numbers, so it would round into a wrong id.
        assert!(Value::Float(1e30).canonical_string().is_none());
        // `i64::MAX as f64` rounds up to 2^63, which `as i64` saturates back to `i64::MAX`: an
        // inclusive upper bound gave this float the identity of a different number.
        assert!(Value::Float(9_223_372_036_854_775_808.0)
            .canonical_string()
            .is_none());
        assert!(Value::Null.canonical_string().is_none());
    }

    #[test]
    fn display_string_renders_everything_but_null() {
        assert_eq!(Value::Float(1.5).display_string().as_deref(), Some("1.5"));
        assert!(Value::Timestamp(
            chrono::DateTime::parse_from_rfc3339("2020-02-03T04:05:06+02:00").unwrap()
        )
        .display_string()
        .is_some());
        assert!(Value::Null.display_string().is_none());
    }

    #[test]
    fn numbers_compare_across_integer_and_float() {
        assert_eq!(
            Value::Integer(2).compare(&Value::Float(2.5)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::Float(10.0).compare(&Value::Integer(9)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Value::Integer(3).compare(&Value::Float(3.0)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn negative_floats_order_before_positive() {
        assert_eq!(
            Value::Float(-1.5).compare(&Value::Float(0.5)),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn mismatched_kinds_and_null_do_not_order() {
        assert!(Value::Text("1".into())
            .compare(&Value::Integer(1))
            .is_none());
        assert!(Value::Null.compare(&Value::Null).is_none());
        assert!(Value::Null.compare(&Value::Integer(1)).is_none());
    }

    #[test]
    fn coerce_to_parses_cleanly_or_returns_none() {
        assert_eq!(
            Value::Text("1".into()).coerce_to(ValueKind::Integer),
            Some(Value::Integer(1))
        );
        assert_eq!(
            Value::Text("2020-01-01T00:00:00Z".into()).coerce_to(ValueKind::Timestamp),
            Some(Value::Timestamp(
                chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap()
            ))
        );
        assert_eq!(
            Value::Text("abc".into()).coerce_to(ValueKind::Integer),
            None
        );
        assert_eq!(
            Value::Text("not-a-date".into()).coerce_to(ValueKind::Timestamp),
            None
        );
    }

    #[test]
    fn join_keys_are_total_except_for_null() {
        // Regression: Float and Timestamp keys used to render through `canonical_string`, which
        // is `None` for both, so a join on such a column silently produced zero rows.
        assert!(Value::Float(1.5).join_key_part().is_some());
        assert!(Value::Timestamp(
            chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap()
        )
        .join_key_part()
        .is_some());
        assert!(Value::Null.join_key_part().is_none());
    }

    /// `DuckDB` and `PostgreSQL` both give floats a total order for comparison: `NaN = NaN` and
    /// `-0.0 = 0.0` are both true. Treating `NaN` like `NULL`, or giving `-0.0` its own key, loses
    /// rows a compiled view keeps.
    #[test]
    fn join_keys_follow_sql_s_total_float_order() {
        assert_eq!(
            Value::Float(f64::NAN).join_key_part(),
            Value::Float(f64::NAN).join_key_part()
        );
        assert!(Value::Float(f64::NAN).join_key_part().is_some());
        assert_eq!(
            Value::Float(f64::INFINITY).join_key_part(),
            Value::Float(f64::INFINITY).join_key_part()
        );
        assert_ne!(
            Value::Float(f64::INFINITY).join_key_part(),
            Value::Float(f64::NEG_INFINITY).join_key_part()
        );
        assert_ne!(
            Value::Float(f64::NAN).join_key_part(),
            Value::Float(f64::INFINITY).join_key_part()
        );
        assert_eq!(
            Value::Float(-0.0).join_key_part(),
            Value::Float(0.0).join_key_part()
        );
        assert_eq!(
            Value::Float(-0.0).join_key_part(),
            Value::Integer(0).join_key_part()
        );
    }

    /// `Value::compare` decides `Compare` and `In`, whose emitted SQL is
    /// `COALESCE(col <op> lit, FALSE)`, run by an engine that gives floats the same total order
    /// `join_key_part` commits to. `partial_cmp` answers `None` for any pair
    /// involving `NaN`, which `PreparedPredicate::evaluate` turns into `false` for every
    /// operator, while `DuckDB` makes `NaN = NaN` and `NaN > 1.0` both true.
    #[test]
    fn compare_follows_sql_s_total_float_order() {
        assert_eq!(
            Value::Float(f64::NAN).compare(&Value::Float(f64::NAN)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            Value::Float(f64::NAN).compare(&Value::Float(1.0)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Value::Float(1.0).compare(&Value::Float(f64::NAN)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::Float(f64::NAN).compare(&Value::Float(f64::INFINITY)),
            Some(Ordering::Greater)
        );
        // Mixed integer/float: an integer is never `NaN`, so it always sorts below one.
        assert_eq!(
            Value::Integer(1).compare(&Value::Float(f64::NAN)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::Float(f64::NAN).compare(&Value::Integer(1)),
            Some(Ordering::Greater)
        );
        // `-0.0` and `0.0` are one value, exactly as in `join_key_part`.
        assert_eq!(
            Value::Float(-0.0).compare(&Value::Float(0.0)),
            Some(Ordering::Equal)
        );
        // `Null` stays incomparable: SQL `NULL` is not equal to itself.
        assert!(Value::Null.compare(&Value::Float(f64::NAN)).is_none());
    }

    #[test]
    fn join_keys_equate_integers_with_floats_but_not_text_with_numbers() {
        assert_eq!(
            Value::Integer(1).join_key_part(),
            Value::Float(1.0).join_key_part()
        );
        assert_ne!(
            Value::Integer(1).join_key_part(),
            Value::Text("1".into()).join_key_part()
        );
    }

    #[test]
    fn join_keys_normalise_a_timestamp_offset() {
        let utc =
            Value::Timestamp(chrono::DateTime::parse_from_rfc3339("2020-01-01T02:00:00Z").unwrap());
        let offset = Value::Timestamp(
            chrono::DateTime::parse_from_rfc3339("2020-01-01T04:00:00+02:00").unwrap(),
        );
        assert_eq!(utc.join_key_part(), offset.join_key_part());
    }

    #[test]
    fn coerce_to_a_value_already_of_that_kind_is_unchanged() {
        assert_eq!(
            Value::Integer(5).coerce_to(ValueKind::Integer),
            Some(Value::Integer(5))
        );
    }

    /// The direct numeric arms must answer exactly what the text round trip answered.
    #[test]
    fn a_number_coerces_to_the_other_numeric_kind_or_not_at_all() {
        assert_eq!(
            Value::Integer(5).coerce_to(ValueKind::Float),
            Some(Value::Float(5.0))
        );
        assert_eq!(
            Value::Float(5.0).coerce_to(ValueKind::Integer),
            Some(Value::Integer(5))
        );
        assert_eq!(Value::Float(5.5).coerce_to(ValueKind::Integer), None);
        assert_eq!(Value::Float(1e30).coerce_to(ValueKind::Integer), None);
        assert_eq!(Value::Float(f64::NAN).coerce_to(ValueKind::Integer), None);
    }

    #[test]
    fn null_never_coerces() {
        assert_eq!(Value::Null.coerce_to(ValueKind::Text), None);
    }
}
