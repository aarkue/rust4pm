//! Value (de)serialization: object attributes use the EAV string format
//! (`to_sql_value`/`from_sql_value`), event attributes the typed `duckdb::types::Value` converters.
use std::borrow::Cow;

use chrono::{DateTime, FixedOffset};
use duckdb::types::{TimeUnit, Value as DuckValue};

use crate::core::event_data::object_centric::ocel_struct::{OCELAttributeType, OCELAttributeValue};
use crate::core::event_data::timestamp_utils::parse_timestamp;

/// Convert a `DuckDB` timestamp `(unit, count)` to a [`DateTime<FixedOffset>`] at `+00:00`, or
/// `None` if the instant is out of `chrono`'s range. The original offset is not recoverable.
pub(crate) fn duck_timestamp_to_datetime(tu: TimeUnit, t: i64) -> Option<DateTime<FixedOffset>> {
    // Euclidean division so pre-epoch counts keep a non-negative sub-second remainder
    // instead of truncating toward zero.
    let (secs, nsecs) = match tu {
        TimeUnit::Second => (t, 0),
        TimeUnit::Millisecond => (t.div_euclid(1_000), t.rem_euclid(1_000) * 1_000_000),
        TimeUnit::Microsecond => (t.div_euclid(1_000_000), t.rem_euclid(1_000_000) * 1_000),
        TimeUnit::Nanosecond => (t.div_euclid(1_000_000_000), t.rem_euclid(1_000_000_000)),
    };
    Some(DateTime::from_timestamp(secs, nsecs as u32)?.fixed_offset())
}

/// Store a timestamp as UTC microseconds for the `TIMESTAMPTZ` columns. `TIMESTAMPTZ` holds only
/// an instant, so the source offset is lost: `10:00:00+02:00` reads back as `08:00:00Z`.
pub(crate) fn datetime_to_duck_timestamp(t: DateTime<FixedOffset>) -> DuckValue {
    DuckValue::Timestamp(TimeUnit::Microsecond, t.to_utc().timestamp_micros())
}

/// Convert an [`OCELAttributeValue`] to the `duckdb::types::Value` matching the target column's
/// [`OCELAttributeType`]. On mismatch the value is parsed from its `Display` form, or `Null`.
pub(crate) fn ocel_value_to_duck(v: &OCELAttributeValue, target: OCELAttributeType) -> DuckValue {
    match target {
        OCELAttributeType::String | OCELAttributeType::Null => match v {
            OCELAttributeValue::Null => DuckValue::Null,
            OCELAttributeValue::String(s) => DuckValue::Text(s.clone()),
            other => DuckValue::Text(other.to_string()),
        },
        OCELAttributeType::Integer => match v {
            OCELAttributeValue::Integer(i) => DuckValue::BigInt(*i),
            OCELAttributeValue::Float(f) => DuckValue::BigInt(*f as i64),
            OCELAttributeValue::Null => DuckValue::Null,
            other => other
                .to_string()
                .parse::<i64>()
                .map(DuckValue::BigInt)
                .unwrap_or(DuckValue::Null),
        },
        OCELAttributeType::Float => match v {
            OCELAttributeValue::Float(f) => DuckValue::Double(*f),
            OCELAttributeValue::Integer(i) => DuckValue::Double(*i as f64),
            OCELAttributeValue::Null => DuckValue::Null,
            other => other
                .to_string()
                .parse::<f64>()
                .map(DuckValue::Double)
                .unwrap_or(DuckValue::Null),
        },
        OCELAttributeType::Boolean => match v {
            OCELAttributeValue::Boolean(b) => DuckValue::Boolean(*b),
            OCELAttributeValue::Null => DuckValue::Null,
            other => other
                .to_string()
                .parse::<bool>()
                .map(DuckValue::Boolean)
                .unwrap_or(DuckValue::Null),
        },
        OCELAttributeType::Time => match v {
            OCELAttributeValue::Time(t) => datetime_to_duck_timestamp(*t),
            OCELAttributeValue::Null => DuckValue::Null,
            other => parse_timestamp(&other.to_string(), None, false)
                .map(datetime_to_duck_timestamp)
                .unwrap_or(DuckValue::Null),
        },
    }
}

/// Reconstruct an [`OCELAttributeValue`] from a typed wide-column `duckdb::types::Value`.
pub(crate) fn duck_value_to_ocel(v: DuckValue) -> OCELAttributeValue {
    match v {
        DuckValue::Null => OCELAttributeValue::Null,
        DuckValue::Boolean(b) => OCELAttributeValue::Boolean(b),
        DuckValue::TinyInt(i) => OCELAttributeValue::Integer(i as i64),
        DuckValue::SmallInt(i) => OCELAttributeValue::Integer(i as i64),
        DuckValue::Int(i) => OCELAttributeValue::Integer(i as i64),
        DuckValue::BigInt(i) => OCELAttributeValue::Integer(i),
        DuckValue::HugeInt(i) => OCELAttributeValue::Integer(i as i64),
        DuckValue::UTinyInt(i) => OCELAttributeValue::Integer(i as i64),
        DuckValue::USmallInt(i) => OCELAttributeValue::Integer(i as i64),
        DuckValue::UInt(i) => OCELAttributeValue::Integer(i as i64),
        DuckValue::UBigInt(i) => OCELAttributeValue::Integer(i as i64),
        DuckValue::Float(f) => OCELAttributeValue::Float(f as f64),
        DuckValue::Double(f) => OCELAttributeValue::Float(f),
        DuckValue::Text(s) => OCELAttributeValue::String(s),
        DuckValue::Timestamp(tu, t) => duck_timestamp_to_datetime(tu, t)
            .map_or(OCELAttributeValue::Null, OCELAttributeValue::Time),
        _ => OCELAttributeValue::Null,
    }
}

/// Serialize a value to its `(value, value_type)` pair for the EAV columns.
pub(crate) fn to_sql_value(v: &OCELAttributeValue) -> (Cow<'_, str>, &'static str) {
    let value_type = v.get_type().as_type_str();
    match v {
        OCELAttributeValue::String(s) => (Cow::Borrowed(s.as_str()), value_type),
        other => (Cow::Owned(other.to_string()), value_type),
    }
}

/// Reconstruct a value from its stored `(value, value_type)`. Unparseable values become `Null`.
pub(crate) fn from_sql_value(value: &str, value_type: &str) -> OCELAttributeValue {
    match OCELAttributeType::from_type_str(value_type) {
        OCELAttributeType::String => OCELAttributeValue::String(value.to_owned()),
        OCELAttributeType::Integer => value
            .parse::<i64>()
            .map(OCELAttributeValue::Integer)
            .unwrap_or(OCELAttributeValue::Null),
        OCELAttributeType::Float => value
            .parse::<f64>()
            .map(OCELAttributeValue::Float)
            .unwrap_or(OCELAttributeValue::Null),
        OCELAttributeType::Boolean => value
            .parse::<bool>()
            .map(OCELAttributeValue::Boolean)
            .unwrap_or(OCELAttributeValue::Null),
        OCELAttributeType::Time => parse_timestamp(value, None, false)
            .map(OCELAttributeValue::Time)
            .unwrap_or(OCELAttributeValue::Null),
        OCELAttributeType::Null => OCELAttributeValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;

    fn roundtrip(v: OCELAttributeValue) -> OCELAttributeValue {
        let (s, t) = to_sql_value(&v);
        from_sql_value(s.as_ref(), t)
    }

    #[test]
    fn roundtrip_scalars() {
        assert_eq!(
            roundtrip(OCELAttributeValue::Integer(42)),
            OCELAttributeValue::Integer(42)
        );
        assert_eq!(
            roundtrip(OCELAttributeValue::Float(3.5)),
            OCELAttributeValue::Float(3.5)
        );
        assert_eq!(
            roundtrip(OCELAttributeValue::Boolean(true)),
            OCELAttributeValue::Boolean(true)
        );
        assert_eq!(
            roundtrip(OCELAttributeValue::String("hi".into())),
            OCELAttributeValue::String("hi".into())
        );
    }

    #[test]
    fn roundtrip_nan_float() {
        // The XML importer turns a literal `null` float into NaN, which must stay a float.
        let (s, t) = to_sql_value(&OCELAttributeValue::Float(f64::NAN));
        assert_eq!(t, "float");
        match from_sql_value(s.as_ref(), t) {
            OCELAttributeValue::Float(f) => assert!(f.is_nan(), "expected NaN, got {f}"),
            other => panic!("expected Float(NaN), got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_time() {
        let dt = DateTime::parse_from_rfc3339("2023-10-06T09:30:21+02:00").unwrap();
        assert_eq!(
            roundtrip(OCELAttributeValue::Time(dt)),
            OCELAttributeValue::Time(dt)
        );
    }

    #[test]
    fn value_type_strings() {
        assert_eq!(to_sql_value(&OCELAttributeValue::Integer(1)).1, "integer");
        assert_eq!(to_sql_value(&OCELAttributeValue::Float(1.0)).1, "float");
        assert_eq!(
            to_sql_value(&OCELAttributeValue::Time(chrono::Utc::now().fixed_offset())).1,
            "time"
        );
    }

    #[test]
    fn null_roundtrips_to_empty_string_documented_caveat() {
        let (s, t) = to_sql_value(&OCELAttributeValue::Null);
        assert_eq!(s.as_ref(), "");
        assert_eq!(t, "string");
        assert_eq!(
            from_sql_value(s.as_ref(), t),
            OCELAttributeValue::String(String::new())
        );
    }
}
