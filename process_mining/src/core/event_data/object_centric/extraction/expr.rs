//! Expressions producing a value from a row, plus splitting and timestamp parsing.

use std::collections::HashSet;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::row::Row;
use super::value::Value;
use crate::core::event_data::object_centric::OCELAttributeType;

/// Produces one text value from a row.
///
/// Used by every position that becomes an identity: entity ids, relation endpoints, type names
/// and qualifiers. Every variant propagates absence, so if any input has no
/// [`Value::canonical_string`] the whole expression is `None` and the caller drops the row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ValueExpression {
    /// The value in a column.
    Column {
        /// Column name.
        column: String,
    },
    /// A fixed string.
    Constant {
        /// The value.
        value: String,
    },
    /// Text with `{column}` placeholders, for example `ORD-{order_id}-{region}`.
    Template {
        /// The template.
        template: String,
    },
    /// The first part that produces a value.
    Coalesce {
        /// Parts, tried in order.
        parts: Vec<ValueExpression>,
    },
}

impl ValueExpression {
    /// Evaluate against one row.
    pub(crate) fn evaluate(&self, row: &Row<'_>) -> Option<String> {
        match self {
            ValueExpression::Constant { value } => Some(value.clone()),
            ValueExpression::Column { column } => row.get(column).and_then(Value::canonical_string),
            ValueExpression::Template { template } => render_template(template, row),
            ValueExpression::Coalesce { parts } => parts.iter().find_map(|p| p.evaluate(row)),
        }
    }

    /// Collect every column name this expression reads into `out`.
    pub fn referenced_columns<'a>(&'a self, out: &mut HashSet<&'a str>) {
        match self {
            ValueExpression::Column { column } => {
                out.insert(column);
            }
            ValueExpression::Template { template } => {
                for name in template_placeholders(template) {
                    out.insert(name);
                }
            }
            ValueExpression::Coalesce { parts } => {
                for p in parts {
                    p.referenced_columns(out);
                }
            }
            ValueExpression::Constant { .. } => {}
        }
    }
}

/// Substitute `{column}` placeholders, scanning the template rather than the result.
///
/// Scanning the output instead misreads a substituted value that itself contains braces, and
/// accepts an unterminated placeholder.
fn render_template(template: &str, row: &Row<'_>) -> Option<String> {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let close = after.find('}')?;
        let name = &after[..close];
        out.push_str(&row.get(name)?.canonical_string()?);
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    Some(out)
}

/// The placeholder names in a template, in order. An unterminated placeholder ends the scan. An
/// empty placeholder (`{}`) contributes no name: it is a template defect, reported once by
/// `validate`'s `InvalidTemplate` check rather than again as `UnknownColumn { column: "" }`.
fn template_placeholders(template: &str) -> Vec<&str> {
    let mut names = Vec::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else { break };
        let name = &after[..close];
        if !name.is_empty() {
            names.push(name);
        }
        rest = &after[close + 1..];
    }
    names
}

/// How to split one cell into several values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SplitSpec {
    /// The splitting rule.
    pub kind: SplitKind,
    /// Trim surrounding whitespace from each part.
    pub trim: bool,
}

/// The splitting rule of a [`SplitSpec`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum SplitKind {
    /// Split on a literal separator.
    Delimiter {
        /// The separator.
        delimiter: String,
    },
    /// Extract values with a regular expression.
    ///
    /// With capture groups, every group of every match yields a value. Without them, each whole
    /// match does.
    Regex {
        /// The pattern.
        pattern: String,
    },
}

impl SplitSpec {
    /// Compile this split's regular expression once, ahead of repeated row evaluation.
    ///
    /// # Errors
    /// Returns the underlying [`regex::Error`] if a `Regex` pattern does not compile.
    pub(crate) fn prepare(&self) -> Result<PreparedSplit, regex::Error> {
        let kind = match &self.kind {
            SplitKind::Delimiter { delimiter } => PreparedSplitKind::Delimiter(delimiter.clone()),
            SplitKind::Regex { pattern } => PreparedSplitKind::Regex(regex::Regex::new(pattern)?),
        };
        Ok(PreparedSplit {
            kind,
            trim: self.trim,
        })
    }
}

/// A [`SplitSpec`] with its regular expression compiled, ready for repeated evaluation.
#[derive(Debug)]
pub(crate) struct PreparedSplit {
    kind: PreparedSplitKind,
    trim: bool,
}

#[derive(Debug)]
enum PreparedSplitKind {
    Delimiter(String),
    Regex(regex::Regex),
}

impl PreparedSplit {
    /// Split `raw` into values. Empty parts are dropped.
    pub(crate) fn split(&self, raw: &str) -> Vec<String> {
        let keep = |s: &str| -> Option<String> {
            let v = if self.trim { s.trim() } else { s };
            (!v.is_empty()).then(|| v.to_string())
        };
        match &self.kind {
            PreparedSplitKind::Delimiter(delimiter) => {
                if delimiter.is_empty() {
                    return keep(raw).into_iter().collect();
                }
                raw.split(delimiter.as_str()).filter_map(keep).collect()
            }
            PreparedSplitKind::Regex(re) => {
                let mut out = Vec::new();
                for caps in re.captures_iter(raw) {
                    if caps.len() > 1 {
                        for i in 1..caps.len() {
                            if let Some(m) = caps.get(i) {
                                out.extend(keep(m.as_str()));
                            }
                        }
                    } else if let Some(m) = caps.get(0) {
                        out.extend(keep(m.as_str()));
                    }
                }
                out
            }
        }
    }
}

/// Maps a source column to a named OCEL attribute.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AttributeMapping {
    /// Column to read.
    pub source_column: String,
    /// Attribute name in the resulting log.
    pub name: String,
    /// Declared attribute type, or `None` to take the catalog's type for `source_column`.
    pub value_type: Option<OCELAttributeType>,
}

/// How to interpret a timestamp value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TimestampFormat {
    /// Try a cascade of common formats, most specific first.
    Auto,
    /// A `chrono` format string.
    FormatString {
        /// The format.
        format: String,
    },
    /// Seconds since the Unix epoch.
    UnixSeconds,
    /// Milliseconds since the Unix epoch.
    UnixMillis,
}

/// One value read as a timestamp: where the text comes from, and how to read it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TimestampPart {
    /// Where the text comes from.
    pub source: ValueExpression,
    /// How to read it. `None` means auto-detection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<TimestampFormat>,
}

impl TimestampPart {
    /// A part reading `column` with auto-detection.
    pub fn column(column: impl Into<String>) -> Self {
        Self {
            source: ValueExpression::Column {
                column: column.into(),
            },
            format: None,
        }
    }

    /// Parse this part as a whole timestamp.
    pub(crate) fn parse(&self, row: &Row<'_>) -> Option<DateTime<FixedOffset>> {
        // A driver-decoded timestamp is already parsed; rendering it back to text to re-parse
        // it would be both slower and lossier.
        if let ValueExpression::Column { column } = &self.source {
            if let Some(Value::Timestamp(ts)) = row.get(column) {
                return Some(*ts);
            }
        }
        let format = self.format.as_ref().unwrap_or(&TimestampFormat::Auto);
        parse_timestamp(&self.resolve(row)?, format)
    }

    /// The part's text, or `None` if the row carries nothing for it. Blank counts as nothing.
    fn resolve(&self, row: &Row<'_>) -> Option<String> {
        let text = match &self.source {
            // Not `evaluate`: its `canonical_string` is `None` for `Float`, which would drop
            // every row of a Unix-epoch column a driver reports as a float.
            ValueExpression::Column { column } => row.get(column).and_then(Value::display_string),
            other => other.evaluate(row),
        }?;
        (!text.trim().is_empty()).then_some(text)
    }
}

/// Where an entity's timestamp comes from.
///
/// `deny_unknown_fields`: a misspelled key would otherwise be ignored, leaving a timestamp that
/// silently drops every row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum TimestampSource {
    /// One value: a column, a constant, a template or a coalesce, with a format.
    Value(TimestampPart),
    /// Separate date and time parts, combined.
    ///
    /// Required by schemas that split them: `ERPNext`'s `posting_date` plus `posting_time`,
    /// and SAP `CDHDR`'s `UDATE` plus `UTIME`. A date column paired with a constant
    /// `"00:00:00"` states that the source has no time of day.
    Components {
        /// Where the date comes from, if anywhere.
        #[serde(default)]
        date: Option<TimestampPart>,
        /// Where the time comes from, if anywhere.
        #[serde(default)]
        time: Option<TimestampPart>,
    },
}

impl TimestampSource {
    /// Read `column` with auto-detection.
    #[must_use]
    pub fn column(column: impl Into<String>) -> Self {
        Self::Value(TimestampPart::column(column))
    }

    /// The same fixed instant for every row.
    #[must_use]
    pub fn constant(value: impl Into<String>) -> Self {
        Self::Value(TimestampPart {
            source: ValueExpression::Constant {
                value: value.into(),
            },
            format: None,
        })
    }

    /// Resolve against one row.
    pub(crate) fn parse(&self, row: &Row<'_>) -> Option<DateTime<FixedOffset>> {
        match self {
            TimestampSource::Value(part) => part.parse(row),
            TimestampSource::Components { date, time } => {
                let d = date.as_ref().and_then(|p| p.resolve(row));
                let t = time.as_ref().and_then(|p| p.resolve(row));
                parse_timestamp_components(
                    d.as_deref(),
                    date.as_ref().and_then(|p| p.format.as_ref()),
                    t.as_deref(),
                    time.as_ref().and_then(|p| p.format.as_ref()),
                )
            }
        }
    }

    /// Whether the row carried any timestamp text at all, to tell a wrong format apart from an
    /// absent value when [`TimestampSource::parse`] returns `None`. Only called on that failure
    /// path.
    pub(crate) fn has_input(&self, row: &Row<'_>) -> bool {
        match self {
            TimestampSource::Value(part) => part.resolve(row).is_some(),
            // A time of day with no date does not name an instant, so only the date side counts.
            TimestampSource::Components { date, .. } => {
                date.as_ref().is_some_and(|d| d.resolve(row).is_some())
            }
        }
    }

    /// Collect every column name this source reads into `out`.
    pub fn referenced_columns<'a>(&'a self, out: &mut HashSet<&'a str>) {
        match self {
            TimestampSource::Value(part) => part.source.referenced_columns(out),
            TimestampSource::Components { date, time } => {
                for part in [date, time].into_iter().flatten() {
                    part.source.referenced_columns(out);
                }
            }
        }
    }
}

/// Read a date, using `format` if given. The separator-less `%Y%m%d` that SAP's `UDATE` uses is
/// accepted only here, not in the general `Auto` cascade, so eight digits elsewhere stay a
/// number.
fn parse_date_part(value: &str, format: Option<&TimestampFormat>) -> Option<NaiveDate> {
    const DATE_COMPONENT_FORMATS: &[&str] = &[
        "%Y-%m-%d", "%Y%m%d", "%d/%m/%Y", "%d.%m.%Y", "%m/%d/%Y", "%Y/%m/%d",
    ];
    match format {
        Some(TimestampFormat::FormatString { format }) => {
            return NaiveDate::parse_from_str(value, format).ok();
        }
        Some(f @ (TimestampFormat::UnixSeconds | TimestampFormat::UnixMillis)) => {
            return parse_timestamp(value, f).map(|ts| ts.date_naive());
        }
        Some(TimestampFormat::Auto) | None => {}
    }
    let head = value
        .split_once('T')
        .or_else(|| value.split_once(' '))
        .map_or(value, |(head, _)| head);
    for candidate in [value, head] {
        for f in DATE_COMPONENT_FORMATS {
            if let Ok(d) = NaiveDate::parse_from_str(candidate, f) {
                return Some(d);
            }
        }
    }
    parse_timestamp(value, &TimestampFormat::Auto).map(|ts| ts.date_naive())
}

/// Read a time of day, using `format` if given. [`parse_timestamp`]'s `Auto` has no bare-time
/// spelling, so the cascade here is the only thing that reads one.
fn parse_time_part(value: &str, format: Option<&TimestampFormat>) -> Option<NaiveTime> {
    const TIME_COMPONENT_FORMATS: &[&str] = &["%H:%M:%S%.f", "%H:%M:%S", "%H:%M", "%H%M%S", "%H%M"];
    match format {
        Some(TimestampFormat::FormatString { format }) => {
            return NaiveTime::parse_from_str(value, format).ok();
        }
        Some(f @ (TimestampFormat::UnixSeconds | TimestampFormat::UnixMillis)) => {
            return parse_timestamp(value, f).map(|ts| ts.time());
        }
        Some(TimestampFormat::Auto) | None => {}
    }
    let tail = value
        .rsplit_once('T')
        .or_else(|| value.rsplit_once(' '))
        .map_or(value, |(_, tail)| tail);
    for candidate in [value, tail] {
        for f in TIME_COMPONENT_FORMATS {
            if let Ok(t) = NaiveTime::parse_from_str(candidate, f) {
                return Some(t);
            }
        }
    }
    parse_timestamp(value, &TimestampFormat::Auto).map(|ts| ts.time())
}

/// Combine a date string and a time string into one instant, with either side's format pinned or
/// auto-detected.
fn parse_timestamp_components(
    date_str: Option<&str>,
    date_format: Option<&TimestampFormat>,
    time_str: Option<&str>,
    time_format: Option<&TimestampFormat>,
) -> Option<DateTime<FixedOffset>> {
    let auto = &TimestampFormat::Auto;

    match (date_str, time_str) {
        (Some(d), Some(t)) => {
            // Read each side as what it claims to be first, because it is the only strategy that
            // cannot lose the time: the ones below fall back to parsing the date alone, turning
            // an unread time into a silent midnight.
            if let (Some(date), Some(time)) = (
                parse_date_part(d, date_format),
                parse_time_part(t, time_format),
            ) {
                return Some(DateTime::from_naive_utc_and_offset(
                    date.and_time(time),
                    FixedOffset::east_opt(0)?,
                ));
            }
            // Concatenating can only be read back with `Auto`, so it is off the table once either
            // side pinned a format: `Auto` tries `%d/%m/%Y` first, reading `%m/%d/%Y`'s
            // "01/02/2024" as February 1st rather than January 2nd.
            if !is_pinned(date_format) && !is_pinned(time_format) {
                if let Some(ts) = parse_timestamp(&format!("{d} {t}"), auto) {
                    return Some(ts);
                }
                // Each side may be a whole datetime, as in "2015-01-06T00:00:00" plus
                // "1970-01-01T15:02:03".
                let date_part = d
                    .split_once('T')
                    .or_else(|| d.split_once(' '))
                    .map_or(d, |(p, _)| p);
                let time_part = t
                    .rsplit_once('T')
                    .or_else(|| t.rsplit_once(' '))
                    .map_or(t, |(_, p)| p);
                if let Some(ts) = parse_timestamp(&format!("{date_part} {time_part}"), auto) {
                    return Some(ts);
                }
            }
            // Last, each value as a standalone whole timestamp, still under its own side's format.
            parse_timestamp(d, date_format.unwrap_or(auto))
                .or_else(|| parse_timestamp(t, time_format.unwrap_or(auto)))
        }
        // A date with no time means midnight.
        (Some(d), None) => parse_timestamp(d, date_format.unwrap_or(auto))
            .or_else(|| parse_date_part(d, date_format).map(midnight_utc)),
        // A time with no date names no instant unless the value is really a whole timestamp.
        (None, Some(t)) => parse_timestamp(t, time_format.unwrap_or(auto)),
        (None, None) => None,
    }
}

/// Whether the author fixed this side's format, as opposed to leaving it to be detected.
fn is_pinned(format: Option<&TimestampFormat>) -> bool {
    !matches!(format, None | Some(TimestampFormat::Auto))
}

fn midnight_utc(date: NaiveDate) -> DateTime<FixedOffset> {
    DateTime::from_naive_utc_and_offset(
        date.and_time(NaiveTime::MIN),
        FixedOffset::east_opt(0).expect("UTC is a valid offset"),
    )
}

/// Parse a timestamp, trying every format `Auto` covers when no format is pinned.
///
/// The cascade runs most specific first: RFC 3339 / ISO 8601, then RFC 2822, then naive
/// datetimes assumed UTC (most fractional digits to none), then date-only values (midnight
/// UTC), then `GMT`-style and UTC-suffix spellings, ending in `chrono`'s generic parser.
fn parse_timestamp(value: &str, format: &TimestampFormat) -> Option<DateTime<FixedOffset>> {
    let utc = FixedOffset::east_opt(0)?;
    match format {
        TimestampFormat::Auto => {
            if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
                return Some(dt);
            }
            // ISO 8601 with non-colon offset (e.g., +0000)
            if let Ok(dt) = DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%z") {
                return Some(dt);
            }
            if let Ok(dt) = DateTime::parse_from_rfc2822(value) {
                return Some(dt);
            }

            // Naive formats, assumed UTC, ordered by specificity.
            const NAIVE_FORMATS: &[&str] = &[
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%dT%H:%M:%S%.f",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y %H:%M",
                "%d.%m.%Y %H:%M:%S",
                "%d.%m.%Y %H:%M",
                "%m/%d/%Y %H:%M:%S",
                "%m/%d/%Y %H:%M",
                // UTC suffix
                "%Y-%m-%d %H:%M:%S UTC",
            ];
            for fmt in NAIVE_FORMATS {
                if let Ok(dt) = NaiveDateTime::parse_from_str(value, fmt) {
                    return Some(DateTime::from_naive_utc_and_offset(dt, utc));
                }
            }

            // Date-only formats, set to midnight UTC.
            const DATE_FORMATS: &[&str] = &["%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y", "%m/%d/%Y"];
            for fmt in DATE_FORMATS {
                if let Ok(d) = chrono::NaiveDate::parse_from_str(value, fmt) {
                    return Some(DateTime::from_naive_utc_and_offset(
                        d.and_hms_opt(0, 0, 0)?,
                        utc,
                    ));
                }
            }

            // GMT format: "Mon Apr 03 2023 12:08:18 GMT+0200 (...)"
            if let Ok((dt, _)) = DateTime::parse_and_remainder(value, "%Z %b %d %Y %T GMT%z") {
                return Some(dt);
            }

            // Last resort: chrono's generic DateTime parse.
            value.parse::<DateTime<FixedOffset>>().ok()
        }
        TimestampFormat::FormatString { format: fmt } => {
            // Try as NaiveDateTime first (format includes time components)
            if let Ok(dt) = NaiveDateTime::parse_from_str(value, fmt) {
                return Some(DateTime::from_naive_utc_and_offset(dt, utc));
            }
            // Fallback: date-only format strings (NaiveDateTime fails without hour)
            if let Ok(d) = chrono::NaiveDate::parse_from_str(value, fmt) {
                return Some(DateTime::from_naive_utc_and_offset(
                    d.and_hms_opt(0, 0, 0)?,
                    utc,
                ));
            }
            None
        }
        TimestampFormat::UnixSeconds => value
            .parse::<i64>()
            .ok()
            .and_then(|s| DateTime::from_timestamp(s, 0))
            .map(|dt| dt.with_timezone(&utc)),
        TimestampFormat::UnixMillis => value
            .parse::<i64>()
            .ok()
            .and_then(DateTime::from_timestamp_millis)
            .map(|dt| dt.with_timezone(&utc)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::extraction::row::with_row;
    use crate::core::event_data::object_centric::extraction::value::Value;

    #[test]
    fn template_substitutes_and_propagates_missing_values() {
        let e = ValueExpression::Template {
            template: "ORD-{id}-{region}".into(),
        };
        with_row(
            &[
                ("id", Value::Integer(7)),
                ("region", Value::Text("EU".into())),
            ],
            |row| {
                assert_eq!(e.evaluate(row).as_deref(), Some("ORD-7-EU"));
            },
        );
        with_row(
            &[("id", Value::Integer(7)), ("region", Value::Null)],
            |row| {
                assert_eq!(e.evaluate(row), None);
            },
        );
    }

    #[test]
    fn a_substituted_value_containing_braces_is_not_treated_as_a_placeholder() {
        // Regression: the old implementation inspected the output for braces, so a JSON
        // value (ERPNext tabVersion.data) made the whole expression None and dropped the row.
        let e = ValueExpression::Template {
            template: "v-{payload}".into(),
        };
        with_row(&[("payload", Value::Text("{\"a\":1}".into()))], |row| {
            assert_eq!(e.evaluate(row).as_deref(), Some("v-{\"a\":1}"));
        });
    }

    #[test]
    fn an_unterminated_placeholder_yields_no_value() {
        // Regression in the other direction: "a{b" used to pass the old brace check and
        // become the literal identity "a{b".
        let e = ValueExpression::Template {
            template: "a{b".into(),
        };
        with_row(&[("b", Value::Text("x".into()))], |row| {
            assert_eq!(e.evaluate(row), None)
        });
    }

    #[test]
    fn coalesce_takes_the_first_value_that_renders() {
        // Odoo mail_tracking_value: five typed columns, one populated.
        let e = ValueExpression::Coalesce {
            parts: vec![
                ValueExpression::Column {
                    column: "old_value_integer".into(),
                },
                ValueExpression::Column {
                    column: "old_value_char".into(),
                },
            ],
        };
        with_row(
            &[
                ("old_value_integer", Value::Null),
                ("old_value_char", Value::Text("draft".into())),
            ],
            |row| assert_eq!(e.evaluate(row).as_deref(), Some("draft")),
        );
        with_row(
            &[
                ("old_value_integer", Value::Integer(3)),
                ("old_value_char", Value::Null),
            ],
            |row| {
                assert_eq!(e.evaluate(row).as_deref(), Some("3"));
            },
        );
        with_row(
            &[
                ("old_value_integer", Value::Null),
                ("old_value_char", Value::Null),
            ],
            |row| {
                assert_eq!(e.evaluate(row), None);
            },
        );
    }

    #[test]
    fn an_empty_column_name_is_not_swallowed() {
        // A dropped guard here used to hide `{"type":"column","column":""}`, which drops every
        // row at evaluation time, from validation.
        let e = ValueExpression::Column {
            column: String::new(),
        };
        let mut cols = HashSet::new();
        e.referenced_columns(&mut cols);
        assert_eq!(cols, HashSet::from([""]));
    }

    #[test]
    fn delimiter_split_trims_and_drops_empties() {
        let s = SplitSpec {
            kind: SplitKind::Delimiter {
                delimiter: ",".into(),
            },
            trim: true,
        };
        assert_eq!(s.prepare().unwrap().split("a, b ,,c"), vec!["a", "b", "c"]);
    }

    #[test]
    fn prepare_reports_an_invalid_split_regex_instead_of_panicking() {
        let s = SplitSpec {
            kind: SplitKind::Regex {
                pattern: "(".into(),
            },
            trim: true,
        };
        assert!(s.prepare().is_err());
    }

    #[test]
    fn regex_split_uses_capture_groups_when_present() {
        let s = SplitSpec {
            kind: SplitKind::Regex {
                pattern: "([a-z])=([0-9]+)".into(),
            },
            trim: true,
        };
        assert_eq!(
            s.prepare().unwrap().split("x=1;y=22"),
            vec!["x", "1", "y", "22"]
        );
        let whole = SplitSpec {
            kind: SplitKind::Regex {
                pattern: "[a-z][0-9]+".into(),
            },
            trim: true,
        };
        assert_eq!(whole.prepare().unwrap().split("a1,b22"), vec!["a1", "b22"]);
    }

    fn components(date: TimestampPart, time: TimestampPart) -> TimestampSource {
        TimestampSource::Components {
            date: Some(date),
            time: Some(time),
        }
    }

    fn constant_part(value: &str) -> TimestampPart {
        TimestampPart {
            source: ValueExpression::Constant {
                value: value.into(),
            },
            format: None,
        }
    }

    fn formatted_column(column: &str, format: &str) -> TimestampPart {
        TimestampPart {
            source: ValueExpression::Column {
                column: column.into(),
            },
            format: Some(TimestampFormat::FormatString {
                format: format.into(),
            }),
        }
    }

    /// A constant time cannot invent an instant for a row whose date is `NULL`, and reporting
    /// those rows as unparseable would point at a format string that is already right.
    #[test]
    fn a_null_date_is_missing_not_unparseable() {
        let ts = components(
            formatted_column("start_date", "%Y-%m-%d"),
            constant_part("00:00:00"),
        );
        with_row(&[("start_date", Value::Text("2018-02-06".into()))], |row| {
            assert_eq!(
                ts.parse(row).expect("a real date parses").to_rfc3339(),
                "2018-02-06T00:00:00+00:00"
            );
            assert!(ts.has_input(row));
        });
        with_row(&[("start_date", Value::Null)], |row| {
            assert!(ts.parse(row).is_none());
            assert!(
                !ts.has_input(row),
                "a NULL date must report as missing, not as a format failure"
            );
        });
    }

    #[test]
    fn a_blank_part_is_absent_so_a_date_alone_is_midnight() {
        let ts = components(TimestampPart::column("d"), constant_part("   "));
        with_row(&[("d", Value::Text("2024-03-04".into()))], |row| {
            assert_eq!(
                ts.parse(row).expect("date alone is midnight").to_rfc3339(),
                "2024-03-04T00:00:00+00:00"
            );
        });
    }

    #[test]
    fn a_present_but_malformed_value_is_unparseable_not_missing() {
        let ts = TimestampSource::column("ts");
        with_row(&[("ts", Value::Text("not a date".into()))], |row| {
            assert!(ts.parse(row).is_none());
            assert!(
                ts.has_input(row),
                "text was there; the format is the problem"
            );
        });
    }

    #[test]
    fn components_timestamp_combines_separate_date_and_time_columns() {
        // ERPNext posting_date + posting_time.
        let ts = components(TimestampPart::column("d"), TimestampPart::column("t"));
        with_row(
            &[
                ("d", Value::Text("2015-01-06".into())),
                ("t", Value::Text("15:02:03".into())),
            ],
            |row| {
                let got = ts.parse(row).expect("should parse");
                assert_eq!(got.to_rfc3339(), "2015-01-06T15:02:03+00:00");
            },
        );
    }

    /// Compact spellings (SAP `CDHDR`'s `UDATE` + `UTIME`) that the concatenation strategies
    /// cannot read must not degrade to a valid-looking midnight with the time dropped.
    #[test]
    fn separatorless_date_and_time_parts_are_read_not_dropped() {
        let ts = components(TimestampPart::column("d"), TimestampPart::column("t"));
        for (d, t) in [("2024-01-02", "150203"), ("20240102", "150203")] {
            with_row(
                &[("d", Value::Text(d.into())), ("t", Value::Text(t.into()))],
                |row| {
                    assert_eq!(
                        ts.parse(row).expect("should parse").to_rfc3339(),
                        "2024-01-02T15:02:03+00:00"
                    );
                },
            );
        }
    }

    #[test]
    fn each_side_can_be_a_constant_independently_of_the_other() {
        let constant_time = components(TimestampPart::column("d"), constant_part("00:00:00"));
        with_row(&[("d", Value::Text("2024-03-04".into()))], |row| {
            assert_eq!(
                constant_time.parse(row).expect("should parse").to_rfc3339(),
                "2024-03-04T00:00:00+00:00"
            );
        });

        let constant_date = components(constant_part("2024-03-04"), TimestampPart::column("t"));
        with_row(&[("t", Value::Text("07:08:09".into()))], |row| {
            assert_eq!(
                constant_date.parse(row).expect("should parse").to_rfc3339(),
                "2024-03-04T07:08:09+00:00"
            );
        });
    }

    #[test]
    fn a_per_side_format_pins_an_ambiguous_spelling() {
        let american = components(
            formatted_column("d", "%m/%d/%Y"),
            TimestampPart::column("t"),
        );
        with_row(
            &[
                ("d", Value::Text("01/02/2024".into())),
                ("t", Value::Text("00:00:00".into())),
            ],
            |row| {
                // January 2nd; Auto reads this spelling as February 1st.
                assert_eq!(
                    american.parse(row).expect("should parse").to_rfc3339(),
                    "2024-01-02T00:00:00+00:00"
                );
            },
        );
    }

    /// A time cell the pinned time format cannot read must not send the date side back to
    /// `Auto`, whose cascade reads "01/02/2024" as February 1st where `%m/%d/%Y` says January 2nd.
    #[test]
    fn an_unreadable_time_never_re_reads_a_pinned_date_with_auto() {
        let ts = components(
            formatted_column("d", "%m/%d/%Y"),
            formatted_column("t", "%H:%M:%S"),
        );
        with_row(
            &[
                ("d", Value::Text("01/02/2024".into())),
                ("t", Value::Text("not a time".into())),
            ],
            |row| {
                assert_eq!(
                    ts.parse(row).expect("the date still parses").to_rfc3339(),
                    "2024-01-02T00:00:00+00:00"
                );
            },
        );
    }

    #[test]
    fn the_json_shape_is_a_source_and_a_format_per_side() {
        let parsed: TimestampSource = serde_json::from_str(
            r#"{"type":"components",
                "date":{"source":{"type":"column","column":"UDATE"}},
                "time":{"source":{"type":"column","column":"UTIME"}}}"#,
        )
        .expect("current shape parses");
        assert_eq!(
            parsed,
            components(
                TimestampPart::column("UDATE"),
                TimestampPart::column("UTIME")
            )
        );

        let ts = components(formatted_column("d", "%Y%m%d"), constant_part("00:00:00"));
        let json = serde_json::to_string(&ts).expect("serialises");
        assert_eq!(
            serde_json::from_str::<TimestampSource>(&json).expect("round trips"),
            ts
        );
    }

    #[test]
    fn the_retired_spellings_no_longer_deserialise() {
        // Silently ignoring `date_column` would leave both sides unset and drop every row.
        let legacy = serde_json::from_str::<TimestampSource>(
            r#"{"type":"components","date_column":"UDATE","time_column":"UTIME"}"#,
        );
        assert!(legacy.is_err(), "the old key names must not be accepted");

        let old_column =
            serde_json::from_str::<TimestampSource>(r#"{"type":"column","column":"at"}"#);
        assert!(old_column.is_err(), "`column` folded into `value`");
    }

    #[test]
    fn a_timestamp_can_be_a_template_or_a_coalesce() {
        let templated = TimestampSource::Value(TimestampPart {
            source: ValueExpression::Template {
                template: "{d}T{t}Z".into(),
            },
            format: None,
        });
        with_row(
            &[
                ("d", Value::Text("2024-01-02".into())),
                ("t", Value::Text("15:02:03".into())),
            ],
            |row| {
                assert_eq!(
                    templated.parse(row).expect("template parses").to_rfc3339(),
                    "2024-01-02T15:02:03+00:00"
                );
            },
        );

        let coalesced = TimestampSource::Value(TimestampPart {
            source: ValueExpression::Coalesce {
                parts: vec![
                    ValueExpression::Column {
                        column: "start_date".into(),
                    },
                    ValueExpression::Column {
                        column: "retrieved_at".into(),
                    },
                ],
            },
            format: None,
        });
        with_row(
            &[
                ("start_date", Value::Null),
                ("retrieved_at", Value::Text("2020-05-06".into())),
            ],
            |row| {
                assert_eq!(
                    coalesced.parse(row).expect("falls back").to_rfc3339(),
                    "2020-05-06T00:00:00+00:00"
                );
            },
        );
    }

    /// `referenced_columns` feeds the column list each scan requests, so an expression-valued
    /// part has to report through to its own columns or the scan will not fetch them.
    #[test]
    fn referenced_columns_sees_through_both_parts() {
        let ts = components(
            TimestampPart {
                source: ValueExpression::Coalesce {
                    parts: vec![
                        ValueExpression::Column {
                            column: "posting_date".into(),
                        },
                        ValueExpression::Column {
                            column: "creation_date".into(),
                        },
                    ],
                },
                format: None,
            },
            constant_part("00:00:00"),
        );
        let mut cols = HashSet::new();
        ts.referenced_columns(&mut cols);
        let mut got: Vec<&str> = cols.into_iter().collect();
        got.sort_unstable();
        assert_eq!(got, vec!["creation_date", "posting_date"]);
    }

    #[test]
    fn a_unix_epoch_column_reported_as_float_still_parses() {
        // The non-Timestamp fallback must not read through `canonical_string`, which is `None`
        // for `Float` and would silently drop every row.
        let ts = TimestampSource::Value(TimestampPart {
            source: ValueExpression::Column { column: "t".into() },
            format: Some(TimestampFormat::UnixSeconds),
        });
        with_row(&[("t", Value::Float(1_580_698_806.0))], |row| {
            assert!(ts.parse(row).is_some());
        });
    }

    #[test]
    fn a_typed_timestamp_column_is_used_without_a_string_roundtrip() {
        let parsed = chrono::DateTime::parse_from_rfc3339("2020-02-03T04:05:06+02:00").unwrap();
        let ts = TimestampSource::column("t");
        with_row(&[("t", Value::Timestamp(parsed))], |row| {
            assert_eq!(ts.parse(row), Some(parsed));
        });
    }
    /// Not a correctness test: a measurement of what `Auto` costs per row against a pinned format,
    /// for the common `SQLite` spelling. Ignored by default. Run with
    /// `cargo test -p process_mining --features extraction-blueprint auto_timestamp_cost -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn auto_timestamp_cost() {
        const N: usize = 1_000_000;
        let sqlite_style = "2023-04-03 12:08:18";
        let rfc3339 = "2023-04-03T12:08:18+00:00";
        let pinned = TimestampFormat::FormatString {
            format: "%Y-%m-%d %H:%M:%S".to_string(),
        };

        for (label, value, format) in [
            (
                "auto / sqlite 'Y-m-d H:M:S'",
                sqlite_style,
                &TimestampFormat::Auto,
            ),
            ("auto / rfc3339", rfc3339, &TimestampFormat::Auto),
            ("pinned / sqlite", sqlite_style, &pinned),
        ] {
            let start = std::time::Instant::now();
            let mut ok = 0usize;
            for _ in 0..N {
                if parse_timestamp(std::hint::black_box(value), format).is_some() {
                    ok += 1;
                }
            }
            let elapsed = start.elapsed();
            assert_eq!(ok, N, "{label} failed to parse");
            println!(
                "{label:32} {:>8.0} ns/row  {:>7.2} s per 10M rows",
                elapsed.as_nanos() as f64 / N as f64,
                elapsed.as_secs_f64() * 10.0,
            );
        }
    }
}
