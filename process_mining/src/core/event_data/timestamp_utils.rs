//! Shared timestamp parsing utilities for event data importers

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime};

/// Parse a timestamp string to `DateTime<FixedOffset>`, trying multiple formats.
///
/// This function is used by both XML and CSV importers to ensure consistent
/// timestamp parsing across all event data formats.
///
/// # Arguments
/// * `time` - The timestamp string to parse
/// * `custom_format` - Optional custom date format to try first
///
/// # Returns
/// * `Ok(DateTime<FixedOffset>)` - The parsed timestamp
/// * `Err(&str)` - Error message if parsing fails
///
/// # Supported Formats (in order of precedence)
/// Leading and trailing whitespace is ignored.
///
/// 1. Custom format (if provided) - tries both with timezone and as naive (assumes UTC)
/// 2. RFC3339: `2023-10-06T09:30:21+00:00`
/// 3. ISO 8601 with any offset (`+0000`, `+00`), optionally fractional seconds and a space
///    separator: `2023-10-06T09:30:21.123+0000`, `2023-10-06 09:30:21+02`
/// 4. RFC2822: `Fri, 06 Oct 2023 09:30:21 +0000`
/// 5. Naive datetime with fractional seconds: `2023-10-06 09:30:21.890421` (assumes UTC)
/// 6. Naive ISO 8601 with fractional: `2023-10-06T09:30:21.348555` (assumes UTC)
/// 7. Naive ISO 8601: `2023-10-06T09:30:21` (assumes UTC)
/// 8. Naive with UTC suffix: `2023-10-06 09:30:21 UTC`
/// 9. GMT format: `Mon Apr 03 2023 12:08:18 GMT+0200 (...)` (timezone part parsed)
/// 10. Without seconds: `2023-10-06T09:30`, `2023-10-06T09:30+02:00` (naive assumes UTC)
/// 11. Date only: `2023-10-06` (midnight UTC)
pub fn parse_timestamp<'a>(
    time: &'a str,
    custom_format: Option<&'a str>,
) -> Result<DateTime<FixedOffset>, &'a str> {
    let time = time.trim();
    // Try custom date format first if provided
    if let Some(date_format) = custom_format {
        // Try as timezone-aware first
        if let Ok(dt) = DateTime::parse_from_str(time, date_format) {
            return Ok(dt);
        }
        // Try as naive datetime (assuming UTC)
        if let Ok(dt) = NaiveDateTime::parse_from_str(time, date_format) {
            return Ok(dt.and_utc().into());
        }
    }

    // Try RFC3339
    if let Ok(dt) = DateTime::parse_from_rfc3339(time) {
        return Ok(dt);
    }

    // `%#z` also takes `+0000` and `+00`; `%.f` is optional
    if let Ok(dt) = DateTime::parse_from_str(time, "%FT%T%.f%#z") {
        return Ok(dt);
    }
    if let Ok(dt) = DateTime::parse_from_str(time, "%F %T%.f%#z") {
        return Ok(dt);
    }

    // Try RFC2822
    if let Ok(dt) = DateTime::parse_from_rfc2822(time) {
        return Ok(dt);
    }

    // Some logs have this date: "2023-10-06 09:30:21.890421"
    // Assuming that this is UTC
    if let Ok(dt) = NaiveDateTime::parse_from_str(time, "%F %T%.f") {
        return Ok(dt.and_utc().into());
    }

    // Also handle "2024-10-02T07:55:15.348555" as well as "2022-01-09T15:00:00"
    // Assuming UTC time zone
    if let Ok(dt) = NaiveDateTime::parse_from_str(time, "%FT%T%.f") {
        return Ok(dt.and_utc().into());
    }

    // ISO 8601 without fractional seconds
    if let Ok(dt) = NaiveDateTime::parse_from_str(time, "%FT%T") {
        return Ok(dt.and_utc().into());
    }

    // Datetime with UTC suffix
    if let Ok(dt) = NaiveDateTime::parse_from_str(time, "%F %T UTC") {
        return Ok(dt.and_utc().into());
    }

    // Some logs have this date: "Mon Apr 03 2023 12:08:18 GMT+0200 (Mitteleuropäische Sommerzeit)"
    // Below ignores the first "Mon " part (%Z) parses the rest (only if "GMT") and then parses the timezone (+0200)
    // The rest of the input is ignored
    if let Ok((dt, _)) = DateTime::parse_and_remainder(time, "%Z %b %d %Y %T GMT%z") {
        return Ok(dt);
    }

    if let Ok(dt) = DateTime::parse_from_str(time, "%FT%R%#z") {
        return Ok(dt);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(time, "%FT%R") {
        return Ok(dt.and_utc().into());
    }

    if let Ok(d) = NaiveDate::parse_from_str(time, "%F") {
        return Ok(d.and_time(chrono::NaiveTime::MIN).and_utc().into());
    }

    Err("Unexpected timestamp format")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rfc3339() {
        let result = parse_timestamp("2023-10-06T09:30:21+00:00", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_naive_datetime() {
        let result = parse_timestamp("2023-10-06 09:30:21.890421", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_naive_iso() {
        let result = parse_timestamp("2023-10-06T09:30:21", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_custom_format() {
        let result = parse_timestamp("06/10/2023 09:30:21", Some("%d/%m/%Y %H:%M:%S"));
        assert!(result.is_ok());
    }

    fn utc(s: &str) -> DateTime<FixedOffset> {
        DateTime::parse_from_rfc3339(s).unwrap()
    }

    #[test]
    fn test_lenient_forms() {
        for (input, expected) in [
            (
                "2023-10-06T09:30:21.123+0000",
                "2023-10-06T09:30:21.123+00:00",
            ),
            ("2023-10-06T09:30:21+0200", "2023-10-06T09:30:21+02:00"),
            ("2023-10-06 09:30:21+0200", "2023-10-06T09:30:21+02:00"),
            ("2023-10-06 09:30:21.5-0130", "2023-10-06T09:30:21.5-01:30"),
            ("2023-10-06T09:30:21+02", "2023-10-06T09:30:21+02:00"),
            ("2023-10-06 09:30:21+02", "2023-10-06T09:30:21+02:00"),
            ("  2023-10-06T09:30:21+00:00\n", "2023-10-06T09:30:21+00:00"),
            (
                " 2023-10-06 09:30:21.890421 ",
                "2023-10-06T09:30:21.890421Z",
            ),
            ("2023-10-06T09:30", "2023-10-06T09:30:00Z"),
            ("2023-10-06T09:30+02:00", "2023-10-06T09:30:00+02:00"),
            ("2023-10-06", "2023-10-06T00:00:00Z"),
            ("\t2023-10-06 ", "2023-10-06T00:00:00Z"),
        ] {
            let parsed = parse_timestamp(input, None);
            assert_eq!(parsed, Ok(utc(expected)), "{input:?}");
            assert_eq!(
                parsed.unwrap().offset(),
                utc(expected).offset(),
                "{input:?}"
            );
        }
    }

    #[test]
    fn test_invalid_forms_fail() {
        for input in ["", "2023-10-06T", "2023-13-06", "09:30:21", "not a date"] {
            assert!(parse_timestamp(input, None).is_err(), "{input:?}");
        }
    }

    #[test]
    fn test_gmt_format() {
        let result = parse_timestamp(
            "Mon Apr 03 2023 12:08:18 GMT+0200 (Mitteleuropäische Sommerzeit)",
            None,
        );
        assert!(result.is_ok());
    }
}
