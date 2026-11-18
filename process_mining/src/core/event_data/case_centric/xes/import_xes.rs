use std::collections::HashSet;
use std::io::BufRead;

use serde::{Deserialize, Serialize};

use crate::core::event_data::case_centric::event_log_struct::{EventLog, Trace};
use crate::core::event_data::case_centric::io::EventLogIOError;
use crate::utils::control::Control;

/// Error in the content of XES data. IO and XML errors are other [`EventLogIOError`] variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum XESParseError {
    /// An Attribute was encountered outside an open `<log>` tag
    AttributeOutsideLog,
    /// There is no top-level `<log>`
    NoTopLevelLog,
    /// Parsing error: Expected to have a previously constructed event available
    MissingLastEvent,
    /// Parsing error: Expected to have a previously constructed trace available
    MissingLastTrace,
    /// Parsing error: Expected to be in a different parsing mode than the current state suggests
    InvalidMode,
    /// Invalid value of XML attribute with key (with key included)
    InvalidKeyValue(&'static str),
    /// Parsing Transformation Error: Expected that `XESOuterLogData` would be emitted first
    ExpectedLogData,
    /// Parsing Transformation Error: Expected that Trace would be emitted now
    ExpectedTraceData,
    /// Attributes are nested deeper than [`MAX_ATTRIBUTE_DEPTH`](super::stream_xes::MAX_ATTRIBUTE_DEPTH)
    AttributeNestingTooDeep,
}

impl std::fmt::Display for XESParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to parse XES: {self:?}")
    }
}

impl std::error::Error for XESParseError {}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
///
///
/// Options for XES Import
///
/// See also [`build_ignore_attributes`] for easy construction of attributes set to not ignore
pub struct XESImportOptions {
    /// If Some: Ignore all top-level log attributes, except attributes with keys in the provided allowlist
    pub ignore_log_attributes_except: Option<HashSet<String>>,
    /// If Some: Ignore all trace attributes, except attributes with keys in the provided allowlist
    ///
    /// Does not effect global trace attributes
    pub ignore_trace_attributes_except: Option<HashSet<String>>,
    /// If Some: Ignore all event attributes except, attributes with keys in the provided allowlist
    ///
    /// Does not effect global event attributes
    pub ignore_event_attributes_except: Option<HashSet<String>>,
    /// Optional date format to try first when parsing dates (with time zone, else as UTC)
    ///
    /// See <https://docs.rs/chrono/latest/chrono/format/strftime/index.html> for all available
    /// specifiers. Dates not matching it are parsed as by
    /// [`parse_timestamp`](crate::core::event_data::timestamp_utils::parse_timestamp).
    pub date_format: Option<String>,
    /// Sort events via timestamp key directly when parsing:
    /// * If None: No sorting (i.e., events of traces are included in order of occurence in event log)
    /// * If Some(key):
    ///   * Sort events via the timestamp provided by key before emitting the trace
    ///   * If no value is present or it is invalid, the global default event attribute value with the provided key will be used (if it exists)
    ///   * if no valid timestamp is available from the event or the global default, it will be sorted before all other events (in stable ordering)
    pub sort_events_with_timestamp_key: Option<String>,
}
///
/// Construct a `HashSet<Vec<u8>>` from a _collection_ of String, &str, ...
///
/// Example usage: `XESImportOptions::build_ignore_attributes(vec!["concept:name"])`
///
pub fn build_ignore_attributes<I, S: AsRef<str>>(keys: I) -> HashSet<String>
where
    I: IntoIterator<Item = S>,
{
    keys.into_iter().map(|s| s.as_ref().to_string()).collect()
}

/// Parse XES from `reader`, warning through `ctl`
pub(crate) fn import_xes<'a>(
    reader: Box<dyn BufRead + 'a>,
    options: XESImportOptions,
    ctl: &'a Control<'a>,
) -> Result<EventLog, EventLogIOError> {
    let (mut trace_stream, log_data) =
        super::stream_xes::XESParsingTraceStream::try_new(reader, options, ctl)?;
    let traces: Vec<Trace> = trace_stream.collect();
    if let Some(e) = trace_stream.error {
        return Err(e);
    }
    Ok(EventLog::from_traces_and_log_data(traces, log_data))
}
