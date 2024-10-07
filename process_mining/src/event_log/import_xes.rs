use std::collections::HashSet;

use std::fs::File;
use std::io::{BufRead, BufReader};

use crate::event_log::event_log_struct::{EventLog, Trace};

use flate2::bufread::GzDecoder;

use quick_xml::Error as QuickXMLError;
use quick_xml::Reader;
use serde::{Deserialize, Serialize};

///
/// Error encountered while parsing XES
///
#[derive(Debug, Clone)]
pub enum XESParseError {
    /// An Attribute was encountered outside an open `<log>` tag
    AttributeOutsideLog,
    /// There is not top-level `<log>`
    NoTopLevelLog,
    /// Parsing error: Expected to have a previously constructed event available
    MissingLastEvent,
    /// Parsing error: Expected to have a previously constructed traec available
    MissingLastTrace,
    /// Parsing error: Expected to have a be in a different parsing mode than the current state suggests
    InvalidMode,
    /// IO errror
    IOError(std::rc::Rc<std::io::Error>),
    /// XML error (e.g., incorrect XML format )
    XMLParsingError(QuickXMLError),
    /// Missing key on XML element (with expected key included)
    MissingKey(&'static str),
    /// Invalid value of XML attribute with key (with key included)
    InvalidKeyValue(&'static str),
    /// Parsing Transformation Error: Expected that `XESOuterLogData` would be emitted first
    ExpectedLogData,
    /// Parsing Transformation Error: Expected that Trace would be emitted now
    ExpectedTraceData,
}

impl std::fmt::Display for XESParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to parse XES: {:?}", self)
    }
}

impl std::error::Error for XESParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            XESParseError::IOError(e) => Some(e.as_ref()),
            XESParseError::XMLParsingError(e) => Some(e),
            _ => None,
        }
    }

    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}

impl From<std::io::Error> for XESParseError {
    fn from(e: std::io::Error) -> Self {
        Self::IOError(std::rc::Rc::new(e))
    }
}

impl From<QuickXMLError> for XESParseError {
    fn from(e: QuickXMLError) -> Self {
        Self::XMLParsingError(e)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
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
    /// Optional date format to use when parsing `DateTimes` (first trying [`chrono::DateTime`] then falling back to [`chrono::NaiveDateTime`] with UTC timezone).
    ///
    /// See <https://docs.rs/chrono/latest/chrono/format/strftime/index.html> for all available Specifiers.
    ///
    /// Will fall back to default formats (e.g., rfc3339) if parsing fails using passed `date_format`
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

/// Parse XES from the given reader
pub fn import_xes<T>(reader: T, options: XESImportOptions) -> Result<EventLog, XESParseError>
where
    T: BufRead,
{
    let (mut trace_stream, log_data) = super::stream_xes::XESParsingTraceStream::try_new(
        Box::new(Reader::from_reader(Box::new(reader))),
        options,
    )?;

    let traces: Vec<Trace> = trace_stream.collect();

    if let Some(e) = trace_stream.error {
        return Err(e);
    }

    Ok(EventLog {
        attributes: log_data.log_attributes,
        traces,
        extensions: Some(log_data.extensions),
        classifiers: Some(log_data.classifiers),
        // Only put global_trace_attrs / global_event_attrs to log data if it is not empty
        global_trace_attrs: if log_data.global_trace_attrs.is_empty() {
            None
        } else {
            Some(log_data.global_trace_attrs)
        },
        global_event_attrs: if log_data.global_event_attrs.is_empty() {
            None
        } else {
            Some(log_data.global_event_attrs)
        },
    })
}

///
/// Import a XES [`EventLog`] from a file path
///
pub fn import_xes_file(path: &str, options: XESImportOptions) -> Result<EventLog, XESParseError> {
    if path.ends_with(".gz") {
        let file = File::open(path)?;
        let dec: GzDecoder<BufReader<&File>> = GzDecoder::new(BufReader::new(&file));
        let reader = BufReader::new(dec);
        import_xes(reader, options)
    } else {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        import_xes(reader, options)
    }
}

///
/// Import a XES [`EventLog`] directly from a string
///
pub fn import_xes_str(xes_str: &str, options: XESImportOptions) -> Result<EventLog, XESParseError> {
    let reader = BufReader::new(xes_str.as_bytes());
    import_xes(reader, options)
}

///
/// Import a XES [`EventLog`] from a byte slice (&\[u8\])
///
/// * `is_compressed_gz`: Parse the passed `xes_data` as a compressed .gz archive
///
pub fn import_xes_slice(
    xes_data: &[u8],
    is_compressed_gz: bool,
    options: XESImportOptions,
) -> Result<EventLog, XESParseError> {
    // let buf_reader = BufReader::new(reader);
    if is_compressed_gz {
        let gz: GzDecoder<&[u8]> = GzDecoder::new(xes_data);
        let reader = BufReader::new(gz);
        return import_xes(reader, options);
    }
    import_xes(BufReader::new(xes_data), options)
}
