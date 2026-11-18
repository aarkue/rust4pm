//! IO implementations for `EventLog`

use std::io::{Read, Write};

use crate::core::event_data::case_centric::xes::export_xes::{write_xes, XesHeader};
use crate::core::event_data::case_centric::xes::import_xes::{
    import_xes, XESImportOptions, XESParseError,
};
use crate::core::event_data::case_centric::xes::stream_xes::xes_reader;
use crate::core::event_data::case_centric::EventLog;
use crate::core::io::{Exportable, ExtensionWithMime, Importable};
use crate::utils::control::{Cancelled, Control};

/// Error type for `EventLog` IO operations
#[derive(Debug)]
pub enum EventLogIOError {
    /// IO Error
    Io(std::io::Error),
    /// XES Parsing Error
    Xes(XESParseError),
    /// JSON Parsing Error
    Json(serde_json::Error),
    /// XML Parsing Error
    Xml(quick_xml::Error),
    /// Unsupported Format
    UnsupportedFormat(String),
    /// The import's [`Control`] was cancelled
    Cancelled,
}

impl std::fmt::Display for EventLogIOError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventLogIOError::Io(e) => write!(f, "IO Error: {}", e),
            EventLogIOError::Xes(e) => write!(f, "XES Error: {}", e),
            EventLogIOError::Json(e) => write!(f, "JSON Error: {}", e),
            EventLogIOError::Xml(e) => write!(f, "XML Error: {}", e),
            EventLogIOError::UnsupportedFormat(s) => write!(f, "Unsupported Format: {}", s),
            EventLogIOError::Cancelled => write!(f, "{Cancelled}"),
        }
    }
}

impl std::error::Error for EventLogIOError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EventLogIOError::Io(e) => Some(e),
            EventLogIOError::Xes(e) => Some(e),
            EventLogIOError::Json(e) => Some(e),
            EventLogIOError::Xml(e) => Some(e),
            EventLogIOError::UnsupportedFormat(_) | EventLogIOError::Cancelled => None,
        }
    }
}

impl From<std::io::Error> for EventLogIOError {
    fn from(e: std::io::Error) -> Self {
        EventLogIOError::Io(e)
    }
}

impl From<Cancelled> for EventLogIOError {
    fn from(_: Cancelled) -> Self {
        EventLogIOError::Cancelled
    }
}

impl From<XESParseError> for EventLogIOError {
    fn from(e: XESParseError) -> Self {
        EventLogIOError::Xes(e)
    }
}

impl From<serde_json::Error> for EventLogIOError {
    fn from(e: serde_json::Error) -> Self {
        EventLogIOError::Json(e)
    }
}

impl From<quick_xml::Error> for EventLogIOError {
    fn from(e: quick_xml::Error) -> Self {
        match e {
            quick_xml::Error::Io(e) => {
                EventLogIOError::Io(std::io::Error::new(e.kind(), e.to_string()))
            }
            e => EventLogIOError::Xml(e),
        }
    }
}

/// A format of [`EventLog`] IO, named as in [`Importable::known_import_formats`]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EventLogFormat {
    Json,
    Xes { gz: bool },
}

impl EventLogFormat {
    /// The format named `format` (`xes`, `xes.gz` or `json`), ignoring case and a leading `.`
    pub(crate) fn parse(format: &str) -> Result<Self, EventLogIOError> {
        let lower = format.to_ascii_lowercase();
        match lower.strip_prefix('.').unwrap_or(&lower) {
            "json" => Ok(Self::Json),
            "xes" => Ok(Self::Xes { gz: false }),
            "xes.gz" => Ok(Self::Xes { gz: true }),
            _ => Err(EventLogIOError::UnsupportedFormat(format.to_string())),
        }
    }

    /// Whether the XES format `format` is gzipped, failing for any other format
    pub(crate) fn parse_xes_gz(format: &str) -> Result<bool, EventLogIOError> {
        match Self::parse(format)? {
            Self::Xes { gz } => Ok(gz),
            Self::Json => Err(EventLogIOError::UnsupportedFormat(format.to_string())),
        }
    }
}

impl Importable for EventLog {
    type Error = EventLogIOError;
    type ImportOptions = XESImportOptions;

    fn import_from_reader_with<R: Read>(
        reader: R,
        format: &str,
        options: Self::ImportOptions,
        ctl: &Control<'_>,
    ) -> Result<Self, Self::Error> {
        match EventLogFormat::parse(format)? {
            EventLogFormat::Json => Ok(serde_json::from_reader(std::io::BufReader::new(reader))?),
            EventLogFormat::Xes { gz } => import_xes(xes_reader(reader, gz), options, ctl),
        }
    }

    fn known_import_formats() -> Vec<ExtensionWithMime> {
        vec![
            ExtensionWithMime::new("xes", "application/xml"),
            ExtensionWithMime::new("xes.gz", "application/gzip"),
            ExtensionWithMime::new("json", "application/json"),
        ]
    }
}

impl Exportable for EventLog {
    type Error = EventLogIOError;
    type ExportOptions = ();

    fn export_to_writer_with_options<W: Write>(
        &self,
        mut writer: W,
        format: &str,
        _: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        match EventLogFormat::parse(format)? {
            EventLogFormat::Json => {
                serde_json::to_writer(&mut writer, self)?;
                // A writer given by value would otherwise drop a failing final write with it
                writer.flush()?;
                Ok(())
            }
            EventLogFormat::Xes { gz } => write_xes(writer, gz, XesHeader::of(self), &self.traces),
        }
    }

    fn known_export_formats() -> Vec<ExtensionWithMime> {
        vec![
            ExtensionWithMime::new("xes", "application/xml"),
            ExtensionWithMime::new("xes.gz", "application/gzip"),
            ExtensionWithMime::new("json", "application/json"),
        ]
    }
}
