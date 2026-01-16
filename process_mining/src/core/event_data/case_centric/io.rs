//! IO implementations for `EventLog`

use std::io::{BufReader, Read, Write};

use crate::core::event_data::case_centric::xes::export_xes::export_xes_event_log;
use crate::core::event_data::case_centric::xes::import_xes::{
    import_xes, XESImportOptions, XESParseError,
};
use crate::core::event_data::case_centric::EventLog;
use crate::core::io::{Exportable, ExtensionWithMime, Importable};

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
}

impl std::fmt::Display for EventLogIOError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventLogIOError::Io(e) => write!(f, "IO Error: {}", e),
            EventLogIOError::Xes(e) => write!(f, "XES Error: {}", e),
            EventLogIOError::Json(e) => write!(f, "JSON Error: {}", e),
            EventLogIOError::Xml(e) => write!(f, "XML Error: {}", e),
            EventLogIOError::UnsupportedFormat(s) => write!(f, "Unsupported Format: {}", s),
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
            EventLogIOError::UnsupportedFormat(_) => None,
        }
    }
}

impl From<std::io::Error> for EventLogIOError {
    fn from(e: std::io::Error) -> Self {
        EventLogIOError::Io(e)
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
        EventLogIOError::Xml(e)
    }
}

impl Importable for EventLog {
    type Error = EventLogIOError;
    type ImportOptions = XESImportOptions;

    fn import_from_reader_with_options<R: Read>(
        reader: R,
        format: &str,
        options: Self::ImportOptions,
    ) -> Result<Self, Self::Error> {
        match format {
            _ if format.ends_with("json") => {
                let log: EventLog = serde_json::from_reader(reader)?;
                Ok(log)
            }
            _ if format.ends_with("xes") => {
                let buf_reader = BufReader::new(reader);
                import_xes(buf_reader, options).map_err(EventLogIOError::Xes)
            }
            _ if format.ends_with("xes.gz") => {
                let gz = flate2::read::GzDecoder::new(reader);
                let buf_reader = BufReader::new(gz);
                import_xes(buf_reader, options).map_err(EventLogIOError::Xes)
            }
            _ => Err(EventLogIOError::UnsupportedFormat(format.to_string())),
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
        writer: W,
        format: &str,
        _: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        if format.ends_with("json") {
            serde_json::to_writer(writer, self)?;
            Ok(())
        } else if format.ends_with("xes.gz") {
            let mut encoder = flate2::write::GzEncoder::new(writer, flate2::Compression::default());
            export_xes_event_log(&mut encoder, self)?;
            encoder.finish()?;
            Ok(())
        } else if format.ends_with("xes") {
            export_xes_event_log(writer, self)?;
            Ok(())
        } else {
            Err(EventLogIOError::UnsupportedFormat(format.to_string()))
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
