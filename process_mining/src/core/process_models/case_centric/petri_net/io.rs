//! IO implementations for `PetriNet`

use std::io::{Read, Write};

use crate::core::io::{Exportable, ExtensionWithMime, Importable};
use crate::core::process_models::case_centric::petri_net::pnml::export_pnml::export_petri_net_to_pnml;
use crate::core::process_models::case_centric::petri_net::pnml::import_pnml::{
    import_pnml_reader, PNMLParseError,
};
use crate::core::process_models::case_centric::petri_net::PetriNet;

/// Error type for `PetriNet` IO operations
#[derive(Debug)]
pub enum PetriNetIOError {
    /// IO Error
    Io(std::io::Error),
    /// PNML Parsing Error
    Pnml(PNMLParseError),
    /// XML Parsing Error
    Xml(quick_xml::Error),
    /// Unsupported Format
    UnsupportedFormat(String),
}

impl std::fmt::Display for PetriNetIOError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PetriNetIOError::Io(e) => write!(f, "IO Error: {}", e),
            PetriNetIOError::Pnml(e) => write!(f, "PNML Error: {}", e),
            PetriNetIOError::Xml(e) => write!(f, "XML Error: {}", e),
            PetriNetIOError::UnsupportedFormat(s) => write!(f, "Unsupported Format: {}", s),
        }
    }
}

impl std::error::Error for PetriNetIOError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PetriNetIOError::Io(e) => Some(e),
            PetriNetIOError::Pnml(e) => Some(e),
            PetriNetIOError::Xml(e) => Some(e),
            PetriNetIOError::UnsupportedFormat(_) => None,
        }
    }
}

impl From<std::io::Error> for PetriNetIOError {
    fn from(e: std::io::Error) -> Self {
        PetriNetIOError::Io(e)
    }
}

impl From<PNMLParseError> for PetriNetIOError {
    fn from(e: PNMLParseError) -> Self {
        PetriNetIOError::Pnml(e)
    }
}

impl From<quick_xml::Error> for PetriNetIOError {
    fn from(e: quick_xml::Error) -> Self {
        PetriNetIOError::Xml(e)
    }
}

impl Importable for PetriNet {
    type Error = PetriNetIOError;
    type ImportOptions = ();

    fn import_from_reader_with_options<R: Read>(
        reader: R,
        format: &str,
        _: Self::ImportOptions,
    ) -> Result<Self, Self::Error> {
        if format == "pnml" || format.ends_with(".pnml") {
            let mut buf_reader = std::io::BufReader::new(reader);
            import_pnml_reader(&mut buf_reader).map_err(PetriNetIOError::Pnml)
        } else {
            Err(PetriNetIOError::UnsupportedFormat(format.to_string()))
        }
    }

    fn known_import_formats() -> Vec<crate::core::io::ExtensionWithMime> {
        vec![ExtensionWithMime::new("pnml", "text/plain")]
    }
}

impl Exportable for PetriNet {
    type Error = PetriNetIOError;
    type ExportOptions = ();

    fn export_to_writer_with_options<W: Write>(
        &self,
        writer: W,
        format: &str,
        _: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        if format == "pnml" || format.ends_with(".pnml") {
            export_petri_net_to_pnml(self, writer).map_err(PetriNetIOError::Xml)
        } else {
            Err(PetriNetIOError::UnsupportedFormat(format.to_string()))
        }
    }

    fn known_export_formats() -> Vec<ExtensionWithMime> {
        vec![ExtensionWithMime::new("pnml", "text/plain")]
    }
}
