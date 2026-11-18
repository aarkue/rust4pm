//! IO implementations for `PetriNet` and `AcceptingPetriNet`

use std::io::{Read, Write};

use crate::core::io::{Exportable, ExtensionWithMime, Importable};
use crate::core::process_models::case_centric::petri_net::{
    pnml::{export_pnml, import_pnml, read::accepting_net, PNMLParseError},
    AcceptingPetriNet, PetriNet, PlaceID,
};
use crate::utils::control::{Cancelled, Control};

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
    /// A marking refers to a place that is not in the net
    InvalidPlaceInMarking(PlaceID),
    /// The import's [`Control`] was cancelled
    Cancelled,
}

impl std::fmt::Display for PetriNetIOError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PetriNetIOError::Io(e) => write!(f, "IO Error: {}", e),
            PetriNetIOError::Pnml(e) => write!(f, "PNML Error: {}", e),
            PetriNetIOError::Xml(e) => write!(f, "XML Error: {}", e),
            PetriNetIOError::UnsupportedFormat(s) => write!(f, "Unsupported Format: {}", s),
            PetriNetIOError::InvalidPlaceInMarking(place) => write!(
                f,
                "A marking refers to place {}, which is not in the net",
                place.index()
            ),
            PetriNetIOError::Cancelled => write!(f, "{Cancelled}"),
        }
    }
}

impl std::error::Error for PetriNetIOError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PetriNetIOError::Io(e) => Some(e),
            PetriNetIOError::Pnml(e) => Some(e),
            PetriNetIOError::Xml(e) => Some(e),
            PetriNetIOError::UnsupportedFormat(_)
            | PetriNetIOError::InvalidPlaceInMarking(_)
            | PetriNetIOError::Cancelled => None,
        }
    }
}

impl From<std::io::Error> for PetriNetIOError {
    fn from(e: std::io::Error) -> Self {
        PetriNetIOError::Io(e)
    }
}

impl From<Cancelled> for PetriNetIOError {
    fn from(_: Cancelled) -> Self {
        PetriNetIOError::Cancelled
    }
}

impl From<PNMLParseError> for PetriNetIOError {
    fn from(e: PNMLParseError) -> Self {
        PetriNetIOError::Pnml(e)
    }
}

impl From<quick_xml::Error> for PetriNetIOError {
    fn from(e: quick_xml::Error) -> Self {
        match e {
            quick_xml::Error::Io(e) => {
                PetriNetIOError::Io(std::io::Error::new(e.kind(), e.to_string()))
            }
            e => PetriNetIOError::Xml(e),
        }
    }
}

/// Accept the formats `pnml` and `apnml`, ignoring case and a leading `.`
fn parse_pnml_format(format: &str) -> Result<(), PetriNetIOError> {
    let lower = format.to_ascii_lowercase();
    match lower.strip_prefix('.').unwrap_or(&lower) {
        "pnml" | "apnml" => Ok(()),
        _ => Err(PetriNetIOError::UnsupportedFormat(format.to_string())),
    }
}

/// Reads the structure only, ignoring markings. See [`import_pnml`] for what is read.
impl Importable for PetriNet {
    type Error = PetriNetIOError;
    type ImportOptions = ();

    fn import_from_reader_with<R: Read>(
        reader: R,
        format: &str,
        _: Self::ImportOptions,
        _: &Control<'_>,
    ) -> Result<Self, Self::Error> {
        parse_pnml_format(format)?;
        let (net, _, _) = import_pnml(std::io::BufReader::new(reader))?;
        Ok(net)
    }

    fn known_import_formats() -> Vec<ExtensionWithMime> {
        vec![
            ExtensionWithMime::new("pnml", "text/plain"),
            ExtensionWithMime::new("apnml", "text/plain"),
        ]
    }
}

/// Fails with [`PNMLParseError::MissingInitialMarking`] if no place has an `<initialMarking>`
/// (even with 0 tokens), and with [`PNMLParseError::MissingFinalMarking`] without a final marking
/// (`ProM`'s `<finalmarkings>`). See [`import_pnml`] for what is read.
impl Importable for AcceptingPetriNet {
    type Error = PetriNetIOError;
    type ImportOptions = ();

    fn import_from_reader_with<R: Read>(
        reader: R,
        format: &str,
        _: Self::ImportOptions,
        _: &Control<'_>,
    ) -> Result<Self, Self::Error> {
        parse_pnml_format(format)?;
        let parts = import_pnml(std::io::BufReader::new(reader))?;
        Ok(accepting_net(parts)?)
    }

    fn known_import_formats() -> Vec<ExtensionWithMime> {
        vec![
            ExtensionWithMime::new("pnml", "text/plain"),
            ExtensionWithMime::new("apnml", "text/plain"),
        ]
    }
}

/// Writes the structure without markings
impl Exportable for PetriNet {
    type Error = PetriNetIOError;
    type ExportOptions = ();

    fn export_to_writer_with_options<W: Write>(
        &self,
        writer: W,
        format: &str,
        _: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        parse_pnml_format(format)?;
        export_pnml(writer, self, None, None)
    }

    fn known_export_formats() -> Vec<ExtensionWithMime> {
        vec![ExtensionWithMime::new("pnml", "text/plain")]
    }
}

/// Writes the net with both markings
impl Exportable for AcceptingPetriNet {
    type Error = PetriNetIOError;
    type ExportOptions = ();

    fn export_to_writer_with_options<W: Write>(
        &self,
        writer: W,
        format: &str,
        _: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        parse_pnml_format(format)?;
        export_pnml(
            writer,
            &self.net,
            Some(&self.initial_marking),
            Some(&self.final_marking),
        )
    }

    fn known_export_formats() -> Vec<ExtensionWithMime> {
        vec![ExtensionWithMime::new("pnml", "text/plain")]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pnml_format_names() {
        for format in ["pnml", "PNML", ".pnml", ".PnMl", "apnml"] {
            assert!(parse_pnml_format(format).is_ok(), "{format}");
        }
        for format in ["xpnml", "foo.pnml", "pnml.gz", ""] {
            assert!(
                matches!(
                    parse_pnml_format(format),
                    Err(PetriNetIOError::UnsupportedFormat(_))
                ),
                "{format}"
            );
        }
        let net = PetriNet::new();
        let mut bytes = Vec::new();
        net.export_to_writer_with_options(&mut bytes, "PNML", ())
            .unwrap();
        PetriNet::import_from_reader_with(bytes.as_slice(), ".pnml", (), &Control::new()).unwrap();
        assert!(net
            .export_to_writer_with_options(Vec::new(), "xpnml", ())
            .is_err());
    }
}
