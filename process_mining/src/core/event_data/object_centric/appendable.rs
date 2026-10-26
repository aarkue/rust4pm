//! Appendable OCEL trait
use std::convert::Infallible;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use chrono::{DateTime, FixedOffset};

use crate::core::event_data::object_centric::io::OCELIOError;
use crate::core::event_data::object_centric::ocel_json::import_ocel_json_into;
use crate::core::event_data::object_centric::ocel_struct::{
    OCELEvent, OCELEventAttribute, OCELObject, OCELObjectAttribute, OCELRelationship, OCELType,
    OCEL,
};
use crate::core::event_data::object_centric::ocel_xml::xml_ocel_import::import_ocel_xml_into;
use crate::core::event_data::object_centric::ocel_xml::OCELImportOptions;
use crate::core::io::infer_format_from_path;

/// Appendable trait for OCEL data.
///
/// Handling of misordered input (appends before declarations, late declarations of an
/// already-seen type, forward-referenced relationships) is implementation-defined; see
/// each impl's docs.
pub trait AppendableOCEL {
    /// Type of error returned by the `declare_*` / `append_*` methods and `finalize`.
    type Error;

    /// Declare an event type. Behavior on re-declaration is implementation-defined.
    fn declare_event_type(&mut self, event_type: OCELType) -> Result<(), Self::Error>;
    /// Declare an object type. Behavior on re-declaration is implementation-defined.
    fn declare_object_type(&mut self, object_type: OCELType) -> Result<(), Self::Error>;

    /// Append an event.
    fn append_event(
        &mut self,
        id: String,
        event_type: &str,
        time: DateTime<FixedOffset>,
        attributes: Vec<OCELEventAttribute>,
        relationships: Vec<OCELRelationship>,
    ) -> Result<(), Self::Error>;

    /// Append an object.
    fn append_object(
        &mut self,
        id: String,
        object_type: &str,
        attributes: Vec<OCELObjectAttribute>,
        relationships: Vec<OCELRelationship>,
    ) -> Result<(), Self::Error>;

    /// Resolve any pending forward references. Default impl is a no-op.
    fn finalize(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl AppendableOCEL for OCEL {
    type Error = Infallible;

    fn declare_event_type(&mut self, event_type: OCELType) -> Result<(), Self::Error> {
        // Overwrite type if it already exists
        if let Some(et) = self
            .event_types
            .iter_mut()
            .find(|et| et.name == event_type.name)
        {
            *et = event_type;
        } else {
            self.event_types.push(event_type);
        }
        Ok(())
    }

    fn declare_object_type(&mut self, object_type: OCELType) -> Result<(), Self::Error> {
        // Overwrite type if it already exists
        if let Some(ot) = self
            .object_types
            .iter_mut()
            .find(|ot| ot.name == object_type.name)
        {
            *ot = object_type;
        } else {
            self.object_types.push(object_type);
        }
        Ok(())
    }

    fn append_event(
        &mut self,
        id: String,
        event_type: &str,
        time: DateTime<FixedOffset>,
        attributes: Vec<OCELEventAttribute>,
        relationships: Vec<OCELRelationship>,
    ) -> Result<(), Self::Error> {
        self.events.push(OCELEvent {
            id,
            event_type: event_type.to_string(),
            time,
            attributes,
            relationships,
        });
        Ok(())
    }

    fn append_object(
        &mut self,
        id: String,
        object_type: &str,
        attributes: Vec<OCELObjectAttribute>,
        relationships: Vec<OCELRelationship>,
    ) -> Result<(), Self::Error> {
        self.objects.push(OCELObject {
            id,
            object_type: object_type.to_string(),
            attributes,
            relationships,
        });
        Ok(())
    }
}

/// Streaming counterpart to [`Importable`](crate::Importable):
/// Import an OCEL from a reader or path straight into an [`AppendableOCEL`] sink.
pub trait StreamImportOCEL: AppendableOCEL + Sized {
    /// Stream an OCEL from `reader` in the given `format` into this sink.
    ///
    /// [`AppendableOCEL::finalize`] runs once the stream ends. Calling it again is harmless.
    fn stream_ocel_from_reader<R: Read>(
        &mut self,
        reader: R,
        format: &str,
        options: OCELImportOptions,
    ) -> Result<(), OCELIOError>
    where
        Self::Error: Into<OCELIOError>,
    {
        if let Some(inner) = format.strip_suffix(".gz") {
            // Erase the reader type for recursion
            let gz: Box<dyn Read> = Box::new(flate2::read::GzDecoder::new(BufReader::new(reader)));
            return self.stream_ocel_from_reader(gz, inner, options);
        }
        if format.ends_with("json") || format.ends_with("jsonocel") {
            import_ocel_json_into(BufReader::new(reader), self)?;
        } else if format.ends_with("xml") || format.ends_with("xmlocel") {
            let mut xml = quick_xml::Reader::from_reader(BufReader::new(reader));
            import_ocel_xml_into(&mut xml, self, options)?;
        } else {
            return Err(OCELIOError::UnsupportedFormat(format!(
                "no streaming OCEL importer for format {format:?}"
            )));
        }
        self.finalize().map_err(Into::into)
    }

    /// Infer the format from `path` and stream the file into this sink.
    fn stream_ocel_from_path<P: AsRef<Path>>(
        &mut self,
        path: P,
        options: OCELImportOptions,
    ) -> Result<(), OCELIOError>
    where
        Self::Error: Into<OCELIOError>,
    {
        let path = path.as_ref();
        let format = infer_format_from_path(path).ok_or_else(|| {
            OCELIOError::UnsupportedFormat(format!("cannot infer OCEL format from {path:?}"))
        })?;
        self.stream_ocel_from_reader(File::open(path)?, &format, options)
    }
}

impl<A: AppendableOCEL> StreamImportOCEL for A {}

/// Whether streaming a given format directly is supported.
pub fn is_streaming_format(format: &str) -> bool {
    let base = format.strip_suffix(".gz").unwrap_or(format);
    base.ends_with("json")
        || base.ends_with("jsonocel")
        || base.ends_with("xml")
        || base.ends_with("xmlocel")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::io::Importable;

    #[test]
    fn is_streaming_format_agrees_with_the_dispatch() {
        for f in OCEL::known_import_formats() {
            let mut ocel = OCEL {
                event_types: Vec::new(),
                object_types: Vec::new(),
                events: Vec::new(),
                objects: Vec::new(),
            };
            let err = ocel
                .stream_ocel_from_reader(
                    std::io::empty(),
                    &f.extension,
                    OCELImportOptions::default(),
                )
                .err();
            // Empty input fails in whatever way the format's parser fails. Only the
            // "no streaming importer" rejection means the dispatch picked no arm.
            let dispatched = !matches!(err, Some(OCELIOError::UnsupportedFormat(_)));
            assert_eq!(
                dispatched,
                is_streaming_format(&f.extension),
                "format {:?}",
                f.extension
            );
        }
    }
}
