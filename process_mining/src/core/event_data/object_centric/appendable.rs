//! Appendable OCEL trait
use std::convert::Infallible;
use std::io::Read;
use std::path::Path;

use chrono::{DateTime, FixedOffset};

use crate::core::event_data::object_centric::io::{
    stream_ocel, OCELIOError, OCELImportOptions, OcelFormat, OcelSource,
};
use crate::core::event_data::object_centric::ocel_struct::{
    OCELEvent, OCELEventAttribute, OCELObject, OCELObjectAttribute, OCELRelationship, OCELType,
    OCEL,
};
use crate::core::io::{controlled, infer_format_or_err};
use crate::utils::control::Control;

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

/// Declare the types of `ocel` in `sink`, then append its objects and its events sorted by time.
///
/// An event or object the sink rejects goes to `on_rejected` with `"event"` or `"object"`, which
/// either skips it (`Ok`) or stops the append with the error.
pub(crate) fn append_ocel<A: AppendableOCEL>(
    sink: &mut A,
    mut ocel: OCEL,
    mut on_rejected: impl FnMut(&str, A::Error) -> Result<(), A::Error>,
) -> Result<(), A::Error> {
    ocel.events.sort_by_key(|e| e.time);
    for t in ocel.event_types {
        sink.declare_event_type(t)?;
    }
    for t in ocel.object_types {
        sink.declare_object_type(t)?;
    }
    for o in ocel.objects {
        if let Err(e) = sink.append_object(o.id, &o.object_type, o.attributes, o.relationships) {
            on_rejected("object", e)?;
        }
    }
    for e in ocel.events {
        if let Err(err) =
            sink.append_event(e.id, &e.event_type, e.time, e.attributes, e.relationships)
        {
            on_rejected("event", err)?;
        }
    }
    Ok(())
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

/// Streaming counterpart to [`Importable`](crate::Importable): import an OCEL from a reader or a
/// path straight into an [`AppendableOCEL`] sink.
///
/// Reads every format [`OCEL`] imports. JSON, XML, bundles and a `DuckDB` file in the consolidated
/// schema stream into the sink. The other formats are imported as an [`OCEL`] first and appended
/// with their events sorted by time. [`AppendableOCEL::finalize`] runs once the input ends;
/// calling it again is harmless.
pub trait StreamImportOCEL: AppendableOCEL + Sized {
    /// Stream an OCEL in `format` (see
    /// [`Importable::known_import_formats`](crate::Importable::known_import_formats)) from
    /// `reader` into this sink, reporting progress and warnings to `ctl` and stopping once it is
    /// cancelled. `DuckDB` needs a path.
    fn stream_ocel_from_reader<R: Read>(
        &mut self,
        reader: R,
        format: &str,
        options: OCELImportOptions,
        ctl: &Control<'_>,
    ) -> Result<(), OCELIOError>
    where
        Self::Error: Into<OCELIOError>,
    {
        let f = OcelFormat::parse(format)?;
        controlled(ctl, || {
            stream_ocel(
                self,
                OcelSource::reader(reader, ctl, None),
                f,
                &options,
                ctl,
            )
        })
    }

    /// Stream the OCEL at `path`, in the format inferred from it, into this sink, reporting to
    /// `ctl` as [`Importable::import_from_path_with`](crate::Importable::import_from_path_with)
    /// does.
    fn stream_ocel_from_path<P: AsRef<Path>>(
        &mut self,
        path: P,
        options: OCELImportOptions,
        ctl: &Control<'_>,
    ) -> Result<(), OCELIOError>
    where
        Self::Error: Into<OCELIOError>,
    {
        let path = path.as_ref();
        let f = infer_format_or_err(path, OcelFormat::infer)?;
        controlled(ctl, || {
            stream_ocel(self, OcelSource::Path(path), f, &options, ctl)
        })
    }
}

impl<A: AppendableOCEL> StreamImportOCEL for A {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ocel, Exportable, Importable};

    fn empty() -> OCEL {
        OCEL {
            event_types: Vec::new(),
            object_types: Vec::new(),
            events: Vec::new(),
            objects: Vec::new(),
        }
    }

    #[test]
    fn known_format_names_parse_back() {
        for f in OcelFormat::known() {
            assert_eq!(OcelFormat::parse(&f.extension).unwrap().name(), f.extension);
            assert_eq!(
                OcelFormat::parse(&f.extension.to_uppercase())
                    .unwrap()
                    .name(),
                f.extension
            );
        }
    }

    #[test]
    fn every_format_streams_into_an_ocel() {
        let source = ocel!(
            events:
            ("place", ["o:1", "i:1", "i:2"]),
            ("pack", ["o:1", "i:2"]),
            ("pay", ["o:1"]),
            o2o:
            ("o:1", "i:1"),
            ("o:1", "i:2")
        );
        let o2o =
            |ocel: &OCEL| -> usize { ocel.objects.iter().map(|o| o.relationships.len()).sum() };
        let dir = tempfile::tempdir().unwrap();
        for f in OCEL::known_import_formats() {
            let path = dir.path().join(format!("log.{}", f.extension));
            source
                .export_to_path_as(&path, &f.extension, Default::default())
                .unwrap();

            let mut from_path = empty();
            from_path
                .stream_ocel_from_path(&path, OCELImportOptions::default(), &Control::new())
                .unwrap_or_else(|e| panic!("{}: {e}", f.extension));
            assert_eq!(
                from_path.events.len(),
                source.events.len(),
                "{}",
                f.extension
            );
            assert_eq!(
                from_path.objects.len(),
                source.objects.len(),
                "{}",
                f.extension
            );
            assert_eq!(o2o(&from_path), o2o(&source), "{}", f.extension);

            if f.extension == "duckdb" {
                continue;
            }
            let mut from_reader = empty();
            from_reader
                .stream_ocel_from_reader(
                    std::fs::File::open(&path).unwrap(),
                    &f.extension,
                    OCELImportOptions::default(),
                    &Control::new(),
                )
                .unwrap_or_else(|e| panic!("{}: {e}", f.extension));
            assert_eq!(
                from_reader.events.len(),
                source.events.len(),
                "{}",
                f.extension
            );
            assert_eq!(
                from_reader.objects.len(),
                source.objects.len(),
                "{}",
                f.extension
            );
            assert_eq!(o2o(&from_reader), o2o(&source), "{}", f.extension);
        }
    }
}
