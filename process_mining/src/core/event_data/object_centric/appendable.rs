//! Appendable OCEL trait
use std::convert::Infallible;

use chrono::{DateTime, FixedOffset};

use crate::core::event_data::object_centric::ocel_struct::{
    OCELEvent, OCELEventAttribute, OCELObject, OCELObjectAttribute, OCELRelationship, OCELType,
    OCEL,
};

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
