//! Read-only OCEL view trait
use std::{borrow::Cow, collections::HashMap};

use chrono::{DateTime, FixedOffset};

use crate::core::event_data::object_centric::ocel_struct::{
    OCELAttributeValue, OCELEvent, OCELObject, OCELType, OCEL,
};

/// Read-only OCEL view used by exports.
pub trait ReadableOCEL {
    /// Concrete id-keyed lookup type, returned by [`Self::lookup`].
    type Lookup<'a>: OCELLookup
    where
        Self: 'a;
    /// Get declared event types
    fn event_types(&self) -> &[OCELType];
    /// Get declared object types
    fn object_types(&self) -> &[OCELType];
    /// Iterate all events
    fn iter_events(&self) -> Box<dyn Iterator<Item = Cow<'_, OCELEvent>> + '_>;
    /// Iterate all events in ascending order by timestamp.
    fn iter_events_sorted_by_time(&self) -> Box<dyn Iterator<Item = Cow<'_, OCELEvent>> + '_> {
        let mut events: Vec<Cow<'_, OCELEvent>> = self.iter_events().collect();
        events.sort_by_key(|e| e.time);
        Box::new(events.into_iter())
    }
    /// Iterate all objects
    fn iter_objects(&self) -> Box<dyn Iterator<Item = Cow<'_, OCELObject>> + '_>;
    /// Iterate events of a given type. Default impl filters [`Self::iter_events`];
    /// indexed backends should override for O(matches) instead of O(total events).
    fn iter_events_of_type<'a>(
        &'a self,
        type_name: &'a str,
    ) -> Box<dyn Iterator<Item = Cow<'a, OCELEvent>> + 'a> {
        Box::new(
            self.iter_events()
                .filter(move |e| e.event_type == type_name),
        )
    }
    /// Iterate objects of a given type. Default impl filters [`Self::iter_objects`];
    /// indexed backends should override for O(matches) instead of O(total objects).
    fn iter_objects_of_type<'a>(
        &'a self,
        type_name: &'a str,
    ) -> Box<dyn Iterator<Item = Cow<'a, OCELObject>> + 'a> {
        Box::new(
            self.iter_objects()
                .filter(move |o| o.object_type == type_name),
        )
    }
    /// Build an id-keyed object lookup.
    fn lookup(&self) -> Self::Lookup<'_>;
}

/// Id-keyed access to an OCEL's objects. Built via [`ReadableOCEL::lookup`].
pub trait OCELLookup {
    /// Iterate object ids
    fn iter_object_ids<'a>(&'a self) -> Box<dyn Iterator<Item = &'a str> + 'a>;
    /// Resolve `id` to a borrowed `&str` whose lifetime is tied to `self`.
    fn get_id_borrow(&self, id: &str) -> Option<&str>;
    /// Get the `object_type` name for the given object id.
    fn object_type_of(&self, id: &str) -> Option<&str>;
    /// Iterate `(attribute_name, value, time)` for the given object.
    fn object_attributes<'a>(
        &'a self,
        id: &str,
    ) -> Box<dyn Iterator<Item = (&'a str, &'a OCELAttributeValue, DateTime<FixedOffset>)> + 'a>;
    /// Iterate O2O relationships for the given object: `(target_object_id, qualifier)`.
    fn object_relationships<'a>(
        &'a self,
        id: &str,
    ) -> Box<dyn Iterator<Item = (&'a str, &'a str)> + 'a>;
}

impl ReadableOCEL for OCEL {
    type Lookup<'a> = OCELHashLookup<'a>;
    fn event_types(&self) -> &[OCELType] {
        &self.event_types
    }
    fn object_types(&self) -> &[OCELType] {
        &self.object_types
    }
    fn iter_events(&self) -> Box<dyn Iterator<Item = Cow<'_, OCELEvent>> + '_> {
        Box::new(self.events.iter().map(Cow::Borrowed))
    }
    fn iter_events_sorted_by_time(&self) -> Box<dyn Iterator<Item = Cow<'_, OCELEvent>> + '_> {
        let mut sorted: Vec<&OCELEvent> = self.events.iter().collect();
        sorted.sort_by_key(|e| e.time);
        Box::new(sorted.into_iter().map(Cow::Borrowed))
    }
    fn iter_objects(&self) -> Box<dyn Iterator<Item = Cow<'_, OCELObject>> + '_> {
        Box::new(self.objects.iter().map(Cow::Borrowed))
    }
    fn lookup(&self) -> OCELHashLookup<'_> {
        OCELHashLookup {
            by_id: self.objects.iter().map(|o| (o.id.as_str(), o)).collect(),
            ids: self.objects.iter().map(|o| o.id.as_str()).collect(),
        }
    }
}

/// Lookup over a raw [`OCEL`], backed by a `HashMap` of id -> object reference.
#[derive(Debug)]
pub struct OCELHashLookup<'a> {
    by_id: HashMap<&'a str, &'a OCELObject>,
    /// Mirrors `OCEL.objects` insertion order; keeps `iter_object_ids` deterministic.
    ids: Vec<&'a str>,
}

impl<'a> OCELLookup for OCELHashLookup<'a> {
    fn iter_object_ids<'b>(&'b self) -> Box<dyn Iterator<Item = &'b str> + 'b> {
        Box::new(self.ids.iter().copied())
    }
    fn get_id_borrow(&self, id: &str) -> Option<&str> {
        self.by_id.get_key_value(id).map(|(k, _)| *k)
    }
    fn object_type_of(&self, id: &str) -> Option<&str> {
        self.by_id.get(id).map(|o| o.object_type.as_str())
    }
    fn object_attributes<'b>(
        &'b self,
        id: &str,
    ) -> Box<dyn Iterator<Item = (&'b str, &'b OCELAttributeValue, DateTime<FixedOffset>)> + 'b>
    {
        match self.by_id.get(id) {
            Some(o) => Box::new(
                o.attributes
                    .iter()
                    .map(|a| (a.name.as_str(), &a.value, a.time)),
            ),
            None => Box::new(std::iter::empty()),
        }
    }
    fn object_relationships<'b>(
        &'b self,
        id: &str,
    ) -> Box<dyn Iterator<Item = (&'b str, &'b str)> + 'b> {
        match self.by_id.get(id) {
            Some(o) => Box::new(
                o.relationships
                    .iter()
                    .map(|r| (r.object_id.as_str(), r.qualifier.as_str())),
            ),
            None => Box::new(std::iter::empty()),
        }
    }
}
