//! Linked Slim (i.e., less duplicate fields) OCEL
//!
//! Allows easy and efficient access to events, objects, and their relations
use std::{
    borrow::{Borrow, Cow},
    collections::HashMap,
    io::{Read, Write},
    path::Path,
};

use chrono::{DateTime, FixedOffset};
use itertools::Itertools;
use macros_process_mining::RegistryEntity;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    core::{
        event_data::object_centric::{
            io::OCELIOError, linked_ocel::LinkedOCELAccess, OCELAttributeValue, OCELEvent,
            OCELEventAttribute, OCELObject, OCELObjectAttribute, OCELRelationship, OCELType,
            OCELTypeAttribute,
        },
        io::ExtensionWithMime,
        OCEL,
    },
    Exportable, Importable,
};

/// An Event Index
///
/// Points to an event in the context of a given OCEL
#[derive(
    PartialEq, Eq, Hash, Clone, Copy, Debug, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
pub struct EventIndex(usize);
impl From<&EventIndex> for EventIndex {
    fn from(value: &EventIndex) -> Self {
        *value
    }
}
impl From<usize> for EventIndex {
    fn from(value: usize) -> Self {
        Self(value)
    }
}
impl EventIndex {
    /// Get the (slim) event referenced by this index in the locel
    ///
    /// Note: If there is no event at the specified index, this will access an array out of bounds!
    /// Use the [`EventIndex::get_ev_opt`] version if you want to handle this explicitly.
    pub fn get_ev<'a>(&self, locel: &'a SlimLinkedOCEL) -> &'a SlimOCELEvent {
        &locel.events[self.0]
    }
    /// Get the (slim) event referenced by this index in the locel
    ///
    /// This version explicitly handles scenarios where the event might not exist.
    /// In case you are sure that the object exists, use the [`EventIndex::get_ev`] function instead.
    pub fn get_ev_opt<'a>(&self, locel: &'a SlimLinkedOCEL) -> Option<&'a SlimOCELEvent> {
        locel.events.get(self.0)
    }
    /// Get the event type of the event referenced through this event index
    pub fn get_ev_type<'a>(&self, locel: &'a SlimLinkedOCEL) -> &'a String {
        &locel.event_types[locel.events[self.0].event_type].name
    }
    /// Get the timestamp of this event
    pub fn get_time<'a>(&self, locel: &'a SlimLinkedOCEL) -> &'a DateTime<FixedOffset> {
        &locel.events[self.0].time
    }
    /// Get E2O relationships of this event
    pub fn get_e2o<'a>(
        &self,
        locel: &'a SlimLinkedOCEL,
    ) -> impl Iterator<Item = &'a ObjectIndex> + use<'a> {
        locel
            .events
            .get(self.0)
            .into_iter()
            .flat_map(|e| e.relationships.iter().map(|(_q, o)| o))
        // .copied()
    }
    /// Get an attribute value of this event, specified by the attribute name
    ///
    /// Returns [`None`] if there is no such attribute.
    pub fn get_attribute_value<'a>(
        &self,
        attr_name: &str,
        locel: &'a SlimLinkedOCEL,
    ) -> Option<&'a OCELAttributeValue> {
        let ev = self.get_ev(locel);
        let (index, _attr) = locel.event_types[ev.event_type]
            .attributes
            .iter()
            .enumerate()
            .find(|(_i, a)| a.name == attr_name)?;
        let attr_val = ev.attributes.get(index)?;
        Some(attr_val)
    }
    /// Get a mutable reference to the attribute value of this event, specified by the attribute name
    ///
    /// Returns [`None`] if there is no such attribute.
    pub fn get_attribute_value_mut<'a>(
        &self,
        attr_name: &str,
        locel: &'a mut SlimLinkedOCEL,
    ) -> Option<&'a mut OCELAttributeValue> {
        let ev = &mut locel.events[self.0];
        let (index, _attr) = locel.event_types[ev.event_type]
            .attributes
            .iter()
            .enumerate()
            .find(|(_i, a)| a.name == attr_name)?;
        let attr_val = ev.attributes.get_mut(index)?;
        Some(attr_val)
    }
    /// Get 'fat' version of Event (i.e., with all fields expanded, with a structure similar to the OCEL 2.0 specification)
    pub fn fat_ev(&self, locel: &SlimLinkedOCEL) -> OCELEvent {
        let sev = self.get_ev(locel);
        let ev_type = &locel.event_types[sev.event_type];
        OCELEvent {
            id: sev.id.clone(),
            event_type: ev_type.name.clone(),
            time: sev.time,
            attributes: ev_type
                .attributes
                .iter()
                .enumerate()
                .map(|(i, at)| OCELEventAttribute {
                    name: at.name.clone(),
                    value: sev
                        .attributes
                        .get(i)
                        .cloned()
                        .unwrap_or(OCELAttributeValue::Null),
                })
                .collect(),
            relationships: sev
                .relationships
                .iter()
                .map(|(q, o)| OCELRelationship {
                    object_id: locel.objects[o.into_inner()].id.clone(),
                    qualifier: q.to_string(),
                })
                .collect(),
        }
    }
}
impl EventIndex {
    /// Retrieve inner index value
    ///
    /// Warning: Only use carefully, as wrong usage can lead to invalid `EventIndex` references, even when using only a single OCEL
    pub fn into_inner(self) -> usize {
        self.0
    }
}

#[derive(
    PartialEq, Eq, Hash, Clone, Copy, Debug, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
/// An Object Index
///
/// Points to an object in the context of a given OCEL
pub struct ObjectIndex(usize);
impl From<&ObjectIndex> for ObjectIndex {
    fn from(value: &ObjectIndex) -> Self {
        *value
    }
}
impl From<usize> for ObjectIndex {
    fn from(value: usize) -> Self {
        Self(value)
    }
}
impl ObjectIndex {
    /// Get the (slim) object referred to by this index in the locel
    ///
    /// Note: If there is no object at the specified index, this will access an array out of bounds!
    /// Use the [`ObjectIndex::get_ob_opt`] version if you want to handle this explicitly.
    pub fn get_ob<'a>(&self, locel: &'a SlimLinkedOCEL) -> &'a SlimOCELObject {
        &locel.objects[self.0]
    }
    /// Get the (slim) object referred to by this index in the locel
    ///
    /// This version explicitly handles scenarios where the object might not exist.
    /// In case you are sure that the object exists, use the [`ObjectIndex::get_ob`] function instead.
    pub fn get_ob_opt<'a>(&self, locel: &'a SlimLinkedOCEL) -> Option<&'a SlimOCELObject> {
        locel.objects.get(self.0)
    }

    /// Get the object type of the object referenced through this object index
    pub fn get_ob_type<'a>(&self, locel: &'a SlimLinkedOCEL) -> &'a String {
        &locel.object_types[locel.objects[self.0].object_type].name
    }
    /// Get O2O relationships
    pub fn get_o2o<'a>(
        &self,
        locel: &'a SlimLinkedOCEL,
    ) -> impl Iterator<Item = &'a ObjectIndex> + use<'a> {
        locel
            .objects
            .get(self.0)
            .into_iter()
            .flat_map(|o| &o.relationships)
            .map(|(_q, o)| o)
        // .copied()
    }
    /// Get reverse O2O relationships
    pub fn get_o2o_rev<'a>(
        &self,
        locel: &'a SlimLinkedOCEL,
    ) -> impl Iterator<Item = &'a ObjectIndex> + use<'a> {
        locel
            .o2o_rel_rev
            .get(self.0)
            .into_iter()
            .flatten()
            .flatten()
        // .copied()
    }
    /// Get reverse E2O relationships
    pub fn get_e2o_rev<'a>(
        &self,
        locel: &'a SlimLinkedOCEL,
    ) -> impl Iterator<Item = &'a EventIndex> + use<'a> {
        locel
            .e2o_rel_rev
            .get(self.0)
            .into_iter()
            .flatten()
            .flatten()
        // .copied()
    }
    /// Get reverse E2O relationships of all events with the specified event type
    ///
    /// Returns an empty iterator if the event type is unknown or the object index is out of bounds.
    pub fn get_e2o_rev_of_evtype<'a>(
        &self,
        locel: &'a SlimLinkedOCEL,
        evtype: &'a str,
    ) -> impl Iterator<Item = &'a EventIndex> + use<'a> {
        let evtype_index = locel.evtype_to_index.get(evtype).copied();
        let ob_idx = self.0;
        evtype_index.into_iter().flat_map(move |ei| {
            locel
                .e2o_rel_rev
                .get(ob_idx)
                .into_iter()
                .flat_map(move |x| x.get(ei))
                .flatten()
        })
    }
    /// Get attribute values of this object, specified by the attribute name
    ///
    /// Returns [`None`] if there is no such attribute.
    pub fn get_attribute_value<'a>(
        &self,
        attr_name: &str,
        locel: &'a SlimLinkedOCEL,
    ) -> Option<&'a Vec<(DateTime<FixedOffset>, OCELAttributeValue)>> {
        let ob = self.get_ob(locel);
        let (index, _attr) = locel.object_types[ob.object_type]
            .attributes
            .iter()
            .enumerate()
            .find(|(_i, a)| a.name == attr_name)?;
        let attr_val = ob.attributes.get(index)?;
        Some(attr_val)
    }
    /// Get a mutable references to the attribute values of this object, specified by the attribute name
    ///
    /// Returns [`None`] if there is no such attribute.
    pub fn get_attribute_value_mut<'a>(
        &self,
        attr_name: &str,
        locel: &'a mut SlimLinkedOCEL,
    ) -> Option<&'a mut Vec<(DateTime<FixedOffset>, OCELAttributeValue)>> {
        let ob = &mut locel.objects[self.0];
        let (index, _attr) = locel.object_types[ob.object_type]
            .attributes
            .iter()
            .enumerate()
            .find(|(_i, a)| a.name == attr_name)?;
        let attr_val = ob.attributes.get_mut(index)?;
        Some(attr_val)
    }

    fn fat_ob(&self, locel: &SlimLinkedOCEL) -> OCELObject {
        let sev = self.get_ob(locel);
        let ob_type = &locel.object_types[sev.object_type];
        OCELObject {
            id: sev.id.clone(),
            object_type: ob_type.name.clone(),
            attributes: ob_type
                .attributes
                .iter()
                .enumerate()
                .flat_map(|(i, at)| {
                    sev.attributes
                        .get(i)
                        .into_iter()
                        .flatten()
                        .map(|(t, v)| OCELObjectAttribute {
                            name: at.name.clone(),
                            value: v.clone(),
                            time: *t,
                        })
                })
                .collect(),
            relationships: sev
                .relationships
                .iter()
                .map(|(q, o)| OCELRelationship {
                    object_id: locel.objects[o.into_inner()].id.clone(),
                    qualifier: q.to_string(),
                })
                .collect(),
        }
    }
}

impl ObjectIndex {
    /// Retrieve inner index value
    ///
    /// Warning: Only use carefully, as wrong usage can lead to invalid `ObjectIndex` references, even when using only a single OCEL
    pub fn into_inner(self) -> usize {
        self.0
    }
}

/// Either an event or an object index
#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
pub enum EventOrObjectIndex {
    /// An event index
    Event(EventIndex),
    /// An object index
    Object(ObjectIndex),
}
impl From<EventIndex> for EventOrObjectIndex {
    fn from(value: EventIndex) -> Self {
        Self::Event(value)
    }
}
impl From<ObjectIndex> for EventOrObjectIndex {
    fn from(value: ObjectIndex) -> Self {
        Self::Object(value)
    }
}

fn sorted_insert<T, B: Ord>(vec: &mut Vec<T>, to_add: T, mut f: impl FnMut(&T) -> B) {
    if let Err(index) = vec.binary_search_by_key(&f(&to_add), f) {
        vec.insert(index, to_add);
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, RegistryEntity, Default)]
/// An object-centric event log where events and objects are referenced by integer indices
/// ([`EventIndex`] / [`ObjectIndex`]) returned from the `add_*` methods, and each indexed
/// event/object is an instance of an event/object type (activity / object class) declared
/// beforehand with an ordered list of attributes.
pub struct SlimLinkedOCEL {
    /// Events
    events: Vec<SlimOCELEvent>,
    /// Objects
    objects: Vec<SlimOCELObject>,
    /// Event types (Activities)
    event_types: Vec<OCELType>,
    /// Object types
    object_types: Vec<OCELType>,
    event_ids_to_index: HashMap<String, EventIndex>,
    object_ids_to_index: HashMap<String, ObjectIndex>,
    /// Events per Event Type
    events_per_type: Vec<Vec<EventIndex>>,
    /// List of object indices per object type
    objects_per_type: Vec<Vec<ObjectIndex>>,
    /// Reverse E2O relationships
    /// Split by event type (i.e., first level: object index -> event type index -> List of events)
    /// The final list of events should be sorted!
    e2o_rel_rev: Vec<Vec<Vec<EventIndex>>>,
    /// Reverse O2O Relationships (i.e., first level object index -> object type index -> List of objects)
    /// The final list of objects should be sorted!
    o2o_rel_rev: Vec<Vec<Vec<ObjectIndex>>>,
    evtype_to_index: HashMap<String, usize>,
    obtype_to_index: HashMap<String, usize>,
}
impl SlimLinkedOCEL {
    /// Create a new empty `SlimLinkedOCEL`
    ///
    /// After creation, new event/object types as well as event/object instances can be added to it.
    pub fn new() -> Self {
        Self::default()
    }
    /// Convert an unlinked [`OCEL`] to a [`SlimLinkedOCEL`]
    ///
    pub fn from_ocel(mut ocel: OCEL) -> Self {
        let evtype_to_index: HashMap<_, _> = ocel
            .event_types
            .iter()
            .enumerate()
            .map(|(i, t)| (t.name.clone(), i))
            .collect();
        let obtype_to_index: HashMap<_, _> = ocel
            .object_types
            .iter()
            .enumerate()
            .map(|(i, t)| (t.name.clone(), i))
            .collect();
        ocel.events.sort_by_key(|e| e.time);
        let event_ids_to_index: HashMap<_, _> = ocel
            .events
            .iter()
            .enumerate()
            .map(|(ev_index, e)| (e.id.clone(), EventIndex(ev_index)))
            .collect();
        let object_ids_to_index: HashMap<_, _> = ocel
            .objects
            .iter()
            .enumerate()
            .map(|(ob_index, o)| (o.id.clone(), ObjectIndex(ob_index)))
            .collect();

        let mut events_per_type: Vec<Vec<EventIndex>> = vec![Vec::new(); ocel.event_types.len()];
        let mut objects_per_type: Vec<Vec<ObjectIndex>> = vec![Vec::new(); ocel.object_types.len()];
        let mut e2o_rel_rev: Vec<Vec<Vec<EventIndex>>> =
            vec![vec![Vec::new(); ocel.event_types.len()]; ocel.objects.len()];
        let mut o2o_rel_rev: Vec<Vec<Vec<ObjectIndex>>> =
            vec![vec![Vec::new(); ocel.object_types.len()]; ocel.objects.len()];
        let events: Vec<SlimOCELEvent> = ocel
            .events
            .into_iter()
            .enumerate()
            .map(|(e_i, e)| {
                let evtype_index = *evtype_to_index.get(&e.event_type).unwrap();
                let ev_index = EventIndex(e_i);
                events_per_type[evtype_index].push(ev_index);
                let ev_type = &ocel.event_types[evtype_index];
                SlimOCELEvent {
                    id: e.id,
                    event_type: evtype_index,
                    time: e.time,
                    attributes: ev_type
                        .attributes
                        .iter()
                        .map(|a| {
                            e.attributes
                                .iter()
                                .find(|ea| ea.name == a.name)
                                .map(|ea| ea.value.clone())
                                .unwrap_or(OCELAttributeValue::Null)
                        })
                        .collect(),
                    relationships: e
                        .relationships
                        .into_iter()
                        .flat_map(|rel| {
                            // Side effect: We also insert the reverse relation here!
                            // The filter_map and ? here prevent invalid O2O/E2O references :(
                            let rel_obj_id = object_ids_to_index.get(&rel.object_id)?;
                            e2o_rel_rev[rel_obj_id.into_inner()][evtype_index].push(ev_index);
                            Some((rel.qualifier, *rel_obj_id))
                        })
                        // These are sorted!
                        // In particular, this allows more efficient binary search for checking if an element is related
                        .sorted_unstable_by_key(|(_q, o)| *o)
                        .collect(),
                }
            })
            .collect();
        let objects: Vec<SlimOCELObject> = ocel
            .objects
            .into_iter()
            .enumerate()
            .map(|(o_i, o)| {
                let obtype_index = *obtype_to_index.get(&o.object_type).unwrap();
                let ob_index = ObjectIndex(o_i);
                objects_per_type[obtype_index].push(ob_index);
                let ob_type = &ocel.object_types[obtype_index];
                SlimOCELObject {
                    id: o.id,
                    object_type: obtype_index,
                    attributes: ob_type
                        .attributes
                        .iter()
                        .map(|a| {
                            o.attributes
                                .iter()
                                .filter(|ea| ea.name == a.name)
                                .map(|ea| (ea.time, ea.value.clone()))
                                .collect()
                        })
                        .collect(),
                    relationships: o
                        .relationships
                        .into_iter()
                        .filter_map(|rel| {
                            // Side effect: We also insert the reverse relation here!
                            // The filter_map and ? here prevent invalid O2O/E2O references :(
                            let rel_obj_id = object_ids_to_index.get(&rel.object_id)?;
                            o2o_rel_rev[rel_obj_id.into_inner()][obtype_index].push(ob_index);
                            Some((rel.qualifier, *rel_obj_id))
                        })
                        // These are sorted!
                        // In particular, this allows more efficient binary search for checking if an element is related
                        .sorted_unstable_by_key(|(_q, e)| *e)
                        .collect(),
                }
            })
            .collect();
        Self {
            events,
            objects,
            event_types: ocel.event_types,
            object_types: ocel.object_types,
            object_ids_to_index,
            event_ids_to_index,
            events_per_type,
            objects_per_type,
            e2o_rel_rev,
            o2o_rel_rev,
            evtype_to_index,
            obtype_to_index,
        }
    }

    /// Get all events of the specified event type
    pub fn get_evs_of_type<'a>(&'a self, event_type: &str) -> impl Iterator<Item = &'a EventIndex> {
        self.evtype_to_index
            .get(event_type)
            .into_iter()
            .flat_map(|et| &self.events_per_type[*et])
    }
    /// Get all objects of the specified object type
    fn get_obs_of_type<'a>(&'a self, object_type: &str) -> impl Iterator<Item = &'a ObjectIndex> {
        self.obtype_to_index
            .get(object_type)
            .into_iter()
            .flat_map(|et| &self.objects_per_type[*et])
    }

    /// Get all object types as strings
    fn get_ob_types(&self) -> impl Iterator<Item = &String> {
        self.object_types.iter().map(|ot| &ot.name)
    }
    /// Get all event types as strings
    fn get_ev_types(&self) -> impl Iterator<Item = &String> {
        self.event_types.iter().map(|et| &et.name)
    }

    /// Add a new event type to the OCEL, with the specified attributes
    pub fn add_event_type(&mut self, event_type: &str, attributes: Vec<OCELTypeAttribute>) {
        if self.evtype_to_index.contains_key(event_type) {
            return;
        }
        let new_index = self.event_types.len();
        self.evtype_to_index
            .insert(event_type.to_string(), new_index);
        self.events_per_type.push(Vec::new());
        self.event_types.push(OCELType {
            name: event_type.to_string(),
            attributes,
        });
        self.e2o_rel_rev.iter_mut().for_each(|x| x.push(Vec::new()));
    }
    /// Add a new object type to the OCEL, with the specified attributes
    pub fn add_object_type(&mut self, object_type: &str, attributes: Vec<OCELTypeAttribute>) {
        if self.obtype_to_index.contains_key(object_type) {
            return;
        }
        let new_index = self.object_types.len();
        self.obtype_to_index
            .insert(object_type.to_string(), new_index);
        self.objects_per_type.push(Vec::new());
        self.object_types.push(OCELType {
            name: object_type.to_string(),
            attributes,
        });
        self.o2o_rel_rev.iter_mut().for_each(|x| x.push(Vec::new()));
    }

    /// Add a new event to the OCEL
    ///
    /// The attribute order must match the attributes defined on the corresponding event type.
    /// E.g., if `price` and `weight` are defined as event attributes on the event type
    /// (in that order), the first element corresponds to the `price` and the second
    /// element to the `weight`.
    ///
    /// Returns the newly added [`EventIndex`]
    /// or None if the event type is unknown or the id is already taken
    ///
    ///
    /// Note: This function maintains the relationship index (e.g., also reverse E2O relationships)
    ///
    pub fn add_event(
        &mut self,
        event_type: &str,
        time: DateTime<FixedOffset>,
        id: Option<String>,
        mut attributes: Vec<OCELAttributeValue>,
        mut relationships: Vec<(String, ObjectIndex)>,
    ) -> Option<EventIndex> {
        let etype = self.evtype_to_index.get(event_type)?;
        let id = id.unwrap_or_else(|| Uuid::new_v4().to_string());
        if self.event_ids_to_index.contains_key(&id) {
            return None;
        }
        let new_ev_index = EventIndex(self.events.len());
        self.event_ids_to_index.insert(id.clone(), new_ev_index);
        self.events_per_type.get_mut(*etype)?.push(new_ev_index);
        // Relationships should be sorted
        relationships.sort_by_key(|(_q, o)| *o);
        for (_q, o) in &relationships {
            // Special case: As the event is newly appended and thus currently has the highest index, we know that when added to the end, the E2O-rev list is still sorted
            self.e2o_rel_rev[o.0][*etype].push(new_ev_index);
        }
        // Pad (or truncate) attributes to expected length; warn on mismatch
        let expected_attr_len = self.event_types[*etype].attributes.len();
        if attributes.len() != expected_attr_len {
            eprintln!(
                "[rust4pm] warning: event_type '{}' expects {} attribute value(s), got {}. \
                 Padding with Null / truncating. Ensure attribute order matches `add_event_type`.",
                event_type,
                expected_attr_len,
                attributes.len()
            );
        }
        attributes.resize_with(expected_attr_len, || OCELAttributeValue::Null);

        self.events.push(SlimOCELEvent {
            id,
            event_type: *etype,
            time,
            attributes,
            relationships,
        });
        Some(new_ev_index)
    }
    /// Add a new object to the OCEL
    ///
    /// The attribute order must match the attributes defined on the corresponding object type.
    /// E.g., if the object type has attributes `price` and `weight` (in that order),
    /// the first attribute array should contain all `price` attribute values
    /// (with their timestamps) and the second array should contain all `weight`
    /// attribute values (with their timestamps)
    ///
    /// Returns the newly added [`ObjectIndex`]
    /// or None if the object type is unknown or the id is already taken
    ///
    /// Note: This function maintains the relationship index (e.g., also reverse O2O relationships)
    pub fn add_object(
        &mut self,
        object_type: &str,
        id: Option<String>,
        mut attributes: Vec<Vec<(DateTime<FixedOffset>, OCELAttributeValue)>>,
        mut relationships: Vec<(String, ObjectIndex)>,
    ) -> Option<ObjectIndex> {
        let otype = self.obtype_to_index.get(object_type)?;
        let id = id.unwrap_or_else(|| Uuid::new_v4().to_string());
        if self.object_ids_to_index.contains_key(&id) {
            return None;
        }
        let new_ob_index = ObjectIndex(self.objects.len());
        self.e2o_rel_rev
            .push(vec![Vec::new(); self.events_per_type.len()]);
        self.o2o_rel_rev
            .push(vec![Vec::new(); self.objects_per_type.len()]);
        self.object_ids_to_index.insert(id.clone(), new_ob_index);
        self.objects_per_type.get_mut(*otype)?.push(new_ob_index);
        // Relationships should be sorted
        relationships.sort_by_key(|(_q, o)| *o);
        for (_q, o) in &relationships {
            // Special case: As the object is newly appended and thus currently has the highest index, we know that when added to the end, the O2O-rev list is still sorted
            self.o2o_rel_rev[o.0][*otype].push(new_ob_index);
        }
        // Pad (or truncate) attributes to expected length; warn on mismatch
        let expected_attr_len = self.object_types[*otype].attributes.len();
        if attributes.len() != expected_attr_len {
            eprintln!(
                "[rust4pm] warning: object_type '{}' expects {} attribute list(s), got {}. \
                 Padding with empty / truncating. Ensure attribute order matches `add_object_type`.",
                object_type,
                expected_attr_len,
                attributes.len()
            );
        }
        attributes.resize_with(expected_attr_len, Vec::new);

        self.objects.push(SlimOCELObject {
            id,
            object_type: *otype,
            attributes,
            relationships,
        });
        Some(new_ob_index)
    }
    /// Add an E2O relationship between the passed event and object, with the specified qualifier.
    ///
    /// Multiple relationships between the same `(event, object)` pair are allowed as long as their
    /// qualifiers differ; re-adding an exact `(event, object, qualifier)` triple is a no-op.
    ///
    /// Returns `true` on success, `false` if either index is out of bounds (with a stderr warning).
    pub fn add_e2o(&mut self, event: EventIndex, object: ObjectIndex, qualifier: String) -> bool {
        if event.0 >= self.events.len() || object.0 >= self.objects.len() {
            eprintln!(
                "[rust4pm] warning: add_e2o called with invalid index(es) (event={}, object={}); ignored",
                event.0, object.0
            );
            return false;
        }
        let evtype_index = self.events[event.0].event_type;
        sorted_insert(&mut self.e2o_rel_rev[object.0][evtype_index], event, |x| *x);
        let rels = &mut self.events[event.0].relationships;
        if !rels.iter().any(|(q, o)| o == &object && q == &qualifier) {
            let insert_pos = rels.partition_point(|(_q, o)| o < &object);
            rels.insert(insert_pos, (qualifier, object));
        }
        true
    }
    /// Add an O2O relationship from `from_obj` to `to_obj`, with the specified qualifier.
    ///
    /// Multiple relationships between the same `(from_obj, to_obj)` pair are allowed as long as
    /// their qualifiers differ; re-adding an exact `(from_obj, to_obj, qualifier)` triple is a no-op.
    ///
    /// Returns `true` on success, `false` if either index is out of bounds (with a stderr warning).
    pub fn add_o2o(
        &mut self,
        from_obj: ObjectIndex,
        to_obj: ObjectIndex,
        qualifier: String,
    ) -> bool {
        if from_obj.0 >= self.objects.len() || to_obj.0 >= self.objects.len() {
            eprintln!(
                "[rust4pm] warning: add_o2o called with invalid index(es) (from_obj={}, to_obj={}); ignored",
                from_obj.0, to_obj.0
            );
            return false;
        }
        let from_obj_type_index = self.objects[from_obj.0].object_type;
        sorted_insert(
            &mut self.o2o_rel_rev[to_obj.0][from_obj_type_index],
            from_obj,
            |x| *x,
        );
        let rels = &mut self.objects[from_obj.0].relationships;
        if !rels.iter().any(|(q, o)| o == &to_obj && q == &qualifier) {
            let insert_pos = rels.partition_point(|(_q, o)| o < &to_obj);
            rels.insert(insert_pos, (qualifier, to_obj));
        }
        true
    }
    /// Remove all E2O relationships between the passed event and object (across every qualifier).
    ///
    /// Returns `true` on success, `false` if either index is out of bounds (with a stderr warning).
    pub fn delete_e2o(&mut self, event: &EventIndex, object: &ObjectIndex) -> bool {
        if event.0 >= self.events.len() || object.0 >= self.objects.len() {
            eprintln!(
                "[rust4pm] warning: delete_e2o called with invalid index(es) (event={}, object={}); ignored",
                event.0, object.0
            );
            return false;
        }
        let evtype_index = self.events[event.0].event_type;
        self.e2o_rel_rev[object.0][evtype_index].retain(|e| e != event);
        self.events[event.0]
            .relationships
            .retain(|(_q, o)| o != object);
        true
    }
    /// Remove all O2O relationships from `from_obj` to `to_obj` (across every qualifier).
    ///
    /// Returns `true` on success, `false` if either index is out of bounds (with a stderr warning).
    pub fn delete_o2o(&mut self, from_obj: &ObjectIndex, to_obj: &ObjectIndex) -> bool {
        if from_obj.0 >= self.objects.len() || to_obj.0 >= self.objects.len() {
            eprintln!(
                "[rust4pm] warning: delete_o2o called with invalid index(es) (from_obj={}, to_obj={}); ignored",
                from_obj.0, to_obj.0
            );
            return false;
        }
        let from_obj_type = self.objects[from_obj.0].object_type;
        self.o2o_rel_rev[to_obj.0][from_obj_type].retain(|e| e != from_obj);
        self.objects[from_obj.0]
            .relationships
            .retain(|(_q, o)| o != to_obj);
        true
    }
}
impl From<OCEL> for SlimLinkedOCEL {
    fn from(value: OCEL) -> Self {
        Self::from_ocel(value)
    }
}

/// A slim version of an OCEL Event
///
/// Some fields (i.e., `event_type` and relationships) are modified for easier and memory-efficient usage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlimOCELEvent {
    /// Event ID
    pub id: String,
    /// Event Type (referring back to the `name` of an [`OCELType`])
    #[serde(rename = "type")]
    pub event_type: usize,
    /// `DateTime` when event occured
    pub time: DateTime<FixedOffset>,
    /// Event attributes
    #[serde(default)]
    pub attributes: Vec<OCELAttributeValue>,
    /// E2O (Event-to-Object) relationships
    #[serde(default)]
    pub relationships: Vec<(String, ObjectIndex)>,
}
/// A slim version of an OCEL Object
///
/// Some fields (i.e., `object_type` and relationships) are modified for easier and memory-efficient usage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlimOCELObject {
    /// Object ID
    pub id: String,
    /// Object Type (referring back to the `name` of an [`OCELType`])
    #[serde(rename = "type")]
    pub object_type: usize,
    /// Object attributes (each inner [`Vec`] holds the time-indexed values for one declared attribute)
    #[serde(default)]
    pub attributes: Vec<Vec<(DateTime<FixedOffset>, OCELAttributeValue)>>,
    /// O2O (Object-to-Object) relationships
    #[serde(default)]
    pub relationships: Vec<(String, ObjectIndex)>,
}

impl<'a> LinkedOCELAccess<'a> for SlimLinkedOCEL {
    type EventRepr = EventIndex;

    type ObjectRepr = ObjectIndex;

    fn get_evs_of_type(&'a self, ev_type: &'_ str) -> impl Iterator<Item = &'a Self::EventRepr> {
        self.get_evs_of_type(ev_type)
    }

    fn get_obs_of_type(&'a self, ob_type: &'_ str) -> impl Iterator<Item = &'a Self::ObjectRepr> {
        self.get_obs_of_type(ob_type)
    }

    fn get_ev_types(&'a self) -> impl Iterator<Item = &'a str> {
        self.get_ev_types().map(String::as_str)
    }

    fn get_ob_types(&'a self) -> impl Iterator<Item = &'a str> {
        self.get_ob_types().map(String::as_str)
    }

    fn get_all_evs(&'a self) -> impl Iterator<Item = Self::EventRepr> {
        (0..self.events.len()).map(EventIndex)
    }

    fn get_all_obs(&'a self) -> impl Iterator<Item = Self::ObjectRepr> {
        (0..self.objects.len()).map(ObjectIndex)
    }

    fn get_full_ev(&'a self, index: impl Borrow<Self::EventRepr>) -> Cow<'a, OCELEvent> {
        Cow::Owned(index.borrow().fat_ev(self))
    }

    fn get_full_ob(&'a self, index: impl Borrow<Self::ObjectRepr>) -> Cow<'a, OCELObject> {
        Cow::Owned(index.borrow().fat_ob(self))
    }

    fn get_e2o(
        &'a self,
        index: impl Borrow<Self::EventRepr>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)> {
        self.events[index.borrow().0]
            .relationships
            .iter()
            .map(|(q, o_idx)| (q.as_str(), o_idx))
    }

    fn get_e2o_rev(
        &'a self,
        index: impl Borrow<Self::ObjectRepr>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::EventRepr)> {
        // `relationships` is sorted by object index; could use partition_point if lists grow large.
        let target = *index.borrow();
        index.borrow().get_e2o_rev(self).flat_map(move |e| {
            self.events[e.0]
                .relationships
                .iter()
                .filter(move |(_q, o)| *o == target)
                .map(move |(q, _)| (q.as_str(), e))
        })
    }

    fn get_o2o(
        &'a self,
        index: impl Borrow<Self::ObjectRepr>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)> {
        self.objects[index.borrow().0]
            .relationships
            .iter()
            .map(|(q, o_idx)| (q.as_str(), o_idx))
    }

    fn get_o2o_rev(
        &'a self,
        index: impl Borrow<Self::ObjectRepr>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)> {
        let target = *index.borrow();
        index.borrow().get_o2o_rev(self).flat_map(move |o1| {
            self.objects[o1.0]
                .relationships
                .iter()
                .filter(move |(_q, o2)| *o2 == target)
                .map(move |(q, _)| (q.as_str(), o1))
        })
    }

    fn get_ev_type(&'a self, ev_type: impl AsRef<str>) -> Option<&'a OCELType> {
        self.event_types
            .iter()
            .find(|et| et.name == ev_type.as_ref())
    }

    fn get_ob_type(&'a self, ob_type: impl AsRef<str>) -> Option<&'a OCELType> {
        self.object_types
            .iter()
            .find(|ot| ot.name == ob_type.as_ref())
    }

    fn get_ob_type_of(&'a self, object: impl Borrow<Self::ObjectRepr>) -> &'a str {
        object.borrow().get_ob_type(self)
    }

    fn get_ev_type_of(&'a self, event: impl Borrow<Self::EventRepr>) -> &'a str {
        event.borrow().get_ev_type(self)
    }

    fn get_ev_attrs(&'a self, ev: impl Borrow<Self::EventRepr>) -> impl Iterator<Item = &'a str> {
        self.events
            .get(ev.borrow().0)
            .and_then(|e| self.event_types.get(e.event_type))
            .iter()
            .flat_map(|et| &et.attributes)
            .map(|a| a.name.as_str())
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn get_ev_attr_val(
        &'a self,
        ev: impl Borrow<Self::EventRepr>,
        attr_name: impl AsRef<str>,
    ) -> Option<&'a OCELAttributeValue> {
        ev.borrow().get_attribute_value(attr_name.as_ref(), self)
    }

    fn get_ob_attrs(&'a self, ob: impl Borrow<Self::ObjectRepr>) -> impl Iterator<Item = &'a str> {
        self.objects
            .get(ob.borrow().0)
            .and_then(|o| self.object_types.get(o.object_type))
            .iter()
            .flat_map(|et| &et.attributes)
            .map(|a| a.name.as_str())
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn get_ob_attr_vals(
        &'a self,
        ob: impl Borrow<Self::ObjectRepr>,
        attr_name: impl AsRef<str>,
    ) -> impl Iterator<Item = (&'a DateTime<FixedOffset>, &'a OCELAttributeValue)> {
        ob.borrow()
            .get_attribute_value(attr_name.as_ref(), self)
            .into_iter()
            .flat_map(|x| x.iter().map(|(a, b)| (a, b)))
    }

    fn get_ob_id(&'a self, ob: impl Borrow<Self::ObjectRepr>) -> &'a str {
        self.objects[ob.borrow().0].id.as_str()
    }

    fn get_ev_id(&'a self, ev: impl Borrow<Self::EventRepr>) -> &'a str {
        self.events[ev.borrow().0].id.as_str()
    }

    fn get_ev_by_id(&'a self, ev_id: impl AsRef<str>) -> Option<Self::EventRepr> {
        self.event_ids_to_index.get(ev_id.as_ref()).copied()
    }

    fn get_ob_by_id(&'a self, ob_id: impl AsRef<str>) -> Option<Self::ObjectRepr> {
        self.object_ids_to_index.get(ob_id.as_ref()).copied()
    }

    fn get_ev_time(&'a self, ev: impl Borrow<Self::EventRepr>) -> &'a DateTime<FixedOffset> {
        ev.borrow().get_time(self)
    }

    fn get_e2o_of_type(
        &'a self,
        index: impl Borrow<Self::EventRepr>,
        ob_type: impl AsRef<str>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)> {
        let ob_type_index = self.obtype_to_index.get(ob_type.as_ref());
        ob_type_index.into_iter().flat_map(move |ot_index| {
            self.events[index.borrow().0]
                .relationships
                .iter()
                .filter(move |(_q, o)| &o.get_ob(self).object_type == ot_index)
                .map(|(q, o)| (q.as_str(), o))
        })
    }
    fn get_o2o_of_type(
        &'a self,
        index: impl Borrow<Self::ObjectRepr>,
        ob_type: impl AsRef<str>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)> {
        let ob_type_index = self.obtype_to_index.get(ob_type.as_ref());
        ob_type_index.into_iter().flat_map(move |ot_index| {
            self.objects[index.borrow().0]
                .relationships
                .iter()
                .filter(move |(_q, o)| &o.get_ob(self).object_type == ot_index)
                .map(|(q, o)| (q.as_str(), o))
        })
    }

    fn get_e2o_rev_of_type(
        &'a self,
        index: impl Borrow<Self::ObjectRepr>,
        ev_type: impl AsRef<str>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::EventRepr)> {
        let evtype_index = self.evtype_to_index.get(ev_type.as_ref()).copied();
        let ob_idx = index.borrow().0;
        let target = *index.borrow();
        evtype_index.into_iter().flat_map(move |ei| {
            self.e2o_rel_rev
                .get(ob_idx)
                .into_iter()
                .flat_map(move |x| x.get(ei))
                .flatten()
                .flat_map(move |e| {
                    e.get_ev(self)
                        .relationships
                        .iter()
                        .filter(move |(_q, o)| *o == target)
                        .map(move |(q, _)| (q.as_str(), e))
                })
        })
    }
    fn get_o2o_rev_of_type(
        &'a self,
        to_obj: impl Borrow<Self::ObjectRepr>,
        from_ob_type: impl AsRef<str>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)> {
        let obtype_index = self.obtype_to_index.get(from_ob_type.as_ref()).copied();
        let ob_idx = to_obj.borrow().0;
        let target = *to_obj.borrow();
        obtype_index.into_iter().flat_map(move |oi| {
            self.o2o_rel_rev
                .get(ob_idx)
                .into_iter()
                .flat_map(move |x| x.get(oi))
                .flatten()
                .flat_map(move |o| {
                    o.get_ob(self)
                        .relationships
                        .iter()
                        .filter(move |(_q, e)| *e == target)
                        .map(move |(q, _)| (q.as_str(), o))
                })
        })
    }
}

impl Importable for SlimLinkedOCEL {
    type Error = OCELIOError;
    type ImportOptions = ();

    fn import_from_reader_with_options<R: Read>(
        reader: R,
        format: &str,
        _: Self::ImportOptions,
    ) -> Result<Self, Self::Error> {
        let ocel = OCEL::import_from_reader(reader, format)?;
        Ok(SlimLinkedOCEL::from_ocel(ocel))
    }

    fn infer_format(path: &Path) -> Option<String> {
        <OCEL as Importable>::infer_format(path)
    }

    fn known_import_formats() -> Vec<crate::core::io::ExtensionWithMime> {
        <OCEL as Importable>::known_import_formats()
    }
}

impl Exportable for SlimLinkedOCEL {
    type Error = OCELIOError;
    type ExportOptions = ();

    fn export_to_writer_with_options<W: Write>(
        &self,
        writer: W,
        format: &str,
        _: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        self.construct_ocel().export_to_writer(writer, format)
    }

    fn known_export_formats() -> Vec<ExtensionWithMime> {
        <OCEL as Exportable>::known_export_formats()
    }
}
