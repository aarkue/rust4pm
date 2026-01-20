//! Linked Slim (i.e., less duplicate fields) OCEL
//!
//! Allows easy and efficient access to events, objects, and their relations
use std::{
    borrow::{Borrow, Cow},
    collections::HashMap,
};

use binding_macros::RegistryEntity;
use chrono::{DateTime, FixedOffset};
use itertools::Itertools;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::core::{
    event_data::object_centric::{
        linked_ocel::LinkedOCELAccess, OCELAttributeValue, OCELEvent, OCELEventAttribute,
        OCELObject, OCELObjectAttribute, OCELRelationship, OCELType,
    },
    OCEL,
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
        unsafe {
            &locel
                .event_types
                .get_unchecked(locel.events.get_unchecked(self.0).event_type)
                .name
        }
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
    /// Get a mutuable reference to the attribute value of this event, specified by the attribute name
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
    /// Get 'fat' version of Event (i.e., with all fields expanded, with a structure similiar to the OCEL 2.0 specification)
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
                .map(|(i, at)| {
                    let val = &sev.attributes[i];
                    OCELEventAttribute {
                        name: at.name.clone(),
                        value: val.clone(),
                    }
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
    pub fn get_e2o_rev_of_evtype<'a>(
        &self,
        locel: &'a SlimLinkedOCEL,
        evtype: &'a str,
    ) -> impl Iterator<Item = &'a EventIndex> + use<'a> {
        let evtype_index = locel.evtype_to_index.get(evtype).unwrap();
        locel
            .e2o_rel_rev
            .get(self.0)
            .into_iter()
            .flat_map(|x| x.get(*evtype_index))
            .flatten()
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
                    sev.attributes[i].iter().map(|(t, v)| OCELObjectAttribute {
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
#[derive(Debug, Clone, Serialize, Deserialize, RegistryEntity)]
/// A slim and linked version of OCEL that allows for convenient usage
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
                    event_type: *evtype_to_index.get(&e.event_type).unwrap(),
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

    /// Get all events of the spcecified event type
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
    /// Add a new event to the OCEL
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
        attributes: Vec<OCELAttributeValue>,
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
    /// Returns the newly added [`ObjectIndex`]
    /// or None if the object type is unknown or the id is already taken
    ///
    /// Note: This function maintains the relationship index (e.g., also reverse O2O relationships)
    pub fn add_object(
        &mut self,
        object_type: &str,
        id: Option<String>,
        attributes: Vec<Vec<(DateTime<FixedOffset>, OCELAttributeValue)>>,
        mut relationships: Vec<(String, ObjectIndex)>,
    ) -> Option<ObjectIndex> {
        let otype = self.obtype_to_index.get(object_type)?;
        let id = id.unwrap_or_else(|| Uuid::new_v4().to_string());
        if self.object_ids_to_index.contains_key(&id) {
            return None;
        }
        let new_ob_index = ObjectIndex(self.objects.len());
        self.object_ids_to_index.insert(id.clone(), new_ob_index);
        self.objects_per_type.get_mut(*otype)?.push(new_ob_index);
        // Relationships should be sorted
        relationships.sort_by_key(|(_q, o)| *o);
        for (_q, o) in &relationships {
            // Special case: As the object is newly appended and thus currently has the highest index, we know that when added to the end, the E2O-rev list is still sorted
            self.o2o_rel_rev[o.0][*otype].push(new_ob_index);
        }
        self.objects.push(SlimOCELObject {
            id,
            object_type: *otype,
            attributes,
            relationships,
        });
        Some(new_ob_index)
    }
    /// Add an E2O relationship between the passed event and object, with the specified qualifier
    pub fn add_e2o(&mut self, event: EventIndex, object: ObjectIndex, qualifier: String) {
        let evtype_index = event.get_ev(self).event_type;
        sorted_insert(&mut self.e2o_rel_rev[object.0][evtype_index], event, |x| *x);
        sorted_insert(
            &mut self.events[event.0].relationships,
            (qualifier, object),
            |(_q, o)| *o,
        );
    }
    /// Add an O2O relationship between the passed objects, with the specified qualifier
    pub fn add_o2o(&mut self, from_obj: ObjectIndex, to_obj: ObjectIndex, qualifier: String) {
        let from_obj_type_index = from_obj.get_ob(self).object_type;
        sorted_insert(
            &mut self.o2o_rel_rev[to_obj.0][from_obj_type_index],
            from_obj,
            |x| *x,
        );
        sorted_insert(
            &mut self.objects[from_obj.0].relationships,
            (qualifier, to_obj),
            |(_q, o)| *o,
        );
    }
    /// Remove the E2O relationship between the passed event and object from the `LinkedOCEL`
    pub fn delete_e2o(&mut self, event: &EventIndex, object: &ObjectIndex) {
        let evtype_index = event.get_ev(self).event_type;
        self.e2o_rel_rev[object.0][evtype_index].retain(|e| e != event);
        self.events[event.0]
            .relationships
            .retain(|(_q, o)| o != object);
    }
    /// Remove the O2O relationship between the passed objects from the `LinkedOCEL`
    pub fn delete_o2o(&mut self, from_obj: &ObjectIndex, to_obj: &ObjectIndex) {
        let from_obj_type = from_obj.get_ob(self).object_type;
        self.o2o_rel_rev[to_obj.0][from_obj_type].retain(|e| e != from_obj);
        self.objects[from_obj.0]
            .relationships
            .retain(|(_q, o)| o != to_obj);
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
    /// Event ID
    pub id: String,
    /// Event Type (referring back to the `name` of an [`OCELType`])
    #[serde(rename = "type")]
    pub object_type: usize,
    /// Event attributes
    #[serde(default)]
    pub attributes: Vec<Vec<(DateTime<FixedOffset>, OCELAttributeValue)>>,
    /// E2O (Event-to-Object) relationships
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
        index.borrow().get_e2o_rev(self).flat_map(move |e| {
            self.events[e.0]
                .relationships
                .iter()
                .find(|(_q, o)| o == index.borrow())
                .map(|(q, _0)| (q.as_str(), e))
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
        index.borrow().get_o2o_rev(self).flat_map(move |o1| {
            self.objects[o1.0]
                .relationships
                .iter()
                .find(|(_q, o2)| o2 == index.borrow())
                .map(|(q, _o2)| (q.as_str(), o1))
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
        let evtype_index = self.evtype_to_index.get(ev_type.as_ref()).unwrap();
        self.e2o_rel_rev
            .get(index.borrow().0)
            .into_iter()
            .flat_map(|x| x.get(*evtype_index))
            .flatten()
            .filter_map(move |e| {
                let rels = &e.get_ev(self).relationships;
                if let Ok(rel_index) = rels.binary_search_by_key(index.borrow(), |(_q, o)| *o) {
                    Some((rels[rel_index].0.as_ref(), e))
                } else {
                    None
                }
            })
    }
    fn get_o2o_rev_of_type(
        &'a self,
        to_obj: impl Borrow<Self::ObjectRepr>,
        from_ob_type: impl AsRef<str>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)> {
        let obtype_index = self.obtype_to_index.get(from_ob_type.as_ref()).unwrap();
        self.o2o_rel_rev
            .get(to_obj.borrow().0)
            .into_iter()
            .flat_map(|x| x.get(*obtype_index))
            .flatten()
            .filter_map(move |o| {
                let rels = &o.get_ob(self).relationships;
                if let Ok(rel_index) = rels.binary_search_by_key(to_obj.borrow(), |(_q, e)| *e) {
                    Some((rels[rel_index].0.as_ref(), o))
                } else {
                    None
                }
            })
    }
}
