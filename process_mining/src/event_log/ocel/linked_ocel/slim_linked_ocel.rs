use std::collections::HashMap;

use chrono::{DateTime, FixedOffset};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    ocel::ocel_struct::{
        OCELAttributeValue, OCELEvent, OCELEventAttribute, OCELRelationship, OCELType,
    },
    OCEL,
};

/// An Event Index
///
/// Points to an event in the context of a given OCEL
#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug, PartialOrd, Ord, Serialize, Deserialize)]
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
            .flat_map(|e| e.relationships.iter().map(|(o, _q)| o))
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
    pub fn fat_ev<'a>(&self, locel: &'a SlimLinkedOCEL) -> OCELEvent {
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
                .map(|(o, q)| OCELRelationship {
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

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug, PartialOrd, Ord, Serialize, Deserialize)]
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
            .map(|o| &o.relationships)
            .flatten()
            .map(|(o, _q)| o)
        // .copied()
    }
    /// Get reverse O2O relationships
    pub fn get_o2o_rev<'a>(
        &self,
        locel: &'a SlimLinkedOCEL,
    ) -> impl Iterator<Item = &'a ObjectIndex> + use<'a> {
        locel.o2o_rel_rev.get(self.0).into_iter().flatten()
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
        // .filter(|e| unsafe { locel.events.get_unchecked(e.0).event_type == *evtype_index })
        // .copied()
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
#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug, PartialOrd, Ord, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
/// A slim and linked version of OCEL that allows for convenient usage
pub struct SlimLinkedOCEL {
    /// Events
    pub events: Vec<SlimOCELEvent>,
    /// Objects
    pub objects: Vec<SlimOCELObject>,
    /// Event types (Activities)
    pub event_types: Vec<OCELType>,
    /// Object types
    pub object_types: Vec<OCELType>,
    event_ids_to_index: HashMap<String, EventIndex>,
    object_ids_to_index: HashMap<String, ObjectIndex>,
    /// Events per Event Type
    events_per_type: Vec<Vec<EventIndex>>,
    /// List of object indices per object type
    objects_per_type: Vec<Vec<ObjectIndex>>,
    /// Reverse E2O relationships
    /// Split by event type (i.e., first level: object index -> evtype index -> List of events)
    e2o_rel_rev: Vec<Vec<Vec<EventIndex>>>,
    /// Reverse O2O Relationships
    o2o_rel_rev: Vec<Vec<ObjectIndex>>,
    // TODO: Change to EventTypeIndex type!
    evtype_to_index: HashMap<String, usize>,
    // TODO: Same
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
        let mut o2o_rel_rev: Vec<Vec<ObjectIndex>> = vec![Vec::new(); ocel.objects.len()];
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
                            Some((*rel_obj_id, rel.qualifier))
                        })
                        // These are sorted!
                        // In particular, this allows more efficient binary search for checking if an element is related
                        .sorted_unstable_by_key(|(o, _q)| *o)
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
                                .map(|ea| (ea.time.clone(), ea.value.clone()))
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
                            o2o_rel_rev[rel_obj_id.into_inner()].push(ob_index);
                            Some((*rel_obj_id, rel.qualifier))
                        })
                        .collect(),
                }
            })
            .collect();
        Self {
            events: events,
            objects: objects,
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

    /// Get all event indices
    pub fn get_all_evs(&self) -> impl Iterator<Item = EventIndex> {
        (0..self.events.len()).into_iter().map(|i| EventIndex(i))
    }
    /// Get all events of the spcecified event type
    pub fn get_evs_of_type<'a>(
        &'a self,
        event_type: &'a str,
    ) -> impl Iterator<Item = &'a EventIndex> + use<'a> {
        self.evtype_to_index
            .get(event_type)
            .into_iter()
            .flat_map(|et| &self.events_per_type[*et])
        // .copied()
    }
    /// Get all objects of the specified object type
    pub fn get_obs_of_type<'a>(
        &'a self,
        object_type: &'a str,
    ) -> impl Iterator<Item = &'a ObjectIndex> + use<'a> {
        self.obtype_to_index
            .get(object_type)
            .into_iter()
            .flat_map(|et| &self.objects_per_type[*et])
        // .copied()
    }

    /// Get all object types as strings
    pub fn get_ob_types<'a>(&'a self) -> impl Iterator<Item = &'a String> {
        self.object_types.iter().map(|ot| &ot.name)
    }
    /// Get the type struct for an object type
    pub fn get_ob_type<'a>(&'a self, ob_type: &'a str) -> &'a OCELType {
        &self.object_types[*self.obtype_to_index.get(ob_type).unwrap()]
    }
    /// Get all event types as strings
    pub fn get_ev_types<'a>(&'a self) -> impl Iterator<Item = &'a String> {
        self.event_types.iter().map(|et| &et.name)
    }
    /// Get the type struct for an event type
    pub fn get_ev_type<'a>(&'a self, ev_type: &'a str) -> &'a OCELType {
        &self.event_types[*self.evtype_to_index.get(ev_type).unwrap()]
    }
    /// Add a new event to the OCEL
    ///
    /// Returns the newly added [`EventIndex`]
    /// or None if the event type is unknown or the id is already taken
    ///
    pub fn add_event<'a>(
        &mut self,
        event_type: &'a str,
        time: DateTime<FixedOffset>,
        id: Option<String>,
        attributes: Vec<OCELAttributeValue>,
        relationships: Vec<(ObjectIndex, String)>,
    ) -> Option<EventIndex> {
        let etype = self.evtype_to_index.get(event_type)?;
        let id = id.unwrap_or_else(|| Uuid::new_v4().to_string());
        if self.event_ids_to_index.contains_key(&id) {
            return None;
        }
        self.events.push(SlimOCELEvent {
            id,
            event_type: *etype,
            time: time,
            attributes,
            relationships,
        });
        Some(EventIndex(self.events.len()))
    }
    /// Add a new object to the OCEL
    ///
    /// Returns the newly added [`ObjectIndex`]
    /// or None if the object type is unknown or the id is already taken
    ///
    pub fn add_object<'a>(
        &mut self,
        object_type: &'a str,
        id: Option<String>,
        attributes: Vec<Vec<(DateTime<FixedOffset>, OCELAttributeValue)>>,
        relationships: Vec<(ObjectIndex, String)>,
    ) -> Option<ObjectIndex> {
        let otype = self.obtype_to_index.get(object_type)?;
        let id = id.unwrap_or_else(|| Uuid::new_v4().to_string());
        if self.object_ids_to_index.contains_key(&id) {
            return None;
        }
        self.objects.push(SlimOCELObject {
            id,
            object_type: *otype,
            attributes,
            relationships,
        });
        Some(ObjectIndex(self.events.len()))
    }
    /// Add an E2O relationship between the passed event and object, with the specified qualifier
    pub fn add_e2o<'a>(&'a mut self, event: EventIndex, object: ObjectIndex, qualifier: String) {
        let evtype_index = event.get_ev(&self).event_type;
        self.e2o_rel_rev[object.0][evtype_index].push(event);
        self.events[event.0].relationships.push((object, qualifier));
    }
    /// Add an O2O relationship between the passed objects, with the specified qualifier
    pub fn add_o2o<'a>(
        &'a mut self,
        from_obj: ObjectIndex,
        to_obj: ObjectIndex,
        qualifier: String,
    ) {
        self.o2o_rel_rev[to_obj.0].push(from_obj);
        self.objects[from_obj.0]
            .relationships
            .push((to_obj, qualifier));
    }
    /// Remove the E2O relationship between the passed event and object from the LinkedOCEL
    pub fn delete_e2o<'a>(&'a mut self, event: &EventIndex, object: &ObjectIndex) {
        let evtype_index = event.get_ev(&self).event_type;
        self.e2o_rel_rev[object.0][evtype_index].retain(|e| e != event);
        self.events[event.0]
            .relationships
            .retain(|(o, _q)| o != object);
    }
    /// Remove the O2O relationship between the passed objects from the LinkedOCEL
    pub fn delete_o2o<'a>(&'a mut self, from_obj: &ObjectIndex, to_obj: &ObjectIndex) {
        self.o2o_rel_rev[to_obj.0].retain(|e| e != from_obj);
        self.objects[from_obj.0]
            .relationships
            .retain(|(o, _q)| o != to_obj);
    }
}
impl From<OCEL> for SlimLinkedOCEL {
    fn from(value: OCEL) -> Self {
        Self::from_ocel(value)
    }
}

/// A slim version of an OCEL Event
///
/// Some fields (i.e., event_type and relationships) are modified for easier and memory-efficient usage
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
    pub relationships: Vec<(ObjectIndex, String)>,
}
/// A slim version of an OCEL Object
///
/// Some fields (i.e., object_type and relationships) are modified for easier and memory-efficient usage
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
    pub relationships: Vec<(ObjectIndex, String)>,
}
