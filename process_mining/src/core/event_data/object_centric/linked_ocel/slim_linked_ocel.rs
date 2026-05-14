//! Linked Slim (i.e., less duplicate fields) OCEL
//!
//! Allows easy and efficient access to events, objects, and their relations
use std::{
    borrow::{Borrow, Cow},
    collections::HashMap,
    hash::BuildHasher,
    io::{Read, Write},
    path::Path,
};

use chrono::{DateTime, FixedOffset};
use hashbrown::{DefaultHashBuilder, HashTable};
use macros_process_mining::RegistryEntity;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    core::{
        event_data::object_centric::{
            appendable::AppendableOCEL,
            io::OCELIOError,
            linked_ocel::LinkedOCELAccess,
            ocel_json::import_ocel_json_into,
            ocel_xml::xml_ocel_import::{import_ocel_xml_into, OCELImportOptions},
            readable::{OCELLookup, ReadableOCEL},
            OCELAttributeType, OCELAttributeValue, OCELEvent, OCELEventAttribute, OCELObject,
            OCELObjectAttribute, OCELRelationship, OCELType, OCELTypeAttribute,
        },
        io::ExtensionWithMime,
        OCEL,
    },
    Exportable, Importable,
};

/// Interned qualifier identifier. Indexes into [`SlimLinkedOCEL::qualifiers`].
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct QualifierIdx(u32);

impl QualifierIdx {
    /// Return the raw `u32` index into the qualifier table.
    #[inline]
    pub fn into_inner(self) -> u32 {
        self.0
    }
}

/// Insert `item` into `v` keeping `v` sorted ascending; if `item` is already present,
/// leaves `v` unchanged. Used for reverse-relationship lists where multi-qualifier
/// edges between the same pair must contribute a single entry.
fn sorted_insert_unique<T: Ord>(v: &mut Vec<T>, item: T) {
    if let Err(pos) = v.binary_search(&item) {
        v.insert(pos, item);
    }
}

/// Intern `s` into `qualifiers`, returning its index.
fn intern_qualifier(
    qualifiers: &mut Vec<String>,
    qualifier_index: &mut HashTable<u32>,
    hasher: &DefaultHashBuilder,
    s: String,
) -> QualifierIdx {
    let h = hasher.hash_one(&s);
    if let Some(&i) = qualifier_index.find(h, |&j| qualifiers[j as usize] == s) {
        return QualifierIdx(i);
    }
    let idx = qualifiers.len() as u32;
    qualifiers.push(s);
    qualifier_index.insert_unique(h, idx, |&j| hasher.hash_one(&qualifiers[j as usize]));
    QualifierIdx(idx)
}

/// Insert `new_type` or merge into an existing entry. New attributes are appended; existing
/// attributes keep their slot but adopt the new `value_type`. Slots are kept stable because
/// already-appended events/objects index attributes positionally.
///
/// Values already stored under an attribute whose declared `value_type` is overridden here
/// are left as-is, so the schema's `value_type` and the stored value variant may diverge for
/// items appended before the late declaration.
fn declare_or_merge_type<T>(
    types: &mut Vec<OCELType>,
    index: &mut HashMap<String, usize>,
    per_type: &mut Vec<Vec<T>>,
    new_type: OCELType,
) {
    if let Some(&idx) = index.get(&new_type.name) {
        let dst = &mut types[idx].attributes;
        for a in new_type.attributes {
            match dst.iter_mut().find(|d| d.name == a.name) {
                Some(existing) => existing.value_type = a.value_type,
                None => dst.push(a),
            }
        }
        return;
    }
    let idx = types.len();
    index.insert(new_type.name.clone(), idx);
    per_type.push(Vec::new());
    types.push(new_type);
}

/// Returns the index of `name` in `index`, or registers a fresh empty type if unknown.
fn ensure_type_idx<T>(
    types: &mut Vec<OCELType>,
    index: &mut HashMap<String, usize>,
    per_type: &mut Vec<Vec<T>>,
    name: &str,
) -> usize {
    if let Some(&i) = index.get(name) {
        return i;
    }
    let i = types.len();
    index.insert(name.to_string(), i);
    per_type.push(Vec::new());
    types.push(OCELType {
        name: name.to_string(),
        attributes: Vec::new(),
    });
    i
}

/// Reconcile a value already known to the type (i.e., its `name` is declared).
///
/// If the value's variant agrees with the declared `value_type` (or the value is `Null`,
/// the missing sentinel), the value is returned as-is. Otherwise the function tries
/// [`OCELAttributeValue::try_coerce_to`]; on success the coerced value is returned silently,
/// on failure the original value is returned and a warning is emitted to stderr.
fn reconcile_known_value(
    value: OCELAttributeValue,
    declared: OCELAttributeType,
    owner_kind: &str,
    owner_id: &str,
    attr_name: &str,
) -> OCELAttributeValue {
    if matches!(value, OCELAttributeValue::Null) {
        return value;
    }
    // `OCELAttributeType::Null` here means the declared type string was unrecognized
    // (see `OCELAttributeType::from_type_str`); pass the value through unchanged.
    if declared == OCELAttributeType::Null {
        return value;
    }
    let observed = value.get_type();
    if declared == observed {
        return value;
    }
    match value.try_coerce_to(declared) {
        Some(coerced) => coerced,
        None => {
            eprintln!(
                "[rust4pm] warning: {} {:?} attribute {:?}: value variant {:?} differs from declared type {:?} and cannot be coerced; storing as-is",
                owner_kind,
                owner_id,
                attr_name,
                observed.as_type_str(),
                declared.as_type_str(),
            );
            value
        }
    }
}

/// Inner index type for events and objects.
///
/// The public `EventIndex` and `ObjectIndex` types are thin wrappers around this, providing type safety and OCEL-specific accessors.
pub type InnerIndex = u32;

/// An Event Index
///
/// Points to an event in the context of a given OCEL
#[derive(
    PartialEq, Eq, Hash, Clone, Copy, Debug, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
pub struct EventIndex(InnerIndex);
impl From<&EventIndex> for EventIndex {
    fn from(value: &EventIndex) -> Self {
        *value
    }
}
impl From<u32> for EventIndex {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl EventIndex {
    /// Inner index as `usize` for slice/Vec indexing.
    #[inline]
    fn ix(self) -> usize {
        self.0 as usize
    }
    /// Get the (slim) event referenced by this index in the locel
    ///
    /// Note: If there is no event at the specified index, this will access an array out of bounds!
    /// Use the [`EventIndex::get_ev_opt`] version if you want to handle this explicitly.
    pub fn get_ev<'a>(&self, locel: &'a SlimLinkedOCEL) -> &'a SlimOCELEvent {
        &locel.events[self.ix()]
    }
    /// Get the (slim) event referenced by this index in the locel
    ///
    /// This version explicitly handles scenarios where the event might not exist.
    /// In case you are sure that the object exists, use the [`EventIndex::get_ev`] function instead.
    pub fn get_ev_opt<'a>(&self, locel: &'a SlimLinkedOCEL) -> Option<&'a SlimOCELEvent> {
        locel.events.get(self.ix())
    }
    /// Get the event type of the event referenced through this event index
    pub fn get_ev_type<'a>(&self, locel: &'a SlimLinkedOCEL) -> &'a String {
        &locel.event_types[locel.events[self.ix()].event_type].name
    }
    /// Get the timestamp of this event
    pub fn get_time<'a>(&self, locel: &'a SlimLinkedOCEL) -> &'a DateTime<FixedOffset> {
        &locel.events[self.ix()].time
    }
    /// Get E2O relationships of this event
    pub fn get_e2o<'a>(
        &self,
        locel: &'a SlimLinkedOCEL,
    ) -> impl Iterator<Item = &'a ObjectIndex> + use<'a> {
        locel
            .events
            .get(self.ix())
            .into_iter()
            .flat_map(|e| e.relationships.iter().map(|(_q, o)| o))
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
        let ev = &mut locel.events[self.ix()];
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
                    object_id: locel.objects[o.into_inner() as usize].id.clone(),
                    qualifier: locel.qualifier_str(*q).to_string(),
                })
                .collect(),
        }
    }

    /// Retrieve inner index value
    ///
    /// Warning: Only use carefully, as wrong usage can lead to invalid `EventIndex` references, even when using only a single OCEL
    pub fn into_inner(self) -> InnerIndex {
        self.0
    }
}

#[derive(
    PartialEq, Eq, Hash, Clone, Copy, Debug, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
/// An Object Index
///
/// Points to an object in the context of a given OCEL
pub struct ObjectIndex(InnerIndex);
impl From<&ObjectIndex> for ObjectIndex {
    fn from(value: &ObjectIndex) -> Self {
        *value
    }
}
impl From<InnerIndex> for ObjectIndex {
    fn from(value: InnerIndex) -> Self {
        Self(value)
    }
}
impl ObjectIndex {
    /// Inner index as `usize` for slice/Vec indexing.
    #[inline]
    fn ix(self) -> usize {
        self.0 as usize
    }
    /// Get the (slim) object referred to by this index in the locel
    ///
    /// Note: If there is no object at the specified index, this will access an array out of bounds!
    /// Use the [`ObjectIndex::get_ob_opt`] version if you want to handle this explicitly.
    pub fn get_ob<'a>(&self, locel: &'a SlimLinkedOCEL) -> &'a SlimOCELObject {
        &locel.objects[self.ix()]
    }
    /// Get the (slim) object referred to by this index in the locel
    ///
    /// This version explicitly handles scenarios where the object might not exist.
    /// In case you are sure that the object exists, use the [`ObjectIndex::get_ob`] function instead.
    pub fn get_ob_opt<'a>(&self, locel: &'a SlimLinkedOCEL) -> Option<&'a SlimOCELObject> {
        locel.objects.get(self.ix())
    }

    /// Get the object type of the object referenced through this object index
    pub fn get_ob_type<'a>(&self, locel: &'a SlimLinkedOCEL) -> &'a String {
        &locel.object_types[locel.objects[self.ix()].object_type].name
    }
    /// Get O2O relationships
    pub fn get_o2o<'a>(
        &self,
        locel: &'a SlimLinkedOCEL,
    ) -> impl Iterator<Item = &'a ObjectIndex> + use<'a> {
        locel
            .objects
            .get(self.ix())
            .into_iter()
            .flat_map(|o| &o.relationships)
            .map(|(_q, o)| o)
    }
    /// Get reverse O2O relationships
    pub fn get_o2o_rev<'a>(
        &self,
        locel: &'a SlimLinkedOCEL,
    ) -> impl Iterator<Item = &'a ObjectIndex> + use<'a> {
        locel
            .objects
            .get(self.ix())
            .into_iter()
            .flat_map(|o| o.o2o_rev.iter())
    }
    /// Get reverse E2O relationships
    pub fn get_e2o_rev<'a>(
        &self,
        locel: &'a SlimLinkedOCEL,
    ) -> impl Iterator<Item = &'a EventIndex> + use<'a> {
        locel
            .objects
            .get(self.ix())
            .into_iter()
            .flat_map(|o| o.e2o_rev.iter())
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
        let this = *self;
        evtype_index.into_iter().flat_map(move |ei| {
            locel
                .objects
                .get(this.ix())
                .into_iter()
                .flat_map(|o| o.e2o_rev.iter())
                .filter(move |ev| locel.events[ev.ix()].event_type == ei)
        })
    }
    /// Get the activity trace of this object as event-type indices, ordered by event timestamp
    ///
    /// Each yielded `usize` is the internal event-type index of an event connected to this object.
    /// This is the cheap form of the trace — useful when you intend to group, count, or otherwise compare
    /// traces without allocating string copies. Use [`ObjectIndex::get_obj_activity_trace`] if you want
    /// the event-type names directly.
    pub fn get_obj_activity_trace_evtype_indices<'a>(
        &self,
        locel: &'a SlimLinkedOCEL,
    ) -> impl Iterator<Item = usize> + use<'a> {
        let mut events: Vec<EventIndex> = self.get_e2o_rev(locel).copied().collect();
        events.sort_by_key(|e| *e.get_time(locel));
        events.into_iter().map(move |e| e.get_ev(locel).event_type)
    }
    /// Get the activity trace of this object (i.e., the sequence of event types connected to the object, ordered by event timestamp)
    pub fn get_obj_activity_trace<'a>(
        &self,
        locel: &'a SlimLinkedOCEL,
    ) -> impl Iterator<Item = &'a String> + use<'a> {
        self.get_obj_activity_trace_evtype_indices(locel)
            .map(move |i| &locel.event_types[i].name)
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
        let ob = &mut locel.objects[self.ix()];
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
                    object_id: locel.objects[o.into_inner() as usize].id.clone(),
                    qualifier: locel.qualifier_str(*q).to_string(),
                })
                .collect(),
        }
    }

    /// Retrieve inner index value
    ///
    /// Warning: Only use carefully, as wrong usage can lead to invalid `ObjectIndex` references, even when using only a single OCEL
    pub fn into_inner(self) -> InnerIndex {
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

#[derive(Debug, Clone, RegistryEntity, Default)]
/// An object-centric event log where events and objects are referenced by integer indices
/// ([`EventIndex`] / [`ObjectIndex`]) returned from the `add_*` methods, and each indexed
/// event/object is an instance of an event/object type (activity / object class) declared
/// beforehand with an ordered list of attributes.
pub struct SlimLinkedOCEL {
    events: Vec<SlimOCELEvent>,
    objects: Vec<SlimOCELObject>,
    event_types: Vec<OCELType>,
    object_types: Vec<OCELType>,
    /// Event-ID -> [`EventIndex`] lookup. Stores only `u32` indices; the id string
    /// lives on [`SlimOCELEvent::id`] and is reached through `events`.
    event_ids_to_index: HashTable<u32>,
    /// Object-ID -> [`ObjectIndex`] lookup. See [`Self::event_ids_to_index`].
    object_ids_to_index: HashTable<u32>,
    /// Hasher used for [`Self::event_ids_to_index`] and [`Self::object_ids_to_index`].
    hasher: DefaultHashBuilder,
    events_per_type: Vec<Vec<EventIndex>>,
    objects_per_type: Vec<Vec<ObjectIndex>>,
    evtype_to_index: HashMap<String, usize>,
    obtype_to_index: HashMap<String, usize>,
    /// Distinct relationship qualifiers; relationships carry [`QualifierIdx`] indices into this.
    qualifiers: Vec<String>,
    /// Qualifier-string -> [`QualifierIdx`] lookup. Indexes into [`Self::qualifiers`].
    qualifier_index: HashTable<u32>,
    /// Forward E2O references whose target object id was unknown at insert time.
    pending_e2o: Vec<(EventIndex, OCELRelationship)>,
    /// Forward O2O references whose target object id was unknown at insert time.
    pending_o2o: Vec<(ObjectIndex, OCELRelationship)>,
}
impl SlimLinkedOCEL {
    /// Create a new empty `SlimLinkedOCEL`
    ///
    /// After creation, new event/object types as well as event/object instances can be added to it.
    pub fn new() -> Self {
        Self::default()
    }
    /// Convert an unlinked [`OCEL`] to a [`SlimLinkedOCEL`].
    ///
    /// Events are sorted by time before insertion so that `events_per_type` lists are
    /// time-ordered. Duplicate event/object ids are skipped with a warning. Unknown
    /// types referenced by an event/object are auto-declared on first use, and
    /// attributes not listed in the declared schema cause the schema to grow.
    pub fn from_ocel(mut ocel: OCEL) -> Self {
        ocel.events.sort_by_key(|e| e.time);
        let mut linked = SlimLinkedOCEL::new();
        for et in ocel.event_types {
            let _ = linked.declare_event_type(et);
        }
        for ot in ocel.object_types {
            let _ = linked.declare_object_type(ot);
        }
        for o in ocel.objects {
            let OCELObject {
                id,
                object_type,
                attributes,
                relationships,
            } = o;
            if let Err(e) = linked.append_object(id, &object_type, attributes, relationships) {
                eprintln!("[rust4pm] warning: skipping object: {e}");
            }
        }
        for ev in ocel.events {
            let OCELEvent {
                id,
                event_type,
                time,
                attributes,
                relationships,
            } = ev;
            if let Err(e) = linked.append_event(id, &event_type, time, attributes, relationships) {
                eprintln!("[rust4pm] warning: skipping event: {e}");
            }
        }
        let _ = linked.finalize();
        linked
    }

    /// Resolve a qualifier index to its string form. Panics if `idx` is out of range, so
    /// only safe for indices read from this OCEL's own relationship lists. Use
    /// [`Self::try_qualifier_str`] for indices that may have come from another OCEL or
    /// from a deserialized struct.
    #[inline]
    pub fn qualifier_str(&self, idx: QualifierIdx) -> &str {
        &self.qualifiers[idx.0 as usize]
    }

    /// Resolve a qualifier index to its string form, or `None` if `idx` is invalid
    #[inline]
    pub fn try_qualifier_str(&self, idx: QualifierIdx) -> Option<&str> {
        self.qualifiers.get(idx.0 as usize).map(String::as_str)
    }

    /// All distinct relationship qualifier strings, indexed by [`QualifierIdx`].
    #[inline]
    pub fn qualifiers(&self) -> &[String] {
        &self.qualifiers
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
        let _ = self.declare_event_type(OCELType {
            name: event_type.to_string(),
            attributes,
        });
    }
    /// Add a new object type to the OCEL, with the specified attributes
    pub fn add_object_type(&mut self, object_type: &str, attributes: Vec<OCELTypeAttribute>) {
        let _ = self.declare_object_type(OCELType {
            name: object_type.to_string(),
            attributes,
        });
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
        relationships: Vec<(String, ObjectIndex)>,
    ) -> Option<EventIndex> {
        let etype = self.evtype_to_index.get(event_type)?;
        let id = id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let h = self.hasher.hash_one(&id);
        if self
            .event_ids_to_index
            .find(h, |&j| self.events[j as usize].id == id)
            .is_some()
        {
            return None;
        }
        let new_ev_index = EventIndex(self.events.len() as u32);
        {
            let events = &self.events;
            let hasher = &self.hasher;
            self.event_ids_to_index
                .insert_unique(h, new_ev_index.0, |&j| {
                    hasher.hash_one(&events[j as usize].id)
                });
        }
        self.events_per_type.get_mut(*etype)?.push(new_ev_index);
        let mut interned: Vec<(QualifierIdx, ObjectIndex)> = {
            let qualifiers = &mut self.qualifiers;
            let qualifier_index = &mut self.qualifier_index;
            let hasher = &self.hasher;
            relationships
                .into_iter()
                .map(|(q, o)| (intern_qualifier(qualifiers, qualifier_index, hasher, q), o))
                .collect()
        };
        interned.sort_by_key(|(_q, o)| *o);
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

        for (_q, obj) in &interned {
            sorted_insert_unique(&mut self.objects[obj.0 as usize].e2o_rev, new_ev_index);
        }
        self.events.push(SlimOCELEvent {
            id,
            event_type: *etype,
            time,
            attributes,
            relationships: interned,
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
        relationships: Vec<(String, ObjectIndex)>,
    ) -> Option<ObjectIndex> {
        let otype = self.obtype_to_index.get(object_type)?;
        let id = id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let h = self.hasher.hash_one(&id);
        if self
            .object_ids_to_index
            .find(h, |&j| self.objects[j as usize].id == id)
            .is_some()
        {
            return None;
        }
        let new_ob_index = ObjectIndex(self.objects.len() as u32);
        {
            let objects = &self.objects;
            let hasher = &self.hasher;
            self.object_ids_to_index
                .insert_unique(h, new_ob_index.0, |&j| {
                    hasher.hash_one(&objects[j as usize].id)
                });
        }
        self.objects_per_type.get_mut(*otype)?.push(new_ob_index);
        let mut interned: Vec<(QualifierIdx, ObjectIndex)> = {
            let qualifiers = &mut self.qualifiers;
            let qualifier_index = &mut self.qualifier_index;
            let hasher = &self.hasher;
            relationships
                .into_iter()
                .map(|(q, o)| (intern_qualifier(qualifiers, qualifier_index, hasher, q), o))
                .collect()
        };
        interned.sort_by_key(|(_q, o)| *o);
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

        for (_q, target) in &interned {
            sorted_insert_unique(&mut self.objects[target.0 as usize].o2o_rev, new_ob_index);
        }
        self.objects.push(SlimOCELObject {
            id,
            object_type: *otype,
            attributes,
            relationships: interned,
            e2o_rev: Vec::new(),
            o2o_rev: Vec::new(),
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
        if (event.0 as usize) >= self.events.len() || (object.0 as usize) >= self.objects.len() {
            eprintln!(
                "[rust4pm] warning: add_e2o called with invalid index(es) (event={}, object={}); ignored",
                event.0, object.0
            );
            return false;
        }
        let q_idx = intern_qualifier(
            &mut self.qualifiers,
            &mut self.qualifier_index,
            &self.hasher,
            qualifier,
        );
        let rels = &mut self.events[event.0 as usize].relationships;
        let changed = !rels.iter().any(|(q, o)| o == &object && *q == q_idx);
        if changed {
            let insert_pos = rels.partition_point(|(_q, o)| o < &object);
            rels.insert(insert_pos, (q_idx, object));
            sorted_insert_unique(&mut self.objects[object.0 as usize].e2o_rev, event);
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
        if (from_obj.0 as usize) >= self.objects.len() || (to_obj.0 as usize) >= self.objects.len()
        {
            eprintln!(
                "[rust4pm] warning: add_o2o called with invalid index(es) (from_obj={}, to_obj={}); ignored",
                from_obj.0, to_obj.0
            );
            return false;
        }
        let q_idx = intern_qualifier(
            &mut self.qualifiers,
            &mut self.qualifier_index,
            &self.hasher,
            qualifier,
        );
        let rels = &mut self.objects[from_obj.0 as usize].relationships;
        let changed = !rels.iter().any(|(q, o)| o == &to_obj && *q == q_idx);
        if changed {
            let insert_pos = rels.partition_point(|(_q, o)| o < &to_obj);
            rels.insert(insert_pos, (q_idx, to_obj));
            sorted_insert_unique(&mut self.objects[to_obj.0 as usize].o2o_rev, from_obj);
        }
        true
    }
    /// Remove all E2O relationships between the passed event and object (across every qualifier).
    ///
    /// Returns `true` on success, `false` if either index is out of bounds (with a stderr warning).
    pub fn delete_e2o(&mut self, event: &EventIndex, object: &ObjectIndex) -> bool {
        if (event.0 as usize) >= self.events.len() || (object.0 as usize) >= self.objects.len() {
            eprintln!(
                "[rust4pm] warning: delete_e2o called with invalid index(es) (event={}, object={}); ignored",
                event.0, object.0
            );
            return false;
        }
        let rels = &mut self.events[event.0 as usize].relationships;
        let before = rels.len();
        rels.retain(|(_q, o)| o != object);
        if rels.len() != before {
            self.objects[object.0 as usize]
                .e2o_rev
                .retain(|e| e != event);
        }
        true
    }
    /// Remove all O2O relationships from `from_obj` to `to_obj` (across every qualifier).
    ///
    /// Returns `true` on success, `false` if either index is out of bounds (with a stderr warning).
    pub fn delete_o2o(&mut self, from_obj: &ObjectIndex, to_obj: &ObjectIndex) -> bool {
        if (from_obj.0 as usize) >= self.objects.len() || (to_obj.0 as usize) >= self.objects.len()
        {
            eprintln!(
                "[rust4pm] warning: delete_o2o called with invalid index(es) (from_obj={}, to_obj={}); ignored",
                from_obj.0, to_obj.0
            );
            return false;
        }
        let rels = &mut self.objects[from_obj.0 as usize].relationships;
        let before = rels.len();
        rels.retain(|(_q, o)| o != to_obj);
        if rels.len() != before {
            self.objects[to_obj.0 as usize]
                .o2o_rev
                .retain(|o| o != from_obj);
        }
        true
    }
}
impl From<OCEL> for SlimLinkedOCEL {
    fn from(value: OCEL) -> Self {
        Self::from_ocel(value)
    }
}

/// A slim version of an OCEL Event.
///
/// Qualifier strings in relationships are interned via [`SlimLinkedOCEL::qualifiers`]
/// and referenced here by [`QualifierIdx`] instead of owned `String`s.
#[derive(Debug, Clone, Serialize)]
pub struct SlimOCELEvent {
    /// Event ID
    pub id: String,
    /// Event Type (referring back to the `name` of an [`OCELType`])
    #[serde(rename = "type")]
    pub event_type: usize,
    /// `DateTime` when event occured
    pub time: DateTime<FixedOffset>,
    /// Event attributes
    pub attributes: Vec<OCELAttributeValue>,
    /// E2O relationships as `(qualifier_idx, object)` pairs, sorted ascending by
    /// [`ObjectIndex`]. Resolve qualifier strings via [`SlimLinkedOCEL::qualifier_str`].
    pub relationships: Vec<(QualifierIdx, ObjectIndex)>,
}
/// A slim version of an OCEL Object.
///
/// Qualifier strings in relationships are interned via [`SlimLinkedOCEL::qualifiers`]
/// and referenced here by [`QualifierIdx`] instead of owned `String`s.
#[derive(Debug, Clone, Serialize)]
pub struct SlimOCELObject {
    /// Object ID
    pub id: String,
    /// Object Type (referring back to the `name` of an [`OCELType`])
    #[serde(rename = "type")]
    pub object_type: usize,
    /// Object attributes (each inner [`Vec`] holds the time-indexed values for one declared attribute)
    pub attributes: Vec<Vec<(DateTime<FixedOffset>, OCELAttributeValue)>>,
    /// O2O relationships as `(qualifier_idx, target_object)` pairs, sorted ascending by
    /// [`ObjectIndex`]. Resolve qualifier strings via [`SlimLinkedOCEL::qualifier_str`].
    pub relationships: Vec<(QualifierIdx, ObjectIndex)>,
    /// Reverse E2O: events whose forward relationships reference this object.
    #[serde(skip)]
    pub e2o_rev: Vec<EventIndex>,
    /// Reverse O2O: source objects whose forward relationships reference this object.
    #[serde(skip)]
    pub o2o_rev: Vec<ObjectIndex>,
}

/// Errors returned by [`AppendableOCEL`] operations on [`SlimLinkedOCEL`]
#[derive(Debug)]
pub enum SlimAppendError {
    /// Event id already used
    DuplicateEventId(String),
    /// Object id already used
    DuplicateObjectId(String),
}

impl std::fmt::Display for SlimAppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateEventId(id) => write!(f, "Duplicate event id: {id}"),
            Self::DuplicateObjectId(id) => write!(f, "Duplicate object id: {id}"),
        }
    }
}

impl std::error::Error for SlimAppendError {}

impl From<SlimAppendError> for OCELIOError {
    fn from(e: SlimAppendError) -> Self {
        OCELIOError::Other(e.to_string())
    }
}

impl AppendableOCEL for SlimLinkedOCEL {
    type Error = SlimAppendError;

    fn declare_event_type(&mut self, event_type: OCELType) -> Result<(), Self::Error> {
        declare_or_merge_type(
            &mut self.event_types,
            &mut self.evtype_to_index,
            &mut self.events_per_type,
            event_type,
        );
        Ok(())
    }

    fn declare_object_type(&mut self, object_type: OCELType) -> Result<(), Self::Error> {
        declare_or_merge_type(
            &mut self.object_types,
            &mut self.obtype_to_index,
            &mut self.objects_per_type,
            object_type,
        );
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
        let h_id = self.hasher.hash_one(&id);
        if self
            .event_ids_to_index
            .find(h_id, |&j| self.events[j as usize].id == id)
            .is_some()
        {
            return Err(SlimAppendError::DuplicateEventId(id));
        }
        let etype = ensure_type_idx(
            &mut self.event_types,
            &mut self.evtype_to_index,
            &mut self.events_per_type,
            event_type,
        );
        let new_idx = EventIndex(self.events.len() as u32);
        // Auto-declare unknown attribute names. Warn only on schema drift (type already
        // had entries); silent during bootstrap of an empty auto-declared type.
        let attrs = &mut self.event_types[etype].attributes;
        for a in &attributes {
            if !attrs.iter().any(|d| d.name == a.name) {
                if !attrs.is_empty() {
                    eprintln!(
                        "[rust4pm] warning: event {:?} of type {:?} has attribute {:?} not in the existing type schema; auto-growing the type",
                        id, event_type, a.name
                    );
                }
                attrs.push(OCELTypeAttribute {
                    name: a.name.clone(),
                    value_type: a.value.get_type().as_type_str().to_string(),
                });
            }
        }
        let positional: Vec<OCELAttributeValue> = attrs
            .iter()
            .map(|d| {
                let declared = OCELAttributeType::from_type_str(&d.value_type);
                match attributes.iter().find(|a| a.name == d.name) {
                    Some(a) => {
                        reconcile_known_value(a.value.clone(), declared, "event", &id, &a.name)
                    }
                    None => OCELAttributeValue::Null,
                }
            })
            .collect();
        let mut resolved: Vec<(QualifierIdx, ObjectIndex)> =
            Vec::with_capacity(relationships.len());
        for r in relationships {
            let h = self.hasher.hash_one(&r.object_id);
            let lookup = self
                .object_ids_to_index
                .find(h, |&j| self.objects[j as usize].id == r.object_id)
                .copied();
            match lookup {
                Some(o_idx) => {
                    let q = intern_qualifier(
                        &mut self.qualifiers,
                        &mut self.qualifier_index,
                        &self.hasher,
                        r.qualifier,
                    );
                    resolved.push((q, ObjectIndex(o_idx)));
                }
                None => self.pending_e2o.push((new_idx, r)),
            }
        }
        resolved.sort_by_key(|(_q, o)| *o);
        {
            let events = &self.events;
            let hasher = &self.hasher;
            self.event_ids_to_index
                .insert_unique(h_id, new_idx.0, |&j| {
                    hasher.hash_one(&events[j as usize].id)
                });
        }
        self.events_per_type[etype].push(new_idx);
        for (_q, obj) in &resolved {
            sorted_insert_unique(&mut self.objects[obj.0 as usize].e2o_rev, new_idx);
        }
        self.events.push(SlimOCELEvent {
            id,
            event_type: etype,
            time,
            attributes: positional,
            relationships: resolved,
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
        let h_id = self.hasher.hash_one(&id);
        if self
            .object_ids_to_index
            .find(h_id, |&j| self.objects[j as usize].id == id)
            .is_some()
        {
            return Err(SlimAppendError::DuplicateObjectId(id));
        }
        let otype = ensure_type_idx(
            &mut self.object_types,
            &mut self.obtype_to_index,
            &mut self.objects_per_type,
            object_type,
        );
        let new_idx = ObjectIndex(self.objects.len() as u32);
        let attrs = &mut self.object_types[otype].attributes;
        for a in &attributes {
            if !attrs.iter().any(|d| d.name == a.name) {
                if !attrs.is_empty() {
                    eprintln!(
                        "[rust4pm] warning: object {:?} of type {:?} has attribute {:?} not in the existing type schema; auto-growing the type",
                        id, object_type, a.name
                    );
                }
                attrs.push(OCELTypeAttribute {
                    name: a.name.clone(),
                    value_type: a.value.get_type().as_type_str().to_string(),
                });
            }
        }
        let positional: Vec<Vec<(DateTime<FixedOffset>, OCELAttributeValue)>> = attrs
            .iter()
            .map(|d| {
                let declared = OCELAttributeType::from_type_str(&d.value_type);
                attributes
                    .iter()
                    .filter(|a| a.name == d.name)
                    .map(|a| {
                        let v = reconcile_known_value(
                            a.value.clone(),
                            declared,
                            "object",
                            &id,
                            &a.name,
                        );
                        (a.time, v)
                    })
                    .collect()
            })
            .collect();
        let mut resolved: Vec<(QualifierIdx, ObjectIndex)> =
            Vec::with_capacity(relationships.len());
        for r in relationships {
            let h = self.hasher.hash_one(&r.object_id);
            let lookup = self
                .object_ids_to_index
                .find(h, |&j| self.objects[j as usize].id == r.object_id)
                .copied();
            match lookup {
                Some(o_idx) => {
                    let q = intern_qualifier(
                        &mut self.qualifiers,
                        &mut self.qualifier_index,
                        &self.hasher,
                        r.qualifier,
                    );
                    resolved.push((q, ObjectIndex(o_idx)));
                }
                None => self.pending_o2o.push((new_idx, r)),
            }
        }
        resolved.sort_by_key(|(_q, o)| *o);
        {
            let objects = &self.objects;
            let hasher = &self.hasher;
            self.object_ids_to_index
                .insert_unique(h_id, new_idx.0, |&j| {
                    hasher.hash_one(&objects[j as usize].id)
                });
        }
        self.objects_per_type[otype].push(new_idx);
        for (_q, target) in &resolved {
            sorted_insert_unique(&mut self.objects[target.0 as usize].o2o_rev, new_idx);
        }
        self.objects.push(SlimOCELObject {
            id,
            object_type: otype,
            attributes: positional,
            relationships: resolved,
            e2o_rev: Vec::new(),
            o2o_rev: Vec::new(),
        });
        Ok(())
    }

    fn finalize(&mut self) -> Result<(), Self::Error> {
        // Resolve pending E2O / O2O forward refs and re-sort touched relationship lists.
        // The OCEL spec disallows duplicate (source, target, qualifier) triples; not
        // deduped here, invalid input flows through as-is.
        let mut ev_dirty: Vec<EventIndex> = Vec::new();
        for (ev_idx, rel) in std::mem::take(&mut self.pending_e2o) {
            let h = self.hasher.hash_one(&rel.object_id);
            let target = self
                .object_ids_to_index
                .find(h, |&j| self.objects[j as usize].id == rel.object_id)
                .copied();
            match target {
                Some(raw_idx) => {
                    let ob_idx = ObjectIndex(raw_idx);
                    let q_idx = intern_qualifier(
                        &mut self.qualifiers,
                        &mut self.qualifier_index,
                        &self.hasher,
                        rel.qualifier,
                    );
                    self.events[ev_idx.0 as usize]
                        .relationships
                        .push((q_idx, ob_idx));
                    sorted_insert_unique(&mut self.objects[ob_idx.0 as usize].e2o_rev, ev_idx);
                    ev_dirty.push(ev_idx);
                }
                None => {
                    eprintln!(
                        "[rust4pm] warning: dropping E2O reference to unknown object id {:?}",
                        rel.object_id
                    );
                }
            }
        }
        ev_dirty.sort_unstable();
        ev_dirty.dedup();
        for ev_idx in ev_dirty {
            self.events[ev_idx.0 as usize]
                .relationships
                .sort_by_key(|(_q, o)| *o);
        }

        let mut ob_dirty: Vec<ObjectIndex> = Vec::new();
        for (from_idx, rel) in std::mem::take(&mut self.pending_o2o) {
            let h = self.hasher.hash_one(&rel.object_id);
            let target = self
                .object_ids_to_index
                .find(h, |&j| self.objects[j as usize].id == rel.object_id)
                .copied();
            match target {
                Some(raw_idx) => {
                    let to_idx = ObjectIndex(raw_idx);
                    let q_idx = intern_qualifier(
                        &mut self.qualifiers,
                        &mut self.qualifier_index,
                        &self.hasher,
                        rel.qualifier,
                    );
                    self.objects[from_idx.0 as usize]
                        .relationships
                        .push((q_idx, to_idx));
                    sorted_insert_unique(&mut self.objects[to_idx.0 as usize].o2o_rev, from_idx);
                    ob_dirty.push(from_idx);
                }
                None => {
                    eprintln!(
                        "[rust4pm] warning: dropping O2O reference to unknown object id {:?}",
                        rel.object_id
                    );
                }
            }
        }
        ob_dirty.sort_unstable();
        ob_dirty.dedup();
        for ob_idx in ob_dirty {
            self.objects[ob_idx.0 as usize]
                .relationships
                .sort_by_key(|(_q, o)| *o);
        }

        // Streaming append preserves input order; from_ocel pre-sorts events by time.
        // Sort here so `events_per_type` is time-ordered regardless of import path.
        let events = &self.events;
        for per_type in &mut self.events_per_type {
            per_type.sort_by_key(|ei| events[ei.0 as usize].time);
        }

        Ok(())
    }
}

impl ReadableOCEL for SlimLinkedOCEL {
    type Lookup<'a> = SlimOCELLookup<'a>;
    fn event_types(&self) -> &[OCELType] {
        &self.event_types
    }
    fn object_types(&self) -> &[OCELType] {
        &self.object_types
    }
    fn iter_events(&self) -> Box<dyn Iterator<Item = Cow<'_, OCELEvent>> + '_> {
        Box::new((0..self.events.len()).map(|i| Cow::Owned(EventIndex(i as u32).fat_ev(self))))
    }
    fn iter_events_sorted_by_time(&self) -> Box<dyn Iterator<Item = Cow<'_, OCELEvent>> + '_> {
        // Streaming append paths may leave events out of order; sort indices, not events.
        let mut indices: Vec<u32> = (0..self.events.len() as u32).collect();
        indices.sort_by_key(|&i| self.events[i as usize].time);
        Box::new(
            indices
                .into_iter()
                .map(move |i| Cow::Owned(EventIndex(i).fat_ev(self))),
        )
    }
    fn iter_objects(&self) -> Box<dyn Iterator<Item = Cow<'_, OCELObject>> + '_> {
        Box::new((0..self.objects.len()).map(|i| Cow::Owned(ObjectIndex(i as u32).fat_ob(self))))
    }
    fn iter_events_of_type<'a>(
        &'a self,
        type_name: &'a str,
    ) -> Box<dyn Iterator<Item = Cow<'a, OCELEvent>> + 'a> {
        let Some(&idx) = self.evtype_to_index.get(type_name) else {
            return Box::new(std::iter::empty());
        };
        Box::new(
            self.events_per_type[idx]
                .iter()
                .map(move |ei| Cow::Owned(ei.fat_ev(self))),
        )
    }
    fn iter_objects_of_type<'a>(
        &'a self,
        type_name: &'a str,
    ) -> Box<dyn Iterator<Item = Cow<'a, OCELObject>> + 'a> {
        let Some(&idx) = self.obtype_to_index.get(type_name) else {
            return Box::new(std::iter::empty());
        };
        Box::new(
            self.objects_per_type[idx]
                .iter()
                .map(move |oi| Cow::Owned(oi.fat_ob(self))),
        )
    }
    fn lookup(&self) -> SlimOCELLookup<'_> {
        SlimOCELLookup { locel: self }
    }
}

/// Thin wrapper around [`SlimLinkedOCEL`] that satisfies [`OCELLookup`] without
/// materializing any objects. Lookups go straight through the existing index
/// tables and slim object representation.
#[derive(Debug)]
pub struct SlimOCELLookup<'a> {
    locel: &'a SlimLinkedOCEL,
}

impl<'a> SlimOCELLookup<'a> {
    fn find_idx(&self, id: &str) -> Option<usize> {
        let h = self.locel.hasher.hash_one(id);
        self.locel
            .object_ids_to_index
            .find(h, |&j| self.locel.objects[j as usize].id == id)
            .map(|&i| i as usize)
    }
}

impl<'a> OCELLookup for SlimOCELLookup<'a> {
    fn iter_object_ids<'b>(&'b self) -> Box<dyn Iterator<Item = &'b str> + 'b> {
        Box::new(self.locel.objects.iter().map(|o| o.id.as_str()))
    }
    fn get_id_borrow(&self, id: &str) -> Option<&str> {
        let i = self.find_idx(id)?;
        Some(self.locel.objects[i].id.as_str())
    }
    fn object_type_of(&self, id: &str) -> Option<&str> {
        let i = self.find_idx(id)?;
        let slim = &self.locel.objects[i];
        Some(self.locel.object_types[slim.object_type].name.as_str())
    }
    fn object_attributes<'b>(
        &'b self,
        id: &str,
    ) -> Box<dyn Iterator<Item = (&'b str, &'b OCELAttributeValue, DateTime<FixedOffset>)> + 'b>
    {
        match self.find_idx(id) {
            Some(i) => {
                let slim = &self.locel.objects[i];
                let ob_type = &self.locel.object_types[slim.object_type];
                Box::new(
                    ob_type
                        .attributes
                        .iter()
                        .enumerate()
                        .flat_map(move |(idx, at)| {
                            slim.attributes
                                .get(idx)
                                .into_iter()
                                .flatten()
                                .map(move |(t, v)| (at.name.as_str(), v, *t))
                        }),
                )
            }
            None => Box::new(std::iter::empty()),
        }
    }
    fn object_relationships<'b>(
        &'b self,
        id: &str,
    ) -> Box<dyn Iterator<Item = (&'b str, &'b str)> + 'b> {
        match self.find_idx(id) {
            Some(i) => {
                let slim = &self.locel.objects[i];
                let locel = self.locel;
                Box::new(slim.relationships.iter().map(move |(q, target)| {
                    (
                        locel.objects[target.0 as usize].id.as_str(),
                        locel.qualifier_str(*q),
                    )
                }))
            }
            None => Box::new(std::iter::empty()),
        }
    }
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
        (0..self.events.len()).map(|i| EventIndex(i as u32))
    }

    fn get_all_obs(&'a self) -> impl Iterator<Item = Self::ObjectRepr> {
        (0..self.objects.len()).map(|i| ObjectIndex(i as u32))
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
        self.events[index.borrow().0 as usize]
            .relationships
            .iter()
            .map(|(q, o_idx)| (self.qualifier_str(*q), o_idx))
    }

    fn get_e2o_rev(
        &'a self,
        index: impl Borrow<Self::ObjectRepr>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::EventRepr)> {
        let target = *index.borrow();
        index.borrow().get_e2o_rev(self).flat_map(move |e| {
            self.events[e.0 as usize]
                .relationships
                .iter()
                .filter(move |(_q, o)| *o == target)
                .map(move |(q, _)| (self.qualifier_str(*q), e))
        })
    }

    fn get_o2o(
        &'a self,
        index: impl Borrow<Self::ObjectRepr>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)> {
        self.objects[index.borrow().0 as usize]
            .relationships
            .iter()
            .map(|(q, o_idx)| (self.qualifier_str(*q), o_idx))
    }

    fn get_o2o_rev(
        &'a self,
        index: impl Borrow<Self::ObjectRepr>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)> {
        let target = *index.borrow();
        index.borrow().get_o2o_rev(self).flat_map(move |o1| {
            self.objects[o1.0 as usize]
                .relationships
                .iter()
                .filter(move |(_q, o2)| *o2 == target)
                .map(move |(q, _)| (self.qualifier_str(*q), o1))
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
            .get(ev.borrow().0 as usize)
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
            .get(ob.borrow().0 as usize)
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
        self.objects[ob.borrow().0 as usize].id.as_str()
    }

    fn get_ev_id(&'a self, ev: impl Borrow<Self::EventRepr>) -> &'a str {
        self.events[ev.borrow().0 as usize].id.as_str()
    }

    fn get_ev_by_id(&'a self, ev_id: impl AsRef<str>) -> Option<Self::EventRepr> {
        let id = ev_id.as_ref();
        let h = self.hasher.hash_one(id);
        self.event_ids_to_index
            .find(h, |&j| self.events[j as usize].id == id)
            .map(|&i| EventIndex(i))
    }

    fn get_ob_by_id(&'a self, ob_id: impl AsRef<str>) -> Option<Self::ObjectRepr> {
        let id = ob_id.as_ref();
        let h = self.hasher.hash_one(id);
        self.object_ids_to_index
            .find(h, |&j| self.objects[j as usize].id == id)
            .map(|&i| ObjectIndex(i))
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
            self.events[index.borrow().0 as usize]
                .relationships
                .iter()
                .filter(move |(_q, o)| &o.get_ob(self).object_type == ot_index)
                .map(|(q, o)| (self.qualifier_str(*q), o))
        })
    }
    fn get_o2o_of_type(
        &'a self,
        index: impl Borrow<Self::ObjectRepr>,
        ob_type: impl AsRef<str>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)> {
        let ob_type_index = self.obtype_to_index.get(ob_type.as_ref());
        ob_type_index.into_iter().flat_map(move |ot_index| {
            self.objects[index.borrow().0 as usize]
                .relationships
                .iter()
                .filter(move |(_q, o)| &o.get_ob(self).object_type == ot_index)
                .map(|(q, o)| (self.qualifier_str(*q), o))
        })
    }

    fn get_e2o_rev_of_type(
        &'a self,
        index: impl Borrow<Self::ObjectRepr>,
        ev_type: impl AsRef<str>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::EventRepr)> {
        let evtype_index = self.evtype_to_index.get(ev_type.as_ref()).copied();
        let target = *index.borrow();
        evtype_index.into_iter().flat_map(move |ei| {
            self.objects[target.0 as usize]
                .e2o_rev
                .iter()
                .filter(move |e| self.events[e.0 as usize].event_type == ei)
                .flat_map(move |e| {
                    e.get_ev(self)
                        .relationships
                        .iter()
                        .filter(move |(_q, o)| *o == target)
                        .map(move |(q, _)| (self.qualifier_str(*q), e))
                })
        })
    }
    fn get_o2o_rev_of_type(
        &'a self,
        to_obj: impl Borrow<Self::ObjectRepr>,
        from_ob_type: impl AsRef<str>,
    ) -> impl Iterator<Item = (&'a str, &'a Self::ObjectRepr)> {
        let obtype_index = self.obtype_to_index.get(from_ob_type.as_ref()).copied();
        let target = *to_obj.borrow();
        obtype_index.into_iter().flat_map(move |oi| {
            self.objects[target.0 as usize]
                .o2o_rev
                .iter()
                .filter(move |o| self.objects[o.0 as usize].object_type == oi)
                .flat_map(move |o| {
                    o.get_ob(self)
                        .relationships
                        .iter()
                        .filter(move |(_q, e)| *e == target)
                        .map(move |(q, _)| (self.qualifier_str(*q), o))
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
        if let Some(inner) = format.strip_suffix(".gz") {
            // Buffer the compressed bytes; `GzDecoder` reads from its inner in chunks.
            let gz: Box<dyn Read> = Box::new(flate2::read::GzDecoder::new(
                std::io::BufReader::new(reader),
            ));
            return Self::import_from_reader_with_options(gz, inner, ());
        }
        if format.ends_with("xml") || format.ends_with("xmlocel") {
            let mut xml_reader = quick_xml::Reader::from_reader(std::io::BufReader::new(reader));
            let mut slim = SlimLinkedOCEL::new();
            import_ocel_xml_into(&mut xml_reader, &mut slim, OCELImportOptions::default())?;
            slim.finalize()?;
            Ok(slim)
        } else if format.ends_with("json") || format.ends_with("jsonocel") {
            let mut slim = SlimLinkedOCEL::new();
            import_ocel_json_into(std::io::BufReader::new(reader), &mut slim)?;
            slim.finalize()?;
            Ok(slim)
        } else {
            let ocel = OCEL::import_from_reader(reader, format)?;
            Ok(SlimLinkedOCEL::from_ocel(ocel))
        }
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

    fn infer_format(path: &Path) -> Option<String> {
        <OCEL as Exportable>::infer_format(path)
    }

    fn export_to_path_with_options<P: AsRef<Path>>(
        &self,
        path: P,
        _: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        let path = path.as_ref();
        let format = <Self as Exportable>::infer_format(path).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Could not infer format from path",
            )
        })?;
        if format.ends_with("sqlite") || (format.ends_with("db") && !format.ends_with("duckdb")) {
            #[cfg(feature = "ocel-sqlite")]
            return crate::core::event_data::object_centric::ocel_sql::export_ocel_sqlite_to_path(
                self, path,
            )
            .map_err(OCELIOError::from);
            #[cfg(not(feature = "ocel-sqlite"))]
            return Err(OCELIOError::UnsupportedFormat(
                "SQLite support not enabled".to_string(),
            ));
        }
        if format.ends_with("duckdb") {
            #[cfg(feature = "ocel-duckdb")]
            return crate::core::event_data::object_centric::ocel_sql::export_ocel_duckdb_to_path(
                self, path,
            )
            .map_err(OCELIOError::from);
            #[cfg(not(feature = "ocel-duckdb"))]
            return Err(OCELIOError::UnsupportedFormat(
                "DuckDB support not enabled".to_string(),
            ));
        }
        let file = std::fs::File::create(path)?;
        let writer = std::io::BufWriter::new(file);
        Self::export_to_writer(self, writer, &format)
    }

    fn export_to_writer_with_options<W: Write>(
        &self,
        #[cfg(feature = "ocel-sqlite")] mut writer: W,
        #[cfg(not(feature = "ocel-sqlite"))] writer: W,
        format: &str,
        _: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        if let Some(inner) = format.strip_suffix(".gz") {
            let mut encoder = flate2::write::GzEncoder::new(
                Box::new(writer) as Box<dyn Write>,
                flate2::Compression::default(),
            );
            self.export_to_writer_with_options(&mut encoder, inner, ())?;
            encoder.finish()?;
            return Ok(());
        }
        if format.ends_with("json") || format.ends_with("jsonocel") {
            crate::core::event_data::object_centric::ocel_json::export_ocel_json_to_writer(
                self, writer,
            )
            .map_err(OCELIOError::Io)
        } else if format.ends_with("xml") || format.ends_with("xmlocel") {
            crate::core::event_data::object_centric::ocel_xml::xml_ocel_export::export_ocel_xml(
                writer, self,
            )
            .map_err(OCELIOError::Xml)
        } else if format.ends_with("ocel.csv") {
            crate::core::event_data::object_centric::ocel_csv::export_ocel_csv(writer, self)
                .map_err(|e| OCELIOError::Other(e.to_string()))
        } else if format.ends_with("sqlite")
            || (format.ends_with("db") && !format.ends_with("duckdb"))
        {
            #[cfg(feature = "ocel-sqlite")]
            {
                let bytes =
                    crate::core::event_data::object_centric::ocel_sql::export_ocel_sqlite_to_vec(
                        self,
                    )
                    .map_err(OCELIOError::from)?;
                writer.write_all(&bytes)?;
                Ok(())
            }
            #[cfg(not(feature = "ocel-sqlite"))]
            return Err(OCELIOError::UnsupportedFormat(
                "SQLite support not enabled".to_string(),
            ));
        } else if format.ends_with("duckdb") {
            Err(OCELIOError::UnsupportedFormat(
                "DuckDB export to writer not supported".to_string(),
            ))
        } else {
            Err(OCELIOError::UnsupportedFormat(format.to_string()))
        }
    }

    fn known_export_formats() -> Vec<ExtensionWithMime> {
        <OCEL as Exportable>::known_export_formats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::OCELAttributeType;

    fn empty_type(name: &str) -> OCELType {
        OCELType {
            name: name.into(),
            attributes: Vec::new(),
        }
    }
    fn ts(s: &str) -> DateTime<FixedOffset> {
        DateTime::parse_from_rfc3339(s).unwrap()
    }

    #[test]
    fn append_resolves_forward_e2o_on_finalize() {
        let mut s: SlimLinkedOCEL = SlimLinkedOCEL::new();
        s.declare_event_type(empty_type("place")).unwrap();
        s.declare_object_type(empty_type("order")).unwrap();
        s.append_event(
            "e1".into(),
            "place",
            ts("2024-01-01T00:00:00Z"),
            Vec::new(),
            vec![OCELRelationship {
                object_id: "o1".into(),
                qualifier: "for".into(),
            }],
        )
        .unwrap();
        s.append_object("o1".into(), "order", Vec::new(), Vec::new())
            .unwrap();
        s.finalize().unwrap();

        let ev = LinkedOCELAccess::get_ev_by_id(&s, "e1").unwrap();
        let ob = LinkedOCELAccess::get_ob_by_id(&s, "o1").unwrap();
        assert_eq!(s.events[ev.into_inner() as usize].relationships.len(), 1);
        assert_eq!(s.events[ev.into_inner() as usize].relationships[0].1, ob);
    }

    #[test]
    fn multi_qualifier_e2o_dedups_reverse_index() {
        let mut s: SlimLinkedOCEL = SlimLinkedOCEL::new();
        s.declare_event_type(empty_type("place")).unwrap();
        s.declare_object_type(empty_type("order")).unwrap();
        s.append_object("o1".into(), "order", Vec::new(), Vec::new())
            .unwrap();
        s.append_event(
            "e1".into(),
            "place",
            ts("2024-01-01T00:00:00Z"),
            Vec::new(),
            vec![
                OCELRelationship {
                    object_id: "o1".into(),
                    qualifier: "for".into(),
                },
                OCELRelationship {
                    object_id: "o1".into(),
                    qualifier: "via".into(),
                },
            ],
        )
        .unwrap();

        let e1 = LinkedOCELAccess::get_ev_by_id(&s, "e1").unwrap();
        let o1 = LinkedOCELAccess::get_ob_by_id(&s, "o1").unwrap();
        assert_eq!(s.objects[o1.into_inner() as usize].e2o_rev, vec![e1]);

        // add_e2o with another qualifier on the same pair stays a single entry.
        s.add_e2o(e1, o1, "alt".into());
        assert_eq!(s.objects[o1.into_inner() as usize].e2o_rev, vec![e1]);
    }

    #[test]
    fn multi_qualifier_o2o_dedups_reverse_index() {
        let mut s: SlimLinkedOCEL = SlimLinkedOCEL::new();
        s.declare_object_type(empty_type("a")).unwrap();
        s.append_object("o2".into(), "a", Vec::new(), Vec::new())
            .unwrap();
        s.append_object(
            "o1".into(),
            "a",
            Vec::new(),
            vec![
                OCELRelationship {
                    object_id: "o2".into(),
                    qualifier: "first".into(),
                },
                OCELRelationship {
                    object_id: "o2".into(),
                    qualifier: "second".into(),
                },
            ],
        )
        .unwrap();

        let o1 = LinkedOCELAccess::get_ob_by_id(&s, "o1").unwrap();
        let o2 = LinkedOCELAccess::get_ob_by_id(&s, "o2").unwrap();
        assert_eq!(s.objects[o2.into_inner() as usize].o2o_rev, vec![o1]);

        s.add_o2o(o1, o2, "third".into());
        assert_eq!(s.objects[o2.into_inner() as usize].o2o_rev, vec![o1]);
    }

    #[test]
    fn reverse_index_one_entry_per_distinct_event() {
        let mut s: SlimLinkedOCEL = SlimLinkedOCEL::new();
        s.declare_event_type(empty_type("e")).unwrap();
        s.declare_object_type(empty_type("o")).unwrap();
        s.append_object("o1".into(), "o", Vec::new(), Vec::new())
            .unwrap();
        for id in ["e3", "e2", "e1"] {
            s.append_event(
                id.into(),
                "e",
                ts("2024-01-01T00:00:00Z"),
                Vec::new(),
                vec![OCELRelationship {
                    object_id: "o1".into(),
                    qualifier: "q".into(),
                }],
            )
            .unwrap();
        }
        let o1 = LinkedOCELAccess::get_ob_by_id(&s, "o1").unwrap();
        let rev = &s.objects[o1.into_inner() as usize].e2o_rev;
        assert!(
            rev.windows(2).all(|w| w[0] < w[1]),
            "e2o_rev must be sorted ascending: {:?}",
            rev
        );
        assert_eq!(rev.len(), 3);
    }

    #[test]
    fn append_resolves_forward_o2o_on_finalize() {
        let mut s: SlimLinkedOCEL = SlimLinkedOCEL::new();
        s.declare_object_type(empty_type("a")).unwrap();
        s.append_object(
            "o1".into(),
            "a",
            Vec::new(),
            vec![OCELRelationship {
                object_id: "o2".into(),
                qualifier: "next".into(),
            }],
        )
        .unwrap();
        s.append_object("o2".into(), "a", Vec::new(), Vec::new())
            .unwrap();
        s.finalize().unwrap();

        let o1 = LinkedOCELAccess::get_ob_by_id(&s, "o1").unwrap();
        let o2 = LinkedOCELAccess::get_ob_by_id(&s, "o2").unwrap();
        let next_q = intern_qualifier(
            &mut s.qualifiers,
            &mut s.qualifier_index,
            &s.hasher,
            "next".to_string(),
        );
        assert_eq!(
            s.objects[o1.into_inner() as usize].relationships,
            vec![(next_q, o2)]
        );
    }

    #[test]
    fn append_unknown_type_auto_declares() {
        let mut s: SlimLinkedOCEL = SlimLinkedOCEL::new();
        s.append_event(
            "e1".into(),
            "missing",
            ts("2024-01-01T00:00:00Z"),
            vec![OCELEventAttribute {
                name: "x".into(),
                value: OCELAttributeValue::Integer(7),
            }],
            Vec::new(),
        )
        .unwrap();
        assert_eq!(s.event_types().len(), 1);
        assert_eq!(s.event_types()[0].name, "missing");
        assert_eq!(s.event_types()[0].attributes.len(), 1);
        assert_eq!(s.event_types()[0].attributes[0].name, "x");
        assert_eq!(s.event_types()[0].attributes[0].value_type, "integer");
    }

    #[test]
    fn append_value_type_mismatch_coerces_when_possible() {
        // Declared `price: float`, `count: integer`, `label: string`.
        // We pass mismatched variants that are all coercible.
        let mut s: SlimLinkedOCEL = SlimLinkedOCEL::new();
        s.declare_event_type(OCELType {
            name: "act".into(),
            attributes: vec![
                OCELTypeAttribute::new("price", &OCELAttributeType::Float),
                OCELTypeAttribute::new("count", &OCELAttributeType::Integer),
                OCELTypeAttribute::new("label", &OCELAttributeType::String),
            ],
        })
        .unwrap();
        s.append_event(
            "e1".into(),
            "act",
            ts("2024-01-01T00:00:00Z"),
            vec![
                OCELEventAttribute {
                    name: "price".into(),
                    value: OCELAttributeValue::Integer(42),
                },
                OCELEventAttribute {
                    name: "count".into(),
                    value: OCELAttributeValue::String("7".into()),
                },
                OCELEventAttribute {
                    name: "label".into(),
                    value: OCELAttributeValue::Integer(99),
                },
            ],
            Vec::new(),
        )
        .unwrap();
        let ev = EventIndex(0);
        assert!(matches!(
            ev.get_attribute_value("price", &s),
            Some(OCELAttributeValue::Float(f)) if (*f - 42.0).abs() < 1e-9
        ));
        assert!(matches!(
            ev.get_attribute_value("count", &s),
            Some(OCELAttributeValue::Integer(7))
        ));
        match ev.get_attribute_value("label", &s) {
            Some(OCELAttributeValue::String(v)) => assert_eq!(v, "99"),
            other => panic!("expected String '99', got {:?}", other),
        }
    }

    #[test]
    fn append_value_type_mismatch_uncoercible_falls_back() {
        // Declared `x: integer`. We pass a String that does not parse to i64.
        // Coercion fails; the value is stored as-is (with a warning to stderr).
        let mut s: SlimLinkedOCEL = SlimLinkedOCEL::new();
        s.declare_event_type(OCELType {
            name: "act".into(),
            attributes: vec![OCELTypeAttribute::new("x", &OCELAttributeType::Integer)],
        })
        .unwrap();
        s.append_event(
            "e1".into(),
            "act",
            ts("2024-01-01T00:00:00Z"),
            vec![OCELEventAttribute {
                name: "x".into(),
                value: OCELAttributeValue::String("not-an-int".into()),
            }],
            Vec::new(),
        )
        .unwrap();
        let ev = EventIndex(0);
        match ev.get_attribute_value("x", &s) {
            Some(OCELAttributeValue::String(v)) => assert_eq!(v, "not-an-int"),
            other => panic!("expected String fallback, got {:?}", other),
        }
    }

    #[test]
    fn append_missing_declared_attr_reads_as_null() {
        // Declared `x` and `y`; event only has `x`. Reading `y` returns Null.
        let mut s: SlimLinkedOCEL = SlimLinkedOCEL::new();
        s.declare_event_type(OCELType {
            name: "act".into(),
            attributes: vec![
                OCELTypeAttribute::new("x", &OCELAttributeType::Integer),
                OCELTypeAttribute::new("y", &OCELAttributeType::String),
            ],
        })
        .unwrap();
        s.append_event(
            "e1".into(),
            "act",
            ts("2024-01-01T00:00:00Z"),
            vec![OCELEventAttribute {
                name: "x".into(),
                value: OCELAttributeValue::Integer(7),
            }],
            Vec::new(),
        )
        .unwrap();
        let ev = EventIndex(0);
        let fat = ev.fat_ev(&s);
        assert_eq!(fat.attributes.len(), 2);
        assert_eq!(fat.attributes[0].name, "x");
        assert!(matches!(
            fat.attributes[0].value,
            OCELAttributeValue::Integer(7)
        ));
        assert_eq!(fat.attributes[1].name, "y");
        assert!(matches!(fat.attributes[1].value, OCELAttributeValue::Null));
    }

    #[test]
    fn declare_after_append_appends_missing_attrs_only() {
        let mut s: SlimLinkedOCEL = SlimLinkedOCEL::new();
        // Auto-grow infers `first: integer` from the value variant.
        s.append_event(
            "e1".into(),
            "act",
            ts("2024-01-01T00:00:00Z"),
            vec![OCELEventAttribute {
                name: "first".into(),
                value: OCELAttributeValue::Integer(1),
            }],
            Vec::new(),
        )
        .unwrap();
        // Real declaration: declares `first` as `float` (overriding the inferred `integer`)
        // and adds `second`. Declared name order is reversed relative to what we observed.
        s.declare_event_type(OCELType {
            name: "act".into(),
            attributes: vec![
                OCELTypeAttribute::new("second", &OCELAttributeType::String),
                OCELTypeAttribute::new("first", &OCELAttributeType::Float),
            ],
        })
        .unwrap();
        let attrs = &s.event_types()[0].attributes;
        assert_eq!(attrs.len(), 2);
        // Existing position preserved: `first` stays at slot 0 (where e1 stored its value).
        assert_eq!(attrs[0].name, "first");
        // Declaration overrides the inferred metadata.
        assert_eq!(attrs[0].value_type, "float");
        // New attribute appended at the end.
        assert_eq!(attrs[1].name, "second");
        assert_eq!(attrs[1].value_type, "string");
        // Stored value is not re-coerced to match the new declared type: the variant stays
        // `Integer` even though the schema now says `float`. Documented on `declare_or_merge_type`.
        let ev0 = &s.events[0];
        assert!(matches!(ev0.attributes[0], OCELAttributeValue::Integer(1)));
    }

    #[test]
    fn iter_events_of_type_matches_filter_for_slim_and_ocel() {
        let mut s: SlimLinkedOCEL = SlimLinkedOCEL::new();
        s.declare_event_type(empty_type("a")).unwrap();
        s.declare_event_type(empty_type("b")).unwrap();
        s.declare_object_type(empty_type("o1")).unwrap();
        s.declare_object_type(empty_type("o2")).unwrap();
        for (i, t) in [("a", "e1"), ("b", "e2"), ("a", "e3"), ("b", "e4")]
            .iter()
            .enumerate()
        {
            s.append_event(
                t.1.into(),
                t.0,
                ts(&format!("2024-01-0{}T00:00:00Z", i + 1)),
                Vec::new(),
                Vec::new(),
            )
            .unwrap();
        }
        for (i, t) in [("o1", "x"), ("o2", "y"), ("o1", "z")].iter().enumerate() {
            s.append_object(
                t.1.into(),
                t.0,
                vec![OCELObjectAttribute {
                    name: "k".into(),
                    value: OCELAttributeValue::Integer(i as i64),
                    time: ts("2024-01-01T00:00:00Z"),
                }],
                Vec::new(),
            )
            .unwrap();
        }
        s.finalize().unwrap();

        let slim_a: Vec<String> = s.iter_events_of_type("a").map(|e| e.id.clone()).collect();
        assert_eq!(slim_a, vec!["e1".to_string(), "e3".to_string()]);
        let slim_o1: Vec<String> = s.iter_objects_of_type("o1").map(|o| o.id.clone()).collect();
        assert_eq!(slim_o1, vec!["x".to_string(), "z".to_string()]);
        assert_eq!(s.iter_events_of_type("missing").count(), 0);

        // The default impl on `OCEL` filters; the `SlimLinkedOCEL` override walks per-type
        // indices. Both must yield the same set.
        let ocel = s.construct_ocel();
        let ocel_a: Vec<String> = ocel
            .iter_events_of_type("a")
            .map(|e| e.id.clone())
            .collect();
        let ocel_o1: Vec<String> = ocel
            .iter_objects_of_type("o1")
            .map(|o| o.id.clone())
            .collect();
        let mut slim_a_sorted = slim_a;
        let mut ocel_a_sorted = ocel_a;
        slim_a_sorted.sort();
        ocel_a_sorted.sort();
        assert_eq!(slim_a_sorted, ocel_a_sorted);
        let mut slim_o1_sorted = slim_o1;
        let mut ocel_o1_sorted = ocel_o1;
        slim_o1_sorted.sort();
        ocel_o1_sorted.sort();
        assert_eq!(slim_o1_sorted, ocel_o1_sorted);
    }

    #[test]
    fn append_attributes_become_positional() {
        let mut s: SlimLinkedOCEL = SlimLinkedOCEL::new();
        s.declare_event_type(OCELType {
            name: "act".into(),
            attributes: vec![
                OCELTypeAttribute::new("first", &OCELAttributeType::String),
                OCELTypeAttribute::new("second", &OCELAttributeType::Integer),
            ],
        })
        .unwrap();
        s.append_event(
            "e1".into(),
            "act",
            ts("2024-01-01T00:00:00Z"),
            vec![
                OCELEventAttribute {
                    name: "second".into(),
                    value: OCELAttributeValue::Integer(42),
                },
                OCELEventAttribute {
                    name: "first".into(),
                    value: OCELAttributeValue::String("hi".into()),
                },
            ],
            Vec::new(),
        )
        .unwrap();
        let ev = &s.events[0];
        assert_eq!(ev.attributes[0], OCELAttributeValue::String("hi".into()));
        assert_eq!(ev.attributes[1], OCELAttributeValue::Integer(42));
    }
}
