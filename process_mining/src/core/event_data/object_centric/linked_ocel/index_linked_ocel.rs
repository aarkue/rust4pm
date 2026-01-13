use std::{
    borrow::{Borrow, Cow},
    collections::{HashMap, HashSet},
    io::{Read, Write},
    ops::Index,
    path::Path,
};

use binding_macros::RegistryEntity;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::event_data::object_centric::io::OCELIOError;
use crate::core::event_data::object_centric::ocel_struct::{OCELEvent, OCELObject, OCELType, OCEL};
use crate::core::io::{Exportable, Importable};

use super::LinkedOCELAccess;

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
    /// Retrieve inner index value
    ///
    /// Warning: Only use carefully, as wrong usage can lead to invalid `EventIndex` references, even when using only a single OCEL
    pub fn into_inner(self) -> usize {
        self.0
    }
}

/// An Object Index
///
/// Points to an object in the context of a given OCEL
#[derive(
    PartialEq, Eq, Hash, Clone, Copy, Debug, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
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

///
/// An [`OCEL`] linked through event and object indices
///
/// Provides an easy way to access event-to-object and object-to-object relations, as well as their reverse relations.
///
/// Additionally, the representation of events and objects as indices of the OCEL allows still fully accessing the underlying [`OCEL`]
/// while event/object indices are small [`usize`] and have only very little overhead.
///
/// Implements the [`LinkedOCELAccess`] trait.
///
#[derive(Debug, Clone, RegistryEntity, Serialize, Deserialize, JsonSchema)]
pub struct IndexLinkedOCEL {
    ocel: OCEL,
    event_ids_to_index: HashMap<String, EventIndex>,
    object_ids_to_index: HashMap<String, ObjectIndex>,
    /// Events per Event Type
    pub events_per_type: HashMap<String, Vec<EventIndex>>,
    /// Reverse event-to-object relationships per event type
    ///
    /// First level: event type, second level: reverse e2o (object index to set of event indices)
    pub e2o_rev_et: HashMap<String, HashMap<ObjectIndex, HashSet<EventIndex>>>,
    /// List of object indices per object type
    pub objects_per_type: HashMap<String, Vec<ObjectIndex>>,
    e2o_rel: Vec<Vec<(String, ObjectIndex)>>,
    e2o_set: Vec<HashSet<ObjectIndex>>,
    o2o_rel: Vec<Vec<(String, ObjectIndex)>>,
    e2o_rel_rev: Vec<Vec<(String, EventIndex)>>,
    o2o_rel_rev: Vec<Vec<(String, ObjectIndex)>>,
}

impl IndexLinkedOCEL {
    /// Process an [`OCEL`] into a [`IndexLinkedOCEL`]
    ///
    /// The [`IndexLinkedOCEL`] takes ownership of the [`OCEL`]
    pub fn from_ocel(ocel: OCEL) -> Self {
        Self::from(ocel)
    }

    /// Get the inner [`OCEL`] of the [`IndexLinkedOCEL`]
    pub fn into_inner(self) -> OCEL {
        self.ocel
    }

    /// Get a immutable reference to the inner [`OCEL`]
    ///
    pub fn get_ocel_ref(&self) -> &OCEL {
        &self.ocel
    }

    /// Get a mutable reference of the inner [`OCEL`]
    ///
    /// Carefull! Changing the inner ocel might render event/object references or relationships invalid or inconsistent.
    ///
    /// Consider alternatively calling [`IndexLinkedOCEL::into_inner`], modifying the returned [`OCEL`] and then re-processing it again ([`IndexLinkedOCEL::from_ocel`])
    pub fn get_ocel_mut(&mut self) -> &mut OCEL {
        &mut self.ocel
    }

    /// Get all objects involved with an event as a [`HashSet`]
    ///
    pub fn get_e2o_set(&self, index: &EventIndex) -> &HashSet<ObjectIndex> {
        &self.e2o_set[index.0]
    }

    /// Get event index by ID
    pub fn get_ev_index(&self, id: impl AsRef<str>) -> Option<EventIndex> {
        self.event_ids_to_index.get(id.as_ref()).copied()
    }
    /// Get object index by ID
    pub fn get_ob_index(&self, id: impl AsRef<str>) -> Option<ObjectIndex> {
        self.object_ids_to_index.get(id.as_ref()).copied()
    }
}

impl Index<EventIndex> for IndexLinkedOCEL {
    type Output = OCELEvent;
    fn index(&self, index: EventIndex) -> &Self::Output {
        &self.ocel.events[index.0]
    }
}
impl Index<&EventIndex> for IndexLinkedOCEL {
    type Output = OCELEvent;
    fn index(&self, index: &EventIndex) -> &Self::Output {
        &self.ocel.events[index.0]
    }
}
impl Index<EventIndex> for &IndexLinkedOCEL {
    type Output = OCELEvent;
    fn index(&self, index: EventIndex) -> &Self::Output {
        &self.ocel.events[index.0]
    }
}
impl Index<&EventIndex> for &IndexLinkedOCEL {
    type Output = OCELEvent;
    fn index(&self, index: &EventIndex) -> &Self::Output {
        &self.ocel.events[index.0]
    }
}

impl Index<ObjectIndex> for IndexLinkedOCEL {
    type Output = OCELObject;
    fn index(&self, index: ObjectIndex) -> &Self::Output {
        &self.ocel.objects[index.0]
    }
}

impl Index<&ObjectIndex> for IndexLinkedOCEL {
    type Output = OCELObject;
    fn index(&self, index: &ObjectIndex) -> &Self::Output {
        &self.ocel.objects[index.0]
    }
}

impl Index<ObjectIndex> for &IndexLinkedOCEL {
    type Output = OCELObject;
    fn index(&self, index: ObjectIndex) -> &Self::Output {
        &self.ocel.objects[index.0]
    }
}

impl Index<&ObjectIndex> for &IndexLinkedOCEL {
    type Output = OCELObject;
    fn index(&self, index: &ObjectIndex) -> &Self::Output {
        &self.ocel.objects[index.0]
    }
}

impl From<OCEL> for IndexLinkedOCEL {
    fn from(mut ocel: OCEL) -> Self {
        // Sort events so that the index order corresponds to the timstamp order
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
        let mut e2o_rel_rev: Vec<Vec<(String, EventIndex)>> = vec![Vec::new(); ocel.objects.len()];

        let mut e2o_rev_et: HashMap<String, HashMap<ObjectIndex, HashSet<EventIndex>>> = ocel
            .event_types
            .iter()
            .map(|et| (et.name.clone(), HashMap::new()))
            .collect();
        let e2o_rel = ocel
            .events
            .iter()
            .enumerate()
            .map(|(e_index, e)| {
                let e_id: EventIndex = EventIndex(e_index);
                e.relationships
                    .iter()
                    .flat_map(|rel| {
                        let obj_id = *object_ids_to_index.get(&rel.object_id)?;
                        let qualifier = rel.qualifier.clone();
                        let ev_type = e2o_rev_et.get_mut(&e.event_type)?;
                        ev_type.entry(obj_id).or_default().insert(e_id);
                        e2o_rel_rev[obj_id.0].push((qualifier.clone(), e_id));
                        Some((qualifier, obj_id))
                    })
                    .collect()
            })
            .collect::<Vec<_>>();

        let e2o_set = ocel
            .events
            .iter()
            .map(|e| {
                e.relationships
                    .iter()
                    .flat_map(|rel| {
                        let obj_id = *object_ids_to_index.get(&rel.object_id)?;
                        // let ob = objects.get(&((&rel.object_id).into()))?;
                        Some(obj_id)
                    })
                    .collect()
            })
            .collect();

        let mut o2o_rel_rev: Vec<Vec<(String, ObjectIndex)>> = vec![Vec::new(); ocel.objects.len()];

        let o2o_rel = ocel
            .objects
            .iter()
            .enumerate()
            .map(|(o_index, o)| {
                let o_id: ObjectIndex = ObjectIndex(o_index);
                o.relationships
                    .iter()
                    .flat_map(|rel| {
                        let qualifier = rel.qualifier.clone();
                        let obj2_id = *object_ids_to_index.get(&rel.object_id)?;
                        o2o_rel_rev[obj2_id.0].push((qualifier, o_id));
                        Some(((&rel.qualifier).into(), obj2_id))
                    })
                    .collect()
            })
            .collect();

        let events_per_type = ocel
            .event_types
            .iter()
            .map(|et| {
                (
                    et.name.clone(),
                    ocel.events
                        .iter()
                        .enumerate()
                        .filter_map(|(index, e)| {
                            if e.event_type == et.name {
                                Some(EventIndex(index))
                            } else {
                                None
                            }
                        })
                        .collect(),
                )
            })
            .collect();

        let objects_per_type = ocel
            .object_types
            .iter()
            .map(|et| {
                (
                    et.name.clone(),
                    ocel.objects
                        .iter()
                        .enumerate()
                        .filter_map(|(index, e)| {
                            if e.object_type == et.name {
                                Some(ObjectIndex(index))
                            } else {
                                None
                            }
                        })
                        .collect(),
                )
            })
            .collect();

        Self {
            ocel,
            event_ids_to_index,
            object_ids_to_index,
            events_per_type,
            objects_per_type,
            e2o_rev_et,
            e2o_rel,
            e2o_set,
            o2o_rel,
            e2o_rel_rev,
            o2o_rel_rev,
        }
    }
}

impl<'a> LinkedOCELAccess<'a> for IndexLinkedOCEL {
    type EventRepr = EventIndex;
    type ObjectRepr = ObjectIndex;

    fn get_evs_of_type(&'a self, ev_type: &'_ str) -> impl Iterator<Item = &'a EventIndex> {
        self.events_per_type.get(ev_type).into_iter().flatten()
    }

    fn get_obs_of_type(&'a self, ob_type: &'_ str) -> impl Iterator<Item = &'a ObjectIndex> {
        self.objects_per_type.get(ob_type).into_iter().flatten()
    }

    fn get_full_ev(&'a self, index: impl Borrow<EventIndex>) -> Cow<'a, OCELEvent> {
        Cow::Borrowed(&self.ocel.events[index.borrow().0])
    }

    fn get_full_ob(&'a self, index: impl Borrow<ObjectIndex>) -> Cow<'a, OCELObject> {
        Cow::Borrowed(&self.ocel.objects[index.borrow().0])
    }

    fn get_e2o(
        &'a self,
        index: impl Borrow<EventIndex>,
    ) -> impl Iterator<Item = (&'a str, &'a ObjectIndex)> {
        self.e2o_rel
            .get(index.borrow().0)
            .into_iter()
            .flatten()
            .map(|(q, o)| (q.as_str(), o))
    }

    fn get_e2o_rev(
        &'a self,
        index: impl Borrow<ObjectIndex>,
    ) -> impl Iterator<Item = (&'a str, &'a EventIndex)> {
        self.e2o_rel_rev
            .get(index.borrow().0)
            .into_iter()
            .flatten()
            .map(|(q, e)| (q.as_str(), e))
    }

    fn get_o2o(
        &'a self,
        index: impl Borrow<ObjectIndex>,
    ) -> impl Iterator<Item = (&'a str, &'a ObjectIndex)> {
        self.o2o_rel
            .get(index.borrow().0)
            .into_iter()
            .flatten()
            .map(|(q, o)| (q.as_str(), o))
    }

    fn get_o2o_rev(
        &'a self,
        index: impl Borrow<ObjectIndex>,
    ) -> impl Iterator<Item = (&'a str, &'a ObjectIndex)> {
        self.o2o_rel_rev
            .get(index.borrow().0)
            .into_iter()
            .flatten()
            .map(|(q, e)| (q.as_str(), e))
    }

    fn get_ev_types(&'a self) -> impl Iterator<Item = &'a str> {
        self.events_per_type.keys().map(|k| k.as_str())
    }

    fn get_ob_types(&'a self) -> impl Iterator<Item = &'a str> {
        self.objects_per_type.keys().map(|k| k.as_str())
    }

    fn get_all_evs(&'a self) -> impl Iterator<Item = EventIndex> {
        (0..self.ocel.events.len()).map(EventIndex)
    }

    fn get_all_obs(&'a self) -> impl Iterator<Item = ObjectIndex> {
        (0..self.ocel.objects.len()).map(ObjectIndex)
    }

    fn get_ev_type(&'a self, ev_type: impl AsRef<str>) -> Option<&'a OCELType> {
        self.ocel
            .event_types
            .iter()
            .find(|et| et.name == ev_type.as_ref())
    }

    fn get_ob_type(&'a self, ob_type: impl AsRef<str>) -> Option<&'a OCELType> {
        self.ocel
            .object_types
            .iter()
            .find(|ot| ot.name == ob_type.as_ref())
    }

    fn get_ob_type_of(&'a self, object: impl Borrow<Self::ObjectRepr>) -> &'a str {
        &self.ocel.objects[object.borrow().0].object_type
    }

    fn get_ev_type_of(&'a self, event: impl Borrow<Self::EventRepr>) -> &'a str {
        &self.ocel.events[event.borrow().0].event_type
    }

    fn get_ev_attrs(&'a self, ev: impl Borrow<Self::EventRepr>) -> impl Iterator<Item = &'a str> {
        self.ocel.events[ev.borrow().0]
            .attributes
            .iter()
            .map(|a| a.name.as_str())
    }

    fn get_ev_attr_val(
        &'a self,
        ev: impl Borrow<Self::EventRepr>,
        attr_name: impl AsRef<str>,
    ) -> Option<&'a crate::core::event_data::object_centric::OCELAttributeValue> {
        let attr_name = attr_name.as_ref();
        self.ocel.events[ev.borrow().0]
            .attributes
            .iter()
            .find(|a| a.name == attr_name)
            .map(|a| &a.value)
    }

    fn get_ob_attrs(&'a self, ob: impl Borrow<Self::ObjectRepr>) -> impl Iterator<Item = &'a str> {
        self.ocel.objects[ob.borrow().0]
            .attributes
            .iter()
            .map(|a| a.name.as_str())
    }

    fn get_ob_attr_vals(
        &'a self,
        ob: impl Borrow<Self::ObjectRepr>,
        attr_name: impl AsRef<str>,
    ) -> impl Iterator<
        Item = (
            &'a chrono::DateTime<chrono::FixedOffset>,
            &'a crate::core::event_data::object_centric::OCELAttributeValue,
        ),
    > {
        let attr_name = attr_name.as_ref();
        self.ocel.objects[ob.borrow().0]
            .attributes
            .iter()
            .filter(|a| a.name == attr_name)
            .map(|a| (&a.time, &a.value))
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn get_ob_id(&'a self, ob: impl Borrow<Self::ObjectRepr>) -> &'a str {
        self.ocel.objects[ob.borrow().0].id.as_str()
    }

    fn get_ev_id(&'a self, ev: impl Borrow<Self::EventRepr>) -> &'a str {
        self.ocel.objects[ev.borrow().0].id.as_str()
    }

    fn get_ev_by_id(&'a self, ev_id: impl AsRef<str>) -> Option<Self::EventRepr> {
        self.event_ids_to_index.get(ev_id.as_ref()).copied()
    }

    fn get_ob_by_id(&'a self, ob_id: impl AsRef<str>) -> Option<Self::ObjectRepr> {
        self.object_ids_to_index.get(ob_id.as_ref()).copied()
    }

    fn get_ev_time(
        &'a self,
        ev: impl Borrow<Self::EventRepr>,
    ) -> &'a chrono::DateTime<chrono::FixedOffset> {
        &self.ocel.events[ev.borrow().0].time
    }
}

impl Importable for IndexLinkedOCEL {
    type Error = OCELIOError;

    fn import_from_reader<R: Read>(reader: R, format: &str) -> Result<Self, Self::Error> {
        if format == "json" || format.ends_with(".json") {
            let reader = std::io::BufReader::new(reader);
            let res: Self = serde_json::from_reader(reader)?;
            Ok(res)
        } else {
            let ocel = OCEL::import_from_reader(reader, format)?;
            Ok(IndexLinkedOCEL::from_ocel(ocel))
        }
    }

    fn infer_format(path: &Path) -> Option<String> {
        let p = path.to_string_lossy().to_lowercase();
        if p.ends_with(".ocel.json") {
            Some("ocel.json".to_string())
        } else if p.ends_with(".json") {
            Some("json".to_string())
        } else {
            <OCEL as Importable>::infer_format(path)
        }
    }
}

impl Exportable for IndexLinkedOCEL {
    type Error = OCELIOError;

    fn export_to_writer<W: Write>(&self, writer: W, format: &str) -> Result<(), Self::Error> {
        if format == "json" || format.ends_with(".json") {
            serde_json::to_writer(writer, self)?;
            Ok(())
        } else {
            self.ocel.export_to_writer(writer, format)
        }
    }
}
#[cfg(test)]
mod tests {

    use crate::{
        core::event_data::object_centric::ocel_xml::xml_ocel_import::import_ocel_xml_path,
        test_utils::get_test_data_path,
    };

    use super::*;

    #[test]
    fn test_indexing() {
        let ocel = import_ocel_xml_path(
            get_test_data_path()
                .join("ocel")
                .join("order-management.xml"),
        )
        .unwrap();
        let locel = IndexLinkedOCEL::from_ocel(ocel);
        let locel_ref = &locel;
        if let Some(ev_index) = locel_ref.get_all_evs().next() {
            let ev1: &OCELEvent = &locel[ev_index];
            let ev2 = &locel[ev_index];
            let ev3 = &locel_ref[ev_index];
            let ev4 = &locel_ref[ev_index];
            assert_eq!(ev1, ev2);
            assert_eq!(ev1, ev3);
            assert_eq!(ev1, ev4);
        };
        if let Some(ob_index) = locel_ref.get_all_obs().next() {
            let ev1: &OCELObject = &locel[ob_index];
            let ev2 = &locel[ob_index];
            let ev3 = &locel_ref[ob_index];
            let ev4 = &locel_ref[ob_index];
            assert_eq!(ev1, ev2);
            assert_eq!(ev1, ev3);
            assert_eq!(ev1, ev4);
        };
    }
}
