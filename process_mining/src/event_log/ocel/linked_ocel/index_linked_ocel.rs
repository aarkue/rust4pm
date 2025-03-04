use std::collections::{HashMap, HashSet};

use crate::{
    ocel::ocel_struct::{OCELEvent, OCELObject},
    OCEL,
};

use super::LinkedOCELAccess;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug, PartialOrd, Ord)]
/// An Event Index
///
/// Points to an event in the context of a given OCEL
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

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug, PartialOrd, Ord)]
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

#[derive(Debug, Clone)]
///
/// An OCEL linked through event and object indices
///
/// Provides an easy way to access event-to-object and object-to-object relations, as well as their reverse relations.
///
/// Additionally, the representation of events and objects as indices of the OCEL allows still fully accessing the underlying [`OCEL`]
/// while event/object indices are small [`usize`] and have only very little overhead.
///
/// Implements the [`LinkedOCELAccess`] trait.
///
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
    objects_per_type: HashMap<String, Vec<ObjectIndex>>,
    e2o_rel: HashMap<EventIndex, Vec<(String, ObjectIndex)>>,
    e2o_set: HashMap<EventIndex, HashSet<ObjectIndex>>,
    o2o_rel: HashMap<ObjectIndex, Vec<(String, ObjectIndex)>>,
    e2o_rel_rev: HashMap<ObjectIndex, Vec<(String, EventIndex)>>,
    o2o_rel_rev: HashMap<ObjectIndex, Vec<(String, ObjectIndex)>>,
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
        self.e2o_set.get(index).unwrap()
    }
}

impl From<OCEL> for IndexLinkedOCEL {
    fn from(ocel: OCEL) -> Self {
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
        let mut e2o_rel_rev: HashMap<ObjectIndex, Vec<(String, EventIndex)>> = HashMap::new();

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
                (
                    e_id,
                    e.relationships
                        .iter()
                        .flat_map(|rel| {
                            let obj_id = *object_ids_to_index.get(&rel.object_id)?;
                            let qualifier = rel.qualifier.clone();
                            e2o_rev_et
                                .get_mut(&e.event_type)
                                .unwrap()
                                .entry(obj_id)
                                .or_default()
                                .insert(e_id);
                            // ((e.event_type.clone()).or_default().insert(e_id.clone());
                            e2o_rel_rev
                                .entry(obj_id)
                                .or_default()
                                .push((qualifier.clone(), e_id));
                            // let ob = objects.get(&((&rel.object_id).into()))?;
                            Some((qualifier, obj_id))
                        })
                        .collect(),
                )
            })
            .collect();

        let e2o_set: HashMap<EventIndex, HashSet<ObjectIndex>> = ocel
            .events
            .iter()
            .enumerate()
            .map(|(e_index, e)| {
                let e_id: EventIndex = EventIndex(e_index);
                (
                    e_id,
                    e.relationships
                        .iter()
                        .flat_map(|rel| {
                            let obj_id = *object_ids_to_index.get(&rel.object_id)?;
                            // let ob = objects.get(&((&rel.object_id).into()))?;
                            Some(obj_id)
                        })
                        .collect(),
                )
            })
            .collect();

        let mut o2o_rel_rev: HashMap<ObjectIndex, Vec<(String, ObjectIndex)>> = HashMap::new();

        let o2o_rel = ocel
            .objects
            .iter()
            .enumerate()
            .map(|(o_index, o)| {
                let o_id: ObjectIndex = ObjectIndex(o_index);
                (
                    o_id,
                    o.relationships
                        .iter()
                        .flat_map(|rel| {
                            let qualifier = rel.qualifier.clone();
                            let obj2_id = *object_ids_to_index.get(&rel.object_id)?;
                            o2o_rel_rev
                                .entry(obj2_id)
                                .or_default()
                                .push((qualifier.clone(), o_id));
                            Some(((&rel.qualifier).into(), obj2_id))
                        })
                        .collect(),
                )
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

impl<'a> LinkedOCELAccess<'a, EventIndex, ObjectIndex, EventIndex, ObjectIndex>
    for IndexLinkedOCEL
{
    fn get_evs_of_type(&'a self, ev_type: &'_ str) -> impl Iterator<Item = &'a EventIndex> {
        self.events_per_type.get(ev_type).into_iter().flatten()
    }

    fn get_obs_of_type(&'a self, ob_type: &'_ str) -> impl Iterator<Item = &'a ObjectIndex> {
        self.objects_per_type.get(ob_type).into_iter().flatten()
    }

    fn get_ev(&'a self, index: &EventIndex) -> &'a OCELEvent {
        &self.ocel.events[index.0]
    }

    fn get_ob(&'a self, index: &ObjectIndex) -> &'a OCELObject {
        &self.ocel.objects[index.0]
    }

    fn get_e2o(&'a self, index: &EventIndex) -> impl Iterator<Item = (&'a str, &'a ObjectIndex)> {
        self.e2o_rel
            .get(index)
            .into_iter()
            .flatten()
            .map(|(q, o)| (q.as_str(), o))
    }

    fn get_e2o_rev(
        &'a self,
        index: &ObjectIndex,
    ) -> impl Iterator<Item = (&'a str, &'a EventIndex)> {
        self.e2o_rel_rev
            .get(index)
            .into_iter()
            .flatten()
            .map(|(q, e)| (q.as_str(), e))
    }

    fn get_o2o(&'a self, index: &ObjectIndex) -> impl Iterator<Item = (&'a str, &'a ObjectIndex)> {
        self.o2o_rel
            .get(index)
            .into_iter()
            .flatten()
            .map(|(q, o)| (q.as_str(), o))
    }

    fn get_o2o_rev(
        &'a self,
        index: &ObjectIndex,
    ) -> impl Iterator<Item = (&'a str, &'a ObjectIndex)> {
        self.o2o_rel_rev
            .get(index)
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

    fn get_all_evs(&'a self) -> impl Iterator<Item = &'a OCELEvent> {
        self.ocel.events.iter()
    }

    fn get_all_obs(&'a self) -> impl Iterator<Item = &'a OCELObject> {
        self.ocel.objects.iter()
    }

    fn get_all_evs_ref(&'a self) -> impl Iterator<Item = &'a EventIndex> {
        self.event_ids_to_index.values()
    }

    fn get_all_obs_ref(&'a self) -> impl Iterator<Item = &'a ObjectIndex> {
        self.object_ids_to_index.values()
    }
}
