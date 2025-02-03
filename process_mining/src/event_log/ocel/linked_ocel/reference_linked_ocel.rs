use std::collections::HashMap;

use crate::{ocel::ocel_struct::{OCELEvent, OCELObject}, OCEL};

use super::LinkedOCELAccess;

impl<'a> LinkedOCELAccess<'a, EventID<'a>, ObjectID<'a>, OCELEvent, OCELObject>
    for ReferenceLinkedOCEL<'a>
{
    fn get_evs_of_type(&'a self, ev_type: &'_ str) -> impl Iterator<Item = &'a OCELEvent> {
        self.events_per_type
            .get(ev_type)
            .into_iter()
            .flatten()
            .cloned()
    }

    fn get_obs_of_type(&'a self, ob_type: &'_ str) -> impl Iterator<Item = &'a OCELObject> {
        self.objects_per_type
            .get(ob_type)
            .into_iter()
            .flatten()
            .cloned()
    }

    fn get_ev(&'a self, ev_id: &EventID<'a>) -> &'a OCELEvent {
        self.events.get(ev_id).unwrap()
    }

    fn get_ob(&'a self, ob_id: &ObjectID<'a>) -> &'a OCELObject {
        self.objects.get(ob_id).unwrap()
    }

    fn get_e2o(&'a self, index: &EventID<'a>) -> impl Iterator<Item = (&'a str, &'a OCELObject)> {
        self.e2o_rel.get(index).into_iter().flatten().cloned()
    }

    fn get_e2o_rev(
        &'a self,
        index: &ObjectID<'a>,
    ) -> impl Iterator<Item = (&'a str, &'a OCELEvent)> {
        self.e2o_rel_rev.get(index).into_iter().flatten().cloned()
    }

    fn get_o2o(&'a self, index: &ObjectID<'a>) -> impl Iterator<Item = (&'a str, &'a OCELObject)> {
        self.o2o_rel.get(index).into_iter().flatten().cloned()
    }

    fn get_o2o_rev(
        &'a self,
        index: &ObjectID<'a>,
    ) -> impl Iterator<Item = (&'a str, &'a OCELObject)> {
        self.o2o_rel_rev.get(index).into_iter().flatten().cloned()
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectID<'a>(&'a str);

impl<'a> From<&'a OCELObject> for ObjectID<'a> {
    fn from(value: &'a OCELObject) -> Self {
        Self(value.id.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventID<'a>(&'a str);

impl<'a> From<&'a OCELEvent> for EventID<'a> {
    fn from(value: &'a OCELEvent) -> Self {
        Self(value.id.as_str())
    }
}
struct ReferenceLinkedOCEL<'a> {
    ocel: &'a OCEL,
    events: HashMap<EventID<'a>, &'a OCELEvent>,
    objects: HashMap<ObjectID<'a>, &'a OCELObject>,
    events_per_type: HashMap<&'a str, Vec<&'a OCELEvent>>,
    objects_per_type: HashMap<&'a str, Vec<&'a OCELObject>>,
    e2o_rel: HashMap<EventID<'a>, Vec<(&'a str, &'a OCELObject)>>,
    o2o_rel: HashMap<ObjectID<'a>, Vec<(&'a str, &'a OCELObject)>>,
    e2o_rel_rev: HashMap<ObjectID<'a>, Vec<(&'a str, &'a OCELEvent)>>,
    o2o_rel_rev: HashMap<ObjectID<'a>, Vec<(&'a str, &'a OCELObject)>>,
}

impl<'a> From<&'a OCEL> for ReferenceLinkedOCEL<'a> {
    fn from(ocel: &'a OCEL) -> Self {
        let events: HashMap<_, _> = ocel.events.iter().map(|e| (EventID(&e.id), e)).collect();
        let objects: HashMap<_, _> = ocel.objects.iter().map(|o| (ObjectID(&o.id), o)).collect();
        let mut e2o_rel_rev: HashMap<ObjectID<'a>, Vec<(&str, &OCELEvent)>> = HashMap::new();
        let e2o_rel = events
            .values()
            .map(|e| {
                let e_id: EventID<'_> = EventID(&e.id);
                (
                    e_id,
                    e.relationships
                        .iter()
                        .flat_map(|rel| {
                            let obj_id: ObjectID<'_> = ObjectID(&rel.object_id);
                            let qualifier = rel.qualifier.as_str();
                            e2o_rel_rev.entry(obj_id).or_default().push((qualifier, &e));
                            let ob = objects.get(&(ObjectID(&rel.object_id)))?;
                            Some((qualifier, *ob))
                        })
                        .collect(),
                )
            })
            .collect();

        let mut o2o_rel_rev: HashMap<ObjectID<'_>, Vec<(&'a str, &OCELObject)>> = HashMap::new();

        let o2o_rel = objects
            .values()
            .map(|o| {
                (
                    ObjectID(&o.id),
                    o.relationships
                        .iter()
                        .flat_map(|rel| {
                            let qualifier = (&rel.qualifier).as_str();
                            let obj2_id: ObjectID<'_> = ObjectID(&rel.object_id);
                            o2o_rel_rev
                                .entry(obj2_id)
                                .or_default()
                                .push((qualifier, &o));
                            let ob = objects.get(&ObjectID(&rel.object_id))?;
                            Some((qualifier, *ob))
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
                    et.name.as_str(),
                    ocel.events
                        .iter()
                        .enumerate()
                        .filter_map(|(index, e)| {
                            if e.event_type == et.name {
                                Some(e)
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
                    et.name.as_str(),
                    ocel.objects
                        .iter()
                        .enumerate()
                        .filter_map(|(index, e)| {
                            if e.object_type == et.name {
                                Some(e)
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
            events,
            objects,
            events_per_type,
            objects_per_type,
            e2o_rel,
            o2o_rel,
            e2o_rel_rev,
            o2o_rel_rev,
        }
    }
}

struct OwnedReferenceLinkedOcel<'a> {
    ocel: OCEL,
    pub linked_ocel: ReferenceLinkedOCEL<'a>,
}

impl<'a> From<OCEL> for OwnedReferenceLinkedOcel<'a> {
    fn from(ocel: OCEL) -> Self {
        let ocel_ref = unsafe { &*(&ocel as *const OCEL) };
        OwnedReferenceLinkedOcel {
            ocel,
            linked_ocel: (ocel_ref).into(),
        }
    }
}

impl<'a> LinkedOCELAccess<'a, EventID<'a>, ObjectID<'a>, OCELEvent, OCELObject>
    for OwnedReferenceLinkedOcel<'a>
{
    fn get_evs_of_type(&'a self, ev_type: &'_ str) -> impl Iterator<Item = &'a OCELEvent> {
        self.linked_ocel.get_evs_of_type(ev_type)
    }

    fn get_obs_of_type(&'a self, ob_type: &'_ str) -> impl Iterator<Item = &'a OCELObject> {
        self.linked_ocel.get_obs_of_type(ob_type)
    }

    fn get_ev(&'a self, index: &EventID<'a>) -> &'a OCELEvent {
        self.linked_ocel.get_ev(index)
    }

    fn get_ob(&'a self, index: &ObjectID<'a>) -> &'a OCELObject {
        self.linked_ocel.get_ob(index)
    }

    fn get_e2o(&'a self, index: &EventID<'a>) -> impl Iterator<Item = (&'a str, &'a OCELObject)> {
        self.linked_ocel.get_e2o(index)
    }

    fn get_e2o_rev(
        &'a self,
        index: &ObjectID<'a>,
    ) -> impl Iterator<Item = (&'a str, &'a OCELEvent)> {
        self.linked_ocel.get_e2o_rev(index)
    }

    fn get_o2o(&'a self, index: &ObjectID<'a>) -> impl Iterator<Item = (&'a str, &'a OCELObject)> {
        self.linked_ocel.get_o2o(index)
    }

    fn get_o2o_rev(
        &'a self,
        index: &ObjectID<'a>,
    ) -> impl Iterator<Item = (&'a str, &'a OCELObject)> {
        self.linked_ocel.get_o2o_rev(index)
    }
}
