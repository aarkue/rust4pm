use std::{collections::HashMap, hash::Hash};

use ambassador::{delegatable_trait, Delegate};
use rayon::str;

use crate::OCEL;

use super::ocel_struct::{OCELEvent, OCELObject};

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Clone)]
struct ObjectID<'a>(&'a str);
// impl<'a> From<&'a str> for ObjectID<'a> {
//     fn from(value: &'a str) -> Self {
//         Self(value)
//     }
// }
// impl<'a> From<&'a String> for ObjectID<'a> {
//     fn from(value: &'a String) -> Self {
//         Self(value)
//     }
// }

impl<'a> From<&'a str> for ObjectID<'a> {
    fn from(value: &'a str) -> Self {
        Self(value)
    }
}
impl<'a> From<&'a String> for ObjectID<'a> {
    fn from(value: &'a String) -> Self {
        Self(value)
    }
}

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Clone)]
struct EventID<'a>(&'a str);
impl<'a> From<&'a str> for EventID<'a> {
    fn from(value: &'a str) -> Self {
        Self(value)
    }
}
impl<'a> From<&'a String> for EventID<'a> {
    fn from(value: &'a String) -> Self {
        Self(value)
    }
}

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Clone)]
struct Qualifier<'a>(&'a str);
impl<'a> From<&'a str> for Qualifier<'a> {
    fn from(value: &'a str) -> Self {
        Self(value)
    }
}
impl<'a> From<&'a String> for Qualifier<'a> {
    fn from(value: &'a String) -> Self {
        Self(value)
    }
}

struct LinkedOCEL<'a> {
    pub ocel: &'a OCEL,
    pub events: HashMap<EventID<'a>, &'a OCELEvent>,
    pub objects: HashMap<ObjectID<'a>, &'a OCELObject>,
    pub e2o_rel: HashMap<EventID<'a>, Vec<(Qualifier<'a>, &'a OCELObject)>>,
    pub o2o_rel: HashMap<ObjectID<'a>, Vec<(Qualifier<'a>, &'a OCELObject)>>,
    pub e2o_rel_rev: HashMap<ObjectID<'a>, Vec<(Qualifier<'a>, &'a OCELEvent)>>,
    pub o2o_rel_rev: HashMap<ObjectID<'a>, Vec<(Qualifier<'a>, &'a OCELObject)>>,
}

impl<'a> From<&'a OCEL> for LinkedOCEL<'a> {
    fn from(ocel: &'a OCEL) -> Self {
        let events: HashMap<_, _> = ocel.events.iter().map(|e| ((&e.id).into(), e)).collect();
        let objects: HashMap<_, _> = ocel.objects.iter().map(|o| ((&o.id).into(), o)).collect();
        let mut e2o_rel_rev: HashMap<ObjectID<'a>, Vec<(Qualifier<'a>, &OCELEvent)>> =
            HashMap::new();
        let e2o_rel = events
            .values()
            .map(|e| {
                let e_id: EventID<'_> = (&e.id).into();
                (
                    e_id,
                    e.relationships
                        .iter()
                        .flat_map(|rel| {
                            let obj_id: ObjectID<'_> = (&rel.object_id).into();
                            let qualifier: Qualifier<'_> = (&rel.qualifier).into();
                            e2o_rel_rev
                                .entry(obj_id)
                                .or_default()
                                .push((qualifier.clone(), &e));
                            let ob = objects.get(&((&rel.object_id).into()))?;
                            Some((qualifier, *ob))
                        })
                        .collect(),
                )
            })
            .collect();

        let mut o2o_rel_rev: HashMap<ObjectID<'_>, Vec<(Qualifier<'a>, &OCELObject)>> =
            HashMap::new();

        let o2o_rel = objects
            .values()
            .map(|o| {
                (
                    (&o.id).into(),
                    o.relationships
                        .iter()
                        .flat_map(|rel| {
                            let qualifier: Qualifier<'_> = (&rel.qualifier).into();
                            let obj2_id: ObjectID<'_> = (&rel.object_id).into();
                            o2o_rel_rev
                                .entry(obj2_id)
                                .or_default()
                                .push((qualifier.clone(), &o));
                            let ob = objects.get(&(&rel.object_id).into())?;
                            Some(((&rel.qualifier).into(), *ob))
                        })
                        .collect(),
                )
            })
            .collect();

        Self {
            ocel,
            events,
            objects,
            e2o_rel,
            o2o_rel,
            e2o_rel_rev,
            o2o_rel_rev,
        }
    }
}

#[delegatable_trait]
trait LinkedOCELAccess<'a> {
    fn get_ev_rels(
        &'a self,
        e: impl Into<EventID<'a>>,
    ) -> Option<&'a Vec<(Qualifier<'a>, &'a OCELObject)>>;
}
impl<'a> LinkedOCELAccess<'a> for LinkedOCEL<'a> {
    fn get_ev_rels(
        &'a self,
        e: impl Into<EventID<'a>>,
    ) -> Option<&'a Vec<(Qualifier<'a>, &'a OCELObject)>> {
        let e_id = e.into();
        self.e2o_rel.get(&e_id)
    }
}

#[derive(Delegate)]
#[delegate(LinkedOCELAccess<'a>, target = "linked_ocel")]
struct OwnedLinkedOcel<'a> {
    ocel: OCEL,
    pub linked_ocel: LinkedOCEL<'a>,
}

impl<'a> From<OCEL> for OwnedLinkedOcel<'a> {
    fn from(ocel: OCEL) -> Self {
        let ocel_ref = unsafe { &*(&ocel as *const OCEL) };
        OwnedLinkedOcel {
            ocel,
            linked_ocel: (ocel_ref).into(),
        }
    }
}

impl<'a> OwnedLinkedOcel<'a> {
    pub fn into_inner(self) -> OCEL {
        self.ocel
    }
    pub fn ocel_ref(&'a self) -> &'a OCEL {
        &self.ocel
    }
}

#[cfg(test)]
mod test {
    use std::time::Instant;

    use crate::{import_ocel_json_from_path, ocel::linked_ocel::{LinkedOCEL, OwnedLinkedOcel}};

    #[test]
    fn test_linked_ocel() {
        let ocel = import_ocel_json_from_path(
            "/home/aarkue/dow/ocel/bpic2017-o2o-workflow-qualifier.json",
        )
        .unwrap();
        let now = Instant::now();
        let locel: LinkedOCEL<'_> = (&ocel).into();
        println!("Linking ocel took {:?}", now.elapsed());
        println!("Linked ocel: {}", locel.e2o_rel_rev.len())
        // let rels = locel.get_ev_rels("place_o-990001").unwrap();
        // locel
        //     .get_ev_rels("place_o-990001")
        //     .unwrap()
        //     .iter()
        //     .for_each(|(q, o)| println!("{q:?}: {} ({})", o.id, o.object_type));
        // locel
        //     .get_ev_rels("place_o-990001")
        //     .unwrap()
        //     .for_each(|(q, o)| {
        //         let o = o.unwrap();
        //         println!("{q}: {} ({})", o.id, o.object_type)
        //     });
    }

    #[test]
    fn test_owned_linked_ocel() {
        let ocel =
            import_ocel_json_from_path("/home/aarkue/dow/ocel/order-management.json").unwrap();
        let locel: OwnedLinkedOcel<'_> = ocel.into();
        let rels = locel.get_ev_rels("place_o-990001").unwrap();
        locel
            .get_ev_rels("place_o-990001")
            .unwrap()
            .iter()
            .for_each(|(q, o)| println!("{q:?}: {} ({})", o.id, o.object_type));
        let n = rels.len();
        let ocel2 = locel.into_inner();
        println!("Got back ocel with {} events", ocel2.events.len());
        println!("Event had relations with {} objects", n);
    }
}
