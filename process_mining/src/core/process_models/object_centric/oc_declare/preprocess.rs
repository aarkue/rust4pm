use itertools::Itertools;

use crate::core::{
    event_data::object_centric::{
        linked_ocel::{IndexLinkedOCEL, LinkedOCELAccess, SlimLinkedOCEL},
        OCELEvent, OCELRelationship, OCELType,
    },
    OCEL,
};

/// Activity Prefix for INIT events (i.e., the creation of an object)
pub const INIT_EVENT_PREFIX: &str = "<init>";
/// Activity Prefix for EXIT events (i.e., the destruction of an object)
pub const EXIT_EVENT_PREFIX: &str = "<exit>";

/// Preprocess an OCEL for OC-DECLARE, adding init and exit events for objects
pub fn preprocess_ocel(ocel: OCEL) -> SlimLinkedOCEL {
    let locel: IndexLinkedOCEL = ocel.into();
    let new_evs = locel
        .get_all_obs_ref()
        .flat_map(|obi| {
            let ob = locel.get_ob(obi);
            let iter = locel
                .get_e2o_rev(obi)
                .map(|(_q, e)| locel.get_ev(e).time)
                .sorted();
            let first_ev = iter.clone().next();
            let first_ev_time = first_ev.unwrap_or_default();
            let last_ev = iter.last();
            let last_ev_time = last_ev.unwrap_or_default();
            vec![
                OCELEvent {
                    id: format!("{}_{}_{}", INIT_EVENT_PREFIX, ob.object_type, ob.id),
                    event_type: format!("{} {}", INIT_EVENT_PREFIX, ob.object_type),
                    time: first_ev_time,
                    attributes: Vec::default(),
                    relationships: vec![OCELRelationship {
                        object_id: ob.id.clone(),
                        qualifier: String::from("init"),
                    }],
                },
                OCELEvent {
                    id: format!("{}_{}_{}", EXIT_EVENT_PREFIX, ob.object_type, ob.id),
                    event_type: format!("{} {}", EXIT_EVENT_PREFIX, ob.object_type),
                    time: last_ev_time,
                    attributes: Vec::default(),
                    relationships: vec![OCELRelationship {
                        object_id: ob.id.clone(),
                        qualifier: String::from("exit"),
                    }],
                },
            ]
        })
        .collect_vec();
    let mut ocel = locel.into_inner();
    ocel.event_types
        .extend(ocel.object_types.iter().flat_map(|ot| {
            vec![
                OCELType {
                    name: format!("{} {}", INIT_EVENT_PREFIX, ot.name),
                    attributes: Vec::default(),
                },
                OCELType {
                    name: format!("{} {}", EXIT_EVENT_PREFIX, ot.name),
                    attributes: Vec::default(),
                },
            ]
        }));
    ocel.events.extend(new_evs);
    SlimLinkedOCEL::from_ocel(ocel)
}
