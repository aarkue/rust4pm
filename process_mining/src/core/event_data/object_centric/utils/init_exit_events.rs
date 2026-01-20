//! Functionality related to artificial init/exit events per object
//! which mark the (implicit) creation or destruction of the object.
use itertools::Itertools;

use crate::core::{
    event_data::object_centric::{
        linked_ocel::{LinkedOCELAccess, SlimLinkedOCEL},
        OCELEvent, OCELRelationship, OCELType,
    },
    OCEL,
};

/// Activity Prefix for INIT events (i.e., the creation of an object)
pub const INIT_EVENT_PREFIX: &str = "<init>";
/// Activity Prefix for EXIT events (i.e., the destruction of an object)
pub const EXIT_EVENT_PREFIX: &str = "<exit>";

/// Add artificial init/exit events to an OCEL
///
///
/// - `<init>` events are added for each object, exactly or slightly before the time they first occur in an event
/// - `<exit>` events are added for each object, exactly or slightly after the time they last occur in an event
///
/// For an object of type `order` the activities are then called `<init> order` or `<exit> order`, respectively.
///
/// __Note: This processing is no longer necessary for OC-DECLARE discovery and conformance checking__
///
/// This function remains, as it might be useful for other applications.
///
pub fn add_init_exit_events_to_ocel(ocel: OCEL) -> OCEL {
    let locel = SlimLinkedOCEL::from_ocel(ocel);
    let new_evs = locel
        .get_all_obs()
        .flat_map(|obi| {
            let ob = locel.get_full_ob(&obi);
            let iter = locel
                .get_e2o_rev(&obi)
                .map(|(_q, e)| locel.get_full_ev(e).time)
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
    let mut ocel = locel.construct_ocel();
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
    ocel
}
