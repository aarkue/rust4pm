//! Binding wrappers for [`SlimLinkedOCEL`] functionality
//!
//! Exposes creation, mutation, and read-access methods through the bindings system.

use chrono::{DateTime, FixedOffset};
use macros_process_mining::register_binding;

use crate::core::event_data::object_centric::{
    linked_ocel::{
        slim_linked_ocel::{EventIndex, ObjectIndex},
        LinkedOCELAccess, SlimLinkedOCEL,
    },
    OCELAttributeValue, OCELEvent, OCELObject, OCELType, OCELTypeAttribute,
};
use crate::core::OCEL;

// ── Creation ──────────────────────────────────────────────────────────

/// Create a new empty [`SlimLinkedOCEL`]
#[register_binding]
fn locel_new() -> SlimLinkedOCEL {
    SlimLinkedOCEL::new()
}

// ── Type Management ───────────────────────────────────────────────────

/// Add a new event type to a [`SlimLinkedOCEL`]
#[register_binding]
fn locel_add_event_type(
    ocel: &mut SlimLinkedOCEL,
    event_type: String,
    #[bind(default)] attributes: Vec<OCELTypeAttribute>,
) {
    ocel.add_event_type(&event_type, attributes);
}

/// Add a new object type to a [`SlimLinkedOCEL`]
#[register_binding]
fn locel_add_object_type(
    ocel: &mut SlimLinkedOCEL,
    object_type: String,
    #[bind(default)] attributes: Vec<OCELTypeAttribute>,
) {
    ocel.add_object_type(&object_type, attributes);
}

// ── Adding Events & Objects ───────────────────────────────────────────

/// Add a new event to a [`SlimLinkedOCEL`]
///
/// The attribute order must match the attributes defined on the corresponding event type.
/// Returns the [`EventIndex`] of the newly added event, or `None` if the event type is unknown or the id is already taken.
#[register_binding]
fn locel_add_event(
    ocel: &mut SlimLinkedOCEL,
    event_type: String,
    time: DateTime<FixedOffset>,
    #[bind(default)] id: Option<String>,
    #[bind(default)] attributes: Vec<OCELAttributeValue>,
    #[bind(default)] relationships: Vec<(String, ObjectIndex)>,
) -> Option<EventIndex> {
    ocel.add_event(&event_type, time, id, attributes, relationships)
}

/// Add a new object to a [`SlimLinkedOCEL`]
///
/// The attribute order must match the attributes defined on the corresponding object type.
/// Returns the [`ObjectIndex`] of the newly added object, or `None` if the object type is unknown or the id is already taken.
#[register_binding]
fn locel_add_object(
    ocel: &mut SlimLinkedOCEL,
    object_type: String,
    #[bind(default)] id: Option<String>,
    #[bind(default)] attributes: Vec<Vec<(DateTime<FixedOffset>, OCELAttributeValue)>>,
    #[bind(default)] relationships: Vec<(String, ObjectIndex)>,
) -> Option<ObjectIndex> {
    ocel.add_object(&object_type, id, attributes, relationships)
}

// ── Relationship Management ───────────────────────────────────────────

/// Add an E2O (event-to-object) relationship with the specified qualifier
#[register_binding]
fn locel_add_e2o(
    ocel: &mut SlimLinkedOCEL,
    event: EventIndex,
    object: ObjectIndex,
    qualifier: String,
) {
    ocel.add_e2o(event, object, qualifier);
}

/// Add an O2O (object-to-object) relationship with the specified qualifier
#[register_binding]
fn locel_add_o2o(
    ocel: &mut SlimLinkedOCEL,
    from_obj: ObjectIndex,
    to_obj: ObjectIndex,
    qualifier: String,
) {
    ocel.add_o2o(from_obj, to_obj, qualifier);
}

/// Remove the E2O relationship between the given event and object
#[register_binding]
fn locel_delete_e2o(ocel: &mut SlimLinkedOCEL, event: EventIndex, object: ObjectIndex) {
    ocel.delete_e2o(&event, &object);
}

/// Remove the O2O relationship between the given objects
#[register_binding]
fn locel_delete_o2o(ocel: &mut SlimLinkedOCEL, from_obj: ObjectIndex, to_obj: ObjectIndex) {
    ocel.delete_o2o(&from_obj, &to_obj);
}

// ── Read Access (LinkedOCELAccess) ────────────────────────────────────

/// Get all event type names
#[register_binding]
fn locel_get_ev_types(ocel: &SlimLinkedOCEL) -> Vec<String> {
    ocel.get_ev_types().map(str::to_string).collect()
}

/// Get all object type names
#[register_binding]
fn locel_get_ob_types(ocel: &SlimLinkedOCEL) -> Vec<String> {
    ocel.get_ob_types().map(str::to_string).collect()
}

/// Get the event type specification for a given type name
#[register_binding]
fn locel_get_ev_type(ocel: &SlimLinkedOCEL, ev_type: String) -> Option<OCELType> {
    ocel.get_ev_type(&ev_type).cloned()
}

/// Get the object type specification for a given type name
#[register_binding]
fn locel_get_ob_type(ocel: &SlimLinkedOCEL, ob_type: String) -> Option<OCELType> {
    ocel.get_ob_type(&ob_type).cloned()
}

/// Get all event indices of a given event type
#[register_binding]
fn locel_get_evs_of_type(ocel: &SlimLinkedOCEL, ev_type: String) -> Vec<EventIndex> {
    ocel.get_evs_of_type(&ev_type).copied().collect()
}

/// Get all object indices of a given object type
#[register_binding]
fn locel_get_obs_of_type(ocel: &SlimLinkedOCEL, ob_type: String) -> Vec<ObjectIndex> {
    ocel.get_obs_of_type(&ob_type).copied().collect()
}

/// Get an event index by its ID
#[register_binding]
fn locel_get_ev_by_id(ocel: &SlimLinkedOCEL, ev_id: String) -> Option<EventIndex> {
    ocel.get_ev_by_id(&ev_id)
}

/// Get an object index by its ID
#[register_binding]
fn locel_get_ob_by_id(ocel: &SlimLinkedOCEL, ob_id: String) -> Option<ObjectIndex> {
    ocel.get_ob_by_id(&ob_id)
}

/// Get the ID of an event
#[register_binding]
fn locel_get_ev_id(ocel: &SlimLinkedOCEL, ev: EventIndex) -> String {
    ocel.get_ev_id(&ev).to_string()
}

/// Get the ID of an object
#[register_binding]
fn locel_get_ob_id(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> String {
    ocel.get_ob_id(&ob).to_string()
}

/// Get the event type (activity) of an event
#[register_binding]
fn locel_get_ev_type_of(ocel: &SlimLinkedOCEL, ev: EventIndex) -> String {
    ocel.get_ev_type_of(&ev).to_string()
}

/// Get the object type of an object
#[register_binding]
fn locel_get_ob_type_of(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> String {
    ocel.get_ob_type_of(&ob).to_string()
}

/// Get the timestamp of an event
#[register_binding]
fn locel_get_ev_time(ocel: &SlimLinkedOCEL, ev: EventIndex) -> DateTime<FixedOffset> {
    *ocel.get_ev_time(&ev)
}

/// Get the E2O (event-to-object) relationships of an event as (qualifier, `object_index`) pairs
#[register_binding]
fn locel_get_e2o(ocel: &SlimLinkedOCEL, ev: EventIndex) -> Vec<(String, ObjectIndex)> {
    ocel.get_e2o(&ev)
        .map(|(q, o)| (q.to_string(), *o))
        .collect()
}

/// Get the reverse E2O relationships of an object as (qualifier, `event_index`) pairs
#[register_binding]
fn locel_get_e2o_rev(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> Vec<(String, EventIndex)> {
    ocel.get_e2o_rev(&ob)
        .map(|(q, e)| (q.to_string(), *e))
        .collect()
}

/// Get the O2O (object-to-object) relationships of an object as (qualifier, `object_index`) pairs
#[register_binding]
fn locel_get_o2o(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> Vec<(String, ObjectIndex)> {
    ocel.get_o2o(&ob)
        .map(|(q, o)| (q.to_string(), *o))
        .collect()
}

/// Get the reverse O2O relationships of an object as (qualifier, `object_index`) pairs
#[register_binding]
fn locel_get_o2o_rev(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> Vec<(String, ObjectIndex)> {
    ocel.get_o2o_rev(&ob)
        .map(|(q, o)| (q.to_string(), *o))
        .collect()
}

/// Get the full materialized event (with expanded type, attributes, and relationships)
#[register_binding]
fn locel_get_full_ev(ocel: &SlimLinkedOCEL, ev: EventIndex) -> OCELEvent {
    ocel.get_full_ev(&ev).into_owned()
}

/// Get the full materialized object (with expanded type, attributes, and relationships)
#[register_binding]
fn locel_get_full_ob(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> OCELObject {
    ocel.get_full_ob(&ob).into_owned()
}

/// Get the value of a specific event attribute by name
#[register_binding]
fn locel_get_ev_attr_val(
    ocel: &SlimLinkedOCEL,
    ev: EventIndex,
    attr_name: String,
) -> Option<OCELAttributeValue> {
    ocel.get_ev_attr_val(&ev, &attr_name).cloned()
}

/// Get all values (with timestamps) of a specific object attribute by name
#[register_binding]
fn locel_get_ob_attr_vals(
    ocel: &SlimLinkedOCEL,
    ob: ObjectIndex,
    attr_name: String,
) -> Vec<(DateTime<FixedOffset>, OCELAttributeValue)> {
    ocel.get_ob_attr_vals(&ob, &attr_name)
        .map(|(t, v)| (*t, v.clone()))
        .collect()
}

/// Reconstruct a full [`OCEL`] from a [`SlimLinkedOCEL`]
#[register_binding]
fn locel_construct_ocel(ocel: &SlimLinkedOCEL) -> OCEL {
    ocel.construct_ocel()
}
