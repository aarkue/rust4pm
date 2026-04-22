//! Binding wrappers for [`SlimLinkedOCEL`] functionality

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

/// Create a new empty [`SlimLinkedOCEL`].
///
/// A [`SlimLinkedOCEL`] is an object-centric event log where events and objects are referenced
/// by integer indices ([`EventIndex`] / [`ObjectIndex`]) returned from the `add_*` calls,
/// and each indexed event/object is an instance of an event/object type (activity / object class)
/// declared beforehand with an ordered list of attributes.
#[register_binding]
fn locel_new() -> SlimLinkedOCEL {
    SlimLinkedOCEL::new()
}

// ── Type Management ───────────────────────────────────────────────────

/// Add an event type with the given ordered attribute declarations.
///
/// No-op if the event type already exists.
#[register_binding]
fn locel_add_event_type(
    ocel: &mut SlimLinkedOCEL,
    event_type: String,
    #[bind(default)] attributes: Vec<OCELTypeAttribute>,
) {
    ocel.add_event_type(&event_type, attributes);
}

/// Add an object type with the given ordered attribute declarations.
///
/// No-op if the object type already exists.
#[register_binding]
fn locel_add_object_type(
    ocel: &mut SlimLinkedOCEL,
    object_type: String,
    #[bind(default)] attributes: Vec<OCELTypeAttribute>,
) {
    ocel.add_object_type(&object_type, attributes);
}

// ── Adding Events & Objects ───────────────────────────────────────────

/// Add an event and return its [`EventIndex`].
///
/// The event type must have been declared via [`locel_add_event_type`] first;
/// otherwise this returns `None`.
///
/// `id`: If `None`, a UUID is assigned. Returns `None` if the id is already taken.
/// `attributes`: Positional values in the declared attribute order. Padded with `Null` or truncated on length mismatch (with a warning).
/// `relationships`: E2O relationships as `(qualifier, object_index)` pairs (can also be added later via [`locel_add_e2o`]).
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

/// Add an object and return its [`ObjectIndex`].
///
/// The object type must have been declared via [`locel_add_object_type`] first;
/// otherwise this returns `None`.
///
/// `id`: If `None`, a UUID is assigned. Returns `None` if the id is already taken.
/// `attributes`: Positional list of time-indexed attribute histories (one `(timestamp, value)` list per declared attribute, in order). Use `1970-01-01T00:00:00Z` for constant/initial values. Padded with empty lists or truncated on length mismatch (with a warning).
/// `relationships`: Outgoing O2O relationships as `(qualifier, object_index)` pairs (can also be added later via [`locel_add_o2o`]).
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

/// Add an E2O (event-to-object) relationship with the given qualifier.
///
/// Multiple qualifiers between the same `(event, object)` pair are allowed; re-adding the exact
/// same `(event, object, qualifier)` triple is a no-op. Returns `true` on success, `false` if
/// either index is out of bounds (with a stderr warning).
#[register_binding]
fn locel_add_e2o(
    ocel: &mut SlimLinkedOCEL,
    event: EventIndex,
    object: ObjectIndex,
    qualifier: String,
) -> bool {
    ocel.add_e2o(event, object, qualifier)
}

/// Add a directed O2O (object-to-object) relationship from `from_obj` to `to_obj` with the given qualifier.
///
/// Multiple qualifiers between the same `(from_obj, to_obj)` pair are allowed; re-adding the exact
/// same `(from_obj, to_obj, qualifier)` triple is a no-op. Returns `true` on success, `false` if
/// either index is out of bounds (with a stderr warning).
#[register_binding]
fn locel_add_o2o(
    ocel: &mut SlimLinkedOCEL,
    from_obj: ObjectIndex,
    to_obj: ObjectIndex,
    qualifier: String,
) -> bool {
    ocel.add_o2o(from_obj, to_obj, qualifier)
}

/// Remove all E2O relationships between the given event and object (across every qualifier).
///
/// Returns `true` on success, `false` if either index is out of bounds (with a stderr warning).
#[register_binding]
fn locel_delete_e2o(ocel: &mut SlimLinkedOCEL, event: EventIndex, object: ObjectIndex) -> bool {
    ocel.delete_e2o(&event, &object)
}

/// Remove all O2O relationships from `from_obj` to `to_obj` (across every qualifier).
///
/// Returns `true` on success, `false` if either index is out of bounds (with a stderr warning).
#[register_binding]
fn locel_delete_o2o(ocel: &mut SlimLinkedOCEL, from_obj: ObjectIndex, to_obj: ObjectIndex) -> bool {
    ocel.delete_o2o(&from_obj, &to_obj)
}

// ── Read Access (LinkedOCELAccess) ────────────────────────────────────

/// Get all declared event type names, in declaration order.
#[register_binding]
fn locel_get_ev_types(ocel: &SlimLinkedOCEL) -> Vec<String> {
    ocel.get_ev_types().map(str::to_string).collect()
}

/// Get all declared object type names, in declaration order.
#[register_binding]
fn locel_get_ob_types(ocel: &SlimLinkedOCEL) -> Vec<String> {
    ocel.get_ob_types().map(str::to_string).collect()
}

/// Get the event type specification (name + attributes), or `None` if unknown.
#[register_binding]
fn locel_get_ev_type(ocel: &SlimLinkedOCEL, ev_type: String) -> Option<OCELType> {
    ocel.get_ev_type(&ev_type).cloned()
}

/// Get the object type specification (name + attributes), or `None` if unknown.
#[register_binding]
fn locel_get_ob_type(ocel: &SlimLinkedOCEL, ob_type: String) -> Option<OCELType> {
    ocel.get_ob_type(&ob_type).cloned()
}

/// Get all event indices of the given event type. Empty if unknown.
#[register_binding]
fn locel_get_evs_of_type(ocel: &SlimLinkedOCEL, ev_type: String) -> Vec<EventIndex> {
    ocel.get_evs_of_type(&ev_type).copied().collect()
}

/// Get all object indices of the given object type. Empty if unknown.
#[register_binding]
fn locel_get_obs_of_type(ocel: &SlimLinkedOCEL, ob_type: String) -> Vec<ObjectIndex> {
    ocel.get_obs_of_type(&ob_type).copied().collect()
}

/// Look up an event by its ID string. `None` if not found.
#[register_binding]
fn locel_get_ev_by_id(ocel: &SlimLinkedOCEL, ev_id: String) -> Option<EventIndex> {
    ocel.get_ev_by_id(&ev_id)
}

/// Look up an object by its ID string. `None` if not found.
#[register_binding]
fn locel_get_ob_by_id(ocel: &SlimLinkedOCEL, ob_id: String) -> Option<ObjectIndex> {
    ocel.get_ob_by_id(&ob_id)
}

/// Get the ID string of an event. Panics if the index is out of bounds.
#[register_binding]
fn locel_get_ev_id(ocel: &SlimLinkedOCEL, ev: EventIndex) -> String {
    ocel.get_ev_id(&ev).to_string()
}

/// Get the ID string of an object. Panics if the index is out of bounds.
#[register_binding]
fn locel_get_ob_id(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> String {
    ocel.get_ob_id(&ob).to_string()
}

/// Get the event type (activity) of an event. Panics if the index is out of bounds.
#[register_binding]
fn locel_get_ev_type_of(ocel: &SlimLinkedOCEL, ev: EventIndex) -> String {
    ocel.get_ev_type_of(&ev).to_string()
}

/// Get the object type of an object. Panics if the index is out of bounds.
#[register_binding]
fn locel_get_ob_type_of(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> String {
    ocel.get_ob_type_of(&ob).to_string()
}

/// Get the timestamp of an event. Panics if the index is out of bounds.
#[register_binding]
fn locel_get_ev_time(ocel: &SlimLinkedOCEL, ev: EventIndex) -> DateTime<FixedOffset> {
    *ocel.get_ev_time(&ev)
}

/// Get the E2O relationships of an event as `(qualifier, object_index)` pairs.
#[register_binding]
fn locel_get_e2o(ocel: &SlimLinkedOCEL, ev: EventIndex) -> Vec<(String, ObjectIndex)> {
    ocel.get_e2o(&ev)
        .map(|(q, o)| (q.to_string(), *o))
        .collect()
}

/// Get the reverse E2O relationships of an object (events relating to it) as `(qualifier, event_index)` pairs.
#[register_binding]
fn locel_get_e2o_rev(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> Vec<(String, EventIndex)> {
    ocel.get_e2o_rev(&ob)
        .map(|(q, e)| (q.to_string(), *e))
        .collect()
}

/// Get the outgoing O2O relationships of an object as `(qualifier, object_index)` pairs.
#[register_binding]
fn locel_get_o2o(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> Vec<(String, ObjectIndex)> {
    ocel.get_o2o(&ob)
        .map(|(q, o)| (q.to_string(), *o))
        .collect()
}

/// Get the reverse O2O relationships of an object (objects with an O2O to it) as `(qualifier, object_index)` pairs.
#[register_binding]
fn locel_get_o2o_rev(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> Vec<(String, ObjectIndex)> {
    ocel.get_o2o_rev(&ob)
        .map(|(q, o)| (q.to_string(), *o))
        .collect()
}

/// Get the full [`OCELEvent`] (resolved type name, named attributes, string object IDs).
///
/// Allocates; prefer the specific `locel_get_ev_*` accessors for single fields.
/// Panics if the index is out of bounds.
#[register_binding]
fn locel_get_full_ev(ocel: &SlimLinkedOCEL, ev: EventIndex) -> OCELEvent {
    ocel.get_full_ev(&ev).into_owned()
}

/// Get the full [`OCELObject`] (resolved type name, named time-indexed attributes, string object IDs).
///
/// Allocates; prefer the specific `locel_get_ob_*` accessors for single fields.
/// Panics if the index is out of bounds.
#[register_binding]
fn locel_get_full_ob(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> OCELObject {
    ocel.get_full_ob(&ob).into_owned()
}

/// Get the value of an event attribute by name. `None` if the attribute does not exist.
///
/// Panics if the index is out of bounds.
#[register_binding]
fn locel_get_ev_attr_val(
    ocel: &SlimLinkedOCEL,
    ev: EventIndex,
    attr_name: String,
) -> Option<OCELAttributeValue> {
    ocel.get_ev_attr_val(&ev, &attr_name).cloned()
}

/// Get the time-indexed history of an object attribute by name as `(timestamp, value)` pairs. Empty if absent.
///
/// Panics if the index is out of bounds.
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

/// Reconstruct a full [`OCEL`] from a [`SlimLinkedOCEL`]. Can be expensive for large logs.
#[register_binding]
fn locel_construct_ocel(ocel: &SlimLinkedOCEL) -> OCEL {
    ocel.construct_ocel()
}
