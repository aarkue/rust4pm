//! Binding wrappers for [`SlimLinkedOCEL`] functionality

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, FixedOffset};
use macros_process_mining::register_binding;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::event_data::object_centric::{
    appendable::AppendableOCEL,
    linked_ocel::{
        slim_linked_ocel::{EventIndex, ObjectIndex},
        LinkedOCELAccess, SlimLinkedOCEL,
    },
    OCELAttributeValue, OCELEvent, OCELObject, OCELType, OCELTypeAttribute,
};

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

/// Add an event and return its [`EventIndex`].
///
/// The event type must have been declared via [`locel_add_event_type`] first.
///
/// `id`: If `None`, a UUID is assigned.
/// `attributes`: Positional values in the declared attribute order. Padded with `Null` or truncated on length mismatch (with a warning).
/// `relationships`: E2O relationships as `(qualifier, object_index)` pairs (can also be added later via [`locel_add_e2o`]).
///
/// # Errors
/// If the event type is undeclared, the id is already taken, or a relationship names an object
/// index that does not exist. The OCEL is left unchanged.
#[register_binding(stringify_error)]
fn locel_add_event(
    ocel: &mut SlimLinkedOCEL,
    event_type: String,
    time: DateTime<FixedOffset>,
    #[bind(default)] id: Option<String>,
    #[bind(default)] attributes: Vec<OCELAttributeValue>,
    #[bind(default)] relationships: Vec<(String, ObjectIndex)>,
) -> Result<EventIndex, String> {
    if ocel.get_ev_type(&event_type).is_none() {
        return Err(format!("no event type '{event_type}'"));
    }
    if let Some(id) = &id {
        if ocel.get_ev_by_id(id).is_some() {
            return Err(format!("event id '{id}' is already taken"));
        }
    }
    check_object_indices(ocel, &relationships)?;
    ocel.add_event(&event_type, time, id, attributes, relationships)
        .ok_or_else(|| "event was not added".to_string())
}

/// Add an object and return its [`ObjectIndex`].
///
/// The object type must have been declared via [`locel_add_object_type`] first.
///
/// `id`: If `None`, a UUID is assigned.
/// `attributes`: Positional list of time-indexed attribute histories (one `(timestamp, value)` list per declared attribute, in order). Use `1970-01-01T00:00:00Z` for constant/initial values. Padded with empty lists or truncated on length mismatch (with a warning).
/// `relationships`: Outgoing O2O relationships as `(qualifier, object_index)` pairs (can also be added later via [`locel_add_o2o`]).
///
/// # Errors
/// If the object type is undeclared, the id is already taken, or a relationship names an object
/// index that does not exist yet. The OCEL is left unchanged.
#[register_binding(stringify_error)]
fn locel_add_object(
    ocel: &mut SlimLinkedOCEL,
    object_type: String,
    #[bind(default)] id: Option<String>,
    #[bind(default)] attributes: Vec<Vec<(DateTime<FixedOffset>, OCELAttributeValue)>>,
    #[bind(default)] relationships: Vec<(String, ObjectIndex)>,
) -> Result<ObjectIndex, String> {
    if ocel.get_ob_type(&object_type).is_none() {
        return Err(format!("no object type '{object_type}'"));
    }
    if let Some(id) = &id {
        if ocel.get_ob_by_id(id).is_some() {
            return Err(format!("object id '{id}' is already taken"));
        }
    }
    check_object_indices(ocel, &relationships)?;
    ocel.add_object(&object_type, id, attributes, relationships)
        .ok_or_else(|| "object was not added".to_string())
}

fn check_object_indices(
    ocel: &SlimLinkedOCEL,
    relationships: &[(String, ObjectIndex)],
) -> Result<(), String> {
    let num_obs = ocel.get_num_obs();
    match relationships
        .iter()
        .find(|(_q, o)| o.into_inner() as usize >= num_obs)
    {
        Some((q, o)) => Err(format!(
            "relationship '{q}' names object index {} but the OCEL has {num_obs} object(s)",
            o.into_inner()
        )),
        None => Ok(()),
    }
}

/// Add a value to the history of an object attribute.
///
/// Inserted after the last change at or before its time. The attribute must be declared on the
/// object's type.
///
/// # Errors
/// If the object index does not exist or the object's type does not declare `attribute`.
/// The OCEL is left unchanged.
#[register_binding(stringify_error)]
fn locel_add_object_attribute_value(
    ocel: &mut SlimLinkedOCEL,
    object: ObjectIndex,
    attribute: String,
    time: DateTime<FixedOffset>,
    value: OCELAttributeValue,
) -> Result<(), String> {
    let index = object.into_inner();
    if index as usize >= ocel.get_num_obs() {
        return Err(format!("no object with index {index}"));
    }
    if ocel.add_object_attribute_value(object, &attribute, time, value) {
        Ok(())
    } else {
        Err(format!(
            "the type of object {index} declares no attribute '{attribute}'"
        ))
    }
}

/// Events, objects and type declarations to add with [`locel_append_batch`]
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct OCELAppendBatch {
    /// Event types to declare. Attributes of an already declared type are merged.
    #[serde(default)]
    pub event_types: Vec<OCELType>,
    /// Object types to declare. Attributes of an already declared type are merged.
    #[serde(default)]
    pub object_types: Vec<OCELType>,
    /// Objects to add. An object whose id already exists, or occurs earlier in the batch, is
    /// updated instead: Its attribute values are added to the histories and its relationships
    /// are added as O2O relationships.
    #[serde(default)]
    pub objects: Vec<OCELObject>,
    /// Events to add
    #[serde(default)]
    pub events: Vec<OCELEvent>,
}

/// What a [`locel_append_batch`] call added
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OCELAppendSummary {
    /// Number of events added
    pub events_added: usize,
    /// Number of objects added
    pub objects_added: usize,
    /// Number of objects that existed before the batch and received attribute values or
    /// relationships
    pub objects_updated: usize,
}

/// Add many events and objects, referring to objects by id.
///
/// Unlike [`locel_add_event`] and [`locel_add_object`], types and attributes that are not
/// declared yet are declared on the fly, and relationships name their target by object id. A
/// relationship may refer to an object that appears later in the same batch. A relationship to
/// an object id that is unknown after the whole batch is dropped with a warning.
///
/// An object whose id already exists is updated, see [`OCELAppendBatch::objects`].
///
/// # Errors
/// If an event id is already taken or occurs twice in the batch, or an object id is given with
/// two different types. The OCEL is left unchanged.
#[register_binding(stringify_error)]
fn locel_append_batch(
    ocel: &mut SlimLinkedOCEL,
    batch: OCELAppendBatch,
) -> Result<OCELAppendSummary, String> {
    let mut event_ids = HashSet::with_capacity(batch.events.len());
    for event in &batch.events {
        if ocel.get_ev_by_id(&event.id).is_some() {
            return Err(format!("event id '{}' is already taken", event.id));
        }
        if !event_ids.insert(event.id.as_str()) {
            return Err(format!("event id '{}' occurs twice in the batch", event.id));
        }
    }
    let mut new_object_types: HashMap<&str, &str> = HashMap::new();
    for object in &batch.objects {
        let known_type = match ocel.get_ob_by_id(&object.id) {
            Some(existing) => existing.get_ob_type(ocel).as_str(),
            None => new_object_types
                .entry(object.id.as_str())
                .or_insert(object.object_type.as_str()),
        };
        if known_type != object.object_type {
            return Err(format!(
                "object '{}' has type '{}', not '{}'",
                object.id, known_type, object.object_type
            ));
        }
    }

    let mut summary = OCELAppendSummary::default();
    for event_type in batch.event_types {
        ocel.declare_event_type(event_type)
            .map_err(|e| e.to_string())?;
    }
    for object_type in batch.object_types {
        ocel.declare_object_type(object_type)
            .map_err(|e| e.to_string())?;
    }
    let num_obs_before = ocel.get_num_obs();
    let mut updates = Vec::new();
    for object in batch.objects {
        match ocel.get_ob_by_id(&object.id) {
            Some(index) => updates.push((index, object)),
            None => {
                ocel.append_object(
                    object.id,
                    &object.object_type,
                    object.attributes,
                    object.relationships,
                )
                .map_err(|e| e.to_string())?;
                summary.objects_added += 1;
            }
        }
    }
    let mut updated = HashSet::new();
    for (index, object) in updates {
        let mut changed = false;
        for attribute in object.attributes {
            let declared = ocel
                .get_ob_type(&object.object_type)
                .is_some_and(|t| t.attributes.iter().any(|a| a.name == attribute.name));
            if !declared {
                ocel.declare_object_type(OCELType {
                    name: object.object_type.clone(),
                    attributes: vec![OCELTypeAttribute {
                        name: attribute.name.clone(),
                        value_type: attribute.value.get_type().as_type_str().to_string(),
                    }],
                })
                .map_err(|e| e.to_string())?;
            }
            if !ocel.add_object_attribute_value(
                index,
                &attribute.name,
                attribute.time,
                attribute.value,
            ) {
                return Err(format!(
                    "object '{}' takes no attribute '{}'",
                    object.id, attribute.name
                ));
            }
            changed = true;
        }
        for relationship in object.relationships {
            match ocel.get_ob_by_id(&relationship.object_id) {
                Some(target) => {
                    let present = ocel
                        .get_o2o(index)
                        .any(|(q, o)| *o == target && q == relationship.qualifier);
                    if !present {
                        ocel.add_o2o(index, target, relationship.qualifier);
                        changed = true;
                    }
                }
                None => log::warn!(
                    "dropping O2O reference to unknown object id {:?}",
                    relationship.object_id
                ),
            }
        }
        if changed && (index.into_inner() as usize) < num_obs_before {
            updated.insert(index);
        }
    }
    summary.objects_updated = updated.len();
    for event in batch.events {
        ocel.append_event(
            event.id,
            &event.event_type,
            event.time,
            event.attributes,
            event.relationships,
        )
        .map_err(|e| e.to_string())?;
        summary.events_added += 1;
    }
    ocel.finalize().map_err(|e| e.to_string())?;
    Ok(summary)
}

/// Add an E2O (event-to-object) relationship with the given qualifier.
///
/// Multiple qualifiers between the same `(event, object)` pair are allowed. Re-adding the exact
/// same `(event, object, qualifier)` triple is a no-op. Returns `true` on success.
///
/// # Errors
/// If either index is out of range.
#[register_binding(stringify_error)]
fn locel_add_e2o(
    ocel: &mut SlimLinkedOCEL,
    event: EventIndex,
    object: ObjectIndex,
    qualifier: String,
) -> Result<bool, String> {
    let (event, object) = (check_ev(ocel, event)?, check_ob(ocel, object)?);
    Ok(ocel.add_e2o(event, object, qualifier))
}

/// Add a directed O2O (object-to-object) relationship from `from_obj` to `to_obj` with the given qualifier.
///
/// Multiple qualifiers between the same `(from_obj, to_obj)` pair are allowed. Re-adding the exact
/// same `(from_obj, to_obj, qualifier)` triple is a no-op. Returns `true` on success.
///
/// # Errors
/// If either index is out of range.
#[register_binding(stringify_error)]
fn locel_add_o2o(
    ocel: &mut SlimLinkedOCEL,
    from_obj: ObjectIndex,
    to_obj: ObjectIndex,
    qualifier: String,
) -> Result<bool, String> {
    let (from_obj, to_obj) = (check_ob(ocel, from_obj)?, check_ob(ocel, to_obj)?);
    Ok(ocel.add_o2o(from_obj, to_obj, qualifier))
}

/// Remove all E2O relationships between the given event and object (across every qualifier).
///
/// Returns `true` on success.
///
/// # Errors
/// If either index is out of range.
#[register_binding(stringify_error)]
fn locel_delete_e2o(
    ocel: &mut SlimLinkedOCEL,
    event: EventIndex,
    object: ObjectIndex,
) -> Result<bool, String> {
    let (event, object) = (check_ev(ocel, event)?, check_ob(ocel, object)?);
    Ok(ocel.delete_e2o(&event, &object))
}

/// Remove all O2O relationships from `from_obj` to `to_obj` (across every qualifier).
///
/// Returns `true` on success.
///
/// # Errors
/// If either index is out of range.
#[register_binding(stringify_error)]
fn locel_delete_o2o(
    ocel: &mut SlimLinkedOCEL,
    from_obj: ObjectIndex,
    to_obj: ObjectIndex,
) -> Result<bool, String> {
    let (from_obj, to_obj) = (check_ob(ocel, from_obj)?, check_ob(ocel, to_obj)?);
    Ok(ocel.delete_o2o(&from_obj, &to_obj))
}

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

/// The error for an event index that does not name an event of `ocel`.
fn check_ev(ocel: &SlimLinkedOCEL, ev: EventIndex) -> Result<EventIndex, String> {
    match ev.get_ev_opt(ocel) {
        Some(_) => Ok(ev),
        None => Err(format!(
            "event index {} is out of range: the OCEL has {} event(s)",
            ev.into_inner(),
            ocel.get_num_evs()
        )),
    }
}

/// The error for an object index that does not name an object of `ocel`.
fn check_ob(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> Result<ObjectIndex, String> {
    match ob.get_ob_opt(ocel) {
        Some(_) => Ok(ob),
        None => Err(format!(
            "object index {} is out of range: the OCEL has {} object(s)",
            ob.into_inner(),
            ocel.get_num_obs()
        )),
    }
}

/// Get the ID string of an event.
///
/// # Errors
/// If the index is out of range.
#[register_binding(stringify_error)]
fn locel_get_ev_id(ocel: &SlimLinkedOCEL, ev: EventIndex) -> Result<String, String> {
    Ok(ocel.get_ev_id(check_ev(ocel, ev)?).to_string())
}

/// Get the ID string of an object.
///
/// # Errors
/// If the index is out of range.
#[register_binding(stringify_error)]
fn locel_get_ob_id(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> Result<String, String> {
    Ok(ocel.get_ob_id(check_ob(ocel, ob)?).to_string())
}

/// Get the event type (activity) of an event.
///
/// # Errors
/// If the index is out of range.
#[register_binding(stringify_error)]
fn locel_get_ev_type_of(ocel: &SlimLinkedOCEL, ev: EventIndex) -> Result<String, String> {
    Ok(ocel.get_ev_type_of(check_ev(ocel, ev)?).to_string())
}

/// Get the object type of an object.
///
/// # Errors
/// If the index is out of range.
#[register_binding(stringify_error)]
fn locel_get_ob_type_of(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> Result<String, String> {
    Ok(ocel.get_ob_type_of(check_ob(ocel, ob)?).to_string())
}

/// Get the timestamp of an event.
///
/// # Errors
/// If the index is out of range.
#[register_binding(stringify_error)]
fn locel_get_ev_time(
    ocel: &SlimLinkedOCEL,
    ev: EventIndex,
) -> Result<DateTime<FixedOffset>, String> {
    Ok(*ocel.get_ev_time(check_ev(ocel, ev)?))
}

/// Get the E2O relationships of an event as `(qualifier, object_index)` pairs.
///
/// # Errors
/// If the index is out of range.
#[register_binding(stringify_error)]
fn locel_get_e2o(
    ocel: &SlimLinkedOCEL,
    ev: EventIndex,
) -> Result<Vec<(String, ObjectIndex)>, String> {
    Ok(ocel
        .get_e2o(check_ev(ocel, ev)?)
        .map(|(q, o)| (q.to_string(), *o))
        .collect())
}

/// Get the reverse E2O relationships of an object (events relating to it) as
/// `(qualifier, event_index)` pairs.
///
/// # Errors
/// If the index is out of range.
#[register_binding(stringify_error)]
fn locel_get_e2o_rev(
    ocel: &SlimLinkedOCEL,
    ob: ObjectIndex,
) -> Result<Vec<(String, EventIndex)>, String> {
    Ok(ocel
        .get_e2o_rev(check_ob(ocel, ob)?)
        .map(|(q, e)| (q.to_string(), *e))
        .collect())
}

/// Get the activity trace of an object (i.e., the sequence of event types connected to the object,
/// ordered by event timestamp)
///
/// # Errors
/// If the index is out of range.
#[register_binding(stringify_error)]
fn locel_get_ob_activity_trace(
    ocel: &SlimLinkedOCEL,
    ob: ObjectIndex,
) -> Result<Vec<String>, String> {
    Ok(check_ob(ocel, ob)?
        .get_obj_activity_trace(ocel)
        .map(|act| act.to_string())
        .collect())
}

/// Get the outgoing O2O relationships of an object as `(qualifier, object_index)` pairs.
///
/// # Errors
/// If the index is out of range.
#[register_binding(stringify_error)]
fn locel_get_o2o(
    ocel: &SlimLinkedOCEL,
    ob: ObjectIndex,
) -> Result<Vec<(String, ObjectIndex)>, String> {
    Ok(ocel
        .get_o2o(check_ob(ocel, ob)?)
        .map(|(q, o)| (q.to_string(), *o))
        .collect())
}

/// Get the reverse O2O relationships of an object (objects with an O2O to it) as `(qualifier, object_index)` pairs.
///
/// # Errors
/// If the index is out of range.
#[register_binding(stringify_error)]
fn locel_get_o2o_rev(
    ocel: &SlimLinkedOCEL,
    ob: ObjectIndex,
) -> Result<Vec<(String, ObjectIndex)>, String> {
    Ok(ocel
        .get_o2o_rev(check_ob(ocel, ob)?)
        .map(|(q, o)| (q.to_string(), *o))
        .collect())
}

/// Get the full [`OCELEvent`] (resolved type name, named attributes, string object IDs).
///
/// Allocates. Prefer the specific `locel_get_ev_*` accessors for single fields.
///
/// # Errors
/// If the index is out of range.
#[register_binding(stringify_error)]
fn locel_get_full_ev(ocel: &SlimLinkedOCEL, ev: EventIndex) -> Result<OCELEvent, String> {
    Ok(ocel.get_full_ev(check_ev(ocel, ev)?).into_owned())
}

/// Get the full [`OCELObject`] (resolved type name, named time-indexed attributes, string object IDs).
///
/// Allocates. Prefer the specific `locel_get_ob_*` accessors for single fields.
///
/// # Errors
/// If the index is out of range.
#[register_binding(stringify_error)]
fn locel_get_full_ob(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> Result<OCELObject, String> {
    Ok(ocel.get_full_ob(check_ob(ocel, ob)?).into_owned())
}

/// Get the value of an event attribute by name. `None` if the attribute does not exist.
///
/// # Errors
/// If the index is out of range.
#[register_binding(stringify_error)]
fn locel_get_ev_attr_val(
    ocel: &SlimLinkedOCEL,
    ev: EventIndex,
    attr_name: String,
) -> Result<Option<OCELAttributeValue>, String> {
    Ok(ocel
        .get_ev_attr_val(check_ev(ocel, ev)?, &attr_name)
        .cloned())
}

/// Get the time-indexed history of an object attribute by name as `(timestamp, value)` pairs. Empty if absent.
///
/// # Errors
/// If the index is out of range.
#[register_binding(stringify_error)]
fn locel_get_ob_attr_vals(
    ocel: &SlimLinkedOCEL,
    ob: ObjectIndex,
    attr_name: String,
) -> Result<Vec<(DateTime<FixedOffset>, OCELAttributeValue)>, String> {
    Ok(ocel
        .get_ob_attr_vals(check_ob(ocel, ob)?, &attr_name)
        .map(|(t, v)| (*t, v.clone()))
        .collect())
}

/// Get the ids of all objects of a type.
#[register_binding]
fn locel_get_ob_ids_of_type(ocel: &SlimLinkedOCEL, ob_type: String) -> Vec<String> {
    ocel.get_obs_of_type(&ob_type)
        .map(|ob| ocel.get_ob_id(ob).to_string())
        .collect()
}

/// Get the ids of all events of a type.
#[register_binding]
fn locel_get_ev_ids_of_type(ocel: &SlimLinkedOCEL, ev_type: String) -> Vec<String> {
    ocel.get_evs_of_type(&ev_type)
        .map(|ev| ocel.get_ev_id(ev).to_string())
        .collect()
}

/// Get the ids of the events related to an object, or `None` for an unknown object id.
#[register_binding]
fn locel_get_e2o_rev_ids(ocel: &SlimLinkedOCEL, ob_id: String) -> Option<Vec<String>> {
    ocel.get_ob_by_id(&ob_id).map(|ob| {
        ob.get_e2o_rev(ocel)
            .map(|ev| ocel.get_ev_id(ev).to_string())
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn batch(value: serde_json::Value) -> OCELAppendBatch {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn append_batch_adds_and_updates() {
        let mut ocel = SlimLinkedOCEL::new();
        let first = locel_append_batch(
            &mut ocel,
            batch(json!({
                "events": [{
                    "id": "e1", "type": "craft", "time": "2026-01-01T10:00:00Z",
                    "attributes": [{"name": "count", "value": 1}],
                    "relationships": [
                        {"objectId": "p1", "qualifier": "actor"},
                        {"objectId": "pick", "qualifier": "output"}
                    ]
                }],
                "objects": [
                    {"id": "pick", "type": "item",
                     "attributes": [{"name": "durability", "time": "2026-01-01T10:00:00Z", "value": 250}],
                     "relationships": [{"objectId": "p1", "qualifier": "owner"}]},
                    {"id": "p1", "type": "player"}
                ]
            })),
        )
        .unwrap();
        assert_eq!(
            first,
            OCELAppendSummary {
                events_added: 1,
                objects_added: 2,
                objects_updated: 0
            }
        );
        let event = ocel.get_ev_by_id("e1").unwrap();
        assert_eq!(event.get_e2o(&ocel).count(), 2);
        let pick = ocel.get_ob_by_id("pick").unwrap();
        assert_eq!(pick.get_o2o(&ocel).count(), 1);

        let second = locel_append_batch(
            &mut ocel,
            batch(json!({
                "objects": [
                    {"id": "pick", "type": "item",
                     "attributes": [
                        {"name": "durability", "time": "2026-01-01T11:00:00Z", "value": 249},
                        {"name": "enchanted", "time": "2026-01-01T11:00:00Z", "value": true}
                     ],
                     "relationships": [{"objectId": "chest", "qualifier": "stored in"}]},
                    {"id": "chest", "type": "container"}
                ]
            })),
        )
        .unwrap();
        assert_eq!(second.objects_added, 1);
        assert_eq!(second.objects_updated, 1);
        assert_eq!(
            pick.get_attribute_value("durability", &ocel).unwrap().len(),
            2
        );
        assert_eq!(
            pick.get_attribute_value("enchanted", &ocel).unwrap().len(),
            1
        );
        assert_eq!(pick.get_o2o(&ocel).count(), 2);

        let exported = ocel.construct_ocel();
        assert_eq!(exported.events.len(), 1);
        assert_eq!(exported.objects.len(), 3);
    }

    #[test]
    fn append_batch_rejects_without_changes() {
        let mut ocel = SlimLinkedOCEL::new();
        let one = json!({
            "events": [{"id": "e1", "type": "a", "time": "2026-01-01T10:00:00Z"}],
            "objects": [{"id": "o1", "type": "x"}]
        });
        locel_append_batch(&mut ocel, batch(one.clone())).unwrap();
        assert!(locel_append_batch(&mut ocel, batch(one)).is_err());
        let wrong_type = json!({
            "objects": [{"id": "o2", "type": "x"}, {"id": "o1", "type": "y"}]
        });
        assert!(locel_append_batch(&mut ocel, batch(wrong_type)).is_err());
        let new_id_two_types = json!({
            "objects": [{"id": "o3", "type": "x"}, {"id": "o3", "type": "y"}]
        });
        let err = locel_append_batch(&mut ocel, batch(new_id_two_types)).unwrap_err();
        assert_eq!(err, "object 'o3' has type 'x', not 'y'");
        let twice = json!({
            "events": [
                {"id": "e2", "type": "a", "time": "2026-01-01T10:00:00Z"},
                {"id": "e2", "type": "a", "time": "2026-01-01T11:00:00Z"}
            ]
        });
        let err = locel_append_batch(&mut ocel, batch(twice)).unwrap_err();
        assert_eq!(err, "event id 'e2' occurs twice in the batch");
        assert_eq!(ocel.get_num_obs(), 1);
        assert_eq!(ocel.get_num_evs(), 1);
    }

    #[test]
    fn append_batch_counts_only_changed_existing_objects() {
        let mut ocel = SlimLinkedOCEL::new();
        let first = json!({
            "objects": [{"id": "o1", "type": "x"}, {"id": "o2", "type": "x"}]
        });
        locel_append_batch(&mut ocel, batch(first)).unwrap();
        let second = locel_append_batch(
            &mut ocel,
            batch(json!({
                "objects": [
                    {"id": "o1", "type": "x"},
                    {"id": "o2", "type": "x",
                     "attributes": [{"name": "n", "time": "2026-01-01T10:00:00Z", "value": 1}]},
                    {"id": "o2", "type": "x",
                     "attributes": [{"name": "n", "time": "2026-01-01T11:00:00Z", "value": 2}]},
                    {"id": "o3", "type": "x"},
                    {"id": "o3", "type": "x",
                     "attributes": [{"name": "n", "time": "2026-01-01T10:00:00Z", "value": 3}]}
                ]
            })),
        )
        .unwrap();
        assert_eq!(
            second,
            OCELAppendSummary {
                events_added: 0,
                objects_added: 1,
                objects_updated: 1
            }
        );
        let o2 = ocel.get_ob_by_id("o2").unwrap();
        assert_eq!(o2.get_attribute_value("n", &ocel).unwrap().len(), 2);
        let o3 = ocel.get_ob_by_id("o3").unwrap();
        assert_eq!(o3.get_attribute_value("n", &ocel).unwrap().len(), 1);
    }

    #[test]
    fn append_batch_does_not_count_a_present_o2o_as_update() {
        let mut ocel = SlimLinkedOCEL::new();
        let linked = json!({
            "objects": [
                {"id": "o1", "type": "x", "relationships": [{"objectId": "o2", "qualifier": "q"}]},
                {"id": "o2", "type": "x"}
            ]
        });
        locel_append_batch(&mut ocel, batch(linked)).unwrap();
        let again = json!({
            "objects": [{"id": "o1", "type": "x",
                         "relationships": [{"objectId": "o2", "qualifier": "q"}]}]
        });
        let summary = locel_append_batch(&mut ocel, batch(again)).unwrap();
        assert_eq!(summary.objects_updated, 0);
        let other_qualifier = json!({
            "objects": [{"id": "o1", "type": "x",
                         "relationships": [{"objectId": "o2", "qualifier": "r"}]}]
        });
        let summary = locel_append_batch(&mut ocel, batch(other_qualifier)).unwrap();
        assert_eq!(summary.objects_updated, 1);
    }

    #[test]
    fn relationship_edits_reject_out_of_range_indices() {
        let mut ocel = SlimLinkedOCEL::new();
        ocel.add_event_type("a", Vec::new());
        ocel.add_object_type("x", Vec::new());
        let ob = ocel.add_object("x", None, Vec::new(), Vec::new()).unwrap();
        let at = DateTime::parse_from_rfc3339("2026-01-01T10:00:00Z").unwrap();
        let ev = ocel
            .add_event("a", at, None, Vec::new(), Vec::new())
            .unwrap();
        let (bad_ev, bad_ob) = (EventIndex::from(7), ObjectIndex::from(7));

        assert!(locel_add_e2o(&mut ocel, ev, ob, "q".to_string()).unwrap());
        assert!(locel_add_o2o(&mut ocel, ob, ob, "q".to_string()).unwrap());
        assert!(locel_delete_e2o(&mut ocel, ev, ob).unwrap());
        assert!(locel_delete_o2o(&mut ocel, ob, ob).unwrap());

        let err = locel_add_e2o(&mut ocel, bad_ev, ob, "q".to_string()).unwrap_err();
        assert!(err.contains("event index 7 is out of range"), "{err}");
        let err = locel_add_o2o(&mut ocel, ob, bad_ob, "q".to_string()).unwrap_err();
        assert!(err.contains("object index 7 is out of range"), "{err}");
        assert!(locel_delete_e2o(&mut ocel, ev, bad_ob).is_err());
        assert!(locel_delete_o2o(&mut ocel, bad_ob, ob).is_err());
    }

    #[test]
    fn attribute_value_keeps_history_ordered() {
        let mut ocel = SlimLinkedOCEL::new();
        ocel.add_object_type(
            "item",
            vec![OCELTypeAttribute {
                name: "durability".to_string(),
                value_type: "integer".to_string(),
            }],
        );
        let item = ocel
            .add_object("item", None, Vec::new(), Vec::new())
            .unwrap();
        let at = |s: &str| DateTime::parse_from_rfc3339(s).unwrap();
        for (time, value) in [("2026-01-01T12:00:00Z", 3), ("2026-01-01T10:00:00Z", 1)] {
            locel_add_object_attribute_value(
                &mut ocel,
                item,
                "durability".to_string(),
                at(time),
                OCELAttributeValue::Integer(value),
            )
            .unwrap();
        }
        let history = item.get_attribute_value("durability", &ocel).unwrap();
        assert_eq!(history[0].1, OCELAttributeValue::Integer(1));
        assert_eq!(history[1].1, OCELAttributeValue::Integer(3));
        assert!(locel_add_object_attribute_value(
            &mut ocel,
            item,
            "missing".to_string(),
            at("2026-01-01T10:00:00Z"),
            OCELAttributeValue::Null,
        )
        .is_err());
    }
}
