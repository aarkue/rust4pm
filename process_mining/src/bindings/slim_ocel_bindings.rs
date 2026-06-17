//! Binding wrappers for [`SlimLinkedOCEL`] functionality

use std::collections::HashMap;

use chrono::{DateTime, FixedOffset};
use macros_process_mining::register_binding;
use rayon::prelude::*;

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

/// Get the activity trace of an object (i.e., the sequence of event types connected to the object, ordered by event timestamp)
#[register_binding]
fn get_obj_activity_trace(ocel: &SlimLinkedOCEL, ob: ObjectIndex) -> Vec<String> {
    ob.get_obj_activity_trace(ocel)
        .map(|act| act.to_string())
        .collect()
}

/// Merge `b` into `a` by summing counts for matching keys. Used as the rayon reduce step.
fn merge_count_maps<K: std::hash::Hash + Eq>(
    mut a: HashMap<K, usize>,
    b: HashMap<K, usize>,
) -> HashMap<K, usize> {
    for (k, v) in b {
        *a.entry(k).or_insert(0) += v;
    }
    a
}

/// Get all activity-trace variants for objects of the given object type, with their occurrence counts
///
/// Each entry is a tuple `(activity_trace, count)`, where `activity_trace` is the sequence of event types
/// connected to an object (ordered by event timestamp), and `count` is the number of objects of the
/// requested type that share that exact trace.
#[register_binding]
fn get_variants_of_object_type(
    ocel: &SlimLinkedOCEL,
    ob_type: String,
) -> Vec<(Vec<String>, usize)> {
    let obs: Vec<ObjectIndex> = ocel.get_obs_of_type(&ob_type).copied().collect();
    let counts: HashMap<Vec<usize>, usize> = obs
        .into_par_iter()
        .fold(HashMap::new, |mut acc, ob| {
            let trace: Vec<usize> = ob.get_obj_activity_trace_evtype_indices(ocel).collect();
            *acc.entry(trace).or_insert(0) += 1;
            acc
        })
        .reduce(HashMap::new, merge_count_maps);
    let ev_type_names: Vec<&str> =
        <SlimLinkedOCEL as LinkedOCELAccess>::get_ev_types(ocel).collect();
    let result: Vec<(Vec<String>, usize)> = counts
        .into_iter()
        .map(|(trace_idx, count)| {
            let trace: Vec<String> = trace_idx
                .into_iter()
                .map(|i| ev_type_names[i].to_string())
                .collect();
            (trace, count)
        })
        .collect();
    // result.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    result
}

/// Get the directly-follows graph (DFG) for objects of the given object type.
///
/// Each entry is `((from_activity, to_activity), count)`, counting adjacent pairs in each
/// object's timestamp-ordered activity trace. Result order is unspecified.
#[register_binding]
fn get_dfg_of_object_type(
    ocel: &SlimLinkedOCEL,
    ob_type: String,
) -> Vec<((String, String), usize)> {
    let obs: Vec<ObjectIndex> = ocel.get_obs_of_type(&ob_type).copied().collect();
    let counts: HashMap<(usize, usize), usize> = obs
        .into_par_iter()
        .fold(HashMap::new, |mut acc, ob| {
            let mut iter = ob.get_obj_activity_trace_evtype_indices(ocel);
            if let Some(mut prev) = iter.next() {
                for next in iter {
                    *acc.entry((prev, next)).or_insert(0) += 1;
                    prev = next;
                }
            }
            acc
        })
        .reduce(HashMap::new, merge_count_maps);
    let ev_type_names: Vec<&str> =
        <SlimLinkedOCEL as LinkedOCELAccess>::get_ev_types(ocel).collect();
    counts
        .into_iter()
        .map(|((from, to), count)| {
            (
                (
                    ev_type_names[from].to_string(),
                    ev_type_names[to].to_string(),
                ),
                count,
            )
        })
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

#[register_binding]
fn get_object_ids_of_type(ocel: &SlimLinkedOCEL, ob_type: String) -> Vec<String> {
    ocel.get_obs_of_type(&ob_type)
        .map(|ob| ocel.get_ob_id(ob).to_string())
        .collect()
}

#[register_binding]
fn get_event_ids_of_type(ocel: &SlimLinkedOCEL, ev_type: String) -> Vec<String> {
    ocel.get_evs_of_type(&ev_type)
        .map(|ev| ocel.get_ev_id(ev).to_string())
        .collect()
}

#[register_binding]
fn get_object_type_of_id(ocel: &SlimLinkedOCEL, ob_id: &String) -> Option<String> {
    ocel.get_ob_by_id(ob_id)
        .map(|ob| ob.get_ob_type(ocel).to_string())
}

#[register_binding]
fn get_e2o_rev_ids(ocel: &SlimLinkedOCEL, ob_id: &String) -> Option<Vec<String>> {
    ocel.get_ob_by_id(ob_id).map(|ob| {
        ob.get_e2o_rev(ocel)
            .map(|ev| ocel.get_ev_id(ev).to_string())
            .collect()
    })
}

#[register_binding]
fn get_e2o_ids(ocel: &SlimLinkedOCEL, ev_id: &String) -> Option<Vec<String>> {
    ocel.get_ev_by_id(ev_id).map(|ev| {
        ev.get_e2o(ocel)
            .map(|ob| ocel.get_ob_id(ob).to_string())
            .collect()
    })
}

#[register_binding]
fn get_o2o_ids(ocel: &SlimLinkedOCEL, ob_id: &String) -> Option<Vec<String>> {
    ocel.get_ob_by_id(ob_id).map(|ob| {
        ob.get_o2o(ocel)
            .map(|ev| ocel.get_ob_id(ev).to_string())
            .collect()
    })
}

#[register_binding]
fn get_event_type_of_id(ocel: &SlimLinkedOCEL, ev_id: &String) -> Option<String> {
    ocel.get_ev_by_id(ev_id)
        .map(|ev| ev.get_ev_type(ocel).to_string())
}

#[register_binding]
fn get_event_timestamp_of_id(ocel: &SlimLinkedOCEL, ev_id: &String) -> Option<String> {
    ocel.get_ev_by_id(ev_id)
        .map(|ev| ev.get_time(ocel).to_string())
}

// ── Analytics ─────────────────────────────────────────────────────────

/// Count E2O relationships per `(event_type, object_type)` pair.
///
/// Each entry is `(event_type, object_type, count)`. A single `(event, object)` pair connected
/// by multiple qualifiers contributes once per qualifier. Pairs with zero relations are omitted.
/// Result row order is unspecified.
#[register_binding]
fn locel_event_object_type_counts(ocel: &SlimLinkedOCEL) -> Vec<(String, String, i64)> {
    let num_events = ocel.get_num_evs() as u32;
    let counts: HashMap<(usize, usize), i64> = (0..num_events)
        .into_par_iter()
        .fold(HashMap::new, |mut acc, i| {
            let ev = EventIndex::from(i).get_ev(ocel);
            for (_q, ob) in &ev.relationships {
                let ot = ob.get_ob(ocel).object_type;
                *acc.entry((ev.event_type, ot)).or_insert(0) += 1;
            }
            acc
        })
        .reduce(HashMap::new, merge_sum_maps);
    let ev_types: Vec<&str> = <SlimLinkedOCEL as LinkedOCELAccess>::get_ev_types(ocel).collect();
    let ob_types: Vec<&str> = <SlimLinkedOCEL as LinkedOCELAccess>::get_ob_types(ocel).collect();
    counts
        .into_iter()
        .map(|((e, o), c)| (ev_types[e].to_string(), ob_types[o].to_string(), c))
        .collect()
}

/// Conversion rate from `source_type` to `target_type` via O2O, restricted to targets touched by `activity`.
///
/// Returns the fraction of `source_type` objects that have at least one outgoing O2O edge to a
/// `target_type` object related (via E2O) to some event of the given event type. Returns `0.0`
/// if no `source_type` objects exist.
#[register_binding]
fn locel_conversion_rate(
    ocel: &SlimLinkedOCEL,
    activity: String,
    source_type: String,
    target_type: String,
) -> f64 {
    let sources: Vec<ObjectIndex> = ocel.get_obs_of_type(&source_type).copied().collect();
    let total = sources.len();
    if total == 0 {
        return 0.0;
    }
    let reached = sources
        .par_iter()
        .filter(|&&s| {
            s.get_o2o(ocel).any(|&t| {
                t.get_ob_type(ocel) == &target_type
                    && t.get_e2o_rev(ocel)
                        .any(|&e| e.get_ev_type(ocel) == &activity)
            })
        })
        .count();
    reached as f64 / total as f64
}

/// Each object's reverse-E2O events in `(time, id)` order.
///
/// Events are sorted per call, so this makes no assumption about global event ordering
fn sorted_events_per_object(ocel: &SlimLinkedOCEL) -> Vec<Vec<EventIndex>> {
    (0..ocel.get_num_obs() as u32)
        .into_par_iter()
        .map(|i| {
            let mut evs: Vec<EventIndex> =
                ObjectIndex::from(i).get_e2o_rev(ocel).copied().collect();
            evs.sort_by(|a, b| {
                a.get_time(ocel)
                    .cmp(b.get_time(ocel))
                    .then_with(|| ocel.get_ev_id(a).cmp(ocel.get_ev_id(b)))
            });
            evs
        })
        .collect()
}

/// The `(time, id)`-immediate predecessor of `e` on object `o`, using the
/// per-object sorted lists from [`sorted_events_per_object`].
/// `None` if `e` is the first event on `o`.
#[inline]
fn df_predecessor(
    sorted: &[Vec<EventIndex>],
    ocel: &SlimLinkedOCEL,
    e: EventIndex,
    o: ObjectIndex,
) -> Option<EventIndex> {
    let evs = &sorted[o.into_inner() as usize];
    let key = (e.get_time(ocel), ocel.get_ev_id(&e));
    let pos = evs
        .binary_search_by(|x| (x.get_time(ocel), ocel.get_ev_id(x)).cmp(&key))
        .ok()?;
    pos.checked_sub(1).map(|p| evs[p])
}

/// Per-event synchronization time and the delaying object.
///
/// For each event with at least one directly-follows predecessor, the synchronization time is
/// `max_predecessor_time - min_predecessor_time` in integer microseconds (the span between its
/// earliest and latest directly-preceding event). The delaying object is the object linking the
/// latest predecessor (ties broken by ascending object id).
/// Returns one row `(event_id, sync_us, delaying_object_id)` per qualifying event.
///
/// `top_k`: if `Some(k)`, return only the `k` rows with the largest `sync_us`, ties broken by
/// ascending event id, sorted descending. `None` returns every qualifying event.
#[register_binding]
fn locel_oc_perf_sync_per_event(
    ocel: &SlimLinkedOCEL,
    #[bind(default)] top_k: Option<usize>,
) -> Vec<(String, i64, String)> {
    let sorted = sorted_events_per_object(ocel);
    let mut rows: Vec<(EventIndex, i64, ObjectIndex)> = (0..ocel.get_num_evs() as u32)
        .into_par_iter()
        .filter_map(|i| {
            let e = EventIndex::from(i);
            let mut min_us = i64::MAX;
            // (latest predecessor time, its object) = the delaying edge.
            let mut delaying: Option<(i64, ObjectIndex)> = None;
            for &o in e.get_e2o(ocel) {
                if let Some(p) = df_predecessor(&sorted, ocel, e, o) {
                    let t = p.get_time(ocel).timestamp_micros();
                    min_us = min_us.min(t);
                    let keep = match delaying {
                        Some((bt, bo)) => {
                            bt > t || (bt == t && ocel.get_ob_id(&bo) <= ocel.get_ob_id(&o))
                        }
                        None => false,
                    };
                    if !keep {
                        delaying = Some((t, o));
                    }
                }
            }
            delaying.map(|(max_us, o)| (e, max_us - min_us, o))
        })
        .collect();
    if let Some(k) = top_k {
        let cmp = |a: &(EventIndex, i64, ObjectIndex), b: &(EventIndex, i64, ObjectIndex)| {
            b.1.cmp(&a.1)
                .then_with(|| ocel.get_ev_id(&a.0).cmp(ocel.get_ev_id(&b.0)))
        };
        if k < rows.len() {
            rows.select_nth_unstable_by(k, cmp);
            rows.truncate(k);
        }
        rows.sort_unstable_by(cmp);
    }
    rows.into_iter()
        .map(|(e, max_minus_min, o)| {
            (
                ocel.get_ev_id(&e).to_string(),
                max_minus_min,
                ocel.get_ob_id(&o).to_string(),
            )
        })
        .collect()
}

/// Per-event sojourn time.
///
/// For each event with at least one directly-follows predecessor, the sojourn time is
/// `event_time - latest_predecessor_time` in integer microseconds. Returns one row
/// `(event_id, sojourn_us)` per qualifying event.
///
/// `top_k`: if `Some(k)`, return only the `k` rows with the largest `sojourn_us`, ties broken by
/// ascending event id, sorted descending. `None` returns every qualifying event.
#[register_binding]
fn locel_oc_perf_sojourn_per_event(
    ocel: &SlimLinkedOCEL,
    #[bind(default)] top_k: Option<usize>,
) -> Vec<(String, i64)> {
    let sorted = sorted_events_per_object(ocel);
    let mut rows: Vec<(EventIndex, i64)> = (0..ocel.get_num_evs() as u32)
        .into_par_iter()
        .filter_map(|i| {
            let e = EventIndex::from(i);
            let latest = e
                .get_e2o(ocel)
                .filter_map(|&o| df_predecessor(&sorted, ocel, e, o))
                .map(|p| p.get_time(ocel).timestamp_micros())
                .max()?;
            Some((e, e.get_time(ocel).timestamp_micros() - latest))
        })
        .collect();
    if let Some(k) = top_k {
        let cmp = |a: &(EventIndex, i64), b: &(EventIndex, i64)| {
            b.1.cmp(&a.1)
                .then_with(|| ocel.get_ev_id(&a.0).cmp(ocel.get_ev_id(&b.0)))
        };
        if k < rows.len() {
            rows.select_nth_unstable_by(k, cmp);
            rows.truncate(k);
        }
        rows.sort_unstable_by(cmp);
    }
    rows.into_iter()
        .map(|(e, sojourn_us)| (ocel.get_ev_id(&e).to_string(), sojourn_us))
        .collect()
}

/// Merge `b` into `a` by summing `i64` counts for matching keys. Used as the rayon reduce step.
fn merge_sum_maps<K: std::hash::Hash + Eq>(
    mut a: HashMap<K, i64>,
    b: HashMap<K, i64>,
) -> HashMap<K, i64> {
    for (k, v) in b {
        *a.entry(k).or_insert(0) += v;
    }
    a
}
