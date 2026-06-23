//! Object-centric performance analysis over [`SlimLinkedOCEL`]: per-event sojourn and
//! synchronization times.

use macros_process_mining::register_binding;
use rayon::prelude::*;

use crate::core::event_data::object_centric::linked_ocel::{
    slim_linked_ocel::{EventIndex, ObjectIndex},
    LinkedOCELAccess, SlimLinkedOCEL,
};

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
pub fn locel_oc_perf_sync_per_event(
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
pub fn locel_oc_perf_sojourn_per_event(
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
