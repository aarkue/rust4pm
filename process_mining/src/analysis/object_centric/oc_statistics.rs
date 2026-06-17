//! Descriptive statistics over object-centric event data (E2O type counts, conversion rates).

use std::collections::HashMap;

use macros_process_mining::register_binding;
use rayon::prelude::*;

use crate::core::event_data::object_centric::linked_ocel::{
    slim_linked_ocel::{EventIndex, ObjectIndex},
    LinkedOCELAccess, SlimLinkedOCEL,
};

/// Count E2O relationships per `(event_type, object_type)` pair.
///
/// Each entry is `(event_type, object_type, count)`. A single `(event, object)` pair connected
/// by multiple qualifiers contributes once per qualifier. Pairs with zero relations are omitted.
/// Result row order is unspecified.
#[register_binding]
pub fn locel_event_object_type_counts(ocel: &SlimLinkedOCEL) -> Vec<(String, String, i64)> {
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
pub fn locel_conversion_rate(
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
