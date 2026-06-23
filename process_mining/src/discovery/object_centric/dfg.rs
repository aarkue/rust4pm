//! Discover directly-follows graphs (DFG) from object-centric event data.

use std::collections::HashMap;

use macros_process_mining::register_binding;
use rayon::prelude::*;

use crate::core::event_data::object_centric::linked_ocel::{
    slim_linked_ocel::ObjectIndex, LinkedOCELAccess, SlimLinkedOCEL,
};

use super::merge_count_maps;

/// Get the directly-follows graph (DFG) for objects of the given object type.
///
/// Each entry is `((from_activity, to_activity), count)`, counting adjacent pairs in each
/// object's timestamp-ordered activity trace. Sorted by count descending, ties broken by
/// `(from_activity, to_activity)`.
#[register_binding]
pub fn get_dfg_of_object_type(
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
    let mut result: Vec<((String, String), usize)> = counts
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
        .collect();
    result.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    result
}
