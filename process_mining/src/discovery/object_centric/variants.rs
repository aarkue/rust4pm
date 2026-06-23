//! Discover activity-trace variants from object-centric event data.

use std::collections::HashMap;

use macros_process_mining::register_binding;
use rayon::prelude::*;

use crate::core::event_data::object_centric::linked_ocel::{
    slim_linked_ocel::ObjectIndex, LinkedOCELAccess, SlimLinkedOCEL,
};

use super::merge_count_maps;

/// Get all activity-trace variants for objects of the given object type, with their occurrence counts
///
/// Each entry is a tuple `(activity_trace, count)`, where `activity_trace` is the sequence of event types
/// connected to an object (ordered by event timestamp), and `count` is the number of objects of the
/// requested type that share that exact trace. Sorted by count descending, ties broken by the
/// activity trace.
#[register_binding]
pub fn get_variants_of_object_type(
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
    let mut result: Vec<(Vec<String>, usize)> = counts
        .into_iter()
        .map(|(trace_idx, count)| {
            let trace: Vec<String> = trace_idx
                .into_iter()
                .map(|i| ev_type_names[i].to_string())
                .collect();
            (trace, count)
        })
        .collect();
    result.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    result
}
