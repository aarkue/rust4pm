//! Descriptive statistics over object-centric event data (E2O type counts, conversion rates).

use std::collections::HashMap;

use macros_process_mining::register_binding;
use rayon::prelude::*;

use crate::core::event_data::object_centric::linked_ocel::{
    slim_linked_ocel::{EventIndex, ObjectIndex},
    LinkedOCELAccess, SlimLinkedOCEL,
};

/// Count related `(event, object)` pairs per `(event_type, object_type)` pair.
///
/// Each entry is `(event_type, object_type, count)`. An object related to an event under several
/// qualifiers counts once for that event. Pairs with zero relations are omitted.
/// Sorted by `(event_type, object_type)`.
#[register_binding]
pub fn event_object_type_counts(ocel: &SlimLinkedOCEL) -> Vec<(String, String, i64)> {
    let num_events = ocel.get_num_evs() as u32;
    let counts: HashMap<(usize, usize), i64> = (0..num_events)
        .into_par_iter()
        .fold(HashMap::new, |mut acc, i| {
            let ev = EventIndex::from(i).get_ev(ocel);
            let mut prev: Option<ObjectIndex> = None;
            // Relationships are kept sorted by object, so repeats of one object are adjacent.
            for (_q, ob) in &ev.relationships {
                if prev == Some(*ob) {
                    continue;
                }
                prev = Some(*ob);
                let ot = ob.get_ob(ocel).object_type;
                *acc.entry((ev.event_type, ot)).or_insert(0) += 1;
            }
            acc
        })
        .reduce(HashMap::new, merge_sum_maps);
    let ev_types: Vec<&str> = <SlimLinkedOCEL as LinkedOCELAccess>::get_ev_types(ocel).collect();
    let ob_types: Vec<&str> = <SlimLinkedOCEL as LinkedOCELAccess>::get_ob_types(ocel).collect();
    let mut result: Vec<(String, String, i64)> = counts
        .into_iter()
        .map(|((e, o), c)| (ev_types[e].to_string(), ob_types[o].to_string(), c))
        .collect();
    result.sort_unstable_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    result
}

/// Conversion rate from `source_type` to `target_type` via O2O, restricted to targets touched by `activity`.
///
/// Returns the fraction of `source_type` objects that have at least one outgoing O2O edge to a
/// `target_type` object related (via E2O) to some event of the given event type. Returns `0.0`
/// if no `source_type` objects exist.
///
/// # Errors
/// Returns an error if `activity` names no declared event type, or if either object type is not
/// declared, so a rate of `0.0` cannot mean a misspelled name.
#[register_binding(stringify_error)]
pub fn conversion_rate(
    ocel: &SlimLinkedOCEL,
    activity: String,
    source_type: String,
    target_type: String,
) -> Result<f64, String> {
    if ocel.get_ev_type(&activity).is_none() {
        return Err(format!("no event type '{activity}'"));
    }
    for ob_type in [&source_type, &target_type] {
        if ocel.get_ob_type(ob_type).is_none() {
            return Err(format!("no object type '{ob_type}'"));
        }
    }
    let sources: Vec<ObjectIndex> = ocel.get_obs_of_type(&source_type).copied().collect();
    let total = sources.len();
    if total == 0 {
        return Ok(0.0);
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
    Ok(reached as f64 / total as f64)
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

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::*;

    fn small_ocel() -> SlimLinkedOCEL {
        let mut ocel = SlimLinkedOCEL::new();
        let t = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap();
        ocel.add_event_type("pay", Vec::new());
        ocel.add_event_type("create", Vec::new());
        ocel.add_object_type("order", Vec::new());
        ocel.add_object_type("item", Vec::new());
        let order = ocel
            .add_object("order", None, Vec::new(), Vec::new())
            .unwrap();
        let item = ocel
            .add_object("item", None, Vec::new(), Vec::new())
            .unwrap();
        ocel.add_event(
            "create",
            t,
            None,
            Vec::new(),
            vec![
                ("created".to_string(), order),
                ("primary".to_string(), order),
                ("part".to_string(), item),
            ],
        )
        .unwrap();
        ocel.add_event(
            "pay",
            t,
            None,
            Vec::new(),
            vec![("paid".to_string(), order)],
        )
        .unwrap();
        ocel
    }

    #[test]
    fn event_object_type_counts_dedupes_qualifiers_and_sorts() {
        let counts = event_object_type_counts(&small_ocel());
        let expected = [
            ("create", "item", 1),
            ("create", "order", 1),
            ("pay", "order", 1),
        ];
        let expected: Vec<(String, String, i64)> = expected
            .iter()
            .map(|(e, o, c)| (e.to_string(), o.to_string(), *c))
            .collect();
        assert_eq!(counts, expected);
    }

    #[test]
    fn conversion_rate_rejects_unknown_names() {
        let ocel = small_ocel();
        let rate = |a: &str, s: &str, t: &str| {
            conversion_rate(&ocel, a.to_string(), s.to_string(), t.to_string())
        };
        assert!(rate("payy", "order", "item").is_err());
        assert!(rate("pay", "orderr", "item").is_err());
        assert!(rate("pay", "order", "itemm").is_err());
        assert_eq!(rate("pay", "order", "item"), Ok(0.0));
    }
}
