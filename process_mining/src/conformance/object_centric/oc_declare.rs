//! Performance-focused implementations of checking OC-DECLARE constraints
use std::sync::atomic::AtomicU32;

use crate::core::{
    event_data::object_centric::linked_ocel::{
        slim_linked_ocel::ObjectIndex, LinkedOCELAccess, SlimLinkedOCEL,
    },
    process_models::oc_declare::{
        EventOrSynthetic, OCDeclareArc, OCDeclareArcLabel, OCDeclareArcType, SetFilter,
    },
};

use chrono::{DateTime, FixedOffset};
use macros_process_mining::register_binding;
use rayon::prelude::*;

/// Get all events of the given event type satisfying the filters
pub fn get_evs_with_objs_perf<'a>(
    objs: &'a [SetFilter<&ObjectIndex>],
    linked_ocel: &'a SlimLinkedOCEL,
    etype: &'a str,
) -> impl Iterator<Item = EventOrSynthetic> + use<'a> {
    let initial: Box<dyn Iterator<Item = EventOrSynthetic>> = if objs.is_empty() {
        Box::new(EventOrSynthetic::get_all_syn_evs(linked_ocel, etype).into_iter())
    } else {
        match &objs[0] {
            SetFilter::Any(items) => Box::new(
                items
                    .iter()
                    .flat_map(|o| EventOrSynthetic::get_all_of_et_for_ob(linked_ocel, etype, **o)),
            ),
            SetFilter::All(items) => {
                if items.is_empty() {
                    Box::new(Vec::new().into_iter())
                } else {
                    Box::new(
                        EventOrSynthetic::get_all_of_et_for_ob(linked_ocel, etype, *items[0])
                            .filter(|e| {
                                items
                                    .iter()
                                    .skip(1)
                                    .all(|o| e.get_e2o_set(linked_ocel).contains(o))
                            }),
                    )
                }
            }
        }
    };
    initial.filter(|e| {
        let obs = &e.get_e2o_set(linked_ocel);
        for o in objs.iter() {
            if !o.check(obs) {
                return false;
            }
        }
        true
    })
}

fn get_df_or_dp_event_perf<'a>(
    objs: &'a [SetFilter<&'a ObjectIndex>],
    linked_ocel: &'a SlimLinkedOCEL,
    // reference_mock_event_index: &'a EventIndex,
    reference_time: &'a DateTime<FixedOffset>,
    // reference_event: &'a OCELEvent,
    following: bool,
) -> Option<EventOrSynthetic> {
    let initial: Box<dyn Iterator<Item = EventOrSynthetic>> = if objs.is_empty() {
        // If no requirements are specified, consider all events
        // TODO: Maybe also consider synthetic events here?
        // But in general, this is not very relevant as there are usually some object requirements
        Box::new(linked_ocel.get_all_evs().map(EventOrSynthetic::Event))
    } else {
        match &objs[0] {
            SetFilter::Any(items) => Box::new(items.iter().flat_map(|o| {
                EventOrSynthetic::get_all_for_ob(linked_ocel, **o)
                    .into_iter()
                    .filter(|e| {
                        let e_time = e.get_timestamp(linked_ocel);
                        if following {
                            e_time > *reference_time
                        } else {
                            e_time < *reference_time
                        }
                    })
            })),
            SetFilter::All(items) => {
                if items.is_empty() {
                    Box::new(Vec::new().into_iter())
                } else {
                    Box::new(
                        EventOrSynthetic::get_all_for_ob(linked_ocel, *items[0])
                            .into_iter()
                            .filter(|e| {
                                let e_time = e.get_timestamp(linked_ocel);
                                if following {
                                    e_time > *reference_time
                                } else {
                                    e_time < *reference_time
                                }
                            }),
                    )
                }
            }
        }
    };
    let x = initial.filter(|e| {
        for o in objs.iter() {
            let obs = &e.get_e2o_set(linked_ocel);
            if !o.check(obs) {
                return false;
            }
        }
        true
    });
    match following {
        true => x.min_by_key(|a| a.get_timestamp(linked_ocel)),
        false => x.max_by_key(|a| a.get_timestamp(linked_ocel)),
    }
}

/// Get fraction of source events violating this constraint arc
///
/// Returns a value from 0 (all source events satisfy this constraint) to 1 (all source events violate this constraint)
pub fn get_for_all_evs_perf(
    from_et: &str,
    to_et: &str,
    label: &OCDeclareArcLabel,
    arc_type: &OCDeclareArcType,
    counts: &(Option<usize>, Option<usize>),
    linked_ocel: &SlimLinkedOCEL,
) -> f64 {
    let evs = EventOrSynthetic::get_all_syn_evs(linked_ocel, from_et);
    let ev_count = evs.len();
    let violated_evs_count = evs
        // .into_par_iter()
        .into_iter()
        .filter(|ev| get_for_ev_perf(ev, label, to_et, arc_type, counts, linked_ocel))
        .count();
    violated_evs_count as f64 / ev_count as f64
}

/// Checks whether the number of events violating this constraint arc is below (<=) the given noise threshold
///
/// Returns false, if the fraction of events violating the constraint is above the noise threshold.
pub fn get_for_all_evs_perf_thresh(
    from_et: &str,
    to_et: &str,
    label: &OCDeclareArcLabel,
    arc_type: &OCDeclareArcType,
    counts: &(Option<usize>, Option<usize>),
    linked_ocel: &SlimLinkedOCEL,
    violation_thresh: f64,
) -> bool {
    let evs = EventOrSynthetic::get_all_syn_evs(linked_ocel, from_et);
    let ev_count = evs.len();
    let min_s = (ev_count as f64 * (1.0 - violation_thresh)).ceil() as u32;
    let min_v = (ev_count as f64 * violation_thresh).floor() as u32 + 1;
    // Non-Atomic:
    // for ev in evs {
    //     let violated = get_for_ev_perf(&ev, label, to_et, arc_type, counts, linked_ocel);
    //     if violated {
    //         min_v -= 1;
    //         if min_v == 0 {
    //             return false;
    //         }
    //     } else {
    //         min_s -= 1;
    //         if min_s == 0 {
    //             return true;
    //         }
    //     }
    // }
    // if min_s <= 0 {
    //     return true;
    // }
    // if min_v <= 0 {
    //     return false;
    // }

    // // Atomic:
    let min_v_atomic = AtomicU32::new(0);
    let min_s_atomic = AtomicU32::new(0);
    evs.into_par_iter()
        .map(|ev| {
            let violated = get_for_ev_perf(&ev, label, to_et, arc_type, counts, linked_ocel);
            if violated {
                min_v_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                min_s_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        })
        .take_any_while(|_| {
            if min_s_atomic.load(std::sync::atomic::Ordering::Relaxed) >= min_s {
                return false;
            }
            if min_v_atomic.load(std::sync::atomic::Ordering::Relaxed) >= min_v {
                return false;
            }
            true
        })
        .for_each(|_| {});
    let min_s_atomic = min_s_atomic.into_inner();
    let min_v_atomic = min_v_atomic.into_inner();
    // println!("{} and {}",min_s_atomic,min_v_atomic);
    if min_s_atomic >= min_s {
        return true;
    }
    if min_v_atomic >= min_v {
        return false;
    }

    unreachable!()
}

/// Returns true if violated!
pub fn get_for_ev_perf(
    ev_index: &EventOrSynthetic,
    label: &OCDeclareArcLabel,
    to_et: &str,
    arc_type: &OCDeclareArcType,
    counts: &(Option<usize>, Option<usize>),
    linked_ocel: &SlimLinkedOCEL,
) -> bool {
    let syn_time = ev_index.get_timestamp(linked_ocel);
    label.get_bindings(ev_index, linked_ocel).any(|binding| {
        match arc_type {
            OCDeclareArcType::AS | OCDeclareArcType::EF | OCDeclareArcType::EP => {
                let target_ev_iterator = get_evs_with_objs_perf(&binding, linked_ocel, to_et)
                    .filter(|ev2| {
                        let ev2_time = ev2.get_timestamp(linked_ocel);
                        match arc_type {
                            OCDeclareArcType::EF => syn_time < ev2_time,
                            OCDeclareArcType::EP => syn_time > ev2_time,
                            OCDeclareArcType::AS => true,
                            _ => unreachable!("DF should not go here."),
                        }
                    });
                if counts.1.is_none() {
                    // Only take necessary
                    if counts.0.unwrap_or_default()
                        > target_ev_iterator
                            .take(counts.0.unwrap_or_default())
                            .count()
                    {
                        // Violated!
                        return true;
                    }
                } else if let Some(c) = counts.1 {
                    let count = target_ev_iterator.take(c + 1).count();
                    if c < count || count < counts.0.unwrap_or_default() {
                        // Violated
                        return true;
                    }
                }
                false
            }
            OCDeclareArcType::DF | OCDeclareArcType::DP => {
                let df_ev = get_df_or_dp_event_perf(
                    &binding,
                    linked_ocel,
                    &syn_time,
                    arc_type == &OCDeclareArcType::DF,
                );
                let count = if df_ev.is_some_and(|e| e.get_as_event_type(linked_ocel) == to_et) {
                    1
                } else {
                    0
                };
                if let Some(min_c) = counts.0 {
                    if count < min_c {
                        return true;
                    }
                }
                if let Some(max_c) = counts.1 {
                    if count > max_c {
                        return true;
                    }
                }
                false
            }
        }
    })
}

#[register_binding]
/// Returns the confidence conformance of an OC-DECLARE arc on the given OCEL
///
/// Returns a value from 0.0 (all source events violate this constraint) to 1.0 (all source events satisfy this constraint)
pub fn oc_declare_conformance(ocel: &SlimLinkedOCEL, arc: &OCDeclareArc) -> f64 {
    1.0 - get_for_all_evs_perf(
        arc.from.as_str(),
        arc.to.as_str(),
        &arc.label,
        &arc.arc_type,
        &arc.counts,
        ocel,
    )
}
