//! Checking OC-DECLARE constraints against object-centric event data
use std::sync::atomic::AtomicU32;

use rustc_hash::FxHashSet;

use crate::core::{
    event_data::object_centric::linked_ocel::{
        e2o_rev_type_index::{E2ORevByTypeIndex, E2ORevTypeView},
        slim_linked_ocel::ObjectIndex,
        LinkedOCELAccess, SlimLinkedOCEL,
    },
    process_models::oc_declare::{
        EventOrSynthetic, OCDeclareArc, OCDeclareArcLabel, OCDeclareArcType, SetFilter,
    },
};

use chrono::{DateTime, FixedOffset};
use macros_process_mining::register_binding;
use rayon::prelude::*;

/// Get all events of the given event type satisfying the filters
///
/// `view` is `etype` resolved against an [`E2ORevByTypeIndex`]; `None` falls back to scanning.
pub(crate) fn target_events_for_binding<'a>(
    objs: &'a [SetFilter<&ObjectIndex>],
    linked_ocel: &'a SlimLinkedOCEL,
    etype: &'a str,
    view: Option<E2ORevTypeView<'a>>,
) -> impl Iterator<Item = EventOrSynthetic> + use<'a> {
    let for_ob = move |ob: ObjectIndex| -> Box<dyn Iterator<Item = EventOrSynthetic> + 'a> {
        match view {
            Some(view) => Box::new(view.events_of(ob).map(EventOrSynthetic::Event)),
            None => EventOrSynthetic::get_all_of_et_for_ob(linked_ocel, etype, ob),
        }
    };
    let initial: Box<dyn Iterator<Item = EventOrSynthetic>> = if objs.is_empty() {
        Box::new(EventOrSynthetic::get_all_syn_evs(linked_ocel, etype).into_iter())
    } else {
        match &objs[0] {
            // ANY is a union: an event referencing several of the objects still counts once.
            SetFilter::Any(items) if items.len() > 1 => {
                let mut seen: FxHashSet<EventOrSynthetic> = FxHashSet::default();
                Box::new(
                    items
                        .iter()
                        .flat_map(move |o| for_ob(**o))
                        .filter(move |e| seen.insert(*e)),
                )
            }
            SetFilter::Any(items) => Box::new(items.iter().flat_map(move |o| for_ob(**o))),
            SetFilter::All(items) => {
                if items.is_empty() {
                    // `SetFilter::check` is vacuously true for an empty `All`, i.e. it imposes
                    // no constraint, so every event of the type is a candidate, exactly as when
                    // `objs` itself is empty.
                    Box::new(EventOrSynthetic::get_all_syn_evs(linked_ocel, etype).into_iter())
                } else {
                    Box::new(for_ob(*items[0]).filter(|e| {
                        items
                            .iter()
                            .skip(1)
                            .all(|o| e.get_e2o_set(linked_ocel).contains(o))
                    }))
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

fn directly_adjacent_event<'a>(
    objs: &'a [SetFilter<&'a ObjectIndex>],
    linked_ocel: &'a SlimLinkedOCEL,
    // reference_mock_event_index: &'a EventIndex,
    reference_time: &'a DateTime<FixedOffset>,
    // reference_event: &'a OCELEvent,
    following: bool,
) -> Option<EventOrSynthetic> {
    let in_direction = move |e: &EventOrSynthetic| {
        let e_time = e.get_timestamp(linked_ocel);
        if following {
            e_time > *reference_time
        } else {
            e_time < *reference_time
        }
    };
    let initial: Box<dyn Iterator<Item = EventOrSynthetic>> = if objs.is_empty() {
        // If no requirements are specified, consider all events
        // TODO: Maybe also consider synthetic events here?
        // But in general, this is not very relevant as there are usually some object requirements
        Box::new(
            linked_ocel
                .get_all_evs()
                .map(EventOrSynthetic::Event)
                .filter(in_direction),
        )
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
                    // `SetFilter::check` is vacuously true for an empty `All`: no constraint,
                    // so every event is a candidate, as when `objs` itself is empty.
                    Box::new(
                        linked_ocel
                            .get_all_evs()
                            .map(EventOrSynthetic::Event)
                            .filter(in_direction),
                    )
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
pub(crate) fn violation_fraction(
    from_et: &str,
    to_et: &str,
    label: &OCDeclareArcLabel,
    arc_type: &OCDeclareArcType,
    counts: &(Option<usize>, Option<usize>),
    linked_ocel: &SlimLinkedOCEL,
    index: Option<&E2ORevByTypeIndex>,
) -> f64 {
    let view = index.and_then(|i| i.for_ev_type(to_et));
    let evs = EventOrSynthetic::get_all_syn_evs(linked_ocel, from_et);
    let ev_count = evs.len();
    let violated_evs_count = evs
        // .into_par_iter()
        .into_iter()
        .filter(|ev| event_violates(ev, label, to_et, arc_type, counts, linked_ocel, view))
        .count();
    violated_evs_count as f64 / ev_count as f64
}

/// Checks whether the number of events violating this constraint arc is below (<=) the given noise threshold
///
/// Returns false, if the fraction of events violating the constraint is above the noise threshold.
#[allow(clippy::too_many_arguments)]
pub(crate) fn satisfies_threshold(
    from_et: &str,
    to_et: &str,
    label: &OCDeclareArcLabel,
    arc_type: &OCDeclareArcType,
    counts: &(Option<usize>, Option<usize>),
    linked_ocel: &SlimLinkedOCEL,
    violation_thresh: f64,
    index: Option<&E2ORevByTypeIndex>,
) -> bool {
    let view = index.and_then(|i| i.for_ev_type(to_et));
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
            let violated = event_violates(&ev, label, to_et, arc_type, counts, linked_ocel, view);
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
#[allow(clippy::too_many_arguments)]
pub(crate) fn event_violates(
    ev_index: &EventOrSynthetic,
    label: &OCDeclareArcLabel,
    to_et: &str,
    arc_type: &OCDeclareArcType,
    counts: &(Option<usize>, Option<usize>),
    linked_ocel: &SlimLinkedOCEL,
    view: Option<E2ORevTypeView<'_>>,
) -> bool {
    let syn_time = ev_index.get_timestamp(linked_ocel);
    label.get_bindings(ev_index, linked_ocel).any(|binding| {
        match arc_type {
            OCDeclareArcType::AS | OCDeclareArcType::EF | OCDeclareArcType::EP => {
                let target_ev_iterator =
                    target_events_for_binding(&binding, linked_ocel, to_et, view).filter(|ev2| {
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
                let df_ev = directly_adjacent_event(
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
    1.0 - violation_fraction(
        arc.from.as_str(),
        arc.to.as_str(),
        &arc.label,
        &arc.arc_type,
        &arc.counts,
        ocel,
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::{
        appendable::AppendableOCEL, OCELRelationship, OCELType,
    };
    use crate::core::process_models::oc_declare::ObjectTypeAssociation;
    use chrono::DateTime;

    fn empty_type(name: &str) -> OCELType {
        OCELType {
            name: name.into(),
            attributes: Vec::new(),
        }
    }

    /// Small OCEL where EACH/ANY/ALL involvements all have something to check
    fn sample_locel() -> SlimLinkedOCEL {
        let mut s = SlimLinkedOCEL::new();
        for t in ["place", "pay", "ship"] {
            s.declare_event_type(empty_type(t)).unwrap();
        }
        for t in ["order", "item"] {
            s.declare_object_type(empty_type(t)).unwrap();
        }
        for (id, ot) in [
            ("o1", "order"),
            ("o2", "order"),
            ("i1", "item"),
            ("i2", "item"),
        ] {
            s.append_object(id.into(), ot, Vec::new(), Vec::new())
                .unwrap();
        }
        let evs = [
            ("e0", "place", vec!["o1", "i1"]),
            ("e1", "place", vec!["o2", "i2"]),
            ("e2", "pay", vec!["o1"]),
            ("e3", "ship", vec!["o1", "i1"]),
            ("e4", "pay", vec!["o2"]),
            ("e5", "place", vec!["o1", "i2"]),
            ("e6", "ship", vec!["o2", "i2"]),
        ];
        for (i, (id, et, obs)) in evs.iter().enumerate() {
            let time =
                DateTime::parse_from_rfc3339(&format!("2024-01-0{}T00:00:00Z", i + 1)).unwrap();
            s.append_event(
                (*id).into(),
                et,
                time,
                Vec::new(),
                obs.iter()
                    .map(|o| OCELRelationship {
                        object_id: (*o).into(),
                        qualifier: "q".into(),
                    })
                    .collect(),
            )
            .unwrap();
        }
        s.finalize().unwrap();
        s
    }

    /// Checking with and without the index must agree, across arc types/labels/counts
    #[test]
    fn constraint_checks_agree_with_and_without_index() {
        let locel = sample_locel();
        let index = E2ORevByTypeIndex::build(&locel);
        let assoc = ObjectTypeAssociation::new_simple("order");
        let item = ObjectTypeAssociation::new_simple("item");
        let labels = [
            OCDeclareArcLabel {
                each: vec![assoc.clone()],
                any: vec![],
                all: vec![],
            },
            OCDeclareArcLabel {
                each: vec![],
                any: vec![assoc.clone()],
                all: vec![],
            },
            OCDeclareArcLabel {
                each: vec![],
                any: vec![],
                all: vec![assoc.clone()],
            },
            OCDeclareArcLabel {
                each: vec![item],
                any: vec![assoc],
                all: vec![],
            },
            OCDeclareArcLabel {
                each: vec![],
                any: vec![],
                all: vec![],
            },
        ];
        let arc_types = [
            OCDeclareArcType::AS,
            OCDeclareArcType::EF,
            OCDeclareArcType::EP,
            OCDeclareArcType::DF,
            OCDeclareArcType::DP,
        ];
        let counts = [
            (Some(1), None),
            (Some(1), Some(1)),
            (Some(0), Some(2)),
            (Some(2), None),
        ];
        let mut compared = 0;
        for from in locel.get_ev_types().map(str::to_string).collect::<Vec<_>>() {
            for to in locel.get_ev_types().map(str::to_string).collect::<Vec<_>>() {
                for label in &labels {
                    for at in &arc_types {
                        for c in &counts {
                            let scanned =
                                violation_fraction(&from, &to, label, at, c, &locel, None);
                            let indexed =
                                violation_fraction(&from, &to, label, at, c, &locel, Some(&index));
                            assert_eq!(
                                scanned, indexed,
                                "{from} -> {to}, {at:?}, {label:?}, {c:?}"
                            );
                            for thresh in [0.0, 0.2, 1.0] {
                                let p = satisfies_threshold(
                                    &from, &to, label, at, c, &locel, thresh, None,
                                );
                                let i = satisfies_threshold(
                                    &from,
                                    &to,
                                    label,
                                    at,
                                    c,
                                    &locel,
                                    thresh,
                                    Some(&index),
                                );
                                assert_eq!(p, i, "threshold {thresh}: {from} -> {to}, {at:?}");
                            }
                            compared += 1;
                        }
                    }
                }
            }
        }
        assert!(
            compared > 500,
            "expected at least 500 comparison instead of just {compared}; something is wrong with the input SlimLinked OCEL."
        );
    }

    // `SetFilter::check` is vacuously true for `All(&[])`, since an event trivially references
    // "all of" an empty set, so the candidate iterator must not special-case it to empty.
    #[test]
    fn all_of_empty_items_matches_every_event_of_the_type_like_no_objs_at_all() {
        let locel = sample_locel();
        let no_objs: Vec<SetFilter<&ObjectIndex>> = Vec::new();
        let empty_all: Vec<SetFilter<&ObjectIndex>> = vec![SetFilter::All(Vec::new())];

        let via_no_objs: FxHashSet<_> =
            target_events_for_binding(&no_objs, &locel, "place", None).collect();
        let via_empty_all: FxHashSet<_> =
            target_events_for_binding(&empty_all, &locel, "place", None).collect();

        assert_eq!(via_empty_all, via_no_objs);
        assert_eq!(via_empty_all.len(), locel.get_evs_of_type("place").count());
    }
}
