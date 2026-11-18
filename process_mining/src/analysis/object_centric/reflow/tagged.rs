//! The tagged OCEL: an OCEL 2.0 log whose event-to-object qualifiers say which participations
//! flow.
//!
//! # Encoding
//!
//! - A participation is *non-flow* when its qualifier starts with [`NON_FLOW_PREFIX`] (`!`),
//!   e.g., `!customer`. The rest of the qualifier is the recorded one.
//! - Every other participation flows.
//! - A participation added by expansion has the qualifier [`ADDED_QUALIFIER`] (`+`), or `!+` if it
//!   does not flow.
//!
//! Object-to-object qualifiers are never tagged. Qualifiers are free strings in OCEL 2.0, so a
//! tagged log imports and exports in every OCEL format, and a tool that does not know the
//! encoding reads it as an ordinary log. A recorded qualifier that already starts with `!`, or
//! is exactly `+`, is read as a tag.
//!
//! Three projections turn a tagged log back into plain logs:
//!
//! | projection | keeps | for |
//! |---|---|---|
//! | [`flow_projection`] | the flowing participations | discovery |
//! | [`full_projection`] | every participation, tags dropped | other analyses |
//! | [`untag`] | the recorded participations, tags dropped | the input log of the tagging |

use std::borrow::Cow;

use crate::core::event_data::object_centric::linked_ocel::{
    slim_linked_ocel::{EventIndex, ObjectIndex, QualifierIdx},
    LinkedOCELAccess, SlimLinkedOCEL,
};

/// Prefix of the event-to-object qualifier of a non-flow participation.
pub const NON_FLOW_PREFIX: char = '!';

/// Qualifier of a participation that expansion added and the log did not record.
pub const ADDED_QUALIFIER: &str = "+";

/// Whether a participation with this event-to-object qualifier flows.
pub fn qualifier_flows(qualifier: &str) -> bool {
    !qualifier.starts_with(NON_FLOW_PREFIX)
}

/// The qualifier without its tag: `!item` and `item` both give `item`.
pub fn untagged_qualifier(qualifier: &str) -> &str {
    qualifier.strip_prefix(NON_FLOW_PREFIX).unwrap_or(qualifier)
}

/// Whether the qualifier marks a participation added by expansion.
pub fn qualifier_is_added(qualifier: &str) -> bool {
    untagged_qualifier(qualifier) == ADDED_QUALIFIER
}

/// Whether the log has a non-flow or an added participation.
pub fn is_tagged(locel: &SlimLinkedOCEL) -> bool {
    let tagged: Vec<bool> = locel
        .qualifiers()
        .iter()
        .map(|q| !qualifier_flows(q) || qualifier_is_added(q))
        .collect();
    // Interned qualifiers outlive the relationships using them, so the table alone can only rule
    // tags out.
    if !tagged.contains(&true) {
        return false;
    }
    locel.get_all_evs().any(|e| {
        e.get_ev(locel)
            .relationships
            .iter()
            .any(|(q, _)| tagged[q.into_inner() as usize])
    })
}

/// Rewrite the tagged log's event-to-object qualifiers, interning every new qualifier once.
///
/// `qualifier` maps an untagged qualifier and its flags (`flows`, `added`) to the new qualifier,
/// or `None` to remove its tuples.
fn rewrite(
    locel: &SlimLinkedOCEL,
    mut qualifier: impl FnMut(&str, bool, bool) -> Option<String>,
) -> SlimLinkedOCEL {
    let mut out = locel.clone();
    let table: Vec<Option<QualifierIdx>> = locel
        .qualifiers()
        .iter()
        .map(|q| {
            let (flows, added) = (qualifier_flows(q), qualifier_is_added(q));
            qualifier(untagged_qualifier(q), flows, added).map(|q| out.intern_qualifier(&q))
        })
        .collect();
    out.rewrite_e2o(|_, _, q| table[q.into_inner() as usize]);
    out
}

/// The log with only the flowing participations, which is what discovery reads.
///
/// An untagged log is its own flow projection and is returned borrowed.
pub fn flow_projection(locel: &SlimLinkedOCEL) -> Cow<'_, SlimLinkedOCEL> {
    if !is_tagged(locel) {
        return Cow::Borrowed(locel);
    }
    Cow::Owned(rewrite(locel, |q, flows, _| flows.then(|| q.to_string())))
}

/// The log with every participation and the [`NON_FLOW_PREFIX`] dropped.
///
/// Added participations keep their [`ADDED_QUALIFIER`]. An untagged log is its own full
/// projection and is returned borrowed.
pub fn full_projection(locel: &SlimLinkedOCEL) -> Cow<'_, SlimLinkedOCEL> {
    if !is_tagged(locel) {
        return Cow::Borrowed(locel);
    }
    Cow::Owned(rewrite(locel, |q, _, _| Some(q.to_string())))
}

/// The log as recorded: added participations removed and the tags dropped.
///
/// For a tagging of `L`, this is `L` again, unless a qualifier of `L` starts with `!` or is `+`
/// and so reads as a tag. An untagged log is returned borrowed.
pub fn untag(locel: &SlimLinkedOCEL) -> Cow<'_, SlimLinkedOCEL> {
    if !is_tagged(locel) {
        return Cow::Borrowed(locel);
    }
    Cow::Owned(rewrite(locel, |q, _, added| {
        (!added).then(|| q.to_string())
    }))
}

/// Tag a log: the recorded participations for which `flows` holds keep their qualifier, the
/// others get the [`NON_FLOW_PREFIX`], and `added` is added as flowing participations.
///
/// `flows` gets the event's native type index and the object. Tags the log already carries are
/// replaced, and participations it marks as added are dropped first. With `flow_only`, the
/// non-flow participations are removed instead, which gives the flow projection of the tagging.
pub(crate) fn tag_log(
    locel: &SlimLinkedOCEL,
    mut flows: impl FnMut(usize, ObjectIndex) -> bool,
    added: &[(EventIndex, ObjectIndex)],
    flow_only: bool,
) -> SlimLinkedOCEL {
    let mut out = locel.clone();
    let table: Vec<(Option<QualifierIdx>, Option<QualifierIdx>)> = locel
        .qualifiers()
        .iter()
        .map(|q| {
            if qualifier_is_added(q) {
                return (None, None);
            }
            let base = untagged_qualifier(q);
            let flow = out.intern_qualifier(base);
            let non_flow =
                (!flow_only).then(|| out.intern_qualifier(&format!("{NON_FLOW_PREFIX}{base}")));
            (Some(flow), non_flow)
        })
        .collect();
    out.rewrite_e2o(|event, o, q| {
        let (flow_q, non_flow_q) = table[q.into_inner() as usize];
        if flows(event.event_type, o) {
            flow_q
        } else {
            non_flow_q
        }
    });
    for (e, o) in added {
        out.add_e2o(*e, *o, ADDED_QUALIFIER.to_string());
    }
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::core::chrono::DateTime;

    /// Two orders, each with one item, placed and then paid.
    pub(crate) fn toy() -> SlimLinkedOCEL {
        let mut ocel = SlimLinkedOCEL::new();
        ocel.add_object_type("items", Vec::new());
        ocel.add_object_type("orders", Vec::new());
        ocel.add_event_type("place", Vec::new());
        ocel.add_event_type("pay", Vec::new());
        let o1 = ocel.add_object("orders", Some("o1".into()), Vec::new(), Vec::new());
        let o2 = ocel.add_object("orders", Some("o2".into()), Vec::new(), Vec::new());
        let (o1, o2) = (o1.unwrap(), o2.unwrap());
        let i1 = ocel.add_object(
            "items",
            Some("i1".into()),
            Vec::new(),
            vec![("of".into(), o1)],
        );
        let i2 = ocel.add_object(
            "items",
            Some("i2".into()),
            Vec::new(),
            vec![("of".into(), o2)],
        );
        let (i1, i2) = (i1.unwrap(), i2.unwrap());
        let t = |ms: i64| DateTime::from_timestamp_millis(ms).unwrap().fixed_offset();
        for (n, (item, order)) in [(i1, o1), (i2, o2)].into_iter().enumerate() {
            for (k, act) in ["place", "pay"].into_iter().enumerate() {
                ocel.add_event(
                    act,
                    t(1_600_000_000_000 + (n * 2 + k) as i64 * 1000),
                    Some(format!("{act}-{n}")),
                    Vec::new(),
                    vec![("item".into(), item), ("order".into(), order)],
                );
            }
        }
        ocel
    }

    pub(crate) fn e2o(locel: &SlimLinkedOCEL) -> Vec<(String, String, String)> {
        let mut out: Vec<(String, String, String)> = locel
            .get_all_evs()
            .flat_map(|e| {
                locel
                    .get_e2o(e)
                    .map(|(q, o)| {
                        (
                            locel.get_ev_id(e).to_string(),
                            locel.get_ob_id(o).to_string(),
                            q.to_string(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        out.sort();
        out
    }

    fn orders_do_not_flow(locel: &SlimLinkedOCEL) -> SlimLinkedOCEL {
        let orders = <SlimLinkedOCEL as LinkedOCELAccess>::get_ob_types(locel)
            .position(|t| t == "orders")
            .unwrap();
        tag_log(
            locel,
            |_, o| o.get_ob(locel).object_type != orders,
            &[],
            false,
        )
    }

    #[test]
    fn tagging_changes_only_qualifiers_and_untag_returns_the_input() {
        let ocel = toy();
        let tagged = orders_do_not_flow(&ocel);
        assert!(is_tagged(&tagged) && !is_tagged(&ocel));
        let qualifiers: Vec<String> = e2o(&tagged).into_iter().map(|(_, _, q)| q).collect();
        assert_eq!(qualifiers.iter().filter(|q| *q == "!order").count(), 4);
        assert_eq!(qualifiers.iter().filter(|q| *q == "item").count(), 4);
        assert_eq!(e2o(&untag(&tagged)), e2o(&ocel));
        assert_eq!(e2o(&full_projection(&tagged)), e2o(&ocel));
        assert!(matches!(untag(&ocel), Cow::Borrowed(_)));
    }

    #[test]
    fn the_flow_projection_drops_non_flow_tuples_and_is_untagged() {
        let ocel = toy();
        let flow = flow_projection(&orders_do_not_flow(&ocel)).into_owned();
        assert_eq!(e2o(&flow).len(), 4);
        assert!(e2o(&flow).iter().all(|(_, o, _)| o.starts_with('i')));
        // The `!order` qualifier is still interned but no tuple uses it.
        assert!(flow.qualifiers().iter().any(|q| q == "!order"));
        assert!(!is_tagged(&flow));
        assert!(matches!(flow_projection(&flow), Cow::Borrowed(_)));
        assert_eq!(flow.get_all_evs().count(), 4);
        let o2o = |l: &SlimLinkedOCEL| l.get_all_obs().map(|o| l.get_o2o(o).count()).sum::<usize>();
        assert_eq!(o2o(&flow), o2o(&ocel));
    }

    #[test]
    fn retagging_replaces_tags_and_added_participations() {
        let ocel = toy();
        let place = ocel.get_evs_of_type("place").next().copied().unwrap();
        let o2 = ocel.get_ob_by_id("o2").unwrap();
        let mut tagged = orders_do_not_flow(&ocel);
        tagged.add_e2o(place, o2, ADDED_QUALIFIER.to_string());
        assert_eq!(e2o(&untag(&tagged)), e2o(&ocel));
        let full = full_projection(&tagged);
        assert!(e2o(&full).iter().any(|(_, o, q)| o == "o2" && q == "+"));

        let all_flow = tag_log(&tagged, |_, _| true, &[], false);
        assert_eq!(e2o(&all_flow), e2o(&ocel));
        let marked = tag_log(&tagged, |_, _| false, &[(place, o2)], false);
        assert!(e2o(&marked).iter().any(|(_, o, q)| o == "o2" && q == "+"));
        assert_eq!(e2o(&untag(&marked)), e2o(&ocel));
        assert!(e2o(&marked)
            .iter()
            .all(|(_, _, q)| q == "+" || !qualifier_flows(q)));
    }

    /// The tags are plain qualifier text, so every OCEL format keeps them.
    #[test]
    fn tags_survive_every_ocel_format() {
        use crate::core::io::{Exportable, Importable};
        let ocel = toy();
        let place = ocel.get_evs_of_type("place").next().copied().unwrap();
        let o2 = ocel.get_ob_by_id("o2").unwrap();
        let tagged = tag_log(
            &orders_do_not_flow(&ocel),
            |_, _| true,
            &[(place, o2)],
            false,
        );
        let tagged = tag_log(&tagged, |_, o| o != o2, &[(place, o2)], false);
        let expected = e2o(&tagged);
        assert!(expected.iter().any(|(_, _, q)| q == "!order"));
        assert!(expected
            .iter()
            .any(|(_, o, q)| o == "o2" && q == ADDED_QUALIFIER));
        let dir = tempfile::tempdir().unwrap();
        for format in <SlimLinkedOCEL as Exportable>::known_export_formats() {
            let path = dir.path().join(format!("log.{}", format.extension));
            tagged.export_to_path(&path).unwrap();
            let back = SlimLinkedOCEL::import_from_path(&path).unwrap();
            assert_eq!(e2o(&back), expected, "{}", format.extension);
        }
    }

    #[test]
    fn qualifier_helpers() {
        assert!(qualifier_flows("item") && qualifier_flows("+") && !qualifier_flows("!item"));
        assert_eq!(untagged_qualifier("!item"), "item");
        assert!(qualifier_is_added("+") && qualifier_is_added("!+") && !qualifier_is_added("+x"));
    }
}
