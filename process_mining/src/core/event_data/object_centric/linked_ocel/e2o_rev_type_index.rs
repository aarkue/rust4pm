//! Reverse-E2O lookups grouped by event type, as a standalone index
//!
//! Kept out of [`SlimLinkedOCEL`], so it's built only when needed.

use std::collections::HashMap;

use rayon::prelude::*;

use super::{
    slim_linked_ocel::{EventIndex, ObjectIndex},
    LinkedOCELAccess, SlimLinkedOCEL,
};

/// Efficiently allows to retrieve "which events of type `T` reference object `o`" using binary search
#[derive(Debug, Clone, Default)]
pub struct E2ORevByTypeIndex {
    /// Per event type, its `(object, event)` pairs sorted ascending
    per_type: Vec<Vec<(ObjectIndex, EventIndex)>>,
    /// Event type name -> position in `per_type`.
    type_pos: HashMap<String, usize>,
}

impl E2ORevByTypeIndex {
    /// Index the reverse E2O relations of `locel`, once per event type
    pub fn build(locel: &SlimLinkedOCEL) -> Self {
        let type_pos: HashMap<String, usize> = locel
            .get_ev_types()
            .enumerate()
            .map(|(i, t)| (t.to_string(), i))
            .collect();
        let mut per_type: Vec<Vec<(ObjectIndex, EventIndex)>> = vec![Vec::new(); type_pos.len()];
        for (name, pos) in &type_pos {
            let bucket = &mut per_type[*pos];
            for ev in locel.get_evs_of_type(name) {
                for ob in ev.get_e2o(locel) {
                    bucket.push((*ob, *ev));
                }
            }
        }
        per_type.par_iter_mut().for_each(|b| {
            b.sort_unstable();
            // `get_e2o` yields one entry per relationship, so multiple qualifiers between the same
            // pair would otherwise be counted as multiple events.
            b.dedup();
        });
        Self { per_type, type_pos }
    }

    /// Resolve one event type for repeated lookups
    ///
    /// `None` for unknown types, including the synthetic `<init>` / `<exit>` ones.
    pub fn for_ev_type(&self, ev_type: &str) -> Option<E2ORevTypeView<'_>> {
        self.type_pos.get(ev_type).map(|pos| E2ORevTypeView {
            runs: &self.per_type[*pos],
        })
    }
}

/// One event type of an [`E2ORevByTypeIndex`], cheap to copy
#[derive(Debug, Clone, Copy)]
pub struct E2ORevTypeView<'a> {
    runs: &'a [(ObjectIndex, EventIndex)],
}

impl<'a> E2ORevTypeView<'a> {
    /// Events of this type referencing `ob`, ascending
    pub fn events_of(&self, ob: ObjectIndex) -> impl Iterator<Item = EventIndex> + use<'a> {
        let lo = self.runs.partition_point(|(o, _)| *o < ob);
        let hi = self.runs.partition_point(|(o, _)| *o <= ob);
        self.runs[lo..hi].iter().map(|(_, ev)| *ev)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::{
        appendable::AppendableOCEL, OCELRelationship, OCELType,
    };
    use chrono::DateTime;

    fn empty_type(name: &str) -> OCELType {
        OCELType {
            name: name.into(),
            attributes: Vec::new(),
        }
    }

    fn rel(object_id: &str, qualifier: &str) -> OCELRelationship {
        OCELRelationship {
            object_id: object_id.into(),
            qualifier: qualifier.into(),
        }
    }

    fn locel_with_interleaved_types() -> SlimLinkedOCEL {
        let mut s = SlimLinkedOCEL::new();
        for t in ["place", "pay", "ship"] {
            s.declare_event_type(empty_type(t)).unwrap();
        }
        s.declare_object_type(empty_type("order")).unwrap();
        for id in ["o1", "o2"] {
            s.append_object(id.into(), "order", Vec::new(), Vec::new())
                .unwrap();
        }
        for (i, (et, obs)) in [
            ("place", vec!["o1", "o2"]),
            ("pay", vec!["o1"]),
            ("ship", vec!["o2"]),
            ("place", vec!["o1"]),
            ("pay", vec!["o2"]),
        ]
        .iter()
        .enumerate()
        {
            s.append_event(
                format!("e{i}"),
                et,
                DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap(),
                Vec::new(),
                obs.iter().map(|o| rel(o, "q")).collect(),
            )
            .unwrap();
        }
        s.finalize().unwrap();
        s
    }

    #[test]
    fn matches_unindexed_reverse_lookup() {
        let locel = locel_with_interleaved_types();
        let index = E2ORevByTypeIndex::build(&locel);
        let ev_types: Vec<String> = locel.get_ev_types().map(str::to_string).collect();
        for ob in locel.get_all_obs() {
            for et in &ev_types {
                let mut indexed: Vec<EventIndex> = index
                    .for_ev_type(et)
                    .map(|view| view.events_of(ob).collect())
                    .unwrap_or_default();
                let mut expected: Vec<EventIndex> =
                    ob.get_e2o_rev_of_evtype(&locel, et).copied().collect();
                indexed.sort_unstable();
                expected.sort_unstable();
                assert_eq!(indexed, expected, "object {ob:?}, event type {et}");
            }
        }
    }

    #[test]
    fn multi_qualifier_pair_appears_once() {
        let mut s = SlimLinkedOCEL::new();
        s.declare_event_type(empty_type("place")).unwrap();
        s.declare_object_type(empty_type("order")).unwrap();
        s.append_object("o1".into(), "order", Vec::new(), Vec::new())
            .unwrap();
        s.append_event(
            "e0".into(),
            "place",
            DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap(),
            Vec::new(),
            vec![rel("o1", "primary"), rel("o1", "secondary")],
        )
        .unwrap();
        s.finalize().unwrap();

        let ob = s.get_all_obs().next().unwrap();
        let index = E2ORevByTypeIndex::build(&s);
        let via_index: Vec<EventIndex> =
            index.for_ev_type("place").unwrap().events_of(ob).collect();
        let via_locel: Vec<EventIndex> = ob.get_e2o_rev_of_evtype(&s, "place").copied().collect();
        assert_eq!(via_index, via_locel);
        assert_eq!(via_index.len(), 1);
    }

    #[test]
    fn unknown_event_type_yields_nothing() {
        let index = E2ORevByTypeIndex::build(&locel_with_interleaved_types());
        assert!(index.for_ev_type("<init>:order").is_none());
        assert!(index.for_ev_type("nope").is_none());
    }
}
