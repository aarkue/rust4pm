//! The log as Reflow reads it: activities and object types by sorted index, and per event the
//! distinct objects it records, without the participations a tagged log marks as added.

use std::collections::{HashMap, HashSet};

use crate::core::event_data::object_centric::{
    linked_ocel::{
        slim_linked_ocel::{EventIndex, ObjectIndex},
        LinkedOCELAccess, SlimLinkedOCEL,
    },
    readable::ReadableOCEL,
};

use super::tagged::{qualifier_flows, qualifier_is_added};

/// Index of an activity in [`LogView::activities`].
pub(crate) type Act = usize;
/// Index of an object type in [`LogView::types`].
pub(crate) type Ot = usize;
/// An activity and an object type.
pub(crate) type Cell = (Act, Ot);

/// The recorded participations of a log, indexed for Reflow.
///
/// Activities and object types are sorted by name, so every tie broken by index is broken the
/// same way whatever order the importer used.
pub(crate) struct LogView {
    pub(crate) activities: Vec<String>,
    pub(crate) types: Vec<String>,
    pub(crate) ev_act: Vec<Act>,
    pub(crate) ev_time: Vec<i64>,
    ev_off: Vec<usize>,
    /// The distinct recorded objects of each event, sorted by type and then by object.
    ev_obj: Vec<u32>,
    ob_off: Vec<usize>,
    /// The events recording each object, in index order.
    ob_ev: Vec<u32>,
    pub(crate) ob_type: Vec<Ot>,
    /// Position of each object among the objects of its type.
    pub(crate) ob_local: Vec<u32>,
    pub(crate) objects_of_type: Vec<Vec<u32>>,
    pub(crate) evs_of_act: Vec<Vec<u32>>,
    /// Participations marked as added, `(event, object, flows)`, sorted.
    pub(crate) added: Vec<(u32, u32, bool)>,
    /// Recorded cells with a flowing participation.
    pub(crate) flowing: HashSet<Cell>,
}

impl LogView {
    pub(crate) fn new(locel: &SlimLinkedOCEL) -> Self {
        let mut activities: Vec<String> =
            locel.event_types().iter().map(|t| t.name.clone()).collect();
        activities.sort();
        activities.dedup();
        let mut types: Vec<String> = locel
            .object_types()
            .iter()
            .map(|t| t.name.clone())
            .collect();
        types.sort();
        types.dedup();
        let act_ix: HashMap<&str, Act> = activities
            .iter()
            .enumerate()
            .map(|(i, a)| (a.as_str(), i))
            .collect();
        let type_ix: HashMap<&str, Ot> = types
            .iter()
            .enumerate()
            .map(|(i, t)| (t.as_str(), i))
            .collect();
        let act_of_native: Vec<Act> = locel
            .event_types()
            .iter()
            .map(|t| act_ix[t.name.as_str()])
            .collect();
        let type_of_native: Vec<Ot> = locel
            .object_types()
            .iter()
            .map(|t| type_ix[t.name.as_str()])
            .collect();

        let n_obs = locel.get_all_obs().count();
        let mut ob_type = Vec::with_capacity(n_obs);
        let mut ob_local = Vec::with_capacity(n_obs);
        let mut objects_of_type: Vec<Vec<u32>> = vec![Vec::new(); types.len()];
        for o in locel.get_all_obs() {
            let t = type_of_native[o.get_ob(locel).object_type];
            ob_type.push(t);
            ob_local.push(objects_of_type[t].len() as u32);
            objects_of_type[t].push(o.into_inner());
        }

        let qualifier_flags: Vec<(bool, bool)> = locel
            .qualifiers()
            .iter()
            .map(|q| (qualifier_flows(q), qualifier_is_added(q)))
            .collect();
        let n_evs = locel.get_all_evs().count();
        let mut ev_act = Vec::with_capacity(n_evs);
        let mut ev_time = Vec::with_capacity(n_evs);
        let mut ev_off = Vec::with_capacity(n_evs + 1);
        let mut ev_obj: Vec<u32> = Vec::new();
        let mut evs_of_act: Vec<Vec<u32>> = vec![Vec::new(); activities.len()];
        let mut added = Vec::new();
        let mut flowing: HashSet<Cell> = HashSet::new();
        ev_off.push(0);
        for e in locel.get_all_evs() {
            let ev = e.get_ev(locel);
            let a = act_of_native[ev.event_type];
            ev_act.push(a);
            ev_time.push(ev.time.timestamp_millis());
            evs_of_act[a].push(e.into_inner());
            let start = ev_obj.len();
            for (q, o) in &ev.relationships {
                let (flows, is_added) = qualifier_flags[q.into_inner() as usize];
                if is_added {
                    added.push((e.into_inner(), o.into_inner(), flows));
                    continue;
                }
                if flows {
                    flowing.insert((a, ob_type[o.into_inner() as usize]));
                }
                ev_obj.push(o.into_inner());
            }
            let objs = &mut ev_obj[start..];
            objs.sort_unstable_by_key(|o| (ob_type[*o as usize], *o));
            let mut len = 0;
            for i in 0..objs.len() {
                if i == 0 || objs[i] != objs[len - 1] {
                    objs[len] = objs[i];
                    len += 1;
                }
            }
            ev_obj.truncate(start + len);
            ev_off.push(ev_obj.len());
        }
        added.sort_unstable();
        let mut merged: Vec<(u32, u32, bool)> = Vec::with_capacity(added.len());
        for (e, o, flows) in added {
            match merged.last_mut() {
                Some(last) if (last.0, last.1) == (e, o) => last.2 |= flows,
                _ => merged.push((e, o, flows)),
            }
        }
        let added = merged;

        let mut ob_off = vec![0usize; n_obs + 1];
        for o in &ev_obj {
            ob_off[*o as usize + 1] += 1;
        }
        for i in 0..n_obs {
            ob_off[i + 1] += ob_off[i];
        }
        let mut fill = ob_off.clone();
        let mut ob_ev = vec![0u32; ev_obj.len()];
        for e in 0..n_evs {
            for o in &ev_obj[ev_off[e]..ev_off[e + 1]] {
                ob_ev[fill[*o as usize]] = e as u32;
                fill[*o as usize] += 1;
            }
        }

        Self {
            activities,
            types,
            ev_act,
            ev_time,
            ev_off,
            ev_obj,
            ob_off,
            ob_ev,
            ob_type,
            ob_local,
            objects_of_type,
            evs_of_act,
            added,
            flowing,
        }
    }

    pub(crate) fn n_events(&self) -> usize {
        self.ev_act.len()
    }

    pub(crate) fn n_objects(&self) -> usize {
        self.ob_type.len()
    }

    /// The distinct recorded objects of an event, sorted by type and then by object.
    pub(crate) fn objects(&self, e: u32) -> &[u32] {
        &self.ev_obj[self.ev_off[e as usize]..self.ev_off[e as usize + 1]]
    }

    /// The events recording an object, in index order.
    pub(crate) fn events(&self, o: u32) -> &[u32] {
        &self.ob_ev[self.ob_off[o as usize]..self.ob_off[o as usize + 1]]
    }

    /// The recorded objects of an event, one sorted slice per type.
    pub(crate) fn objects_by_type(&self, e: u32) -> impl Iterator<Item = (Ot, &[u32])> + '_ {
        let objs = self.objects(e);
        let mut start = 0;
        std::iter::from_fn(move || {
            if start >= objs.len() {
                return None;
            }
            let t = self.ob_type[objs[start] as usize];
            let end = start + objs[start..].partition_point(|o| self.ob_type[*o as usize] == t);
            let slice = &objs[start..end];
            start = end;
            Some((t, slice))
        })
    }

    /// The first and last recorded timestamp of every object, `None` for an object without
    /// events.
    pub(crate) fn lifetimes(&self) -> Vec<Option<(i64, i64)>> {
        let mut out: Vec<Option<(i64, i64)>> = vec![None; self.n_objects()];
        for e in 0..self.n_events() {
            let ts = self.ev_time[e];
            for o in self.objects(e as u32) {
                let span = &mut out[*o as usize];
                *span = Some(match *span {
                    Some((lo, hi)) => (lo.min(ts), hi.max(ts)),
                    None => (ts, ts),
                });
            }
        }
        out
    }

    /// Objects of each type.
    pub(crate) fn objects_per_type(&self) -> Vec<usize> {
        self.objects_of_type.iter().map(Vec::len).collect()
    }
}

/// Every object's events in order, the recorded ones and those of `extra`.
///
/// Events are ordered by timestamp, then activity, then event id, so the order is a function of
/// the log and not of the import order.
pub(crate) struct Traces {
    off: Vec<usize>,
    /// `(timestamp, activity)` per event.
    items: Vec<(i64, Act)>,
}

impl Traces {
    /// The traces of the objects of `types` (all types if `None`), with the participations of
    /// `extra` added.
    pub(crate) fn build(
        view: &LogView,
        locel: &SlimLinkedOCEL,
        extra: &[(u32, u32)],
        only_type: Option<Ot>,
    ) -> Self {
        let keep = |o: u32| only_type.is_none_or(|t| view.ob_type[o as usize] == t);
        let mut extra_of: HashMap<u32, Vec<u32>> = HashMap::new();
        for (e, o) in extra.iter().filter(|(_, o)| keep(*o)) {
            extra_of.entry(*o).or_default().push(*e);
        }
        let mut off = Vec::with_capacity(view.n_objects() + 1);
        let mut items = Vec::new();
        off.push(0);
        let mut evs: Vec<u32> = Vec::new();
        for o in 0..view.n_objects() as u32 {
            if !keep(o) {
                off.push(items.len());
                continue;
            }
            evs.clear();
            evs.extend_from_slice(view.events(o));
            evs.extend(extra_of.get(&o).into_iter().flatten());
            evs.sort_unstable_by(|x, y| {
                let key = |e: u32| (view.ev_time[e as usize], view.ev_act[e as usize]);
                key(*x).cmp(&key(*y)).then_with(|| {
                    let id = |e: u32| locel.get_ev_id(EventIndex::from(e));
                    id(*x).cmp(id(*y))
                })
            });
            evs.dedup();
            items.extend(
                evs.iter()
                    .map(|e| (view.ev_time[*e as usize], view.ev_act[*e as usize])),
            );
            off.push(items.len());
        }
        Self { off, items }
    }

    /// The events of one object, in order.
    pub(crate) fn of(&self, o: u32) -> &[(i64, Act)] {
        &self.items[self.off[o as usize]..self.off[o as usize + 1]]
    }
}

/// Distinct activity sequences per object type, to count the directly-follows arcs a set of
/// flow cells induces.
pub(crate) struct Variants {
    /// `(object type, activities)`, one per distinct sequence, sorted.
    pub(crate) variants: Vec<(Ot, Vec<Act>)>,
}

impl Variants {
    pub(crate) fn build(view: &LogView, traces: &Traces) -> Self {
        let mut seen: std::collections::HashSet<(Ot, Vec<Act>)> = Default::default();
        for o in 0..view.n_objects() as u32 {
            let seq: Vec<Act> = traces.of(o).iter().map(|(_, a)| *a).collect();
            seen.insert((view.ob_type[o as usize], seq));
        }
        let mut variants: Vec<(Ot, Vec<Act>)> = seen.into_iter().collect();
        variants.sort();
        Self { variants }
    }

    /// The directly-follows arcs of the traces projected onto the cells for which `flows` holds.
    pub(crate) fn arcs(&self, flows: impl Fn(Cell) -> bool) -> usize {
        count_arcs(&self.variants, flows)
    }
}

/// The directly-follows arcs `(type, from, to)` of activity sequences projected onto the cells
/// for which `flows` holds.
pub(crate) fn count_arcs(variants: &[(Ot, Vec<Act>)], flows: impl Fn(Cell) -> bool) -> usize {
    let mut arcs: std::collections::HashSet<(Ot, Act, Act)> = Default::default();
    for (t, seq) in variants {
        let mut prev: Option<Act> = None;
        for a in seq.iter().filter(|a| flows((**a, *t))) {
            if let Some(p) = prev {
                arcs.insert((*t, p, *a));
            }
            prev = Some(*a);
        }
    }
    arcs.len()
}

/// The event and object of a participation, by id.
pub(crate) fn tuple_ids(locel: &SlimLinkedOCEL, e: u32, o: u32) -> (String, String) {
    (
        locel.get_ev_id(EventIndex::from(e)).to_string(),
        locel.get_ob_id(ObjectIndex::from(o)).to_string(),
    )
}
