use macros_process_mining::register_binding;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;

use crate::core::{
    event_data::object_centric::linked_ocel::{LinkedOCELAccess, SlimLinkedOCEL},
    process_models::case_centric::dfg::dfg_struct::DirectlyFollowsGraph,
};

///
/// An object-centric directly-follows graph containing a [`DirectlyFollowsGraph`] for each object
/// type involved.
///
#[derive(Debug, Serialize, Deserialize, Default, JsonSchema)]
pub struct OCDirectlyFollowsGraph<'a> {
    /// The DFG per object type
    pub object_type_to_dfg: HashMap<String, DirectlyFollowsGraph<'a>>,
}

impl<'a> OCDirectlyFollowsGraph<'a> {
    ///
    /// Create new [`OCDirectlyFollowsGraph`] with no object types and no [`DirectlyFollowsGraph`]s.
    ///
    pub fn new() -> Self {
        Self {
            object_type_to_dfg: HashMap::new(),
        }
    }

    ///
    /// Construct an [`OCDirectlyFollowsGraph`] from an OCEL
    ///
    pub fn create_from_ocel<O>(locel: &'a O) -> Self
    where
        O: LinkedOCELAccess<'a> + Sync,
        O::ObjectRepr: Sync,
        O::EventRepr: Sync,
    {
        discover_dfg_from_ocel(locel)
    }

    ///
    /// Serialize to JSON string.
    ///
    pub fn to_json(self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

/// Construct an [`OCDirectlyFollowsGraph`] from an OCEL
///
/// Each object is one case (all events involving it, in time order), and an event involving the
/// same object under several qualifiers counts once. The result is the DFG of [`flatten_ocel_on`]
/// for each object type.
///
/// [`flatten_ocel_on`]: crate::core::event_data::object_centric::utils::flatten::flatten_ocel_on
pub fn discover_dfg_from_ocel<'a, O>(ocel: &'a O) -> OCDirectlyFollowsGraph<'a>
where
    O: LinkedOCELAccess<'a> + Sync,
    O::ObjectRepr: Sync,
    O::EventRepr: Sync,
{
    let mut result = OCDirectlyFollowsGraph::new();
    for ob_type in ocel.get_ob_types() {
        let objects: Vec<&'a O::ObjectRepr> = ocel.get_obs_of_type(ob_type).collect();
        let counts = objects
            .par_iter()
            .fold(TypeCounts::default, |mut counts, ob| {
                counts.add_case(&object_trace(ocel, *ob));
                counts
            })
            .reduce(TypeCounts::default, TypeCounts::merge);
        result
            .object_type_to_dfg
            .insert(ob_type.to_string(), counts.into_dfg());
    }
    result
}

/// [`discover_dfg_from_ocel`] on a [`SlimLinkedOCEL`], which is what the binding takes.
#[register_binding(name = "discover_dfg_from_ocel")]
pub fn discover_dfg_from_slim_ocel(ocel: &SlimLinkedOCEL) -> OCDirectlyFollowsGraph<'_> {
    discover_dfg_from_ocel(ocel)
}

/// The activities of an object's events in time order, each event once.
fn object_trace<'a, O: LinkedOCELAccess<'a>>(ocel: &'a O, ob: &'a O::ObjectRepr) -> Vec<&'a str> {
    let mut events: Vec<&'a O::EventRepr> = ocel.get_e2o_rev_evs(ob).collect();
    events.sort_by_key(|ev| ocel.get_ev_time(*ev));
    events
        .into_iter()
        .map(|ev| ocel.get_ev_type_of(ev))
        .collect()
}

/// The counts of one object type's cases.
#[derive(Default)]
struct TypeCounts<'a> {
    activities: FxHashMap<&'a str, u32>,
    starts: FxHashMap<&'a str, u32>,
    ends: FxHashMap<&'a str, u32>,
    relations: FxHashMap<(&'a str, &'a str), u32>,
}

impl<'a> TypeCounts<'a> {
    fn add_case(&mut self, trace: &[&'a str]) {
        let (Some(&first), Some(&last)) = (trace.first(), trace.last()) else {
            return;
        };
        *self.starts.entry(first).or_default() += 1;
        *self.ends.entry(last).or_default() += 1;
        for &activity in trace {
            *self.activities.entry(activity).or_default() += 1;
        }
        for pair in trace.windows(2) {
            *self.relations.entry((pair[0], pair[1])).or_default() += 1;
        }
    }

    fn merge(mut self, other: Self) -> Self {
        fn add<K: std::hash::Hash + Eq>(into: &mut FxHashMap<K, u32>, from: FxHashMap<K, u32>) {
            for (k, v) in from {
                *into.entry(k).or_default() += v;
            }
        }
        add(&mut self.activities, other.activities);
        add(&mut self.starts, other.starts);
        add(&mut self.ends, other.ends);
        add(&mut self.relations, other.relations);
        self
    }

    fn into_dfg(self) -> DirectlyFollowsGraph<'a> {
        let mut dfg = DirectlyFollowsGraph::new();
        for (activity, count) in self.activities {
            dfg.add_activity(activity.to_string(), count);
        }
        for (activity, count) in self.starts {
            dfg.add_start_activity(activity.to_string(), count);
        }
        for (activity, count) in self.ends {
            dfg.add_end_activity(activity.to_string(), count);
        }
        for ((from, to), count) in self.relations {
            dfg.add_df_relation(Cow::Borrowed(from), Cow::Borrowed(to), count);
        }
        dfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::linked_ocel::IndexLinkedOCEL;
    use crate::test_utils::get_test_data_path;
    use crate::{Importable, OCEL};

    fn assert_same_as_flattened<'a, O>(locel: &'a O, name: &str)
    where
        O: LinkedOCELAccess<'a> + Sync,
        O::ObjectRepr: Sync,
        O::EventRepr: Sync,
    {
        use crate::core::event_data::object_centric::utils::flatten::flatten_ocel_on;
        let model = discover_dfg_from_ocel(locel);
        for ob_type in locel.get_ob_types() {
            let flat = DirectlyFollowsGraph::discover(&flatten_ocel_on(locel, ob_type));
            let dfg = &model.object_type_to_dfg[ob_type];
            assert_eq!(dfg.activities, flat.activities, "{name} {ob_type}");
            assert_eq!(
                dfg.start_activities, flat.start_activities,
                "{name} {ob_type}"
            );
            assert_eq!(dfg.end_activities, flat.end_activities, "{name} {ob_type}");
            assert_eq!(
                dfg.directly_follows_relations, flat.directly_follows_relations,
                "{name} {ob_type}"
            );
        }
    }

    /// The direct traversal gives the DFGs of the flattened logs.
    #[test]
    fn equals_the_dfgs_of_the_flattened_logs() {
        use crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;
        for name in ["pm4py-ocel20_example.jsonocel", "order-management.json"] {
            let ocel =
                OCEL::import_from_path(get_test_data_path().join("ocel").join(name)).unwrap();
            assert_same_as_flattened(&IndexLinkedOCEL::from(ocel.clone()), name);
            assert_same_as_flattened(&SlimLinkedOCEL::from_ocel(ocel), name);
        }
    }

    /// One case per object, and objects without events start and end no case.
    #[test]
    fn start_and_end_counts_are_per_object_of_the_type() {
        let path = get_test_data_path()
            .join("ocel")
            .join("pm4py-ocel20_example.jsonocel");
        let locel = IndexLinkedOCEL::from(OCEL::import_from_path(path).unwrap());
        let model = discover_dfg_from_ocel(&locel);
        for object_type in locel.get_ob_types() {
            let with_events = locel
                .get_obs_of_type(object_type)
                .filter(|ob| locel.get_e2o_rev(*ob).next().is_some())
                .count();
            let dfg = &model.object_type_to_dfg[object_type];
            let total = |counts: &HashMap<String, u32>| counts.values().sum::<u32>() as usize;
            assert_eq!(total(&dfg.start_activities), with_events, "{object_type}");
            assert_eq!(total(&dfg.end_activities), with_events, "{object_type}");
        }
    }
}
