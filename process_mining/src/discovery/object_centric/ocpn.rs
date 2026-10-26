//! Discovery of object-centric Petri nets.
//!
//! Implements the discovery procedure of van der Aalst and Berti, "Discovering Object-Centric
//! Petri Nets" (Fundamenta Informaticae 175, 2020): flatten the object-centric event log on each
//! object type, discover a Petri net per type, stitch those together on their shared activities,
//! and decide which arcs are variable.

use std::collections::{HashMap, HashSet};

use macros_process_mining::register_binding;
use rayon::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::event_data::object_centric::linked_ocel::{LinkedOCELAccess, SlimLinkedOCEL};
use crate::core::event_data::object_centric::utils::flatten::flatten_ocel_on;
use crate::core::process_models::object_centric::ocpn::json::ObjectCentricPetriNetJson;
use crate::core::process_models::object_centric::ocpn::ObjectCentricPetriNet;
use crate::discovery::case_centric::inductive_miner::{
    discover_inductive_tree_from_log, InductiveMiner,
};

/// Settings for object-centric Petri net discovery.
///
/// In JSON, fields left out take their default value.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct OcpnDiscoveryOptions {
    /// How the Petri net of each object type is discovered from its flattened log. The
    /// [DFG variant](InductiveMiner::Dfg) reads each log once, which suits large logs.
    pub miner: InductiveMiner,
    /// Object types to discover a net for. If this is `None`, every object type of the log is used.
    ///
    /// Each type's net and variable arcs are discovered independently, so leaving a type out does
    /// not change the nets of the others.
    pub object_types_to_use: Option<Vec<String>>,
    /// Activities to keep. If this is `None`, every activity is kept.
    ///
    /// The other activities' events are removed from each flattened log before mining, so the
    /// nets describe the log without them. An object left without events adds no trace.
    pub activities_to_use: Option<Vec<String>>,
    /// Fraction of an activity's executions that may involve other than exactly one object of a
    /// type while its arc to that type still counts as normal.
    ///
    /// The default `0.0` is the strict reading. A small tolerance keeps data-quality noise from
    /// making arcs variable.
    pub variable_arc_tolerance: f64,
}

impl OcpnDiscoveryOptions {
    /// Discovery with the given Inductive Miner, every object type and activity, and no tolerance
    /// for variable arcs.
    pub fn new(miner: impl Into<InductiveMiner>) -> Self {
        Self {
            miner: miner.into(),
            ..Self::default()
        }
    }

    /// Keeps only the given [activities](Self::activities_to_use).
    pub fn with_activities<S: Into<String>>(self, activities: impl IntoIterator<Item = S>) -> Self {
        Self {
            activities_to_use: Some(activities.into_iter().map(Into::into).collect()),
            ..self
        }
    }

    /// Whether `activity` is kept.
    pub fn uses_activity(&self, activity: &str) -> bool {
        self.activities_to_use
            .as_ref()
            .is_none_or(|activities| activities.iter().any(|a| a == activity))
    }

    /// Discovers only the given [object types](Self::object_types_to_use).
    pub fn with_object_types<S: Into<String>>(
        self,
        object_types: impl IntoIterator<Item = S>,
    ) -> Self {
        Self {
            object_types_to_use: Some(object_types.into_iter().map(Into::into).collect()),
            ..self
        }
    }

    /// Whether a net is discovered for `object_type`.
    pub fn uses_object_type(&self, object_type: &str) -> bool {
        self.object_types_to_use
            .as_ref()
            .is_none_or(|types| types.iter().any(|t| t == object_type))
    }

    /// Sets the [`variable_arc_tolerance`](Self::variable_arc_tolerance), clamped to `[0, 1]`.
    pub fn with_variable_arc_tolerance(self, tolerance: f64) -> Self {
        Self {
            variable_arc_tolerance: tolerance.clamp(0.0, 1.0),
            ..self
        }
    }
}

/// Discovers an [`ObjectCentricPetriNet`] from an object-centric event log.
///
/// Flattens the log on every selected object type (one case per object), mines a Petri net from
/// each in parallel with the Inductive Miner, and determines the
/// [variable arcs](discover_variable_arcs).
pub fn discover_ocpn_from<'a, O>(
    ocel: &'a O,
    options: OcpnDiscoveryOptions,
) -> ObjectCentricPetriNet
where
    O: LinkedOCELAccess<'a> + Sync,
    O::ObjectRepr: Eq + std::hash::Hash,
{
    let object_types: Vec<String> = ocel
        .get_ob_types()
        .filter(|object_type| options.uses_object_type(object_type))
        .map(str::to_string)
        .collect();

    let classifier = EventLogClassifier::default();
    let nets = object_types
        .par_iter()
        .map(|object_type| {
            let mut flattened = flatten_ocel_on(ocel, object_type);
            if options.activities_to_use.is_some() {
                for trace in &mut flattened.traces {
                    trace
                        .events
                        .retain(|e| options.uses_activity(&classifier.get_class_identity(e)));
                }
            }
            // Objects without events would add empty traces, which make the whole net optional.
            flattened.traces.retain(|trace| !trace.events.is_empty());

            let tree = discover_inductive_tree_from_log(&flattened, &classifier, options.miner);
            (object_type.clone(), tree.to_petri_net())
        })
        .collect();

    let mut variable_arcs = discover_variable_arcs(ocel, options.variable_arc_tolerance);
    variable_arcs.retain(|object_type, _| options.uses_object_type(object_type));
    for activities in variable_arcs.values_mut() {
        activities.retain(|activity| options.uses_activity(activity));
    }
    variable_arcs.retain(|_, activities| !activities.is_empty());

    ObjectCentricPetriNet {
        nets,
        variable_arcs,
    }
}

/// [`discover_ocpn_from`] on a [`SlimLinkedOCEL`], returning the flat JSON form.
#[register_binding]
pub fn discover_ocpn(
    locel: &SlimLinkedOCEL,
    #[bind(default)] options: OcpnDiscoveryOptions,
) -> ObjectCentricPetriNetJson {
    discover_ocpn_from(locel, options).to_json_form()
}

/// Determines, per object type, which activities have variable arcs.
///
/// A normal arc moves exactly one token, so an arc is variable as soon as one execution of the
/// activity involves several objects of the type, or none. Flattening hides both cases, so this
/// reads the object-centric log. `tolerance` allows a fraction of the executions to deviate.
///
/// Types without a variable arc are absent from the result, as are activities never relating to
/// the type.
pub fn discover_variable_arcs<'a, O>(
    ocel: &'a O,
    tolerance: f64,
) -> HashMap<String, HashSet<String>>
where
    O: LinkedOCELAccess<'a>,
    O::ObjectRepr: Eq + std::hash::Hash,
{
    let mut activity_occurrences: HashMap<&str, u64> = HashMap::new();
    // Per (activity, object type) that occur together: events with exactly one such object.
    let mut with_exactly_one: HashMap<(&str, &str), u64> = HashMap::new();
    let mut objects_per_type: HashMap<&str, HashSet<&O::ObjectRepr>> = HashMap::new();

    for event in ocel.get_all_evs() {
        objects_per_type.clear();
        for (_qualifier, object) in ocel.get_e2o(&event) {
            // A set, since an event may relate to the same object under several qualifiers.
            objects_per_type
                .entry(ocel.get_ob_type_of(object))
                .or_default()
                .insert(object);
        }

        // An event without objects appears in no flattened log, so it says nothing about arcs.
        if objects_per_type.is_empty() {
            continue;
        }

        let activity = ocel.get_ev_type_of(&event);
        *activity_occurrences.entry(activity).or_default() += 1;

        for (object_type, objects) in &objects_per_type {
            *with_exactly_one.entry((activity, object_type)).or_default() +=
                u64::from(objects.len() == 1);
        }
    }

    let mut variable: HashMap<String, HashSet<String>> = HashMap::new();
    for ((activity, object_type), exactly_one) in with_exactly_one {
        let occurrences = activity_occurrences[activity];
        let deviating = occurrences - exactly_one;
        if deviating as f64 > tolerance * occurrences as f64 {
            variable
                .entry(object_type.to_string())
                .or_default()
                .insert(activity.to_string());
        }
    }

    variable
}

#[cfg(test)]
mod test_discover_ocpn {
    use super::*;
    use crate::core::event_data::object_centric::linked_ocel::IndexLinkedOCEL;
    use crate::core::event_data::object_centric::ocel_struct::{
        OCELEvent, OCELObject, OCELRelationship, OCELType,
    };
    use crate::OCEL;

    fn object(id: &str, object_type: &str) -> OCELObject {
        OCELObject {
            id: id.to_string(),
            object_type: object_type.to_string(),
            attributes: vec![],
            relationships: vec![],
        }
    }

    fn event(id: &str, event_type: &str, objects: &[&str]) -> OCELEvent {
        OCELEvent {
            id: id.to_string(),
            event_type: event_type.to_string(),
            time: chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap(),
            attributes: vec![],
            relationships: objects
                .iter()
                .map(|o| OCELRelationship {
                    object_id: (*o).to_string(),
                    qualifier: "rel".to_string(),
                })
                .collect(),
        }
    }

    fn types(names: &[&str]) -> Vec<OCELType> {
        names
            .iter()
            .map(|name| OCELType {
                name: name.to_string(),
                attributes: vec![],
            })
            .collect()
    }

    /// One order with two items, picked individually and then paid. "place order" touches both
    /// items at once, everything else one object at a time.
    fn example_ocel() -> OCEL {
        OCEL {
            event_types: types(&["place order", "pick", "pay"]),
            object_types: types(&["order", "item"]),
            objects: vec![
                object("o1", "order"),
                object("i1", "item"),
                object("i2", "item"),
            ],
            events: vec![
                event("e1", "place order", &["o1", "i1", "i2"]),
                event("e2", "pick", &["i1"]),
                event("e3", "pick", &["i2"]),
                event("e4", "pay", &["o1"]),
            ],
        }
    }

    #[test]
    fn test_variable_arcs_follow_the_number_of_objects_per_event() {
        let locel = IndexLinkedOCEL::from(example_ocel());
        let variable = discover_variable_arcs(&locel, 0.0);

        // "place order" relates to two items at once but to a single order.
        assert!(variable["item"].contains("place order"));
        assert!(!variable.contains_key("order"));
        assert!(!variable["item"].contains("pick"));
    }

    /// An event may relate to the same object under several qualifiers, which is one occurrence.
    /// Counting it twice would invent a repetition and make the arc look variable.
    #[test]
    fn test_repeated_relations_to_the_same_object_count_once() {
        let mut ocel = example_ocel();
        ocel.events[3].relationships.push(OCELRelationship {
            object_id: "o1".to_string(),
            qualifier: "also".to_string(),
        });

        let locel = IndexLinkedOCEL::from(ocel);
        assert!(!discover_variable_arcs(&locel, 0.0).contains_key("order"));
    }

    #[test]
    fn test_variable_arc_tolerance() {
        // A third "pick" that touches no item at all: one of three executions deviates.
        let mut ocel = example_ocel();
        ocel.events.push(event("e5", "pick", &["o1"]));
        let locel = IndexLinkedOCEL::from(ocel);

        assert!(discover_variable_arcs(&locel, 0.0)["item"].contains("pick"));
        assert!(!discover_variable_arcs(&locel, 0.6)["item"].contains("pick"));
    }

    /// Leaving a type out has to give the same nets for the rest, since nothing about one type's
    /// net depends on the others being discovered.
    #[test]
    fn test_selecting_object_types_leaves_the_rest_alone() {
        let locel = IndexLinkedOCEL::from(example_ocel());
        let options = OcpnDiscoveryOptions::default();
        let all = discover_ocpn_from(&locel, options.clone());

        let some = discover_ocpn_from(&locel, options.with_object_types(["item"]));
        assert_eq!(some.object_types(), vec!["item"]);
        assert_eq!(some.num_places(), all.nets["item"].net.num_places());
        assert_eq!(some.num_arcs(), all.nets["item"].net.arcs().count());
        assert_eq!(some.activities(), vec!["pick", "place order"]);
        let mut expected = all.variable_arcs;
        expected.retain(|object_type, _| object_type == "item");
        assert_eq!(some.variable_arcs, expected);
    }

    #[test]
    fn test_keeping_some_activities() {
        let locel = IndexLinkedOCEL::from(example_ocel());
        let ocpn = discover_ocpn_from(
            &locel,
            OcpnDiscoveryOptions::default().with_activities(["place order", "pay"]),
        );
        assert_eq!(ocpn.activities(), vec!["pay", "place order"]);
        assert!(ocpn.is_variable_arc("item", "place order"));
        assert!(!ocpn.variable_arcs["item"].contains("pick"));
    }

    #[test]
    fn test_the_dfg_variant_discovers_the_same_nets_here() {
        use crate::discovery::case_centric::inductive_miner::InductiveMinerDfgOptions;
        let locel = IndexLinkedOCEL::from(example_ocel());
        let log = discover_ocpn_from(&locel, OcpnDiscoveryOptions::default());
        let dfg = discover_ocpn_from(
            &locel,
            OcpnDiscoveryOptions::new(InductiveMinerDfgOptions::default()),
        );
        assert_eq!(dfg.activities(), log.activities());
        for (object_type, net) in &log.nets {
            assert_eq!(dfg.nets[object_type].net.num_places(), net.net.num_places());
        }
    }

    #[test]
    fn test_discovers_a_net_per_object_type() {
        let locel = IndexLinkedOCEL::from(example_ocel());
        let ocpn = discover_ocpn_from(&locel, OcpnDiscoveryOptions::default());

        assert_eq!(ocpn.object_types(), vec!["item", "order"]);
        assert_eq!(ocpn.activities(), vec!["pay", "pick", "place order"]);

        // "place order" is shared by both types, which is the stitching point.
        assert_eq!(ocpn.object_types_of("place order"), vec!["item", "order"]);
        assert_eq!(ocpn.object_types_of("pay"), vec!["order"]);
        assert!(ocpn.is_variable_arc("item", "place order"));
        assert!(!ocpn.is_variable_arc("order", "place order"));

        for (object_type, net) in &ocpn.nets {
            assert!(
                !net.initial_marking.is_empty(),
                "{object_type} has no initial marking"
            );
            assert!(
                !net.final_marking.is_empty(),
                "{object_type} has no final marking"
            );
        }
    }

    /// Objects taking part in no event must not contribute an empty trace, which the miner would
    /// read as "this object type may be skipped".
    #[test]
    fn test_objects_without_events_do_not_make_the_net_optional() {
        let mut ocel = example_ocel();
        ocel.objects.push(object("i3", "item"));
        let locel = IndexLinkedOCEL::from(ocel);

        let ocpn = discover_ocpn_from(&locel, OcpnDiscoveryOptions::default());
        assert_eq!(ocpn.object_types_of("pick"), vec!["item"]);
        assert!(ocpn.nets["item"]
            .net
            .transitions()
            .any(|(_, t)| t.label.as_deref() == Some("pick")));
    }
}
