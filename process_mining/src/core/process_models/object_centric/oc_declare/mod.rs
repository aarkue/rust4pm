//! OC-DECLARE Object-Centric Declarative Process Models
pub use crate::core::event_data::object_centric::utils::init_exit_events::{
    add_init_exit_events_to_ocel, EXIT_EVENT_PREFIX, INIT_EVENT_PREFIX,
};
use chrono::{DateTime, Duration, FixedOffset};
use schemars::JsonSchema;

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::conformance::oc_declare::{satisfies_threshold, violation_fraction};
use crate::core::event_data::object_centric::linked_ocel::e2o_rev_type_index::E2ORevByTypeIndex;
use crate::core::event_data::object_centric::linked_ocel::slim_linked_ocel::{
    EventIndex, ObjectIndex,
};
use crate::core::event_data::object_centric::linked_ocel::{LinkedOCELAccess, SlimLinkedOCEL};

#[derive(
    Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord, JsonSchema,
)]
/// OC-DECLARE node (Activity or Object Init/Exit)
pub struct OCDeclareNode(String);

impl<'a> From<&'a OCDeclareNode> for &'a String {
    fn from(val: &'a OCDeclareNode) -> Self {
        &val.0
    }
}

impl OCDeclareNode {
    /// Create OC-DECLARE node from String
    pub fn new<T: Into<String>>(act: T) -> Self {
        Self(act.into())
    }

    /// Return node name
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash, PartialOrd, Ord, JsonSchema,
)]
/// OC-DECLARE Constraint arc/edge between two nodes (i.e., activities)
pub struct OCDeclareArc {
    /// Source node (e.g., triggering activity)
    pub from: OCDeclareNode,
    /// Target node (e.g., target activity)
    pub to: OCDeclareNode,
    /// Arc type, modeling temporal relation
    pub arc_type: OCDeclareArcType,
    /// Arc label specifying object involvement criteria
    pub label: OCDeclareArcLabel,
    /// First tuple element: min count (optional), Second: max count (optional)
    pub counts: (Option<usize>, Option<usize>),
}

impl OCDeclareArc {
    /// Clone this arc, only modifying its arc/arrow type
    pub fn clone_with_arc_type(&self, arc_type: OCDeclareArcType) -> Self {
        let mut ret = self.clone();
        ret.arc_type = arc_type;
        ret
    }

    /// Generate template string representation
    pub fn as_template_string(&self) -> String {
        format!(
            "{}({}, {}, {},{},{})",
            self.arc_type.get_name(),
            self.from.0,
            self.to.0,
            self.label.as_template_string(),
            self.counts.0.unwrap_or_default(),
            self.counts
                .1
                .map(|x| x.to_string())
                .unwrap_or(String::from("∞"))
        )
    }

    /// Get fraction of source events violating this constraint arc
    ///
    /// Returns a value from 0 (all source events satisfy this constraint) to 1 (all source events
    /// violate this constraint). Without any source events, the constraint is vacuously satisfied
    /// and this returns 0.
    pub fn violation_fraction(&self, linked_ocel: &SlimLinkedOCEL) -> f64 {
        violation_fraction(
            self.from.as_str(),
            self.to.as_str(),
            &self.label,
            &self.arc_type,
            &self.counts,
            linked_ocel,
            None,
        )
    }

    /// Checks whether the number of events violating this constraint arc is below (<=) the given noise threshold
    ///
    /// Returns false, if the fraction of events violating the constraint is above the noise threshold.
    pub fn satisfies_threshold(&self, linked_ocel: &SlimLinkedOCEL, noise_thresh: f64) -> bool {
        self.satisfies_threshold_indexed(linked_ocel, noise_thresh, None)
    }

    /// As [`OCDeclareArc::satisfies_threshold`], but reusing an existing index
    pub(crate) fn satisfies_threshold_indexed(
        &self,
        linked_ocel: &SlimLinkedOCEL,
        noise_thresh: f64,
        index: Option<&E2ORevByTypeIndex>,
    ) -> bool {
        satisfies_threshold(
            self.from.as_str(),
            self.to.as_str(),
            &self.label,
            &self.arc_type,
            &self.counts,
            linked_ocel,
            noise_thresh,
            index,
        )
    }
}

/// OC-DECLARE Arc Direction/Type
///
/// Models temporal relationships
#[derive(
    Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord, JsonSchema,
)]
pub enum OCDeclareArcType {
    /// Association: No temporal restrictions
    AS,
    /// Eventually-Follows: Target must occur after source event
    EF,
    /// Eventually-Precedes: Target must occur before source event
    EP,
    /// Directly-Follows: Target must occur directly after source event (considering events that involve all required objects)
    DF,
    /// Directly-Precedes: Target must occur directly before source event (considering events that involve all required objects)
    DP,
}
/// All OC-DECLARE Arc Types
pub const ALL_OC_DECLARE_ARC_TYPES: &[OCDeclareArcType] = &[
    OCDeclareArcType::AS,
    OCDeclareArcType::EF,
    OCDeclareArcType::EP,
    OCDeclareArcType::DF,
    OCDeclareArcType::DP,
];

impl OCDeclareArcType {
    /// Parse a string to an arc type
    ///
    /// e.g., `"AS"` -> [`OCDeclareArcType::AS`], `"EF"` -> [`OCDeclareArcType::EF`]
    ///
    /// Returns `None` if the string cannot be parsed
    pub fn parse_str(s: impl AsRef<str>) -> Option<Self> {
        match s.as_ref() {
            "AS" => Some(Self::AS),
            "EF" => Some(Self::EF),
            "EP" => Some(Self::EP),
            "DF" => Some(Self::DF),
            "DP" => Some(Self::DP),
            _ => None,
        }
    }

    /// Get name of this arc type as string (e.g., `"EF"`)
    pub fn get_name(&self) -> &'static str {
        match self {
            OCDeclareArcType::AS => "AS",
            OCDeclareArcType::EF => "EF",
            OCDeclareArcType::EP => "EP",
            OCDeclareArcType::DF => "DF",
            OCDeclareArcType::DP => "DP",
        }
    }

    /// Check if this arc type is dominated by other arc type
    pub fn is_dominated_by_or_eq(&self, arc_type: &OCDeclareArcType) -> bool {
        if *self == OCDeclareArcType::AS || self == arc_type {
            return true;
        }
        if *arc_type == OCDeclareArcType::AS {
            return false;
        }
        match arc_type {
            OCDeclareArcType::DF => *self == OCDeclareArcType::EF,
            OCDeclareArcType::DP => *self == OCDeclareArcType::EP,
            _ => false,
        }
    }
}

/// Object Type Association: Direct or O2O Object Types
#[derive(
    Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord, JsonSchema,
)]
#[serde(tag = "type")]
pub enum ObjectTypeAssociation {
    /// Simple: Direct Object Types involved with an Activity
    Simple {
        /// The object type
        object_type: String,
    },
    /// Indirect: Object Association through an O2O relationship
    O2O {
        /// First object type (for source event)
        first: String,
        /// Second object type (for target event)
        second: String,
        /// Specifies the direction of the O2O relationship.
        ///
        /// If reversed is `False`, `(first,second)` is considered
        reversed: bool,
    },
}

impl ObjectTypeAssociation {
    /// Create new simple (i.e., direct) object type association
    pub fn new_simple<T: Into<String>>(ot: T) -> Self {
        Self::Simple {
            object_type: ot.into(),
        }
    }
    /// Create indirect (i.e., O2O) object type association
    ///
    /// Considers the non-reversed direction, i.e., O2O from `ot1` to `ot2`
    pub fn new_o2o<T: Into<String>>(ot1: T, ot2: T) -> Self {
        Self::O2O {
            first: ot1.into(),
            second: ot2.into(),
            reversed: false,
        }
    }
    /// Create reversed indirect (i.e., O2O) object type association
    ///
    /// Considers the reversed direction, i.e., O2O from `ot2` to `ot1`
    pub fn new_o2o_rev<T: Into<String>>(ot1: T, ot2: T) -> Self {
        Self::O2O {
            first: ot1.into(),
            second: ot2.into(),
            reversed: true,
        }
    }

    /// Format as string
    pub fn as_template_string(&self) -> String {
        match self {
            ObjectTypeAssociation::Simple { object_type } => object_type.clone(),
            ObjectTypeAssociation::O2O {
                first,
                second,
                reversed,
            } => format!("{}{}{}", first, if !reversed { ">" } else { "<" }, second),
        }
    }

    /// Get the object index for all objects specified by the association for a specified event
    pub fn get_for_ev<'a>(
        &'a self,
        ev: &'a EventOrSynthetic,
        linked_ocel: &'a SlimLinkedOCEL,
    ) -> Vec<&'a ObjectIndex> {
        match self {
            ObjectTypeAssociation::Simple { object_type } => ev
                .get_e2o(linked_ocel)
                .filter(|o| {
                    let ot = o.get_ob_type(linked_ocel);
                    ot == object_type
                })
                .collect(),
            ObjectTypeAssociation::O2O {
                first,
                second,
                reversed,
            } => ev
                .get_e2o(linked_ocel)
                .filter(|o| o.get_ob_type(linked_ocel) == first)
                .flat_map(|o| {
                    if !reversed {
                        o.get_o2o(linked_ocel)
                            .filter(|o2| o2.get_ob_type(linked_ocel) == second)
                            .collect_vec()
                    } else {
                        o.get_o2o_rev(linked_ocel)
                            .filter(|o2| o2.get_ob_type(linked_ocel) == second)
                            .collect_vec()
                    }
                })
                .collect(),
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, Hash, PartialOrd, Ord, JsonSchema,
)]
/// Object Involvement Label of an OC-DECLARE arc
pub struct OCDeclareArcLabel {
    /// Each (for each object of that type separately, there must be the specified number of relevant target events)
    pub each: Vec<ObjectTypeAssociation>,
    /// Any (there must be the specified number of relevant target events involving at least one of the objects of this type involved in the source event)
    pub any: Vec<ObjectTypeAssociation>,
    /// All (there must be the specified number of relevant target events involving all of the objects of this type involved in the source event)
    pub all: Vec<ObjectTypeAssociation>,
}

impl OCDeclareArcLabel {
    /// Format as template string
    pub fn as_template_string(&self) -> String {
        let mut ret = String::new();
        if !self.each.is_empty() {
            ret.push_str(&format!(
                "Each({})",
                self.each.iter().map(|ot| ot.as_template_string()).join(",")
            ));
        }
        if !self.all.is_empty() {
            if !self.each.is_empty() {
                ret.push_str(", ");
            }
            ret.push_str(&format!(
                "All({})",
                self.all.iter().map(|ot| ot.as_template_string()).join(",")
            ));
        }
        if !self.any.is_empty() {
            if !self.each.is_empty() || !self.all.is_empty() {
                ret.push_str(", ");
            }
            ret.push_str(&format!(
                "Any({})",
                self.any.iter().map(|ot| ot.as_template_string()).join(",")
            ));
        }
        ret
    }
}

impl OCDeclareArcLabel {
    /// Combine this OC-DECLARE arc label with another one
    ///
    /// Merges the different object involvements, where more strict requirements take precedence (e.g., ALL over ANY)
    pub fn combine(&self, other: &Self) -> Self {
        let all = self
            .all
            .iter()
            .chain(other.all.iter())
            .cloned()
            .collect::<HashSet<_>>();
        let each = self
            .each
            .iter()
            .chain(other.each.iter())
            .filter(|e| !all.contains(e))
            .cloned()
            .collect::<HashSet<_>>();
        let any = self
            .any
            .iter()
            .chain(other.any.iter())
            .filter(|e| !all.contains(e) && !each.contains(e))
            .cloned()
            .collect::<HashSet<_>>();
        Self {
            each: each.into_iter().sorted().collect(),
            all: all.into_iter().sorted().collect(),
            any: any.into_iter().sorted().collect(),
        }
    }

    /// Tests if this arc label is dominated by the other one
    pub fn is_dominated_by(&self, other: &Self) -> bool {
        let all_all = self.all.iter().all(|a| other.all.contains(a));
        if !all_all {
            return false;
        }
        let all_each = self
            .each
            .iter()
            .all(|a| other.each.contains(a) || other.all.contains(a));
        if !all_each {
            return false;
        }
        let all_any = self
            .any
            .iter()
            .all(|a| other.any.contains(a) || other.each.contains(a) || other.all.contains(a));
        all_any
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
/// Set filter modeling the predicate that all or any of the included elements must be present
pub enum SetFilter<T: Eq + Hash> {
    /// Any predicate: At least one of the contained elements must be present
    Any(Vec<T>),
    /// All predicate: All of the contained elements must be present
    All(Vec<T>),
}

impl<T: Eq + Hash + Ord> SetFilter<T> {
    /// Check if the specified `HashSet` fulfills this predicate
    pub fn check(&self, s: &[T]) -> bool {
        match self {
            SetFilter::Any(items) => items.iter().any(|i| s.binary_search(i).is_ok()),
            SetFilter::All(items) => items.iter().all(|i| s.binary_search(i).is_ok()),
        }
    }
}

impl<'b> OCDeclareArcLabel {
    /// Get all bindings for an OC-DECLARE arc label for a specified event.
    ///
    /// Bindings correspond to all scenarios for which the constraint has to be checked.
    /// In particular, there are multiple bindings for an event if there are multiple objects of a type that is included with EACH involvement.
    pub fn get_bindings<'a>(
        &'a self,
        ev: &'a EventOrSynthetic,
        linked_ocel: &'a SlimLinkedOCEL,
    ) -> impl Iterator<Item = Vec<SetFilter<&'a ObjectIndex>>> + use<'a, 'b> {
        self.each
            .iter()
            .sorted_by_key(|ot| match ot {
                ObjectTypeAssociation::Simple { object_type } => {
                    -(linked_ocel.get_obs_of_type(object_type).count() as i32)
                }
                ObjectTypeAssociation::O2O { second, .. } => {
                    -(linked_ocel.get_obs_of_type(second).count() as i32)
                }
            })
            .map(|otass| otass.get_for_ev(ev, linked_ocel))
            .multi_cartesian_product()
            .map(|product| {
                self.all
                    .iter()
                    .sorted_by_key(|ot| match ot {
                        ObjectTypeAssociation::Simple { object_type } => {
                            -(linked_ocel.get_obs_of_type(object_type).count() as i32)
                        }
                        ObjectTypeAssociation::O2O { second, .. } => {
                            -(linked_ocel.get_obs_of_type(second).count() as i32)
                        }
                    })
                    .map(|otass| SetFilter::All(otass.get_for_ev(ev, linked_ocel)))
                    .chain(if product.is_empty() {
                        Vec::default()
                    } else {
                        vec![SetFilter::All(product)]
                    })
                    .chain(
                        self.any
                            .iter()
                            .sorted_by_key(|ot| match ot {
                                ObjectTypeAssociation::Simple { object_type } => {
                                    -(linked_ocel.get_obs_of_type(object_type).count() as i32)
                                }
                                ObjectTypeAssociation::O2O { second, .. } => {
                                    -(linked_ocel.get_obs_of_type(second).count() as i32)
                                }
                            })
                            .map(|otass| {
                                let x = otass.get_for_ev(ev, linked_ocel);
                                if x.len() == 1 {
                                    SetFilter::All(x)
                                } else {
                                    SetFilter::Any(x)
                                }
                            }),
                    )
                    .collect_vec()
            })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
/// Stores statistics on the number of objects of a certain type involved with an activity or in an O2O relationship.
pub struct ObjectInvolvementCounts {
    /// The minimum number of objects of a given type involved in a single instance.
    pub min: usize,
    /// The maximum number of objects of a given type involved in a single instance.
    pub max: usize,
    // mean: usize,
}
impl Default for ObjectInvolvementCounts {
    fn default() -> Self {
        Self {
            min: usize::MAX,
            max: Default::default(),
        }
    }
}

/// The fewest and most objects of each type related to an instance (an event or an object),
/// over all `instances`. An instance without an object of a type counts as 0. Only types some
/// instance relates to are listed.
fn involvement_counts<'a, R: Iterator<Item = &'a str>>(
    instances: impl Iterator<Item = R>,
) -> HashMap<String, ObjectInvolvementCounts> {
    // Per type, the counts over the instances relating to it, and how many those are.
    let mut counts: HashMap<&str, (ObjectInvolvementCounts, usize)> = HashMap::new();
    let mut num_instances = 0;
    let mut here: HashMap<&str, usize> = HashMap::new();
    for related in instances {
        num_instances += 1;
        here.clear();
        for ot in related {
            *here.entry(ot).or_default() += 1;
        }
        for (&ot, &n) in &here {
            let (c, instances_with_type) = counts.entry(ot).or_default();
            c.min = c.min.min(n);
            c.max = c.max.max(n);
            *instances_with_type += 1;
        }
    }
    counts
        .into_iter()
        .map(|(ot, (mut c, instances_with_type))| {
            if instances_with_type < num_instances {
                c.min = 0;
            }
            (ot.to_string(), c)
        })
        .collect()
}

/// Get the object type involvements for an activity
///
/// Produces the min and max counts for objects per object type and activity, over all events of
/// the activity: an event without an object of the type counts as 0. Only object types the
/// activity involves at least once are listed.
///
/// The result is a mapping: Activity -> (Object Type -> Counts)
pub fn get_activity_object_involvements(
    locel: &SlimLinkedOCEL,
) -> HashMap<String, HashMap<String, ObjectInvolvementCounts>> {
    locel
        .get_ev_types()
        .map(|et| {
            let events = locel
                .get_evs_of_type(et)
                .map(|ev| ev.get_e2o(locel).map(|oi| oi.get_ob_type(locel).as_str()));
            (et.to_string(), involvement_counts(events))
        })
        .collect()
}

/// Get Object-to-Object Involvements in the passed OCEL
///
/// Returns a mapping Object Type -> (Object Type -> Count)
///
/// where Count specifies how many objects of the second object type are referenced by each object
/// of the first object type (0 for an object referencing none). Only object types referenced at
/// least once are listed.
pub fn get_object_to_object_involvements(
    locel: &SlimLinkedOCEL,
) -> HashMap<String, HashMap<String, ObjectInvolvementCounts>> {
    locel
        .get_ob_types()
        .map(|ot| {
            let objects = locel
                .get_obs_of_type(ot)
                .map(|ob| ob.get_o2o(locel).map(|oi| oi.get_ob_type(locel).as_str()));
            (ot.to_string(), involvement_counts(objects))
        })
        .collect()
}

/// Get object involvement counts for the reverse direction of O2O relationships
///
/// Returns a mapping Object Type -> (Object Type -> Count)
///
/// where Count specifies how many objects of the second object type reference each object of the
/// first object type (0 for an object no object of that type references). Only object types
/// referencing at least once are listed.
pub fn get_rev_object_to_object_involvements(
    locel: &SlimLinkedOCEL,
) -> HashMap<String, HashMap<String, ObjectInvolvementCounts>> {
    locel
        .get_ob_types()
        .map(|ot| {
            let objects = locel.get_obs_of_type(ot).map(|ob| {
                ob.get_o2o_rev(locel)
                    .map(|oi| oi.get_ob_type(locel).as_str())
            });
            (ot.to_string(), involvement_counts(objects))
        })
        .collect()
}
/// Represents either a regular event or a synthetic initialization/exit event for an object.
///
/// This enum is used to model synthetic events (as source or target) for OC-DECLARE constraints, which can be activated by
/// regular events from the log or by synthetic events marking object lifecycles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventOrSynthetic {
    /// A regular event from the event log, identified by its index.
    Event(EventIndex),
    /// A synthetic event marking the initialization of an object, identified by the object's index.
    Init(ObjectIndex),
    /// A synthetic event marking the exit of an object, identified by the object's index.
    Exit(ObjectIndex),
}

impl EventOrSynthetic {
    /// Get the event type of the event (regular or synthetic)
    pub fn get_as_event_type(&self, locel: &SlimLinkedOCEL) -> String {
        match self {
            EventOrSynthetic::Event(event_index) => event_index.get_ev_type(locel).to_string(),
            EventOrSynthetic::Init(object_index) => {
                format!("{INIT_EVENT_PREFIX} {}", object_index.get_ob_type(locel))
            }
            EventOrSynthetic::Exit(object_index) => {
                format!("{EXIT_EVENT_PREFIX} {}", object_index.get_ob_type(locel))
            }
        }
    }
    fn get_mock_ev_index(&self, locel: &SlimLinkedOCEL) -> EventIndex {
        match self {
            EventOrSynthetic::Event(event_index) => *event_index,
            EventOrSynthetic::Init(x) | EventOrSynthetic::Exit(x) => {
                let evs = x.get_e2o_rev(locel);

                if matches!(self, EventOrSynthetic::Init(_)) {
                    evs.min_by_key(|ev| locel.get_ev_time(*ev))
                        .copied()
                        .unwrap_or(0_u32.into())
                } else {
                    evs.max_by_key(|ev| locel.get_ev_time(*ev))
                        .copied()
                        .unwrap_or(0_u32.into())
                }
            }
        }
    }

    /// Get the timestamp of the event (regular or synthetic)
    pub fn get_timestamp(&self, locel: &SlimLinkedOCEL) -> DateTime<FixedOffset> {
        let mock_ev_index = self.get_mock_ev_index(locel);

        let time = mock_ev_index.get_time(locel);
        match self {
            EventOrSynthetic::Event(_) => *time,
            EventOrSynthetic::Init(_) => *time - Duration::milliseconds(1),
            EventOrSynthetic::Exit(_) => *time + Duration::milliseconds(1),
        }
    }

    /// Get iterator over objects involved in the event (regular or synthetic)
    pub fn get_e2o<'a>(
        &'a self,
        locel: &'a SlimLinkedOCEL,
    ) -> Box<dyn Iterator<Item = &'a ObjectIndex> + 'a> {
        match self {
            EventOrSynthetic::Event(event_index) => Box::new(event_index.get_e2o(locel)),
            EventOrSynthetic::Init(x) | EventOrSynthetic::Exit(x) => Box::new(vec![x].into_iter()),
        }
    }
    /// Get set of objects involved in the event (regular or synthetic)
    pub fn get_e2o_set<'a>(&'a self, locel: &'a SlimLinkedOCEL) -> Vec<&'a ObjectIndex> {
        match self {
            EventOrSynthetic::Event(event_index) => {
                event_index
                    .get_e2o(locel)
                    // .sorted_unstable()
                    .collect()
            }
            EventOrSynthetic::Init(x) | EventOrSynthetic::Exit(x) => vec![x].into_iter().collect(),
        }
    }
    /// Get all events (regular or synthetic) of a specific event type
    pub fn get_all_syn_evs(locel: &SlimLinkedOCEL, ev_type: &str) -> Vec<Self> {
        if let Some(ob_type) = synthetic_object_type(ev_type, INIT_EVENT_PREFIX) {
            locel
                .get_obs_of_type(ob_type)
                .map(|ob| EventOrSynthetic::Init(*ob))
                .collect()
        } else if let Some(ob_type) = synthetic_object_type(ev_type, EXIT_EVENT_PREFIX) {
            locel
                .get_obs_of_type(ob_type)
                .map(|ob| EventOrSynthetic::Exit(*ob))
                .collect()
        } else {
            locel
                .get_evs_of_type(ev_type)
                .map(|ev| EventOrSynthetic::Event(*ev))
                .collect()
        }
    }

    /// Get all events (regular or synthetic) of a specific event type involving a specific object
    pub fn get_all_of_et_for_ob<'a>(
        locel: &'a SlimLinkedOCEL,
        ev_type: &'a str,
        ob: ObjectIndex,
    ) -> Box<dyn Iterator<Item = Self> + 'a> {
        if let Some(ob_type) = synthetic_object_type(ev_type, INIT_EVENT_PREFIX) {
            if ob.get_ob_type(locel) == ob_type {
                Box::new(vec![Self::Init(ob)].into_iter())
            } else {
                Box::new(Vec::default().into_iter())
            }
        } else if let Some(ob_type) = synthetic_object_type(ev_type, EXIT_EVENT_PREFIX) {
            if ob.get_ob_type(locel) == ob_type {
                Box::new(vec![Self::Exit(ob)].into_iter())
            } else {
                Box::new(Vec::default().into_iter())
            }
        } else {
            Box::new(
                ob.get_e2o_rev_of_evtype(locel, ev_type)
                    .map(|ev| Self::Event(*ev)),
            )
            // .collect()
        }
    }
    /// Get all events (regular or synthetic) involving a specific object
    pub fn get_all_for_ob(locel: &SlimLinkedOCEL, ob: ObjectIndex) -> Vec<Self> {
        ob.get_e2o_rev(locel)
            .map(|e| Self::Event(*e))
            .chain(vec![Self::Init(ob), Self::Exit(ob)])
            .collect()
    }
}

/// The object type of a synthetic event type `"{prefix} {object type}"`, or `None` if `ev_type`
/// is not of that form
fn synthetic_object_type<'a>(ev_type: &'a str, prefix: &str) -> Option<&'a str> {
    ev_type.strip_prefix(prefix)?.strip_prefix(' ')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::{appendable::AppendableOCEL, OCELType};

    #[test]
    fn malformed_synthetic_event_types_do_not_panic() {
        let mut locel = SlimLinkedOCEL::new();
        locel
            .declare_object_type(OCELType {
                name: "order".into(),
                attributes: Vec::new(),
            })
            .unwrap();
        locel
            .append_object("o1".into(), "order", Vec::new(), Vec::new())
            .unwrap();
        locel.finalize().unwrap();
        let ob = *locel.get_obs_of_type("order").next().unwrap();

        assert_eq!(
            EventOrSynthetic::get_all_syn_evs(&locel, "<init> order"),
            vec![EventOrSynthetic::Init(ob)]
        );
        assert_eq!(
            EventOrSynthetic::get_all_of_et_for_ob(&locel, "<exit> order", ob).collect_vec(),
            vec![EventOrSynthetic::Exit(ob)]
        );
        for ev_type in ["<init>", "<exit>", "<init>\u{e9}order", "<exit>\u{e9}"] {
            assert!(EventOrSynthetic::get_all_syn_evs(&locel, ev_type).is_empty());
            assert_eq!(
                EventOrSynthetic::get_all_of_et_for_ob(&locel, ev_type, ob).count(),
                0
            );
        }
    }

    #[test]
    fn involvement_counts_treat_missing_types_as_zero() {
        let counts = |min, max| ObjectInvolvementCounts { min, max };
        let got = involvement_counts(
            [
                vec!["order", "order", "item"],
                vec!["order"],
                vec![],
                vec!["order", "item", "item", "item"],
            ]
            .into_iter()
            .map(|types| types.into_iter()),
        );
        assert_eq!(
            got,
            HashMap::from([
                ("order".to_string(), counts(0, 2)),
                ("item".to_string(), counts(0, 3)),
            ])
        );
        let got = involvement_counts(
            [vec!["order", "order"], vec!["order"]]
                .into_iter()
                .map(|types| types.into_iter()),
        );
        assert_eq!(got, HashMap::from([("order".to_string(), counts(1, 2))]));
        let none: [std::vec::IntoIter<&str>; 0] = [];
        assert!(involvement_counts(none.into_iter()).is_empty());
    }
}
