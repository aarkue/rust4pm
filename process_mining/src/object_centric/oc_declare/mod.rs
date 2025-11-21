mod discovery;
mod preprocess;
use chrono::{DateTime, Duration, FixedOffset};
pub use discovery::{
    discover_behavior_constraints, reduce_oc_arcs, O2OMode, OCDeclareDiscoveryOptions, *,
};
pub use preprocess::{preprocess_ocel, EXIT_EVENT_PREFIX, INIT_EVENT_PREFIX};

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::ocel::linked_ocel::{
    index_linked_ocel::{EventIndex, ObjectIndex},
    IndexLinkedOCEL, LinkedOCELAccess,
};

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
/// OC-DECLARE node (Activity or Object Init/Exit, also see [`preprocess::preprocess_ocel`])
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Returns a value from 0 (all source events satisfy this constraint) to 1 (all source events violate this constraint)
    pub fn get_for_all_evs_perf(&self, linked_ocel: &IndexLinkedOCEL) -> f64 {
        perf::get_for_all_evs_perf(
            self.from.as_str(),
            self.to.as_str(),
            &self.label,
            &self.arc_type,
            &self.counts,
            linked_ocel,
        )
    }

    /// Checks whether the number of events violating this constraint arc is below (<=) the given noise threshold
    ///
    /// Returns false, if the fraction of events violating the constraint is above the noise threshold.
    pub fn get_for_all_evs_perf_thresh(
        &self,
        linked_ocel: &IndexLinkedOCEL,
        noise_thresh: f64,
    ) -> bool {
        perf::get_for_all_evs_perf_thresh(
            self.from.as_str(),
            self.to.as_str(),
            &self.label,
            &self.arc_type,
            &self.counts,
            linked_ocel,
            noise_thresh,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Information on an event violating an OC-DECLARE constraint arc.
pub struct ViolationInfo {
    /// The ID of the triggering source event.
    pub source_ev: String,
    /// The IDs of matching target events.
    pub matching_evs: Vec<String>,
    /// A collection of all objects that must be involved.
    pub all_obs: Vec<String>,
    /// A collection of object sets where at least one object from each set must be involved.
    pub any_obs: Vec<Vec<String>>,
    /// The actual count of observed target events.
    pub count: usize,
    /// The type of violation that occurred.
    pub violation_type: ViolationType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Describes the type of violation for an OC-DECLARE constraint.
pub enum ViolationType {
    /// Indicates that more target events were found than allowed by the constraint's upper bound.
    TooMany,
    /// Indicates that fewer target events were found than required by the constraint's lower bound.
    TooFew,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
/// OC-DECLARE Arc Direction/Type
///
/// Models temporal relationships
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

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(tag = "type")]
/// Object Type Association: Direct or O2O Object Types
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
    pub fn get_for_ev(
        &self,
        ev: &EventOrSynthetic,
        linked_ocel: &IndexLinkedOCEL,
    ) -> Vec<ObjectIndex> {
        match self {
            ObjectTypeAssociation::Simple { object_type } => ev
                .get_e2o(linked_ocel)
                .filter(|o| {
                    let ob = linked_ocel.get_ob(o);
                    ob.object_type == *object_type
                })
                .collect(),
            ObjectTypeAssociation::O2O {
                first,
                second,
                reversed,
            } => ev
                .get_e2o(linked_ocel)
                .filter(|o| linked_ocel.get_ob(o).object_type == *first)
                .flat_map(|o| {
                    if !reversed {
                        linked_ocel
                            .get_o2o(&o)
                            .map(|rel| rel.1)
                            .filter(|o2| linked_ocel.get_ob(o2).object_type == *second)
                            .collect_vec()
                    } else {
                        linked_ocel
                            .get_o2o_rev(&o)
                            .map(|rel| rel.1)
                            .filter(|o2| linked_ocel.get_ob(o2).object_type == *second)
                            .collect_vec()
                    }
                })
                .copied()
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, Hash)]
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

impl<T: Eq + Hash> SetFilter<T> {
    /// Check if the specified `HashSet` fulfills this predicate
    pub fn check(&self, s: &HashSet<T>) -> bool {
        match self {
            SetFilter::Any(items) => items.iter().any(|i| s.contains(i)),
            SetFilter::All(items) => items.iter().all(|i| s.contains(i)),
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
        linked_ocel: &'a IndexLinkedOCEL,
    ) -> impl Iterator<Item = Vec<SetFilter<ObjectIndex>>> + use<'a, 'b> {
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

#[derive(Serialize, Deserialize, Clone, Debug)]
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

/// Get the object type involvements for an activity
///
/// Produces the min and max counts for objects per object type and activity
///
/// The result is a mapping: Activity -> (Object Type -> Counts)
pub fn get_activity_object_involvements(
    locel: &IndexLinkedOCEL,
) -> HashMap<String, HashMap<String, ObjectInvolvementCounts>> {
    locel
        .get_ev_types()
        .map(|et| {
            let mut nums_of_objects_per_type: HashMap<String, ObjectInvolvementCounts> = locel
                .get_ob_types()
                .map(|ot| (ot.to_string(), ObjectInvolvementCounts::default()))
                .collect();
            for ev in locel.get_evs_of_type(et) {
                let mut num_of_objects_for_ev: HashMap<&str, usize> = HashMap::new();
                for (_q, oi) in locel.get_e2o(ev) {
                    let o = locel.get_ob(oi);
                    *num_of_objects_for_ev.entry(&o.object_type).or_default() += 1;
                }
                for (ot, count) in num_of_objects_for_ev {
                    let num_ob_per_type = nums_of_objects_per_type.get_mut(ot).unwrap();

                    if count < num_ob_per_type.min {
                        num_ob_per_type.min = count
                    }
                    if count > num_ob_per_type.max {
                        num_ob_per_type.max = count;
                    }
                }
            }
            (
                et.to_string(),
                nums_of_objects_per_type
                    .into_iter()
                    .filter(|(_x, y)| y.max > 0)
                    .collect(),
            )
        })
        .collect()
}

/// Get Object-to-Object Involvements in the passed OCEL
///
/// Returns a mapping Object Type -> (Object Type -> Count)
///
/// where Count specifies how many objects of the second object type are referenced by each objects of the first object type
pub fn get_object_to_object_involvements(
    locel: &IndexLinkedOCEL,
) -> HashMap<String, HashMap<String, ObjectInvolvementCounts>> {
    locel
        .get_ob_types()
        .map(|ot| {
            let mut nums_of_objects_per_type: HashMap<String, ObjectInvolvementCounts> = locel
                .get_ob_types()
                .map(|ot| (ot.to_string(), ObjectInvolvementCounts::default()))
                .collect();
            for ob in locel.get_obs_of_type(ot) {
                let mut num_of_objects_for_ob: HashMap<&str, usize> = HashMap::new();
                for (_q, oi) in locel.get_o2o(ob) {
                    let o = locel.get_ob(oi);
                    *num_of_objects_for_ob.entry(&o.object_type).or_default() += 1;
                }
                for (ot, count) in num_of_objects_for_ob {
                    let num_ob_per_type = nums_of_objects_per_type.get_mut(ot).unwrap();

                    if count < num_ob_per_type.min {
                        num_ob_per_type.min = count
                    }
                    if count > num_ob_per_type.max {
                        num_ob_per_type.max = count;
                    }
                }
            }
            (
                ot.to_string(),
                nums_of_objects_per_type
                    .into_iter()
                    .filter(|(_x, y)| y.max > 0)
                    .collect(),
            )
        })
        .collect()
}

/// Get object involvement counts for the reverse direction of O2O relationships
///
/// Returns a mapping Object Type -> (Object Type -> Count)
///
/// where Count specifies how many objects of the second object type reference each object of the first object type
pub fn get_rev_object_to_object_involvements(
    locel: &IndexLinkedOCEL,
) -> HashMap<String, HashMap<String, ObjectInvolvementCounts>> {
    locel
        .get_ob_types()
        .map(|ot| {
            let mut nums_of_objects_per_type: HashMap<String, ObjectInvolvementCounts> = locel
                .get_ob_types()
                .map(|ot| (ot.to_string(), ObjectInvolvementCounts::default()))
                .collect();
            for ob in locel.get_obs_of_type(ot) {
                let mut num_of_objects_for_ob: HashMap<&str, usize> = HashMap::new();
                for (_q, oi) in locel.get_o2o_rev(ob) {
                    let o = locel.get_ob(oi);
                    *num_of_objects_for_ob.entry(&o.object_type).or_default() += 1;
                }
                for (ot, count) in num_of_objects_for_ob {
                    let num_ob_per_type = nums_of_objects_per_type.get_mut(ot).unwrap();

                    if count < num_ob_per_type.min {
                        num_ob_per_type.min = count
                    }
                    if count > num_ob_per_type.max {
                        num_ob_per_type.max = count;
                    }
                }
            }
            (
                ot.to_string(),
                nums_of_objects_per_type
                    .into_iter()
                    .filter(|(_x, y)| y.max > 0)
                    .collect(),
            )
        })
        .collect()
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Represents either a regular event or a synthetic initialization/exit event for an object.
///
/// This enum is used to model synthetic events (as source or target) for OC-DECLARE constraints, which can be activated by
/// regular events from the log or by synthetic events marking object lifecycles.
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
    pub fn get_as_event_type(&self, locel: &IndexLinkedOCEL) -> String {
        match self {
            EventOrSynthetic::Event(event_index) => locel[event_index].event_type.to_string(),
            EventOrSynthetic::Init(object_index) => {
                format!("{INIT_EVENT_PREFIX} {}", locel[object_index].object_type)
            }
            EventOrSynthetic::Exit(object_index) => {
                format!("{EXIT_EVENT_PREFIX} {}", locel[object_index].object_type)
            }
        }
    }
    fn get_mock_ev_index(&self, locel: &IndexLinkedOCEL) -> EventIndex {
        match self {
            EventOrSynthetic::Event(event_index) => *event_index,
            EventOrSynthetic::Init(x) | EventOrSynthetic::Exit(x) => {
                let evs = locel.get_e2o_rev(x).map(|(_q, e)| e);

                if matches!(self, EventOrSynthetic::Init(_)) {
                    evs.min().copied().unwrap_or(0_usize.into())
                } else {
                    evs.max().copied().unwrap_or(0_usize.into())
                }
            }
        }
    }

    /// Get the timestamp of the event (regular or synthetic)
    pub fn get_timestamp(&self, locel: &IndexLinkedOCEL) -> DateTime<FixedOffset> {
        let mock_ev_index = self.get_mock_ev_index(locel);

        let time = &locel[mock_ev_index].time;
        match self {
            EventOrSynthetic::Event(_) => *time,
            EventOrSynthetic::Init(_) => *time - Duration::milliseconds(1),
            EventOrSynthetic::Exit(_) => *time + Duration::milliseconds(1),
        }
    }

    /// Get iterator over objects involved in the event (regular or synthetic)
    pub fn get_e2o<'a>(
        &self,
        locel: &'a IndexLinkedOCEL,
    ) -> Box<dyn Iterator<Item = ObjectIndex> + 'a> {
        match self {
            EventOrSynthetic::Event(event_index) => {
                Box::new(locel.get_e2o_set(event_index).iter().copied())
            }
            EventOrSynthetic::Init(x) | EventOrSynthetic::Exit(x) => Box::new(vec![*x].into_iter()),
        }
    }
    /// Get set of objects involved in the event (regular or synthetic)
    pub fn get_e2o_set(&self, locel: &IndexLinkedOCEL) -> HashSet<ObjectIndex> {
        match self {
            EventOrSynthetic::Event(event_index) => locel.get_e2o_set(event_index).clone(),
            EventOrSynthetic::Init(x) | EventOrSynthetic::Exit(x) => vec![*x].into_iter().collect(),
        }
    }
    /// Get all events (regular or synthetic) of a specific event type
    pub fn get_all_syn_evs(locel: &IndexLinkedOCEL, ev_type: &str) -> Vec<Self> {
        if ev_type.starts_with(INIT_EVENT_PREFIX) {
            let ob_type = &ev_type[INIT_EVENT_PREFIX.len() + 1..ev_type.len()];
            locel
                .get_obs_of_type(ob_type)
                .map(|ob| EventOrSynthetic::Init(*ob))
                .collect()
        } else if ev_type.starts_with(EXIT_EVENT_PREFIX) {
            let ob_type = &ev_type[EXIT_EVENT_PREFIX.len() + 1..ev_type.len()];
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
    pub fn get_all_of_et_for_ob(
        locel: &IndexLinkedOCEL,
        ev_type: &str,
        ob: ObjectIndex,
    ) -> Vec<Self> {
        if ev_type.starts_with(INIT_EVENT_PREFIX) {
            let ob_type = &ev_type[INIT_EVENT_PREFIX.len() + 1..ev_type.len()];
            if locel[ob].object_type == ob_type {
                vec![Self::Init(ob)]
            } else {
                Vec::default()
            }
        } else if ev_type.starts_with(EXIT_EVENT_PREFIX) {
            let ob_type = &ev_type[EXIT_EVENT_PREFIX.len() + 1..ev_type.len()];
            if locel[ob].object_type == ob_type {
                vec![Self::Exit(ob)]
            } else {
                Vec::default()
            }
        } else {
            locel
                .e2o_rev_et
                .get(ev_type)
                .iter()
                .flat_map(|x| x.get(&ob))
                .flatten()
                .map(|ev| Self::Event(*ev))
                .collect()
        }
    }
    /// Get all events (regular or synthetic) involving a specific object
    pub fn get_all_for_ob(locel: &IndexLinkedOCEL, ob: ObjectIndex) -> Vec<Self> {
        locel
            .get_e2o_rev(&ob)
            .map(|x| Self::Event(*x.1))
            .chain(vec![Self::Init(ob), Self::Exit(ob)])
            .collect()
    }
}

/// Performance-focused implementations of checking OC-DECLARE constraints
pub mod perf {
    use std::sync::atomic::AtomicI32;

    use super::{OCDeclareArcLabel, OCDeclareArcType, SetFilter};
    use crate::{
        object_centric::oc_declare::EventOrSynthetic,
        ocel::linked_ocel::{index_linked_ocel::ObjectIndex, IndexLinkedOCEL, LinkedOCELAccess},
    };
    use chrono::{DateTime, FixedOffset};
    use rayon::prelude::*;

    /// Get all events of the given event type satisfying the filters
    pub fn get_evs_with_objs_perf<'a>(
        objs: &'a [SetFilter<ObjectIndex>],
        linked_ocel: &'a IndexLinkedOCEL,
        etype: &'a str,
    ) -> impl Iterator<Item = EventOrSynthetic> + use<'a> {
        let initial: Box<dyn Iterator<Item = EventOrSynthetic>> = if objs.is_empty() {
            Box::new(EventOrSynthetic::get_all_syn_evs(linked_ocel, etype).into_iter())
        } else {
            match &objs[0] {
                SetFilter::Any(items) => {
                    Box::new(items.iter().flat_map(|o| {
                        EventOrSynthetic::get_all_of_et_for_ob(linked_ocel, etype, *o)
                    }))
                }
                SetFilter::All(items) => {
                    if items.is_empty() {
                        Box::new(Vec::new().into_iter())
                    } else {
                        Box::new(
                            EventOrSynthetic::get_all_of_et_for_ob(linked_ocel, etype, items[0])
                                .into_iter()
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
        objs: &'a [SetFilter<ObjectIndex>],
        linked_ocel: &'a IndexLinkedOCEL,
        // reference_mock_event_index: &'a EventIndex,
        reference_time: &'a DateTime<FixedOffset>,
        // reference_event: &'a OCELEvent,
        following: bool,
    ) -> Option<EventOrSynthetic> {
        let initial: Box<dyn Iterator<Item = EventOrSynthetic>> = if objs.is_empty() {
            // If no requirements are specified, consider all events
            // TODO: Maybe also consider synthetic events here?
            // But in general, this is not very relevant as there are usually some object requirements
            Box::new(
                linked_ocel
                    .get_all_evs_ref()
                    .map(|ev| EventOrSynthetic::Event(*ev)),
            )
        } else {
            match &objs[0] {
                SetFilter::Any(items) => Box::new(items.iter().flat_map(|o| {
                    EventOrSynthetic::get_all_for_ob(linked_ocel, *o)
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
                            EventOrSynthetic::get_all_for_ob(linked_ocel, items[0])
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
        linked_ocel: &IndexLinkedOCEL,
    ) -> f64 {
        let evs = EventOrSynthetic::get_all_syn_evs(linked_ocel, from_et);
        let ev_count = evs.len();
        let violated_evs_count = evs
            .into_par_iter()
            // .into_iter()
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
        linked_ocel: &IndexLinkedOCEL,
        violation_thresh: f64,
    ) -> bool {
        let evs = EventOrSynthetic::get_all_syn_evs(linked_ocel, from_et);
        let ev_count = evs.len();
        let min_s = (ev_count as f64 * (1.0 - violation_thresh)).ceil() as usize;
        let min_v = (ev_count as f64 * violation_thresh).floor() as usize + 1;
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

        // Atomic:
        let min_v_atomic = AtomicI32::new(min_v as i32);
        let min_s_atomic = AtomicI32::new(min_s as i32);
        evs.into_par_iter()
            .map(|ev| {
                let violated = get_for_ev_perf(&ev, label, to_et, arc_type, counts, linked_ocel);
                if violated {
                    min_v_atomic.fetch_add(-1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    min_s_atomic.fetch_add(-1, std::sync::atomic::Ordering::Relaxed);
                }
                ev
            })
            .take_any_while(|_x| {
                if min_s_atomic.load(std::sync::atomic::Ordering::Relaxed) <= 0 {
                    return false;
                }
                if min_v_atomic.load(std::sync::atomic::Ordering::Relaxed) <= 0 {
                    return false;
                }
                true
            })
            .count();
        let min_s_atomic = min_s_atomic.into_inner();
        let min_v_atomic = min_v_atomic.into_inner();
        // println!("{} and {}",min_s_atomic,min_v_atomic);
        if min_s_atomic <= 0 {
            return true;
        }
        if min_v_atomic <= 0 {
            return false;
        }

        unreachable!()

        // println!("{} and {} of {} (min_s: {}, min_v: {})",min_s_atomic,min_v_atomic,ev_count,min_s,min_v);
        // true

        // Previous:
        // let violated_evs_count =
        // evs
        //     .into_par_iter()
        //     // .into_iter()
        //     .filter(|ev| get_for_ev_perf(ev, label, to_et, arc_type, counts, linked_ocel))
        //     // .take_any(min_v)
        //     .take_any(min_s)
        //     .count();
        // violated_evs_count < min_v
        // // sat_evs_count >= min_s
    }

    /// Returns true if violated!
    pub fn get_for_ev_perf(
        ev_index: &EventOrSynthetic,
        label: &OCDeclareArcLabel,
        to_et: &str,
        arc_type: &OCDeclareArcType,
        counts: &(Option<usize>, Option<usize>),
        linked_ocel: &IndexLinkedOCEL,
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
                    let count = if df_ev.is_some_and(|e| e.get_as_event_type(linked_ocel) == to_et)
                    {
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
}
