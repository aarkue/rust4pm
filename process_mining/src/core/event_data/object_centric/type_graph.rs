//! The type graph of an OCEL: its event types and object types, joined by the E2O and O2O
//! relations between them.

use std::collections::{BTreeSet, HashMap};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::event_data::object_centric::linked_ocel::{LinkedOCELAccess, SlimLinkedOCEL};

/// Whether a type graph node is an event type or an object type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TypeKind {
    /// An event type (activity), drawn as a box.
    Event,
    /// An object type, drawn as a pill.
    Object,
}

/// An event type or object type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OcTypeGraphNode {
    /// Event type or object type.
    pub kind: TypeKind,
    /// The type's name.
    pub label: String,
    /// How many events or objects have this type, shown under the name.
    #[serde(default)]
    pub count: Option<u64>,
}

/// A relation between two types, by their index in [`OcTypeGraph::nodes`]. One from an event type
/// is an E2O relation, one from an object type an O2O relation, which is drawn dashed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OcTypeGraphEdge {
    /// Index of the source type.
    pub source: usize,
    /// Index of the target type.
    pub target: usize,
    /// The relation's qualifier, shown on the edge.
    #[serde(default)]
    pub qualifier: Option<String>,
}

/// The types of an OCEL and the relations between them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OcTypeGraph {
    /// The event types and object types.
    pub nodes: Vec<OcTypeGraphNode>,
    /// The E2O and O2O relations, one per source type, target type and qualifier.
    pub edges: Vec<OcTypeGraphEdge>,
}

impl OcTypeGraph {
    /// Every event type and object type of `ocel` with its count, and one edge per qualified
    /// relation between two types that occurs in it, in a fixed order.
    pub fn from_linked_ocel(ocel: &SlimLinkedOCEL) -> Self {
        let mut nodes = Vec::new();
        let mut index: HashMap<(TypeKind, &str), usize> = HashMap::new();
        for name in ocel.get_ev_types() {
            index.insert((TypeKind::Event, name), nodes.len());
            nodes.push(OcTypeGraphNode {
                kind: TypeKind::Event,
                label: name.to_string(),
                count: Some(ocel.get_evs_of_type(name).count() as u64),
            });
        }
        for name in ocel.get_ob_types() {
            index.insert((TypeKind::Object, name), nodes.len());
            nodes.push(OcTypeGraphNode {
                kind: TypeKind::Object,
                label: name.to_string(),
                count: Some(ocel.get_obs_of_type(name).count() as u64),
            });
        }

        // Dedup over borrowed names, so a name is cloned once per edge, not per relationship.
        let mut relations: BTreeSet<(usize, usize, &str)> = BTreeSet::new();
        for ev in ocel.get_all_evs() {
            let source = index[&(TypeKind::Event, ocel.get_ev_type_of(&ev))];
            for (qualifier, ob) in ocel.get_e2o(&ev) {
                let target = index[&(TypeKind::Object, ocel.get_ob_type_of(ob))];
                relations.insert((source, target, qualifier));
            }
        }
        for ob in ocel.get_all_obs() {
            let source = index[&(TypeKind::Object, ocel.get_ob_type_of(&ob))];
            for (qualifier, target_ob) in ocel.get_o2o(&ob) {
                let target = index[&(TypeKind::Object, ocel.get_ob_type_of(target_ob))];
                relations.insert((source, target, qualifier));
            }
        }
        let edges = relations
            .into_iter()
            .map(|(source, target, qualifier)| OcTypeGraphEdge {
                source,
                target,
                qualifier: Some(qualifier.to_string()),
            })
            .collect();
        OcTypeGraph { nodes, edges }
    }
}
