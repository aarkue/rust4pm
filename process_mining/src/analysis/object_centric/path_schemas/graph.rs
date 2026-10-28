use crate::core::event_data::object_centric::linked_ocel::{
    slim_linked_ocel::{EventOrObjectIndex, SlimLinkedOCEL},
    LinkedOCELAccess,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::core::event_data::object_centric::type_graph::{OcTypeGraph, TypeKind};

/// Qualifier for E2O or O2O relations.
pub type Qualifier = String;
/// Type name (event type or object type).
pub type TypeName = String;

/// A reference to a concrete entity in the OCEL: an event or object index.
pub type EntityRef = EventOrObjectIndex;

/// Whether a node in the type graph is an event type or an object type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum EntityKind {
    /// An event type node.
    Event,
    /// An object type node.
    Object,
}

/// A reference to an OCEL type: an event type or an object type, by name.
///
/// Type-level analogue of [`EntityRef`] (which references an instance). Event and object
/// types live in separate namespaces, so the same name can denote both; carrying the kind
/// here keeps them distinct everywhere a type is named.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
pub enum TypeRef {
    /// An event type with the given name.
    Event(TypeName),
    /// An object type with the given name.
    Object(TypeName),
}

impl TypeRef {
    /// The type name (without the kind).
    pub fn name(&self) -> &str {
        match self {
            TypeRef::Event(name) | TypeRef::Object(name) => name,
        }
    }

    /// Whether this is an event type or an object type.
    pub fn kind(&self) -> EntityKind {
        match self {
            TypeRef::Event(_) => EntityKind::Event,
            TypeRef::Object(_) => EntityKind::Object,
        }
    }

    /// Whether this is an event type.
    pub fn is_event(&self) -> bool {
        matches!(self, TypeRef::Event(_))
    }
}

impl std::fmt::Display for TypeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A directed, typed edge in the type graph (a qualified E2O or O2O relationship type).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct TypeEdge {
    /// Source type of the edge.
    pub source: TypeRef,
    /// Target type of the edge.
    pub target: TypeRef,
    /// Relationship qualifier this edge represents.
    pub qualifier: Qualifier,
}

impl std::fmt::Display for TypeEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prefix = |kind| match kind {
            EntityKind::Event => "E",
            EntityKind::Object => "O",
        };
        write!(
            f,
            "{}:{} --[{}]--> {}:{}",
            prefix(self.source.kind()),
            self.source.name(),
            self.qualifier,
            prefix(self.target.kind()),
            self.target.name()
        )
    }
}

/// The type graph (schema graph): a compact, type-level view of the OCEL data graph.
///
/// Nodes are event/object types, edges are qualified E2O and O2O relationship types.
#[derive(Debug, Clone)]
pub struct TypeGraph {
    /// All event type names.
    pub event_types: Vec<TypeName>,
    /// All object type names.
    pub object_types: Vec<TypeName>,
    /// All typed edges of the graph.
    pub edges: Vec<TypeEdge>,
    /// Forward adjacency: type -> indices into [`TypeGraph::edges`].
    pub adj: HashMap<TypeRef, Vec<usize>>,
    /// Reverse adjacency: type -> indices into [`TypeGraph::edges`].
    pub rev_adj: HashMap<TypeRef, Vec<usize>>,
}

impl TypeGraph {
    /// Build the type graph from a [`SlimLinkedOCEL`].
    pub fn from_linked_ocel(ocel: &SlimLinkedOCEL) -> Self {
        Self::from(&OcTypeGraph::from_linked_ocel(ocel))
    }
}

impl From<&OcTypeGraph> for TypeGraph {
    fn from(graph: &OcTypeGraph) -> Self {
        let names = |kind: TypeKind| -> Vec<TypeName> {
            graph
                .nodes
                .iter()
                .filter(|n| n.kind == kind)
                .map(|n| n.label.clone())
                .collect()
        };
        let event_types = names(TypeKind::Event);
        let object_types = names(TypeKind::Object);
        let type_ref = |index: usize| {
            let node = &graph.nodes[index];
            match node.kind {
                TypeKind::Event => TypeRef::Event(node.label.clone()),
                TypeKind::Object => TypeRef::Object(node.label.clone()),
            }
        };
        let mut edges: Vec<TypeEdge> = graph
            .edges
            .iter()
            .map(|e| TypeEdge {
                source: type_ref(e.source),
                target: type_ref(e.target),
                qualifier: e.qualifier.clone().unwrap_or_default(),
            })
            .collect();
        // Deterministic order so schema enumeration indices are reproducible.
        edges.sort_by(|a, b| {
            (&a.source, &a.target, &a.qualifier).cmp(&(&b.source, &b.target, &b.qualifier))
        });

        let mut adj: HashMap<TypeRef, Vec<usize>> = HashMap::new();
        let mut rev_adj: HashMap<TypeRef, Vec<usize>> = HashMap::new();
        for (i, edge) in edges.iter().enumerate() {
            adj.entry(edge.source.clone()).or_default().push(i);
            rev_adj.entry(edge.target.clone()).or_default().push(i);
        }

        TypeGraph {
            event_types,
            object_types,
            edges,
            adj,
            rev_adj,
        }
    }
}

impl TypeGraph {
    /// Edge indices reachable from a type, both forward and reverse (undirected traversal).
    /// Yields `(edge_index, neighbor_type, is_reverse)`.
    pub fn neighbors_undirected(&self, type_ref: &TypeRef) -> Vec<(usize, &TypeRef, bool)> {
        let forward = self
            .adj
            .get(type_ref)
            .into_iter()
            .flatten()
            .map(|&idx| (idx, &self.edges[idx].target, false));
        let reverse = self
            .rev_adj
            .get(type_ref)
            .into_iter()
            .flatten()
            .map(|&idx| (idx, &self.edges[idx].source, true));
        forward.chain(reverse).collect()
    }
}

/// Get the type name of an entity reference.
pub fn entity_type<'a>(ocel: &'a SlimLinkedOCEL, entity: &EntityRef) -> &'a str {
    match entity {
        EntityRef::Event(e) => ocel.get_ev_type_of(e),
        EntityRef::Object(o) => ocel.get_ob_type_of(o),
    }
}

/// Get the id string of an entity reference.
pub fn entity_id<'a>(ocel: &'a SlimLinkedOCEL, entity: &EntityRef) -> &'a str {
    match entity {
        EntityRef::Event(e) => ocel.get_ev_id(e),
        EntityRef::Object(o) => ocel.get_ob_id(o),
    }
}

/// Get the neighbors of an entity reachable via a typed edge (forward or reverse).
pub fn get_neighbors_via_edge(
    ocel: &SlimLinkedOCEL,
    entity: &EntityRef,
    type_edge: &TypeEdge,
    reverse: bool,
) -> Vec<EntityRef> {
    let mut result = Vec::new();
    if !reverse {
        match (entity, type_edge.source.kind(), type_edge.target.kind()) {
            (EntityRef::Event(ev), EntityKind::Event, EntityKind::Object) => {
                for (qual, ob) in ocel.get_e2o_of_type(ev, type_edge.target.name()) {
                    if qual == type_edge.qualifier {
                        result.push(EntityRef::Object(*ob));
                    }
                }
            }
            (EntityRef::Object(ob), EntityKind::Object, EntityKind::Object) => {
                for (qual, target_ob) in ocel.get_o2o_of_type(ob, type_edge.target.name()) {
                    if qual == type_edge.qualifier {
                        result.push(EntityRef::Object(*target_ob));
                    }
                }
            }
            _ => {}
        }
    } else {
        match (entity, type_edge.target.kind(), type_edge.source.kind()) {
            (EntityRef::Object(ob), EntityKind::Object, EntityKind::Event) => {
                for (qual, ev) in ocel.get_e2o_rev_of_type(ob, type_edge.source.name()) {
                    if qual == type_edge.qualifier {
                        result.push(EntityRef::Event(*ev));
                    }
                }
            }
            (EntityRef::Object(ob), EntityKind::Object, EntityKind::Object) => {
                for (qual, source_ob) in ocel.get_o2o_rev_of_type(ob, type_edge.source.name()) {
                    if qual == type_edge.qualifier {
                        result.push(EntityRef::Object(*source_ob));
                    }
                }
            }
            _ => {}
        }
    }
    result
}
