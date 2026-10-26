//! Petri nets with dense node ids
use std::collections::HashSet;

#[cfg(feature = "token-based-replay")]
use nalgebra::DMatrix;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    io::PetriNetIOError,
    marking::Marking,
    pnml::{
        export_petri_net_to_pnml_path, import_pnml_from_path, read::accepting_net, PNMLParseError,
    },
};
use crate::core::process_models::process_tree::ProcessTree;

/// Id of a place, handed out by its [`PetriNet`] in creation order
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct PlaceID(pub(crate) u32);

impl PlaceID {
    /// Position of the place in its net (`0..num_places`)
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// Id of a transition, handed out by its [`PetriNet`] in creation order
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct TransitionID(pub(crate) u32);

impl TransitionID {
    /// Position of the transition in its net (`0..num_transitions`)
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// Id of a place or a transition, to treat a [`PetriNet`] as a graph
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeID {
    /// A place
    Place(PlaceID),
    /// A transition
    Transition(TransitionID),
}

impl From<PlaceID> for NodeID {
    fn from(place: PlaceID) -> Self {
        Self::Place(place)
    }
}

impl From<TransitionID> for NodeID {
    fn from(transition: TransitionID) -> Self {
        Self::Transition(transition)
    }
}

/// Place in a [`PetriNet`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct Place {
    /// External identifier (e.g., the PNML id), not used for identity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Transitions taking tokens from this place, in id order
    #[serde(skip)]
    consumers: Vec<TransitionID>,
    /// Transitions putting tokens into this place, in id order
    #[serde(skip)]
    producers: Vec<TransitionID>,
}

/// Transition in a [`PetriNet`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct Transition {
    /// Activity label (`None` if this transition is silent)
    pub label: Option<String>,
    /// External identifier (e.g., the PNML id), not used for identity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The places this transition takes tokens from, with the number of tokens
    inputs: Vec<(PlaceID, u32)>,
    /// The places this transition puts tokens into, with the number of tokens
    outputs: Vec<(PlaceID, u32)>,
}

impl Transition {
    /// The places this transition takes tokens from, with the number of tokens
    pub fn inputs(&self) -> &[(PlaceID, u32)] {
        &self.inputs
    }

    /// The places this transition puts tokens into, with the number of tokens
    pub fn outputs(&self) -> &[(PlaceID, u32)] {
        &self.outputs
    }
}

/// Source and target of an arc
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ArcType {
    /// From a place to a transition
    PlaceTransition(PlaceID, TransitionID),
    /// From a transition to a place
    TransitionPlace(TransitionID, PlaceID),
}

impl ArcType {
    /// Arc from a place to a transition
    pub fn place_to_transition(from: PlaceID, to: TransitionID) -> ArcType {
        ArcType::PlaceTransition(from, to)
    }

    /// Arc from a transition to a place
    pub fn transition_to_place(from: TransitionID, to: PlaceID) -> ArcType {
        ArcType::TransitionPlace(from, to)
    }
}

/// Arc of a [`PetriNet`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Arc {
    /// Source and target
    pub from_to: ArcType,
    /// How many tokens the arc moves (positive)
    pub weight: u32,
}

/// A Petri net: places and transitions connected by weighted arcs
///
/// Nodes get dense ids in creation order, and everything iterates in id order. Markings are not
/// part of the net; see [`AcceptingPetriNet`] for a net with an initial and a final marking.
///
/// JSON lists the places and the transitions (with their arcs) by index. The shape of earlier
/// versions, with UUID-keyed maps and an arc list, still loads.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, JsonSchema)]
pub struct PetriNet {
    /// The places, indexed by [`PlaceID`]
    places: Vec<Place>,
    /// The transitions, indexed by [`TransitionID`]
    transitions: Vec<Transition>,
}

impl PetriNet {
    /// Create an empty net
    pub fn new() -> Self {
        Self::default()
    }

    /// Serialize to a JSON string
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("a Petri net serializes to JSON")
    }

    /// Import the structure of a net from a PNML file, ignoring its markings
    ///
    /// See [`import_pnml`](super::pnml::import_pnml) for what is read, and
    /// [`AcceptingPetriNet::import_pnml`] to keep the markings.
    pub fn import_pnml<P: AsRef<std::path::Path>>(path: P) -> Result<PetriNet, PNMLParseError> {
        Ok(import_pnml_from_path(path)?.0)
    }

    /// Export the net to a PNML file, without markings
    ///
    /// The file can be imported again with [`PetriNet::import_pnml`].
    pub fn export_pnml<P: AsRef<std::path::Path>>(&self, path: P) -> Result<(), PetriNetIOError> {
        export_petri_net_to_pnml_path(self, None, None, path)
    }

    #[cfg(feature = "visualization")]
    /// Export a drawing of the net as an SVG file, laid out with `graphlay`
    ///
    /// For visualization only: The image cannot be imported again.
    /// [`crate::visualization::svg`] returns the same SVG as a string.
    pub fn export_svg<P: AsRef<std::path::Path>>(&self, path: P) -> Result<(), std::io::Error> {
        AcceptingPetriNet::new(self.clone(), Marking::new(), Marking::new()).export_svg(path)
    }

    /// Add a place with an optional external name
    pub fn add_place(&mut self, name: Option<String>) -> PlaceID {
        let id = PlaceID(u32::try_from(self.places.len()).expect("more than u32::MAX places"));
        self.places.push(Place {
            name,
            consumers: Vec::new(),
            producers: Vec::new(),
        });
        id
    }

    /// Add a transition with a label (`None` for a silent transition) and an optional external name
    pub fn add_transition(&mut self, label: Option<String>, name: Option<String>) -> TransitionID {
        let id = TransitionID(
            u32::try_from(self.transitions.len()).expect("more than u32::MAX transitions"),
        );
        self.transitions.push(Transition {
            label,
            name,
            inputs: Vec::new(),
            outputs: Vec::new(),
        });
        id
    }

    /// Add an arc with the given weight (1 if `None`)
    ///
    /// An arc between the same place and transition in the same direction adds its weight to the
    /// existing one, and an arc of weight 0 adds nothing.
    ///
    /// Panics if a node id does not belong to this net.
    pub fn add_arc(&mut self, from_to: ArcType, weight: Option<u32>) {
        let (place, transition, is_input) = match from_to {
            ArcType::PlaceTransition(place, transition) => (place, transition, true),
            ArcType::TransitionPlace(transition, place) => (place, transition, false),
        };
        assert!(
            place.index() < self.places.len() && transition.index() < self.transitions.len(),
            "{from_to:?} refers to a node that is not in this net"
        );
        let weight = weight.unwrap_or(1);
        if weight == 0 {
            return;
        }
        let t = &mut self.transitions[transition.index()];
        let arcs = if is_input {
            &mut t.inputs
        } else {
            &mut t.outputs
        };
        if let Some((_, total)) = arcs.iter_mut().find(|(p, _)| *p == place) {
            *total = total.checked_add(weight).expect("arc weight overflows u32");
            return;
        }
        arcs.push((place, weight));
        let p = &mut self.places[place.index()];
        let neighbors = if is_input {
            &mut p.consumers
        } else {
            &mut p.producers
        };
        let position = neighbors.partition_point(|t| *t < transition);
        neighbors.insert(position, transition);
    }

    /// Number of places
    pub fn num_places(&self) -> usize {
        self.places.len()
    }

    /// Number of transitions
    pub fn num_transitions(&self) -> usize {
        self.transitions.len()
    }

    /// The place with this id (panics for an id of another net)
    pub fn place(&self, id: PlaceID) -> &Place {
        &self.places[id.index()]
    }

    /// The transition with this id (panics for an id of another net)
    pub fn transition(&self, id: TransitionID) -> &Transition {
        &self.transitions[id.index()]
    }

    /// All places, in id order
    pub fn places(&self) -> impl ExactSizeIterator<Item = (PlaceID, &Place)> {
        self.places
            .iter()
            .enumerate()
            .map(|(i, place)| (PlaceID(i as u32), place))
    }

    /// All transitions, in id order
    pub fn transitions(&self) -> impl ExactSizeIterator<Item = (TransitionID, &Transition)> {
        self.transitions
            .iter()
            .enumerate()
            .map(|(i, t)| (TransitionID(i as u32), t))
    }

    /// All places, then all transitions, in id order
    pub fn nodes(&self) -> impl Iterator<Item = NodeID> + '_ {
        self.places()
            .map(|(id, _)| NodeID::Place(id))
            .chain(self.transitions().map(|(id, _)| NodeID::Transition(id)))
    }

    /// The nodes with an arc into `node`
    pub fn preset_of(&self, node: impl Into<NodeID>) -> Vec<NodeID> {
        match node.into() {
            NodeID::Place(p) => self.preset_of_place(p).map(NodeID::Transition).collect(),
            NodeID::Transition(t) => self.preset_of_transition(t).map(NodeID::Place).collect(),
        }
    }

    /// The nodes with an arc from `node`
    pub fn postset_of(&self, node: impl Into<NodeID>) -> Vec<NodeID> {
        match node.into() {
            NodeID::Place(p) => self.postset_of_place(p).map(NodeID::Transition).collect(),
            NodeID::Transition(t) => self.postset_of_transition(t).map(NodeID::Place).collect(),
        }
    }

    /// All arcs, by transition id, each transition's inputs before its outputs
    pub fn arcs(&self) -> impl Iterator<Item = Arc> + '_ {
        self.transitions().flat_map(|(id, t)| {
            let inputs = t.inputs.iter().map(move |&(place, weight)| Arc {
                from_to: ArcType::PlaceTransition(place, id),
                weight,
            });
            let outputs = t.outputs.iter().map(move |&(place, weight)| Arc {
                from_to: ArcType::TransitionPlace(id, place),
                weight,
            });
            inputs.chain(outputs)
        })
    }

    /// The places a transition takes tokens from
    pub fn preset_of_transition(
        &self,
        t: TransitionID,
    ) -> impl ExactSizeIterator<Item = PlaceID> + '_ {
        self.transition(t).inputs.iter().map(|(place, _)| *place)
    }

    /// The places a transition puts tokens into
    pub fn postset_of_transition(
        &self,
        t: TransitionID,
    ) -> impl ExactSizeIterator<Item = PlaceID> + '_ {
        self.transition(t).outputs.iter().map(|(place, _)| *place)
    }

    /// The transitions putting tokens into a place, in id order
    pub fn preset_of_place(&self, p: PlaceID) -> impl ExactSizeIterator<Item = TransitionID> + '_ {
        self.place(p).producers.iter().copied()
    }

    /// The transitions taking tokens from a place, in id order
    pub fn postset_of_place(&self, p: PlaceID) -> impl ExactSizeIterator<Item = TransitionID> + '_ {
        self.place(p).consumers.iter().copied()
    }

    /// A place of `marking` that is not in this net, if any
    pub(crate) fn place_outside(&self, marking: &Marking) -> Option<PlaceID> {
        marking
            .iter()
            .map(|(place, _)| place)
            .find(|place| place.index() >= self.places.len())
    }

    /// Whether `transition` may fire in `marking`
    pub fn is_enabled(&self, marking: &Marking, transition: TransitionID) -> bool {
        self.transition(transition)
            .inputs
            .iter()
            .all(|&(place, weight)| marking.tokens(place) >= weight)
    }

    /// The marking after firing `transition` in `marking`, or `None` if it is not enabled
    ///
    /// Panics if a place would hold more than `u32::MAX` tokens.
    pub fn fire(&self, marking: &Marking, transition: TransitionID) -> Option<Marking> {
        if !self.is_enabled(marking, transition) {
            return None;
        }
        let t = self.transition(transition);
        let mut next = marking.clone();
        for &(place, weight) in &t.inputs {
            next.set(place, next.tokens(place) - weight);
        }
        for &(place, weight) in &t.outputs {
            next.add(place, weight);
        }
        Some(next)
    }

    /// The transitions enabled in `marking`, in id order
    pub fn enabled_transitions<'a>(
        &'a self,
        marking: &'a Marking,
    ) -> impl Iterator<Item = TransitionID> + 'a {
        self.transitions()
            .map(|(id, _)| id)
            .filter(|id| self.is_enabled(marking, *id))
    }

    /// Keep only the transitions for which `keep` returns true, with their arcs
    ///
    /// The kept transitions are renumbered in order, so transition ids obtained before no longer
    /// refer to the same transitions.
    pub fn retain_transitions(&mut self, mut keep: impl FnMut(TransitionID, &Transition) -> bool) {
        let mut index = 0;
        self.transitions.retain(|t| {
            let id = TransitionID(index);
            index += 1;
            keep(id, t)
        });
        self.link_places();
    }

    /// Remove a node and its arcs
    ///
    /// The ids of the same kind above it drop by one, so remove several nodes in descending id
    /// order or with [`PetriNet::retain_nodes`]. On an [`AcceptingPetriNet`], use
    /// [`AcceptingPetriNet::remove_node`], which keeps the markings in step.
    pub fn remove_node(&mut self, node: impl Into<NodeID>) {
        let node = node.into();
        self.retain_nodes(|other| other != node);
    }

    /// Keep only the nodes for which `keep` returns true, with the arcs between them
    ///
    /// The kept places and the kept transitions are each renumbered in order, so ids obtained
    /// before no longer refer to the same nodes. On an [`AcceptingPetriNet`], use
    /// [`AcceptingPetriNet::retain_nodes`], which keeps the markings in step.
    pub fn retain_nodes(&mut self, keep: impl FnMut(NodeID) -> bool) {
        self.retain(keep);
    }

    /// [`PetriNet::retain_nodes`], returning the new id of every place (`None` if removed)
    fn retain(&mut self, mut keep: impl FnMut(NodeID) -> bool) -> Vec<Option<PlaceID>> {
        let mut kept = 0;
        let new_ids: Vec<Option<PlaceID>> = self
            .places()
            .map(|(id, _)| {
                keep(NodeID::Place(id)).then(|| {
                    kept += 1;
                    PlaceID(kept - 1)
                })
            })
            .collect();
        let mut index = 0;
        self.places.retain(|_| {
            index += 1;
            new_ids[index - 1].is_some()
        });
        let mut index = 0;
        self.transitions.retain(|_| {
            index += 1;
            keep(NodeID::Transition(TransitionID(index - 1)))
        });
        for t in &mut self.transitions {
            for arcs in [&mut t.inputs, &mut t.outputs] {
                arcs.retain_mut(|(place, _)| match new_ids[place.index()] {
                    Some(new_id) => {
                        *place = new_id;
                        true
                    }
                    None => false,
                });
            }
        }
        self.link_places();
        new_ids
    }

    /// Record at every place the transitions consuming from and producing into it
    fn link_places(&mut self) {
        for place in &mut self.places {
            place.consumers.clear();
            place.producers.clear();
        }
        for (id, t) in (0..).map(TransitionID).zip(&self.transitions) {
            for (place, _) in &t.inputs {
                self.places[place.index()].consumers.push(id);
            }
            for (place, _) in &t.outputs {
                self.places[place.index()].producers.push(id);
            }
        }
    }

    /// Checks if the Petri net contains duplicate or silent transitions
    pub fn contains_duplicate_or_silent_transitions(&self) -> bool {
        let mut activities = HashSet::new();
        self.transitions
            .iter()
            .filter_map(|t| t.label.as_ref())
            .any(|label| !activities.insert(label))
    }

    #[cfg(feature = "token-based-replay")]
    /// The pre-incidence matrix: arc weights from places (rows) to transitions (columns)
    pub fn create_pre_incidence_matrix(&self) -> DMatrix<i64> {
        self.incidence(|t| &t.inputs)
    }

    #[cfg(feature = "token-based-replay")]
    /// The post-incidence matrix: arc weights from transitions (columns) to places (rows)
    pub fn create_post_incidence_matrix(&self) -> DMatrix<i64> {
        self.incidence(|t| &t.outputs)
    }

    #[cfg(feature = "token-based-replay")]
    /// The incidence matrix: how firing each transition (column) changes each place (row)
    pub fn create_incidence_matrix(&self) -> DMatrix<i64> {
        self.create_post_incidence_matrix() - self.create_pre_incidence_matrix()
    }

    #[cfg(feature = "token-based-replay")]
    fn incidence(&self, arcs: impl Fn(&Transition) -> &[(PlaceID, u32)]) -> DMatrix<i64> {
        let mut matrix = DMatrix::zeros(self.places.len(), self.transitions.len());
        for (column, t) in self.transitions.iter().enumerate() {
            for &(place, weight) in arcs(t) {
                matrix[(place.index(), column)] = i64::from(weight);
            }
        }
        matrix
    }
}

/// A [`PetriNet`] with an initial and a final marking
///
/// In JSON, the net's fields sit next to `initial_marking` and `final_marking`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct AcceptingPetriNet {
    /// The net
    #[serde(flatten)]
    pub net: PetriNet,
    /// The marking the net starts in
    pub initial_marking: Marking,
    /// The marking a complete run ends in
    pub final_marking: Marking,
}

impl AcceptingPetriNet {
    /// Combine a net with its initial and final marking
    pub fn new(net: PetriNet, initial_marking: Marking, final_marking: Marking) -> Self {
        Self {
            net,
            initial_marking,
            final_marking,
        }
    }

    /// Serialize to a JSON string
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("a Petri net serializes to JSON")
    }

    /// Whether `place` holds a token in the initial marking
    pub fn is_in_initial_marking(&self, place: PlaceID) -> bool {
        self.initial_marking.tokens(place) > 0
    }

    /// Whether `place` holds a token in the final marking
    pub fn is_in_final_marking(&self, place: PlaceID) -> bool {
        self.final_marking.tokens(place) > 0
    }

    /// Remove a node and its arcs, and its tokens from both markings
    ///
    /// Ids change as with [`PetriNet::remove_node`], and the markings follow them.
    pub fn remove_node(&mut self, node: impl Into<NodeID>) {
        let node = node.into();
        self.retain_nodes(|other| other != node);
    }

    /// Keep only the nodes for which `keep` returns true, with the arcs between them and the
    /// tokens on the kept places
    ///
    /// Ids change as with [`PetriNet::retain_nodes`], and the markings follow them.
    ///
    /// Panics if a marking has a place that is not in the net.
    pub fn retain_nodes(&mut self, keep: impl FnMut(NodeID) -> bool) {
        let new_ids = self.net.retain(keep);
        for marking in [&mut self.initial_marking, &mut self.final_marking] {
            *marking = marking
                .iter()
                .filter_map(|(place, tokens)| Some((new_ids[place.index()]?, tokens)))
                .collect();
        }
    }

    /// Import a net with its initial and final marking from a PNML file
    ///
    /// Fails with [`PNMLParseError::MissingInitialMarking`] if no place is marked and with
    /// [`PNMLParseError::MissingFinalMarking`] if the file has no final marking (`ProM`'s
    /// `<finalmarkings>`). See [`import_pnml`](super::pnml::import_pnml) for what is read.
    pub fn import_pnml<P: AsRef<std::path::Path>>(path: P) -> Result<Self, PNMLParseError> {
        accepting_net(import_pnml_from_path(path)?)
    }

    /// Export the net with both markings to a PNML file
    ///
    /// The file can be imported again with [`AcceptingPetriNet::import_pnml`].
    pub fn export_pnml<P: AsRef<std::path::Path>>(&self, path: P) -> Result<(), PetriNetIOError> {
        export_petri_net_to_pnml_path(
            &self.net,
            Some(&self.initial_marking),
            Some(&self.final_marking),
            path,
        )
    }

    #[cfg(feature = "visualization")]
    /// Export a drawing of the net with its markings as an SVG file, laid out with `graphlay`
    ///
    /// For visualization only: The image cannot be imported again.
    /// [`crate::visualization::svg`] returns the same SVG as a string.
    pub fn export_svg<P: AsRef<std::path::Path>>(&self, path: P) -> Result<(), std::io::Error> {
        let svg = crate::visualization::svg(self, &Default::default(), &Default::default())
            .map_err(std::io::Error::other)?;
        std::fs::write(path, svg)
    }
}

/// Creates an [`AcceptingPetriNet`] from a [`ProcessTree`]
impl From<ProcessTree> for AcceptingPetriNet {
    fn from(process_tree: ProcessTree) -> Self {
        process_tree.to_petri_net()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_dense_in_creation_order() {
        let mut net = PetriNet::new();
        let places: Vec<_> = (0..3).map(|_| net.add_place(None)).collect();
        let t = net.add_transition(Some("a".into()), Some("t_a".into()));
        assert_eq!(
            places.iter().map(|p| p.index()).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(t.index(), 0);
        assert_eq!(net.transition(t).name.as_deref(), Some("t_a"));
    }

    #[test]
    fn presets_and_postsets() {
        let mut net = PetriNet::new();
        let p1 = net.add_place(None);
        let t1 = net.add_transition(Some("Have fun".into()), None);
        let t2 = net.add_transition(Some("Sleep".into()), None);
        net.add_arc(ArcType::place_to_transition(p1, t1), None);
        net.add_arc(ArcType::transition_to_place(t2, p1), None);
        assert_eq!(net.postset_of_transition(t1).len(), 0);
        assert_eq!(net.preset_of_transition(t1).collect::<Vec<_>>(), vec![p1]);
        assert_eq!(net.postset_of_place(p1).collect::<Vec<_>>(), vec![t1]);
        assert_eq!(net.preset_of_place(p1).collect::<Vec<_>>(), vec![t2]);
        assert_eq!(net.preset_of_transition(t2).len(), 0);
    }

    #[test]
    fn parallel_arcs_merge_and_zero_weight_adds_nothing() {
        let mut net = PetriNet::new();
        let p = net.add_place(None);
        let q = net.add_place(None);
        let t = net.add_transition(None, None);
        net.add_arc(ArcType::place_to_transition(p, t), None);
        net.add_arc(ArcType::place_to_transition(p, t), Some(2));
        net.add_arc(ArcType::place_to_transition(q, t), Some(0));
        assert_eq!(net.transition(t).inputs(), &[(p, 3)]);
        assert_eq!(net.postset_of_place(p).len(), 1);
        assert_eq!(net.postset_of_place(q).len(), 0);
        assert_eq!(net.arcs().count(), 1);
    }

    #[test]
    #[should_panic]
    fn arcs_to_foreign_ids_panic() {
        let mut net = PetriNet::new();
        let t = net.add_transition(None, None);
        net.add_arc(ArcType::place_to_transition(PlaceID(5), t), None);
    }

    #[test]
    fn token_game() {
        let mut net = PetriNet::new();
        let p = net.add_place(None);
        let q = net.add_place(None);
        let t = net.add_transition(Some("a".into()), None);
        let source = net.add_transition(Some("s".into()), None);
        net.add_arc(ArcType::place_to_transition(p, t), Some(2));
        net.add_arc(ArcType::transition_to_place(t, q), None);
        net.add_arc(ArcType::transition_to_place(source, p), None);
        let one = Marking::from([(p, 1)]);
        assert!(!net.is_enabled(&one, t));
        assert_eq!(net.fire(&one, t), None);
        assert_eq!(
            net.enabled_transitions(&one).collect::<Vec<_>>(),
            vec![source]
        );
        let two = Marking::from([(p, 2)]);
        assert_eq!(
            net.enabled_transitions(&two).collect::<Vec<_>>(),
            vec![t, source]
        );
        assert_eq!(net.fire(&two, t), Some(Marking::from([(q, 1)])));
        assert_eq!(
            net.fire(&Marking::new(), source),
            Some(Marking::from([(p, 1)]))
        );
    }

    #[test]
    fn retain_transitions_renumbers() {
        let mut net = PetriNet::new();
        let p = net.add_place(None);
        for label in ["a", "b", "c"] {
            let t = net.add_transition(Some(label.into()), None);
            net.add_arc(ArcType::place_to_transition(p, t), None);
        }
        net.retain_transitions(|_, t| t.label.as_deref() != Some("b"));
        let labels: Vec<_> = net
            .transitions()
            .map(|(id, t)| (id.index(), t.label.clone().unwrap()))
            .collect();
        assert_eq!(labels, vec![(0, "a".to_string()), (1, "c".to_string())]);
        assert_eq!(
            net.postset_of_place(p).collect::<Vec<_>>(),
            vec![TransitionID(0), TransitionID(1)]
        );
    }

    /// `p0 -> t0 -> p2`, `p1 -> t0` and `p1 -> t1`
    fn two_inputs() -> (PetriNet, [PlaceID; 3], [TransitionID; 2]) {
        let mut net = PetriNet::new();
        let p = [0, 1, 2].map(|_| net.add_place(None));
        let t = [0, 1].map(|_| net.add_transition(None, None));
        net.add_arc(ArcType::place_to_transition(p[0], t[0]), None);
        net.add_arc(ArcType::place_to_transition(p[1], t[0]), None);
        net.add_arc(ArcType::transition_to_place(t[0], p[2]), None);
        net.add_arc(ArcType::place_to_transition(p[1], t[1]), None);
        (net, p, t)
    }

    #[test]
    fn graph_view() {
        let (net, p, t) = two_inputs();
        assert_eq!(
            net.nodes().collect::<Vec<_>>(),
            vec![
                NodeID::Place(p[0]),
                NodeID::Place(p[1]),
                NodeID::Place(p[2]),
                NodeID::Transition(t[0]),
                NodeID::Transition(t[1])
            ]
        );
        assert_eq!(
            net.preset_of(t[0]),
            vec![NodeID::Place(p[0]), NodeID::Place(p[1])]
        );
        assert_eq!(net.postset_of(t[0]), vec![NodeID::Place(p[2])]);
        assert_eq!(
            net.postset_of(p[1]),
            vec![NodeID::Transition(t[0]), NodeID::Transition(t[1])]
        );
        assert_eq!(
            net.preset_of(NodeID::Place(p[2])),
            vec![NodeID::Transition(t[0])]
        );
    }

    #[test]
    fn removing_a_place_drops_its_arcs_and_shifts_later_places() {
        let (mut net, p, t) = two_inputs();
        net.remove_node(p[1]);
        assert_eq!(net.num_places(), 2);
        assert_eq!(net.transition(t[0]).inputs(), &[(PlaceID(0), 1)]);
        assert_eq!(net.transition(t[0]).outputs(), &[(PlaceID(1), 1)]);
        assert_eq!(net.transition(t[1]).inputs(), &[]);
        assert_eq!(net.preset_of(PlaceID(1)), vec![NodeID::Transition(t[0])]);
    }

    #[test]
    fn removing_a_transition_drops_its_arcs() {
        let (mut net, p, t) = two_inputs();
        net.remove_node(t[0]);
        assert_eq!(net.num_transitions(), 1);
        assert_eq!(
            net.postset_of(p[1]),
            vec![NodeID::Transition(TransitionID(0))]
        );
        assert_eq!(net.postset_of(p[0]), vec![]);
        assert_eq!(net.preset_of(p[2]), vec![]);
    }

    #[test]
    fn accepting_net_keeps_markings_in_step() {
        let (net, p, _) = two_inputs();
        let mut net = AcceptingPetriNet::new(
            net,
            Marking::from([(p[0], 1), (p[1], 2)]),
            Marking::from([(p[2], 1)]),
        );
        net.remove_node(p[0]);
        assert_eq!(net.initial_marking, Marking::from([(PlaceID(0), 2)]));
        assert_eq!(net.final_marking, Marking::from([(PlaceID(1), 1)]));
        assert!(net.is_in_initial_marking(PlaceID(0)));
        assert!(!net.is_in_initial_marking(PlaceID(1)));
        assert!(net.is_in_final_marking(PlaceID(1)));
        net.retain_nodes(|node| node != NodeID::Place(PlaceID(1)));
        assert_eq!(net.final_marking, Marking::new());
        assert_eq!(net.net.num_places(), 1);
    }

    #[cfg(feature = "token-based-replay")]
    #[test]
    fn incidence_matrices_hold_weights() {
        use nalgebra::DMatrix;
        let mut net = PetriNet::new();
        let p: Vec<_> = (0..3).map(|_| net.add_place(None)).collect();
        let t: Vec<_> = ["a", "b", "c", "d"]
            .map(|label| net.add_transition(Some(label.into()), None))
            .to_vec();
        net.add_arc(ArcType::place_to_transition(p[0], t[0]), None);
        net.add_arc(ArcType::place_to_transition(p[0], t[1]), None);
        net.add_arc(ArcType::transition_to_place(t[0], p[1]), Some(2));
        net.add_arc(ArcType::transition_to_place(t[1], p[1]), None);
        net.add_arc(ArcType::place_to_transition(p[1], t[2]), None);
        net.add_arc(ArcType::transition_to_place(t[2], p[2]), None);
        net.add_arc(ArcType::transition_to_place(t[3], p[1]), None);
        net.add_arc(ArcType::place_to_transition(p[1], t[3]), None);

        let pre = DMatrix::from_row_slice(3, 4, &[1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0]);
        let post = DMatrix::from_row_slice(3, 4, &[0, 0, 0, 0, 2, 1, 0, 1, 0, 0, 1, 0]);
        assert_eq!(net.create_pre_incidence_matrix(), pre);
        assert_eq!(net.create_post_incidence_matrix(), post);
        assert_eq!(net.create_incidence_matrix(), post - pre);
    }
}
