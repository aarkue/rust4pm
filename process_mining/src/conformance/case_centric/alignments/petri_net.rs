//! Optimal alignment search: the synchronous product net as a [`SearchProblem`], solved with the
//! generic [`crate::utils::dijkstra_search`] Dijkstra.
use std::hash::Hasher;

use hashbrown::HashTable;
use rustc_hash::FxHasher;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    conformance::alignments::{
        sync_prod_net::{SyncProdNetConstructionError, SyncProdNetTransition, SyncProductNet},
        AlignmentMove, AlignmentResult,
    },
    utils::dijkstra_search::{search, NodeID, SearchError, SearchProblem, SearchState},
};

/// Alignment Error
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum AlignmentError {
    /// Search Error
    SearchError(SearchError),
    /// Constructing the synchronous product net failed
    SyncProdNetConstructionFailed(SyncProdNetConstructionError),
}

impl std::fmt::Display for AlignmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for AlignmentError {}

impl From<SyncProdNetConstructionError> for AlignmentError {
    fn from(value: SyncProdNetConstructionError) -> Self {
        Self::SyncProdNetConstructionFailed(value)
    }
}

impl From<SearchError> for AlignmentError {
    fn from(value: SearchError) -> Self {
        Self::SearchError(value)
    }
}

/// Type representing the count of tokens (e.g., in a marking)
pub type TokenCount = u8;

/// Type representing trace position
pub type TracePos = u16;

/// An edge/step of the Petri-net search: the transition fired, and whether it was a log move.
///
/// The log-move-flag allows for log-before-model-moves pruning in [`PetriNetAlignment::expand`].
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct PetriNetStep {
    /// Index of the fired transition in the sync. prod. net
    transition: u32,
    /// Whether the fired transition was a log move
    was_log_move: bool,
}

/// Reusable state storage for a Petri-net alignment search
#[derive(Debug, Default)]
pub(crate) struct PetriNetAlignmentSpace {
    /// Number of model places
    num_places: usize,
    /// Flat storage for model markings, indexed using [`NodeID`]
    markings: Vec<TokenCount>,
    /// Trace position per node, indexed using [`NodeID`]
    trace_pos: Vec<TracePos>,
    /// Index of visited states, mapping a `(marking, trace_pos)` tuple to a [`NodeID`]
    seen: HashTable<NodeID>,
    /// Current marking (re-used to reduce allocations)
    current: Vec<TokenCount>,
    /// Next marking, reached by firing a transition (re-used to reduce allocations)
    next: Vec<TokenCount>,
}

impl PetriNetAlignmentSpace {
    fn reset(&mut self, net: &SyncProductNet) {
        self.num_places = net.num_model_places;
        self.markings.clear();
        self.trace_pos.clear();
        self.seen.clear();
        self.current.resize(net.num_model_places, 0);
        self.next.resize(net.num_model_places, 0);
        self.markings
            .extend_from_slice(&net.initial_marking[..net.num_model_places]);
        self.trace_pos.push(0);
        self.add_seen(0);
    }

    #[inline]
    fn add_seen(&mut self, node: NodeID) {
        let off = node as usize * self.num_places;
        let hash = hash_state(
            &self.markings[off..off + self.num_places],
            self.trace_pos[node as usize],
        );
        let markings = &self.markings;
        let trace_pos = &self.trace_pos;
        let num_places = self.num_places;
        self.seen.insert_unique(hash, node, |other| {
            let off = *other as usize * num_places;
            hash_state(&markings[off..off + num_places], trace_pos[*other as usize])
        });
    }

    #[inline]
    fn find_seen(&self, marking: &[TokenCount], trace_position: TracePos) -> Option<NodeID> {
        let hash = hash_state(marking, trace_position);
        let num_places = self.num_places;
        self.seen
            .find(hash, |node| {
                let off = *node as usize * num_places;
                &self.markings[off..off + num_places] == marking
                    && self.trace_pos[*node as usize] == trace_position
            })
            .copied()
    }
}

/// Alignment as a [`SearchProblem`]: a state is a `(model marking, trace position)`, an edge fires
/// a sync. prod. net transition
#[derive(Debug)]
struct PetriNetAlignment<'a> {
    net: &'a SyncProductNet,
    space: &'a mut PetriNetAlignmentSpace,
}

impl<'a> PetriNetAlignment<'a> {
    /// Build the alignment problem over `net`, storing search state in `space`
    fn new(net: &'a SyncProductNet, space: &'a mut PetriNetAlignmentSpace) -> Self {
        Self { net, space }
    }
}

impl SearchProblem for PetriNetAlignment<'_> {
    type Step = PetriNetStep;
    type Cost = u32;

    fn initial(&mut self) -> NodeID {
        self.space.reset(self.net);
        0
    }

    fn max_edge_cost(&self) -> u32 {
        self.net.max_edge_cost
    }

    #[inline]
    fn is_goal(&self, node: NodeID) -> bool {
        let np = self.net.num_model_places;
        let off = node as usize * np;
        self.space.trace_pos[node as usize] == self.net.trace_length
            && self.space.markings[off..off + np] == self.net.final_marking[..np]
    }

    #[inline]
    fn expand<F: FnMut(NodeID, bool, u32, PetriNetStep)>(
        &mut self,
        node: NodeID,
        via: Option<PetriNetStep>,
        mut emit: F,
    ) {
        let net = self.net;
        let space = &mut *self.space;
        let np = space.num_places;
        let off = node as usize * np;
        let trace_pos = space.trace_pos[node as usize];
        let last_move_was_log = via.is_some_and(|s| s.was_log_move);
        space
            .current
            .copy_from_slice(&space.markings[off..off + np]);

        // Log/sync moves for the current event, then model moves (fixed ordering prunes states).
        let log_or_sync = net
            .transitions_by_trace_pos
            .get(trace_pos as usize)
            .map(|v| v.as_slice())
            .unwrap_or_default();
        // After a log move, model moves are pruned, so the range collapses to empty.
        let model_end = if last_move_was_log {
            0
        } else {
            net.num_model_trans
        };

        for trans_idx in log_or_sync.iter().copied().chain(0..model_end) {
            let trans = &net.transitions[trans_idx];
            if !is_enabled(&space.current, trans) {
                continue;
            }
            if fire_transition(&space.current, &mut space.next, trans).is_none() {
                continue;
            }
            let is_model_move = matches!(trans.move_type, AlignmentMove::ModelMove { .. });
            let new_trace_pos = if is_model_move {
                trace_pos
            } else {
                trace_pos + 1
            };
            let step = PetriNetStep {
                transition: trans_idx as u32,
                was_log_move: matches!(trans.move_type, AlignmentMove::LogMove { .. }),
            };

            let cost = trans.cost;
            match space.find_seen(&space.next, new_trace_pos) {
                Some(existing) => emit(existing, false, cost, step),
                None => {
                    let new_id = space.trace_pos.len() as NodeID;
                    space.markings.extend_from_slice(&space.next);
                    space.trace_pos.push(new_trace_pos);
                    space.add_seen(new_id);
                    emit(new_id, true, cost, step);
                }
            }
        }
    }
}

/// Compute an optimal alignment using [`search`]
pub(crate) fn align(
    net: &SyncProductNet,
    space: &mut PetriNetAlignmentSpace,
    state: &mut SearchState<PetriNetStep>,
    max_states: Option<usize>,
) -> Result<AlignmentResult, AlignmentError> {
    let mut problem = PetriNetAlignment::new(net, space);
    let res = search(&mut problem, state, max_states)?;
    Ok(AlignmentResult {
        moves: res
            .path
            .iter()
            .map(|s| net.transitions[s.transition as usize].move_type.clone())
            .collect(),
        cost: res.cost,
        states_visited: res.states_visited,
    })
}

#[inline]
/// Tests whether the given transition is enabled in the given marking
fn is_enabled(marking: &[TokenCount], trans: &SyncProdNetTransition) -> bool {
    trans
        .inputs
        .iter()
        .all(|(place, weight)| &marking[*place] >= weight)
}

#[inline]
#[must_use]
/// Fire the given transition, transforming the current marking into the `reached` marking.
///
/// Returns `None` if the reached marking would exceed `TokenCount::MAX` tokens.
/// In this case, the reached marking is considered out-of-bounds and should be pruned.
fn fire_transition(
    current: &[TokenCount],
    reached: &mut [TokenCount],
    trans: &SyncProdNetTransition,
) -> Option<()> {
    reached.copy_from_slice(current);
    for (place, weight) in &trans.inputs {
        reached[*place] -= weight;
    }
    for (place, weight) in &trans.outputs {
        // Handle overflow to prevent arriving at incorrect markings
        reached[*place] = reached[*place].checked_add(*weight)?;
    }
    Some(())
}

#[inline]
/// Hash a given state (combination of marking and trace position)
fn hash_state(marking: &[TokenCount], trace_pos: TracePos) -> u64 {
    let mut h = FxHasher::default();
    h.write(marking);
    h.write_u16(trace_pos);
    h.finish()
}
