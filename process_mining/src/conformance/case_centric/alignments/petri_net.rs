//! Optimal alignment search: the synchronous product net as a [`SearchProblem`], solved with the
//! generic [`crate::utils::dijkstra_search`] Dijkstra.
use std::hash::Hasher;

use hashbrown::HashTable;
use rustc_hash::FxHasher;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    conformance::alignments::{
        sync_prod_net::{Arcs, SyncProdNetConstructionError, SyncProductNet},
        AlignmentResult,
    },
    utils::dijkstra_search::{
        search_bidirectional, worth_releasing, NodeID, ReversibleSearchProblem, SearchError,
        SearchLimits, SearchProblem, SearchResult, SearchState,
    },
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
///
/// May be changed to larger types if Petri nets with more than 255 tokens in a place should be supported.
pub type TokenCount = u8;

/// Type representing trace position
pub type TracePos = u16;

/// An edge/step of the Petri-net search: the transition fired, and whether it is a log move.
///
/// The log-move-flag allows for log-before-model-moves pruning in [`PetriNetAlignment::expand`].
/// It rides in the transition index's top bit: one step is stored per search node, so its width
/// carries straight into the search's memory use.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct PetriNetStep(u32);

impl PetriNetStep {
    /// Marks a step as a log move, leaving 31 bits for the transition index
    const LOG_MOVE: u32 = 1 << 31;

    #[inline]
    fn new(transition: usize, log_move: bool) -> Self {
        debug_assert!(
            transition < Self::LOG_MOVE as usize,
            "the sync. prod. net may hold at most 2^31 transitions"
        );
        Self(transition as u32 | if log_move { Self::LOG_MOVE } else { 0 })
    }

    /// Index of the fired transition in the sync. prod. net
    #[inline]
    fn transition(self) -> usize {
        (self.0 & !Self::LOG_MOVE) as usize
    }

    /// Whether the fired transition is a log move
    #[inline]
    fn log_move(self) -> bool {
        self.0 & Self::LOG_MOVE != 0
    }
}

/// Marked places a fresh layout makes room for
const INITIAL_MAX_MARKED: usize = 4;

/// Number of places a [`PlaceSet`] can hold
pub(crate) const PLACE_SET_LIMIT: usize = u64::BITS as usize;

/// A set of places, holding only the first [`PLACE_SET_LIMIT`] of them
///
/// Places beyond that are silently left out. That only ever weakens a test, never makes it
/// wrong: the set rules candidates out, and the exact check confirms whatever survives.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PlaceSet(u64);

impl PlaceSet {
    /// Add `place`, ignoring any the set cannot hold
    #[inline]
    pub(crate) fn insert(&mut self, place: usize) {
        if place < PLACE_SET_LIMIT {
            self.0 |= 1 << place;
        }
    }

    /// Whether every place in `self` is also in `other`
    #[inline]
    pub(crate) fn contained_in(self, other: Self) -> bool {
        self.0 & !other.0 == 0
    }
}

/// The places a marking holds a token in, as one bit per place, opening every record
#[derive(Debug, Clone, Copy)]
struct MarkedPlaces<'a>(&'a [u8]);

impl MarkedPlaces<'_> {
    /// Whether `place` holds a token
    #[inline]
    fn contains(self, place: usize) -> bool {
        self.0[place / 8] >> (place % 8) & 1 == 1
    }

    /// How many marked places come before `place`, which is where its token count sits
    #[inline]
    fn count_before(self, place: usize) -> usize {
        let (group, bit) = (place / 8, place % 8);
        let preceding: u32 = self.0[..group].iter().map(|g| g.count_ones()).sum();
        (preceding + (self.0[group] & ((1 << bit) - 1)).count_ones()) as usize
    }

    /// How many places are marked
    #[inline]
    fn count(self) -> usize {
        self.0.iter().map(|group| group.count_ones() as usize).sum()
    }

    /// The marked places as a set, for testing a transition's needs in one operation
    #[inline]
    fn to_place_set(self) -> PlaceSet {
        let head = self.0.len().min(8);
        let mut bytes = [0; 8];
        bytes[..head].copy_from_slice(&self.0[..head]);
        PlaceSet(u64::from_le_bytes(bytes))
    }

    /// Visit each marked place, lowest first
    #[inline]
    fn for_each(self, mut visit: impl FnMut(usize)) {
        for (group, &bits) in self.0.iter().enumerate() {
            let mut bits = bits;
            while bits != 0 {
                visit(group * 8 + bits.trailing_zeros() as usize);
                bits &= bits - 1;
            }
        }
    }
}

/// The marked places of a record, for adding and removing one
struct MarkedPlacesMut<'a>(&'a mut [u8]);

impl MarkedPlacesMut<'_> {
    #[inline]
    fn insert(&mut self, place: usize) {
        self.0[place / 8] |= 1 << (place % 8);
    }

    #[inline]
    fn remove(&mut self, place: usize) {
        self.0[place / 8] &= !(1 << (place % 8));
    }
}

/// The nodes a search has reached, as flat fixed-width records
///
/// A record is `[bitmask of the marked places][their token counts, in place order]`, padded to
/// `stride`. Only a few places hold a token at any time, so this is far smaller than one count
/// per place. Padding is kept zeroed, so a whole record stays canonical.
#[derive(Debug, Default)]
struct NodeRecords {
    /// Number of model places
    num_places: usize,
    /// Bytes of bitmask in a record, i.e. one bit per place
    mask_bytes: usize,
    /// Most marked places a record has room for
    max_marked: usize,
    /// Bytes per record
    stride: usize,
    /// The records, indexed using [`NodeID`]
    records: Vec<u8>,
    /// Trace position per node, indexed using [`NodeID`]
    positions: Vec<TracePos>,
}

impl NodeRecords {
    /// Drop the previous search, keeping the layout unless the net changed
    fn reset(&mut self, num_places: usize) {
        if self.num_places != num_places {
            self.num_places = num_places;
            self.mask_bytes = num_places.div_ceil(8);
            self.max_marked = 0;
        }
        if worth_releasing(self.records.capacity(), self.records.len()) {
            self.records = Vec::new();
            self.positions = Vec::new();
            // Nothing left to relay out, so let this trace size the layout afresh
            self.max_marked = 0;
        } else {
            self.records.clear();
            self.positions.clear();
        }
    }

    /// Size the layout to hold a key of `longest`, keeping any width earlier traces needed
    fn fit_layout(&mut self, longest: usize) {
        self.max_marked = self
            .max_marked
            .max(longest - self.mask_bytes)
            .max(INITIAL_MAX_MARKED)
            .min(self.num_places);
        // A net without places still needs a non-zero stride to index by
        self.stride = (self.mask_bytes + self.max_marked).max(1);
    }

    #[inline]
    fn len(&self) -> usize {
        self.positions.len()
    }

    /// Encode a marking as a key.
    ///
    /// Keys are independent of the layout, so the two search directions stay comparable even
    /// when their layouts differ.
    fn encode(&self, marking: &[TokenCount], key: &mut Vec<u8>) {
        key.clear();
        key.resize(self.mask_bytes, 0);
        for (place, &tokens) in marking.iter().enumerate() {
            if tokens != 0 {
                MarkedPlacesMut(&mut key[..self.mask_bytes]).insert(place);
                key.push(tokens);
            }
        }
    }

    /// Expand `node`'s marking into one entry per place
    #[inline]
    fn decode(&self, node: NodeID, marking: &mut [TokenCount]) {
        marking.fill(0);
        let record = self.record(node);
        let mut count = self.mask_bytes;
        MarkedPlaces(&record[..self.mask_bytes]).for_each(|place| {
            marking[place] = record[count];
            count += 1;
        });
    }

    /// Take `weight` tokens from `place`, dropping it once it runs empty.
    ///
    /// The place must hold at least `weight` tokens, which enabledness has already established.
    #[inline]
    fn consume(&self, key: &mut Vec<u8>, place: usize, weight: TokenCount) {
        let slot = {
            let marked = MarkedPlaces(&key[..self.mask_bytes]);
            debug_assert!(
                marked.contains(place),
                "a transition may only consume from a marked place"
            );
            self.mask_bytes + marked.count_before(place)
        };
        key[slot] -= weight;
        if key[slot] == 0 {
            key.remove(slot);
            MarkedPlacesMut(&mut key[..self.mask_bytes]).remove(place);
        }
    }

    /// Add `weight` tokens to `place`, or `None` if it would hold more than [`TokenCount::MAX`],
    /// which puts the reached marking out of bounds
    #[inline]
    #[must_use]
    fn produce(&self, key: &mut Vec<u8>, place: usize, weight: TokenCount) -> Option<()> {
        let (slot, present) = {
            let marked = MarkedPlaces(&key[..self.mask_bytes]);
            (
                self.mask_bytes + marked.count_before(place),
                marked.contains(place),
            )
        };
        if present {
            key[slot] = key[slot].checked_add(weight)?;
        } else {
            key.insert(slot, weight);
            MarkedPlacesMut(&mut key[..self.mask_bytes]).insert(place);
        }
        Some(())
    }

    /// Append a node holding `key`
    #[inline]
    fn push(&mut self, key: &[u8], trace_pos: TracePos) {
        let padding = self.stride - key.len();
        self.records.extend_from_slice(key);
        self.records.resize(self.records.len() + padding, 0);
        self.positions.push(trace_pos);
    }

    #[inline]
    fn fits(&self, key: &[u8]) -> bool {
        key.len() <= self.stride
    }

    /// Widen the records so `key` fits, relaying out the existing ones.
    ///
    /// A state is hashed over its meaningful prefix alone, so this leaves any index valid.
    #[cold]
    fn widen_for(&mut self, key: &[u8]) {
        let max_marked = (key.len() - self.mask_bytes)
            .max(self.max_marked * 2)
            .min(self.num_places);
        let stride = self.mask_bytes + max_marked;
        // Backwards, so a record is only overwritten once it has been moved itself
        self.records.resize(self.len() * stride, 0);
        for node in (0..self.len()).rev() {
            let (from, to) = (node * self.stride, node * stride);
            self.records.copy_within(from..from + self.stride, to);
            self.records[to + self.stride..to + stride].fill(0);
        }
        self.max_marked = max_marked;
        self.stride = stride;
    }

    #[inline]
    fn record(&self, node: NodeID) -> &[u8] {
        let off = node as usize * self.stride;
        &self.records[off..off + self.stride]
    }

    #[inline]
    fn marked(&self, node: NodeID) -> MarkedPlaces<'_> {
        MarkedPlaces(&self.record(node)[..self.mask_bytes])
    }

    #[inline]
    fn trace_pos(&self, node: NodeID) -> TracePos {
        self.positions[node as usize]
    }

    /// The key `node` holds, i.e. its record without the padding
    #[inline]
    fn key_of(&self, node: NodeID) -> &[u8] {
        let record = self.record(node);
        let marked = MarkedPlaces(&record[..self.mask_bytes]).count();
        &record[..self.mask_bytes + marked]
    }

    /// Whether `node` holds the marking `key` encodes.
    ///
    /// Comparing the prefix is exact: equal bitmasks imply equally many counts, and differing
    /// ones already differ inside the bitmask.
    #[inline]
    fn holds_key(&self, node: NodeID, key: &[u8]) -> bool {
        // A record holds at most `max_marked` counts, so a longer key matches nothing
        self.fits(key) && &self.record(node)[..key.len()] == key
    }

    #[inline]
    fn hash_of(&self, node: NodeID) -> u64 {
        hash_state(self.key_of(node), self.trace_pos(node))
    }
}

/// The state a goal node must hold
#[derive(Debug, Default)]
struct Goal {
    key: Vec<u8>,
    trace_pos: TracePos,
}

/// Buffers reused across one node's expansion
#[derive(Debug, Default)]
struct Scratch {
    /// Key of the node being expanded, which every successor is built from
    expanding_key: Vec<u8>,
    /// Places that node marks, to reject transitions against
    expanding_places: PlaceSet,
    /// That node's marking with one entry per place, for the exact enabledness check
    expanding_marking: Vec<TokenCount>,
    /// Model transitions its marked places could trigger
    candidates: Vec<u32>,
    /// Key of the successor currently being built
    successor_key: Vec<u8>,
}

/// Reusable state storage for a Petri-net alignment search
#[derive(Debug, Default)]
pub(crate) struct PetriNetAlignmentSpace {
    nodes: NodeRecords,
    /// Index of visited states, mapping a `(marking, trace_pos)` tuple to a [`NodeID`]
    seen: HashTable<NodeID>,
    goal: Goal,
    scratch: Scratch,
}

impl PetriNetAlignmentSpace {
    fn reset(&mut self, net: &SyncProductNet<'_>, reverse: bool) {
        let num_places = net.model.num_places;
        // Clearing costs the whole capacity, so an oversized index is cheaper to drop
        if worth_releasing(self.seen.capacity(), self.nodes.len()) {
            self.seen = HashTable::new();
        } else {
            self.seen.clear();
        }
        self.nodes.reset(num_places);
        self.scratch.expanding_marking.resize(num_places, 0);
        // A backward search starts where a forward one ends, and aims for the other end
        let (start, start_pos, goal, goal_pos) = if reverse {
            (
                &net.model.final_marking,
                net.trace_length,
                &net.model.initial_marking,
                0,
            )
        } else {
            (
                &net.model.initial_marking,
                0,
                &net.model.final_marking,
                net.trace_length,
            )
        };
        self.nodes
            .encode(&start[..num_places], &mut self.scratch.successor_key);
        self.nodes.encode(&goal[..num_places], &mut self.goal.key);
        // The layout has to fit both ends, and is carried between traces: a trace needing a wide
        // layout suggests the next will too
        self.nodes
            .fit_layout(self.scratch.successor_key.len().max(self.goal.key.len()));
        self.goal.trace_pos = goal_pos;
        self.nodes.push(&self.scratch.successor_key, start_pos);
        let hash = hash_state(&self.scratch.successor_key, start_pos);
        self.add_seen(0, hash);
    }

    /// Record a node under the given hash of its state
    #[inline]
    fn add_seen(&mut self, node: NodeID, hash: u64) {
        let nodes = &self.nodes;
        self.seen
            .insert_unique(hash, node, |other| nodes.hash_of(*other));
    }

    #[inline]
    fn find_seen(&self, key: &[u8], trace_pos: TracePos) -> Option<NodeID> {
        self.find_seen_hashed(hash_state(key, trace_pos), key, trace_pos)
    }

    #[inline]
    fn find_seen_hashed(&self, hash: u64, key: &[u8], trace_pos: TracePos) -> Option<NodeID> {
        self.seen
            .find(hash, |node| {
                self.nodes.trace_pos(*node) == trace_pos && self.nodes.holds_key(*node, key)
            })
            .copied()
    }
}

/// Bytes one queued state costs: its search node, its `(marking, trace position)`, and its slot
/// in the `seen` index. Both directions store the same per state.
///
/// Markings are stored sparsely, so this charges the worst case of every place marked at once.
/// Real markings hold a token in a small fraction of them.
pub(crate) const fn bytes_per_state(num_places: usize) -> usize {
    SearchState::<PetriNetStep>::bytes_per_node()
        + size_of::<TracePos>()
        // A `seen` slot is a NodeID, plus hashbrown's control byte and spare capacity
        + 2 * size_of::<NodeID>()
        // A bitmask, and a count for every place
        + num_places
        + num_places.div_ceil(8)
}

/// Alignment as a [`SearchProblem`]: a state is a `(model marking, trace position)`, an edge fires
/// a sync. prod. net transition
#[derive(Debug)]
struct PetriNetAlignment<'a> {
    net: &'a SyncProductNet<'a>,
    space: &'a mut PetriNetAlignmentSpace,
    /// Search backwards, from the final marking, firing every transition in reverse
    reverse: bool,
}

impl SearchProblem for PetriNetAlignment<'_> {
    type Step = PetriNetStep;
    type Cost = u32;

    fn initial(&mut self) -> NodeID {
        self.space.reset(self.net, self.reverse);
        0
    }

    fn max_edge_cost(&self) -> u32 {
        self.net.max_edge_cost
    }

    /// Whether the whole trace is consumed and the target marking reached
    #[inline]
    fn is_goal(&self, node: NodeID) -> bool {
        self.space.nodes.trace_pos(node) == self.space.goal.trace_pos
            && self.space.nodes.holds_key(node, &self.space.goal.key)
    }

    #[inline]
    fn expand<F: FnMut(NodeID, bool, u32, PetriNetStep)>(
        &mut self,
        node: NodeID,
        via: Option<PetriNetStep>,
        mut emit: F,
    ) {
        let reverse = self.reverse;
        let net = self.net;
        let space = &mut *self.space;
        let trace_pos = space.nodes.trace_pos(node);
        let last_move_was_log = via.is_some_and(PetriNetStep::log_move);
        // Token counts are only needed where the bitmask cannot settle enabledness on its own
        if net.model.needs_token_counts {
            space
                .nodes
                .decode(node, &mut space.scratch.expanding_marking);
        }
        // Rejecting a transition then costs one test against the places this node marks
        let marked = space.nodes.marked(node);
        space.scratch.expanding_places = marked.to_place_set();
        // Successors are built from this rather than re-encoded from a dense marking
        space.scratch.expanding_key.clear();
        space
            .scratch
            .expanding_key
            .extend_from_slice(space.nodes.key_of(node));

        // Model moves worth trying: only what this node's marked places can trigger, rather than
        // every transition in the net. Taken out of the space so the loop below may borrow it.
        let mut candidates = std::mem::take(&mut space.scratch.candidates);
        candidates.clear();
        // After a log move, model moves are pruned, so nothing is gathered at all. Log and model
        // moves commute at equal cost, so each side still finds exact distances. Backward, the
        // rule keeps the opposite order, and where the sides meet the commuted path is allowed by
        // both, so the meeting cost stays optimal.
        if !last_move_was_log {
            let triggers = if reverse {
                &net.model.backward_triggers
            } else {
                &net.model.forward_triggers
            };
            candidates.extend_from_slice(triggers.consuming_nothing());
            marked.for_each(|place| candidates.extend_from_slice(triggers.triggered_by(place)));
        }

        // Log/sync moves for the current event, then model moves (fixed ordering prunes states).
        // The two commute at equal cost, so keeping one order per pair loses no shortest path.
        // Going backwards, the event to consume is the one before the current position.
        let event = if reverse {
            trace_pos.checked_sub(1)
        } else {
            Some(trace_pos)
        };
        let log_or_sync = event
            .map(|event| net.moves_at(event as usize))
            .unwrap_or_default();
        for trans_idx in log_or_sync
            .iter()
            .chain(candidates.iter())
            .map(|index| *index as usize)
        {
            let trans = net.transition(trans_idx);
            // Every place a transition consumes has to hold a token, so a place it consumes that
            // is unmarked rules it out before its inputs are ever walked
            let (consumed_places, marked_suffice) = if reverse {
                (trans.output_places, trans.marked_outputs_suffice)
            } else {
                (trans.input_places, trans.marked_inputs_suffice)
            };
            if !consumed_places.contained_in(space.scratch.expanding_places) {
                continue;
            }
            let (inputs, outputs) = net.arcs(trans);
            let (consumed, produced) = if reverse {
                (outputs, inputs)
            } else {
                (inputs, outputs)
            };
            // Where every consumed place takes one token and fits the word, the test above has
            // already established enabledness; otherwise the counts have to be checked
            if !marked_suffice && !is_enabled(&space.scratch.expanding_marking, consumed) {
                continue;
            }
            space.scratch.successor_key.clear();
            space
                .scratch
                .successor_key
                .extend_from_slice(&space.scratch.expanding_key);
            for (place, weight) in consumed {
                space
                    .nodes
                    .consume(&mut space.scratch.successor_key, *place, *weight);
            }
            if produced.iter().any(|(place, weight)| {
                space
                    .nodes
                    .produce(&mut space.scratch.successor_key, *place, *weight)
                    .is_none()
            }) {
                continue;
            }
            // A model move leaves the trace position alone; log and sync moves consume an event
            let new_trace_pos = if net.is_model_transition(trans_idx) {
                trace_pos
            } else if reverse {
                trace_pos - 1
            } else {
                trace_pos + 1
            };
            let step = PetriNetStep::new(trans_idx, trans.is_log_move());
            let cost = trans.cost;
            // One hash for both the lookup and a possible insert
            let hash = hash_state(&space.scratch.successor_key, new_trace_pos);
            match space.find_seen_hashed(hash, &space.scratch.successor_key, new_trace_pos) {
                Some(existing) => emit(existing, false, cost, step),
                None => {
                    if !space.nodes.fits(&space.scratch.successor_key) {
                        space.nodes.widen_for(&space.scratch.successor_key);
                    }
                    let new_id = space.nodes.len() as NodeID;
                    space
                        .nodes
                        .push(&space.scratch.successor_key, new_trace_pos);
                    space.add_seen(new_id, hash);
                    emit(new_id, true, cost, step);
                }
            }
        }
        space.scratch.candidates = candidates;
    }
}

impl ReversibleSearchProblem for PetriNetAlignment<'_> {
    #[inline]
    fn find_in(&self, node: NodeID, other: &Self) -> Option<NodeID> {
        other.space.find_seen(
            self.space.nodes.key_of(node),
            self.space.nodes.trace_pos(node),
        )
    }
}

/// Search spaces reused across traces, one per search direction
#[derive(Debug, Default)]
pub(crate) struct AlignmentContext {
    forward: (PetriNetAlignmentSpace, SearchState<PetriNetStep>),
    backward: (PetriNetAlignmentSpace, SearchState<PetriNetStep>),
}

/// Compute an optimal alignment, searching from the initial and the final marking at once
pub(crate) fn align(
    net: &SyncProductNet<'_>,
    ctx: &mut AlignmentContext,
    limits: SearchLimits,
) -> Result<AlignmentResult, AlignmentError> {
    let mut forward = PetriNetAlignment {
        net,
        space: &mut ctx.forward.0,
        reverse: false,
    };
    let mut backward = PetriNetAlignment {
        net,
        space: &mut ctx.backward.0,
        reverse: true,
    };
    let res = search_bidirectional(
        &mut forward,
        &mut backward,
        &mut ctx.forward.1,
        &mut ctx.backward.1,
        limits,
    )?;
    Ok(result_from(net, res))
}

/// Compute an optimal alignment searching forward only, as a reference for the bidirectional one
#[cfg(test)]
pub(crate) fn align_forward(
    net: &SyncProductNet<'_>,
    limits: SearchLimits,
) -> Result<AlignmentResult, AlignmentError> {
    let mut space = PetriNetAlignmentSpace::default();
    let mut problem = PetriNetAlignment {
        net,
        space: &mut space,
        reverse: false,
    };
    let res =
        crate::utils::dijkstra_search::search(&mut problem, &mut SearchState::default(), limits)?;
    Ok(result_from(net, res))
}

fn result_from(net: &SyncProductNet<'_>, res: SearchResult<PetriNetStep>) -> AlignmentResult {
    AlignmentResult {
        moves: res
            .path
            .iter()
            .map(|s| net.transition(s.transition()).move_type.clone())
            .collect(),
        cost: res.cost,
        states_visited: res.states_visited,
    }
}

/// Tests whether the given places all hold the tokens a transition would take from them
#[inline]
fn is_enabled(marking: &[TokenCount], consumed: Arcs<'_>) -> bool {
    consumed
        .iter()
        .all(|(place, weight)| &marking[*place] >= weight)
}

/// Hash a given state (combination of encoded marking and trace position)
#[inline]
fn hash_state(key: &[u8], trace_pos: TracePos) -> u64 {
    let mut h = FxHasher::default();
    h.write(key);
    h.write_u16(trace_pos);
    h.finish()
}

#[cfg(test)]
mod test {
    use super::{MarkedPlaces, MarkedPlacesMut, PetriNetStep, PlaceSet};

    /// A step is stored once per search node, so its width drives the search's memory use
    #[test]
    fn step_stays_four_bytes() {
        assert_eq!(size_of::<PetriNetStep>(), 4);
    }

    #[test]
    fn step_round_trips() {
        for transition in [0, 1, 42, (1 << 31) - 1] {
            for log_move in [false, true] {
                let step = PetriNetStep::new(transition, log_move);
                assert_eq!(step.transition(), transition);
                assert_eq!(step.log_move(), log_move);
            }
        }
    }

    #[test]
    fn marked_places_locate_their_counts() {
        // Places 1, 9 and 17 marked, so their counts sit at 0, 1 and 2
        let mut bytes = [0u8; 3];
        for place in [1, 9, 17] {
            MarkedPlacesMut(&mut bytes).insert(place);
        }
        let marked = MarkedPlaces(&bytes);
        assert_eq!(marked.count(), 3);
        for (position, place) in [1, 9, 17].into_iter().enumerate() {
            assert!(marked.contains(place));
            assert_eq!(marked.count_before(place), position);
        }
        for place in [0, 2, 8, 16, 23] {
            assert!(!marked.contains(place));
        }
        let mut visited = Vec::new();
        marked.for_each(|place| visited.push(place));
        assert_eq!(visited, vec![1, 9, 17]);

        MarkedPlacesMut(&mut bytes).remove(9);
        let marked = MarkedPlaces(&bytes);
        assert!(!marked.contains(9));
        assert_eq!(
            marked.count_before(17),
            1,
            "unmarking a place shifts later counts down"
        );
    }

    #[test]
    fn place_set_tests_containment() {
        let mut consumed = PlaceSet::default();
        consumed.insert(2);
        consumed.insert(5);
        let mut marked = PlaceSet::default();
        marked.insert(2);
        assert!(!consumed.contained_in(marked), "place 5 is not marked");
        marked.insert(5);
        assert!(consumed.contained_in(marked));
        // Places the set cannot hold are dropped, which only weakens the test
        let mut beyond = PlaceSet::default();
        beyond.insert(super::PLACE_SET_LIMIT);
        assert!(beyond.contained_in(PlaceSet::default()));
    }
}
