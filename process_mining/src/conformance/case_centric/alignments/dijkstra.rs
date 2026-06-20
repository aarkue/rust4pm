//! Dijkstra state space exploration to find optimal alignments
use std::{collections::VecDeque, hash::Hasher};

use crate::conformance::alignments::{
    sync_prod_net::{SyncProdNetConstructionError, SyncProdNetTransition, SyncProductNet},
    AlignmentMove, AlignmentResult,
};
use hashbrown::HashTable;
use rustc_hash::FxHasher;
type NodeID = u32;

#[derive(Debug, Default)]
struct Node {
    /// Shortest known distance (alignment cost) to this node from the start
    distance: u32,
    /// The parent node (from which this one is reached)
    /// For initial start node, this is its own ID
    parent: NodeID,
    /// Which transition was fired in sync. prod. net to reach this node from parent
    trans_fired_from_parent: u32,
    /// Trace position (how many trace events were consumed)
    trace_pos: u16,
    /// If this node is finished (final distance known and removed from processing)
    finished: bool,
    /// Whether the last move was a log move or not (used for pruning)
    last_move_was_log: bool,
}

#[derive(Debug, Default)]
/// Dijkstra search state
pub struct DijkstraContext {
    /// Number of *model* places
    num_places: usize,
    /// Flat storage for model markings, indexed using [`NodeID`]
    markings_store: Vec<u8>,
    /// Node info, indexed using [`NodeID`]
    nodes: Vec<Node>,
    /// Index of visited states, mapping a `(marking, rank)` tuple to a `NodeID`
    seen_states: HashTable<NodeID>,
    /// Priority bucket queue. Nodes with distance `d` are scheduled in `buckets[d % len]`, where `len` is the number of buckets (max edge cost + 1)
    buckets: Vec<VecDeque<NodeID>>,
    /// Current marking (re-used to reduce allocations)
    current_marking: Vec<u8>,
    /// Next marking, reached by firing a transition (re-used to reduce allocations)
    next_marking: Vec<u8>,
}

impl DijkstraContext {
    fn init(&mut self, sp: &SyncProductNet, num_buckets: &usize) {
        self.buckets.resize_with(*num_buckets, VecDeque::new);
        self.buckets.iter_mut().for_each(|b| b.clear());
        self.num_places = sp.num_model_places;
        self.current_marking.resize(sp.num_model_places, 0);
        self.next_marking.resize(sp.num_model_places, 0);
        self.markings_store.clear();
        self.nodes.clear();
        self.seen_states.clear();
        self.markings_store
            .extend_from_slice(&sp.initial_marking[..sp.num_model_places]);
        self.nodes.push(Node::default());
        self.add_seen(NodeID::default());
        self.buckets[0].push_back(NodeID::default());
    }

    fn add_seen(&mut self, node_id: NodeID) {
        let offset = node_id as usize * self.num_places;

        let hash = hash_state(
            &self.markings_store[offset..offset + self.num_places],
            self.nodes[node_id as usize].trace_pos,
        );
        self.seen_states
            .insert_unique(hash, node_id, |other_node_id| {
                let offset = *other_node_id as usize * self.num_places;
                hash_state(
                    &self.markings_store[offset..offset + self.num_places],
                    self.nodes[*other_node_id as usize].trace_pos,
                )
            });
    }

    fn find_seen(&self, trace_position: u16, marking: &[u8]) -> Option<NodeID> {
        let hash = hash_state(marking, trace_position);
        let num_places = self.num_places;
        self.seen_states
            .find(hash, |node_id| {
                let offset = *node_id as usize * num_places;
                &self.markings_store[offset..offset + num_places] == marking
                    && self.nodes[*node_id as usize].trace_pos == trace_position
            })
            .copied()
    }
}
/// Hash a state, i.e., marking and trace position
fn hash_state(marking: &[u8], trace_pos: u16) -> u64 {
    let mut h = FxHasher::default();
    h.write(marking);
    h.write_u16(trace_pos);
    h.finish()
}

#[derive(Debug, Clone, PartialEq)]
/// Alignment Error
pub enum AlignmentError {
    /// The specified maximum number of states was reached
    MaxStatesReached,
    /// No final marking reachable
    FinalMarkingUnreachable,
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

/// Search for an optimal alignment using Dijkstra
pub fn search(
    sp: &SyncProductNet,
    context: &mut DijkstraContext,
    max_states: Option<usize>,
) -> Result<AlignmentResult, AlignmentError> {
    let max_costs = sp.transitions.iter().map(|t| t.cost).max().unwrap_or(1);
    let num_buckets = max_costs as usize + 1;
    context.init(sp, &num_buckets);
    let final_model_marking = &sp.final_marking[..sp.num_model_places];
    let limit = max_states.unwrap_or(usize::MAX);

    let mut num_states_visited: usize = 0;
    let mut queued: usize = 1;
    let mut bucket: usize = 0;

    while queued > 0 {
        // Find next non-empty bucket
        while context.buckets[bucket].is_empty() {
            bucket = (bucket + 1) % num_buckets;
        }
        let node_id = context.buckets[bucket]
            .pop_front()
            .expect("Bucket not empty");
        queued -= 1;

        if context.nodes[node_id as usize].finished {
            continue;
        }
        context.nodes[node_id as usize].finished = true;
        num_states_visited += 1;

        if num_states_visited > limit {
            return Err(AlignmentError::MaxStatesReached);
        }
        let trace_pos = context.nodes[node_id as usize].trace_pos;
        let offset = node_id as usize * context.num_places;
        context
            .current_marking
            .copy_from_slice(&context.markings_store[offset..offset + context.num_places]);
        if trace_pos == sp.trace_length && context.current_marking == final_model_marking {
            // Done! :)
            return Ok(AlignmentResult {
                moves: reconstruct(sp, &context.nodes, node_id),
                cost: context.nodes[node_id as usize].distance,
                states_visited: num_states_visited,
            });
        }

        let distance = context.nodes[node_id as usize].distance;
        let last_move_was_log = context.nodes[node_id as usize].last_move_was_log;
        // Transitions that can be fired are log or sync moves for this event, or model moves
        // We fix that log/sync moves are executed before model moves, without any semantic changes to reduce the state space
        let log_or_sync_moves = sp
            .transitions_by_trace_pos
            .get(trace_pos as usize)
            .map(|v| v.as_slice())
            .unwrap_or_default();
        let model_moves: &[usize] = if last_move_was_log {
            &[]
        } else {
            &sp.model_trans
        };

        for &trans_idx in log_or_sync_moves.iter().chain(model_moves.iter()) {
            let trans = &sp.transitions[trans_idx];
            let enabled = is_enabled(&context.current_marking, trans);
            if !enabled {
                continue;
            }
            fire_transition(&context.current_marking, &mut context.next_marking, trans);
            let new_distance = distance + trans.cost as u32;
            let new_trace_pos = if matches!(trans.move_type, AlignmentMove::ModelMove { .. }) {
                trace_pos
            } else {
                trace_pos + 1
            };
            let did_log_move = matches!(trans.move_type, AlignmentMove::LogMove { .. });

            match context.find_seen(new_trace_pos, &context.next_marking) {
                Some(existing_node_id) => {
                    let ex_node = &mut context.nodes[existing_node_id as usize];
                    if !ex_node.finished && new_distance < ex_node.distance {
                        ex_node.distance = new_distance;
                        ex_node.parent = node_id;
                        ex_node.trans_fired_from_parent = trans_idx as u32;
                        ex_node.last_move_was_log = did_log_move;
                        context.buckets[new_distance as usize % num_buckets]
                            .push_back(existing_node_id);
                        queued += 1;
                    }
                }
                None => {
                    let new_node_id = context.nodes.len() as NodeID;
                    context
                        .markings_store
                        .extend_from_slice(&context.next_marking);
                    context.nodes.push(Node {
                        distance: new_distance,
                        parent: node_id,
                        trans_fired_from_parent: trans_idx as u32,
                        trace_pos: new_trace_pos,
                        finished: false,
                        last_move_was_log: did_log_move,
                    });
                    context.add_seen(new_node_id);
                    context.buckets[new_distance as usize % num_buckets].push_back(new_node_id);
                    queued += 1;
                }
            }
        }
    }
    Err(AlignmentError::FinalMarkingUnreachable)
}

fn reconstruct(sp: &SyncProductNet, nodes: &[Node], target_id: NodeID) -> Vec<AlignmentMove> {
    let mut res = Vec::new();
    let mut current = target_id;

    // While initial node is not reached...
    while nodes[current as usize].parent != current {
        // ...push the move of the corresponding transition...
        let node = &nodes[current as usize];
        let trans = &sp.transitions[node.trans_fired_from_parent as usize];
        res.push(trans.move_type.clone());
        // ... and continue going backwards
        current = node.parent;
    }
    // We went backwards, so reverse order here (to match trace/model order)
    res.reverse();
    res
}

fn is_enabled(marking: &[u8], trans: &SyncProdNetTransition) -> bool {
    trans
        .inputs
        .iter()
        .all(|(place, weight)| &marking[*place] >= weight)
}

fn fire_transition(
    current_marking: &[u8],
    reached_marking: &mut [u8],
    trans: &SyncProdNetTransition,
) {
    reached_marking.copy_from_slice(current_marking);
    for (place, weight) in &trans.inputs {
        reached_marking[*place] -= weight;
    }
    for (place, weight) in &trans.outputs {
        reached_marking[*place] += weight;
    }
}
