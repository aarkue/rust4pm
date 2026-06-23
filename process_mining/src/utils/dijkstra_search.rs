//! Generic Dijkstra state space exploration
//!
//! A [`SearchProblem`] defines states, edges, and the goal state.
//! The [`search`] function finds an optimal path in the statespace.
//! Consumers (e.g. alignments) implement [`SearchProblem`] for their specific
//! state space and can then reuse [`search`].
use std::{
    collections::VecDeque,
    ops::{Add, Rem},
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Index of a node in the search (assigned in creation order).
///
/// Must be dense, i.e., no index may be skipped.
pub type NodeID = u32;

/// A state space with non-negative integer edge costs, explored by [`search`].
///
/// The problem instance owns the state storage, while the search tracks distance,
/// parent and [`Step`] per node, keyed by [`NodeID`].
///
/// New nodes get ids in creation order (see [`expand`]), without skipping an index.
///
/// [`Step`]: SearchProblem::Step
/// [`expand`]: SearchProblem::expand
pub trait SearchProblem {
    /// The edge taken to reach a node, used to reconstruct paths.
    type Step: Copy + Default;

    /// The integer type for edge and path costs.
    /// Any primitive integer (`u16`, `u32`, etc.) may be used.
    /// The type must be large enough to hold the largest path costs.
    type Cost: Copy
        + Ord
        + Add<Output = Self::Cost>
        + Rem<Output = Self::Cost>
        + From<u8>
        + TryInto<usize>;

    /// The start state; returns its id (`0`).
    fn initial(&mut self) -> NodeID;

    /// Largest possible cost of a _single_ edge.
    ///
    /// This determines the number of buckets (`num_buckets` = `max_edge_cost` + 1).
    fn max_edge_cost(&self) -> Self::Cost;

    /// Whether the given `node` is a final (goal) state.
    fn is_goal(&self, node: NodeID) -> bool;

    /// Generate the successors of `node`.
    /// For each outgoing edge, find or create the node id for the
    /// reached state and call `emit(successor, is_new, edge_cost, step)`:
    /// `is_new` is `true` when the state was newly created, and a new id must
    /// equal the current node count (no id may be skipped).
    /// `via` is the edge `node` is currently best reached by (`None` for the start)
    fn expand<F: FnMut(NodeID, bool, Self::Cost, Self::Step)>(
        &mut self,
        node: NodeID,
        via: Option<Self::Step>,
        emit: F,
    );
}

#[derive(Debug)]
struct Node<S, C> {
    /// Shortest known distance to this node from the start node
    distance: C,
    /// The parent node (from which this one is reached).
    ///
    /// For the start node, this is set to its own
    parent: NodeID,
    /// The edge fired to reach this node from its parent
    step: S,
    /// If this node is finished (final distance known and removed from the queue)
    finished: bool,
}

/// An optimal path found by [`search`]
#[derive(Debug, Clone)]
pub struct SearchResult<S, C = u32> {
    /// The edges fired from start to goal, in order
    pub path: Vec<S>,
    /// Total cost of the path
    pub cost: C,
    /// Number of states visited during search
    pub states_visited: usize,
}

/// Reason [`search`] found no path
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum SearchError {
    /// The specified maximum number of states was reached
    LimitReached,
    /// No goal state is reachable
    Unreachable,
    /// The maximum edge cost does not fit `usize`, so the bucket queue cannot be sized
    MaxEdgeCostTooLarge,
}

/// Reusable node store and bucket queue for [`search`], cleared initially.
/// Reusing it across searches avoids reallocations.
#[derive(Debug)]
pub struct SearchState<S, C = u32> {
    /// Node info, indexed using [`NodeID`]
    nodes: Vec<Node<S, C>>,
    /// Priority bucket queue.
    ///
    /// Nodes with distance `d` are scheduled in `buckets[d % len]`,
    /// where `len` is the number of buckets (max edge cost + 1)
    buckets: Vec<VecDeque<NodeID>>,
}

impl<S, C> Default for SearchState<S, C> {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            buckets: Vec::new(),
        }
    }
}

/// Search for an optimal path from the start to a goal state using Dijkstra
#[inline]
pub fn search<P: SearchProblem>(
    problem: &mut P,
    state: &mut SearchState<P::Step, P::Cost>,
    max_states: Option<usize>,
) -> Result<SearchResult<P::Step, P::Cost>, SearchError> {
    let num_buckets_cost = problem.max_edge_cost() + P::Cost::from(1);
    let num_buckets: usize = num_buckets_cost
        .try_into()
        .map_err(|_| SearchError::MaxEdgeCostTooLarge)?;
    let start = problem.initial();

    let nodes = &mut state.nodes;
    let buckets = &mut state.buckets;
    nodes.clear();
    nodes.push(Node {
        distance: P::Cost::from(0),
        parent: start,
        step: P::Step::default(),
        finished: false,
    });
    buckets.resize_with(num_buckets, VecDeque::new);
    buckets.iter_mut().for_each(VecDeque::clear);
    buckets[0].push_back(start);

    let limit = max_states.unwrap_or(usize::MAX);
    let mut queued: usize = 1;
    let mut bucket: usize = 0;
    let mut states_visited: usize = 0;

    while queued > 0 {
        while buckets[bucket].is_empty() {
            bucket = (bucket + 1) % num_buckets;
        }
        let node_id = buckets[bucket].pop_front().expect("Bucket not empty");
        queued -= 1;

        if nodes[node_id as usize].finished {
            continue;
        }
        nodes[node_id as usize].finished = true;
        states_visited += 1;
        if states_visited > limit {
            return Err(SearchError::LimitReached);
        }

        if problem.is_goal(node_id) {
            return Ok(SearchResult {
                path: reconstruct(nodes, node_id),
                cost: nodes[node_id as usize].distance,
                states_visited,
            });
        }

        let distance = nodes[node_id as usize].distance;
        let via = if nodes[node_id as usize].parent == node_id {
            None
        } else {
            Some(nodes[node_id as usize].step)
        };
        problem.expand(node_id, via, |next_id, is_new, cost, step| {
            let new_distance = distance + cost;
            // Modulo in cost space first: the remainder is `< num_buckets` so in usize.
            let idx = (new_distance % num_buckets_cost).try_into().unwrap_or(0);
            if is_new {
                debug_assert_eq!(next_id as usize, nodes.len(), "no NodeID should be skipped");
                nodes.push(Node {
                    distance: new_distance,
                    parent: node_id,
                    step,
                    finished: false,
                });
                // add it to the back of corresponding bucket
                buckets[idx].push_back(next_id);
                queued += 1;
            } else {
                let node = &mut nodes[next_id as usize];
                // node already existed before, so update it if distance is smaller.
                if !node.finished && new_distance < node.distance {
                    node.distance = new_distance;
                    node.parent = node_id;
                    node.step = step;
                    buckets[idx].push_back(next_id);
                    queued += 1;
                }
            }
        });
    }
    Err(SearchError::Unreachable)
}

/// Reconstruct a path using the steps taken
///
/// Returns an ordered sequence of steps to reach a goal node from the start node.
fn reconstruct<S: Copy, C>(nodes: &[Node<S, C>], target: NodeID) -> Vec<S> {
    let mut path = Vec::new();
    let mut current = target;
    while nodes[current as usize].parent != current {
        let node = &nodes[current as usize];
        path.push(node.step);
        current = node.parent;
    }
    path.reverse();
    path
}
