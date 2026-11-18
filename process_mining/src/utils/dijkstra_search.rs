//! Generic Dijkstra state space exploration
//!
//! A [`SearchProblem`] defines states, edges, and the goal state.
//! The [`search`] function finds an optimal path in the statespace.
//! Consumers (e.g. alignments) implement [`SearchProblem`] for their specific
//! state space and can then reuse [`search`].
use std::{
    cell::Cell,
    collections::VecDeque,
    ops::Add,
    sync::atomic::{AtomicBool, Ordering},
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
    type Cost: Copy + Ord + Add<Output = Self::Cost> + From<u8> + TryInto<usize>;

    /// The start state; returns its id (`0`).
    fn initial(&mut self) -> NodeID;

    /// Largest possible cost of a _single_ edge.
    ///
    /// This determines the number of buckets (`num_buckets` = `max_edge_cost` + 1). If they do
    /// not fit [`SearchLimits::max_memory`], the search fails with
    /// [`SearchError::MemoryLimitReached`].
    fn max_edge_cost(&self) -> Self::Cost;

    /// Whether the given `node` is a final (goal) state.
    fn is_goal(&self, node: NodeID) -> bool;

    /// Generate the successors of `node`.
    /// For each outgoing edge, find or create the node id for the
    /// reached state and call `emit(successor, is_new, edge_cost, step)`:
    /// `is_new` is `true` when the state was newly created, and a new id must
    /// equal the current node count (no id may be skipped).
    /// `via` is the edge `node` is currently best reached by (`None` for the start).
    ///
    /// Buffers that grow with the search may only grow through `budget`. Once it is exhausted,
    /// stop expanding: the search then ends with [`SearchError::MemoryLimitReached`].
    fn expand<F: FnMut(NodeID, bool, Self::Cost, Self::Step)>(
        &mut self,
        node: NodeID,
        via: Option<Self::Step>,
        budget: &MemoryBudget,
        emit: F,
    );

    /// Bytes the problem's buffers hold (their capacity), counted against the budget
    fn allocated_bytes(&self) -> usize;
}

/// A [`SearchProblem`] that can also be stated backwards, from the goal towards the start.
///
/// The reversed instance starts at the goal and must expand the reversed edges at equal cost.
pub trait ReversibleSearchProblem: SearchProblem {
    /// The node of `other` holding the same state as `node` does here, if `other` reached it
    fn find_in(&self, node: NodeID, other: &Self) -> Option<NodeID>;
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
    /// The search would have needed more memory than [`SearchLimits::max_memory`]
    MemoryLimitReached,
    /// No goal state is reachable
    Unreachable,
    /// The maximum edge cost does not fit `usize`, so the bucket queue cannot be sized
    MaxEdgeCostTooLarge,
    /// The [`SearchLimits::cancel`] flag was set
    Cancelled,
}

impl std::fmt::Display for SearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SearchError::LimitReached => "search reached its maximum number of states",
            SearchError::MemoryLimitReached => "search reached its memory limit",
            SearchError::Unreachable => "no goal state is reachable",
            SearchError::MaxEdgeCostTooLarge => "maximum edge cost does not fit usize",
            SearchError::Cancelled => "search was cancelled",
        })
    }
}

impl std::error::Error for SearchError {}

/// How far a search may go before it gives up (`None` means no limit)
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchLimits<'a> {
    /// Maximum number of states to visit (bounds runtime). Shared by both search directions.
    pub max_states: Option<usize>,
    /// Maximum bytes the search may hold (bounds memory). Shared by both directions.
    pub max_memory: Option<usize>,
    /// Stops the search with [`SearchError::Cancelled`] once set. Checked once per visited
    /// state.
    pub cancel: Option<&'a AtomicBool>,
}

impl SearchLimits<'_> {
    fn cancelled(&self) -> bool {
        self.cancel.is_some_and(|c| c.load(Ordering::Relaxed))
    }
}

impl PartialEq for SearchLimits<'_> {
    /// Cancel flags are equal if they are the same flag
    fn eq(&self, other: &Self) -> bool {
        self.max_states == other.max_states
            && self.max_memory == other.max_memory
            && self.cancel.map(std::ptr::from_ref) == other.cancel.map(std::ptr::from_ref)
    }
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

impl<S, C> SearchState<S, C> {
    /// Bytes held by the node store and the bucket queue (their capacity)
    pub fn allocated_bytes(&self) -> usize {
        self.nodes.capacity() * size_of::<Node<S, C>>()
            + self.buckets.capacity() * size_of::<VecDeque<NodeID>>()
            + self
                .buckets
                .iter()
                .map(|bucket| bucket.capacity() * size_of::<NodeID>())
                .sum::<usize>()
    }
}

impl<S, C> Default for SearchState<S, C> {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            buckets: Vec::new(),
        }
    }
}

/// How many times larger than the last search a buffer may be before it is given back, so one
/// large search does not leave its thread holding that much for every search after it
const RELEASE_ABOVE_MULTIPLE: usize = 8;

/// Buffer length (in elements) always worth keeping, so small searches never pay to reallocate
const ALWAYS_KEEP_LEN: usize = 4096;

/// Whether a reused buffer of `capacity` is too large to keep after a search that used `used`
#[inline]
pub(crate) fn worth_releasing(capacity: usize, used: usize) -> bool {
    capacity > RELEASE_ABOVE_MULTIPLE * used.max(ALWAYS_KEEP_LEN)
}

/// The bytes one search may hold, and how many it holds so far.
///
/// Buffers that grow with the search ask it first. A growing buffer is copied into a larger one
/// before the old one is freed, so for a moment both count.
#[derive(Debug)]
pub struct MemoryBudget {
    limit: usize,
    used: Cell<usize>,
    exhausted: Cell<bool>,
}

impl MemoryBudget {
    /// A budget of `limit` bytes (`None` means no limit)
    pub fn new(limit: Option<usize>) -> Self {
        Self {
            limit: limit.unwrap_or(usize::MAX),
            used: Cell::new(0),
            exhausted: Cell::new(false),
        }
    }

    /// Bytes held so far
    pub fn used(&self) -> usize {
        self.used.get()
    }

    /// Whether a growth was refused, which ends the search
    pub fn exhausted(&self) -> bool {
        self.exhausted.get()
    }

    /// Bytes a new allocation may take while everything held so far stays allocated
    fn available(&self) -> usize {
        self.limit.saturating_sub(self.used.get())
    }

    /// Replace an allocation of `from` bytes by one of `to` bytes, if `to` fits next to
    /// everything held so far. Otherwise marks the budget as exhausted.
    pub fn grow(&self, from: usize, to: usize) -> bool {
        if to > self.available() {
            return self.refuse();
        }
        self.settle(from, to);
        true
    }

    /// Mark the budget as exhausted, returning false
    fn refuse(&self) -> bool {
        self.exhausted.set(true);
        false
    }

    /// Record that an allocation of `from` bytes now has `to` bytes, once the exact size is known
    pub fn settle(&self, from: usize, to: usize) {
        self.used.set(self.used.get() - from + to);
    }
}

/// Capacity a full buffer grows to: double, or else the most that still fits (at least `needed`)
fn grown_capacity(
    capacity: usize,
    needed: usize,
    elem_size: usize,
    budget: &MemoryBudget,
) -> Option<usize> {
    let doubled = capacity.saturating_mul(2).max(needed).max(4);
    let fits = |elements: usize| {
        elements
            .checked_mul(elem_size)
            .is_some_and(|bytes| bytes <= budget.available())
    };
    if fits(doubled) {
        return Some(doubled);
    }
    let most = budget
        .available()
        .checked_div(elem_size)
        .unwrap_or(usize::MAX);
    (most >= needed).then_some(most)
}

/// Make room for `additional` more elements within `budget`. Returns false (and marks the budget
/// as exhausted) if they do not fit.
pub(crate) fn reserve_vec<T>(vec: &mut Vec<T>, additional: usize, budget: &MemoryBudget) -> bool {
    let (len, capacity) = (vec.len(), vec.capacity());
    reserve(len, capacity, additional, size_of::<T>(), budget, |grown| {
        vec.reserve_exact(grown - len);
        vec.capacity()
    })
}

/// [`reserve_vec`] for a [`VecDeque`]
pub(crate) fn reserve_deque<T>(
    deque: &mut VecDeque<T>,
    additional: usize,
    budget: &MemoryBudget,
) -> bool {
    let (len, capacity) = (deque.len(), deque.capacity());
    reserve(len, capacity, additional, size_of::<T>(), budget, |grown| {
        deque.reserve_exact(grown - len);
        deque.capacity()
    })
}

/// Grow a buffer of `len` elements in `capacity` to hold `additional` more, where `grow_to` grows
/// it to the given capacity and returns the capacity it got
fn reserve(
    len: usize,
    capacity: usize,
    additional: usize,
    elem_size: usize,
    budget: &MemoryBudget,
    grow_to: impl FnOnce(usize) -> usize,
) -> bool {
    let Some(needed) = len.checked_add(additional) else {
        return budget.refuse();
    };
    if needed <= capacity {
        return true;
    }
    let Some(grown) = grown_capacity(capacity, needed, elem_size, budget) else {
        return budget.refuse();
    };
    let got = grow_to(grown);
    budget.settle(capacity * elem_size, got * elem_size);
    true
}

/// Search for an optimal path from the start to a goal state using Dijkstra
#[inline]
pub fn search<P: SearchProblem>(
    problem: &mut P,
    state: &mut SearchState<P::Step, P::Cost>,
    limits: SearchLimits<'_>,
) -> Result<SearchResult<P::Step, P::Cost>, SearchError> {
    let start = problem.initial();
    let budget = MemoryBudget::new(limits.max_memory);
    let num_buckets = num_buckets(problem.max_edge_cost(), 1, &budget)?;
    let mut side = SearchSide::seed(state, num_buckets, start)?;
    if !budget.grow(0, side.state.allocated_bytes() + problem.allocated_bytes()) {
        return Err(SearchError::MemoryLimitReached);
    }

    let limit = limits.max_states.unwrap_or(usize::MAX);
    let mut states_visited: usize = 0;
    let mut improved = Vec::new();

    while let Some((distance, node)) = side.peek() {
        side.finish(node);
        if limits.cancelled() {
            return Err(SearchError::Cancelled);
        }
        states_visited += 1;
        if states_visited > limit {
            return Err(SearchError::LimitReached);
        }
        if problem.is_goal(node) {
            return Ok(SearchResult {
                path: reconstruct(&side.state.nodes, node),
                cost: distance,
                states_visited,
            });
        }
        side.expand(problem, node, distance, &mut improved, &budget);
        if budget.exhausted() {
            return Err(SearchError::MemoryLimitReached);
        }
        improved.clear();
    }
    Err(SearchError::Unreachable)
}

/// Search from both ends at once, meeting in the middle.
///
/// `backward` must be the same problem reversed (see [`ReversibleSearchProblem`]).
///
/// When one side reaches a state the other already reached, their two halves form a full path.
/// The cheapest one so far is kept, but a later meeting may still beat it. Any path left to find
/// costs at least the two sides' next-cheapest distances combined, so the search stops once that
/// sum reaches the kept cost.
pub fn search_bidirectional<P: ReversibleSearchProblem>(
    forward: &mut P,
    backward: &mut P,
    forward_state: &mut SearchState<P::Step, P::Cost>,
    backward_state: &mut SearchState<P::Step, P::Cost>,
    limits: SearchLimits<'_>,
) -> Result<SearchResult<P::Step, P::Cost>, SearchError> {
    let start = forward.initial();
    let goal = backward.initial();
    let budget = MemoryBudget::new(limits.max_memory);
    let num_buckets = num_buckets(forward.max_edge_cost(), 2, &budget)?;
    let mut forward_side = SearchSide::seed(forward_state, num_buckets, start)?;
    let mut backward_side = SearchSide::seed(backward_state, num_buckets, goal)?;
    let held = forward_side.state.allocated_bytes()
        + backward_side.state.allocated_bytes()
        + forward.allocated_bytes()
        + backward.allocated_bytes();
    if !budget.grow(0, held) {
        return Err(SearchError::MemoryLimitReached);
    }

    // Cheapest meeting so far, as (cost, node of `forward`, node of `backward`)
    let mut best = forward
        .find_in(start, backward)
        .map(|other| (P::Cost::from(0), start, other));

    let limit = limits.max_states.unwrap_or(usize::MAX);
    let mut states_visited: usize = 0;
    let mut improved = Vec::new();
    // If either side runs dry, no meeting can be left to find
    while let (Some((forward_top, forward_node)), Some((backward_top, backward_node))) =
        (forward_side.peek(), backward_side.peek())
    {
        if best.is_some_and(|(cost, _, _)| forward_top + backward_top >= cost) {
            break;
        }
        if limits.cancelled() {
            return Err(SearchError::Cancelled);
        }
        states_visited += 1;
        if states_visited > limit {
            return Err(SearchError::LimitReached);
        }
        // Advance whichever side has explored less, so both grow at a similar rate. Choosing the
        // side with the cheaper next node instead stalls one side for as long as the other keeps
        // finding zero-cost edges.
        if forward_side.queued <= backward_side.queued {
            forward_side.finish(forward_node);
            forward_side.expand(forward, forward_node, forward_top, &mut improved, &budget);
            if budget.exhausted() {
                return Err(SearchError::MemoryLimitReached);
            }
            meet(
                forward,
                backward,
                &backward_side,
                &mut improved,
                &mut best,
                false,
            );
        } else {
            backward_side.finish(backward_node);
            backward_side.expand(
                backward,
                backward_node,
                backward_top,
                &mut improved,
                &budget,
            );
            if budget.exhausted() {
                return Err(SearchError::MemoryLimitReached);
            }
            meet(
                backward,
                forward,
                &forward_side,
                &mut improved,
                &mut best,
                true,
            );
        }
    }

    let Some((cost, forward_node, backward_node)) = best else {
        return Err(SearchError::Unreachable);
    };
    let mut path = reconstruct(&forward_side.state.nodes, forward_node);
    let mut from_goal = reconstruct(&backward_side.state.nodes, backward_node);
    // Both halves end at the meeting state, but the second was walked from the goal
    from_goal.reverse();
    path.extend(from_goal);
    Ok(SearchResult {
        path,
        cost,
        states_visited,
    })
}

/// The number of buckets (`max_edge_cost + 1`) of each of `sides` bucket queues, if they fit
/// `budget` together
fn num_buckets<C: TryInto<usize>>(
    max_edge_cost: C,
    sides: usize,
    budget: &MemoryBudget,
) -> Result<usize, SearchError> {
    let num_buckets = max_edge_cost
        .try_into()
        .ok()
        .and_then(|max: usize| max.checked_add(1))
        .ok_or(SearchError::MaxEdgeCostTooLarge)?;
    let fits = num_buckets
        .checked_mul(size_of::<VecDeque<NodeID>>())
        .and_then(|bytes| bytes.checked_mul(sides))
        .is_some_and(|bytes| bytes <= budget.available());
    if !fits {
        return Err(SearchError::MemoryLimitReached);
    }
    Ok(num_buckets)
}

/// Check the nodes one side just reached against the other side, keeping the cheapest meeting
/// in `best`. Empties `improved`.
///
/// `swap` tells whether `problem` is the backward one, which decides the order of the node pair.
fn meet<P: ReversibleSearchProblem>(
    problem: &P,
    other_problem: &P,
    other_side: &SearchSide<'_, P::Step, P::Cost>,
    improved: &mut Vec<(NodeID, P::Cost)>,
    best: &mut Option<(P::Cost, NodeID, NodeID)>,
    swap: bool,
) {
    for (node, distance) in improved.drain(..) {
        let Some(other) = problem.find_in(node, other_problem) else {
            continue;
        };
        let total = distance + other_side.state.nodes[other as usize].distance;
        if best.is_none_or(|(cost, _, _)| total < cost) {
            *best = Some(if swap {
                (total, other, node)
            } else {
                (total, node, other)
            });
        }
    }
}

/// One side of a search: a bucket queue over its own [`SearchState`]
struct SearchSide<'a, S, C> {
    state: &'a mut SearchState<S, C>,
    /// Bucket currently being drained
    bucket: usize,
    /// Entries in the buckets, some of which may be obsolete
    queued: usize,
}

impl<'a, S, C> SearchSide<'a, S, C>
where
    S: Copy + Default,
    C: Copy + Ord + Add<Output = C> + From<u8> + TryInto<usize>,
{
    /// Clear a search state and start it off at `start`, with `num_buckets` buckets (see
    /// [`num_buckets`])
    fn seed(
        state: &'a mut SearchState<S, C>,
        num_buckets: usize,
        start: NodeID,
    ) -> Result<Self, SearchError> {
        if worth_releasing(state.nodes.capacity(), state.nodes.len()) {
            state.nodes = Vec::new();
            state.buckets = Vec::new();
        } else {
            state.nodes.clear();
        }
        state.nodes.push(Node {
            distance: C::from(0),
            parent: start,
            step: S::default(),
            finished: false,
        });
        state.buckets.truncate(num_buckets);
        state
            .buckets
            .try_reserve_exact(num_buckets - state.buckets.len())
            .map_err(|_| SearchError::MemoryLimitReached)?;
        state.buckets.resize_with(num_buckets, VecDeque::new);
        state.buckets.iter_mut().for_each(VecDeque::clear);
        state.buckets[0].push_back(start);
        Ok(Self {
            state,
            bucket: 0,
            queued: 1,
        })
    }

    /// The node this side finishes next, dropping entries obsoleted by a shorter distance
    fn peek(&mut self) -> Option<(C, NodeID)> {
        while self.queued > 0 {
            while self.state.buckets[self.bucket].is_empty() {
                self.bucket = (self.bucket + 1) % self.state.buckets.len();
            }
            let node = self.state.buckets[self.bucket][0];
            if self.state.nodes[node as usize].finished {
                self.state.buckets[self.bucket].pop_front();
                self.queued -= 1;
                continue;
            }
            return Some((self.state.nodes[node as usize].distance, node));
        }
        None
    }

    /// Take the peeked `node` out of the queue, its distance now being final
    fn finish(&mut self, node: NodeID) {
        self.state.buckets[self.bucket].pop_front();
        self.queued -= 1;
        self.state.nodes[node as usize].finished = true;
    }

    /// Expand `node`, queueing every successor it brings closer and listing it in `improved`.
    /// Stops early once `budget` is exhausted.
    fn expand<P: SearchProblem<Step = S, Cost = C>>(
        &mut self,
        problem: &mut P,
        node: NodeID,
        distance: C,
        improved: &mut Vec<(NodeID, C)>,
        budget: &MemoryBudget,
    ) {
        let via = if self.state.nodes[node as usize].parent == node {
            None
        } else {
            Some(self.state.nodes[node as usize].step)
        };
        let nodes = &mut self.state.nodes;
        let buckets = &mut self.state.buckets;
        let queued = &mut self.queued;
        // `node` sits in the bucket being drained, so a successor goes `cost` buckets further.
        // This stays in `usize`, as `cost < num_buckets` and the buckets fit the budget.
        let bucket = self.bucket;
        let num_buckets = buckets.len();
        problem.expand(node, via, budget, |next, is_new, cost, step| {
            if budget.exhausted() {
                return;
            }
            let new_distance = distance + cost;
            let offset: usize = cost.try_into().unwrap_or(0);
            let idx = (bucket + offset) % num_buckets;
            if is_new {
                debug_assert_eq!(next as usize, nodes.len(), "no NodeID should be skipped");
                if !reserve_vec(nodes, 1, budget) {
                    return;
                }
                nodes.push(Node {
                    distance: new_distance,
                    parent: node,
                    step,
                    finished: false,
                });
            } else {
                let entry = &mut nodes[next as usize];
                if entry.finished || new_distance >= entry.distance {
                    return;
                }
                entry.distance = new_distance;
                entry.parent = node;
                entry.step = step;
            }
            if !reserve_deque(&mut buckets[idx], 1, budget) || !reserve_vec(improved, 1, budget) {
                return;
            }
            buckets[idx].push_back(next);
            *queued += 1;
            improved.push((next, new_distance));
        });
    }
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::{
        reserve_vec, search, search_bidirectional, MemoryBudget, NodeID, ReversibleSearchProblem,
        SearchError, SearchLimits, SearchProblem, SearchState,
    };

    /// A path of `len` edges, each of cost `max_edge_cost`. Node `i` is `i` edges from the start
    /// (or, reversed, from the goal).
    struct Chain<C> {
        max_edge_cost: C,
        len: u32,
        reached: u32,
    }

    fn chain<C>(max_edge_cost: C, len: u32) -> Chain<C> {
        Chain {
            max_edge_cost,
            len,
            reached: 0,
        }
    }

    impl<C> SearchProblem for Chain<C>
    where
        C: Copy + Ord + std::ops::Add<Output = C> + From<u8> + TryInto<usize>,
    {
        type Step = ();
        type Cost = C;

        fn initial(&mut self) -> NodeID {
            self.reached = 1;
            0
        }

        fn max_edge_cost(&self) -> C {
            self.max_edge_cost
        }

        fn is_goal(&self, node: NodeID) -> bool {
            node == self.len
        }

        fn expand<F: FnMut(NodeID, bool, C, ())>(
            &mut self,
            node: NodeID,
            _via: Option<()>,
            _budget: &MemoryBudget,
            mut emit: F,
        ) {
            if node < self.len {
                self.reached += 1;
                emit(node + 1, true, self.max_edge_cost, ());
            }
        }

        fn allocated_bytes(&self) -> usize {
            0
        }
    }

    impl<C> ReversibleSearchProblem for Chain<C>
    where
        Chain<C>: SearchProblem,
    {
        fn find_in(&self, node: NodeID, other: &Self) -> Option<NodeID> {
            let there = self.len - node;
            (there < other.reached).then_some(there)
        }
    }

    #[test]
    fn huge_edge_costs_fail_cleanly() {
        let limits = SearchLimits {
            max_memory: Some(1 << 30),
            ..SearchLimits::default()
        };
        for max_edge_cost in [u32::MAX, 1 << 31] {
            assert_eq!(
                search(
                    &mut chain(max_edge_cost, 1),
                    &mut SearchState::default(),
                    limits
                )
                .unwrap_err(),
                SearchError::MemoryLimitReached
            );
            assert_eq!(
                search_bidirectional(
                    &mut chain(max_edge_cost, 1),
                    &mut chain(max_edge_cost, 1),
                    &mut SearchState::default(),
                    &mut SearchState::default(),
                    limits,
                )
                .unwrap_err(),
                SearchError::MemoryLimitReached
            );
        }
        assert_eq!(
            search(
                &mut chain(u128::MAX, 1),
                &mut SearchState::default(),
                limits
            )
            .unwrap_err(),
            SearchError::MaxEdgeCostTooLarge
        );
    }

    #[test]
    fn cancelled_search_is_not_a_limit() {
        let flag = AtomicBool::new(false);
        let limits = SearchLimits {
            max_states: Some(0),
            cancel: Some(&flag),
            ..SearchLimits::default()
        };
        let mut state = SearchState::default();
        assert_eq!(
            search(&mut chain(1u32, 5), &mut state, limits).unwrap_err(),
            SearchError::LimitReached
        );
        flag.store(true, Ordering::Relaxed);
        assert_eq!(
            search(&mut chain(1u32, 5), &mut state, limits).unwrap_err(),
            SearchError::Cancelled
        );
        assert_eq!(
            search_bidirectional(
                &mut chain(1u32, 5),
                &mut chain(1u32, 5),
                &mut state,
                &mut SearchState::default(),
                limits,
            )
            .unwrap_err(),
            SearchError::Cancelled
        );
    }

    /// A [`chain`] that sets `flag` while expanding node `at`
    struct Tripwire<'a> {
        chain: Chain<u32>,
        at: NodeID,
        flag: &'a AtomicBool,
    }

    impl SearchProblem for Tripwire<'_> {
        type Step = ();
        type Cost = u32;

        fn initial(&mut self) -> NodeID {
            self.chain.initial()
        }

        fn max_edge_cost(&self) -> u32 {
            self.chain.max_edge_cost()
        }

        fn is_goal(&self, node: NodeID) -> bool {
            self.chain.is_goal(node)
        }

        fn expand<F: FnMut(NodeID, bool, u32, ())>(
            &mut self,
            node: NodeID,
            via: Option<()>,
            budget: &MemoryBudget,
            emit: F,
        ) {
            if node == self.at {
                self.flag.store(true, Ordering::Relaxed);
            }
            self.chain.expand(node, via, budget, emit);
        }

        fn allocated_bytes(&self) -> usize {
            0
        }
    }

    #[test]
    fn search_stops_right_after_the_flag_is_set() {
        let flag = AtomicBool::new(false);
        let mut problem = Tripwire {
            chain: chain(1, 100),
            at: 10,
            flag: &flag,
        };
        let limits = SearchLimits {
            cancel: Some(&flag),
            ..SearchLimits::default()
        };
        assert_eq!(
            search(&mut problem, &mut SearchState::default(), limits).unwrap_err(),
            SearchError::Cancelled
        );
        // Node 10 reached node 11, which was popped but not expanded
        assert_eq!(problem.chain.reached, 12);
    }

    #[test]
    fn distances_wrap_around_the_buckets() {
        let mut state = SearchState::default();
        let result = search(&mut chain(7u32, 5), &mut state, SearchLimits::default()).unwrap();
        assert_eq!((result.cost, result.path.len()), (35, 5));
        let result = search_bidirectional(
            &mut chain(7u32, 5),
            &mut chain(7u32, 5),
            &mut state,
            &mut SearchState::default(),
            SearchLimits::default(),
        )
        .unwrap();
        assert_eq!((result.cost, result.path.len()), (35, 5));
    }

    #[test]
    fn growth_counts_old_and_new_allocation() {
        let budget = MemoryBudget::new(Some(100));
        assert!(budget.grow(0, 40));
        assert!(!budget.grow(40, 70), "40 + 70 > 100 while both are held");
        assert!(budget.exhausted());
        let budget = MemoryBudget::new(Some(100));
        assert!(budget.grow(0, 30));
        assert!(budget.grow(30, 60));
        assert_eq!(budget.used(), 60);
        assert!(!budget.exhausted());
    }

    #[test]
    fn vec_growth_doubles_then_takes_what_fits() {
        let budget = MemoryBudget::new(Some(1200));
        let mut v: Vec<u64> = Vec::new();
        while reserve_vec(&mut v, 1, &budget) {
            v.push(0);
            assert_eq!(budget.used(), v.capacity() * 8);
        }
        // Doubling 64 to 128 elements would need 512 + 1024 bytes at once, so the last growth
        // takes the 86 elements that still fit next to the old 64
        assert_eq!(v.capacity(), 86);
        assert_eq!(budget.used(), 86 * 8);
        assert!(budget.exhausted());
    }

    #[test]
    fn no_limit_never_exhausts() {
        let budget = MemoryBudget::new(None);
        let mut v: Vec<u8> = Vec::new();
        assert!(reserve_vec(&mut v, 1 << 20, &budget));
        assert!(!budget.exhausted());
        assert_eq!(budget.used(), v.capacity());
    }
}
