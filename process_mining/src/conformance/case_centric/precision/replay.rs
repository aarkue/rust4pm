//! Replays symbols on a compiled Petri net, memoizing the expensive parts per marking
use std::{borrow::Cow, hash::Hasher, ops::Range};

use hashbrown::HashTable;
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};

use super::{EscapingEdgesOptions, PathStep, PrecisionError, SilentTransitions, TraceEnds};
use crate::{
    conformance::alignments::{
        petri_net::TokenCount,
        sync_prod_net::{
            arcs_as_token_counts, marking_as_token_counts, SyncProdNetConstructionError,
        },
    },
    core::process_models::petri_net::{AcceptingPetriNet, Marking, TransitionID},
};

/// An activity (label id), or a transition index with [`SilentTransitions::AsActivities`]
pub(super) type Symbol = u32;
/// Id of an interned marking
pub(super) type MarkingId = u32;

#[derive(Debug)]
struct Transition {
    label: Option<Symbol>,
    inputs: Vec<(usize, TokenCount)>,
    outputs: Vec<(usize, TokenCount)>,
}

impl Transition {
    fn is_enabled(&self, marking: &[TokenCount]) -> bool {
        self.inputs
            .iter()
            .all(|&(place, weight)| marking[place] >= weight)
    }
}

/// Markings stored once in a flat buffer and referred to by id
#[derive(Debug, Default)]
struct Markings {
    num_places: usize,
    tokens: Vec<TokenCount>,
    index: HashTable<MarkingId>,
    scratch: Vec<TokenCount>,
}

impl Markings {
    fn len(&self) -> usize {
        self.index.len()
    }

    fn get(&self, id: MarkingId) -> &[TokenCount] {
        &self.tokens[id as usize * self.num_places..][..self.num_places]
    }

    fn intern(&mut self, marking: &[TokenCount]) -> MarkingId {
        let hash = hash_marking(marking);
        if let Some(&id) = self.index.find(hash, |&id| self.get(id) == marking) {
            return id;
        }
        let id = self.len() as MarkingId;
        self.tokens.extend_from_slice(marking);
        let (tokens, n) = (&self.tokens, self.num_places);
        self.index.insert_unique(hash, id, |&id| {
            hash_marking(&tokens[id as usize * n..][..n])
        });
        id
    }

    /// The marking reached by firing `transition`, or `None` if it is not enabled or a place
    /// would exceed [`TokenCount::MAX`] tokens
    fn fire(&mut self, from: MarkingId, transition: &Transition) -> Option<MarkingId> {
        if !transition.is_enabled(self.get(from)) {
            return None;
        }
        let mut next = std::mem::take(&mut self.scratch);
        next.clear();
        next.extend_from_slice(self.get(from));
        let reached = apply(&mut next, transition).map(|()| self.intern(&next));
        self.scratch = next;
        reached
    }
}

fn apply(marking: &mut [TokenCount], transition: &Transition) -> Option<()> {
    for &(place, weight) in &transition.inputs {
        marking[place] -= weight;
    }
    for &(place, weight) in &transition.outputs {
        marking[place] = marking[place].checked_add(weight)?;
    }
    Some(())
}

fn hash_marking(marking: &[TokenCount]) -> u64 {
    let mut hasher = FxHasher::default();
    hasher.write(marking);
    hasher.finish()
}

/// A set of symbols as a fixed-size bitset
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SymbolSet(Box<[u64]>);

impl SymbolSet {
    fn new(num_symbols: usize) -> Self {
        Self(vec![0; num_symbols.div_ceil(64)].into())
    }

    fn insert(&mut self, symbol: Symbol) {
        self.0[symbol as usize / 64] |= 1 << (symbol % 64);
    }

    pub(super) fn contains(&self, symbol: Symbol) -> bool {
        self.0[symbol as usize / 64] & (1 << (symbol % 64)) != 0
    }

    pub(super) fn len(&self) -> usize {
        self.0.iter().map(|word| word.count_ones() as usize).sum()
    }

    pub(super) fn union_with(&mut self, other: &Self) {
        for (word, other) in self.0.iter_mut().zip(&other.0) {
            *word |= other;
        }
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = Symbol> + '_ {
        self.0.iter().enumerate().flat_map(|(i, &word)| {
            (0..64)
                .filter(move |bit| word & (1 << bit) != 0)
                .map(move |bit| (i * 64 + bit) as Symbol)
        })
    }
}

/// Replays symbols on a Petri net, memoizing per marking what silent transitions reach, what is
/// available, and where each symbol leads
#[derive(Debug)]
pub(super) struct Replayer {
    silent: SilentTransitions,
    search_limit: Option<usize>,
    exclude_dead_ends: bool,
    /// The symbol for stopping, with [`TraceEnds::AsActivity`]
    end: Option<Symbol>,
    final_marking: Option<MarkingId>,
    /// Indexed like the net's transitions
    transitions: Vec<Transition>,
    labels: Vec<String>,
    label_ids: FxHashMap<String, Symbol>,
    transitions_by_label: Vec<Vec<usize>>,
    silent_transitions: Vec<usize>,
    markings: Markings,
    initial: MarkingId,
    memo: Vec<MarkingMemo>,
    /// The silent reaches of all markings, one range each
    reach: Vec<MarkingId>,
    seen: FxHashSet<MarkingId>,
    steps: FxHashMap<(MarkingId, Symbol), Box<[MarkingId]>>,
    /// Whether a search stopped at `search_limit`
    pub(super) limit_reached: bool,
}

/// What is memoized per marking
#[derive(Debug, Default)]
struct MarkingMemo {
    silent_reach: Option<Range<usize>>,
    available: Option<SymbolSet>,
    completes: Option<bool>,
}

impl Replayer {
    pub(super) fn new(
        net: &AcceptingPetriNet,
        options: &EscapingEdgesOptions,
    ) -> Result<Self, PrecisionError> {
        let silent = options.silent_transitions;
        let num_places = net.net.num_places();
        let mut labels = Vec::new();
        let mut label_ids = FxHashMap::default();
        let mut transitions = Vec::with_capacity(net.net.num_transitions());
        // A transition's symbol (with `AsActivities`) is its index, so symbols and search order
        // follow transition order
        for (_, t) in net.net.transitions() {
            let label = t.label.as_ref().map(|label| {
                *label_ids.entry(label.clone()).or_insert_with(|| {
                    labels.push(label.clone());
                    (labels.len() - 1) as Symbol
                })
            });
            transitions.push(Transition {
                label,
                inputs: arcs_as_token_counts(t.inputs())?,
                outputs: arcs_as_token_counts(t.outputs())?,
            });
        }

        let mut transitions_by_label = vec![Vec::new(); labels.len()];
        let mut silent_transitions = Vec::new();
        for (i, transition) in transitions.iter().enumerate() {
            match transition.label {
                Some(label) => transitions_by_label[label as usize].push(i),
                None => silent_transitions.push(i),
            }
        }

        let mut markings = Markings {
            num_places,
            ..Default::default()
        };
        let mut intern = |marking: &Marking| {
            Ok::<_, SyncProdNetConstructionError>(
                markings.intern(&marking_as_token_counts(marking, num_places)?),
            )
        };
        let initial = intern(&net.initial_marking)?;
        let final_marking =
            if options.exclude_dead_ends || options.trace_ends == TraceEnds::AsActivity {
                Some(intern(&net.final_marking)?)
            } else {
                None
            };
        let num_symbols = match silent {
            SilentTransitions::Skip => labels.len(),
            SilentTransitions::AsActivities => transitions.len(),
        };

        Ok(Self {
            silent,
            search_limit: options.search_limit,
            exclude_dead_ends: options.exclude_dead_ends,
            end: (options.trace_ends == TraceEnds::AsActivity).then_some(num_symbols as Symbol),
            final_marking,
            transitions,
            labels,
            label_ids,
            transitions_by_label,
            silent_transitions,
            markings,
            initial,
            memo: Vec::new(),
            reach: Vec::new(),
            seen: FxHashSet::default(),
            steps: FxHashMap::default(),
            limit_reached: false,
        })
    }

    pub(super) fn initial(&self) -> MarkingId {
        self.initial
    }

    /// The symbol of an activity, only meaningful with [`SilentTransitions::Skip`]
    pub(super) fn activity_symbol(&self, activity: &str) -> Option<Symbol> {
        debug_assert_eq!(self.silent, SilentTransitions::Skip);
        self.label_ids.get(activity).copied()
    }

    /// The symbol a transition contributes to a path, `None` for a skipped silent transition
    pub(super) fn transition_symbol(
        &self,
        transition: TransitionID,
    ) -> Result<Option<Symbol>, PrecisionError> {
        if transition.index() >= self.transitions.len() {
            return Err(PrecisionError::UnknownTransition(transition));
        }
        let index = transition.index() as Symbol;
        Ok(match self.silent {
            SilentTransitions::Skip => self.transitions[index as usize].label,
            SilentTransitions::AsActivities => Some(index),
        })
    }

    /// Moves every silent transition of a firing sequence (with
    /// [`SilentTransitions::AsActivities`]) right past every transition that does not consume
    /// from a place it produces into, so that the result does not depend on where an aligner
    /// happened to place it. Such a swap keeps the sequence fireable.
    pub(super) fn delay_silent_transitions(&self, path: &mut [Symbol]) {
        debug_assert_eq!(self.silent, SilentTransitions::AsActivities);
        for start in (0..path.len()).rev() {
            if self.transitions[path[start] as usize].label.is_some() {
                continue;
            }
            let mut i = start;
            while i + 1 < path.len() && !self.feeds(path[i], path[i + 1]) {
                path.swap(i, i + 1);
                i += 1;
            }
        }
    }

    /// Whether `to` consumes from a place `from` produces into
    fn feeds(&self, from: Symbol, to: Symbol) -> bool {
        let outputs = &self.transitions[from as usize].outputs;
        self.transitions[to as usize]
            .inputs
            .iter()
            .any(|(place, _)| outputs.iter().any(|(output, _)| output == place))
    }

    pub(super) fn end_symbol(&self) -> Option<Symbol> {
        self.end
    }

    pub(super) fn path_step(&self, symbol: Symbol) -> PathStep {
        if Some(symbol) == self.end {
            return PathStep::End;
        }
        match self.silent {
            SilentTransitions::Skip => PathStep::Activity(self.labels[symbol as usize].clone()),
            SilentTransitions::AsActivities => PathStep::Transition(TransitionID(symbol)),
        }
    }

    fn num_symbols(&self) -> usize {
        let base = match self.silent {
            SilentTransitions::Skip => self.labels.len(),
            SilentTransitions::AsActivities => self.transitions.len(),
        };
        base + usize::from(self.end.is_some())
    }

    /// All markings reached from any of `from` by `symbol`, sorted and without duplicates.
    /// Empty if `symbol` cannot occur.
    pub(super) fn step(&mut self, from: &[MarkingId], symbol: Symbol) -> Box<[MarkingId]> {
        if let [marking] = *from {
            return self.step_from(marking, symbol).into();
        }
        let mut reached = Vec::new();
        for &marking in from {
            reached.extend_from_slice(self.step_from(marking, symbol));
        }
        reached.sort_unstable();
        reached.dedup();
        reached.into()
    }

    fn step_from(&mut self, marking: MarkingId, symbol: Symbol) -> &[MarkingId] {
        if !self.steps.contains_key(&(marking, symbol)) {
            let mut reached: Vec<MarkingId> = match self.silent {
                SilentTransitions::Skip => {
                    let reach = self.silent_reach(marking);
                    let mut reached = Vec::new();
                    for &start in &self.reach[reach] {
                        for &t in &self.transitions_by_label[symbol as usize] {
                            reached.extend(self.markings.fire(start, &self.transitions[t]));
                        }
                    }
                    reached
                }
                SilentTransitions::AsActivities => self
                    .markings
                    .fire(marking, &self.transitions[symbol as usize])
                    .into_iter()
                    .collect(),
            };
            reached.sort_unstable();
            reached.dedup();
            self.steps.insert((marking, symbol), reached.into());
        }
        &self.steps[&(marking, symbol)]
    }

    /// What is available in any of `markings`
    pub(super) fn available_in_any(&mut self, markings: &[MarkingId]) -> Cow<'_, SymbolSet> {
        match *markings {
            [marking] => Cow::Borrowed(self.available(marking)),
            [first, ref rest @ ..] => {
                let mut available = self.available(first).clone();
                for &marking in rest {
                    available.union_with(self.available(marking));
                }
                Cow::Owned(available)
            }
            [] => Cow::Owned(SymbolSet::new(self.num_symbols())),
        }
    }

    /// What is available in `marking`: the activities enabled after any silent sequence, or the
    /// directly enabled transitions with [`SilentTransitions::AsActivities`]
    fn available(&mut self, marking: MarkingId) -> &SymbolSet {
        if self.memo(marking).available.is_none() {
            let mut available = SymbolSet::new(self.num_symbols());
            let sources = match self.silent {
                SilentTransitions::Skip => {
                    let reach = self.silent_reach(marking);
                    self.reach[reach].to_vec()
                }
                SilentTransitions::AsActivities => vec![marking],
            };
            for &source in &sources {
                for t in 0..self.transitions.len() {
                    let symbol = match self.silent {
                        SilentTransitions::Skip => match self.transitions[t].label {
                            Some(label) => label,
                            None => continue,
                        },
                        SilentTransitions::AsActivities => t as Symbol,
                    };
                    if available.contains(symbol)
                        || !self.transitions[t].is_enabled(self.markings.get(source))
                    {
                        continue;
                    }
                    if !self.exclude_dead_ends || self.completes_after(source, t) {
                        available.insert(symbol);
                    }
                }
            }
            if let (Some(end), Some(target)) = (self.end, self.final_marking) {
                if sources.contains(&target) {
                    available.insert(end);
                }
            }
            self.memo[marking as usize].available = Some(available);
        }
        self.memo[marking as usize]
            .available
            .as_ref()
            .expect("computed above")
    }

    fn completes_after(&mut self, marking: MarkingId, transition: usize) -> bool {
        let reached = self.markings.fire(marking, &self.transitions[transition]);
        reached.is_some_and(|reached| self.can_complete(reached))
    }

    /// Whether the final marking can be reached from `start`. A search stopped at `search_limit`
    /// counts as reaching it.
    fn can_complete(&mut self, start: MarkingId) -> bool {
        if let Some(known) = self.memo(start).completes {
            return known;
        }
        let target = self
            .final_marking
            .expect("a final marking is required to exclude dead ends");
        // Breadth-first, with the index of the marking each one was reached from
        let mut visited = vec![(start, 0)];
        self.seen.clear();
        self.seen.insert(start);
        let mut next = 0;
        let found = loop {
            let Some(&(marking, _)) = visited.get(next) else {
                break None;
            };
            match self.memo(marking).completes {
                Some(true) => break Some(next),
                Some(false) => {
                    next += 1;
                    continue;
                }
                None if marking == target => break Some(next),
                None => {}
            }
            for t in 0..self.transitions.len() {
                let Some(reached) = self.markings.fire(marking, &self.transitions[t]) else {
                    continue;
                };
                if !self.seen.insert(reached) {
                    continue;
                }
                if self.search_limit.is_some_and(|max| visited.len() >= max) {
                    self.limit_reached = true;
                    self.memo(start).completes = Some(true);
                    return true;
                }
                visited.push((reached, next));
            }
            next += 1;
        };
        match found {
            Some(mut index) => loop {
                let (marking, parent) = visited[index];
                self.memo(marking).completes = Some(true);
                if index == 0 {
                    return true;
                }
                index = parent;
            },
            None => {
                for (marking, _) in visited {
                    self.memo(marking).completes = Some(false);
                }
                false
            }
        }
    }

    /// `start` and every marking reachable from it by silent transitions alone, as a range of
    /// `self.reach`
    fn silent_reach(&mut self, start: MarkingId) -> Range<usize> {
        if let Some(reach) = &self.memo(start).silent_reach {
            return reach.clone();
        }
        let begin = self.reach.len();
        self.reach.push(start);
        self.seen.clear();
        self.seen.insert(start);
        let mut next = begin;
        'search: while let Some(&marking) = self.reach.get(next) {
            next += 1;
            for &t in &self.silent_transitions {
                let Some(reached) = self.markings.fire(marking, &self.transitions[t]) else {
                    continue;
                };
                if !self.seen.insert(reached) {
                    continue;
                }
                if self
                    .search_limit
                    .is_some_and(|max| self.reach.len() - begin >= max)
                {
                    self.limit_reached = true;
                    break 'search;
                }
                self.reach.push(reached);
            }
        }
        let reach = begin..self.reach.len();
        self.memo[start as usize].silent_reach = Some(reach.clone());
        reach
    }

    fn memo(&mut self, marking: MarkingId) -> &mut MarkingMemo {
        if self.memo.len() < self.markings.len() {
            self.memo.resize_with(self.markings.len(), Default::default);
        }
        &mut self.memo[marking as usize]
    }
}
