//! Behavioral abstractions: the facts a log shows, the cells each
//! fact needs to be visible, and when visible facts entail a fact.
//!
//! A new abstraction implements [`Abstraction`] and is passed to
//! [`reflow_with`](super::reflow_with). To make it selectable by name, e.g., from the bindings,
//! add a variant to [`AbstractionKind`].

use std::collections::{HashMap, HashSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::log::{Act, Cell, LogView, Ot, Traces};

/// The kind of a fact, e.g., [`FactKind::ORDER`].
///
/// The search covers a fact by any fact of the same kind about the same activities, so two
/// abstractions that mean different things must use different kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FactKind(pub &'static str);

impl FactKind {
    /// The first activity precedes the second.
    pub const ORDER: FactKind = FactKind("order");
}

/// A statement one object type makes about two activities.
///
/// The type shows the fact only while it flows at both activities, so the fact needs the cells
/// `(activities[0], object_type)` and `(activities[1], object_type)`. A fact about a single
/// activity names it twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Fact {
    /// What the fact states.
    pub kind: FactKind,
    /// The type that makes the statement, as an index into
    /// [`AbstractionInput::object_types`].
    pub object_type: usize,
    /// The activities, as indices into [`AbstractionInput::activities`].
    pub activities: [usize; 2],
}

impl Fact {
    /// The cells the fact needs to be visible, `(activity, object type)`, one or two.
    pub fn cells(&self) -> impl Iterator<Item = (usize, usize)> {
        let [a, b] = self.activities;
        let t = self.object_type;
        std::iter::once((a, t)).chain((b != a).then_some((b, t)))
    }
}

/// What an abstraction reads: the activities, object types, candidate cells, and the events of
/// every object.
pub struct AbstractionInput<'a> {
    pub(crate) view: &'a LogView,
    pub(crate) traces: &'a Traces,
    pub(crate) candidates: &'a HashSet<Cell>,
    pub(crate) only_type: Option<Ot>,
}

impl std::fmt::Debug for AbstractionInput<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AbstractionInput")
            .field("activities", &self.view.activities)
            .field("object_types", &self.view.types)
            .finish_non_exhaustive()
    }
}

impl AbstractionInput<'_> {
    /// Activity names, sorted. Facts refer to activities by index into this.
    pub fn activities(&self) -> &[String] {
        &self.view.activities
    }

    /// Object type names, sorted. Facts refer to object types by index into this.
    pub fn object_types(&self) -> &[String] {
        &self.view.types
    }

    /// The object types to extract facts for: all, or one when Reflow only needs the facts of
    /// a single type.
    pub fn types(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.view.types.len()).filter(|t| self.only_type.is_none_or(|o| o == *t))
    }

    /// Whether `(activity, object_type)` is a candidate cell. A fact may only need candidate
    /// cells.
    pub fn is_candidate(&self, activity: usize, object_type: usize) -> bool {
        self.candidates.contains(&(activity, object_type))
    }

    /// The events of every object of a type, each as `(timestamp in milliseconds, activity)`
    /// pairs in order. Ties are ordered by activity and then event id.
    pub fn traces(&self, object_type: usize) -> impl Iterator<Item = &[(i64, usize)]> + '_ {
        self.view.objects_of_type[object_type]
            .iter()
            .map(|o| self.traces.of(*o))
    }
}

/// Decides which facts a set of visible facts entails.
pub trait Entailment {
    /// Whether the visible facts entail `fact`.
    ///
    /// Must hold for every visible fact (reflexive), and keep holding when more facts become
    /// visible (monotone).
    fn entails(&self, fact: &Fact) -> bool;
}

/// A behavioral abstraction: which facts a log shows, and when facts entail a fact.
pub trait Abstraction {
    /// The facts the log shows over its candidate cells, for the types of
    /// [`AbstractionInput::types`]. A fact that needs a non-candidate cell is ignored.
    fn facts(&self, input: &AbstractionInput<'_>) -> Vec<Fact>;

    /// The entailment of a set of visible facts, over `n_activities` activities.
    fn entailment(&self, visible: &[Fact], n_activities: usize) -> Box<dyn Entailment>;
}

/// The abstractions Reflow can select by name.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AbstractionKind {
    /// [`OrderingAbstraction`].
    #[default]
    Ordering,
}

impl AbstractionKind {
    /// The abstraction of this kind.
    pub fn abstraction(self) -> Box<dyn Abstraction> {
        match self {
            AbstractionKind::Ordering => Box::new(OrderingAbstraction),
        }
    }

    /// Whether the abstraction's facts are the orderings, so the repair phase need not extract
    /// them again.
    pub(crate) fn facts_are_orderings(self) -> bool {
        match self {
            AbstractionKind::Ordering => true,
        }
    }
}

/// The ordering abstraction.
///
/// An object votes for `(a, b)` when its first `a` event is strictly before its last `b` event.
/// A type orders `(a, b)` when one of its objects votes for it and none for `(b, a)`. Visible
/// orderings entail every ordering on a path of visible orderings, whichever types they are of.
#[derive(Debug, Clone, Copy, Default)]
pub struct OrderingAbstraction;

impl Abstraction for OrderingAbstraction {
    fn facts(&self, input: &AbstractionInput<'_>) -> Vec<Fact> {
        let n = input.activities().len();
        let mut out = Vec::new();
        for t in input.types() {
            let mut votes = vec![false; n * n];
            let mut bounds: Vec<(Act, i64, i64)> = Vec::new();
            for trace in input.traces(t) {
                bounds.clear();
                for (ts, a) in trace {
                    match bounds.iter_mut().find(|(x, _, _)| x == a) {
                        Some(b) => {
                            b.1 = b.1.min(*ts);
                            b.2 = b.2.max(*ts);
                        }
                        None => bounds.push((*a, *ts, *ts)),
                    }
                }
                for (x, first, _) in &bounds {
                    for (y, _, last) in &bounds {
                        if x != y && first < last {
                            votes[x * n + y] = true;
                        }
                    }
                }
            }
            for x in 0..n {
                for y in 0..n {
                    if votes[x * n + y]
                        && !votes[y * n + x]
                        && input.is_candidate(x, t)
                        && input.is_candidate(y, t)
                    {
                        out.push(Fact {
                            kind: FactKind::ORDER,
                            object_type: t,
                            activities: [x, y],
                        });
                    }
                }
            }
        }
        out
    }

    fn entailment(&self, visible: &[Fact], n_activities: usize) -> Box<dyn Entailment> {
        Box::new(OrderEntailment::new(visible, n_activities))
    }
}

/// Orderings entailed through a path of visible orderings, and every other visible fact.
pub(crate) struct OrderEntailment {
    paths: Reachability,
    other: HashSet<Fact>,
}

impl OrderEntailment {
    pub(crate) fn new(visible: &[Fact], n_activities: usize) -> Self {
        let pairs = visible
            .iter()
            .filter(|f| f.kind == FactKind::ORDER)
            .map(|f| (f.activities[0], f.activities[1]));
        Self {
            paths: Reachability::new(pairs, n_activities),
            other: visible
                .iter()
                .filter(|f| f.kind != FactKind::ORDER)
                .copied()
                .collect(),
        }
    }
}

impl Entailment for OrderEntailment {
    fn entails(&self, fact: &Fact) -> bool {
        if fact.kind == FactKind::ORDER {
            self.paths.holds(fact.activities[0], fact.activities[1])
        } else {
            self.other.contains(fact)
        }
    }
}

/// The transitive closure of a relation over `n` activities, as bit rows.
pub(crate) struct Reachability {
    n: usize,
    words: usize,
    bits: Vec<u64>,
}

impl Reachability {
    pub(crate) fn new(pairs: impl IntoIterator<Item = (Act, Act)>, n: usize) -> Self {
        let words = n.div_ceil(64).max(1);
        let mut bits = vec![0u64; n * words];
        for (x, y) in pairs {
            bits[x * words + y / 64] |= 1u64 << (y % 64);
        }
        for k in 0..n {
            let (head, tail) = bits.split_at_mut(k * words);
            let (row_k, rest) = tail.split_at_mut(words);
            let through = |row: &mut [u64]| {
                if row[k / 64] >> (k % 64) & 1 == 1 {
                    for (x, y) in row.iter_mut().zip(row_k.iter()) {
                        *x |= *y;
                    }
                }
            };
            head.chunks_mut(words).for_each(through);
            rest.chunks_mut(words).for_each(through);
        }
        Self { n, words, bits }
    }

    pub(crate) fn holds(&self, x: Act, y: Act) -> bool {
        x < self.n && y < self.n && self.bits[x * self.words + y / 64] >> (y % 64) & 1 == 1
    }

    /// Word operations one closure over `n` activities takes, the unit of the work budget.
    pub(crate) fn cost(n: usize) -> u64 {
        let n = n as u64;
        (n * n * n.div_ceil(64)).max(1)
    }
}

/// Facts grouped by kind and activities, i.e., by what any type could show instead.
pub(crate) fn by_statement(facts: &[Fact]) -> HashMap<(FactKind, [usize; 2]), Vec<usize>> {
    let mut out: HashMap<(FactKind, [usize; 2]), Vec<usize>> = HashMap::new();
    for (i, f) in facts.iter().enumerate() {
        out.entry((f.kind, f.activities)).or_default().push(i);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(t: usize, a: usize, b: usize) -> Fact {
        Fact {
            kind: FactKind::ORDER,
            object_type: t,
            activities: [a, b],
        }
    }

    #[test]
    fn a_path_of_orderings_entails_its_ends_whatever_the_types() {
        let e = OrderEntailment::new(&[order(0, 0, 1), order(1, 1, 2)], 3);
        assert!(e.entails(&order(2, 0, 2)));
        assert!(!e.entails(&order(0, 2, 0)));
        let other = Fact {
            kind: FactKind("exclusive"),
            ..order(0, 0, 2)
        };
        assert!(!e.entails(&other), "a path says nothing about another kind");
    }

    #[test]
    fn reachability_crosses_word_boundaries() {
        let r = Reachability::new((0..130).map(|i| (i, i + 1)), 131);
        assert!(r.holds(0, 130) && r.holds(63, 64) && !r.holds(130, 0));
    }

    #[test]
    fn a_fact_about_one_activity_needs_one_cell() {
        let f = Fact {
            kind: FactKind("repeats"),
            object_type: 2,
            activities: [3, 3],
        };
        assert_eq!(f.cells().collect::<Vec<_>>(), vec![(3, 2)]);
    }
}
