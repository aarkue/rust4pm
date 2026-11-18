//! The flow-layer search: cover the facts rarest first, prune the most expensive cells
//! first, from two seeds, repair both, and keep the layer with fewer directly-follows arcs.

use std::collections::{HashMap, HashSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::abstraction::{by_statement, Abstraction, Entailment, Fact, Reachability};
use super::grid::ActivityGrid;
use super::log::{count_arcs, Act, Cell, Ot, Variants};
use crate::utils::control::Control;

/// Where the search started from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum SearchSeed {
    /// The empty layer.
    Greedy,
    /// One flow cell per activity, of the type with the fewest objects among those showing a
    /// fact there.
    Handoff,
    /// The exhaustive search.
    Exact,
}

/// Work done, against a limit.
///
/// The unit is roughly a nanosecond of search on a current machine, counted from the operations
/// done instead of measured, so whether a search stops early does not depend on the machine.
/// Cancelling through the [`Control`] stops the search as if the limit were reached.
pub(crate) struct Budget<'c> {
    pub(crate) max: u64,
    pub(crate) used: u64,
    pub(crate) hit: bool,
    ctl: &'c Control<'c>,
    /// `used` at which the control is polled next.
    next_poll: u64,
}

/// Work between two polls of the control, about a millisecond.
const POLL_EVERY: u64 = 1 << 20;

impl<'c> Budget<'c> {
    pub(crate) fn new(max: u64, ctl: &'c Control<'c>) -> Self {
        Self {
            max,
            used: 0,
            hit: false,
            ctl,
            next_poll: 0,
        }
    }

    /// Spend `cost`, or report that the limit is reached or the search cancelled.
    fn spend(&mut self, cost: u64) -> bool {
        if self.hit || self.used.saturating_add(cost) > self.max {
            self.hit = true;
            return false;
        }
        self.used += cost;
        self.used < self.next_poll || self.poll()
    }

    /// Report progress, or stop the search once cancelled.
    #[cold]
    pub(crate) fn poll(&mut self) -> bool {
        self.next_poll = self.used.saturating_add(POLL_EVERY);
        if self.ctl.is_cancelled() {
            self.hit = true;
            return false;
        }
        let total = (self.max < u64::MAX).then_some(self.max);
        self.ctl.progress("Searching flow", self.used, total);
        true
    }
}

/// Allocations of one entailment rebuild, in work units.
const REBUILD_OVERHEAD: u64 = 1_000;
/// One node of the exact search, in work units.
const NODE_COST: u64 = 100;
/// Checking at a leaf of the exact search that one recorded cell of an ordering flows or is
/// determined, in work units.
const STATED_COST: u64 = 50;

/// Everything the search reads.
pub(crate) struct SearchInput<'a> {
    pub(crate) facts: &'a [Fact],
    pub(crate) abstraction: &'a dyn Abstraction,
    /// Participations per candidate cell.
    pub(crate) cost: &'a HashMap<Cell, usize>,
    pub(crate) recorded: &'a HashMap<Cell, usize>,
    pub(crate) objects_per_type: &'a [usize],
    pub(crate) rep: &'a [Ot],
    pub(crate) types: &'a [String],
    pub(crate) n_acts: usize,
    pub(crate) variants: &'a Variants,
    pub(crate) grid: &'a [ActivityGrid],
    /// The orderings the repair phase reads, which are not `facts` under another abstraction.
    pub(crate) orderings: &'a [Fact],
}

impl SearchInput<'_> {
    fn cost(&self, c: Cell) -> usize {
        self.cost.get(&c).copied().unwrap_or(0)
    }

    /// Collecting the visible facts, the closure, and checking every fact against it.
    fn rebuild_cost(&self) -> u64 {
        REBUILD_OVERHEAD + Reachability::cost(self.n_acts) + 2 * self.facts.len() as u64
    }
}

/// A flow layer and what decides between layers.
#[derive(Debug, Clone)]
pub(crate) struct Layer {
    pub(crate) cells: HashSet<Cell>,
    pub(crate) seed: SearchSeed,
    pub(crate) entailed: usize,
    pub(crate) arcs: usize,
    pub(crate) participations: usize,
    /// Cells the repair phase added.
    pub(crate) repaired: usize,
}

impl Layer {
    fn new(
        input: &SearchInput<'_>,
        cells: HashSet<Cell>,
        seed: SearchSeed,
        entailed: usize,
    ) -> Self {
        Layer {
            arcs: input.variants.arcs(|c| cells.contains(&c)),
            participations: cells.iter().map(|c| input.cost(*c)).sum(),
            cells,
            seed,
            entailed,
            repaired: 0,
        }
    }

    /// The layer with the cells [`repair`] adds.
    fn repaired(self, input: &SearchInput<'_>) -> Self {
        let mut cells = self.cells;
        let added = repair(input.grid, input.orderings, &mut cells);
        Layer {
            repaired: added,
            ..Layer::new(input, cells, self.seed, self.entailed)
        }
    }

    fn key(&self) -> (std::cmp::Reverse<usize>, usize, usize, usize) {
        (
            std::cmp::Reverse(self.entailed),
            self.arcs,
            self.participations,
            self.cells.len(),
        )
    }
}

/// The kept cells, the facts they make visible, and the entailment of those, rebuilt lazily.
struct State<'a> {
    input: &'a SearchInput<'a>,
    kept: HashSet<Cell>,
    /// Cells of each fact.
    needs: Vec<Vec<Cell>>,
    /// Facts each candidate cell is needed by.
    needed_by: HashMap<Cell, Vec<usize>>,
    /// Kept cells of each fact.
    have: Vec<usize>,
    entailment: Option<Box<dyn Entailment>>,
}

impl<'a> State<'a> {
    fn new(input: &'a SearchInput<'a>) -> Self {
        let needs: Vec<Vec<Cell>> = input.facts.iter().map(|f| f.cells().collect()).collect();
        let mut needed_by: HashMap<Cell, Vec<usize>> = HashMap::new();
        for (i, cells) in needs.iter().enumerate() {
            for c in cells {
                needed_by.entry(*c).or_default().push(i);
            }
        }
        Self {
            input,
            kept: HashSet::new(),
            have: vec![0; needs.len()],
            needs,
            needed_by,
            entailment: None,
        }
    }

    fn insert(&mut self, c: Cell) {
        if self.kept.insert(c) {
            for f in self.needed_by.get(&c).into_iter().flatten() {
                self.have[*f] += 1;
            }
            self.entailment = None;
        }
    }

    fn remove(&mut self, c: Cell) {
        if self.kept.remove(&c) {
            for f in self.needed_by.get(&c).into_iter().flatten() {
                self.have[*f] -= 1;
            }
            self.entailment = None;
        }
    }

    /// The entailment of the visible facts, or `None` once the budget is spent.
    fn entailment(&mut self, budget: &mut Budget<'_>) -> Option<&dyn Entailment> {
        if self.entailment.is_none() {
            if !budget.spend(self.input.rebuild_cost()) {
                return None;
            }
            let visible: Vec<Fact> = (0..self.needs.len())
                .filter(|f| self.have[*f] == self.needs[*f].len())
                .map(|f| self.input.facts[f])
                .collect();
            self.entailment = Some(
                self.input
                    .abstraction
                    .entailment(&visible, self.input.n_acts),
            );
        }
        self.entailment.as_deref()
    }

    fn entailed(&mut self, budget: &mut Budget<'_>) -> Option<usize> {
        let facts = self.input.facts;
        let e = self.entailment(budget)?;
        Some(facts.iter().filter(|f| e.entails(f)).count())
    }

    /// Keep every cell of the facts not yet visible, which makes every fact visible.
    fn keep_all_needed(&mut self) {
        let missing: Vec<Cell> = self.needs.iter().flatten().copied().collect();
        for c in missing {
            self.insert(c);
        }
    }

    fn layer(&mut self, seed: SearchSeed, budget: &mut Budget<'_>) -> Layer {
        let entailed = match self.entailed(budget) {
            Some(n) => n,
            // Without the budget to check, the only layer known to be preserving is one where
            // every fact is visible.
            None => {
                self.keep_all_needed();
                self.input.facts.len()
            }
        };
        Layer::new(self.input, self.kept.clone(), seed, entailed)
    }
}

/// For every fact not yet entailed, rarest first, keep the cells of the cheapest fact of the
/// same kind about the same activities.
fn cover(st: &mut State<'_>, budget: &mut Budget<'_>) {
    let input = st.input;
    let statements = by_statement(input.facts);
    let mut facts_per_type: HashMap<Ot, usize> = HashMap::new();
    for f in input.facts {
        *facts_per_type.entry(f.object_type).or_default() += 1;
    }
    let mut order: Vec<usize> = (0..input.facts.len()).collect();
    order.sort_by_key(|i| {
        let f = &input.facts[*i];
        (
            statements[&(f.kind, f.activities)].len(),
            f.kind,
            f.activities,
            f.object_type,
        )
    });
    for i in order {
        let f = input.facts[i];
        match st.entailment(budget) {
            Some(e) if e.entails(&f) => continue,
            Some(_) => {}
            None => {
                st.keep_all_needed();
                return;
            }
        }
        let best = statements[&(f.kind, f.activities)]
            .iter()
            .map(|j| &input.facts[*j])
            .min_by_key(|g| {
                let spend: usize = g
                    .cells()
                    .filter(|c| !st.kept.contains(c))
                    .map(|c| input.cost(c))
                    .sum();
                let t = g.object_type;
                (
                    spend,
                    std::cmp::Reverse(facts_per_type[&t]),
                    &input.types[input.rep[t]],
                    t,
                )
            })
            .copied();
        if let Some(g) = best {
            for c in g.cells() {
                st.insert(c);
            }
        }
    }
}

/// Drop every kept cell, most expensive first, whose removal keeps every fact entailed.
fn prune(st: &mut State<'_>, budget: &mut Budget<'_>) {
    let mut order: Vec<Cell> = st.kept.iter().copied().collect();
    order.sort_by_key(|c| (std::cmp::Reverse(st.input.cost(*c)), *c));
    let Some(mut before) = st.entailed(budget) else {
        return;
    };
    for c in order {
        let entailment = st.entailment.take();
        st.remove(c);
        match st.entailed(budget) {
            Some(after) if after >= before => before = after,
            after => {
                st.insert(c);
                st.entailment = entailment;
                if after.is_none() {
                    return;
                }
            }
        }
    }
}

fn run(input: &SearchInput<'_>, seed: SearchSeed, budget: &mut Budget<'_>) -> Layer {
    let mut st = State::new(input);
    if seed == SearchSeed::Handoff {
        let mentions: HashSet<(Act, Ot)> = input
            .facts
            .iter()
            .flat_map(|f| f.activities.map(|a| (a, f.object_type)))
            .collect();
        for a in 0..input.n_acts {
            let native = (0..input.types.len())
                .filter(|t| input.recorded.contains_key(&(a, *t)) && mentions.contains(&(a, *t)))
                .min_by_key(|t| (input.objects_per_type[*t], &input.types[input.rep[*t]], *t));
            if let Some(t) = native {
                st.insert((a, t));
            }
        }
    }
    cover(&mut st, budget);
    prune(&mut st, budget);
    st.layer(seed, budget)
}

/// The better of both seeds after repair, the handoff one on a tie.
pub(crate) fn heuristic(input: &SearchInput<'_>, budget: &mut Budget<'_>) -> Layer {
    let handoff = run(input, SearchSeed::Handoff, budget).repaired(input);
    let greedy = run(input, SearchSeed::Greedy, budget).repaired(input);
    if greedy.key() < handoff.key() {
        greedy
    } else {
        handoff
    }
}

/// The recorded cells `(x, t)` of the orderings `(t, a, b)` with `x` in `{a, b}`, in the order
/// of `orderings`.
fn ordering_cells(grid: &[ActivityGrid], orderings: &[Fact]) -> Vec<Cell> {
    let mut seen = HashSet::new();
    orderings
        .iter()
        .flat_map(|f| f.activities.map(|x| (x, f.object_type)))
        .filter(|(x, t)| grid[*x].slot(*t).is_some())
        .filter(|c| seen.insert(*c))
        .collect()
}

/// For every slot of `x`, the slot it is reconstructed from under `flow`.
fn determining_at(grid: &[ActivityGrid], x: Act, flow: &HashSet<Cell>) -> Vec<Option<usize>> {
    let here = &grid[x];
    let flows: Vec<bool> = here
        .present
        .iter()
        .map(|t| flow.contains(&(x, *t)))
        .collect();
    here.determining(&flows)
}

/// Whether each of the recorded cells `(x, t)`, `t` in `types`, flows or is determined by `flow`.
fn stated_at(grid: &[ActivityGrid], x: Act, types: &[Ot], flow: &HashSet<Cell>) -> bool {
    let via = determining_at(grid, x, flow);
    types.iter().all(|t| {
        flow.contains(&(x, *t)) || grid[x].slot(*t).is_some_and(|slot| via[slot].is_some())
    })
}

/// The types of `cells` by activity.
fn by_activity(cells: &[Cell]) -> HashMap<Act, Vec<Ot>> {
    let mut by_act: HashMap<Act, Vec<Ot>> = HashMap::new();
    for (x, t) in cells.iter().copied() {
        by_act.entry(x).or_default().push(t);
    }
    by_act
}

/// Add recorded cells of the orderings `(t, a, b)` to `flow` until every recorded cell `(x, t)`,
/// `x` in `{a, b}`, flows or is determined by the flow cells at `x`, and return how many.
///
/// One pass adds each such cell not yet determined, in the order of `orderings`. A second pass
/// drops again each added cell, last first, without which every such cell still flows or is
/// determined, so no proper subset of the added cells suffices.
pub(crate) fn repair(grid: &[ActivityGrid], orderings: &[Fact], flow: &mut HashSet<Cell>) -> usize {
    let cells = ordering_cells(grid, orderings);
    let mut via: HashMap<Act, Vec<Option<usize>>> = HashMap::new();
    let mut added: Vec<Cell> = Vec::new();
    for (x, t) in cells.iter().copied() {
        if flow.contains(&(x, t)) {
            continue;
        }
        let at_x = via
            .entry(x)
            .or_insert_with(|| determining_at(grid, x, flow));
        if grid[x].slot(t).is_some_and(|slot| at_x[slot].is_none()) {
            flow.insert((x, t));
            added.push((x, t));
            via.remove(&x);
        }
    }
    let by_act = by_activity(&cells);
    let mut kept = added.len();
    for c in added.iter().rev() {
        flow.remove(c);
        if stated_at(grid, c.0, &by_act[&c.0], flow) {
            kept -= 1;
        } else {
            flow.insert(*c);
        }
    }
    kept
}

/// The preserving flow layer with the fewest arcs that leaves no recorded cell of an ordering
/// undetermined (what [`repair`] establishes), by branch and bound over the rows of the object
/// types, or `None` if the budget runs out first.
///
/// Only types that show a fact or order a pair get a row. The repaired heuristic layer `seed`
/// lies in that space, so the result has at most its arcs.
pub(crate) fn exact(
    input: &SearchInput<'_>,
    seed: &Layer,
    budget: &mut Budget<'_>,
) -> Option<Layer> {
    // A cell of another type makes no fact visible and is never repaired.
    let shown: HashSet<Ot> = input
        .facts
        .iter()
        .chain(input.orderings)
        .map(|f| f.object_type)
        .collect();
    let mut rows_of: HashMap<Ot, Vec<Act>> = HashMap::new();
    for c in input.cost.keys().filter(|c| shown.contains(&c.1)) {
        rows_of.entry(c.1).or_default().push(c.0);
    }
    let mut types: Vec<TypeRow<'_>> = rows_of
        .into_iter()
        .map(|(t, mut acts)| {
            acts.sort_unstable();
            TypeRow::build(input, t, acts)
        })
        .collect();
    // Types with the most facts decide feasibility, so they go first.
    types.sort_by_key(|r| (std::cmp::Reverse(r.facts.len()), r.t));
    let mut suffix: Vec<Vec<Fact>> = vec![Vec::new(); types.len() + 1];
    for i in (0..types.len()).rev() {
        let mut s = suffix[i + 1].clone();
        s.extend(types[i].facts.iter().copied());
        suffix[i] = s;
    }
    let mut seed_cells: Vec<Cell> = seed.cells.iter().copied().collect();
    seed_cells.sort_unstable();
    let stated = by_activity(&ordering_cells(input.grid, input.orderings));
    let stated_cost = STATED_COST * stated.values().map(Vec::len).sum::<usize>() as u64;
    let mut walk = Descent {
        input,
        types,
        suffix,
        stated,
        stated_cost,
        row: Vec::new(),
        overlap: Vec::new(),
        fixed: Vec::new(),
        visible: Vec::new(),
        best: (seed.entailed == input.facts.len()).then_some((
            seed_cells,
            (seed.arcs, seed.participations, seed.cells.len()),
        )),
        budget,
    };
    walk.enter(0, (0, 0, 0));
    if walk.budget.hit {
        return None;
    }
    let (cells, _) = walk.best?;
    Some(Layer::new(
        input,
        cells.into_iter().collect(),
        SearchSeed::Exact,
        input.facts.len(),
    ))
}

/// What the exact search needs about one object type.
struct TypeRow<'a> {
    t: Ot,
    /// Candidate activities of the type, sorted. A row is a subset.
    acts: Vec<Act>,
    facts: Vec<Fact>,
    variants: &'a [(Ot, Vec<Act>)],
    /// Activities over all variants, what scoring a row reads.
    variant_len: u64,
    /// Variants each activity of `acts` occurs in.
    in_variants: Vec<Vec<usize>>,
    cost: Vec<usize>,
}

impl<'a> TypeRow<'a> {
    fn build(input: &SearchInput<'a>, t: Ot, acts: Vec<Act>) -> Self {
        let facts: Vec<Fact> = input
            .facts
            .iter()
            .filter(|f| f.object_type == t)
            .copied()
            .collect();
        let all = input.variants.variants.as_slice();
        let start = all.partition_point(|(vt, _)| *vt < t);
        let variants = &all[start..all.partition_point(|(vt, _)| *vt <= t)];
        let in_variants = acts
            .iter()
            .map(|a| {
                (0..variants.len())
                    .filter(|v| variants[*v].1.contains(a))
                    .collect()
            })
            .collect();
        let cost = acts.iter().map(|a| input.cost((*a, t))).collect();
        Self {
            t,
            acts,
            facts,
            variant_len: variants.iter().map(|(_, v)| v.len()).sum::<usize>() as u64,
            variants,
            in_variants,
            cost,
        }
    }

    /// Directly-follows arcs of the type's variants projected onto the kept activities.
    fn arcs(&self, row: &[bool]) -> usize {
        let keep: HashSet<Act> = self
            .acts
            .iter()
            .zip(row)
            .filter(|(_, on)| **on)
            .map(|(a, _)| *a)
            .collect();
        count_arcs(self.variants, |(a, _)| keep.contains(&a))
    }
}

/// Arcs, participations and cells of the rows fixed so far.
type Acc = (usize, usize, usize);

struct Descent<'a, 'b, 'c> {
    input: &'a SearchInput<'a>,
    types: Vec<TypeRow<'a>>,
    /// Facts the types from `i` on can make visible.
    suffix: Vec<Vec<Fact>>,
    /// The recorded cells of the orderings by activity, each to flow or be determined.
    stated: HashMap<Act, Vec<Ot>>,
    /// Checking `stated`, in work units.
    stated_cost: u64,
    row: Vec<bool>,
    /// How many kept activities of the current row each of its variants holds.
    overlap: Vec<usize>,
    fixed: Vec<Cell>,
    visible: Vec<Fact>,
    best: Option<(Vec<Cell>, Acc)>,
    budget: &'b mut Budget<'c>,
}

impl Descent<'_, '_, '_> {
    fn best_arcs(&self) -> usize {
        self.best.as_ref().map_or(usize::MAX, |(_, k)| k.0)
    }

    fn covers(&mut self, visible: &[Fact]) -> bool {
        if !self.budget.spend(self.input.rebuild_cost()) {
            return false;
        }
        let e = self
            .input
            .abstraction
            .entailment(visible, self.input.n_acts);
        self.input.facts.iter().all(|f| e.entails(f))
    }

    /// Whether every cell of `stated` flows or is determined by the fixed cells.
    fn states(&mut self) -> bool {
        if !self.budget.spend(self.stated_cost) {
            return false;
        }
        let flow: HashSet<Cell> = self.fixed.iter().copied().collect();
        self.stated
            .iter()
            .all(|(x, types)| stated_at(self.input.grid, *x, types, &flow))
    }

    /// Adding an activity can remove an arc, but a variant holding `k` kept activities still
    /// draws at least `k - 1` arcs.
    fn bound(&self, acc: Acc) -> usize {
        acc.0
            + self
                .overlap
                .iter()
                .copied()
                .max()
                .unwrap_or(0)
                .saturating_sub(1)
    }

    fn set(&mut self, ti: usize, ci: usize, on: bool) {
        self.row[ci] = on;
        for v in &self.types[ti].in_variants[ci] {
            if on {
                self.overlap[*v] += 1;
            } else {
                self.overlap[*v] -= 1;
            }
        }
    }

    fn enter(&mut self, ti: usize, acc: Acc) {
        if !self.budget.spend(NODE_COST) || acc.0 > self.best_arcs() {
            return;
        }
        if ti == self.types.len() {
            let visible = self.visible.clone();
            if !self.states() || !self.covers(&visible) {
                return;
            }
            if self.best.as_ref().is_none_or(|(_, b)| acc < *b) {
                let mut cells = self.fixed.clone();
                cells.sort_unstable();
                self.best = Some((cells, acc));
            }
            return;
        }
        // Entailment is monotone, so if everything undecided cannot cover, nothing below can.
        let mut reach = self.visible.clone();
        reach.extend(self.suffix[ti].iter().copied());
        if !self.covers(&reach) {
            return;
        }
        let row = std::mem::replace(&mut self.row, vec![false; self.types[ti].acts.len()]);
        let overlap = std::mem::replace(&mut self.overlap, vec![0; self.types[ti].variants.len()]);
        self.cells(ti, 0, acc);
        self.row = row;
        self.overlap = overlap;
    }

    /// Decide cell `ci` of type `ti`, leaving it out first so cheap rows come first.
    fn cells(&mut self, ti: usize, ci: usize, acc: Acc) {
        if !self.budget.spend(NODE_COST) || self.bound(acc) > self.best_arcs() {
            return;
        }
        if ci == self.types[ti].acts.len() {
            self.close(ti, acc);
            return;
        }
        self.cells(ti, ci + 1, acc);
        if self.budget.hit {
            return;
        }
        self.set(ti, ci, true);
        self.cells(ti, ci + 1, acc);
        self.set(ti, ci, false);
    }

    fn close(&mut self, ti: usize, acc: Acc) {
        if !self.budget.spend(self.types[ti].variant_len) {
            return;
        }
        let row = &self.types[ti];
        let kept: Vec<Act> = row
            .acts
            .iter()
            .zip(&self.row)
            .filter(|(_, on)| **on)
            .map(|(a, _)| *a)
            .collect();
        let parts: usize = row
            .cost
            .iter()
            .zip(&self.row)
            .filter(|(_, on)| **on)
            .map(|(c, _)| *c)
            .sum();
        let next = (
            acc.0 + row.arcs(&self.row),
            acc.1 + parts,
            acc.2 + kept.len(),
        );
        if next.0 > self.best_arcs() {
            return;
        }
        let t = row.t;
        let shown: Vec<Fact> = row
            .facts
            .iter()
            .filter(|f| f.activities.iter().all(|a| kept.contains(a)))
            .copied()
            .collect();
        let (n_fixed, n_visible) = (self.fixed.len(), self.visible.len());
        self.fixed.extend(kept.into_iter().map(|a| (a, t)));
        self.visible.extend(shown);
        self.enter(ti + 1, next);
        self.fixed.truncate(n_fixed);
        self.visible.truncate(n_visible);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::object_centric::reflow::abstraction::{FactKind, OrderingAbstraction};

    fn order(t: Ot) -> Fact {
        Fact {
            kind: FactKind::ORDER,
            object_type: t,
            activities: [0, 1],
        }
    }

    /// Both types order `0 < 1`, the second at ten times the cost.
    fn with_input(check: impl FnOnce(&SearchInput<'_>)) {
        let facts = [order(0), order(1)];
        let cost: HashMap<Cell, usize> =
            HashMap::from([((0, 0), 1), ((1, 0), 1), ((0, 1), 10), ((1, 1), 10)]);
        let variants = Variants {
            variants: vec![(0, vec![0, 1]), (1, vec![0, 0, 1])],
        };
        let types = ["a".to_string(), "b".to_string()];
        check(&SearchInput {
            facts: &facts,
            abstraction: &OrderingAbstraction,
            cost: &cost,
            recorded: &cost,
            objects_per_type: &[1, 1],
            rep: &[0, 1],
            types: &types,
            n_acts: 2,
            variants: &variants,
            grid: &[],
            orderings: &[],
        });
    }

    /// Pruning rebuilds the entailment once for the full layer and once per removal tried. A
    /// budget spent on the last removal keeps the layer pruned so far.
    #[test]
    fn a_budget_spent_while_pruning_keeps_the_pruned_layer() {
        with_input(|input| {
            for rebuilds in [4, 5] {
                let ctl = Control::new();
                let mut budget = Budget::new(rebuilds * input.rebuild_cost(), &ctl);
                let mut st = State::new(input);
                st.keep_all_needed();
                prune(&mut st, &mut budget);
                let layer = st.layer(SearchSeed::Greedy, &mut budget);
                assert_eq!(layer.cells, HashSet::from([(0, 0), (1, 0)]), "{rebuilds}");
                assert_eq!(layer.entailed, 2);
                assert_eq!(budget.hit, rebuilds == 4);
            }
        });
    }

    #[test]
    fn a_row_counts_the_self_loop_of_a_single_activity() {
        with_input(|input| {
            let row = TypeRow::build(input, 1, vec![0, 1]);
            assert_eq!(row.arcs(&[true, false]), 1);
            assert_eq!(row.arcs(&[true, true]), 2);
            assert_eq!(TypeRow::build(input, 0, vec![0, 1]).arcs(&[true, false]), 0);
        });
    }
}
