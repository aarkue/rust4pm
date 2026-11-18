//! The object schema: functions between object types, read
//! off the object-to-object relation (recorded) or forced by co-participation (derived), and
//! closed under composition.

use std::collections::{BTreeMap, HashMap};

use crate::core::event_data::object_centric::linked_ocel::{LinkedOCELAccess, SlimLinkedOCEL};

use super::log::{LogView, Ot};

/// A target type with fewer objects makes every map into it trivially total.
pub(crate) const MIN_TARGET_OBJECTS: usize = 2;

/// Rounds of composition. Each round composes all functions found so far, so the closure reaches
/// chains of up to `2^4 = 16` functions.
pub(crate) const COMPOSITION_ROUNDS: usize = 4;

/// Most functions kept per pair of object types.
pub(crate) const MAX_FUNCTIONS_PER_PAIR: usize = 8;

/// No image.
pub(crate) const NONE: u32 = u32::MAX;

/// Pairs of objects, by their positions among the objects of their types.
type ObjectPairs = Vec<(u32, u32)>;

/// A function between the objects of two types: for each source object, by its position among
/// the objects of its type, the position of its image, or [`NONE`].
pub(crate) type Func = Vec<u32>;

/// A relation between the objects of two types as adjacency lists over positions.
#[derive(Debug, Clone, Default)]
pub(crate) struct Relation {
    off: Vec<u32>,
    targets: Vec<u32>,
}

impl Relation {
    fn from_pairs(n_sources: usize, mut pairs: Vec<(u32, u32)>) -> Self {
        pairs.sort_unstable();
        pairs.dedup();
        let mut off = vec![0u32; n_sources + 1];
        for (s, _) in &pairs {
            off[*s as usize + 1] += 1;
        }
        for i in 0..n_sources {
            off[i + 1] += off[i];
        }
        Self {
            off,
            targets: pairs.into_iter().map(|(_, t)| t).collect(),
        }
    }

    pub(crate) fn image(&self, s: u32) -> &[u32] {
        &self.targets[self.off[s as usize] as usize..self.off[s as usize + 1] as usize]
    }
}

/// One recorded object-to-object relation between two types under one qualifier, read in one
/// direction.
#[derive(Debug, Clone)]
pub(crate) struct RecordedRelation {
    pub(crate) qualifier: String,
    /// Whether the edges run from the target type to the source type.
    pub(crate) reversed: bool,
    pub(crate) relation: Relation,
}

/// A function of the schema before composition, as expansion's routes start from.
#[derive(Debug, Clone)]
pub(crate) struct BaseMap {
    pub(crate) source: Ot,
    pub(crate) target: Ot,
    pub(crate) f: Func,
}

/// The object schema closed under composition, indexed for the reconstruction checks.
#[derive(Debug, Clone)]
pub(crate) struct Schema {
    /// Recorded functions (one per qualifier and direction) and derived ones, in that order.
    pub(crate) base: Vec<BaseMap>,
    /// The functions per pair of types, base and composed.
    pub(crate) functions: BTreeMap<(Ot, Ot), Vec<Func>>,
    /// For each function of `functions`, the size of the preimage of every target object.
    pub(crate) preimage_sizes: BTreeMap<(Ot, Ot), Vec<Vec<u32>>>,
    /// Recorded relations per pair of types, each qualifier in both directions.
    pub(crate) recorded: BTreeMap<(Ot, Ot), Vec<RecordedRelation>>,
    /// Representative of each type's class of mutually reachable types: the least name.
    pub(crate) rep: Vec<Ot>,
}

impl Schema {
    pub(crate) fn discover(view: &LogView, locel: &SlimLinkedOCEL) -> Self {
        let edges = o2o_edges(locel);
        let mut base = recorded_functions(view, &edges);
        base.extend(derived_functions(view));
        let functions = compose(&base);
        let preimage_sizes = functions
            .iter()
            .map(|(&(s, t), fs)| {
                let sizes = fs
                    .iter()
                    .map(|f| {
                        let mut sizes = vec![0u32; view.objects_of_type[t].len()];
                        for y in f.iter().filter(|y| **y != NONE) {
                            sizes[*y as usize] += 1;
                        }
                        sizes
                    })
                    .collect();
                ((s, t), sizes)
            })
            .collect();
        let recorded = recorded_relations(view, &edges);
        let rep = representatives(view, &functions);
        Self {
            base,
            functions,
            preimage_sizes,
            recorded,
            rep,
        }
    }
}

/// Recorded object-to-object edges `(source, target, qualifier)`.
fn o2o_edges(locel: &SlimLinkedOCEL) -> Vec<(u32, u32, String)> {
    let mut out = Vec::new();
    for o in locel.get_all_obs() {
        for (q, target) in &o.get_ob(locel).relationships {
            out.push((
                o.into_inner(),
                target.into_inner(),
                locel.qualifier_str(*q).to_string(),
            ));
        }
    }
    out
}

/// Functional recorded relations, one candidate per pair of types, qualifier and direction.
///
/// An edge `A -[q]-> B` is read both ways: the reverse direction is named `~q`.
fn recorded_functions(view: &LogView, edges: &[(u32, u32, String)]) -> Vec<BaseMap> {
    let mut by_key: BTreeMap<(Ot, Ot, bool, &str), ObjectPairs> = BTreeMap::new();
    for (s, t, q) in edges {
        let (st, tt) = (view.ob_type[*s as usize], view.ob_type[*t as usize]);
        if st == tt {
            continue;
        }
        let (ls, lt) = (view.ob_local[*s as usize], view.ob_local[*t as usize]);
        by_key.entry((st, tt, false, q)).or_default().push((ls, lt));
        by_key.entry((tt, st, true, q)).or_default().push((lt, ls));
    }
    // Ordered as the names `q` and `~q` sort, which decides the order of composition.
    let mut keys: Vec<_> = by_key.into_iter().collect();
    keys.sort_by_cached_key(|((s, t, reversed, q), _)| {
        let name = if *reversed {
            format!("~{q}")
        } else {
            q.to_string()
        };
        (*s, *t, name)
    });
    let mut out = Vec::new();
    for ((st, tt, _, _), mut pairs) in keys {
        if view.objects_of_type[tt].len() < MIN_TARGET_OBJECTS {
            continue;
        }
        pairs.sort_unstable();
        pairs.dedup();
        if pairs.windows(2).any(|w| w[0].0 == w[1].0) {
            continue;
        }
        let mut f = vec![NONE; view.objects_of_type[st].len()];
        for (s, t) in pairs {
            f[s as usize] = t;
        }
        out.push(BaseMap {
            source: st,
            target: tt,
            f,
        });
    }
    out
}

/// What the running intersection of one source object's target objects holds so far.
#[derive(Clone)]
enum Cand {
    One(u32),
    Many(Vec<u32>),
    Dead,
}

/// Derived functions, in one pass over the events: per source object and target type, the
/// intersection of the target objects of the events naming both. A source object enters the
/// function where the intersection holds exactly one object.
fn derived_functions(view: &LogView) -> Vec<BaseMap> {
    let n_types = view.types.len();
    let eligible: Vec<bool> = view
        .objects_of_type
        .iter()
        .map(|v| v.len() >= MIN_TARGET_OBJECTS)
        .collect();
    let mut cand: Vec<Vec<(Ot, Cand)>> = vec![Vec::new(); view.n_objects()];
    let mut groups: Vec<(Ot, &[u32])> = Vec::new();
    for e in 0..view.n_events() as u32 {
        groups.clear();
        groups.extend(view.objects_by_type(e));
        for (tt, targets) in &groups {
            if !eligible[*tt] {
                continue;
            }
            for (st, sources) in &groups {
                if st == tt {
                    continue;
                }
                for o in *sources {
                    let slots = &mut cand[*o as usize];
                    match slots.iter_mut().find(|(t, _)| t == tt) {
                        None => slots.push((
                            *tt,
                            if targets.len() == 1 {
                                Cand::One(targets[0])
                            } else {
                                Cand::Many(targets.to_vec())
                            },
                        )),
                        Some((_, c)) => {
                            *c = match std::mem::replace(c, Cand::Dead) {
                                Cand::Dead => Cand::Dead,
                                Cand::One(x) => {
                                    if targets.binary_search(&x).is_ok() {
                                        Cand::One(x)
                                    } else {
                                        Cand::Dead
                                    }
                                }
                                Cand::Many(mut cur) => {
                                    cur.retain(|x| targets.binary_search(x).is_ok());
                                    match cur.len() {
                                        0 => Cand::Dead,
                                        1 => Cand::One(cur[0]),
                                        _ => Cand::Many(cur),
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    let mut out = Vec::new();
    for st in 0..n_types {
        for (tt, ok) in eligible.iter().enumerate() {
            if st == tt || !ok {
                continue;
            }
            let mut f = vec![NONE; view.objects_of_type[st].len()];
            let mut any = false;
            for (i, s) in view.objects_of_type[st].iter().enumerate() {
                if let Some((_, Cand::One(x))) = cand[*s as usize].iter().find(|(t, _)| *t == tt) {
                    f[i] = view.ob_local[*x as usize];
                    any = true;
                }
            }
            if any {
                out.push(BaseMap {
                    source: st,
                    target: tt,
                    f,
                });
            }
        }
    }
    out
}

/// Close the functions under composition, keeping at most [`MAX_FUNCTIONS_PER_PAIR`] distinct
/// ones per pair.
///
/// Which composition reaches a pair first decides which functions are kept once the cap is hit,
/// so pairs are visited in a fixed order. A round only composes what the round before could not:
/// pairs of functions at least one of which it added.
fn compose(base: &[BaseMap]) -> BTreeMap<(Ot, Ot), Vec<Func>> {
    let mut by_pair: BTreeMap<(Ot, Ot), Vec<Func>> = BTreeMap::new();
    for m in base {
        let fs = by_pair.entry((m.source, m.target)).or_default();
        if !fs.contains(&m.f) {
            fs.push(m.f.clone());
        }
    }
    // Per pair, how many of its functions the round before found.
    let mut old: HashMap<(Ot, Ot), usize> = HashMap::new();
    for _ in 0..COMPOSITION_ROUNDS {
        let mut from: HashMap<Ot, Vec<(Ot, &Vec<Func>)>> = HashMap::new();
        for ((s, t), fs) in &by_pair {
            from.entry(*s).or_default().push((*t, fs));
        }
        let old_of = |pair: (Ot, Ot)| old.get(&pair).copied().unwrap_or(0);
        let mut added: Vec<((Ot, Ot), Func)> = Vec::new();
        for ((s, t), fs) in &by_pair {
            for (u, gs) in from.get(t).into_iter().flatten() {
                let full = by_pair
                    .get(&(*s, *u))
                    .is_some_and(|fs| fs.len() >= MAX_FUNCTIONS_PER_PAIR);
                if s == u || full {
                    continue;
                }
                let (f_old, g_old) = (old_of((*s, *t)), old_of((*t, *u)));
                for (i, f) in fs.iter().enumerate() {
                    for g in gs.iter().skip(if i < f_old { g_old } else { 0 }) {
                        let composed: Func = f
                            .iter()
                            .map(|y| if *y == NONE { NONE } else { g[*y as usize] })
                            .collect();
                        if composed.iter().any(|z| *z != NONE) {
                            added.push(((*s, *u), composed));
                        }
                    }
                }
            }
        }
        old = by_pair.iter().map(|(pair, fs)| (*pair, fs.len())).collect();
        let mut grew = false;
        for (pair, f) in added {
            let fs = by_pair.entry(pair).or_default();
            if fs.len() < MAX_FUNCTIONS_PER_PAIR && !fs.contains(&f) {
                fs.push(f);
                grew = true;
            }
        }
        if !grew {
            break;
        }
    }
    by_pair
}

/// The recorded object-to-object relations per pair of types, each qualifier read in both
/// directions.
fn recorded_relations(
    view: &LogView,
    edges: &[(u32, u32, String)],
) -> BTreeMap<(Ot, Ot), Vec<RecordedRelation>> {
    let mut by_key: BTreeMap<(Ot, Ot, &str, bool), ObjectPairs> = BTreeMap::new();
    for (s, t, q) in edges {
        let (st, tt) = (view.ob_type[*s as usize], view.ob_type[*t as usize]);
        if st == tt {
            continue;
        }
        let (ls, lt) = (view.ob_local[*s as usize], view.ob_local[*t as usize]);
        by_key.entry((st, tt, q, false)).or_default().push((ls, lt));
        by_key.entry((tt, st, q, true)).or_default().push((lt, ls));
    }
    let mut out: BTreeMap<(Ot, Ot), Vec<RecordedRelation>> = BTreeMap::new();
    for ((st, tt, q, reversed), pairs) in by_key {
        out.entry((st, tt)).or_default().push(RecordedRelation {
            qualifier: q.to_string(),
            reversed,
            relation: Relation::from_pairs(view.objects_of_type[st].len(), pairs),
        });
    }
    out
}

/// The least type name among the types each type reaches and is reached from.
fn representatives(view: &LogView, functions: &BTreeMap<(Ot, Ot), Vec<Func>>) -> Vec<Ot> {
    let n = view.types.len();
    let mut reach = vec![vec![false; n]; n];
    for (s, t) in functions.keys() {
        reach[*s][*t] = true;
    }
    for k in 0..n {
        let through = reach[k].clone();
        for row in reach.iter_mut().filter(|row| row[k]) {
            for (x, y) in row.iter_mut().zip(&through) {
                *x |= *y;
            }
        }
    }
    (0..n)
        .map(|t| {
            (0..n)
                .filter(|s| *s == t || (reach[t][*s] && reach[*s][t]))
                .min_by_key(|s| &view.types[*s])
                .unwrap_or(t)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every round composes every pair of functions.
    fn compose_naive(base: &[BaseMap]) -> BTreeMap<(Ot, Ot), Vec<Func>> {
        let mut by_pair: BTreeMap<(Ot, Ot), Vec<Func>> = BTreeMap::new();
        for m in base {
            let fs = by_pair.entry((m.source, m.target)).or_default();
            if !fs.contains(&m.f) {
                fs.push(m.f.clone());
            }
        }
        for _ in 0..COMPOSITION_ROUNDS {
            let mut added: Vec<((Ot, Ot), Func)> = Vec::new();
            for ((s, t), fs) in &by_pair {
                for ((_, u), gs) in by_pair.range((*t, 0)..=(*t, Ot::MAX)) {
                    if s == u {
                        continue;
                    }
                    for f in fs {
                        for g in gs {
                            let composed: Func = f
                                .iter()
                                .map(|y| if *y == NONE { NONE } else { g[*y as usize] })
                                .collect();
                            if composed.iter().any(|z| *z != NONE) {
                                added.push(((*s, *u), composed));
                            }
                        }
                    }
                }
            }
            let mut grew = false;
            for (pair, f) in added {
                let fs = by_pair.entry(pair).or_default();
                if fs.len() < MAX_FUNCTIONS_PER_PAIR && !fs.contains(&f) {
                    fs.push(f);
                    grew = true;
                }
            }
            if !grew {
                break;
            }
        }
        by_pair
    }

    /// Random partial functions between four types of five objects, many enough that some pairs
    /// reach the cap.
    #[test]
    fn composing_only_new_functions_keeps_the_same_ones() {
        let mut state: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = |n: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % n
        };
        let mut capped = false;
        for _ in 0..40 {
            let base: Vec<BaseMap> = (0..12)
                .map(|_| {
                    let (source, target) = (next(4) as Ot, next(4) as Ot);
                    let f = (0..5)
                        .map(|_| match next(6) {
                            5 => NONE,
                            y => y as u32,
                        })
                        .collect();
                    BaseMap { source, target, f }
                })
                .filter(|m| m.source != m.target)
                .collect();
            let fast = compose(&base);
            capped |= fast.values().any(|fs| fs.len() == MAX_FUNCTIONS_PER_PAIR);
            assert_eq!(fast, compose_naive(&base));
        }
        assert!(capped);
    }
}
