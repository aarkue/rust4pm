//! Expansion: unrecorded cells a route of the object schema writes, kept when
//! the written participations fall inside their objects' lifetimes and show a fact the recorded
//! cells do not entail.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;
use crate::utils::control::{Cancelled, Control};

use super::abstraction::{Abstraction, AbstractionInput, Entailment};
use super::log::{Cell, LogView, Ot, Traces};
use super::schema::{Func, Schema, MAX_FUNCTIONS_PER_PAIR, NONE};

/// Longest chain of schema functions a route composes.
const MAX_ROUTE_LENGTH: usize = 4;

/// Writing one participation, in the work units of the search.
const WRITE_COST: u64 = 20;
/// Reading one object or participation of a type's traces to check what a cell shows, in the
/// work units of the search.
const READ_COST: u64 = 5;

/// A chain of schema functions and the function it computes.
#[derive(Clone)]
struct Route {
    source: Ot,
    target: Ot,
    /// Indices into [`Schema::base`], in application order.
    via: Vec<usize>,
    f: Func,
}

/// Every chain of up to [`MAX_ROUTE_LENGTH`] functions, at most [`MAX_FUNCTIONS_PER_PAIR`]
/// distinct functions per pair of types.
fn chains(schema: &Schema) -> Vec<Route> {
    let mut base: Vec<Route> = Vec::new();
    for (i, m) in schema.base.iter().enumerate() {
        if base
            .iter()
            .any(|r| (r.source, r.target) == (m.source, m.target) && r.f == m.f)
        {
            continue;
        }
        base.push(Route {
            source: m.source,
            target: m.target,
            via: vec![i],
            f: m.f.clone(),
        });
    }
    let mut out: Vec<Route> = base.clone();
    let mut per_pair: HashMap<(Ot, Ot), Vec<usize>> = HashMap::new();
    for (i, r) in out.iter().enumerate() {
        per_pair.entry((r.source, r.target)).or_default().push(i);
    }
    let mut frontier: Vec<usize> = (0..out.len()).collect();
    for _ in 1..MAX_ROUTE_LENGTH {
        let mut grown: Vec<usize> = Vec::new();
        for ri in frontier {
            for b in &base {
                let r = &out[ri];
                if b.source != r.target || b.target == r.source {
                    continue;
                }
                let f: Func =
                    r.f.iter()
                        .map(|y| if *y == NONE { NONE } else { b.f[*y as usize] })
                        .collect();
                if f.iter().all(|z| *z == NONE) {
                    continue;
                }
                let pair = (r.source, b.target);
                let known = per_pair.get(&pair).map_or(&[][..], Vec::as_slice);
                if known.len() >= MAX_FUNCTIONS_PER_PAIR || known.iter().any(|k| out[*k].f == f) {
                    continue;
                }
                let mut via = r.via.clone();
                via.extend_from_slice(&b.via);
                per_pair.entry(pair).or_default().push(out.len());
                grown.push(out.len());
                out.push(Route {
                    source: pair.0,
                    target: pair.1,
                    via,
                    f,
                });
            }
        }
        if grown.is_empty() {
            break;
        }
        frontier = grown;
    }
    out
}

/// One route per pair of types: the chains merged on the objects they agree about. A source
/// object two chains map to different objects is dropped, as the pair determines nothing for
/// it. The route is named by its shortest chain.
fn agreed_routes(schema: &Schema) -> Vec<Route> {
    let mut by_pair: BTreeMap<(Ot, Ot), Vec<Route>> = BTreeMap::new();
    for r in chains(schema) {
        by_pair.entry((r.source, r.target)).or_default().push(r);
    }
    let mut out = Vec::new();
    for ((source, target), mut rs) in by_pair {
        rs.sort_by(|x, y| (x.via.len(), &x.via).cmp(&(y.via.len(), &y.via)));
        let mut f = vec![NONE; rs[0].f.len()];
        let mut split = vec![false; f.len()];
        for r in &rs {
            for (x, y) in r.f.iter().enumerate() {
                if *y == NONE {
                    continue;
                }
                if f[x] == NONE {
                    f[x] = *y;
                } else if f[x] != *y {
                    split[x] = true;
                }
            }
        }
        for (x, s) in split.iter().enumerate() {
            if *s {
                f[x] = NONE;
            }
        }
        if f.iter().all(|y| *y == NONE) {
            continue;
        }
        out.push(Route {
            source,
            target,
            via: rs.swap_remove(0).via,
            f,
        });
    }
    out
}

/// Participations, `(event, object)`.
type Tuples = Vec<(u32, u32)>;

/// An admitted cell and the participations it writes, `(event, object)`.
pub(crate) struct Admitted {
    pub(crate) cell: Cell,
    pub(crate) tuples: Tuples,
}

/// What expansion found.
pub(crate) struct Expansion {
    pub(crate) admitted: Vec<Admitted>,
    /// Unrecorded cells some route writes.
    pub(crate) candidates: usize,
    /// Whether enumerating the candidates would have exceeded the work budget, so nothing was
    /// admitted.
    pub(crate) skipped: bool,
    pub(crate) work: u64,
}

/// The admitted cells, each with the participations of the shortest route
/// that admits it, ties broken by the name of the route's source type.
#[allow(clippy::too_many_arguments)]
pub(crate) fn expand(
    view: &LogView,
    locel: &SlimLinkedOCEL,
    schema: &Schema,
    recorded: &HashMap<Cell, usize>,
    abstraction: &dyn Abstraction,
    recorded_entailment: &dyn Entailment,
    lifetime_share: f64,
    max_work: u64,
    ctl: &Control<'_>,
) -> Result<Expansion, Cancelled> {
    let routes = agreed_routes(schema);
    let n_acts = view.activities.len();
    let applicable = |a: usize, r: &Route| {
        !recorded.contains_key(&(a, r.target)) && recorded.contains_key(&(a, r.source))
    };
    let mut e2o_of_type = vec![0u64; view.types.len()];
    for ((_, t), n) in recorded {
        e2o_of_type[*t] += *n as u64;
    }
    let work: u64 = routes
        .iter()
        .flat_map(|r| {
            (0..n_acts)
                .filter(move |a| applicable(*a, r))
                .map(move |a| (a, r))
        })
        .map(|(a, r)| {
            WRITE_COST * recorded[&(a, r.source)] as u64
                + READ_COST * (view.n_objects() as u64 + e2o_of_type[r.target])
        })
        .sum();
    if work > max_work {
        return Ok(Expansion {
            admitted: Vec::new(),
            candidates: 0,
            skipped: true,
            work,
        });
    }

    // Per candidate cell, every applicable route with what it writes.
    let mut written: BTreeMap<Cell, Vec<(usize, Tuples)>> = BTreeMap::new();
    for (ri, r) in routes.iter().enumerate() {
        ctl.check()?;
        for a in (0..n_acts).filter(|a| applicable(*a, r)) {
            let mut tuples = Vec::new();
            for e in &view.evs_of_act[a] {
                for (t, objs) in view.objects_by_type(*e) {
                    if t != r.source {
                        continue;
                    }
                    for o in objs {
                        let img = r.f[view.ob_local[*o as usize] as usize];
                        if img != NONE {
                            tuples.push((*e, view.objects_of_type[r.target][img as usize]));
                        }
                    }
                }
            }
            tuples.sort_unstable();
            tuples.dedup();
            if !tuples.is_empty() {
                written.entry((a, r.target)).or_default().push((ri, tuples));
            }
        }
    }

    let lifetimes = view.lifetimes();
    let mut candidates_cells: HashSet<Cell> = recorded.keys().copied().collect();
    let candidates = written.len();
    let mut admitted = Vec::new();
    let counter = ctl.counter("Expanding", Some(candidates as u64));
    for (cell, mut options) in written {
        ctl.check()?;
        options.sort_by(|(x, _), (y, _)| {
            let (x, y) = (&routes[*x], &routes[*y]);
            (x.via.len(), &view.types[x.source], &x.via).cmp(&(
                y.via.len(),
                &view.types[y.source],
                &y.via,
            ))
        });
        candidates_cells.insert(cell);
        for (_, tuples) in options {
            let alive = tuples
                .iter()
                .filter(|(e, o)| {
                    let ts = view.ev_time[*e as usize];
                    lifetimes[*o as usize].is_some_and(|(lo, hi)| lo <= ts && ts <= hi)
                })
                .count();
            if (alive as f64) < lifetime_share * tuples.len() as f64 {
                continue;
            }
            let traces = Traces::build(view, locel, &tuples, Some(cell.1));
            let input = AbstractionInput {
                view,
                traces: &traces,
                candidates: &candidates_cells,
                only_type: Some(cell.1),
            };
            let shows_new = abstraction.facts(&input).iter().any(|f| {
                f.object_type == cell.1
                    && f.cells().any(|c| c == cell)
                    && !recorded_entailment.entails(f)
            });
            if shows_new {
                admitted.push(Admitted { cell, tuples });
                break;
            }
        }
        candidates_cells.remove(&cell);
        counter.tick();
    }
    Ok(Expansion {
        admitted,
        candidates,
        skipped: false,
        work,
    })
}
