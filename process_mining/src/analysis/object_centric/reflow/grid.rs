//! Which recorded cells reconstruct which, per activity.

use rayon::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::log::{LogView, Ot};
use super::schema::{Schema, NONE};
use crate::utils::control::{Cancelled, Control};

/// A recorded object-to-object relation, by qualifier and direction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct QualifiedRelation {
    /// The object-to-object qualifier.
    pub qualifier: String,
    /// Whether the recorded edges run from the reconstructed type to the reconstructing one.
    pub reversed: bool,
}

/// How the objects of one type at an event are recomputed from those of another type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ReconstructionVia {
    /// The objects the union of these recorded relations relates the source objects to.
    Recorded {
        /// The relations, at least one.
        relations: Vec<QualifiedRelation>,
    },
    /// The images of the source objects under a function of the object schema.
    Image,
    /// The objects a function of the object schema maps onto the source objects.
    Preimage,
}

/// A reconstruction of one recorded cell from another at the same activity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Reconstruction {
    /// How the objects are recomputed.
    pub via: ReconstructionVia,
    /// Share of the activity's events at which the recomputed objects are exactly the recorded
    /// ones, at least the determination threshold.
    pub share: f64,
}

/// The recorded cells of one activity and which reconstructs which.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub(crate) struct ActivityGrid {
    /// Object types the activity's events name, sorted.
    pub(crate) present: Vec<Ot>,
    /// Participations per present type.
    pub(crate) counts: Vec<usize>,
    /// Per present type, the fewest and most objects of it an event of the activity has.
    pub(crate) objects: Vec<(usize, usize)>,
    /// `recon[i * k + j]`: how the cell of `present[j]` is reconstructed from that of
    /// `present[i]`, for `k` present types.
    pub(crate) recon: Vec<Option<Reconstruction>>,
}

impl ActivityGrid {
    pub(crate) fn slot(&self, t: Ot) -> Option<usize> {
        self.present.iter().position(|x| *x == t)
    }

    pub(crate) fn recon(&self, from: usize, to: usize) -> Option<&Reconstruction> {
        self.recon[from * self.present.len() + to].as_ref()
    }

    /// For every present type, the slot it is reconstructed from on a shortest chain from the
    /// flowing slots, or `None` if it flows or nothing reaches it.
    ///
    /// The chain is walked in rounds, and in each round a cell is reached from the lowest slot
    /// that reaches it.
    pub(crate) fn determining(&self, flows: &[bool]) -> Vec<Option<usize>> {
        let k = self.present.len();
        let mut have = flows.to_vec();
        let mut via = vec![None; k];
        loop {
            let reached: Vec<(usize, usize)> = (0..k)
                .filter(|j| !have[*j])
                .filter_map(|j| {
                    (0..k)
                        .find(|i| have[*i] && self.recon(*i, j).is_some())
                        .map(|i| (j, i))
                })
                .collect();
            if reached.is_empty() {
                return via;
            }
            for (j, i) in reached {
                via[j] = Some(i);
                have[j] = true;
            }
        }
    }
}

/// The objects of each type at one event, as positions among the objects of the type.
type EventObjects = Vec<(Ot, Vec<u32>)>;

fn of_type(event: &EventObjects, t: Ot) -> &[u32] {
    event
        .iter()
        .find(|(x, _)| *x == t)
        .map_or(&[], |(_, objs)| objs.as_slice())
}

/// Check every ordered pair of recorded cells at every activity.
pub(crate) fn build_grid(
    view: &LogView,
    schema: &Schema,
    theta: f64,
    ctl: &Control<'_>,
) -> Result<Vec<ActivityGrid>, Cancelled> {
    let counter = ctl.counter("Building activity grid", Some(view.activities.len() as u64));
    let grid = (0..view.activities.len())
        .into_par_iter()
        .map(|a| {
            if ctl.is_cancelled() {
                return ActivityGrid {
                    present: Vec::new(),
                    counts: Vec::new(),
                    objects: Vec::new(),
                    recon: Vec::new(),
                };
            }
            let events: Vec<EventObjects> = view.evs_of_act[a]
                .iter()
                .map(|e| {
                    view.objects_by_type(*e)
                        .map(|(t, objs)| {
                            (t, objs.iter().map(|o| view.ob_local[*o as usize]).collect())
                        })
                        .collect()
                })
                .collect();
            let mut present: Vec<Ot> = events.iter().flatten().map(|(t, _)| *t).collect();
            present.sort_unstable();
            present.dedup();
            let counts: Vec<usize> = present
                .iter()
                .map(|t| events.iter().map(|ev| of_type(ev, *t).len()).sum())
                .collect();
            let objects: Vec<(usize, usize)> = present
                .iter()
                .map(|t| {
                    events.iter().fold((usize::MAX, 0), |(lo, hi), ev| {
                        let n = of_type(ev, *t).len();
                        (lo.min(n), hi.max(n))
                    })
                })
                .collect();
            let k = present.len();
            let mut recon = vec![None; k * k];
            for i in 0..k {
                for j in 0..k {
                    if i != j {
                        recon[i * k + j] =
                            reconstructs(&events, present[i], present[j], schema, theta);
                    }
                }
            }
            counter.tick();
            ActivityGrid {
                present,
                counts,
                objects,
                recon,
            }
        })
        .collect();
    ctl.check()?;
    Ok(grid)
}

/// Sort and deduplicate.
fn set(mut v: Vec<u32>) -> Vec<u32> {
    v.sort_unstable();
    v.dedup();
    v
}

/// The share of events at which `ok` holds, given the objects of `s` and `t`, if it reaches
/// `theta`. An event naming neither type is recomputed exactly, one naming only one of them is
/// not.
fn share_at_least(
    events: &[EventObjects],
    s: Ot,
    t: Ot,
    theta: f64,
    mut ok: impl FnMut(&[u32], &[u32]) -> bool,
) -> Option<f64> {
    if events.is_empty() {
        return Some(1.0);
    }
    let good = events
        .iter()
        .filter(|ev| {
            let (os, ot) = (of_type(ev, s), of_type(ev, t));
            match (os.is_empty(), ot.is_empty()) {
                (true, true) => true,
                (false, false) => ok(os, ot),
                _ => false,
            }
        })
        .count();
    let share = good as f64 / events.len() as f64;
    (good as f64 >= theta * events.len() as f64).then_some(share)
}

/// Whether the cell of `t` is reconstructed from the cell of `s` at a share `theta` of the
/// events: by a union of recorded relations, the image under a function, or the preimage under
/// a function (tried in this order).
///
/// A recorded relation joins the union only if its image never exceeds the objects of `t` at
/// any event, and the union of all such relations is the largest union that may work.
fn reconstructs(
    events: &[EventObjects],
    s: Ot,
    t: Ot,
    schema: &Schema,
    theta: f64,
) -> Option<Reconstruction> {
    if let Some(relations) = schema.recorded.get(&(s, t)) {
        let admissible: Vec<_> = relations
            .iter()
            .filter(|r| {
                events.iter().all(|ev| {
                    let ot = of_type(ev, t);
                    of_type(ev, s)
                        .iter()
                        .flat_map(|x| r.relation.image(*x))
                        .all(|y| ot.binary_search(y).is_ok())
                })
            })
            .collect();
        if !admissible.is_empty() {
            let share = share_at_least(events, s, t, theta, |os, ot| {
                let image = set(admissible
                    .iter()
                    .flat_map(|r| os.iter().flat_map(|x| r.relation.image(*x)))
                    .copied()
                    .collect());
                image == ot
            });
            if let Some(share) = share {
                return Some(Reconstruction {
                    via: ReconstructionVia::Recorded {
                        relations: admissible
                            .iter()
                            .map(|r| QualifiedRelation {
                                qualifier: r.qualifier.clone(),
                                reversed: r.reversed,
                            })
                            .collect(),
                    },
                    share,
                });
            }
        }
    }

    for f in schema.functions.get(&(s, t)).into_iter().flatten() {
        let share = share_at_least(events, s, t, theta, |os, ot| {
            os.iter().all(|x| f[*x as usize] != NONE)
                && set(os.iter().map(|x| f[*x as usize]).collect()) == ot
        });
        if let Some(share) = share {
            return Some(Reconstruction {
                via: ReconstructionVia::Image,
                share,
            });
        }
    }

    let functions = schema.functions.get(&(t, s)).into_iter().flatten();
    let sizes = schema.preimage_sizes.get(&(t, s)).into_iter().flatten();
    for (g, sizes) in functions.zip(sizes) {
        let share = share_at_least(events, s, t, theta, |os, ot| {
            ot.iter().all(|y| os.binary_search(&g[*y as usize]).is_ok())
                && os
                    .iter()
                    .map(|x| sizes[*x as usize] as usize)
                    .sum::<usize>()
                    == ot.len()
        });
        if let Some(share) = share {
            return Some(Reconstruction {
                via: ReconstructionVia::Preimage,
                share,
            });
        }
    }
    None
}
