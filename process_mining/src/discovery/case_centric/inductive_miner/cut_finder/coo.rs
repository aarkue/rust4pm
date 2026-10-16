//! Telling concurrency, inclusive choice and optionality apart (the coo abstraction).

use std::collections::HashMap;

use crate::core::process_models::process_tree::OperatorType;

use super::super::log::{ActivityID, ActivityLog};
use super::cut::{part_lookup, Cut};

/// Regroups the parts of a concurrent cut by how the traces combine them, and reports the result
/// as `∧` or `∨`, as `cooCut` of `IMa` does (§5.6.5, §6.4.2.1).
///
/// The directly-follows graph cannot tell `∧`, `∨` and optional children apart (Requirement Cb.5),
/// so this looks at which parts each trace touches. While more than two parts remain, two of them
/// are merged if they are interchangeable, always occur together, or one is optional and implies
/// the other. The remaining parts form an `∨` if two of them are interchangeable, and an `∧`
/// otherwise.
///
/// If no two of more than two parts are related, the thesis takes `∧`. When every part is skipped
/// often enough to become optional, by at least a fraction `noise_threshold` of the traces as in
/// the empty-traces step, that `∧` accepts the empty trace, which the log never shows, so this
/// takes `∨` instead. For `⟨a,b⟩, ⟨b,c⟩, ⟨a,c⟩, ⟨a,b,c⟩` that is `∨(a, b, c)` in place of
/// `∧(×(τ, a), ×(τ, b), ×(τ, c))`.
pub fn coo_cut(log: &ActivityLog, cut: Cut, noise_threshold: f64) -> Cut {
    let mut parts = cut.partitions().to_vec();
    let mut combinations = Combinations::of(log, &parts);

    while parts.len() > 2 {
        let Some((kept, merged)) = combinations.related_pair() else {
            break;
        };
        let part = parts.remove(merged);
        parts[kept].extend(part);
        parts[kept].sort_unstable();
        combinations.merge(kept, merged);
    }

    let n = parts.len();
    let interchangeable = (0..n).any(|a| (a + 1..n).any(|b| combinations.interchangeable(a, b)));
    let noise = noise_threshold * log.num_traces() as f64;
    let all_optional = (0..n).all(|part| {
        let skipped = combinations.skipped_by(part);
        skipped > 0 && skipped as f64 >= noise
    });
    let operator = if interchangeable || all_optional {
        OperatorType::InclusiveChoice
    } else {
        OperatorType::Concurrency
    };
    Cut::new(operator, parts)
}

/// The parts each trace touches (the activity set log of Definition 5.16), with the number of
/// traces per combination. Empty traces are settled before cut detection, so no combination is
/// empty.
struct Combinations {
    counts: HashMap<Vec<bool>, u64>,
    parts: usize,
}

impl Combinations {
    fn of(log: &ActivityLog, parts: &[Vec<ActivityID>]) -> Self {
        let part_of = part_lookup(parts, log.alphabet_size());
        let mut counts = HashMap::new();
        for variant in log.variants() {
            let mut present = vec![false; parts.len()];
            for &activity in &variant.activities {
                if let Some(seen) = present.get_mut(part_of[activity]) {
                    *seen = true;
                }
            }
            *counts.entry(present).or_insert(0) += variant.count;
        }
        Self {
            counts,
            parts: parts.len(),
        }
    }

    fn combinations(&self) -> impl Iterator<Item = &Vec<bool>> {
        self.counts.keys()
    }

    /// Whether the traces also show `combination` with the given parts set to present or absent.
    fn shows_with(&self, combination: &[bool], changes: &[(usize, bool)]) -> bool {
        let mut changed = combination.to_vec();
        for &(part, present) in changes {
            changed[part] = present;
        }
        self.counts.contains_key(&changed)
    }

    /// `a ⇒ b`: every trace with `a` also has `b`.
    fn implies(&self, a: usize, b: usize) -> bool {
        self.combinations().all(|t| !t[a] || t[b])
    }

    /// `?a`: dropping `a` from a combination gives another one the traces show.
    fn optional(&self, a: usize) -> bool {
        self.combinations()
            .all(|t| !t[a] || self.shows_with(t, &[(a, false)]))
    }

    /// `a ∨ b`: wherever `a` or `b` occurs, the traces also show the same combination with only `a`,
    /// only `b`, and both.
    fn interchangeable(&self, a: usize, b: usize) -> bool {
        self.combinations().all(|t| {
            !(t[a] || t[b])
                || [(true, true), (true, false), (false, true)]
                    .iter()
                    .all(|&(with_a, with_b)| self.shows_with(t, &[(a, with_a), (b, with_b)]))
        })
    }

    /// The first pair related by `∨`, `∧` or `∧?`, as `(kept, merged)` with `kept < merged`.
    ///
    /// `a ∧? b` holds if `a` is optional and implies `b`, unless some other part `c` also implies
    /// `b` without `b` implying it. Such a `c` has to be grouped first. The thesis also asks `c` to
    /// be connected both ways to `a` and `b`, which parts of a concurrent cut always are.
    fn related_pair(&self) -> Option<(usize, usize)> {
        let n = self.parts;
        let mut implies = vec![false; n * n];
        for a in 0..n {
            for b in 0..n {
                implies[a * n + b] = a != b && self.implies(a, b);
            }
        }
        let implies = |a: usize, b: usize| implies[a * n + b];
        let optionally_concurrent = |a: usize, b: usize| {
            implies(a, b)
                && self.optional(a)
                && !(0..n).any(|c| c != a && c != b && implies(c, b) && !implies(b, c))
        };

        (0..n)
            .flat_map(|a| (a + 1..n).map(move |b| (a, b)))
            .find(|&(a, b)| {
                (implies(a, b) && implies(b, a))
                    || optionally_concurrent(a, b)
                    || optionally_concurrent(b, a)
                    || self.interchangeable(a, b)
            })
    }

    /// Merges part `merged` into part `kept`, removing it from every combination.
    fn merge(&mut self, kept: usize, merged: usize) {
        let mut counts = HashMap::with_capacity(self.counts.len());
        for (mut combination, count) in self.counts.drain() {
            combination[kept] |= combination[merged];
            combination.remove(merged);
            *counts.entry(combination).or_insert(0) += count;
        }
        self.counts = counts;
        self.parts -= 1;
    }

    /// The number of traces without `part`.
    fn skipped_by(&self, part: usize) -> u64 {
        self.counts
            .iter()
            .filter(|(combination, _)| !combination[part])
            .map(|(_, count)| count)
            .sum()
    }
}

#[cfg(test)]
mod test_coo_cut {
    use super::super::super::dfg::ActivityDfg;
    use super::super::super::log::test_utils::log_of;
    use super::super::concurrent_cut;
    use super::*;

    /// The operator and parts of the coo cut, as names.
    fn coo(traces: &[&[&str]]) -> (OperatorType, Vec<Vec<String>>) {
        let (labels, log) = log_of(traces);
        let cut = concurrent_cut(&ActivityDfg::discover(&log), None).expect("a concurrent cut");
        let cut = coo_cut(&log, cut, 0.0);
        let parts = cut
            .partitions()
            .iter()
            .map(|part| part.iter().map(|&a| labels[a].clone()).collect())
            .collect();
        (cut.operator(), parts)
    }

    fn names(parts: &[&[&str]]) -> Vec<Vec<String>> {
        parts
            .iter()
            .map(|part| part.iter().map(|a| a.to_string()).collect())
            .collect()
    }

    /// Every order of the given activities.
    fn permutations(activities: &[&'static str]) -> Vec<Vec<&'static str>> {
        if activities.len() <= 1 {
            return vec![activities.to_vec()];
        }
        let mut all = Vec::new();
        for (index, &first) in activities.iter().enumerate() {
            let mut rest = activities.to_vec();
            rest.remove(index);
            for mut tail in permutations(&rest) {
                tail.insert(0, first);
                all.push(tail);
            }
        }
        all
    }

    #[test]
    fn test_two_parts() {
        use OperatorType::{Concurrency, InclusiveChoice};
        assert_eq!(
            coo(&[&["a", "b"], &["b", "a"]]),
            (Concurrency, names(&[&["a"], &["b"]]))
        );
        assert_eq!(
            coo(&[&["a", "b"], &["b", "a"], &["a"], &["b"]]),
            (InclusiveChoice, names(&[&["a"], &["b"]]))
        );
        // b is optional, a is not.
        assert_eq!(
            coo(&[&["a", "b"], &["b", "a"], &["a"]]),
            (Concurrency, names(&[&["a"], &["b"]]))
        );
    }

    /// L108 (§6.4.1): b and c only occur with a, and c only with b, so a, b and c share a part.
    /// Which of the interchangeable d and e joins them is open, as in the thesis.
    #[test]
    fn test_nested_optionality() {
        let mut traces: Vec<Vec<&str>> = Vec::new();
        for combination in [
            &["a"] as &[&str],
            &["a", "b"],
            &["a", "b", "c"],
            &["d"],
            &["e"],
            &["d", "e"],
            &["a", "d"],
            &["a", "b", "d"],
            &["a", "b", "c", "d"],
            &["a", "e"],
            &["a", "b", "e"],
            &["a", "b", "c", "e"],
            &["a", "d", "e"],
            &["a", "b", "d", "e"],
            &["a", "b", "c", "d", "e"],
        ] {
            traces.extend(permutations(combination));
        }
        let traces: Vec<&[&str]> = traces.iter().map(Vec::as_slice).collect();

        let (operator, parts) = coo(&traces);
        assert_eq!(operator, OperatorType::InclusiveChoice);
        assert_eq!(parts.len(), 2);
        assert!(parts.iter().any(|part| ["a", "b", "c"]
            .iter()
            .all(|a| part.contains(&a.to_string()))));
    }

    #[test]
    fn test_no_relation_takes_inclusive_choice_if_all_parts_are_skipped() {
        let mut traces: Vec<Vec<&str>> = Vec::new();
        for combination in [
            &["a", "b"] as &[&str],
            &["b", "c"],
            &["a", "c"],
            &["a", "b", "c"],
        ] {
            traces.extend(permutations(combination));
        }
        let traces: Vec<&[&str]> = traces.iter().map(Vec::as_slice).collect();
        assert_eq!(
            coo(&traces),
            (
                OperatorType::InclusiveChoice,
                names(&[&["a"], &["b"], &["c"]])
            )
        );
    }
}
