//! Detection of loop (`↺`) cuts.

use crate::core::process_models::process_tree::OperatorType;

use super::super::dfg::ActivityDfg;
use super::super::structures::union_find::UnionFind;
use super::cut::Cut;

/// Finds the maximal loop cut, if there is one. The first part is the loop body, the rest are the
/// redo parts.
///
/// Four requirements:
///
/// ```text
/// ↺.1  Start ∪ End ⊆ Σ1
/// ↺.2  ∀ j ≥ 2, a ∈ Σ1, b ∈ Σj:  a ↦ b ⇒ a ∈ End   ∧   b ↦ a ⇒ a ∈ Start
/// ↺.3  ∀ i ≠ j ≥ 2, a ∈ Σi, b ∈ Σj:  a ↛ b  ∧  b ↛ a
/// ↺.4  ∀ i ≥ 2, b ∈ Σi:  (∃ a ∈ Start: b ↦ a) ⇒ (∀ a ∈ Start: b ↦ a)
///      ∀ i ≥ 2, b ∈ Σi:  (∃ a ∈ End:   a ↦ b) ⇒ (∀ a ∈ End:   a ↦ b)
/// ```
///
/// The body starts as `Start ∪ End` (↺.1) and the rest is grouped into connected components (↺.3).
/// Components violating ↺.2 or ↺.4 are then absorbed into the body. One pass is enough: absorbing
/// only adds activities to the body, components are unconnected by construction so the new
/// activities cannot violate ↺.2 for another component, and ↺.4 only refers to the unchanged start
/// and end activities.
pub fn loop_cut(dfg: &ActivityDfg) -> Option<Cut> {
    let n = dfg.len();
    let body: Vec<bool> = (0..n).map(|a| dfg.is_start(a) || dfg.is_end(a)).collect();
    let starts: Vec<usize> = dfg.start_activities().collect();
    let ends: Vec<usize> = dfg.end_activities().collect();
    let body_root = (0..n).find(|&a| body[a])?;

    let mut parts = UnionFind::new(n);
    for (activity, &in_body) in body.iter().enumerate() {
        if in_body {
            parts.union(body_root, activity);
        }
    }

    // ↺.3: the redo parts are the connected components of the remaining activities.
    for a in 0..n {
        if body[a] {
            continue;
        }
        for &b in dfg.successors(a) {
            if !body[b as usize] {
                parts.union(a, b as usize);
            }
        }
    }

    // ↺.2: the body holds only start and end activities so far, so only an edge out of a start
    // activity that is no end activity, or into an end activity that is no start one, violates it.
    for &a in &starts {
        if !dfg.is_end(a) {
            for &b in dfg.successors(a) {
                parts.union(body_root, b as usize);
            }
        }
    }
    for &a in &ends {
        if !dfg.is_start(a) {
            for &b in dfg.predecessors(a) {
                parts.union(body_root, b as usize);
            }
        }
    }

    // ↺.4
    for (activity, &in_body) in body.iter().enumerate() {
        if in_body {
            continue;
        }
        let to_start = dfg
            .successors(activity)
            .iter()
            .filter(|&&s| dfg.is_start(s as usize))
            .count();
        let from_end = dfg
            .predecessors(activity)
            .iter()
            .filter(|&&e| dfg.is_end(e as usize))
            .count();
        if is_partial(to_start, starts.len()) || is_partial(from_end, ends.len()) {
            parts.union(body_root, activity);
        }
    }

    let mut groups = parts.groups();
    let body_index = groups
        .iter()
        .position(|group| group.contains(&body_root))
        .expect("every activity is in exactly one group");
    groups.swap(0, body_index);
    groups[1..].sort_unstable_by_key(|group| group[0]);

    let cut = Cut::new(OperatorType::Loop, super::to_partitions(dfg, &groups));
    cut.is_non_trivial().then_some(cut)
}

/// Returns `true` if `connected` is at least one but not all of `total`.
fn is_partial(connected: usize, total: usize) -> bool {
    connected > 0 && connected < total
}

#[cfg(test)]
mod test_loop_cut {
    use super::super::test_utils::{cut_of, parts};
    use super::*;

    fn cut(traces: &[&[&str]]) -> Option<Vec<Vec<String>>> {
        cut_of(traces, loop_cut)
    }

    #[test]
    fn test_redo_parts() {
        assert_eq!(
            cut(&[&["a", "c"], &["a", "c", "b", "a", "c"]]),
            parts(&[&["a", "c"], &["b"]])
        );
        assert_eq!(
            cut(&[&["a", "c"], &["a", "c", "b", "d", "a", "c"]]),
            parts(&[&["a", "c"], &["b", "d"]])
        );
        // c and d are alternative redo parts and are not connected to each other.
        assert_eq!(
            cut(&[
                &["a", "b"],
                &["a", "b", "c", "a", "b"],
                &["a", "b", "d", "a", "b"],
            ]),
            parts(&[&["a", "b"], &["c"], &["d"]])
        );
    }

    #[test]
    fn test_body_absorbs_activity_after_a_start_activity() {
        // d follows the start activity a, so it cannot be a redo part (↺.2).
        assert_eq!(
            cut(&[
                &["a", "b"],
                &["a", "b", "c", "a", "b"],
                &["a", "d", "b"],
                &["a", "d", "b", "c", "a", "d", "b"],
            ]),
            parts(&[&["a", "b", "d"], &["c"]])
        );
    }

    #[test]
    fn test_only_the_outer_loop_is_cut() {
        // The inner loop over b stays in the body, only g is a redo part.
        assert_eq!(
            cut(&[
                &["s", "a", "c", "e"],
                &["s", "a", "c", "b", "a", "c", "e"],
                &["s", "a", "c", "e", "g", "s", "a", "c", "e"],
            ]),
            parts(&[&["a", "b", "c", "e", "s"], &["g"]])
        );
    }

    #[test]
    fn test_redo_part_must_connect_to_all_start_activities() {
        // c only leads back to a, which ↺.4 forbids, so it is absorbed and no cut remains.
        assert_eq!(
            cut(&[&["a", "x"], &["b", "x"], &["a", "x", "c", "a", "x"]]),
            None
        );
        assert_eq!(
            cut(&[
                &["a", "x"],
                &["b", "x"],
                &["a", "x", "c", "a", "x"],
                &["b", "x", "c", "b", "x"],
            ]),
            parts(&[&["a", "b", "x"], &["c"]])
        );
    }

    #[test]
    fn test_no_cut() {
        assert_eq!(cut(&[&["a", "b", "c"]]), None);
        assert_eq!(cut(&[]), None);
        assert_eq!(cut(&[&[]]), None);
    }
}
