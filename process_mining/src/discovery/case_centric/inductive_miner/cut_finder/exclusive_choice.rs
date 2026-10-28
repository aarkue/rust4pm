//! Detection of exclusive choice (`×`) cuts.

use crate::core::process_models::process_tree::OperatorType;

use super::super::dfg::ActivityDfg;
use super::super::structures::union_find::UnionFind;
use super::cut::Cut;

/// Finds the maximal exclusive choice cut, if there is one.
///
/// Requirement ×.1 is that no part is connected to any other part, so activities connected by an
/// edge in either direction must share a part. The maximal cut is thus the connected components of
/// the graph read as undirected.
pub fn exclusive_choice_cut(dfg: &ActivityDfg) -> Option<Cut> {
    let n = dfg.len();
    let mut components = UnionFind::new(n);

    for from in 0..n {
        for &to in dfg.successors(from) {
            components.union(from, to as usize);
        }
    }

    let partitions = super::to_partitions(dfg, &components.groups());
    let cut = Cut::new(OperatorType::ExclusiveChoice, partitions);
    cut.is_non_trivial().then_some(cut)
}

#[cfg(test)]
mod test_exclusive_choice_cut {
    use super::super::test_utils::{cut_of, parts};
    use super::*;

    fn cut(traces: &[&[&str]]) -> Option<Vec<Vec<String>>> {
        cut_of(traces, exclusive_choice_cut)
    }

    #[test]
    fn test_branches() {
        assert_eq!(
            cut(&[&["b", "d"], &["c", "e"]]),
            parts(&[&["b", "d"], &["c", "e"]])
        );
        assert_eq!(
            cut(&[&["b", "e"], &["c", "f"], &["d", "g"]]),
            parts(&[&["b", "e"], &["c", "f"], &["d", "g"]])
        );
        assert_eq!(cut(&[&["a"], &["e"]]), parts(&[&["a"], &["e"]]));
    }

    #[test]
    fn test_no_cut() {
        // A sequence and a concurrency are both connected, so they are one component.
        assert_eq!(cut(&[&["a", "b", "c"]]), None);
        assert_eq!(cut(&[&["a", "b"], &["b", "a"]]), None);
        assert_eq!(cut(&[]), None);
        assert_eq!(cut(&[&[]]), None);
    }
}
