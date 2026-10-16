//! Detection of sequence (`→`) cuts.

use crate::core::process_models::process_tree::OperatorType;

use super::super::dfg::ActivityDfg;
use super::super::structures::complement_components::complement_components;
use super::super::structures::reachability::Reachability;
use super::cut::Cut;

/// Finds the maximal sequence cut, if there is one.
///
/// Requirement →.1 is that for `i < j`, every `a` in part `i` reaches every `b` in part `j` and no
/// `b` reaches back. Two activities may therefore only be separated if exactly one of them reaches
/// the other; mutually reachable ones (a loop) and mutually unreachable ones (a choice or
/// concurrency) have to be merged. Reachability then orders the parts.
///
/// Almost every pair has to be merged on an unstructured log, so the parts are computed on the
/// sparse complement of the merge relation.
pub fn sequence_cut(dfg: &ActivityDfg) -> Option<Cut> {
    let reaches = Reachability::of(dfg);
    let may_be_separated = |a: usize, b: usize| reaches.reaches(a, b) != reaches.reaches(b, a);

    let mut groups = complement_components(dfg.len(), may_be_separated);
    if groups.len() <= 1 {
        return None;
    }

    // Every activity of a part reaches the same activities outside it, so comparing any two
    // representatives orders the parts.
    groups.sort_by(|left, right| {
        let (a, b) = (left[0], right[0]);
        reaches.reaches(b, a).cmp(&reaches.reaches(a, b))
    });

    let cut = Cut::new(OperatorType::Sequence, super::to_partitions(dfg, &groups));
    cut.is_non_trivial().then_some(cut)
}

/// Merges the parts of a sequence cut that can only occur together with a skippable neighbour,
/// as `sequenceCutStrict` of `IMa` does (§5.6.4, §6.4.2.1).
///
/// Requirement →.1 alone can separate parts that always occur together. For
/// `⟨c,c,c,a,c⟩, ⟨b,b,d,a⟩, ⟨b,c⟩` it separates `{b}` from `{d}`, and the resulting tree accepts a
/// `d` without a preceding `b`, which no trace does. A part `p` that some path skips is a pivot:
/// the parts before it that only lead up to `p`, and the parts after it that are only reached from
/// `p` onwards, are merged into it. `ProM` and `PM4Py` apply this by default.
///
/// Returns `None` if nothing is left to cut.
pub fn strict_sequence_cut(dfg: &ActivityDfg, cut: Cut) -> Option<Cut> {
    let parts = cut.partitions();
    let n = parts.len();
    let lookup = cut.partition_lookup(dfg.alphabet_size());
    let part_of = |local: usize| lookup[dfg.activity(local)] as isize;

    // The earliest part with an edge into each part and the latest part with an edge out of it,
    // where a trace start counts as part -1 and a trace end as part n.
    let mut min_from = vec![isize::MAX; n];
    let mut max_to = vec![isize::MIN; n];
    for start in dfg.start_activities() {
        min_from[part_of(start) as usize] = -1;
    }
    for end in dfg.end_activities() {
        max_to[part_of(end) as usize] = n as isize;
    }
    for (from, to, _) in dfg.edges() {
        let (from, to) = (part_of(from), part_of(to));
        min_from[to as usize] = min_from[to as usize].min(from);
        max_to[from as usize] = max_to[from as usize].max(to);
    }

    // `joined[i]`: parts `i` and `i + 1` end up in the same part.
    let mut joined = vec![false; n.saturating_sub(1)];
    for pivot in 0..n {
        let p = pivot as isize;
        let skippable = max_to[..pivot].iter().any(|&to| to > p)
            || min_from[pivot + 1..].iter().any(|&from| from < p);
        if !skippable {
            continue;
        }
        let mut q = pivot;
        while q > 0 && max_to[q - 1] <= p {
            joined[q - 1] = true;
            q -= 1;
        }
        let mut q = pivot + 1;
        while q < n && min_from[q] >= p {
            joined[q - 1] = true;
            q += 1;
        }
    }

    let mut merged: Vec<Vec<usize>> = Vec::with_capacity(n);
    for (index, part) in parts.iter().enumerate() {
        match merged.last_mut() {
            Some(previous) if joined[index - 1] => previous.extend(part),
            _ => merged.push(part.clone()),
        }
    }
    for part in &mut merged {
        part.sort_unstable();
    }

    let cut = Cut::new(OperatorType::Sequence, merged);
    cut.is_non_trivial().then_some(cut)
}

#[cfg(test)]
mod test_sequence_cut {
    use super::super::test_utils::{cut_of, parts};
    use super::*;

    fn cut(traces: &[&[&str]]) -> Option<Vec<Vec<String>>> {
        cut_of(traces, sequence_cut)
    }

    #[test]
    fn test_simple_sequence() {
        assert_eq!(cut(&[&["a", "b", "c"]]), parts(&[&["a"], &["b"], &["c"]]));
        assert_eq!(cut(&[&["a"]]), None);
    }

    #[test]
    fn test_parts_that_have_to_be_merged() {
        // b and c never reach each other (a choice), and B and C reach each other (concurrency).
        assert_eq!(
            cut(&[&["a", "b", "d"], &["a", "c", "d"]]),
            parts(&[&["a"], &["b", "c"], &["d"]])
        );
        assert_eq!(
            cut(&[&["A", "B", "C", "D"], &["A", "C", "B", "D"]]),
            parts(&[&["A"], &["B", "C"], &["D"]])
        );
        assert_eq!(
            cut(&[&["a", "c", "d"], &["b", "c", "e"]]),
            parts(&[&["a", "b"], &["c"], &["d", "e"]])
        );
        assert_eq!(
            cut(&[&["A", "C"], &["B", "C", "D"], &["B", "D"]]),
            parts(&[&["A", "B"], &["C"], &["D"]])
        );
    }

    #[test]
    fn test_skipped_activity_still_sequences() {
        assert_eq!(
            cut(&[&["a", "b", "c"], &["a", "c"]]),
            parts(&[&["a"], &["b"], &["c"]])
        );
    }

    #[test]
    fn test_strict_merges_parts_that_never_occur_apart() {
        let strict = |traces: &[&[&str]]| -> Option<Vec<Vec<String>>> {
            let (labels, log) = super::super::super::log::test_utils::log_of(traces);
            let dfg = ActivityDfg::discover(&log);
            sequence_cut(&dfg)
                .and_then(|cut| strict_sequence_cut(&dfg, cut))
                .map(|cut| {
                    cut.partitions()
                        .iter()
                        .map(|part| part.iter().map(|&a| labels[a].clone()).collect())
                        .collect()
                })
        };

        // No trace has d without b, so separating the two would allow a d on its own.
        assert_eq!(
            strict(&[
                &["c", "c", "c", "a", "c"],
                &["b", "b", "d", "a"],
                &["b", "c"]
            ]),
            parts(&[&["b", "d"], &["a", "c"]])
        );
        // Both parts are seen with and without the other, so the cut stays as it is.
        assert_eq!(
            strict(&[&["a", "b"], &["b", "c"]]),
            parts(&[&["a"], &["b"], &["c"]])
        );
        assert_eq!(
            strict(&[&["a", "b", "c"], &["a", "c"]]),
            parts(&[&["a"], &["b"], &["c"]])
        );
        // Skipping both parts is what the empty traces decide, so it does not merge them.
        assert_eq!(
            strict(&[&["a", "b"], &["a"], &["b"]]),
            parts(&[&["a"], &["b"]])
        );
    }

    #[test]
    fn test_no_cut() {
        // Concurrency, a choice (handled by the exclusive choice cut first), and a loop.
        assert_eq!(cut(&[&["B", "C"], &["C", "B"]]), None);
        assert_eq!(cut(&[&["a", "b", "c"], &["d"]]), None);
        assert_eq!(
            cut(&[
                &["B", "C"],
                &["C", "B"],
                &["B", "C", "E", "F", "B", "C"],
                &["C", "B", "E", "F", "C", "B"],
            ]),
            None
        );
    }
}
