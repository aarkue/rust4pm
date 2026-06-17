//! Object-centric Process Discovery Techniques
pub mod dfg;
pub mod oc_declare;
pub mod variants;

use std::collections::HashMap;

/// Merge `b` into `a` by summing counts for matching keys. Used as the rayon reduce step.
pub(crate) fn merge_count_maps<K: std::hash::Hash + Eq>(
    mut a: HashMap<K, usize>,
    b: HashMap<K, usize>,
) -> HashMap<K, usize> {
    for (k, v) in b {
        *a.entry(k).or_insert(0) += v;
    }
    a
}
