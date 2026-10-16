//! Connected components of the *complement* of a sparse relation.
//!
//! The concurrent and sequence cuts group activities that may not be separated, and on a real
//! graph almost every pair has to be merged, so that relation is nearly complete. Building it and
//! merging pair by pair costs `O(n²)` union-find operations.
//!
//! Computing the components on the complement, which is the sparse relation, avoids that. The
//! search keeps the vertices it has not placed in one list and, for every vertex it visits, walks
//! that list once: everything not separated from the vertex joins its component and leaves the
//! list. Each step either removes a vertex, at most `n` times, or passes over one edge of the
//! sparse relation, so this is `O(n + m)`.

/// Groups `0..n` into the connected components of the complement of `separate`.
///
/// Two elements share a component unless `separate` holds for them, so `separate` describes the
/// edges absent from the graph being decomposed. It must be symmetric, and `separate(a, a)` is
/// never called.
///
/// Components are sorted internally and ordered by their smallest element, so the result does not
/// depend on the order the search visited things in.
pub fn complement_components(n: usize, separate: impl Fn(usize, usize) -> bool) -> Vec<Vec<usize>> {
    let mut unplaced: Vec<usize> = (0..n).collect();
    let mut components: Vec<Vec<usize>> = Vec::new();
    let mut queue: Vec<usize> = Vec::new();

    while let Some(seed) = unplaced.pop() {
        let mut component = vec![seed];
        queue.push(seed);

        while let Some(current) = queue.pop() {
            let mut index = 0;
            while index < unplaced.len() {
                let candidate = unplaced[index];
                if separate(current, candidate) {
                    index += 1;
                } else {
                    unplaced.swap_remove(index);
                    component.push(candidate);
                    queue.push(candidate);
                }
            }
        }

        component.sort_unstable();
        components.push(component);
    }

    components.sort_unstable_by_key(|component| component[0]);
    components
}

#[cfg(test)]
mod test_complement_components {
    use super::complement_components;

    #[test]
    fn test_extremes() {
        assert_eq!(
            complement_components(3, |_, _| true),
            vec![vec![0], vec![1], vec![2]]
        );
        assert_eq!(
            complement_components(4, |_, _| false),
            vec![vec![0, 1, 2, 3]]
        );
        assert!(complement_components(0, |_, _| true).is_empty());
    }

    #[test]
    fn test_grouping() {
        // 0 and 1 are separated from 2 and 3 but not from each other.
        let separate = |a: usize, b: usize| (a < 2) != (b < 2);
        assert_eq!(
            complement_components(4, separate),
            vec![vec![0, 1], vec![2, 3]]
        );

        // 3 is separated from nobody, so it pulls all other components together.
        let separate = |a: usize, b: usize| a != 3 && b != 3 && a != b;
        assert_eq!(complement_components(4, separate), vec![vec![0, 1, 2, 3]]);

        // Only (0, 1) and (1, 2) are in the complement, which still merges all three.
        let separate = |a: usize, b: usize| a.abs_diff(b) > 1;
        assert_eq!(complement_components(3, separate), vec![vec![0, 1, 2]]);
    }

    #[test]
    fn test_matches_merging_pair_by_pair() {
        use super::super::union_find::UnionFind;

        for seed in 0u64..64 {
            let separate = |a: usize, b: usize| {
                let (low, high) = (a.min(b) as u64, a.max(b) as u64);
                (seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(low * 31 + high * 7)
                    >> 13)
                    % 3
                    == 0
            };

            let n = 12;
            let mut naive = UnionFind::new(n);
            for a in 0..n {
                for b in (a + 1)..n {
                    if !separate(a, b) {
                        naive.union(a, b);
                    }
                }
            }

            assert_eq!(
                complement_components(n, separate),
                naive.groups(),
                "seed {seed}"
            );
        }
    }
}
