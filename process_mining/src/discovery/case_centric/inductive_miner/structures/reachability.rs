//! Transitive closure of the directly-follows relation, over its strongly connected components:
//! within a component everything reaches everything, and the rest is one sweep in reverse
//! topological order.

use super::super::dfg::ActivityDfg;

/// Number of activities covered by one word of the bit matrix.
const BITS: usize = u64::BITS as usize;

/// The transitive closure of a directly-follows relation over local activity indices.
#[derive(Debug, Clone)]
pub struct Reachability {
    words_per_row: usize,
    /// The strongly connected component each activity belongs to.
    component_of: Vec<u32>,
    /// One bit row per component. Bit `to` is set iff the component reaches activity `to`.
    rows: Vec<u64>,
}

impl Reachability {
    /// Computes the transitive closure of the given graph.
    pub fn of(dfg: &ActivityDfg) -> Self {
        let n = dfg.len();
        let words_per_row = n.div_ceil(BITS);

        // Components come out in topological order of the condensation.
        let (component_of, components) = strongly_connected_components(dfg);
        let mut rows = vec![0u64; components.len() * words_per_row];

        // Sweep in reverse topological order, so a component's successors are already done.
        for (index, component) in components.iter().enumerate().rev() {
            for &activity in component {
                for &successor in dfg.successors(activity as usize) {
                    let successor_component = component_of[successor as usize] as usize;
                    rows[index * words_per_row + successor as usize / BITS] |=
                        1 << (successor as usize % BITS);

                    if successor_component != index {
                        let (target, source) =
                            (index * words_per_row, successor_component * words_per_row);
                        for word in 0..words_per_row {
                            rows[target + word] |= rows[source + word];
                        }
                    }
                }
            }
        }

        Self {
            words_per_row,
            component_of,
            rows,
        }
    }

    /// Returns `true` if `from` eventually reaches `to`.
    pub fn reaches(&self, from: usize, to: usize) -> bool {
        let row = self.component_of[from] as usize * self.words_per_row;
        self.rows[row + to / BITS] & (1 << (to % BITS)) != 0
    }
}

/// Finds the strongly connected components with Kosaraju's algorithm.
///
/// Returns the component index of each activity and the components in topological order of the
/// condensation, so a component reaching another comes first. Both passes are iterative, so a
/// long path cannot overflow the stack.
fn strongly_connected_components(dfg: &ActivityDfg) -> (Vec<u32>, Vec<Vec<u32>>) {
    let n = dfg.len();

    // First pass: depth-first search over the graph, recording activities by finishing time.
    let mut visited = vec![false; n];
    let mut finish_order: Vec<u32> = Vec::with_capacity(n);
    let mut stack: Vec<(u32, usize)> = Vec::new();

    for root in 0..n {
        if visited[root] {
            continue;
        }
        visited[root] = true;
        stack.push((root as u32, 0));

        while let Some((activity, next_successor)) = stack.pop() {
            let successors = dfg.successors(activity as usize);
            match successors.get(next_successor) {
                Some(&successor) => {
                    stack.push((activity, next_successor + 1));
                    if !visited[successor as usize] {
                        visited[successor as usize] = true;
                        stack.push((successor, 0));
                    }
                }
                None => finish_order.push(activity),
            }
        }
    }

    // Second pass: a search on the reversed graph, latest finishing time first. Each tree
    // it grows is one strongly connected component, and they are found in topological order.
    let mut component_of = vec![u32::MAX; n];
    let mut components: Vec<Vec<u32>> = Vec::new();

    for &root in finish_order.iter().rev() {
        if component_of[root as usize] != u32::MAX {
            continue;
        }

        let index = components.len() as u32;
        component_of[root as usize] = index;
        let mut component = vec![root];
        // The component doubles as the work list, since visiting order does not matter here.
        let mut next = 0;
        while let Some(&activity) = component.get(next) {
            next += 1;
            for &predecessor in dfg.predecessors(activity as usize) {
                if component_of[predecessor as usize] == u32::MAX {
                    component_of[predecessor as usize] = index;
                    component.push(predecessor);
                }
            }
        }

        components.push(component);
    }

    (component_of, components)
}

#[cfg(test)]
mod test_reachability {
    use super::super::super::dfg::test_utils::dfg_of;
    use super::super::super::dfg::ActivityDfg;
    use super::*;

    /// The definition, computed the slow way.
    fn naive(dfg: &ActivityDfg) -> Vec<bool> {
        let n = dfg.len();
        let mut reaches = vec![false; n * n];
        for from in 0..n {
            for to in 0..n {
                reaches[from * n + to] = dfg.follows(from, to);
            }
        }
        for via in 0..n {
            for from in 0..n {
                for to in 0..n {
                    if reaches[from * n + via] && reaches[via * n + to] {
                        reaches[from * n + to] = true;
                    }
                }
            }
        }
        reaches
    }

    fn assert_matches_naive(dfg: &ActivityDfg) {
        let expected = naive(dfg);
        let actual = Reachability::of(dfg);
        for from in 0..dfg.len() {
            for to in 0..dfg.len() {
                assert_eq!(
                    actual.reaches(from, to),
                    expected[from * dfg.len() + to],
                    "{from} to {to}"
                );
            }
        }
    }

    #[test]
    fn test_chain() {
        let dfg = dfg_of(3, &[(0, 1), (1, 2)], &[0], &[2]);
        let reachability = Reachability::of(&dfg);

        assert!(reachability.reaches(0, 2));
        assert!(!reachability.reaches(2, 0));
        assert!(!reachability.reaches(0, 0));
        assert_matches_naive(&dfg);
    }

    #[test]
    fn test_cycles() {
        // Everything in a cycle reaches everything, including itself.
        let dfg = dfg_of(3, &[(0, 1), (1, 2), (2, 0)], &[0], &[2]);
        let reachability = Reachability::of(&dfg);
        for from in 0..3 {
            for to in 0..3 {
                assert!(reachability.reaches(from, to), "{from} should reach {to}");
            }
        }

        let dfg = dfg_of(2, &[(0, 0), (0, 1)], &[0], &[1]);
        let reachability = Reachability::of(&dfg);
        assert!(reachability.reaches(0, 0));
        assert!(!reachability.reaches(1, 1));

        // A cycle feeding into a chain.
        assert_matches_naive(&dfg_of(4, &[(0, 1), (1, 0), (1, 2), (2, 3)], &[0], &[3]));
    }

    #[test]
    fn test_more_activities_than_fit_in_one_word() {
        let n = 100;
        let edges: Vec<(usize, usize)> = (0..n - 1).map(|i| (i, i + 1)).collect();
        let reachability = Reachability::of(&dfg_of(n, &edges, &[0], &[n - 1]));

        for from in 0..n {
            for to in 0..n {
                assert_eq!(reachability.reaches(from, to), from < to, "{from} to {to}");
            }
        }
    }

    #[test]
    fn test_matches_naive_on_pseudo_random_graphs() {
        for seed in 0u64..48 {
            let n = 9;
            let edges: Vec<(usize, usize)> = (0..n)
                .flat_map(|from| (0..n).map(move |to| (from, to)))
                .filter(|&(from, to)| {
                    let key = seed
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add((from * 31 + to * 7) as u64);
                    (key >> 11) % 4 == 0
                })
                .collect();
            assert_matches_naive(&dfg_of(n, &edges, &[0], &[n - 1]));
        }
    }

    #[test]
    fn test_empty_graph() {
        assert!(Reachability::of(&dfg_of(0, &[], &[], &[])).rows.is_empty());
    }
}
