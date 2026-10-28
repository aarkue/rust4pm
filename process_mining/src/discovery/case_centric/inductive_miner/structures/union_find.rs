//! A minimal disjoint-set (union-find) structure over activity indices.

/// Disjoint-set forest over the indices `0..n` with union by size and path halving.
#[derive(Debug, Clone)]
pub struct UnionFind {
    parent: Vec<usize>,
    size: Vec<usize>,
}

impl UnionFind {
    /// Creates a new structure in which each of the `n` elements forms its own singleton set.
    pub fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            size: vec![1; n],
        }
    }

    /// Returns the representative of the set containing `x`.
    pub fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }

    /// Merges the sets containing `a` and `b`.
    ///
    /// Returns `true` if the two elements were in different sets before, i.e., if something changed.
    pub fn union(&mut self, a: usize, b: usize) -> bool {
        let (mut ra, mut rb) = (self.find(a), self.find(b));
        if ra == rb {
            return false;
        }
        if self.size[ra] < self.size[rb] {
            std::mem::swap(&mut ra, &mut rb);
        }
        self.parent[rb] = ra;
        self.size[ra] += self.size[rb];
        true
    }

    /// Groups `0..n` into their sets.
    ///
    /// The groups are ordered by their smallest contained element and each group is sorted
    /// ascending, which makes the output independent of the order in which unions were applied.
    pub fn groups(&mut self) -> Vec<Vec<usize>> {
        let n = self.parent.len();
        let mut group_of_root: Vec<Option<usize>> = vec![None; n];
        let mut groups: Vec<Vec<usize>> = Vec::new();

        for i in 0..n {
            let root = self.find(i);
            match group_of_root[root] {
                Some(g) => groups[g].push(i),
                None => {
                    group_of_root[root] = Some(groups.len());
                    groups.push(vec![i]);
                }
            }
        }

        groups
    }
}

#[cfg(test)]
mod test_union_find {
    use super::UnionFind;

    #[test]
    fn test_union_and_find() {
        let mut uf = UnionFind::new(5);
        assert!(uf.union(4, 1));
        assert!(!uf.union(1, 4));
        assert!(uf.union(2, 3));
        assert!(uf.union(3, 4));
        assert_eq!(uf.groups(), vec![vec![0], vec![1, 2, 3, 4]]);
    }

    #[test]
    fn test_groups_are_ordered_by_smallest_element() {
        let mut uf = UnionFind::new(6);
        uf.union(5, 3);
        uf.union(4, 0);
        assert_eq!(uf.groups(), vec![vec![0, 4], vec![1], vec![2], vec![3, 5]]);
    }

    #[test]
    fn test_empty() {
        let mut uf = UnionFind::new(0);
        assert!(uf.groups().is_empty());
    }
}
