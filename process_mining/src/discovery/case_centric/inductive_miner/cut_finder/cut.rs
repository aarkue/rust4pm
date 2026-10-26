//! The result of cut detection: a partition of the activities and the corresponding operator.

use crate::core::process_models::process_tree::OperatorType;

use super::super::log::ActivityID;

/// How early [`find_cut`](super::find_cut) tries an operator. Earlier ones give more precise
/// models.
pub fn precedence(operator: OperatorType) -> usize {
    match operator {
        OperatorType::ExclusiveChoice => 0,
        OperatorType::Sequence => 1,
        OperatorType::Interleaving => 2,
        // An inclusive choice is found by regrouping a concurrent cut.
        OperatorType::Concurrency | OperatorType::InclusiveChoice => 3,
        OperatorType::Loop => 4,
    }
}

/// A cut of a directly-follows graph.
///
/// A cut partitions the activities of a graph into parts `Σ1 ... Σn` that, together with an
/// operator, adhere to the directly-follows footprint of the operator (Definition 5.3).
///
/// The order of the parts is significant for [`OperatorType::Sequence`] (the order of the
/// sequence) and for [`OperatorType::Loop`] (the first part is the loop body, the remaining ones
/// are the redo parts). Within a part, activity ids are ascending.
#[derive(Debug, Clone, PartialEq)]
pub struct Cut {
    operator: OperatorType,
    partitions: Vec<Vec<ActivityID>>,
}

impl Cut {
    /// Creates a cut with the given operator and parts, without checking the footprint.
    pub fn new(operator: OperatorType, partitions: Vec<Vec<ActivityID>>) -> Self {
        Self {
            operator,
            partitions,
        }
    }

    /// The operator of this cut.
    pub fn operator(&self) -> OperatorType {
        self.operator
    }

    /// The parts of this cut.
    pub fn partitions(&self) -> &[Vec<ActivityID>] {
        &self.partitions
    }

    /// The number of parts.
    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    /// Returns `true` if the cut has more than one part and no part is empty.
    ///
    /// Only such a cut makes the recursion progress. The cut finders report any other as no cut.
    pub fn is_non_trivial(&self) -> bool {
        self.partitions.len() > 1 && self.partitions.iter().all(|p| !p.is_empty())
    }

    /// Maps each activity of the alphabet to the index of the part containing it, or
    /// [`usize::MAX`] for activities that no part contains.
    pub fn partition_lookup(&self, alphabet_size: usize) -> Vec<usize> {
        part_lookup(&self.partitions, alphabet_size)
    }
}

/// [`Cut::partition_lookup`] for parts that do not form a [`Cut`] yet.
pub(super) fn part_lookup(parts: &[Vec<ActivityID>], alphabet_size: usize) -> Vec<usize> {
    let mut lookup = vec![usize::MAX; alphabet_size];
    for (index, part) in parts.iter().enumerate() {
        for &activity in part {
            lookup[activity] = index;
        }
    }
    lookup
}

#[cfg(test)]
mod test_cut {
    use super::*;

    #[test]
    fn test_non_trivial() {
        assert!(Cut::new(OperatorType::Sequence, vec![vec![0], vec![1]]).is_non_trivial());
        assert!(!Cut::new(OperatorType::Sequence, vec![vec![0, 1]]).is_non_trivial());
        assert!(!Cut::new(OperatorType::Sequence, vec![vec![0], vec![]]).is_non_trivial());
        assert!(!Cut::new(OperatorType::Sequence, vec![]).is_non_trivial());
    }

    #[test]
    fn test_partition_lookup() {
        let cut = Cut::new(OperatorType::ExclusiveChoice, vec![vec![0, 3], vec![1]]);
        assert_eq!(
            cut.partition_lookup(5),
            vec![0, 1, usize::MAX, 0, usize::MAX]
        );
    }
}
