use std::borrow::Cow;
use std::collections::HashSet;
use crate::core::process_models::process_tree::OperatorType;

/// Represents a cut typically found by the inductive miner in a directly follows graph.
/// A 'Cut' partitions activities of a graph or log into disjoints sets, according to a
/// specific cut operator (e.g. sequence, xor etc.)
#[derive(Debug, PartialEq)]
pub struct Cut<'a>{
    operator: OperatorType, // define what operator this cut is about
    partitions: Vec<HashSet<Cow<'a, str>>>,
}

impl<'a> Cut<'a>{

    /// Creates a new cut with the given Operator and partitions.
    ///
    /// The caller must ensure that partitions form a valid cut according to the chosen operator.
    pub fn new(operator: OperatorType, partitions: Vec<HashSet<Cow<'a, str>>>) -> Cut<'a>{
        Self{operator, partitions}
    }


    /// Returns the number of partitions in this cut.
    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    /// Returns an iterator over the partitions of this cut.
    pub fn get_iter(&self) -> std::slice::Iter<'_, HashSet<Cow<'_, str>>> {
        self.partitions.iter()
    }

    /// Consumes the cut and returns the partitions of this cut.
    pub fn get_own(self) -> Vec<HashSet<Cow<'a, str>>> {
        self.partitions
    }

    /// Returns the operator associated with this cut
    pub fn get_operator(&self) -> OperatorType {
        self.operator // possible due to copy trait
    }


    /// Returns true if this cut contains no partitions.
    pub fn is_empty(&self) -> bool{
        self.partitions.is_empty()
    }



    /// Converts this cut into an owned version with `'static` lifetime.
    ///
    /// All activity labels are cloned into owned `String`s.
    /// This is useful when the cut must outlive the original event log data.
    pub fn to_owned_cut(&self) ->Cut<'static>{
        let owned_partitions = self.partitions.iter().map(|partition|{
            partition.iter().map(|cow| Cow::Owned(cow.to_string())).collect()
        }).collect::<Vec<HashSet<Cow<'static, str>>>>();

        Cut{
            operator: self.operator,
            partitions: owned_partitions,
        }
    }
}