//! This module contains a struct used for representing a found cut by specifying the found partitions and cut type.
use std::borrow::Cow;
use std::collections::HashSet;
use crate::core::process_models::process_tree::OperatorType;

/// Represents a cut typically found by the inductive miner in a directly follows graph.
/// A 'Cut' partitions activities of a graph or log into disjoints sets, according to a
/// specific cut operator (e.g. sequence, xor etc.)
#[derive(Debug, PartialEq)]
pub struct Cut<'a>{
    pub operator: OperatorType, // define what operator this cut is about
    pub partitions: Vec<HashSet<Cow<'a, str>>>,
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
    
}