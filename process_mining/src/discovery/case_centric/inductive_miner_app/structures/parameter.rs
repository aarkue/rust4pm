use std::collections::HashSet;

/// A helper type aggregating parameters which user maybe want the inductive miner to adhere.
/// The Hashset is used, so that every parameter is unique
pub type Parameters = HashSet<Parameter>;



/// Helper enum to express which option shall be activated in the inductive miner
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Parameter{
    StrictSequenceCut, // apply strict sequence cut additionally to the 'ordinary' sequence cut
    FoldTree, // automatically fold tree
    MinimumSelfDistance, // consider minimum self distance while looking for concurrent cut
    ApplyFallthrough, // apply fallthrough's (Flower Model will always be applied
    //-------Ideas for additional parameters:
    // Multiprocessing
}



impl Parameter{

    /// Generate a Hashset containing all default parameters s.t.:
    /// - Strict Sequence Cut is used
    /// - Fallthrough's are being applied
    /// - Minimum Self Distance is calculated and used during looking for a concurrent cut
    /// - Resulting Tree is folded
    pub fn generate_default_parameters() -> Parameters{
        HashSet::from([Parameter::StrictSequenceCut, Parameter::FoldTree, Parameter::MinimumSelfDistance, Parameter::ApplyFallthrough])
    }
}