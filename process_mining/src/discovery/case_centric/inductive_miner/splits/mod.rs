//! Log splitting: dividing a log into the sub-logs the recursion continues on.
//!
//! Every cut comes with a way to split the log into one sub-log per part, so that recombining the
//! discovered sub-trees with the cut's operator reproduces the original behaviour.
//!
//! Leemans defines two families: the plain splits for IM and filtering ones for `IMf` that discard
//! events the cut cannot explain. Only the filtering ones are implemented here, since they
//! coincide with the plain ones whenever the cut adheres to its footprint (§6.2.2.2), and plain IM
//! only detects cuts on an unfiltered graph so its cuts always do.

use crate::core::process_models::process_tree::OperatorType;

use super::cut_finder::cut::Cut;
use super::log::ActivityLog;

mod concurrency;
mod exclusive_choice;
mod redo_loop;
mod sequence;

pub use concurrency::{concurrency_split, inclusive_choice_split};
pub use exclusive_choice::exclusive_choice_split;
pub use redo_loop::loop_split;
pub use sequence::sequence_split;

/// Splits a log according to a cut, yielding one sub-log per part of the cut.
///
/// The sub-logs are returned in the order of the cut's parts, which matters for sequence and loop
/// cuts.
pub fn split_log(log: &ActivityLog, cut: &Cut) -> Vec<ActivityLog> {
    match cut.operator() {
        OperatorType::ExclusiveChoice => exclusive_choice_split(log, cut),
        OperatorType::Sequence => sequence_split(log, cut),
        // An interleaved cut keeps each part in one block per trace, so projecting splits it.
        OperatorType::Concurrency | OperatorType::Interleaving => concurrency_split(log, cut),
        OperatorType::Loop => loop_split(log, cut),
        OperatorType::InclusiveChoice => inclusive_choice_split(log, cut),
    }
}
