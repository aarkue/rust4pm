//! Log splitting: dividing a log into the sub-logs the recursion continues on.
//!
//! Every cut comes with a way to split the log into one sub-log per part, so that recombining the
//! discovered sub-trees with the cut's operator reproduces the original behavior.
//!
//! Only the filtering splits of `IMf`, which drop events the cut cannot explain, are implemented.
//! On a cut that adheres to its footprint, they coincide with the plain splits of IM (§6.2.2.2).

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
