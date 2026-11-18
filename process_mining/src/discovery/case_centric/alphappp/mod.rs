//! Alpha+++ Process Discovery Algorithm
/// Automatic determining algorithm parameters for Alpha+++
pub mod auto_parameters;
/// Alpha+++ Place Candidate Building
pub(crate) mod candidate_building;
/// Alpha+++ Place Candidate Pruning
pub(crate) mod candidate_pruning;
/// Full Alpha+++ Discovery algorithm
pub mod full;
/// Event Log Repair (Adding artificial activities)
pub(crate) mod log_repair;
