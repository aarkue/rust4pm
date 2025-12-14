//! Event Data
//!
//! Fundamental structs and adjacient utilities for process data
pub mod case_centric;
pub mod object_centric;
#[cfg(test)]
mod tests;

#[doc(inline)]
pub use case_centric::EventLog;
