//! Conformance Checking
//!
//! Conformance checking techniques typically compare the behavior of a process model with
//! event data.
pub mod case_centric;
pub mod object_centric;
pub use case_centric::*;
pub use object_centric::*;
