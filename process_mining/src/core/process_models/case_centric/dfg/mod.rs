//! Directly-Follows Graph
pub(crate) mod dfg_struct;
#[cfg(feature = "graphviz-export")]
pub mod image_export;

#[doc(inline)]
pub use dfg_struct::*;
