#[cfg(feature = "graphviz-export")]
/// Export [`PetriNet`] to images (SVG, PNG, ...)
///
/// __Requires the `graphviz-export` feature to be enabled__
///
/// Also requires an active graphviz installation in the PATH.
/// See also <https://github.com/besok/graphviz-rust?tab=readme-ov-file#caveats> and <https://graphviz.org/download/>
pub mod image_export;
pub mod petri_net_struct;
pub mod pnml;
