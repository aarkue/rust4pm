//! Layout and SVG drawing of this crate's process models with [`graphlay`].
//!
//! 🔐 Requires the `visualization` feature, which is on by default.
//!
//! Each model (accepting Petri nets, object-centric Petri nets, process trees, DFGs, OC-DFGs,
//! OC-DECLARE models, partial order traces and OCEL type graphs) implements [`Visualize`], with its
//! build options, style and element refs in its own submodule. The pipeline is the same for all:
//!
//! | function | what it does |
//! |---|---|
//! | [`layout_request`] | the model's graph as a graphlay request (preset and options patch), with the model element behind each node and edge |
//! | [`layout`] | runs the engine and returns a [`graphlay::Layout`] with positions only |
//! | [`scene_from_layout`] | draws the model over any layout of its graph, e.g., one the user rearranged, in a style and with per-element [`StyleOverrides`], as a [`graphlay::svg::Scene`] |
//! | [`svg`] | lays the model out, draws it in a style and returns a standalone SVG `String` |
//!
//! [`engine`] lays out any model's request, re-routes it after a drag, and draws any scene, so a
//! host needs one call for each of those for every model. Each model's build options take
//! [`LayoutSettings`], whose `layout_patch` is deep-merged over the model's own engine options.
//! Those of DFGs, OC-DFGs, Petri nets and object-centric Petri nets also take [`Annotations`] by
//! model element: labels, layout weights and line widths that replace the model's own, e.g.,
//! durations instead of counts.
//!
//! ```no_run
//! use process_mining::visualization::engine::{layout_graph, render_scene_svg, reroute_graph};
//! use process_mining::visualization::petri_net::{PetriBuildOptions, PetriStyle};
//! use process_mining::visualization::{layout_request, scene_from_layout, svg, StyleOverrides};
//! use process_mining::{AcceptingPetriNet, Importable};
//!
//! let net = AcceptingPetriNet::import_from_path("model.pnml")?;
//! let options = PetriBuildOptions::default();
//! let style = PetriStyle::default();
//! let drawing: String = svg(&net, &options, &style)?;
//!
//! // `layout.nodes[i]` is where the place or transition `request.nodes[i]` went.
//! let request = layout_request(&net, &options)?;
//! let layout = layout_graph(&request.request)?;
//!
//! // The user drags the first node, the edges are routed again, and the result is exported.
//! let mut positions: Vec<_> = layout.nodes.iter().map(|n| n.center).collect();
//! positions[0].y += 100.0;
//! let moved = reroute_graph(&request.request, &positions)?;
//! let scene = scene_from_layout(&net, &moved, &options, &style, &StyleOverrides::default())?;
//! let drawing: String = render_scene_svg(&scene, None);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod annotation;
#[cfg(all(feature = "bindings", feature = "visualization"))]
pub mod bindings;
pub mod dfg;
pub mod engine;
pub mod graph_build;
pub mod model;
pub mod oc_declare;
pub mod oc_type_graph;
pub mod ocdfg;
pub mod ocpn;
pub mod partial_order;
pub mod petri_net;
pub mod process_tree;
pub mod styling;

#[cfg(test)]
mod gallery_tests;

/// The engine, so a caller can name its types (e.g., `json::LayoutRequest`, `svg::Palette`) in the
/// version this crate uses.
pub use graphlay;

pub use annotation::{EdgeAnnotation, NodeAnnotation};
pub use engine::VisualizationError;
pub use graph_build::{
    dfg_width, dfg_width_with, edge_label_size, edge_label_size_with, leaf_lines, leaf_size,
    leaf_size_with, thickness_from_count, weight_from_count, LabelWidth,
};
pub use model::{
    layout, layout_request, scene_from_layout, svg, Annotations, LayoutRequestOf, LayoutSettings,
    ModelLayoutRequest, Visualize,
};
pub use styling::{key_color, ElementStyle, StyleOverrides};
