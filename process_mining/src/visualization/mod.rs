//! Layout and SVG drawing of this crate's process models with [`graphlay`].
//!
//! 🔐 Requires the `visualization` feature, which is on by default.
//!
//! Each model has its own submodule with the same four entry points:
//!
//! | function | what it does |
//! |---|---|
//! | `<model>_to_graph` | builds the [`graphlay::Graph`] and the tables mapping ids to the model |
//! | `layout_<model>` | runs the engine and returns a [`graphlay::Layout`] with positions only |
//! | `<model>_scene` | turns a `Layout` and a style into a [`graphlay::svg::Scene`] |
//! | `export_<model>_svg` | does all of the above and returns a standalone SVG `String` |
//!
//! ```no_run
//! use process_mining::visualization::petri_net::{
//!     export_petri_net_svg, layout_petri_net, petri_net_to_graph, PetriBuildOptions,
//! };
//! use process_mining::{Importable, PetriNet};
//!
//! let net = PetriNet::import_from_path("model.pnml")?;
//! let options = PetriBuildOptions::default();
//! let svg: String = export_petri_net_svg(&net, &options)?;
//!
//! // Positions only: `layout.nodes[i]` is where the model element `meta[i]` went.
//! let (_, meta, _) = petri_net_to_graph(&net, &options);
//! let layout = layout_petri_net(&net, &options)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#[cfg(all(feature = "bindings", feature = "visualization"))]
pub mod bindings;
pub mod dfg;
pub mod graph_build;
pub mod oc_declare;
pub mod ocdfg;
pub mod partial_order;
pub mod petri_net;
pub mod process_tree;
pub mod styling;

pub use graph_build::{
    dfg_width, edge_label_size, edge_label_size_with, leaf_lines, leaf_size, thickness_from_count,
    weight_from_count, LabelWidth,
};
pub use styling::key_color;
