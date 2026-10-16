//! The layout and SVG adapters, registered in the [`crate::bindings`] registry.

use graphlay::{Layout, LayoutError};
use macros_process_mining::register_binding;

use crate::core::process_models::dfg::DirectlyFollowsGraph;
use crate::core::process_models::oc_declare::OCDeclareArc;
use crate::core::process_models::ocdfg::OCDirectlyFollowsGraph;
use crate::core::process_models::process_tree::ProcessTree;
use crate::visualization::{dfg, oc_declare, ocdfg, petri_net, process_tree};
use crate::PetriNet;

/// Lay a [`PetriNet`] out, returning the positions only.
#[register_binding(stringify_error)]
pub fn layout_petri_net(
    net: &PetriNet,
    #[bind(default)] options: petri_net::PetriBuildOptions,
) -> Result<Layout, LayoutError> {
    petri_net::layout_petri_net(net, &options)
}

/// Draw a [`PetriNet`] as a standalone SVG document.
#[register_binding(stringify_error)]
pub fn export_petri_net_svg(
    net: &PetriNet,
    #[bind(default)] options: petri_net::PetriBuildOptions,
) -> Result<String, LayoutError> {
    petri_net::export_petri_net_svg(net, &options)
}

/// Lay a [`DirectlyFollowsGraph`] out, returning the positions only.
#[register_binding(stringify_error)]
pub fn layout_dfg(
    dfg: &DirectlyFollowsGraph<'_>,
    #[bind(default)] options: dfg::DfgBuildOptions,
) -> Result<Layout, LayoutError> {
    dfg::layout_dfg(dfg, &options)
}

/// Draw a [`DirectlyFollowsGraph`] as a standalone SVG document.
#[register_binding(stringify_error)]
pub fn export_dfg_svg(
    dfg: &DirectlyFollowsGraph<'_>,
    #[bind(default)] options: dfg::DfgBuildOptions,
) -> Result<String, LayoutError> {
    dfg::export_dfg_svg(dfg, &options)
}

/// Lay an [`OCDirectlyFollowsGraph`] out, returning the positions only.
#[register_binding(stringify_error)]
pub fn layout_ocdfg(
    ocdfg: &OCDirectlyFollowsGraph<'_>,
    #[bind(default)] options: ocdfg::OcdfgBuildOptions,
) -> Result<Layout, LayoutError> {
    ocdfg::layout_ocdfg(ocdfg, &options)
}

/// Draw an [`OCDirectlyFollowsGraph`] as a standalone SVG document.
#[register_binding(stringify_error)]
pub fn export_ocdfg_svg(
    ocdfg: &OCDirectlyFollowsGraph<'_>,
    #[bind(default)] options: ocdfg::OcdfgBuildOptions,
) -> Result<String, LayoutError> {
    ocdfg::export_ocdfg_svg(ocdfg, &options)
}

/// Lay OC-DECLARE arcs out, returning the positions only.
#[register_binding(stringify_error)]
pub fn layout_oc_declare(
    arcs: &[OCDeclareArc],
    #[bind(default = oc_declare::OCDeclareBuildOptions::new())]
    options: oc_declare::OCDeclareBuildOptions,
) -> Result<Layout, LayoutError> {
    oc_declare::layout_oc_declare(arcs, &options)
}

/// Draw OC-DECLARE arcs as a standalone SVG document.
#[register_binding(stringify_error)]
pub fn export_oc_declare_svg(
    arcs: &[OCDeclareArc],
    #[bind(default = oc_declare::OCDeclareBuildOptions::new())]
    options: oc_declare::OCDeclareBuildOptions,
) -> Result<String, LayoutError> {
    oc_declare::export_oc_declare_svg(arcs, &options)
}

/// Lay a [`ProcessTree`] out, returning the positions only.
#[register_binding(stringify_error)]
pub fn layout_process_tree(
    tree: &ProcessTree,
    #[bind(default)] options: process_tree::ProcessTreeBuildOptions,
) -> Result<Layout, LayoutError> {
    process_tree::layout_process_tree(tree, &options)
}

/// Draw a [`ProcessTree`] as a standalone SVG document.
#[register_binding(stringify_error)]
pub fn export_process_tree_svg(
    tree: &ProcessTree,
    #[bind(default)] options: process_tree::ProcessTreeBuildOptions,
) -> Result<String, LayoutError> {
    process_tree::export_process_tree_svg(tree, &options)
}

#[cfg(test)]
mod tests {
    use crate::bindings::list_functions_meta;

    #[test]
    fn the_adapter_bindings_are_registered_once() {
        let names: Vec<String> = list_functions_meta().into_iter().map(|b| b.name).collect();
        for expected in [
            "layout_petri_net",
            "export_petri_net_svg",
            "layout_dfg",
            "export_dfg_svg",
            "layout_ocdfg",
            "export_ocdfg_svg",
            "layout_oc_declare",
            "export_oc_declare_svg",
            "layout_process_tree",
            "export_process_tree_svg",
        ] {
            assert_eq!(
                names.iter().filter(|n| *n == expected).count(),
                1,
                "expected exactly one binding named {expected}"
            );
        }
    }

    /// Needs `graphlay`'s `schemars` feature, which `bindings` turns on.
    #[test]
    fn a_layout_return_type_has_a_schema() {
        let binding = list_functions_meta()
            .into_iter()
            .find(|b| b.name == "layout_petri_net")
            .expect("layout_petri_net is registered");
        assert!(binding.return_type.is_object());
        assert!(binding.return_type.to_string().contains("nodes"));
        assert!(binding.args.iter().any(|(name, _)| name == "options"));
        assert_eq!(binding.required_args, vec!["net".to_string()]);
    }
}
