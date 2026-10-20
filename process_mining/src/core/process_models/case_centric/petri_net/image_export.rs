//! Image Export of Petri nets
//!
//! 🔐 Requires the `graphviz-export` feature to be enabled
use std::{fs::File, io::Write};

use graphviz_rust::{
    cmd::Format,
    dot_generator::{attr, edge, graph, id, node, node_id, stmt},
    dot_structures::*,
    printer::{DotPrinter, PrinterContext},
};
use macros_process_mining::register_binding;

use crate::core::process_models::case_centric::petri_net::{
    AcceptingPetriNet, ArcType, Marking, PetriNet,
};

///
/// Export the image of a [`PetriNet`], drawing the places of the given markings
///
/// Also see [`export_petri_net_image_svg`] and [`export_petri_net_image_png`]
///
pub fn export_petri_net_image<P: AsRef<std::path::Path>>(
    net: &PetriNet,
    initial_marking: Option<&Marking>,
    final_marking: Option<&Marking>,
    path: P,
    format: Format,
    dpi_factor: Option<f32>,
) -> Result<(), std::io::Error> {
    let g = export_petri_net_to_dot_graph(net, initial_marking, final_marking, dpi_factor);
    let out = graphviz_rust::exec(g, &mut PrinterContext::default(), vec![format.into()])?;
    File::create(path)?.write_all(&out)
}

///
/// Export a [`PetriNet`] to a DOT graph (used in Graphviz)
///
/// Places marked in `initial_marking` show a token, and places marked in `final_marking` are
/// drawn as double circles.
///
/// Also see [`export_petri_net_image`], as well as [`export_petri_net_image_svg`] and [`export_petri_net_image_png`]
///
pub fn export_petri_net_to_dot_graph(
    net: &PetriNet,
    initial_marking: Option<&Marking>,
    final_marking: Option<&Marking>,
    dpi_factor: Option<f32>,
) -> Graph {
    let marked = |marking: Option<&Marking>, place| marking.is_some_and(|m| m.tokens(place) > 0);
    let place_nodes: Vec<_> = net
        .places()
        .map(|(id, _)| {
            let node_id = format!("p{}", id.index());
            let symbol = if marked(initial_marking, id) { "●" } else { "" };
            let is_final = marked(final_marking, id);
            let shape = if is_final { "doublecircle" } else { "circle" };
            let size = if is_final { 0.4 } else { 0.5 };
            stmt!(node!(esc node_id; attr!("label", esc symbol), attr!("shape",shape), attr!("fixedsize",true), attr!("width",size), attr!("height",size)))
        }).collect();

    let transition_nodes: Vec<_> = net
        .transitions()
        .map(|(id, t)| {
            let node_id = format!("t{}", id.index());
            let label = t.label.as_ref().cloned().unwrap_or_default();
            let (font_size,width) = (12,1);
            let fill_color = if t.label.is_none() { "black" } else { "white" };
            stmt!(node!(esc node_id; attr!("label", esc label), attr!("shape","box"), attr!("fontsize",font_size),attr!("style","filled"), attr!("fillcolor",fill_color), attr!("width",width), attr!("height",0.5)))
        }).collect();

    let arcs: Vec<_> = net
        .arcs()
        .map(|arc| {
            let (from_id, to_id) = match arc.from_to {
                ArcType::PlaceTransition(p, t) => {
                    (format!("p{}", p.index()), format!("t{}", t.index()))
                }
                ArcType::TransitionPlace(t, p) => {
                    (format!("t{}", t.index()), format!("p{}", p.index()))
                }
            };
            let attrs = if arc.weight == 1 {
                Vec::default()
            } else {
                vec![attr!("label", (format!("{}", arc.weight)))]
            };
            stmt!(edge!(node_id!(esc from_id) => node_id!(esc to_id), attrs))
        })
        .collect();

    let mut global_graph_options = vec![stmt!(attr!("rankdir", "LR"))];
    if let Some(dpi_fac) = dpi_factor {
        global_graph_options.push(stmt!(attr!("dpi", (dpi_fac * 96.0))))
    }

    graph!(strict di id!("petri_net"),vec![global_graph_options,place_nodes,transition_nodes, arcs].into_iter().flatten().collect())
}

///
/// Convery a DOT graph to a String containing the DOT source
///
pub fn graph_to_dot(g: &Graph) -> String {
    g.print(&mut PrinterContext::default())
}

///
/// Export the image of an [`AcceptingPetriNet`] as a SVG file
///
/// Also consider using [`AcceptingPetriNet::export_svg`] for convenience.
#[register_binding(stringify_error)]
pub fn export_petri_net_image_svg(
    net: &AcceptingPetriNet,
    path: impl AsRef<std::path::Path>,
) -> Result<(), std::io::Error> {
    export_petri_net_image(
        &net.net,
        Some(&net.initial_marking),
        Some(&net.final_marking),
        path,
        Format::Svg,
        None,
    )
}

///
/// Export the image of an [`AcceptingPetriNet`] as a PNG file
///
/// Also consider using [`AcceptingPetriNet::export_png`] for convenience.
#[register_binding(stringify_error)]
pub fn export_petri_net_image_png(
    net: &AcceptingPetriNet,
    path: impl AsRef<std::path::Path>,
) -> Result<(), std::io::Error> {
    export_petri_net_image(
        &net.net,
        Some(&net.initial_marking),
        Some(&net.final_marking),
        path,
        Format::Png,
        Some(2.0),
    )
}

#[cfg(test)]
mod test {

    use crate::{
        core::event_data::case_centric::xes::import_xes::{import_xes_path, XESImportOptions},
        discovery::case_centric::alphappp::auto_parameters,
        test_utils::get_test_data_path,
    };

    use super::{export_petri_net_image_png, export_petri_net_image_svg};

    #[test]
    pub fn test_petri_net_png_export() {
        let path = get_test_data_path().join("xes").join("AN1-example.xes");
        let log = import_xes_path(&path, XESImportOptions::default()).unwrap();
        let (_, pn) = auto_parameters::alphappp_discover_with_auto_parameters(&(&log).into());
        let export_path = get_test_data_path()
            .join("export")
            .join("petri-net-export-test.png");
        export_petri_net_image_png(&pn, export_path).unwrap();
    }

    #[test]
    pub fn test_petri_net_svg_export() {
        let path = get_test_data_path().join("xes").join("AN1-example.xes");
        let log = import_xes_path(&path, XESImportOptions::default()).unwrap();
        let (_, pn) = auto_parameters::alphappp_discover_with_auto_parameters(&(&log).into());
        let export_path = get_test_data_path()
            .join("export")
            .join("petri-net-export-test.svg");
        export_petri_net_image_svg(&pn, export_path).unwrap();
    }
}
