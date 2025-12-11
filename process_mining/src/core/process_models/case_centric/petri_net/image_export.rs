use std::{fs::File, io::Write};

use graphviz_rust::{
    cmd::Format,
    dot_generator::{attr, edge, graph, id, node, node_id, stmt},
    dot_structures::*,
    printer::{DotPrinter, PrinterContext},
};
use uuid::Uuid;

use crate::core::PetriNet;

///
/// Export the image of a [`PetriNet`]
///
/// Also see [`export_petri_net_image_svg`] and [`export_petri_net_image_png`]
///
pub fn export_petri_net_image<P: AsRef<std::path::Path>>(
    net: &PetriNet,
    path: P,
    format: Format,
    dpi_factor: Option<f32>,
) -> Result<(), std::io::Error> {
    let g = export_petri_net_to_dot_graph(net, dpi_factor);

    g.print(&mut PrinterContext::default());

    let out = graphviz_rust::exec(g, &mut PrinterContext::default(), vec![format.into()])?;

    let mut f = File::create(path)?;
    f.write_all(&out)?;
    Ok(())
}
///
/// Export the a [`PetriNet`] to a DOT graph (used in Graphviz)
///
/// Also see [`export_petri_net_image`], as well as [`export_petri_net_image_svg`] and [`export_petri_net_image_png`]
///
pub fn export_petri_net_to_dot_graph(net: &PetriNet, dpi_factor: Option<f32>) -> Graph {
    let place_nodes: Vec<_> = net
        .places
        .iter()
        .map(|(p_id, p)| {
            let symbol = if net.is_in_initial_marking(&p.into()) {
                "●"
            } else {
                ""
            };
            let shape = if net.is_in_a_final_marking(&p.into()) {"doublecircle"} else {"circle"};
            let size = if net.is_in_a_final_marking(&p.into()) {0.4} else {0.5};
            stmt!(node!(esc p_id; attr!("label", esc symbol), attr!("shape",shape), attr!("fixedsize",true), attr!("width",size), attr!("height",size)))
        }).collect();

    let transition_nodes: Vec<_> = net
        .transitions
        .iter()
        .map(|(t_id, t)| {
            let label = t.label.as_ref().cloned().unwrap_or_default();
            let (font_size,width) = (12,1);
            let fill_color = if t.label.is_none() { "black" } else { "white" };
            stmt!(node!(esc t_id; attr!("label", esc label), attr!("shape","box"), attr!("fontsize",font_size),attr!("style","filled"), attr!("fillcolor",fill_color), attr!("width",width), attr!("height",0.5)))
        }).collect();

    let arcs: Vec<_> = net
        .arcs
        .iter()
        .map(|arc| {
            let (from_id, to_id) = match arc.from_to {
                super::petri_net_struct::ArcType::PlaceTransition(place_id, transition_id) => {
                    (place_id, transition_id)
                }
                super::petri_net_struct::ArcType::TransitionPlace(transition_id, place_id) => {
                    (transition_id, place_id)
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

    let g = graph!(strict di id!(esc Uuid::new_v4()),vec![global_graph_options,place_nodes,transition_nodes, arcs].into_iter().flatten().collect());
    g
}

///
/// Convery a DOT graph to a String containing the DOT source
///
pub fn graph_to_dot(g: &Graph) -> String {
    g.print(&mut PrinterContext::default())
}

///
/// Export the image of a [`PetriNet`] as a SVG file
///
/// Also consider using [`PetriNet::export_svg`] for convenience.
pub fn export_petri_net_image_svg<P: AsRef<std::path::Path>>(
    net: &PetriNet,
    path: P,
) -> Result<(), std::io::Error> {
    export_petri_net_image(net, path, Format::Svg, None)
}

///
/// Export the image of a [`PetriNet`] as a PNG file
///
/// Also consider using [`PetriNet::export_png`] for convenience.
pub fn export_petri_net_image_png<P: AsRef<std::path::Path>>(
    net: &PetriNet,
    path: P,
) -> Result<(), std::io::Error> {
    export_petri_net_image(net, path, Format::Png, Some(2.0))
}

#[cfg(test)]
mod test {

    use crate::{
        core::event_data::case_centric::xes::import_xes::{import_xes_file, XESImportOptions},
        discovery::case_centric::alphappp::auto_parameters,
        test_utils::get_test_data_path,
    };

    use super::{export_petri_net_image_png, export_petri_net_image_svg};

    #[test]
    pub fn test_petri_net_png_export() {
        let path = get_test_data_path().join("xes").join("AN1-example.xes");
        let log = import_xes_file(&path, XESImportOptions::default()).unwrap();
        let (_, pn) = auto_parameters::alphappp_discover_with_auto_parameters(&(&log).into());
        let export_path = get_test_data_path()
            .join("export")
            .join("petri-net-export-test.png");
        export_petri_net_image_png(&pn, export_path).unwrap();
    }

    #[test]
    pub fn test_petri_net_svg_export() {
        let path = get_test_data_path().join("xes").join("AN1-example.xes");
        let log = import_xes_file(&path, XESImportOptions::default()).unwrap();
        let (_, pn) = auto_parameters::alphappp_discover_with_auto_parameters(&(&log).into());
        let export_path = get_test_data_path()
            .join("export")
            .join("petri-net-export-test.svg");
        export_petri_net_image_svg(&pn, export_path).unwrap();
    }
}
