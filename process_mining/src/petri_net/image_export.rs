use std::{fs::File, io::Write};

use graphviz_rust::{
    cmd::Format,
    dot_generator::{attr, edge, graph, id, node, node_id, stmt},
    dot_structures::*,
    printer::{DotPrinter, PrinterContext},
};

use crate::PetriNet;

///
/// Export the image of a [`PetriNet`]
///
/// Also see [`export_petri_net_image_svg`] and [`export_petri_net_image_png`]
///
pub fn export_petri_net_image(
    net: &PetriNet,
    path: &str,
    format: Format,
    dpi_factor: Option<f32>,
) -> Result<(), std::io::Error> {
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
            let fill_color = if t.label.is_none() { "white"} else {"black"};
            stmt!(node!(esc t_id; attr!("label", esc label), attr!("shape","box"), attr!("fillcolor",fill_color), attr!("fixedsize",true), attr!("width",1.0), attr!("height",0.5)))
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
            stmt!(edge!(node_id!(esc from_id) => node_id!(esc to_id)))
        })
        .collect();

    let mut global_graph_options = vec![stmt!(attr!("rankdir", "LR"))];
    if let Some(dpi_fac) = dpi_factor {
        global_graph_options.push(stmt!(attr!("dpi", (dpi_fac * 96.0))))
    }

    let g = graph!(strict di id!("id"),vec![global_graph_options,place_nodes,transition_nodes, arcs].into_iter().flatten().collect());

    g.print(&mut PrinterContext::default());

    let mut out = graphviz_rust::exec(g, &mut PrinterContext::default(), vec![format.into()])?;

    let mut f = File::create(path)?;
    f.write(&mut out)?;
    Ok(())
}

///
/// Export the image of a [`PetriNet`] as a SVG file
///
pub fn export_petri_net_image_svg(net: &PetriNet, path: &str) -> Result<(), std::io::Error> {
    export_petri_net_image(net, path, Format::Svg, None)
}

///
/// Export the image of a [`PetriNet`] as a PNG file
///
pub fn export_petri_net_image_png(net: &PetriNet, path: &str) -> Result<(), std::io::Error> {
    export_petri_net_image(net, path, Format::Png, Some(2.0))
}

#[cfg(test)]
mod test {
    use std::fs::{remove_file, File};

    use crate::{
        alphappp::auto_parameters, import_ocel_xml_slice, import_xes_file, import_xes_slice,
        XESImportOptions,
    };

    use super::{export_petri_net_image_png, export_petri_net_image_svg};

    #[test]
    pub fn test_petri_net_png_export() {
        let xes_bytes = include_bytes!("../event_log/tests/test_data/AN1-example.xes");
        let log = import_xes_slice(xes_bytes, false, XESImportOptions::default()).unwrap();
        let (_, pn) = auto_parameters::alphappp_discover_with_auto_parameters(&(&log).into());
        export_petri_net_image_png(&pn, "/tmp/petri-net-export-test.png").unwrap();
        remove_file("/tmp/petri-net-export-test.png").unwrap();
    }

    #[test]
    pub fn test_petri_net_svg_export() {
        let xes_bytes = include_bytes!("../event_log/tests/test_data/AN1-example.xes");
        let log = import_xes_slice(xes_bytes, false, XESImportOptions::default()).unwrap();
        let (_, pn) = auto_parameters::alphappp_discover_with_auto_parameters(&(&log).into());
        export_petri_net_image_svg(&pn, "/tmp/petri-net-export-test.svg").unwrap();
        remove_file("/tmp/petri-net-export-test.svg").unwrap();
    }
}
