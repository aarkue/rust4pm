use std::{cmp::Ordering, fs::File, io::Write};

use graphviz_rust::{
    cmd::Format,
    dot_generator::{attr, edge, graph, id, node, node_id, stmt},
    dot_structures::*,
    printer::{DotPrinter, PrinterContext},
};
use uuid::Uuid;

use crate::core::process_models::case_centric::dfg::dfg_struct::DirectlyFollowsGraph;

///
/// Export the image of a [`DirectlyFollowsGraph`]
///
/// Also see [`export_dfg_image_svg`] and [`export_dfg_image_png`]
///
pub fn export_dfg_image<P: AsRef<std::path::Path>>(
    dfg: &DirectlyFollowsGraph<'_>,
    path: P,
    format: Format,
    dpi_factor: Option<f32>,
) -> Result<(), std::io::Error> {
    let g = export_dfg_to_dot_graph(dfg, dpi_factor);

    g.print(&mut PrinterContext::default());

    let out = graphviz_rust::exec(g, &mut PrinterContext::default(), vec![format.into()])?;

    let mut f = File::create(path)?;
    f.write_all(&out)?;
    Ok(())
}

///
/// Export the a [`DirectlyFollowsGraph`] to a DOT graph (used in Graphviz)
///
/// Also see [`export_dfg_image`], as well as [`export_dfg_image_svg`] and [`export_dfg_image_png`]
///
pub fn export_dfg_to_dot_graph(dfg: &DirectlyFollowsGraph<'_>, dpi_factor: Option<f32>) -> Graph {
    let mut sorted_acts: Vec<_> = dfg.activities.iter().collect();
    sorted_acts.sort_by(|(a_act, _), (b_act, _)| {
        if dfg.start_activities.contains(*a_act) {
            Ordering::Less
        } else if dfg.start_activities.contains(*b_act) || dfg.end_activities.contains(*a_act) {
            Ordering::Greater
        } else if dfg.end_activities.contains(*b_act) {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    });
    let activity_nodes: Vec<Stmt> = sorted_acts
        .into_iter()
        .map(|(x, &y)| {
            let mut counted_label = x.to_owned();
            counted_label.push_str(": ");
            counted_label.push_str(&y.to_string());
            let fill_color: String = if dfg.is_start_activity(x) && dfg.is_end_activity(x) {
                // "\"#009966:#C1272D\"".into()
                "\"#4B9969:#D4001F\"".into()
            } else if dfg.is_start_activity(x) {
                "\"#4B9969\"".into()
            } else if dfg.is_end_activity(x) {
                "\"#D4001F\"".into()
            } else {
                "\"white\"".into()
            };

            let (font_size, width) = (12, 1);
            stmt!(node!(esc &x; attr!("label", esc counted_label), attr!("gradientangle", "45"), attr!("shape","box"), attr!("fontsize",font_size),attr!("style","filled"), attr!("fillcolor",fill_color), attr!("width",width), attr!("height",0.5)))
        }).collect();

    let arcs: Vec<Stmt> = dfg
        .directly_follows_relations
        .iter()
        .map(|(dfr, &frequency)| {
            let attrs = if frequency == 1 {
                Vec::default()
            } else {
                vec![attr!("label", (format!("{}", frequency)))]
            };
            stmt!(edge!(node_id!(esc dfr.0) => node_id!(esc dfr.1), attrs))
        })
        .collect();

    let mut global_graph_options = vec![stmt!(attr!("rankdir", "LR"))];
    if let Some(dpi_fac) = dpi_factor {
        global_graph_options.push(stmt!(attr!("dpi", (dpi_fac * 96.0))))
    }

    graph!(strict di id!(esc Uuid::new_v4()),vec![global_graph_options,activity_nodes, arcs].into_iter().flatten().collect())
}

///
/// Convert a DOT graph to a String containing the DOT source
///
pub fn graph_to_dot(g: &Graph) -> String {
    g.print(&mut PrinterContext::default())
}

///
/// Export the image of a [`DirectlyFollowsGraph`] as a SVG file
///
/// Also consider using [`DirectlyFollowsGraph::export_svg`] for convenience.
pub fn export_dfg_image_svg<P: AsRef<std::path::Path>>(
    dfg: &DirectlyFollowsGraph<'_>,
    path: P,
) -> Result<(), std::io::Error> {
    export_dfg_image(dfg, path, Format::Svg, None)
}

///
/// Export the image of a [`DirectlyFollowsGraph`] as a PNG file
///
/// Also consider using [`DirectlyFollowsGraph::export_png`] for convenience.
pub fn export_dfg_image_png<P: AsRef<std::path::Path>>(
    dfg: &DirectlyFollowsGraph<'_>,
    path: P,
) -> Result<(), std::io::Error> {
    export_dfg_image(dfg, path, Format::Png, Some(2.0))
}

#[cfg(test)]
mod test {
    pub const SAMPLE_JSON_DFG: &str = r#"
{
    "activities": {
        "Sleep": 13,
        "Cook": 3,
        "Work": 11,
        "Have fun": 9
    },
    "directly_follows_relations": [
        [
            ["Work","Sleep"],
            4
        ],
        [
            ["Have fun","Sleep"],
            9
        ],
        [
            ["Work","Have fun"],
            6
        ],
        [
            ["Cook","Have fun"],
            3
        ]
    ],
    "start_activities": [
        "Work",
        "Cook"
    ],
    "end_activities": [
        "Work",
        "Sleep"
    ]
}"#;

    use crate::{
        core::process_models::case_centric::dfg::dfg_struct::DirectlyFollowsGraph,
        test_utils::get_test_data_path,
    };

    use super::{export_dfg_image_png, export_dfg_image_svg};

    #[test]
    pub fn test_dfg_png_export() {
        let export_path = get_test_data_path()
            .join("export")
            .join("dfg-export-test.png");
        let dfg: DirectlyFollowsGraph<'_> = serde_json::from_str(SAMPLE_JSON_DFG).unwrap();
        export_dfg_image_png(&dfg, &export_path).unwrap();
    }

    #[test]
    pub fn test_dfg_svg_export() {
        let export_path = get_test_data_path()
            .join("export")
            .join("dfg-export-test.svg");
        let dfg: DirectlyFollowsGraph<'_> = serde_json::from_str(SAMPLE_JSON_DFG).unwrap();
        export_dfg_image_svg(&dfg, &export_path).unwrap();
    }
}
