use crate::event_log::event_log_struct::EventLogClassifier;
use crate::partial_orders::partial_event_log_struct::{EventHash, PartialOrderTrace};
use graphviz_rust::{
    cmd::Format,
    dot_generator::{attr, edge, graph, id, node, node_id, stmt},
    dot_structures::*,
    printer::{DotPrinter, PrinterContext},
};
use std::collections::HashMap;
use std::{fs::File, io::Write};
use uuid::Uuid;

///
/// Export the image of a [`PartialOrderTrace`]
///
/// Also see [`export_p_trace_image_svg`] and [`export_p_trace_image_png`]
///
pub fn export_p_trace_image<P: AsRef<std::path::Path>>(
    p_trace: &PartialOrderTrace,
    classifier: &EventLogClassifier,
    path: P,
    format: Format,
    dpi_factor: Option<f32>,
) -> Result<(), std::io::Error> {
    let g = export_p_trace_to_dot_graph(p_trace, classifier, dpi_factor);

    g.print(&mut PrinterContext::default());

    let out = graphviz_rust::exec(g, &mut PrinterContext::default(), vec![format.into()])?;

    let mut f = File::create(path)?;
    f.write_all(&out)?;
    Ok(())
}

///
/// Export the a [`PartialOrderTrace`] to a DOT graph (used in Graphviz)
///
/// Also see [`export_p_trace_image`], as well as [`export_p_trace_image_svg`] and [`export_p_trace_image_png`]
///
pub fn export_p_trace_to_dot_graph(
    p_trace: &PartialOrderTrace,
    classifier: &EventLogClassifier,
    dpi_factor: Option<f32>,
) -> Graph {
    let mut event_hash_to_classified_event: HashMap<&EventHash, String> = HashMap::new();

    p_trace.event_map.iter().for_each(|(event_hash, event)| {
        event_hash_to_classified_event.insert(event_hash, classifier.get_class_identity(event));
    });

    let mut sorted_event_hash: Vec<EventHash> = p_trace.event_map.keys().cloned().collect();
    sorted_event_hash.sort_by_key(|x| event_hash_to_classified_event.get(x).unwrap());

    let nodes: Vec<Stmt> = sorted_event_hash
        .iter()
        .map(|event_hash: &EventHash| {
            let label = event_hash_to_classified_event.get(event_hash).unwrap();

            let (font_size, width) = (12, 1);
            stmt!(node!(esc label; attr!("label", esc label), attr!("gradientangle", "45"), attr!("shape","box"), attr!("fontsize",font_size),attr!("style","filled"), attr!("width",width), attr!("height",0.5)))
        }).collect();

    let arcs: Vec<Stmt> = p_trace
        .partial_relations
        .iter()
        .map(|(from_hash, to_hash)| {
            let from_label = event_hash_to_classified_event.get(from_hash).unwrap();
            let to_label = event_hash_to_classified_event.get(to_hash).unwrap();

            stmt!(edge!(node_id!(esc from_label) => node_id!(esc to_label), Vec::default()))
        })
        .collect();

    let mut global_graph_options = vec![stmt!(attr!("rankdir", "LR"))];
    if let Some(dpi_fac) = dpi_factor {
        global_graph_options.push(stmt!(attr!("dpi", (dpi_fac * 96.0))))
    }

    graph!(strict di id!(esc Uuid::new_v4()),vec![global_graph_options,nodes, arcs].into_iter().flatten().collect())
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
pub fn export_p_trace_image_svg<P: AsRef<std::path::Path>>(
    p_trace: &PartialOrderTrace,
    classifier: &EventLogClassifier,
    path: P,
) -> Result<(), std::io::Error> {
    export_p_trace_image(p_trace, classifier, path, Format::Svg, None)
}

///
/// Export the image of a [`DirectlyFollowsGraph`] as a PNG file
///
/// Also consider using [`DirectlyFollowsGraph::export_png`] for convenience.
pub fn export_p_trace_image_png<P: AsRef<std::path::Path>>(
    p_trace: &PartialOrderTrace,
    classifier: &EventLogClassifier,
    path: P,
) -> Result<(), std::io::Error> {
    export_p_trace_image(p_trace, classifier, path, Format::Png, Some(2.0))
}

#[cfg(test)]
mod test {
    pub const SAMPLE_JSON_P_TRACE: &str = r#"
{
    "attributes":[],
    "event_map":
    {
        "10110542754164153265":
        {
            "attributes":[{
                "key":"concept:name",
                "value":{"type":"String",
                "content":"Wait for call from Internet provider"},"own_attributes":null
            }]
        },
        "10135014032808600890":
        {
            "attributes":[{
                "key":"concept:name",
                "value":{"type":"String","content":"Wake up"},
                "own_attributes":null
            }]
        },
        "16623231828871474506":
        {
            "attributes":[{
                "key":"concept:name",
                "value":{"type":"String","content":"Breakfast"},
                "own_attributes":null
            }]
        },
        "5747163295916315711":
        {
            "attributes":[{
                "key":"concept:name",
                "value":{"type":"String","content":"Make Coffee"},
                "own_attributes":null
            }]
        },
        "18102607635049523792":
        {
            "attributes":[{
                "key":"concept:name",
                "value":{"type":"String","content":"Brush teeth"},
                "own_attributes":null
            }]
        },
        "6954268098552642400":
        {
            "attributes":[{
                "key":"concept:name",
                "value":{"type":"String","content":"Listen to Podcast"},
                "own_attributes":null
            }]
        },
        "10735273421821633029":
        {
            "attributes":[{
                "key":"concept:name",
                "value":{"type":"String","content":"Work"},
                "own_attributes":null
            }]
        }
    },
    "partial_relations":
    [
        [10135014032808600890,5747163295916315711],
        [16623231828871474506,10735273421821633029],
        [6954268098552642400,10735273421821633029],
        [18102607635049523792,16623231828871474506],
        [5747163295916315711,16623231828871474506],
        [10135014032808600890,18102607635049523792],
        [10135014032808600890,6954268098552642400]
    ]
}"#;

    use super::{export_p_trace_image_png, export_p_trace_image_svg};
    use crate::event_log::event_log_struct::EventLogClassifier;
    use crate::partial_orders::partial_event_log_struct::PartialOrderTrace;
    use crate::utils::test_utils::get_test_data_path;
    use std::default::Default;

    #[test]
    pub fn test_dfg_png_export() {
        let p_trace: PartialOrderTrace = serde_json::from_str(SAMPLE_JSON_P_TRACE).unwrap();
        let export_path = get_test_data_path()
            .join("export")
            .join("p_trace-export-test.png");

        export_p_trace_image_png(&p_trace, &EventLogClassifier::default(), export_path).unwrap();
    }

    #[test]
    pub fn test_dfg_svg_export() {
        let p_trace: PartialOrderTrace = serde_json::from_str(SAMPLE_JSON_P_TRACE).unwrap();
        let export_path = get_test_data_path()
            .join("export")
            .join("p_trace-export-test.svg");

        export_p_trace_image_svg(&p_trace, &EventLogClassifier::default(), export_path).unwrap();
    }
}
