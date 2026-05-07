//! Image Export of process trees
//!
//! 🔐 Requires the `graphviz-export` feature to be enabled
use std::{fs::File, io::Write};

use crate::core::process_models::process_tree::{
    process_tree_struct, Leaf, LeafLabel, Operator, ProcessTree,
};
use graphviz_rust::{
    cmd::Format,
    dot_generator::{attr, edge, graph, id, node, node_id, stmt},
    dot_structures::*,
    printer::{DotPrinter, PrinterContext},
};
use macros_process_mining::register_binding;
use uuid::Uuid;

///
/// Export the image of a [`ProcessTree`]
///
/// Also see [`export_process_tree_image_svg`] and [`export_process_tree_image_png`]
///
pub fn export_process_tree_image<P: AsRef<std::path::Path>>(
    process_tree: &ProcessTree,
    path: P,
    format: Format,
    dpi_factor: Option<f32>,
) -> Result<(), std::io::Error> {
    let g = export_pt_to_dot_graph(process_tree, dpi_factor);

    g.print(&mut PrinterContext::default());

    let out = graphviz_rust::exec(g, &mut PrinterContext::default(), vec![format.into()])?;

    let mut f = File::create(path)?;
    f.write_all(&out)?;
    Ok(())
}

///
/// Creates the GraphViz code for a [`Node`] and expects a [`Uuid`] that is consistent
/// throughout the graph
///
fn tree_node_to_gviz_node(node: &process_tree_struct::Node, node_id: Uuid) -> Stmt {
    match node {
        process_tree_struct::Node::Operator(op) => operator_to_node(op, node_id),
        process_tree_struct::Node::Leaf(leaf) => leaf_to_node(leaf, node_id),
    }
}

///
/// Creates the GraphViz code for a [`Operator`] and expects a [`Uuid`] that is consistent
/// throughout the graph
///
fn operator_to_node(op: &Operator, op_id: Uuid) -> Stmt {
    let symbol = op.operator_type.to_string();
    let shape = "circle";
    let size = 0.5;
    stmt!(
        node!(esc op_id; attr!("label", esc symbol), attr!("shape", shape), attr!("fixedsize", true), attr!("width", size), attr!("height", size))
    )
}

///
/// Creates the GraphViz code for a [`Leaf`] and expects a [`Uuid`] that is consistent
/// throughout the graph
///
fn leaf_to_node(leaf: &Leaf, leaf_id: Uuid) -> Stmt {
    let (label, is_silent) = match &leaf.activity_label {
        LeafLabel::Activity(act_label) => (act_label.clone(), false),
        LeafLabel::Tau => ("".to_string(), true),
    };
    let (font_size, width, height) = (12, 1, 0.5);
    let fill_color = if is_silent { "black" } else { "white" };
    stmt!(
        node!(esc leaf_id; attr!("label", esc label), attr!("shape", "box"), attr!("fontsize", font_size), attr!("style", "filled"), attr!("fillcolor", fill_color), attr!("width", width), attr!("height", height))
    )
}

///
/// Creates the GraphViz code for an edge between two process tree [`Node`]s and expects two [`Uuid`]
/// that are consistent throughout the graph for creating the edge
///
fn arc_to_edge(from: Uuid, to: Uuid) -> Stmt {
    let attrs = Vec::default();

    stmt!(edge!(node_id!(esc from) => node_id!(esc to), attrs))
}

///
/// Export a [`ProcessTree`] to a DOT graph (used in Graphviz)
///
pub fn export_pt_to_dot_graph(pt: &ProcessTree, dpi_factor: Option<f32>) -> Graph {
    let mut gviz_nodes = Vec::new();
    let mut gviz_edges = Vec::new();

    let root_id = Uuid::new_v4();
    gviz_nodes.push(tree_node_to_gviz_node(&pt.root, root_id));

    let mut curr_nodes = vec![(root_id, &pt.root)];
    let mut next_nodes = Vec::new();

    while !curr_nodes.is_empty() {
        curr_nodes.iter().for_each(|(from_id, node)| match node {
            process_tree_struct::Node::Operator(op) => op.children.iter().for_each(|child| {
                let child_id = Uuid::new_v4();
                gviz_nodes.push(tree_node_to_gviz_node(child, child_id));
                gviz_edges.push(arc_to_edge(*from_id, child_id));
                next_nodes.push((child_id, child));
            }),
            process_tree_struct::Node::Leaf(_) => {}
        });

        curr_nodes = next_nodes;
        next_nodes = Vec::new();
    }

    let mut global_graph_options = vec![stmt!(GraphAttributes::Node(vec![
        attr!("fontname", esc "DejaVu Sans")
    ]))];
    if let Some(dpi_fac) = dpi_factor {
        global_graph_options.push(stmt!(attr!("dpi", (dpi_fac * 96.0))))
    }

    let g = graph!(strict di id!(esc Uuid::new_v4()),vec![global_graph_options, gviz_nodes, gviz_edges].into_iter().flatten().collect());
    g
}

///
/// Convert a DOT graph to a String containing the DOT source
///
pub fn graph_to_dot(g: &Graph) -> String {
    g.print(&mut PrinterContext::default())
}

///
/// Export the image of a [`ProcessTree`] as a SVG file
///
/// Also consider using [`ProcessTree::export_svg`] for convenience.
#[register_binding(stringify_error)]
pub fn export_process_tree_image_svg(
    process_tree: &ProcessTree,
    path: impl AsRef<std::path::Path>,
) -> Result<(), std::io::Error> {
    export_process_tree_image(process_tree, path, Format::Svg, None)
}

///
/// Export the image of a [`ProcessTree`] as a PNG file
///
/// Also consider using [`ProcessTree::export_png`] for convenience.
#[register_binding(stringify_error)]
pub fn export_process_tree_image_png(
    process_tree: &ProcessTree,
    path: impl AsRef<std::path::Path>,
) -> Result<(), std::io::Error> {
    export_process_tree_image(process_tree, path, Format::Png, Some(2.0))
}

#[cfg(test)]
mod test {
    use crate::core::process_models::process_tree::image_export::{
        export_process_tree_image_png, export_process_tree_image_svg,
    };
    use crate::core::process_models::process_tree::{
        Leaf, Node, Operator, OperatorType, ProcessTree,
    };
    use crate::test_utils::get_test_data_path;

    fn create_example_tree() -> ProcessTree {
        let mut seq = Operator::new(OperatorType::Sequence);
        let leaf_a = Leaf::new(Some("a".to_string()));
        seq.children.push(Node::Leaf(leaf_a));

        let mut conc = Operator::new(OperatorType::Concurrency);
        let leaf_a = Leaf::new(Some("a".to_string()));
        let leaf_b = Leaf::new(Some("b".to_string()));

        conc.children.push(Node::Leaf(leaf_a));
        conc.children.push(Node::Leaf(leaf_b));

        let mut loop_op = Operator::new(OperatorType::Loop);
        let leaf_e = Leaf::new(Some("e".to_string()));
        let leaf_f = Leaf::new(Some("f".to_string()));
        let leaf_silent = Leaf::new(None);

        loop_op.children.push(Node::Leaf(leaf_e));
        loop_op.children.push(Node::Operator(conc));
        loop_op.children.push(Node::Leaf(leaf_f));
        loop_op.children.push(Node::Leaf(leaf_silent));

        let mut choice = Operator::new(OperatorType::ExclusiveChoice);
        let leaf_b = Leaf::new(Some("b".to_string()));
        let leaf_c = Leaf::new(Some("c".to_string()));
        let leaf_d = Leaf::new(Some("d".to_string()));
        choice.children.push(Node::Leaf(leaf_b));
        choice.children.push(Node::Leaf(leaf_c));
        choice.children.push(Node::Leaf(leaf_d));

        seq.children.push(Node::Operator(loop_op));
        seq.children.push(Node::Operator(choice));

        ProcessTree::new(Node::Operator(seq))
    }

    #[test]
    pub fn test_petri_net_png_export() {
        let tree = create_example_tree();

        let export_path = get_test_data_path()
            .join("export")
            .join("process-tree-export-test.png");
        export_process_tree_image_png(&tree, export_path).unwrap();
    }

    #[test]
    pub fn test_petri_net_svg_export() {
        let tree = create_example_tree();

        let export_path = get_test_data_path()
            .join("export")
            .join("process-tree-export-test.svg");
        export_process_tree_image_svg(&tree, export_path).unwrap();
    }
}
