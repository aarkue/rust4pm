//! The visualization bindings, registered in the [`crate::bindings`] registry: five per model
//! (`<model>_layout_request`, `<model>_layout`, `<model>_scene_from_layout`, `<model>_svg` and
//! `<model>_svg_from_layout`), and [`layout_graph`], [`reroute_graph`], [`layout_tree`] and
//! [`render_scene_svg`] for any model.

use std::collections::BTreeMap;

use graphlay::json::{LayoutRequest, TreeRequest};
use graphlay::svg::{Palette, Scene};
use graphlay::{Layout, Point};
use macros_process_mining::register_binding;

use crate::core::event_data::case_centric::utils::partial_orders::PartialOrderTrace;
use crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;
use crate::core::process_models::dfg::DirectlyFollowsGraph;
use crate::core::process_models::object_centric::ocpn::json::ObjectCentricPetriNetJson;
use crate::core::process_models::oc_declare::{OCDeclareArc, ObjectInvolvementCounts};
use crate::core::process_models::ocdfg::OCDirectlyFollowsGraph;
use crate::core::process_models::process_tree::ProcessTree;
use crate::visualization::{
    dfg, engine, model, oc_declare, oc_type_graph, ocdfg, ocpn, partial_order, petri_net,
    process_tree, StyleOverrides, VisualizationError,
};
use crate::AcceptingPetriNet;

/// The five bindings of a model: `<model>_layout`, `<model>_svg`, `<model>_layout_request`,
/// `<model>_scene_from_layout` and `<model>_svg_from_layout`. The model's type is given in
/// parentheses, as it is taken by reference.
macro_rules! model_bindings {
    (
        $model:ident: ($($model_ty:tt)+),
        $module:ident::{$options:ident, $style:ident, $request:ident},
        $layout:ident, $svg:ident, $layout_request:ident, $scene_from_layout:ident,
        $svg_from_layout:ident $(,)?
    ) => {
        /// Lay the model out, returning the positions only.
        #[register_binding(stringify_error)]
        pub fn $layout(
            $model: &$($model_ty)+,
            #[bind(default)] options: $module::$options,
        ) -> Result<Layout, VisualizationError> {
            model::layout($model, &options)
        }

        /// Draw the model as a standalone SVG document.
        #[register_binding(stringify_error)]
        pub fn $svg(
            $model: &$($model_ty)+,
            #[bind(default)] options: $module::$options,
            #[bind(default)] style: $module::$style,
            #[bind(default)] palette: Option<Palette>,
        ) -> Result<String, VisualizationError> {
            let layout = model::layout($model, &options)?;
            let overrides = StyleOverrides::default();
            let scene = model::scene_from_layout($model, &layout, &options, &style, &overrides)?;
            Ok(engine::render_scene_svg(&scene, palette.as_ref()))
        }

        /// The layout request of the model, with the model element behind each node and edge.
        #[register_binding(stringify_error)]
        pub fn $layout_request(
            $model: &$($model_ty)+,
            #[bind(default)] options: $module::$options,
        ) -> Result<$module::$request, VisualizationError> {
            model::layout_request($model, &options)
        }

        /// Draw the model over a layout of its graph, with per-node and per-edge style overrides.
        #[register_binding(stringify_error)]
        pub fn $scene_from_layout(
            $model: &$($model_ty)+,
            layout: Layout,
            #[bind(default)] options: $module::$options,
            #[bind(default)] style: $module::$style,
            #[bind(default)] overrides: StyleOverrides,
        ) -> Result<Scene, VisualizationError> {
            model::scene_from_layout($model, &layout, &options, &style, &overrides)
        }

        /// Draw the model over a layout of its graph as a standalone SVG document, so no [`Scene`]
        /// crosses the binding boundary.
        #[register_binding(stringify_error)]
        pub fn $svg_from_layout(
            $model: &$($model_ty)+,
            layout: Layout,
            #[bind(default)] options: $module::$options,
            #[bind(default)] style: $module::$style,
            #[bind(default)] overrides: StyleOverrides,
            #[bind(default)] palette: Option<Palette>,
        ) -> Result<String, VisualizationError> {
            let scene = model::scene_from_layout($model, &layout, &options, &style, &overrides)?;
            Ok(engine::render_scene_svg(&scene, palette.as_ref()))
        }
    };
}

model_bindings! {
    net: (AcceptingPetriNet),
    petri_net::{PetriBuildOptions, PetriStyle, PetriLayoutRequest},
    petri_net_layout, petri_net_svg, petri_net_layout_request, petri_net_scene_from_layout,
    petri_net_svg_from_layout,
}

model_bindings! {
    ocpn: (ObjectCentricPetriNetJson),
    ocpn::{OcpnBuildOptions, OcpnStyle, OcpnLayoutRequest},
    ocpn_layout, ocpn_svg, ocpn_layout_request, ocpn_scene_from_layout, ocpn_svg_from_layout,
}

model_bindings! {
    dfg: (DirectlyFollowsGraph<'_>),
    dfg::{DfgBuildOptions, DfgStyle, DfgLayoutRequest},
    dfg_layout, dfg_svg, dfg_layout_request, dfg_scene_from_layout, dfg_svg_from_layout,
}

model_bindings! {
    ocdfg: (OCDirectlyFollowsGraph<'_>),
    ocdfg::{OcdfgBuildOptions, OcdfgStyle, OcdfgLayoutRequest},
    ocdfg_layout, ocdfg_svg, ocdfg_layout_request, ocdfg_scene_from_layout, ocdfg_svg_from_layout,
}

model_bindings! {
    arcs: ([OCDeclareArc]),
    oc_declare::{OCDeclareBuildOptions, OCDeclareStyle, OCDeclareLayoutRequest},
    oc_declare_layout, oc_declare_svg, oc_declare_layout_request, oc_declare_scene_from_layout,
    oc_declare_svg_from_layout,
}

/// Per activity of an OCEL, how many objects of each type one of its events involves, as the
/// `involvements` of the OC-DECLARE build options take them.
#[register_binding]
pub fn oc_declare_involvements(
    locel: &SlimLinkedOCEL,
) -> BTreeMap<String, BTreeMap<String, ObjectInvolvementCounts>> {
    oc_declare::oc_declare_involvements(locel)
}

model_bindings! {
    tree: (ProcessTree),
    process_tree::{ProcessTreeBuildOptions, ProcessTreeStyle, ProcessTreeLayoutRequest},
    process_tree_layout, process_tree_svg, process_tree_layout_request,
    process_tree_scene_from_layout, process_tree_svg_from_layout,
}

/// The type graph of an OCEL: its event and object types with their counts, and their qualified
/// E2O and O2O relations.
#[register_binding]
pub fn oc_type_graph_from_ocel(ocel: &SlimLinkedOCEL) -> oc_type_graph::OcTypeGraph {
    oc_type_graph::OcTypeGraph::from_linked_ocel(ocel)
}

model_bindings! {
    graph: (oc_type_graph::OcTypeGraph),
    oc_type_graph::{OcTypeGraphBuildOptions, OcTypeGraphStyle, OcTypeGraphLayoutRequest},
    oc_type_graph_layout, oc_type_graph_svg, oc_type_graph_layout_request,
    oc_type_graph_scene_from_layout, oc_type_graph_svg_from_layout,
}

model_bindings! {
    trace: (PartialOrderTrace),
    partial_order::{PartialOrderBuildOptions, PartialOrderStyle, PartialOrderLayoutRequest},
    partial_order_layout, partial_order_svg, partial_order_layout_request,
    partial_order_scene_from_layout, partial_order_svg_from_layout,
}

/// Lay out the graph of a layout request, e.g., the `request` of a `<model>_layout_request`.
#[register_binding(stringify_error)]
pub fn layout_graph(request: LayoutRequest) -> Result<Layout, VisualizationError> {
    engine::layout_graph(&request)
}

/// Keep every node of a layout request where it is and route the edges again, e.g., after the
/// user dragged a node. `positions` are the node centers in node order. Without them, every node
/// of the request needs a `position`.
#[register_binding(stringify_error)]
pub fn reroute_graph(
    request: LayoutRequest,
    #[bind(default)] positions: Vec<Point>,
) -> Result<Layout, VisualizationError> {
    engine::reroute_graph(&request, &positions)
}

/// Lay out the rooted tree or forest of a tidy-tree request, e.g., the `request` of
/// `process_tree_layout_request`.
#[register_binding(stringify_error)]
pub fn layout_tree(request: TreeRequest) -> Result<Layout, VisualizationError> {
    engine::layout_tree(&request)
}

/// Draw a scene, e.g., from a `<model>_scene_from_layout`, as a standalone SVG document. Elements
/// that name no color fall back to `palette`.
#[register_binding]
pub fn render_scene_svg(scene: Scene, #[bind(default)] palette: Option<Palette>) -> String {
    engine::render_scene_svg(&scene, palette.as_ref())
}

#[cfg(test)]
mod tests {
    use crate::bindings::list_functions_meta;
    use crate::core::event_data::case_centric::utils::partial_orders::PartialOrderTrace;

    #[test]
    fn the_adapter_bindings_are_registered_once() {
        let names: Vec<String> = list_functions_meta().into_iter().map(|b| b.name).collect();
        for expected in [
            "petri_net_layout",
            "petri_net_svg",
            "petri_net_layout_request",
            "petri_net_scene_from_layout",
            "ocpn_layout",
            "ocpn_svg",
            "ocpn_layout_request",
            "ocpn_scene_from_layout",
            "dfg_layout",
            "dfg_svg",
            "dfg_layout_request",
            "dfg_scene_from_layout",
            "ocdfg_layout",
            "ocdfg_svg",
            "ocdfg_layout_request",
            "ocdfg_scene_from_layout",
            "oc_declare_layout",
            "oc_declare_svg",
            "oc_declare_layout_request",
            "oc_declare_scene_from_layout",
            "process_tree_layout",
            "process_tree_svg",
            "process_tree_layout_request",
            "process_tree_scene_from_layout",
            "oc_type_graph_from_ocel",
            "oc_type_graph_layout",
            "oc_type_graph_svg",
            "oc_type_graph_layout_request",
            "oc_type_graph_scene_from_layout",
            "layout_graph",
            "reroute_graph",
            "layout_tree",
            "render_scene_svg",
            "petri_net_svg_from_layout",
            "ocpn_svg_from_layout",
            "dfg_svg_from_layout",
            "ocdfg_svg_from_layout",
            "oc_declare_svg_from_layout",
            "process_tree_svg_from_layout",
            "oc_type_graph_svg_from_layout",
            "oc_declare_involvements",
            "partial_order_layout",
            "partial_order_svg",
            "partial_order_layout_request",
            "partial_order_scene_from_layout",
            "partial_order_svg_from_layout",
        ] {
            assert_eq!(
                names.iter().filter(|n| *n == expected).count(),
                1,
                "expected exactly one binding named {expected}"
            );
        }
    }

    fn call(name: &str, args: serde_json::Value) -> serde_json::Value {
        let binding = crate::bindings::list_functions()
            .into_iter()
            .find(|b| b.name == name)
            .unwrap_or_else(|| panic!("{name} is registered"));
        let state = crate::bindings::AppState::default();
        let out = crate::bindings::call(binding, &args, &state).unwrap_or_else(|e| panic!("{e}"));
        serde_json::from_slice(&out).unwrap()
    }

    /// What a host does: take the request, lay it out, let the user drag a node, re-route, and
    /// export the rearranged drawing with a dimmed node.
    #[test]
    fn a_request_goes_through_layout_reroute_scene_and_svg() {
        use crate::core::process_models::dfg::DirectlyFollowsGraph;
        use crate::test_utils::get_test_data_path;
        use crate::{EventLog, Importable};
        use serde_json::json;

        let path = get_test_data_path().join("xes").join("small-example.xes");
        let log = EventLog::import_from_path(path).unwrap();
        let dfg = serde_json::to_value(DirectlyFollowsGraph::discover(&log)).unwrap();
        let options = json!({"layout_patch": {"routing": {"style": "diagonal"}}});

        let request = call(
            "dfg_layout_request",
            json!({"dfg": dfg, "options": options}),
        );
        assert_eq!(request["nodes"][0], json!({"kind": "start"}));
        assert_eq!(request["request"]["preset"], "process_map");
        assert_eq!(
            request["request"]["options"]["routing"]["style"],
            "diagonal"
        );

        let layout = call("layout_graph", json!({"request": request["request"]}));
        let mut positions: Vec<serde_json::Value> = layout["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["center"].clone())
            .collect();
        positions[2]["y"] = json!(positions[2]["y"].as_f64().unwrap() + 300.0);
        let moved = call(
            "reroute_graph",
            json!({"request": request["request"], "positions": positions}),
        );
        assert_eq!(moved["nodes"][2]["center"], positions[2]);

        let scene = call(
            "dfg_scene_from_layout",
            json!({
                "dfg": dfg,
                "layout": moved,
                "options": options,
                "overrides": {"nodes": {"2": {"opacity": 0.35}}},
            }),
        );
        let svg = call("render_scene_svg", json!({"scene": scene}));
        assert!(svg.as_str().unwrap().contains("opacity=\"0.35\""));
    }

    #[test]
    fn an_ocpn_request_goes_through_layout_and_svg() {
        use serde_json::json;
        let ocpn = json!({
            "places": [
                {"name": "p0", "object_type": "orders"},
                {"name": "p1", "object_type": "items"},
            ],
            "transitions": [{"name": "place order", "label": "place order"}],
            "arcs": [
                {"source": "p0", "target": "place order"},
                {"source": "p1", "target": "place order", "type": "variable"},
            ],
            "initial_marking": {"p0": 1},
            "final_marking": {},
        });
        let request = call("ocpn_layout_request", json!({"ocpn": ocpn}));
        assert_eq!(
            request["nodes"][2],
            json!({"kind": "transition", "index": 0})
        );
        assert_eq!(request["edges"][1]["arc"], 1);
        let layout = call("layout_graph", json!({"request": request["request"]}));
        let svg = call(
            "ocpn_svg_from_layout",
            json!({
                "ocpn": ocpn,
                "layout": layout,
                "overrides": {"edges": {"0": {"dash": "1 2"}}},
            }),
        );
        let svg = svg.as_str().unwrap();
        assert!(svg.contains("stroke-dasharray=\"1 2\""));
        assert!(svg.contains("stroke-dasharray=\"6 3\""));
    }

    #[test]
    fn a_tree_request_goes_through_layout_tree() {
        use crate::core::process_models::process_tree::{
            Leaf, Node, Operator, OperatorType, ProcessTree,
        };
        use serde_json::json;
        let mut sequence = Operator::new(OperatorType::Sequence);
        sequence.children = vec![
            Node::Leaf(Leaf::new(Some("a".to_string()))),
            Node::Leaf(Leaf::new(None)),
        ];
        let tree = serde_json::to_value(ProcessTree::new(Node::Operator(sequence))).unwrap();
        let request = call("process_tree_layout_request", json!({"tree": tree}));
        assert_eq!(request["nodes"][1], json!({"kind": "leaf", "path": [0]}));
        let layout = call("layout_tree", json!({"request": request["request"]}));
        assert_eq!(layout["nodes"].as_array().unwrap().len(), 3);
    }

    /// Needs `graphlay`'s `schemars` feature, which `bindings` turns on.
    #[test]
    fn a_layout_return_type_has_a_schema() {
        let binding = list_functions_meta()
            .into_iter()
            .find(|b| b.name == "petri_net_layout")
            .expect("petri_net_layout is registered");
        assert!(binding.return_type.is_object());
        assert!(binding.return_type.to_string().contains("nodes"));
        assert!(binding.args.iter().any(|(name, _)| name == "options"));
        assert_eq!(binding.required_args, vec!["net".to_string()]);
    }

    #[test]
    fn a_partial_order_trace_goes_through_its_bindings() {
        use crate::core::event_data::case_centric::Event;
        use serde_json::json;
        let mut trace = PartialOrderTrace::new();
        let (a, b) = (Event::new("a".to_string()), Event::new("b".to_string()));
        trace.add_event(&a);
        trace.add_event(&b);
        trace.add_partial_relation(&a, &b);
        let trace = serde_json::to_value(trace).unwrap();
        let request = call("partial_order_layout_request", json!({"trace": trace}));
        assert_eq!(request["edges"].as_array().unwrap().len(), 1);
        let layout = call("layout_graph", json!({"request": request["request"]}));
        let svg = call(
            "partial_order_svg_from_layout",
            json!({"trace": trace, "layout": layout, "style": {"base": {"background": "#fafafa"}}}),
        );
        assert!(svg.as_str().unwrap().contains("#fafafa"));
        let svg = call("partial_order_svg", json!({"trace": trace}));
        assert!(svg.as_str().unwrap().contains(">b</text>"));
    }

    #[test]
    fn an_svg_binding_takes_a_style() {
        use crate::core::process_models::dfg::DirectlyFollowsGraph;
        use crate::test_utils::get_test_data_path;
        use crate::{EventLog, Importable};
        use serde_json::json;
        let path = get_test_data_path().join("xes").join("small-example.xes");
        let log = EventLog::import_from_path(path).unwrap();
        let dfg = serde_json::to_value(DirectlyFollowsGraph::discover(&log)).unwrap();
        let svg = call(
            "dfg_svg",
            json!({"dfg": dfg, "style": {"start_fill": "#123456"}}),
        );
        assert!(svg.as_str().unwrap().contains("#123456"));
    }
}
