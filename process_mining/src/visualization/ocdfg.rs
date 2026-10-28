//! Laying out and drawing an [`OCDirectlyFollowsGraph`]: one DFG per object type in one picture.

use graphlay::json::LayoutRequest;
use graphlay::layout::{EdgeRoute, Placement};
use graphlay::svg::{SceneEdge, SceneNode, Stroke};
use graphlay::{Edge, Graph, GroupId, Layout, Preset, Shape};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::core::process_models::ocdfg::OCDirectlyFollowsGraph;
use crate::visualization::annotation::node_text;
use crate::visualization::dfg::{add_dfg, count_text, DfgEdgeRef, DfgNodeRef};
use crate::visualization::engine::VisualizationError;
use crate::visualization::graph_build::{GraphAcc, LabelWidth};
use crate::visualization::model::{Annotations, LayoutSettings, ModelLayoutRequest, Visualize};
use crate::visualization::styling::{
    edge_label, key_color, lighten, node_label, push_extra_line, scene_edge, scene_node,
    terminal_mark, BaseStyle,
};

/// A node of an [`OCDirectlyFollowsGraph`] drawing, e.g., `{"kind": "start", "name": "orders"}`.
/// The name is the object type of a terminal, or the activity.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "kind", content = "name", rename_all = "snake_case")]
pub enum OcdfgNodeRef {
    /// One object type's START terminal.
    Start(String),
    /// One object type's END terminal.
    End(String),
    /// An activity, shared by every object type.
    Activity(String),
}

/// An edge of an [`OCDirectlyFollowsGraph`] drawing, in one object type's DFG.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct OcdfgEdgeRef {
    /// The object type.
    pub object_type: String,
    /// The edge within that type's directly-follows graph.
    pub edge: DfgEdgeRef,
}

impl OcdfgEdgeRef {
    /// The object type, source and target, which key the edge's annotation.
    pub(crate) fn annotation_key(&self) -> (String, DfgNodeRef, DfgNodeRef) {
        let (from, to) = self.edge.endpoints();
        (self.object_type.clone(), from, to)
    }
}

/// Labels, layout weights and line widths of an [`OCDirectlyFollowsGraph`] drawing that replace
/// the counts, e.g., durations. Edges are keyed by object type and the source and target node in
/// that type's DFG.
pub type OcdfgAnnotations = Annotations<OcdfgNodeRef, (String, DfgNodeRef, DfgNodeRef)>;

/// How an [`OCDirectlyFollowsGraph`] is turned into a [`Graph`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(JsonSchema))]
#[serde(default)]
pub struct OcdfgBuildOptions {
    /// Direction (top to bottom by default) and engine patch over the process-map preset.
    #[serde(flatten)]
    pub layout: LayoutSettings,
    /// Draw each object type's START / END terminals. On by default.
    pub terminals: bool,
    /// Object types to draw, in this order. Empty draws all of them, by name.
    pub object_types: Vec<String>,
    /// Labels, layout weights and line widths by node and edge, instead of the counts.
    pub annotations: OcdfgAnnotations,
}

impl Default for OcdfgBuildOptions {
    fn default() -> Self {
        OcdfgBuildOptions {
            layout: LayoutSettings::default(),
            terminals: true,
            object_types: Vec::new(),
            annotations: OcdfgAnnotations::default(),
        }
    }
}

impl OcdfgBuildOptions {
    /// The object types to draw, in group order, each once.
    pub fn types_of(&self, model: &OCDirectlyFollowsGraph<'_>) -> Vec<String> {
        if !self.object_types.is_empty() {
            let mut seen = std::collections::HashSet::new();
            return self
                .object_types
                .iter()
                .filter(|t| model.object_type_to_dfg.contains_key(*t) && seen.insert(*t))
                .cloned()
                .collect();
        }
        let mut types: Vec<String> = model.object_type_to_dfg.keys().cloned().collect();
        types.sort();
        types
    }
}

/// The layout request of an [`OCDirectlyFollowsGraph`].
pub type OcdfgLayoutRequest = ModelLayoutRequest<LayoutRequest, OcdfgNodeRef, OcdfgEdgeRef>;

/// Colors an object-centric directly-follows graph is drawn with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct OcdfgStyle {
    /// Colors and paddings shared with every other model.
    pub base: BaseStyle,
    /// How far towards white an object type's color is washed for the play or stop mark on its
    /// START and END terminals.
    pub fill_lightness: f64,
}

impl Default for OcdfgStyle {
    fn default() -> Self {
        OcdfgStyle {
            base: BaseStyle::default(),
            fill_lightness: 0.86,
        }
    }
}

fn type_color(object_type: &str) -> String {
    key_color("objectType", object_type)
}

impl Visualize for OCDirectlyFollowsGraph<'_> {
    type NodeRef = OcdfgNodeRef;
    type EdgeRef = OcdfgEdgeRef;
    type Options = OcdfgBuildOptions;
    type Style = OcdfgStyle;
    type Request = LayoutRequest;
    type Context = ();

    fn to_graph(
        &self,
        options: &OcdfgBuildOptions,
        measure: LabelWidth<'_>,
    ) -> (Graph, Vec<OcdfgNodeRef>, Vec<OcdfgEdgeRef>) {
        let mut acc: GraphAcc<OcdfgNodeRef, OcdfgEdgeRef> = GraphAcc::new();
        let annotations = &options.annotations;
        for (index, object_type) in options.types_of(self).iter().enumerate() {
            let Some(dfg) = self.object_type_to_dfg.get(object_type) else {
                continue;
            };
            let (start_key, end_key) =
                (format!("START:{object_type}"), format!("END:{object_type}"));
            add_dfg(
                &mut acc,
                dfg,
                options
                    .terminals
                    .then_some((start_key.as_str(), end_key.as_str())),
                Some(GroupId(index as u32)),
                |node| {
                    let node = match node {
                        DfgNodeRef::Start => OcdfgNodeRef::Start(object_type.clone()),
                        DfgNodeRef::End => OcdfgNodeRef::End(object_type.clone()),
                        DfgNodeRef::Activity(name) => OcdfgNodeRef::Activity(name),
                    };
                    let note = node_text(annotations.nodes.get(&node));
                    (node, note)
                },
                |edge| {
                    let edge = OcdfgEdgeRef {
                        object_type: object_type.clone(),
                        edge,
                    };
                    let annotation = annotations.edges.get(&edge.annotation_key());
                    (edge, annotation)
                },
                measure,
            );
        }
        acc.finish()
    }

    fn request(
        graph: Graph,
        options: &OcdfgBuildOptions,
    ) -> Result<LayoutRequest, VisualizationError> {
        options
            .layout
            .layered(graph, Preset::ProcessMap, None, Value::Null)
    }

    fn base(style: &OcdfgStyle) -> &BaseStyle {
        &style.base
    }

    fn context(
        &self,
        _layout: &Layout,
        _nodes: &[OcdfgNodeRef],
        _options: &OcdfgBuildOptions,
    ) -> Result<(), VisualizationError> {
        Ok(())
    }

    fn draw_node(
        &self,
        node: &OcdfgNodeRef,
        placement: &Placement,
        _context: &(),
        options: &OcdfgBuildOptions,
        style: &OcdfgStyle,
    ) -> SceneNode {
        let base = &style.base;
        let mut n = match node {
            OcdfgNodeRef::Activity(name) => {
                let mut n = scene_node(
                    placement,
                    Shape::Box,
                    Some(base.node_fill.clone()),
                    Some(Stroke::new(&base.node_border, base.border_width)),
                    base,
                );
                n.content
                    .push(node_label(name, placement.size.width, 2, base));
                n
            }
            OcdfgNodeRef::Start(t) | OcdfgNodeRef::End(t) => {
                let start = matches!(node, OcdfgNodeRef::Start(_));
                let color = type_color(t);
                let mut n = scene_node(
                    placement,
                    Shape::Box,
                    Some(color.clone()),
                    Some(Stroke::new(&color, base.border_width)),
                    base,
                );
                n.content.push(terminal_mark(
                    start,
                    placement.size.height / 2.0,
                    &lighten(&color, style.fill_lightness),
                ));
                n
            }
        };
        if let Some(note) = node_text(options.annotations.nodes.get(node)) {
            push_extra_line(&mut n, note, base);
        }
        n
    }

    fn draw_edge(
        &self,
        edge: &OcdfgEdgeRef,
        graph_edge: &Edge,
        route: &EdgeRoute,
        _context: &(),
        options: &OcdfgBuildOptions,
        style: &OcdfgStyle,
    ) -> Option<SceneEdge> {
        let mut e = scene_edge(route, &type_color(&edge.object_type), graph_edge.thickness)?;
        let annotation = options.annotations.edges.get(&edge.annotation_key());
        if let Some(text) = count_text(&edge.edge, annotation) {
            e.labels.push(edge_label(&text, route, &style.base));
        }
        Some(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::linked_ocel::IndexLinkedOCEL;
    use crate::test_utils::get_test_data_path;
    use crate::visualization::graph_build::heuristic_label_width;
    use crate::visualization::model::{self, Visualize};
    use crate::visualization::styling::key_color;
    use crate::visualization::StyleOverrides;
    use crate::visualization::{EdgeAnnotation, NodeAnnotation};
    use crate::{Importable, OCEL};

    /// The model borrows the linked OCEL, so the two have to live in one scope.
    fn with_model<R>(
        file: &str,
        f: impl FnOnce(&IndexLinkedOCEL, &OCDirectlyFollowsGraph<'_>) -> R,
    ) -> R {
        let ocel = OCEL::import_from_path(get_test_data_path().join("ocel").join(file)).unwrap();
        let locel = IndexLinkedOCEL::from(ocel);
        let model = OCDirectlyFollowsGraph::create_from_ocel(&locel);
        f(&locel, &model)
    }

    #[test]
    fn activities_are_shared_and_terminals_and_groups_are_per_type() {
        with_model("pm4py-ocel20_example.jsonocel", |_, model| {
            let options = OcdfgBuildOptions::default();
            let types = options.types_of(model);
            let (graph, nodes, edges) = model.to_graph(&options, &heuristic_label_width);
            let distinct: std::collections::BTreeSet<&String> = model
                .object_type_to_dfg
                .values()
                .flat_map(|d| d.activities.keys())
                .collect();
            let activities = nodes
                .iter()
                .filter(|n| matches!(n, OcdfgNodeRef::Activity(_)));
            assert_eq!(activities.count(), distinct.len());
            assert_eq!(nodes.len(), distinct.len() + 2 * types.len());
            for (edge, meta) in graph.edges.iter().zip(&edges) {
                let index = types.iter().position(|t| *t == meta.object_type).unwrap();
                assert_eq!(edge.group, Some(GroupId(index as u32)));
            }
        });
    }

    #[test]
    fn the_layout_request_names_each_terminal_by_its_object_type() {
        use crate::visualization::engine::layout_options_of;
        with_model("pm4py-ocel20_example.jsonocel", |_, model| {
            let options = OcdfgBuildOptions::default();
            let request = model::layout_request(model, &options).unwrap();
            assert_eq!(
                layout_options_of(&request.request).unwrap(),
                graphlay::LayoutOptions::process_map()
            );
            let json = serde_json::to_value(&request).unwrap();
            let first_type = &options.types_of(model)[0];
            assert_eq!(
                json["nodes"][0],
                serde_json::json!({"kind": "start", "name": first_type})
            );
            let back: OcdfgLayoutRequest = serde_json::from_value(json).unwrap();
            assert_eq!(back.nodes, request.nodes);
            assert_eq!(back.edges, request.edges);
        });
    }

    /// A type's color depends only on its name, so it stays the same when other types are left out.
    #[test]
    fn object_types_take_their_key_color() {
        with_model("pm4py-ocel20_example.jsonocel", |_, model| {
            let all = OcdfgBuildOptions::default();
            let one = OcdfgBuildOptions {
                object_types: vec![all.types_of(model)[1].clone()],
                ..OcdfgBuildOptions::default()
            };
            for options in [all, one] {
                let request = model::layout_request(model, &options).unwrap();
                let layout = crate::visualization::engine::layout_graph(&request.request).unwrap();
                let style = OcdfgStyle::default();
                let overrides = StyleOverrides::default();
                let scene =
                    model::scene_from_layout(model, &layout, &options, &style, &overrides).unwrap();
                for (node, meta) in scene.nodes.iter().zip(&request.nodes) {
                    if let OcdfgNodeRef::Start(t) | OcdfgNodeRef::End(t) = meta {
                        assert_eq!(node.fill, Some(key_color("objectType", t)), "{meta:?}");
                    }
                }
            }
        });
    }

    #[test]
    fn annotations_replace_the_counts_of_one_object_type() {
        use crate::visualization::graph_build::edge_label_size;
        with_model("pm4py-ocel20_example.jsonocel", |_, model| {
            let plain = OcdfgBuildOptions::default();
            let (plain_graph, nodes, edges) = model.to_graph(&plain, &heuristic_label_width);
            let start = edges
                .iter()
                .position(|e| matches!(e.edge, DfgEdgeRef::Start { .. }))
                .unwrap();
            let (object_type, from, to) = edges[start].annotation_key();
            let DfgNodeRef::Activity(activity) = &to else {
                unreachable!()
            };
            let mut options = OcdfgBuildOptions::default();
            options.annotations.edges.insert(
                (object_type.clone(), from, to.clone()),
                EdgeAnnotation {
                    label: Some("3 h".to_string()),
                    thickness: Some(8.0),
                    ..EdgeAnnotation::default()
                },
            );
            options.annotations.nodes.insert(
                OcdfgNodeRef::Activity(activity.clone()),
                NodeAnnotation {
                    label: Some("avg 2 h".to_string()),
                },
            );
            let json = serde_json::to_value(&options.annotations).unwrap();
            assert_eq!(json["edges"][0][0][0], serde_json::json!(object_type));
            let back: OcdfgAnnotations = serde_json::from_value(json).unwrap();
            assert_eq!(back, options.annotations);

            let request = model::layout_request(model, &options).unwrap();
            let graph = &request.request.graph;
            assert_eq!(graph.edges[start].label_size, Some(edge_label_size("3 h")));
            assert_eq!(graph.edges[start].thickness, 8.0);
            for i in (0..edges.len()).filter(|&i| i != start) {
                assert_eq!(graph.edges[i], plain_graph.edges[i], "{:?}", edges[i]);
            }

            let layout = crate::visualization::engine::layout_graph(&request.request).unwrap();
            let scene = model::scene_from_layout(
                model,
                &layout,
                &options,
                &OcdfgStyle::default(),
                &StyleOverrides::default(),
            )
            .unwrap();
            assert_eq!(scene.edges[start].labels[0].text.text, "3 h");
            assert_eq!(scene.edges[start].stroke.width, 8.0);
            let node = nodes
                .iter()
                .position(|n| *n == OcdfgNodeRef::Activity(activity.clone()))
                .unwrap();
            let last = scene.nodes[node].content.last();
            assert!(
                matches!(last, Some(graphlay::svg::NodeContent::Text(run)) if run.text == "avg 2 h")
            );
        });
    }

    #[test]
    fn writes_the_svg_export() {
        with_model("order-management.json", |_, model| {
            let types = ["orders", "items", "packages"];
            let options = OcdfgBuildOptions {
                object_types: types.map(String::from).to_vec(),
                ..OcdfgBuildOptions::default()
            };
            let svg = model::svg(model, &options, &OcdfgStyle::default()).unwrap();
            assert!(svg.contains("<polygon") && !svg.contains("<use"));
            crate::test_utils::write_export("order-management-OCDFG.svg", &svg);
        });
    }

    #[test]
    fn an_object_type_named_twice_is_drawn_once() {
        with_model("order-management.json", |_, model| {
            let options = OcdfgBuildOptions {
                object_types: ["orders", "items", "orders"].map(String::from).to_vec(),
                ..OcdfgBuildOptions::default()
            };
            assert_eq!(options.types_of(model), ["orders", "items"]);
        });
    }

    /// Each object type's edges are widened against that type's busiest edge, in the layout and in
    /// the drawing.
    #[test]
    fn edges_are_drawn_as_wide_as_the_layout_keeps_them() {
        with_model("order-management.json", |_, model| {
            let options = OcdfgBuildOptions::default();
            let request = model::layout_request(model, &options).unwrap();
            let layout = crate::visualization::engine::layout_graph(&request.request).unwrap();
            let style = OcdfgStyle::default();
            let overrides = StyleOverrides::default();
            let scene =
                model::scene_from_layout(model, &layout, &options, &style, &overrides).unwrap();
            let drawn: Vec<f64> = scene.edges.iter().map(|e| e.stroke.width).collect();
            let kept: Vec<f64> = request
                .request
                .graph
                .edges
                .iter()
                .map(|e| e.thickness)
                .collect();
            assert_eq!(drawn, kept);
            let widest = crate::visualization::graph_build::thickness_from_count(1.0, 1.0);
            for object_type in options.types_of(model) {
                let of_type = request.edges.iter().zip(&kept);
                let max = of_type
                    .filter(|(e, _)| e.object_type == object_type)
                    .map(|(_, &t)| t)
                    .reduce(f64::max);
                assert!(max.is_none_or(|m| m == widest), "{object_type}: {max:?}");
            }
        });
    }
}
