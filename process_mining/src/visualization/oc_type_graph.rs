//! Laying out and drawing an OCEL type graph: the event types and object types of a log, joined by
//! their E2O and O2O relations. Such a graph is dense, with a few object types as hubs, so it is
//! laid out with graphlay's hub-and-spoke flow preset.

use graphlay::json::LayoutRequest;
use graphlay::layout::{EdgeRoute, Placement};
use graphlay::svg::{
    NodeContent, Paint, SceneEdge, SceneNode, Shape as SShape, Stroke, TextRun, TextWrap,
};
use graphlay::{Direction, Edge, Graph, Layout, Node, Preset, RoutingStyle, Shape, Size};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use crate::core::event_data::object_centric::type_graph::{
    OcTypeGraph, OcTypeGraphEdge, OcTypeGraphNode, TypeKind,
};
use crate::visualization::engine::{json_of, VisualizationError};
use crate::visualization::graph_build::{
    dfg_width_with, edge_label_size_with, GraphAcc, LabelWidth,
};
use crate::visualization::model::{LayoutSettings, ModelLayoutRequest, Visualize};
use crate::visualization::styling::{
    darken, edge_label, key_color, lighten, scene_edge, scene_node, BaseStyle,
};

/// Height of a type box.
pub const TYPE_NODE_HEIGHT: f64 = 44.0;

/// How a [`OcTypeGraph`] is turned into a [`Graph`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(JsonSchema))]
#[serde(default)]
pub struct OcTypeGraphBuildOptions {
    /// Direction (top to bottom by default) and engine patch over the hub-and-spoke flow preset
    /// with diagonal routing, which crosses the many long edges of a dense graph more readably.
    #[serde(flatten)]
    pub layout: LayoutSettings,
}

/// The layout request of a [`OcTypeGraph`], with the index of the type or relation in
/// [`OcTypeGraph::nodes`] or [`OcTypeGraph::edges`] behind each graph node and edge.
pub type OcTypeGraphLayoutRequest = ModelLayoutRequest<LayoutRequest, usize, usize>;

/// Colors and sizes a type graph is drawn with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct OcTypeGraphStyle {
    /// Colors and paddings shared with every other model.
    pub base: BaseStyle,
    /// How far towards white a type's color is washed for its fill.
    pub fill_lightness: f64,
    /// Width of a relation's line.
    pub edge_width: f64,
    /// Dash pattern of an O2O relation.
    pub o2o_dash: String,
}

impl Default for OcTypeGraphStyle {
    fn default() -> Self {
        OcTypeGraphStyle {
            base: BaseStyle::default(),
            fill_lightness: 0.85,
            edge_width: 1.6,
            o2o_dash: "6 3".to_string(),
        }
    }
}

/// `n` with a comma between each group of three digits.
fn grouped(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// The type's [`key_color`], so it matches the other drawings of the same log.
fn type_color(node: &OcTypeGraphNode) -> String {
    match node.kind {
        TypeKind::Event => key_color("activity", &node.label),
        TypeKind::Object => key_color("objectType", &node.label),
    }
}

/// Edges naming a node that does not exist are left out.
impl Visualize for OcTypeGraph {
    type NodeRef = usize;
    type EdgeRef = usize;
    type Options = OcTypeGraphBuildOptions;
    type Style = OcTypeGraphStyle;
    type Request = LayoutRequest;
    type Context = ();

    fn to_graph(
        &self,
        _options: &OcTypeGraphBuildOptions,
        measure: LabelWidth<'_>,
    ) -> (Graph, Vec<usize>, Vec<usize>) {
        let mut acc: GraphAcc<usize, usize> = GraphAcc::new();
        let ids: Vec<graphlay::NodeId> = self
            .nodes
            .iter()
            .enumerate()
            .map(|(i, node)| {
                let size = Size::new(dfg_width_with(&node.label, measure), TYPE_NODE_HEIGHT);
                let node = Node {
                    size,
                    shape: Shape::Box,
                    ..Node::default()
                };
                acc.add_node(i.to_string(), node, || i)
            })
            .collect();
        for (i, edge) in self.edges.iter().enumerate() {
            let (Some(&source), Some(&target)) = (ids.get(edge.source), ids.get(edge.target))
            else {
                continue;
            };
            // A self-loop's label goes on the loop's apex, which needs no room reserved.
            let label_size = edge
                .qualifier
                .as_deref()
                .filter(|_| source != target)
                .map(|q| edge_label_size_with(q, measure));
            acc.add_edge(
                Edge {
                    label_size,
                    ..Edge::new(source, target)
                },
                i,
            );
        }
        acc.finish()
    }

    fn request(
        graph: Graph,
        options: &OcTypeGraphBuildOptions,
    ) -> Result<LayoutRequest, VisualizationError> {
        options.layout.layered(
            graph,
            Preset::HubAndSpokeFlow,
            Some(Direction::TopBottom),
            serde_json::json!({"routing": {"style": json_of(RoutingStyle::Diagonal)}}),
        )
    }

    fn base(style: &OcTypeGraphStyle) -> &BaseStyle {
        &style.base
    }

    fn context(
        &self,
        _layout: &Layout,
        _nodes: &[usize],
        _options: &OcTypeGraphBuildOptions,
    ) -> Result<(), VisualizationError> {
        Ok(())
    }

    fn draw_node(
        &self,
        &index: &usize,
        placement: &Placement,
        _context: &(),
        _options: &OcTypeGraphBuildOptions,
        style: &OcTypeGraphStyle,
    ) -> SceneNode {
        let base = &style.base;
        let node = &self.nodes[index];
        let color = type_color(node);
        let mut n = scene_node(
            placement,
            Shape::Box,
            Some(lighten(&color, style.fill_lightness)),
            Some(Stroke {
                paint: Paint::Color(color.clone()),
                width: 2.0,
                opacity: Some(0.8),
                ..Stroke::default()
            }),
            base,
        );
        n.shape = SShape::Rect {
            corner_radius: match node.kind {
                TypeKind::Event => 6.0,
                TypeKind::Object => placement.size.height / 2.0,
            },
        };
        let text = darken(&color, 0.25);
        n.content.push(NodeContent::Text(TextRun {
            text: node.label.clone(),
            dy: if node.count.is_some() { -6.5 } else { 0.0 },
            size: Some(base.label_size),
            weight: Some(600.0),
            color: Some(text.clone()),
            wrap: Some(TextWrap {
                max_width: placement.size.width - 20.0,
                max_lines: 1,
                line_height: base.label_size + 1.5,
            }),
            ..TextRun::default()
        }));
        if let Some(count) = node.count {
            n.content.push(NodeContent::Text(TextRun {
                text: grouped(count),
                dy: 9.0,
                size: Some(9.5),
                weight: Some(700.0),
                color: Some(text),
                ..TextRun::default()
            }));
        }
        n
    }

    fn draw_edge(
        &self,
        &index: &usize,
        _graph_edge: &Edge,
        route: &EdgeRoute,
        _context: &(),
        _options: &OcTypeGraphBuildOptions,
        style: &OcTypeGraphStyle,
    ) -> Option<SceneEdge> {
        let base = &style.base;
        let edge = &self.edges[index];
        let mut e = scene_edge(route, &base.edge, style.edge_width)?;
        let from_object = self
            .nodes
            .get(edge.source)
            .is_some_and(|n| n.kind == TypeKind::Object);
        if from_object && !style.o2o_dash.is_empty() {
            e.stroke.dash = Some(style.o2o_dash.clone());
        }
        if let Some(qualifier) = &edge.qualifier {
            e.labels.push(edge_label(qualifier, route, base));
        }
        Some(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;
    use crate::test_utils::{assert_xml_well_formed, get_test_data_path};
    use crate::visualization::graph_build::heuristic_label_width;
    use crate::visualization::model::{self, Visualize};
    use crate::visualization::styling::ElementStyle;
    use crate::visualization::StyleOverrides;
    use crate::{Importable, OCEL};

    fn order_management() -> OcTypeGraph {
        let path = get_test_data_path()
            .join("ocel")
            .join("order-management.json");
        let locel = SlimLinkedOCEL::from_ocel(OCEL::import_from_path(path).unwrap());
        OcTypeGraph::from_linked_ocel(&locel)
    }

    #[test]
    fn the_layout_request_carries_the_layout_options() {
        use crate::visualization::engine::layout_options_of;
        let graph = order_management();
        for direction in [None, Some(Direction::LeftRight)] {
            let options = OcTypeGraphBuildOptions {
                layout: LayoutSettings {
                    direction,
                    ..LayoutSettings::default()
                },
            };
            let request = model::layout_request(&graph, &options).unwrap();
            let resolved = layout_options_of(&request.request).unwrap();
            let mut expected = graphlay::LayoutOptions::hub_and_spoke_flow();
            expected.direction = direction.unwrap_or(Direction::TopBottom);
            expected.routing.style = RoutingStyle::Diagonal;
            assert_eq!(resolved, expected);
        }
    }

    #[test]
    fn every_type_and_qualified_relation_is_a_node_and_an_edge() {
        let graph = order_management();
        let kinds = |kind| graph.nodes.iter().filter(|n| n.kind == kind).count();
        assert!(kinds(TypeKind::Event) > 0 && kinds(TypeKind::Object) > 0);
        assert!(graph.nodes.iter().all(|n| n.count.is_some_and(|c| c > 0)));
        for edge in &graph.edges {
            assert!(edge.qualifier.is_some());
            if graph.nodes[edge.source].kind == TypeKind::Event {
                assert_eq!(graph.nodes[edge.target].kind, TypeKind::Object);
            }
        }
        let (g, nodes, edges) =
            graph.to_graph(&OcTypeGraphBuildOptions::default(), &heuristic_label_width);
        assert_eq!(nodes, (0..graph.nodes.len()).collect::<Vec<_>>());
        assert_eq!(edges, (0..graph.edges.len()).collect::<Vec<_>>());
        assert_eq!(g.edges.len(), graph.edges.len());
    }

    #[test]
    fn edges_to_missing_types_are_left_out() {
        let node = |kind, label: &str| OcTypeGraphNode {
            kind,
            label: label.to_string(),
            count: None,
        };
        let graph = OcTypeGraph {
            nodes: vec![
                node(TypeKind::Event, "pay"),
                node(TypeKind::Object, "orders"),
            ],
            edges: vec![
                OcTypeGraphEdge {
                    source: 0,
                    target: 7,
                    qualifier: None,
                },
                OcTypeGraphEdge {
                    source: 0,
                    target: 1,
                    qualifier: None,
                },
            ],
        };
        let request = model::layout_request(&graph, &Default::default()).unwrap();
        assert_eq!(request.edges, [1]);
        assert_eq!(request.request.preset, Preset::HubAndSpokeFlow);
    }

    #[test]
    fn writes_the_svg_export_with_highlights() {
        let graph = order_management();
        let options = OcTypeGraphBuildOptions::default();
        let request = model::layout_request(&graph, &options).unwrap();
        let layout = crate::visualization::engine::layout_graph(&request.request).unwrap();
        let mut overrides = StyleOverrides::default();
        overrides.nodes.insert(
            0,
            ElementStyle {
                stroke: Some("#dc2626".to_string()),
                stroke_width: Some(4.0),
                ..ElementStyle::default()
            },
        );
        overrides.nodes.insert(
            1,
            ElementStyle {
                opacity: Some(0.35),
                ..ElementStyle::default()
            },
        );
        overrides.edges.insert(
            0,
            ElementStyle {
                dash: Some("1 4".to_string()),
                ..ElementStyle::default()
            },
        );
        let scene = model::scene_from_layout(
            &graph,
            &layout,
            &options,
            &OcTypeGraphStyle::default(),
            &overrides,
        )
        .unwrap();
        let svg = render_scene_svg(&scene);
        assert_xml_well_formed(&svg);
        assert!(svg.contains("opacity=\"0.35\""));
        assert!(
            svg.contains("stroke-dasharray=\"6 3\""),
            "O2O relations are dashed"
        );
        assert!(svg.contains("stroke-dasharray=\"1 4\""));
        crate::test_utils::write_export("order-management-type-graph.svg", &svg);

        let plain = model::svg(&graph, &options, &OcTypeGraphStyle::default()).unwrap();
        assert!(!plain.contains("opacity=\"0.35\""));
    }

    fn render_scene_svg(scene: &graphlay::svg::Scene) -> String {
        crate::visualization::engine::render_scene_svg(scene, None)
    }

    #[test]
    fn counts_are_grouped_by_thousands() {
        assert_eq!(grouped(7), "7");
        assert_eq!(grouped(1234), "1,234");
        assert_eq!(grouped(1_234_567), "1,234,567");
    }
}
