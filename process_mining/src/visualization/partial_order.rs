//! Laying out and drawing a [`PartialOrderTrace`], left to right.

use std::collections::HashMap;

use graphlay::json::LayoutRequest;
use graphlay::layout::{EdgeRoute, Placement};
use graphlay::svg::{SceneEdge, SceneNode, Stroke};
use graphlay::{Direction, Edge, Graph, Layout, Node, Preset, Shape};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::core::event_data::case_centric::utils::partial_orders::{EventHash, PartialOrderTrace};
use crate::core::event_data::case_centric::EventLogClassifier;
use crate::visualization::engine::VisualizationError;
use crate::visualization::graph_build::{leaf_size_with, GraphAcc, LabelWidth};
use crate::visualization::model::{LayoutSettings, ModelLayoutRequest, Visualize};
use crate::visualization::styling::{leaf_runs, scene_edge, scene_node, BaseStyle};

/// How a [`PartialOrderTrace`] is turned into a [`Graph`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(JsonSchema))]
#[serde(default)]
pub struct PartialOrderBuildOptions {
    /// Direction (left to right by default) and engine patch over the layered preset.
    #[serde(flatten)]
    pub layout: LayoutSettings,
    /// What an event is labeled with.
    pub classifier: EventLogClassifier,
}

/// The layout request of a [`PartialOrderTrace`].
pub type PartialOrderLayoutRequest =
    ModelLayoutRequest<LayoutRequest, EventHash, (EventHash, EventHash)>;

/// Colors a partial order trace is drawn with.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct PartialOrderStyle {
    /// Colors and paddings shared with every other model.
    pub base: BaseStyle,
}

const EDGE_WIDTH: f64 = 2.0;

impl Visualize for PartialOrderTrace {
    type NodeRef = EventHash;
    type EdgeRef = (EventHash, EventHash);
    type Options = PartialOrderBuildOptions;
    type Style = PartialOrderStyle;
    type Request = LayoutRequest;
    type Context = ();

    fn to_graph(
        &self,
        options: &PartialOrderBuildOptions,
        measure: LabelWidth<'_>,
    ) -> (Graph, Vec<EventHash>, Vec<(EventHash, EventHash)>) {
        let mut events: Vec<(String, EventHash)> = self
            .event_map
            .iter()
            .map(|(hash, event)| (options.classifier.get_class_identity(event), *hash))
            .collect();
        events.sort();

        let mut acc: GraphAcc<EventHash, (EventHash, EventHash)> = GraphAcc::new();
        let mut ids = HashMap::new();
        for (index, (label, hash)) in events.iter().enumerate() {
            let node = Node {
                size: leaf_size_with(label, measure),
                shape: Shape::Box,
                ..Node::default()
            };
            ids.insert(*hash, acc.add_node(index.to_string(), node, || *hash));
        }

        let mut relations: Vec<_> = self
            .partial_relations
            .iter()
            .filter_map(|(from, to)| Some((*ids.get(from)?, *ids.get(to)?, (*from, *to))))
            .collect();
        relations.sort_by_key(|(from, to, _)| (from.index(), to.index()));
        for (from, to, relation) in relations {
            acc.add_edge(Edge::new(from, to), relation);
        }
        acc.finish()
    }

    fn request(
        graph: Graph,
        options: &PartialOrderBuildOptions,
    ) -> Result<LayoutRequest, VisualizationError> {
        options.layout.layered(
            graph,
            Preset::Layered,
            Some(Direction::LeftRight),
            Value::Null,
        )
    }

    fn base(style: &PartialOrderStyle) -> &BaseStyle {
        &style.base
    }

    fn context(
        &self,
        _layout: &Layout,
        _nodes: &[EventHash],
        _options: &PartialOrderBuildOptions,
    ) -> Result<(), VisualizationError> {
        Ok(())
    }

    fn draw_node(
        &self,
        hash: &EventHash,
        placement: &Placement,
        _context: &(),
        options: &PartialOrderBuildOptions,
        style: &PartialOrderStyle,
    ) -> SceneNode {
        let base = &style.base;
        let mut n = scene_node(
            placement,
            Shape::Box,
            Some(base.node_fill.clone()),
            Some(Stroke::new(&base.node_border, base.border_width)),
            base,
        );
        if let Some(event) = self.event_map.get(hash) {
            let label = options.classifier.get_class_identity(event);
            n.content.extend(leaf_runs(&label, base, &base.text));
        }
        n
    }

    fn draw_edge(
        &self,
        _edge: &(EventHash, EventHash),
        _graph_edge: &Edge,
        route: &EdgeRoute,
        _context: &(),
        _options: &PartialOrderBuildOptions,
        style: &PartialOrderStyle,
    ) -> Option<SceneEdge> {
        scene_edge(route, &style.base.edge, EDGE_WIDTH)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::case_centric::Event;
    use crate::visualization::graph_build::heuristic_label_width;
    use crate::visualization::model::{self, Visualize};

    /// Wake up, then coffee, teeth and a podcast in any order, then breakfast after coffee and
    /// teeth, then work after breakfast and the podcast.
    fn trace() -> PartialOrderTrace {
        let ev = |name: &str| Event::new(name.to_string());
        let (wake, coffee, teeth, podcast, breakfast, work) = (
            ev("Wake up"),
            ev("Make Coffee"),
            ev("Brush teeth"),
            ev("Listen to Podcast"),
            ev("Breakfast"),
            ev("Work"),
        );
        let mut trace = PartialOrderTrace::new();
        for e in [&wake, &coffee, &teeth, &podcast, &breakfast, &work] {
            trace.add_event(e);
        }
        for (from, to) in [
            (&wake, &coffee),
            (&wake, &teeth),
            (&wake, &podcast),
            (&coffee, &breakfast),
            (&teeth, &breakfast),
            (&breakfast, &work),
            (&podcast, &work),
        ] {
            trace.add_partial_relation(from, to);
        }
        trace
    }

    #[test]
    fn every_relation_runs_left_to_right() {
        let trace = trace();
        let options = PartialOrderBuildOptions::default();
        let (graph, nodes, edges) = Visualize::to_graph(&trace, &options, &heuristic_label_width);
        let layout = model::layout(&trace, &options).unwrap();
        assert_eq!((graph.nodes.len(), graph.edges.len()), (6, 7));
        for (edge, (from, to)) in graph.edges.iter().zip(&edges) {
            let (a, b) = (edge.source.index(), edge.target.index());
            assert_eq!((nodes[a], nodes[b]), (*from, *to));
            assert!(layout.nodes[a].center.x < layout.nodes[b].center.x);
        }
    }

    #[test]
    fn the_layout_request_resolves_to_the_layout_options() {
        use crate::visualization::engine::layout_options_of;
        let trace = trace();
        let options = PartialOrderBuildOptions::default();
        let request = model::layout_request(&trace, &options).unwrap();
        let mut expected = graphlay::LayoutOptions::layered_default();
        expected.direction = Direction::LeftRight;
        assert_eq!(layout_options_of(&request.request).unwrap(), expected);
        assert_eq!(request.nodes.len(), 6);
        let json = serde_json::to_string(&request).unwrap();
        let back: PartialOrderLayoutRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back, request);
    }

    #[test]
    fn writes_the_svg_export() {
        let options = PartialOrderBuildOptions::default();
        let style = PartialOrderStyle::default();
        let svg = model::svg(&trace(), &options, &style).unwrap();
        assert!(svg.contains("Breakfast"));
        crate::test_utils::write_export("partial-order-trace.svg", &svg);
    }
}
