//! Laying out and drawing a [`PartialOrderTrace`], left to right.

use std::collections::HashMap;

use graphlay::{Direction, Edge, Graph, LayoutError, LayoutOptions, Node, Shape};
use serde::{Deserialize, Serialize};

use crate::core::event_data::case_centric::utils::partial_orders::{EventHash, PartialOrderTrace};
use crate::core::event_data::case_centric::EventLogClassifier;
use crate::visualization::graph_build::{
    heuristic_label_width, leaf_size_with, GraphAcc, LabelWidth,
};

/// How a [`PartialOrderTrace`] is turned into a [`Graph`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PartialOrderBuildOptions {
    /// Which way the order runs. `None` means left to right.
    pub direction: Option<Direction>,
}

impl PartialOrderBuildOptions {
    /// The layered-engine options for these build options.
    pub fn layout_options(&self) -> LayoutOptions {
        let mut options = LayoutOptions::layered_default();
        options.direction = self.direction.unwrap_or(Direction::LeftRight);
        options
    }
}

/// The [`Graph`] of `trace`, the event behind each graph node, and the relation behind each edge.
pub fn partial_order_to_graph(
    trace: &PartialOrderTrace,
    classifier: &EventLogClassifier,
    options: &PartialOrderBuildOptions,
) -> (Graph, Vec<EventHash>, Vec<(EventHash, EventHash)>) {
    partial_order_to_graph_with(trace, classifier, options, &heuristic_label_width)
}

/// [`partial_order_to_graph`] with a caller's own label measurement.
pub fn partial_order_to_graph_with(
    trace: &PartialOrderTrace,
    classifier: &EventLogClassifier,
    options: &PartialOrderBuildOptions,
    measure: LabelWidth<'_>,
) -> (Graph, Vec<EventHash>, Vec<(EventHash, EventHash)>) {
    let _ = options;
    let mut events: Vec<(String, EventHash)> = trace
        .event_map
        .iter()
        .map(|(hash, event)| (classifier.get_class_identity(event), *hash))
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

    let mut relations: Vec<_> = trace
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

/// Lay `trace` out.
///
/// # Errors
///
/// [`LayoutError`] from the engine.
pub fn layout_partial_order(
    trace: &PartialOrderTrace,
    classifier: &EventLogClassifier,
    options: &PartialOrderBuildOptions,
) -> Result<graphlay::Layout, LayoutError> {
    let (graph, _, _) = partial_order_to_graph(trace, classifier, options);
    graphlay::layout(&graph, &options.layout_options())
}

mod draw {
    use super::*;
    use graphlay::svg::{Scene, Stroke};
    use graphlay::Layout;

    use crate::visualization::process_tree::leaf_runs;
    use crate::visualization::styling::{render_scene, scene, scene_edge, scene_node, BaseStyle};

    /// The drawing of `trace` over an existing [`Layout`].
    pub fn partial_order_scene(
        trace: &PartialOrderTrace,
        classifier: &EventLogClassifier,
        layout: &Layout,
        nodes: &[EventHash],
        style: &BaseStyle,
    ) -> Scene {
        let mut scene_nodes = Vec::with_capacity(nodes.len());
        for (placement, hash) in layout.nodes.iter().zip(nodes) {
            let mut n = scene_node(
                placement,
                Shape::Box,
                Some(style.node_fill.clone()),
                Some(Stroke::new(&style.node_border, style.border_width)),
                style,
            );
            if let Some(event) = trace.event_map.get(hash) {
                let label = classifier.get_class_identity(event);
                n.content.extend(leaf_runs(&label, style, &style.text));
            }
            scene_nodes.push(n);
        }
        let scene_edges = layout
            .edges
            .iter()
            .filter_map(|route| scene_edge(route, &style.edge, 2.0))
            .collect();
        scene(style, scene_nodes, scene_edges)
    }

    /// Lay `trace` out and draw it, as one standalone SVG document.
    ///
    /// # Errors
    ///
    /// As [`layout_partial_order`].
    pub fn export_partial_order_svg(
        trace: &PartialOrderTrace,
        classifier: &EventLogClassifier,
        options: &PartialOrderBuildOptions,
    ) -> Result<String, LayoutError> {
        let (graph, nodes, _) = partial_order_to_graph(trace, classifier, options);
        let layout = graphlay::layout(&graph, &options.layout_options())?;
        Ok(render_scene(&partial_order_scene(
            trace,
            classifier,
            &layout,
            &nodes,
            &BaseStyle::default(),
        )))
    }
}

pub use draw::{export_partial_order_svg, partial_order_scene};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::case_centric::Event;

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
        let (trace, classifier) = (trace(), EventLogClassifier::default());
        let options = PartialOrderBuildOptions::default();
        let (graph, nodes, edges) = partial_order_to_graph(&trace, &classifier, &options);
        let layout = layout_partial_order(&trace, &classifier, &options).unwrap();
        assert_eq!((graph.nodes.len(), graph.edges.len()), (6, 7));
        for (edge, (from, to)) in graph.edges.iter().zip(&edges) {
            let (a, b) = (edge.source.index(), edge.target.index());
            assert_eq!((nodes[a], nodes[b]), (*from, *to));
            assert!(layout.nodes[a].center.x < layout.nodes[b].center.x);
        }
    }

    #[test]
    fn writes_the_svg_export() {
        let options = PartialOrderBuildOptions::default();
        let svg =
            export_partial_order_svg(&trace(), &EventLogClassifier::default(), &options).unwrap();
        assert!(svg.contains("Breakfast"));
        crate::test_utils::write_export("partial-order-trace.svg", &svg);
    }
}
