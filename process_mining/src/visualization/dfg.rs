//! Laying out and drawing a [`DirectlyFollowsGraph`], with START and END terminals.

use std::collections::{BTreeMap, HashMap};

use graphlay::json::LayoutRequest;
use graphlay::layout::{EdgeRoute, Placement};
use graphlay::svg::{SceneEdge, SceneNode, Stroke};
use graphlay::{Edge, Graph, GroupId, Layout, Node, Preset, RankConstraint, Shape};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::core::process_models::dfg::DirectlyFollowsGraph;
use crate::visualization::annotation::{
    edge_text, edge_thickness, edge_weight, node_text, EdgeAnnotation,
};
use crate::visualization::engine::VisualizationError;
use crate::visualization::graph_build::{
    dfg_width_with, edge_label_size_with, thickness_from_count, weight_from_count, GraphAcc,
    LabelWidth, TERMINAL_SIZE,
};
use crate::visualization::model::{Annotations, LayoutSettings, ModelLayoutRequest, Visualize};
use crate::visualization::styling::{
    edge_label, node_label, push_extra_line, scene_edge, scene_node, terminal_mark, BaseStyle,
};

/// Height of a DFG activity box.
pub const ACTIVITY_HEIGHT: f64 = 52.0;

/// A node of a [`DirectlyFollowsGraph`] drawing, e.g., `{"kind": "activity", "name": "pay"}`.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "kind", content = "name", rename_all = "snake_case")]
pub enum DfgNodeRef {
    /// The artificial START terminal, pinned to the first layer.
    Start,
    /// The artificial END terminal, pinned to the last layer.
    End,
    /// An activity, by name.
    Activity(String),
}

/// An edge of a [`DirectlyFollowsGraph`] drawing, with its count.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DfgEdgeRef {
    /// A directly-follows relation.
    Follows {
        /// Source activity.
        from: String,
        /// Target activity, equal to `from` for a self-loop.
        to: String,
        /// How often the relation was observed.
        count: u32,
    },
    /// START to an activity that begins a trace.
    Start {
        /// The activity.
        activity: String,
        /// How many traces begin with it.
        count: u32,
    },
    /// An activity that ends a trace, to END.
    End {
        /// The activity.
        activity: String,
        /// How many traces end with it.
        count: u32,
    },
}

impl DfgEdgeRef {
    /// The frequency this edge is weighted and labeled by.
    pub fn count(&self) -> u32 {
        match self {
            DfgEdgeRef::Follows { count, .. }
            | DfgEdgeRef::Start { count, .. }
            | DfgEdgeRef::End { count, .. } => *count,
        }
    }

    /// The source and target node, which key the edge's annotation.
    pub(crate) fn endpoints(&self) -> (DfgNodeRef, DfgNodeRef) {
        match self {
            DfgEdgeRef::Follows { from, to, .. } => (
                DfgNodeRef::Activity(from.clone()),
                DfgNodeRef::Activity(to.clone()),
            ),
            DfgEdgeRef::Start { activity, .. } => {
                (DfgNodeRef::Start, DfgNodeRef::Activity(activity.clone()))
            }
            DfgEdgeRef::End { activity, .. } => {
                (DfgNodeRef::Activity(activity.clone()), DfgNodeRef::End)
            }
        }
    }
}

/// Labels, layout weights and line widths of a [`DirectlyFollowsGraph`] drawing that replace the
/// counts, e.g., durations. Edges are keyed by source and target node, so a START edge starts at
/// `{"kind": "start"}`.
pub type DfgAnnotations = Annotations<DfgNodeRef, (DfgNodeRef, DfgNodeRef)>;

/// How a [`DirectlyFollowsGraph`] is turned into a [`Graph`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(JsonSchema))]
#[serde(default)]
pub struct DfgBuildOptions {
    /// Direction (top to bottom by default) and engine patch over the process-map preset.
    #[serde(flatten)]
    pub layout: LayoutSettings,
    /// Draw the artificial START / END terminals. On by default.
    pub terminals: bool,
    /// Labels, layout weights and line widths by node and edge, instead of the counts.
    pub annotations: DfgAnnotations,
}

impl Default for DfgBuildOptions {
    fn default() -> Self {
        DfgBuildOptions {
            layout: LayoutSettings::default(),
            terminals: true,
            annotations: DfgAnnotations::default(),
        }
    }
}

/// The layout request of a [`DirectlyFollowsGraph`].
pub type DfgLayoutRequest = ModelLayoutRequest<LayoutRequest, DfgNodeRef, DfgEdgeRef>;

/// Colors a directly-follows graph is drawn with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct DfgStyle {
    /// Colors and paddings shared with every other model.
    pub base: BaseStyle,
    /// Fill of the START terminal.
    pub start_fill: String,
    /// Fill of the END terminal.
    pub end_fill: String,
}

impl Default for DfgStyle {
    fn default() -> Self {
        DfgStyle {
            base: BaseStyle::default(),
            start_fill: "#4B9969".to_string(),
            end_fill: "#D4001F".to_string(),
        }
    }
}

impl Visualize for DirectlyFollowsGraph<'_> {
    type NodeRef = DfgNodeRef;
    type EdgeRef = DfgEdgeRef;
    type Options = DfgBuildOptions;
    type Style = DfgStyle;
    type Request = LayoutRequest;
    type Context = ();

    fn to_graph(
        &self,
        options: &DfgBuildOptions,
        measure: LabelWidth<'_>,
    ) -> (Graph, Vec<DfgNodeRef>, Vec<DfgEdgeRef>) {
        let mut acc: GraphAcc<DfgNodeRef, DfgEdgeRef> = GraphAcc::new();
        let annotations = &options.annotations;
        add_dfg(
            &mut acc,
            self,
            options.terminals.then_some((START_KEY, END_KEY)),
            None,
            |node| {
                let note = node_text(annotations.nodes.get(&node));
                (node, note)
            },
            |edge| {
                let annotation = annotations.edges.get(&edge.endpoints());
                (edge, annotation)
            },
            measure,
        );
        acc.finish()
    }

    fn request(
        graph: Graph,
        options: &DfgBuildOptions,
    ) -> Result<LayoutRequest, VisualizationError> {
        options
            .layout
            .layered(graph, Preset::ProcessMap, None, Value::Null)
    }

    fn base(style: &DfgStyle) -> &BaseStyle {
        &style.base
    }

    fn context(
        &self,
        _layout: &Layout,
        _nodes: &[DfgNodeRef],
        _options: &DfgBuildOptions,
    ) -> Result<(), VisualizationError> {
        Ok(())
    }

    fn draw_node(
        &self,
        node: &DfgNodeRef,
        placement: &Placement,
        _context: &(),
        options: &DfgBuildOptions,
        style: &DfgStyle,
    ) -> SceneNode {
        let base = &style.base;
        let mut n = scene_node(
            placement,
            Shape::Box,
            Some(base.node_fill.clone()),
            Some(Stroke::new(&base.node_border, base.border_width)),
            base,
        );
        match node {
            DfgNodeRef::Start | DfgNodeRef::End => {
                let start = matches!(node, DfgNodeRef::Start);
                n.fill = Some(if start {
                    style.start_fill.clone()
                } else {
                    style.end_fill.clone()
                });
                n.content
                    .push(terminal_mark(start, placement.size.height / 2.0, "#ffffff"));
            }
            DfgNodeRef::Activity(name) => {
                n.content
                    .push(node_label(name, placement.size.width, 2, base));
            }
        }
        if let Some(note) = node_text(options.annotations.nodes.get(node)) {
            push_extra_line(&mut n, note, base);
        }
        n
    }

    fn draw_edge(
        &self,
        edge: &DfgEdgeRef,
        graph_edge: &Edge,
        route: &EdgeRoute,
        _context: &(),
        options: &DfgBuildOptions,
        style: &DfgStyle,
    ) -> Option<SceneEdge> {
        let base = &style.base;
        let mut e = scene_edge(route, &base.edge, graph_edge.thickness)?;
        let annotation = options.annotations.edges.get(&edge.endpoints());
        if let Some(text) = count_text(edge, annotation) {
            e.labels.push(edge_label(&text, route, base));
        }
        Some(e)
    }
}

const START_KEY: &str = "START";
const END_KEY: &str = "END";

/// By name only, so an OC-DFG's per-type sub-graphs share one node per activity.
fn activity_key(activity: &str) -> String {
    format!("act:{activity}")
}

/// Adds the activities and relations of `dfg` to `acc`, and with `terminals` (the keys of its START
/// and END node) also those nodes and their edges. An activity already in `acc` is shared.
///
/// `node` and `edge` give the meta of a node or edge, with the node's annotated line or the edge's
/// annotation. Edges are widened by their count against the largest count among the edges added
/// here.
pub(crate) fn add_dfg<'a, N, E>(
    acc: &mut GraphAcc<N, E>,
    dfg: &DirectlyFollowsGraph<'_>,
    terminals: Option<(&str, &str)>,
    group: Option<GroupId>,
    node: impl Fn(DfgNodeRef) -> (N, Option<&'a str>),
    edge: impl Fn(DfgEdgeRef) -> (E, Option<&'a EdgeAnnotation>),
    measure: LabelWidth<'_>,
) {
    if let Some((start_key, end_key)) = terminals {
        for (key, meta, rank) in [
            (start_key, DfgNodeRef::Start, RankConstraint::First),
            (end_key, DfgNodeRef::End, RankConstraint::Last),
        ] {
            let terminal = Node {
                size: TERMINAL_SIZE,
                shape: Shape::Box,
                rank,
                group,
                ..Node::default()
            };
            acc.add_node(key, terminal, || node(meta).0);
        }
    }

    let mut activities: Vec<&String> = dfg.activities.keys().collect();
    activities.sort();
    for name in activities {
        let (meta, note) = node(DfgNodeRef::Activity(name.clone()));
        let activity = Node {
            size: graphlay::Size::new(activity_width(name, note, measure), ACTIVITY_HEIGHT),
            shape: Shape::Box,
            group,
            ..Node::default()
        };
        acc.add_node(activity_key(name), activity, || meta);
    }

    let key = |activity: &str| acc.node_id(&activity_key(activity));
    let mut edges = Vec::new();
    for (from, to, count) in sorted_relations(dfg) {
        let ends = (key(&from), key(&to));
        edges.push((ends, DfgEdgeRef::Follows { from, to, count }));
    }
    if let Some((start_key, end_key)) = terminals {
        let (start, end) = (acc.node_id(start_key), acc.node_id(end_key));
        for (activity, count) in boundary_counts(&dfg.start_activities, &dfg.activities) {
            let ends = (start, key(&activity));
            edges.push((ends, DfgEdgeRef::Start { activity, count }));
        }
        for (activity, count) in boundary_counts(&dfg.end_activities, &dfg.activities) {
            let ends = (key(&activity), end);
            edges.push((ends, DfgEdgeRef::End { activity, count }));
        }
    }
    let edges: Vec<_> = edges
        .into_iter()
        .filter_map(|((source, target), meta)| Some((source?, target?, meta)))
        .collect();
    let max_count = edges.iter().map(|(_, _, e)| e.count()).max().unwrap_or(1) as f64;
    for (source, target, meta) in edges {
        let (edge_meta, annotation) = edge(meta.clone());
        let e = count_edge(source, target, &meta, max_count, group, annotation, measure);
        acc.add_edge(e, edge_meta);
    }
}

/// An activity box as wide as its name or its annotated line needs.
fn activity_width(name: &str, note: Option<&str>, measure: LabelWidth<'_>) -> f64 {
    let width = dfg_width_with(name, measure);
    note.map_or(width, |note| width.max(dfg_width_with(note, measure)))
}

/// The label an edge shows: its annotation's, or its count.
pub(crate) fn count_text(meta: &DfgEdgeRef, annotation: Option<&EdgeAnnotation>) -> Option<String> {
    edge_text(annotation, || Some(meta.count().to_string()))
}

/// An edge weighted and widened by the count of `meta` against `max_count`, with room for its
/// [`count_text`], unless `annotation` sets them. A self-loop's label sits at the loop's apex and
/// reserves no room.
fn count_edge(
    source: graphlay::NodeId,
    target: graphlay::NodeId,
    meta: &DfgEdgeRef,
    max_count: f64,
    group: Option<GroupId>,
    annotation: Option<&EdgeAnnotation>,
    measure: LabelWidth<'_>,
) -> Edge {
    let count = meta.count() as f64;
    Edge {
        weight: edge_weight(annotation, weight_from_count(count)),
        thickness: edge_thickness(annotation, thickness_from_count(count, max_count)),
        label_size: count_text(meta, annotation)
            .filter(|_| source != target)
            .map(|text| edge_label_size_with(&text, measure)),
        group,
        ..Edge::new(source, target)
    }
}

fn sorted_relations(dfg: &DirectlyFollowsGraph<'_>) -> Vec<(String, String, u32)> {
    let mut relations: Vec<(String, String, u32)> = dfg
        .directly_follows_relations
        .iter()
        .map(|((a, b), &count)| (a.to_string(), b.to_string(), count))
        .collect();
    relations.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    relations
}

/// The start or end counts of the activities that are nodes, sorted by activity.
fn boundary_counts(
    boundary: &HashMap<String, u32>,
    activities: &HashMap<String, u32>,
) -> BTreeMap<String, u32> {
    boundary
        .iter()
        .filter(|(a, _)| activities.contains_key(*a))
        .map(|(a, &c)| (a.clone(), c))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::test_utils::{assert_xml_well_formed, get_test_data_path};
    use crate::visualization::graph_build::heuristic_label_width;
    use crate::visualization::model::{self, Visualize};
    use crate::visualization::StyleOverrides;
    use crate::{EventLog, Importable};
    use graphlay::Direction;

    fn log() -> EventLog {
        EventLog::import_from_path(get_test_data_path().join("xes").join("small-example.xes"))
            .unwrap()
    }

    #[test]
    fn every_activity_and_relation_is_drawn_with_its_count() {
        let log = log();
        let dfg = DirectlyFollowsGraph::discover(&log);
        let (graph, nodes, edges) =
            dfg.to_graph(&DfgBuildOptions::default(), &heuristic_label_width);
        assert_eq!(graph.nodes.len(), dfg.activities.len() + 2);
        for e in &edges {
            if let DfgEdgeRef::Follows { from, to, count } = e {
                let stored = dfg
                    .directly_follows_relations
                    .iter()
                    .find_map(|((a, b), &c)| (a.as_ref() == from && b.as_ref() == to).then_some(c));
                assert_eq!(stored, Some(*count));
            }
        }
        assert_eq!(nodes[..2], [DfgNodeRef::Start, DfgNodeRef::End]);
    }

    #[test]
    fn start_and_end_edges_count_the_traces_beginning_and_ending_there() {
        let log = log();
        let dfg = DirectlyFollowsGraph::discover(&log);
        let (_, _, edges) = dfg.to_graph(&DfgBuildOptions::default(), &heuristic_label_width);
        let classifier = EventLogClassifier::default();
        let name = |e| classifier.get_class_identity(e);
        let mut drawn = (BTreeMap::new(), BTreeMap::new());
        for e in &edges {
            match e {
                DfgEdgeRef::Start { activity, count } => drawn.0.insert(activity.clone(), *count),
                DfgEdgeRef::End { activity, count } => drawn.1.insert(activity.clone(), *count),
                DfgEdgeRef::Follows { .. } => None,
            };
        }
        let mut traced = (BTreeMap::new(), BTreeMap::new());
        for trace in &log.traces {
            *traced.0.entry(name(&trace.events[0])).or_insert(0) += 1;
            *traced
                .1
                .entry(name(trace.events.last().unwrap()))
                .or_insert(0) += 1;
        }
        assert_eq!(drawn, traced);
    }

    #[test]
    fn terminals_are_pinned_and_optional() {
        let log = log();
        let dfg = DirectlyFollowsGraph::discover(&log);
        let (graph, _, _) = dfg.to_graph(&DfgBuildOptions::default(), &heuristic_label_width);
        assert_eq!(graph.nodes[0].rank, RankConstraint::First);
        assert_eq!(graph.nodes[1].rank, RankConstraint::Last);

        let options = DfgBuildOptions {
            terminals: false,
            ..DfgBuildOptions::default()
        };
        let (graph, _, edges) = dfg.to_graph(&options, &heuristic_label_width);
        assert_eq!(graph.nodes.len(), dfg.activities.len());
        assert!(edges
            .iter()
            .all(|e| matches!(e, DfgEdgeRef::Follows { .. })));
    }

    #[test]
    fn the_layout_request_carries_the_options_and_the_refs() {
        use crate::visualization::engine::layout_options_of;
        let log = log();
        let dfg = DirectlyFollowsGraph::discover(&log);
        let options = DfgBuildOptions::default();
        let request = model::layout_request(&dfg, &options).unwrap();
        assert_eq!(request.request.preset, Preset::ProcessMap);
        assert_eq!(
            layout_options_of(&request.request).unwrap(),
            graphlay::LayoutOptions::process_map()
        );

        let json = serde_json::to_string(&request).unwrap();
        assert!(
            json.contains(r#""nodes":[{"kind":"start"},{"kind":"end"},{"kind":"activity","name":"#)
        );
        let back: DfgLayoutRequest = serde_json::from_str(&json).unwrap();
        assert_eq!((back.nodes, back.edges), (request.nodes, request.edges));

        let diagonal = DfgBuildOptions {
            layout: LayoutSettings {
                direction: Some(Direction::LeftRight),
                layout_patch: serde_json::json!({"routing": {"style": "diagonal"}}),
            },
            ..options
        };
        let resolved = layout_options_of(&model::layout_request(&dfg, &diagonal).unwrap().request);
        let resolved = resolved.unwrap();
        assert_eq!(resolved.routing.style, graphlay::RoutingStyle::Diagonal);
        assert_eq!(resolved.direction, Direction::LeftRight);
        assert_eq!(
            resolved.spacing,
            graphlay::LayoutOptions::process_map().spacing
        );
    }

    #[test]
    fn overrides_restyle_the_element_at_their_index() {
        use crate::visualization::styling::ElementStyle;
        use graphlay::svg::Paint;
        let log = log();
        let dfg = DirectlyFollowsGraph::discover(&log);
        let options = DfgBuildOptions::default();
        let layout = model::layout(&dfg, &options).unwrap();
        let red = || "#dc2626".to_string();
        let mut overrides = StyleOverrides::default();
        overrides.nodes.insert(
            3,
            ElementStyle {
                fill: Some(red()),
                extra_label: Some("42%".to_string()),
                ..ElementStyle::default()
            },
        );
        overrides.edges.insert(
            2,
            ElementStyle {
                stroke: Some(red()),
                opacity: Some(0.3),
                ..ElementStyle::default()
            },
        );
        let scene =
            model::scene_from_layout(&dfg, &layout, &options, &DfgStyle::default(), &overrides)
                .unwrap();
        let filled: Vec<usize> = (0..scene.nodes.len())
            .filter(|&i| scene.nodes[i].fill == Some(red()))
            .collect();
        assert_eq!(filled, [3]);
        assert_eq!(
            scene.nodes[3].content.len(),
            2,
            "the label and the extra line"
        );
        let stroked: Vec<usize> = (0..scene.edges.len())
            .filter(|&i| scene.edges[i].stroke.paint == Paint::Color(red()))
            .collect();
        assert_eq!(stroked, [2]);
        assert_eq!(
            scene.edges[2].attrs.extra,
            [("opacity".to_string(), "0.3".to_string())]
        );

        let mut short = layout;
        short.nodes.pop();
        let err =
            model::scene_from_layout(&dfg, &short, &options, &DfgStyle::default(), &overrides);
        assert!(matches!(
            err,
            Err(VisualizationError::LayoutMismatch { .. })
        ));
    }

    fn texts(content: &[graphlay::svg::NodeContent]) -> Vec<&str> {
        content
            .iter()
            .filter_map(|c| match c {
                graphlay::svg::NodeContent::Text(run) => Some(run.text.as_str()),
                _ => None,
            })
            .collect()
    }

    /// Durations instead of counts: the layout reserves room for the annotated labels as it does
    /// for the counts, and the drawing shows them at the annotated widths.
    #[test]
    fn annotations_replace_the_counts_by_model_element() {
        use crate::visualization::engine::layout_graph;
        use crate::visualization::graph_build::{dfg_width, edge_label_size};
        let log = log();
        let dfg = DirectlyFollowsGraph::discover(&log);
        let plain = DfgBuildOptions::default();
        let (plain_graph, nodes, edges) = dfg.to_graph(&plain, &heuristic_label_width);
        let start = edges
            .iter()
            .position(|e| matches!(e, DfgEdgeRef::Start { .. }))
            .unwrap();
        let hidden = edges
            .iter()
            .position(|e| matches!(e, DfgEdgeRef::Follows { from, to, .. } if from != to))
            .unwrap();
        let DfgEdgeRef::Start { activity, .. } = &edges[start] else {
            unreachable!()
        };
        let note = "a line much wider than any activity name";
        let options: DfgBuildOptions = serde_json::from_value(serde_json::json!({
            "annotations": {
                "nodes": [[{"kind": "activity", "name": activity}, {"label": note}]],
                "edges": [
                    [
                        [{"kind": "start"}, {"kind": "activity", "name": activity}],
                        {"label": "1.5 days", "weight": 7.0, "thickness": 9.0}
                    ],
                    [edges[hidden].endpoints(), {"label": ""}]
                ]
            }
        }))
        .unwrap();

        let request = model::layout_request(&dfg, &options).unwrap();
        assert_eq!((&request.nodes, &request.edges), (&nodes, &edges));
        let graph = &request.request.graph;
        let annotated = &graph.edges[start];
        assert_eq!(annotated.label_size, Some(edge_label_size("1.5 days")));
        assert_eq!((annotated.weight, annotated.thickness), (7.0, 9.0));
        assert_eq!(graph.edges[hidden].label_size, None);
        assert_eq!(graph.edges[hidden].weight, plain_graph.edges[hidden].weight);
        for i in (0..edges.len()).filter(|i| ![start, hidden].contains(i)) {
            assert_eq!(graph.edges[i], plain_graph.edges[i]);
        }
        let node = nodes
            .iter()
            .position(|n| *n == DfgNodeRef::Activity(activity.clone()))
            .unwrap();
        assert_eq!(graph.nodes[node].size.width, dfg_width(note));
        assert!(plain_graph.nodes[node].size.width < dfg_width(note));

        let layout = layout_graph(&request.request).unwrap();
        let style = DfgStyle::default();
        let overrides = StyleOverrides::default();
        let scene = model::scene_from_layout(&dfg, &layout, &options, &style, &overrides).unwrap();
        let plain_scene =
            model::scene_from_layout(&dfg, &layout, &plain, &style, &overrides).unwrap();
        let label = |e: &graphlay::svg::SceneEdge| e.labels.first().map(|l| l.text.text.clone());
        assert_eq!(label(&scene.edges[start]).as_deref(), Some("1.5 days"));
        assert_eq!(scene.edges[start].stroke.width, 9.0);
        assert!(scene.edges[hidden].labels.is_empty());
        assert_eq!(
            label(&plain_scene.edges[start]),
            Some(edges[start].count().to_string())
        );
        assert_eq!(texts(&scene.nodes[node].content).last(), Some(&note));
        assert_eq!(texts(&plain_scene.nodes[node].content).len(), 1);
    }

    #[test]
    fn start_and_end_marks_are_inline_polygons() {
        let log = log();
        let dfg = DirectlyFollowsGraph::discover(&log);
        let svg = model::svg(&dfg, &DfgBuildOptions::default(), &DfgStyle::default()).unwrap();
        assert_xml_well_formed(&svg);
        assert_eq!(svg.matches("<polygon").count(), 2);
        assert!(!svg.contains("<use"));
    }

    /// Without terminals, the start and end counts are not edges, so they must not set the
    /// widest edge, in the layout or in the drawing.
    #[test]
    fn edges_are_drawn_as_wide_as_the_layout_keeps_them() {
        use crate::visualization::engine::layout_graph;
        let log = log();
        let dfg = DirectlyFollowsGraph::discover(&log);
        for terminals in [true, false] {
            let options = DfgBuildOptions {
                terminals,
                ..DfgBuildOptions::default()
            };
            let request = model::layout_request(&dfg, &options).unwrap();
            let graph = &request.request.graph;
            let widest = graph.edges.iter().map(|e| e.thickness).fold(0.0, f64::max);
            assert_eq!(widest, thickness_from_count(1.0, 1.0));
            let layout = layout_graph(&request.request).unwrap();
            let style = DfgStyle::default();
            let overrides = StyleOverrides::default();
            let scene =
                model::scene_from_layout(&dfg, &layout, &options, &style, &overrides).unwrap();
            let drawn: Vec<f64> = scene.edges.iter().map(|e| e.stroke.width).collect();
            let kept: Vec<f64> = graph.edges.iter().map(|e| e.thickness).collect();
            assert_eq!(drawn, kept);
        }
    }
}
