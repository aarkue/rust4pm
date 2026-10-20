//! Laying out and drawing a [`DirectlyFollowsGraph`], with START and END terminals.

use std::collections::BTreeMap;

use graphlay::{Direction, Edge, Graph, LayoutError, LayoutOptions, Node, RankConstraint, Shape};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::dfg::DirectlyFollowsGraph;
use crate::visualization::graph_build::{
    dfg_width_with, edge_label_size_with, heuristic_label_width, thickness_from_count,
    weight_from_count, GraphAcc, LabelWidth, TERMINAL_SIZE,
};
use crate::visualization::styling::BaseStyle;
use crate::EventLog;

/// Height of a DFG activity box.
pub const ACTIVITY_HEIGHT: f64 = 52.0;

/// A node of a [`DirectlyFollowsGraph`] drawing.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
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
}

/// How a [`DirectlyFollowsGraph`] is turned into a [`Graph`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(JsonSchema))]
#[serde(default)]
pub struct DfgBuildOptions {
    /// Flow direction. `None` means top to bottom.
    pub direction: Option<Direction>,
    /// Draw the artificial START / END terminals. On by default.
    pub terminals: bool,
    /// How many traces begin with each activity, which a [`DirectlyFollowsGraph`] does not record.
    /// [`DfgBuildOptions::from_log`] fills it. Empty weights each start activity by its frequency.
    pub start_counts: BTreeMap<String, u32>,
    /// How many traces end with each activity. See [`DfgBuildOptions::start_counts`].
    pub end_counts: BTreeMap<String, u32>,
}

impl Default for DfgBuildOptions {
    fn default() -> Self {
        DfgBuildOptions {
            direction: None,
            terminals: true,
            start_counts: BTreeMap::new(),
            end_counts: BTreeMap::new(),
        }
    }
}

impl DfgBuildOptions {
    /// The default options plus the trace-boundary counts of `log`, under the default classifier.
    pub fn from_log(log: &EventLog) -> Self {
        Self::from_log_with(log, &EventLogClassifier::default())
    }

    /// [`DfgBuildOptions::from_log`] with the classifier the [`DirectlyFollowsGraph`] was
    /// discovered with, so the counts name its activities.
    pub fn from_log_with(log: &EventLog, classifier: &EventLogClassifier) -> Self {
        let (start_counts, end_counts) = trace_boundary_counts(log, classifier);
        DfgBuildOptions {
            start_counts,
            end_counts,
            ..Self::default()
        }
    }

    /// The engine options for these build options.
    pub fn layout_options(&self) -> LayoutOptions {
        let mut options = LayoutOptions::process_map();
        if let Some(direction) = self.direction {
            options.direction = direction;
        }
        options
    }
}

/// How often each activity begins, and ends, a trace of `log`.
pub fn trace_boundary_counts(
    log: &EventLog,
    classifier: &EventLogClassifier,
) -> (BTreeMap<String, u32>, BTreeMap<String, u32>) {
    let mut starts: BTreeMap<String, u32> = BTreeMap::new();
    let mut ends: BTreeMap<String, u32> = BTreeMap::new();
    for trace in &log.traces {
        if let Some(first) = trace.events.first() {
            *starts
                .entry(classifier.get_class_identity(first))
                .or_default() += 1;
        }
        if let Some(last) = trace.events.last() {
            *ends.entry(classifier.get_class_identity(last)).or_default() += 1;
        }
    }
    (starts, ends)
}

/// Colors a directly-follows graph is drawn with.
#[derive(Debug, Clone, PartialEq)]
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

/// The [`Graph`] of `dfg`, plus the node and edge meta tables.
pub fn dfg_to_graph(
    dfg: &DirectlyFollowsGraph<'_>,
    options: &DfgBuildOptions,
) -> (Graph, Vec<DfgNodeRef>, Vec<DfgEdgeRef>) {
    dfg_to_graph_with(dfg, options, &heuristic_label_width)
}

/// [`dfg_to_graph`] with a caller's own label measurement.
pub fn dfg_to_graph_with(
    dfg: &DirectlyFollowsGraph<'_>,
    options: &DfgBuildOptions,
    measure: LabelWidth<'_>,
) -> (Graph, Vec<DfgNodeRef>, Vec<DfgEdgeRef>) {
    let mut acc: GraphAcc<DfgNodeRef, DfgEdgeRef> = GraphAcc::new();

    if options.terminals {
        acc.add_node(
            START_KEY,
            Node {
                size: TERMINAL_SIZE,
                shape: Shape::Box,
                rank: RankConstraint::First,
                ..Node::default()
            },
            || DfgNodeRef::Start,
        );
        acc.add_node(
            END_KEY,
            Node {
                size: TERMINAL_SIZE,
                shape: Shape::Box,
                rank: RankConstraint::Last,
                ..Node::default()
            },
            || DfgNodeRef::End,
        );
    }

    let mut activities: Vec<&String> = dfg.activities.keys().collect();
    activities.sort();
    for name in &activities {
        acc.add_node(
            activity_key(name),
            Node {
                size: graphlay::Size::new(dfg_width_with(name, measure), ACTIVITY_HEIGHT),
                shape: Shape::Box,
                ..Node::default()
            },
            || DfgNodeRef::Activity((*name).clone()),
        );
    }

    let relations = sorted_relations(dfg);
    let starts = boundary_counts(
        &options.start_counts,
        &dfg.start_activities,
        &dfg.activities,
    );
    let ends = boundary_counts(&options.end_counts, &dfg.end_activities, &dfg.activities);
    let max_count = relations
        .iter()
        .map(|(_, _, c)| *c)
        .chain(starts.values().copied())
        .chain(ends.values().copied())
        .max()
        .unwrap_or(1) as f64;

    for (from, to, count) in &relations {
        let (Some(source), Some(target)) = (
            acc.node_id(&activity_key(from)),
            acc.node_id(&activity_key(to)),
        ) else {
            continue;
        };
        acc.add_edge(
            count_edge(source, target, *count, max_count, None, measure),
            DfgEdgeRef::Follows {
                from: from.clone(),
                to: to.clone(),
                count: *count,
            },
        );
    }

    if options.terminals {
        let start = acc.node_id(START_KEY);
        for (activity, count) in &starts {
            let (Some(source), Some(target)) = (start, acc.node_id(&activity_key(activity))) else {
                continue;
            };
            acc.add_edge(
                count_edge(source, target, *count, max_count, None, measure),
                DfgEdgeRef::Start {
                    activity: activity.clone(),
                    count: *count,
                },
            );
        }
        let end = acc.node_id(END_KEY);
        for (activity, count) in &ends {
            let (Some(source), Some(target)) = (acc.node_id(&activity_key(activity)), end) else {
                continue;
            };
            acc.add_edge(
                count_edge(source, target, *count, max_count, None, measure),
                DfgEdgeRef::End {
                    activity: activity.clone(),
                    count: *count,
                },
            );
        }
    }

    acc.finish()
}

pub(crate) const START_KEY: &str = "START";
pub(crate) const END_KEY: &str = "END";

/// By name only, so an OC-DFG's per-type sub-graphs share one node per activity.
pub(crate) fn activity_key(activity: &str) -> String {
    format!("act:{activity}")
}

/// A self-loop's label reserves no room: the renderer puts it at the loop's apex, which is clear,
/// and reserving room would push it onto a neighbor.
pub(crate) fn count_edge(
    source: graphlay::NodeId,
    target: graphlay::NodeId,
    count: u32,
    max_count: f64,
    group: Option<graphlay::GroupId>,
    measure: LabelWidth<'_>,
) -> Edge {
    Edge {
        weight: weight_from_count(count as f64),
        thickness: thickness_from_count(count as f64, max_count),
        label_size: (source != target).then(|| edge_label_size_with(&count.to_string(), measure)),
        group,
        ..Edge::new(source, target)
    }
}

pub(crate) fn sorted_relations(dfg: &DirectlyFollowsGraph<'_>) -> Vec<(String, String, u32)> {
    let mut relations: Vec<(String, String, u32)> = dfg
        .directly_follows_relations
        .iter()
        .map(|((a, b), &count)| (a.to_string(), b.to_string(), count))
        .collect();
    relations.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    relations
}

/// `supplied`, or the boundary activities' total frequency when it is empty.
pub(crate) fn boundary_counts(
    supplied: &BTreeMap<String, u32>,
    boundary: &std::collections::HashSet<String>,
    activities: &std::collections::HashMap<String, u32>,
) -> BTreeMap<String, u32> {
    if !supplied.is_empty() {
        return supplied
            .iter()
            .map(|(a, &c)| (a.clone(), c))
            .filter(|(a, _)| activities.contains_key(a))
            .collect();
    }
    boundary
        .iter()
        .filter_map(|a| activities.get_key_value(a))
        .map(|(a, &c)| (a.clone(), c))
        .collect()
}

/// Lay `dfg` out.
///
/// # Errors
///
/// [`LayoutError`] from the engine.
pub fn layout_dfg(
    dfg: &DirectlyFollowsGraph<'_>,
    options: &DfgBuildOptions,
) -> Result<graphlay::Layout, LayoutError> {
    let (graph, _, _) = dfg_to_graph(dfg, options);
    graphlay::layout(&graph, &options.layout_options())
}

mod draw {
    use super::*;
    use graphlay::svg::{Scene, Stroke};
    use graphlay::Layout;

    use crate::visualization::styling::{
        edge_label, node_label, scene, scene_edge, scene_node, terminal_mark,
    };

    /// The drawing of `dfg` over an existing [`Layout`].
    pub fn dfg_scene(
        dfg: &DirectlyFollowsGraph<'_>,
        layout: &Layout,
        nodes: &[DfgNodeRef],
        edges: &[DfgEdgeRef],
        style: &DfgStyle,
    ) -> Scene {
        let _ = dfg;
        let base = &style.base;
        let mut scene_nodes = Vec::with_capacity(nodes.len());
        for (placement, node) in layout.nodes.iter().zip(nodes) {
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
            scene_nodes.push(n);
        }

        // The meta table holds every count, so this is the maximum the graph was built with.
        let max_count = edges.iter().map(DfgEdgeRef::count).max().unwrap_or(1) as f64;
        let mut scene_edges = Vec::with_capacity(edges.len());
        for (route, meta) in layout.edges.iter().zip(edges) {
            let width = thickness_from_count(meta.count() as f64, max_count);
            let Some(mut e) = scene_edge(route, &base.edge, width) else {
                continue;
            };
            e.labels
                .push(edge_label(&meta.count().to_string(), route, base));
            scene_edges.push(e);
        }
        scene(base, scene_nodes, scene_edges)
    }

    /// Lay `dfg` out and draw it, as one standalone SVG document.
    ///
    /// # Errors
    ///
    /// As [`layout_dfg`].
    pub fn export_dfg_svg(
        dfg: &DirectlyFollowsGraph<'_>,
        options: &DfgBuildOptions,
    ) -> Result<String, LayoutError> {
        let (graph, nodes, edges) = dfg_to_graph(dfg, options);
        let layout = graphlay::layout(&graph, &options.layout_options())?;
        Ok(crate::visualization::styling::render_scene(&dfg_scene(
            dfg,
            &layout,
            &nodes,
            &edges,
            &DfgStyle::default(),
        )))
    }
}

pub use draw::{dfg_scene, export_dfg_svg};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{assert_xml_well_formed, get_test_data_path};
    use crate::Importable;

    fn log() -> EventLog {
        EventLog::import_from_path(get_test_data_path().join("xes").join("small-example.xes"))
            .unwrap()
    }

    #[test]
    fn every_activity_and_relation_is_drawn_with_its_count() {
        let log = log();
        let dfg = DirectlyFollowsGraph::discover(&log);
        let (graph, nodes, edges) = dfg_to_graph(&dfg, &DfgBuildOptions::from_log(&log));
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
    fn terminals_are_pinned_and_optional() {
        let log = log();
        let dfg = DirectlyFollowsGraph::discover(&log);
        let (graph, _, _) = dfg_to_graph(&dfg, &DfgBuildOptions::from_log(&log));
        assert_eq!(graph.nodes[0].rank, RankConstraint::First);
        assert_eq!(graph.nodes[1].rank, RankConstraint::Last);

        let options = DfgBuildOptions {
            terminals: false,
            ..DfgBuildOptions::from_log(&log)
        };
        let (graph, _, edges) = dfg_to_graph(&dfg, &options);
        assert_eq!(graph.nodes.len(), dfg.activities.len());
        assert!(edges
            .iter()
            .all(|e| matches!(e, DfgEdgeRef::Follows { .. })));
    }

    #[test]
    fn start_and_end_marks_are_inline_polygons() {
        let log = log();
        let dfg = DirectlyFollowsGraph::discover(&log);
        let svg = export_dfg_svg(&dfg, &DfgBuildOptions::from_log(&log)).unwrap();
        assert_xml_well_formed(&svg);
        assert_eq!(svg.matches("<polygon").count(), 2);
        assert!(!svg.contains("<use"));
    }
}
