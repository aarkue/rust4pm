//! Laying out and drawing an [`OCDirectlyFollowsGraph`]: one DFG per object type in one picture.

use std::collections::BTreeMap;

use graphlay::{
    Direction, Graph, GroupId, LayoutError, LayoutOptions, Node, RankConstraint, Shape,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::ocdfg::OCDirectlyFollowsGraph;
use crate::visualization::dfg::{
    activity_key, boundary_counts, count_edge, sorted_relations, trace_boundary_counts, DfgEdgeRef,
    ACTIVITY_HEIGHT,
};
use crate::visualization::graph_build::{
    dfg_width_with, heuristic_label_width, GraphAcc, LabelWidth, TERMINAL_SIZE,
};
use crate::visualization::styling::BaseStyle;
use crate::EventLog;

/// A node of an [`OCDirectlyFollowsGraph`] drawing.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
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

/// How an [`OCDirectlyFollowsGraph`] is turned into a [`Graph`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(JsonSchema))]
#[serde(default)]
pub struct OcdfgBuildOptions {
    /// Flow direction. `None` means top to bottom.
    pub direction: Option<Direction>,
    /// Draw each object type's START / END terminals. On by default.
    pub terminals: bool,
    /// Object types to draw, in this order. Empty draws all of them, by name.
    pub object_types: Vec<String>,
    /// Per object type, how many of its cases begin with each activity. See
    /// [`OcdfgBuildOptions::with_log`].
    pub start_counts: BTreeMap<String, BTreeMap<String, u32>>,
    /// Per object type, how many of its cases end with each activity.
    pub end_counts: BTreeMap<String, BTreeMap<String, u32>>,
}

impl Default for OcdfgBuildOptions {
    fn default() -> Self {
        OcdfgBuildOptions {
            direction: None,
            terminals: true,
            object_types: Vec::new(),
            start_counts: BTreeMap::new(),
            end_counts: BTreeMap::new(),
        }
    }
}

impl OcdfgBuildOptions {
    /// Record the trace-boundary counts of one object type's flattened log.
    #[must_use]
    pub fn with_log(self, object_type: impl Into<String>, log: &EventLog) -> Self {
        self.with_log_using(object_type, log, &EventLogClassifier::default())
    }

    /// [`OcdfgBuildOptions::with_log`] with a custom classifier.
    #[must_use]
    pub fn with_log_using(
        mut self,
        object_type: impl Into<String>,
        log: &EventLog,
        classifier: &EventLogClassifier,
    ) -> Self {
        let object_type = object_type.into();
        let (starts, ends) = trace_boundary_counts(log, classifier);
        self.start_counts.insert(object_type.clone(), starts);
        self.end_counts.insert(object_type, ends);
        self
    }

    /// The object types to draw, in group order.
    pub fn types_of(&self, model: &OCDirectlyFollowsGraph<'_>) -> Vec<String> {
        if !self.object_types.is_empty() {
            return self
                .object_types
                .iter()
                .filter(|t| model.object_type_to_dfg.contains_key(*t))
                .cloned()
                .collect();
        }
        let mut types: Vec<String> = model.object_type_to_dfg.keys().cloned().collect();
        types.sort();
        types
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

/// Colors an object-centric directly-follows graph is drawn with.
#[derive(Debug, Clone, PartialEq)]
pub struct OcdfgStyle {
    /// Colors and paddings shared with every other model.
    pub base: BaseStyle,
    /// How far towards white an object type's color is washed for a node fill.
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

/// The [`Graph`] of `model`, plus the node and edge meta tables.
pub fn ocdfg_to_graph(
    model: &OCDirectlyFollowsGraph<'_>,
    options: &OcdfgBuildOptions,
) -> (Graph, Vec<OcdfgNodeRef>, Vec<OcdfgEdgeRef>) {
    ocdfg_to_graph_with(model, options, &heuristic_label_width)
}

/// [`ocdfg_to_graph`] with a caller's own label measurement.
pub fn ocdfg_to_graph_with(
    model: &OCDirectlyFollowsGraph<'_>,
    options: &OcdfgBuildOptions,
    measure: LabelWidth<'_>,
) -> (Graph, Vec<OcdfgNodeRef>, Vec<OcdfgEdgeRef>) {
    let mut acc: GraphAcc<OcdfgNodeRef, OcdfgEdgeRef> = GraphAcc::new();
    let types = options.types_of(model);

    for (index, object_type) in types.iter().enumerate() {
        let group = Some(GroupId(index as u32));
        let Some(dfg) = model.object_type_to_dfg.get(object_type) else {
            continue;
        };
        let (start_key, end_key) = (format!("START:{object_type}"), format!("END:{object_type}"));
        if options.terminals {
            acc.add_node(
                &start_key,
                Node {
                    size: TERMINAL_SIZE,
                    shape: Shape::Box,
                    rank: RankConstraint::First,
                    group,
                    ..Node::default()
                },
                || OcdfgNodeRef::Start(object_type.clone()),
            );
            acc.add_node(
                &end_key,
                Node {
                    size: TERMINAL_SIZE,
                    shape: Shape::Box,
                    rank: RankConstraint::Last,
                    group,
                    ..Node::default()
                },
                || OcdfgNodeRef::End(object_type.clone()),
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
                    group,
                    ..Node::default()
                },
                || OcdfgNodeRef::Activity((*name).clone()),
            );
        }

        let empty = BTreeMap::new();
        let relations = sorted_relations(dfg);
        let starts = boundary_counts(
            options.start_counts.get(object_type).unwrap_or(&empty),
            &dfg.start_activities,
            &dfg.activities,
        );
        let ends = boundary_counts(
            options.end_counts.get(object_type).unwrap_or(&empty),
            &dfg.end_activities,
            &dfg.activities,
        );
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
                count_edge(source, target, *count, max_count, group, measure),
                OcdfgEdgeRef {
                    object_type: object_type.clone(),
                    edge: DfgEdgeRef::Follows {
                        from: from.clone(),
                        to: to.clone(),
                        count: *count,
                    },
                },
            );
        }
        if options.terminals {
            for (activity, count) in &starts {
                let (Some(source), Some(target)) = (
                    acc.node_id(&start_key),
                    acc.node_id(&activity_key(activity)),
                ) else {
                    continue;
                };
                acc.add_edge(
                    count_edge(source, target, *count, max_count, group, measure),
                    OcdfgEdgeRef {
                        object_type: object_type.clone(),
                        edge: DfgEdgeRef::Start {
                            activity: activity.clone(),
                            count: *count,
                        },
                    },
                );
            }
            for (activity, count) in &ends {
                let (Some(source), Some(target)) =
                    (acc.node_id(&activity_key(activity)), acc.node_id(&end_key))
                else {
                    continue;
                };
                acc.add_edge(
                    count_edge(source, target, *count, max_count, group, measure),
                    OcdfgEdgeRef {
                        object_type: object_type.clone(),
                        edge: DfgEdgeRef::End {
                            activity: activity.clone(),
                            count: *count,
                        },
                    },
                );
            }
        }
    }

    acc.finish()
}

/// Lay `model` out.
///
/// # Errors
///
/// [`LayoutError`] from the engine.
pub fn layout_ocdfg(
    model: &OCDirectlyFollowsGraph<'_>,
    options: &OcdfgBuildOptions,
) -> Result<graphlay::Layout, LayoutError> {
    let (graph, _, _) = ocdfg_to_graph(model, options);
    graphlay::layout(&graph, &options.layout_options())
}

mod draw {
    use super::*;
    use graphlay::svg::{Scene, Stroke};
    use graphlay::Layout;

    use crate::visualization::styling::{
        edge_label, key_color, lighten, node_label, scene, scene_edge, scene_node, terminal_mark,
    };

    /// The drawing of `model` over an existing [`Layout`], one color per object type.
    pub fn ocdfg_scene(
        model: &OCDirectlyFollowsGraph<'_>,
        layout: &Layout,
        nodes: &[OcdfgNodeRef],
        edges: &[OcdfgEdgeRef],
        style: &OcdfgStyle,
    ) -> Scene {
        let _ = model;
        let base = &style.base;
        let color_of = |object_type: &str| key_color("objectType", object_type);

        let mut scene_nodes = Vec::with_capacity(nodes.len());
        for (placement, node) in layout.nodes.iter().zip(nodes) {
            let n = match node {
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
                    let color = color_of(t);
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
            scene_nodes.push(n);
        }

        let max_count = edges.iter().map(|e| e.edge.count()).max().unwrap_or(1) as f64;
        let mut scene_edges = Vec::with_capacity(edges.len());
        for (route, meta) in layout.edges.iter().zip(edges) {
            let width = crate::visualization::graph_build::thickness_from_count(
                meta.edge.count() as f64,
                max_count,
            );
            let Some(mut e) = scene_edge(route, &color_of(&meta.object_type), width) else {
                continue;
            };
            e.labels
                .push(edge_label(&meta.edge.count().to_string(), route, base));
            scene_edges.push(e);
        }
        scene(base, scene_nodes, scene_edges)
    }

    /// Lay `model` out and draw it, as one standalone SVG document.
    ///
    /// # Errors
    ///
    /// As [`layout_ocdfg`].
    pub fn export_ocdfg_svg(
        model: &OCDirectlyFollowsGraph<'_>,
        options: &OcdfgBuildOptions,
    ) -> Result<String, LayoutError> {
        let (graph, nodes, edges) = ocdfg_to_graph(model, options);
        let layout = graphlay::layout(&graph, &options.layout_options())?;
        Ok(crate::visualization::styling::render_scene(&ocdfg_scene(
            model,
            &layout,
            &nodes,
            &edges,
            &OcdfgStyle::default(),
        )))
    }
}

pub use draw::{export_ocdfg_svg, ocdfg_scene};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::linked_ocel::IndexLinkedOCEL;
    use crate::test_utils::get_test_data_path;
    use crate::visualization::styling::key_color;
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
            let (graph, nodes, edges) = ocdfg_to_graph(model, &options);
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
                let (graph, nodes, edges) = ocdfg_to_graph(model, &options);
                let layout = graphlay::layout(&graph, &options.layout_options()).unwrap();
                let scene = ocdfg_scene(model, &layout, &nodes, &edges, &OcdfgStyle::default());
                for (node, meta) in scene.nodes.iter().zip(&nodes) {
                    if let OcdfgNodeRef::Start(t) | OcdfgNodeRef::End(t) = meta {
                        assert_eq!(node.fill, Some(key_color("objectType", t)), "{meta:?}");
                    }
                }
            }
        });
    }

    #[test]
    fn writes_the_svg_export() {
        use crate::core::event_data::object_centric::utils::flatten::flatten_ocel_on;
        with_model("order-management.json", |locel, model| {
            let types = ["orders", "items", "packages"];
            let mut options = OcdfgBuildOptions {
                object_types: types.map(String::from).to_vec(),
                ..OcdfgBuildOptions::default()
            };
            for object_type in types {
                let mut flattened = flatten_ocel_on(locel, object_type);
                flattened.traces.retain(|t| !t.events.is_empty());
                options = options.with_log(object_type, &flattened);
            }
            let svg = export_ocdfg_svg(model, &options).unwrap();
            assert!(svg.contains("<polygon") && !svg.contains("<use"));
            crate::test_utils::write_export("order-management-OCDFG.svg", &svg);
        });
    }
}
