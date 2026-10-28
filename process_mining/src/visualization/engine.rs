//! Laying out any model's layout request with graphlay, and drawing any scene as SVG.
//!
//! The functions here take the request of [`layout_request`](super::layout_request) unchanged:
//! [`layout_graph`] for a fresh layout, [`reroute_graph`] after the user dragged nodes, and
//! [`layout_tree`] for a [`TreeRequest`].

use graphlay::json::{LayoutRequest, TreeRequest};
use graphlay::svg::{HeuristicMeasurer, Palette, Scene, Theme};
use graphlay::{Direction, Graph, Layout, LayoutError, LayoutOptions, Point, Preset, TreeOptions};
use serde_json::Value;

/// Why a model could not be laid out or drawn.
#[derive(Debug, Clone, PartialEq)]
pub enum VisualizationError {
    /// The layout options patch does not describe valid options. The message names the field.
    InvalidOptions(String),
    /// The engine rejected the graph.
    Layout(LayoutError),
    /// The layout was not made from this model's graph.
    LayoutMismatch {
        /// Nodes of the model's graph.
        nodes: usize,
        /// Edges of the model's graph.
        edges: usize,
        /// Nodes of the layout.
        layout_nodes: usize,
        /// Edges of the layout.
        layout_edges: usize,
    },
    /// A reroute was given positions, but not one per node.
    PositionCount {
        /// Nodes of the graph.
        nodes: usize,
        /// Positions given.
        positions: usize,
    },
}

impl std::fmt::Display for VisualizationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VisualizationError::InvalidOptions(message) => f.write_str(message),
            VisualizationError::Layout(e) => e.fmt(f),
            VisualizationError::LayoutMismatch {
                nodes,
                edges,
                layout_nodes,
                layout_edges,
            } => write!(
                f,
                "the layout has {layout_nodes} nodes and {layout_edges} edges, but the model's \
                 graph has {nodes} nodes and {edges} edges"
            ),
            VisualizationError::PositionCount { nodes, positions } => write!(
                f,
                "a reroute needs one position per node, but got {positions} for {nodes} nodes"
            ),
        }
    }
}

impl std::error::Error for VisualizationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            VisualizationError::Layout(e) => Some(e),
            _ => None,
        }
    }
}

impl From<LayoutError> for VisualizationError {
    fn from(e: LayoutError) -> Self {
        VisualizationError::Layout(e)
    }
}

/// Deep-merges `patch` into `base` the way graphlay resolves a request's options: objects merge
/// key by key, anything else replaces. A `null` patch changes nothing.
pub(crate) fn merge_patch(base: &mut Value, patch: &Value) {
    if patch.is_null() {
        return;
    }
    merge(base, patch);
}

fn merge(base: &mut Value, patch: &Value) {
    match (base, patch) {
        (Value::Object(b), Value::Object(p)) => {
            for (k, v) in p {
                merge(b.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (b, p) => *b = p.clone(),
    }
}

/// `value` as JSON. For the engine's own option types, which always serialize.
pub(crate) fn json_of<T: serde::Serialize>(value: T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

/// An options patch setting `direction`, or an empty one.
pub(crate) fn direction_patch(direction: Option<Direction>) -> Value {
    let mut patch = serde_json::Map::new();
    if let Some(direction) = direction {
        patch.insert("direction".into(), json_of(direction));
    }
    Value::Object(patch)
}

/// A layered request for `graph`: the model's own `patch` over `preset`, then the caller's
/// `user_patch` over that. Fails if the merged patch is not valid options.
pub(crate) fn layered_request(
    graph: Graph,
    preset: Preset,
    mut patch: Value,
    user_patch: &Value,
) -> Result<LayoutRequest, VisualizationError> {
    merge_patch(&mut patch, user_patch);
    let request = LayoutRequest {
        graph,
        preset,
        options: patch,
    };
    layout_options_of(&request)?;
    Ok(request)
}

/// A tidy-tree request for `graph`, with the caller's `user_patch` over the model's `options`.
pub(crate) fn tree_request(
    graph: Graph,
    options: TreeOptions,
    user_patch: &Value,
) -> Result<TreeRequest, VisualizationError> {
    let mut patch = json_of(options);
    merge_patch(&mut patch, user_patch);
    let request = TreeRequest {
        graph,
        options: patch,
    };
    tree_options_of(&request)?;
    Ok(request)
}

/// The options `request` resolves to: its preset with its patch merged over it.
///
/// # Errors
///
/// [`VisualizationError::InvalidOptions`] if the patch does not describe valid options.
pub fn layout_options_of(request: &LayoutRequest) -> Result<LayoutOptions, VisualizationError> {
    let options_only = LayoutRequest {
        graph: Graph::default(),
        preset: request.preset,
        options: request.options.clone(),
    };
    let (_, options) = options_only
        .split()
        .map_err(VisualizationError::InvalidOptions)?;
    Ok(options)
}

/// The options `request` resolves to: the tidy-tree defaults with its patch merged over them.
///
/// # Errors
///
/// [`VisualizationError::InvalidOptions`] if the patch does not describe valid options.
pub fn tree_options_of(request: &TreeRequest) -> Result<TreeOptions, VisualizationError> {
    let options_only = TreeRequest {
        graph: Graph::default(),
        options: request.options.clone(),
    };
    let (_, options) = options_only
        .split()
        .map_err(VisualizationError::InvalidOptions)?;
    Ok(options)
}

/// Lay the graph of `request` out with the layered engine.
///
/// `layout.nodes[i]` is where `request.graph.nodes[i]` went, and `layout.edges[i]` the route of
/// `request.graph.edges[i]`. Nodes with a `position` steer the result as the options'
/// [`seed_mode`](LayoutOptions::seed_mode) sets, and a `pinned` one stays exactly there.
///
/// # Errors
///
/// An invalid options patch, or a graph the engine rejects.
pub fn layout_graph(request: &LayoutRequest) -> Result<Layout, VisualizationError> {
    let options = layout_options_of(request)?;
    Ok(graphlay::layout(&request.graph, &options)?)
}

/// Keep every node where it is and route the edges again, e.g., after the user dragged a node.
///
/// `positions` are the node centers, one per node, in node order. An empty slice uses the
/// `position` each node of `request` already carries.
///
/// # Errors
///
/// An invalid options patch, a position count other than the node count, or a node without a
/// position.
pub fn reroute_graph(
    request: &LayoutRequest,
    positions: &[Point],
) -> Result<Layout, VisualizationError> {
    let options = layout_options_of(request)?;
    if positions.is_empty() {
        return Ok(graphlay::reroute(&request.graph, &options)?);
    }
    if positions.len() != request.graph.nodes.len() {
        return Err(VisualizationError::PositionCount {
            nodes: request.graph.nodes.len(),
            positions: positions.len(),
        });
    }
    let mut graph = request.graph.clone();
    for (node, &p) in graph.nodes.iter_mut().zip(positions) {
        node.position = Some(p);
    }
    Ok(graphlay::reroute(&graph, &options)?)
}

/// Lay the rooted tree or forest of `request` out with the tidy-tree engine.
///
/// # Errors
///
/// An invalid options patch, or a graph that is not a rooted tree or forest.
pub fn layout_tree(request: &TreeRequest) -> Result<Layout, VisualizationError> {
    let options = tree_options_of(request)?;
    Ok(graphlay::tree::layout(&request.graph, &options)?)
}

/// Draw `scene` as a standalone SVG document. Elements that name no color of their own fall back
/// to `palette`, or to graphlay's default palette.
pub fn render_scene_svg(scene: &Scene, palette: Option<&Palette>) -> String {
    let theme = Theme::new(palette.cloned().unwrap_or_default());
    graphlay::svg::render(scene, &theme, &HeuristicMeasurer::new())
}

/// Checks that `layout` was made from a graph of `nodes` nodes and `edges` edges.
pub(crate) fn check_layout(
    layout: &Layout,
    nodes: usize,
    edges: usize,
) -> Result<(), VisualizationError> {
    if layout.nodes.len() == nodes && layout.edges.len() == edges {
        return Ok(());
    }
    Err(VisualizationError::LayoutMismatch {
        nodes,
        edges,
        layout_nodes: layout.nodes.len(),
        layout_edges: layout.edges.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphlay::{Edge, Node, RoutingStyle, Size};
    use serde_json::json;

    fn two_boxes() -> Graph {
        let node = Node {
            size: Size::new(80.0, 40.0),
            ..Node::default()
        };
        Graph {
            nodes: vec![node.clone(), node],
            edges: vec![Edge::new(graphlay::NodeId(0), graphlay::NodeId(1))],
        }
    }

    #[test]
    fn the_callers_patch_goes_over_the_models_own() {
        let model = json!({"direction": "leftright", "routing": {"lane_gap": 3.0}});
        let user = json!({"routing": {"style": "diagonal"}});
        let request = layered_request(two_boxes(), Preset::ProcessMap, model, &user).unwrap();
        let options = layout_options_of(&request).unwrap();
        assert_eq!(options.direction, graphlay::Direction::LeftRight);
        assert_eq!(options.routing.style, RoutingStyle::Diagonal);
        assert_eq!(options.routing.lane_gap, 3.0);
        assert_eq!(
            options.spacing,
            LayoutOptions::process_map().spacing,
            "untouched fields keep the preset's value"
        );
    }

    #[test]
    fn an_invalid_patch_is_an_error_naming_the_field() {
        let user = json!({"routing": {"style": "curvy"}});
        let err = layered_request(two_boxes(), Preset::Layered, json!({}), &user).unwrap_err();
        assert!(
            matches!(&err, VisualizationError::InvalidOptions(m) if m.starts_with("options.routing.style")),
            "{err}"
        );
        let err =
            tree_request(two_boxes(), TreeOptions::default(), &json!({"row_gp": 1})).unwrap_err();
        assert!(err.to_string().contains("row_gp"), "{err}");
    }

    #[test]
    fn a_reroute_keeps_the_given_positions() {
        let request =
            layered_request(two_boxes(), Preset::Layered, json!({}), &Value::Null).unwrap();
        let first = layout_graph(&request).unwrap();
        let mut positions: Vec<Point> = first.nodes.iter().map(|n| n.center).collect();
        positions[1].y += 200.0;
        let moved = reroute_graph(&request, &positions).unwrap();
        assert_eq!(moved.nodes[1].center, positions[1]);
        assert!(matches!(
            reroute_graph(&request, &positions[..1]),
            Err(VisualizationError::PositionCount {
                nodes: 2,
                positions: 1
            })
        ));
        assert!(matches!(
            reroute_graph(&request, &[]),
            Err(VisualizationError::Layout(
                LayoutError::MissingPosition { .. }
            ))
        ));
    }

    #[test]
    fn a_tree_request_patches_the_models_tree_options() {
        let options = TreeOptions {
            row_gap: 10.0,
            ..TreeOptions::default()
        };
        let request = tree_request(two_boxes(), options, &json!({"sibling_gap": 5.0})).unwrap();
        let resolved = tree_options_of(&request).unwrap();
        assert_eq!((resolved.row_gap, resolved.sibling_gap), (10.0, 5.0));
        let layout = layout_tree(&request).unwrap();
        assert_eq!(layout.nodes.len(), 2);
    }

    #[test]
    fn a_layout_of_another_graph_is_rejected() {
        let request =
            layered_request(two_boxes(), Preset::Layered, json!({}), &Value::Null).unwrap();
        let layout = layout_graph(&request).unwrap();
        assert!(check_layout(&layout, 2, 1).is_ok());
        let err = check_layout(&layout, 3, 1).unwrap_err();
        assert!(err.to_string().contains("3 nodes"), "{err}");
    }
}
