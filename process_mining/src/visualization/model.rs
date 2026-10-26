//! The pipeline every model shares: its graph, the layout request, the layout, the drawing over a
//! layout, and the SVG.

use std::collections::BTreeMap;

use graphlay::json::{LayoutRequest, TreeRequest};
use graphlay::layout::{EdgeRoute, Placement};
use graphlay::svg::{DrawOrder, Scene, SceneEdge, SceneNode};
use graphlay::{Direction, Edge, Graph, Layout, Preset};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::serde_as;

use crate::visualization::annotation::{EdgeAnnotation, NodeAnnotation};
use crate::visualization::engine::{
    check_layout, direction_patch, layered_request, layout_graph, layout_tree, merge_patch,
    render_scene_svg, VisualizationError,
};
use crate::visualization::graph_build::{heuristic_label_width, LabelWidth};
use crate::visualization::styling::{
    scene, style_edge_at, style_node_at, BaseStyle, StyleOverrides,
};

/// A process model that can be laid out and drawn.
///
/// The model turns itself into a [`Graph`], with the model element behind each graph node and
/// edge, and draws single nodes and edges. Everything else, from the layout request to the SVG, is
/// the same for every model: [`layout_request`], [`layout`], [`scene_from_layout`] and [`svg`].
pub trait Visualize {
    /// The model element behind a graph node.
    type NodeRef: Clone;
    /// The model element behind a graph edge.
    type EdgeRef: Clone;
    /// How the model is turned into a graph and laid out.
    type Options: Default;
    /// How the model is drawn.
    type Style: Default;
    /// The engine request, [`LayoutRequest`] or [`TreeRequest`].
    type Request: EngineRequest;
    /// What drawing a node or edge needs from the whole layout, e.g., the rows of a tree.
    type Context;

    /// The graph, the node behind each graph node and the edge behind each graph edge. `measure`
    /// gives the drawn width of a label.
    fn to_graph(
        &self,
        options: &Self::Options,
        measure: LabelWidth<'_>,
    ) -> (Graph, Vec<Self::NodeRef>, Vec<Self::EdgeRef>);

    /// The engine request for the model's graph.
    ///
    /// # Errors
    ///
    /// [`VisualizationError::InvalidOptions`] if the options do not describe valid engine options.
    fn request(graph: Graph, options: &Self::Options) -> Result<Self::Request, VisualizationError>;

    /// The style shared by all models.
    fn base(style: &Self::Style) -> &BaseStyle;

    /// The [`Visualize::Context`] of a drawing over `layout`.
    ///
    /// # Errors
    ///
    /// [`VisualizationError::InvalidOptions`] if the options do not describe valid engine options.
    fn context(
        &self,
        layout: &Layout,
        nodes: &[Self::NodeRef],
        options: &Self::Options,
    ) -> Result<Self::Context, VisualizationError>;

    /// `node` at `placement`.
    fn draw_node(
        &self,
        node: &Self::NodeRef,
        placement: &Placement,
        context: &Self::Context,
        options: &Self::Options,
        style: &Self::Style,
    ) -> SceneNode;

    /// Whether edges are drawn over nodes or the other way round.
    fn draw_order() -> DrawOrder {
        DrawOrder::default()
    }

    /// `edge` along `route`, or `None` if the route has no line. `graph_edge` is the edge the layout
    /// was made for, e.g., for its line width.
    fn draw_edge(
        &self,
        edge: &Self::EdgeRef,
        graph_edge: &Edge,
        route: &EdgeRoute,
        context: &Self::Context,
        options: &Self::Options,
        style: &Self::Style,
    ) -> Option<SceneEdge>;
}

/// A request one of graphlay's engines lays out.
pub trait EngineRequest {
    /// Runs the engine.
    ///
    /// # Errors
    ///
    /// A [`graphlay::LayoutError`] from the engine.
    fn lay_out(&self) -> Result<Layout, VisualizationError>;
}

impl EngineRequest for LayoutRequest {
    fn lay_out(&self) -> Result<Layout, VisualizationError> {
        layout_graph(self)
    }
}

impl EngineRequest for TreeRequest {
    fn lay_out(&self) -> Result<Layout, VisualizationError> {
        layout_tree(self)
    }
}

/// The layout request of a model, with the model element behind each of its nodes and edges:
/// `layout.nodes[i]` of its layout is where `nodes[i]` went.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ModelLayoutRequest<R, N, E> {
    /// The graph, preset and options patch, for the engine.
    pub request: R,
    /// The model element behind each graph node.
    pub nodes: Vec<N>,
    /// The model element behind each graph edge.
    pub edges: Vec<E>,
}

/// The [`ModelLayoutRequest`] of a model type.
pub type LayoutRequestOf<M> = ModelLayoutRequest<
    <M as Visualize>::Request,
    <M as Visualize>::NodeRef,
    <M as Visualize>::EdgeRef,
>;

/// The layout request of `model`.
///
/// # Errors
///
/// [`VisualizationError::InvalidOptions`] if the options do not describe valid engine options.
pub fn layout_request<M: Visualize + ?Sized>(
    model: &M,
    options: &M::Options,
) -> Result<LayoutRequestOf<M>, VisualizationError> {
    let (graph, nodes, edges) = model.to_graph(options, &heuristic_label_width);
    Ok(ModelLayoutRequest {
        request: M::request(graph, options)?,
        nodes,
        edges,
    })
}

/// Lays `model` out.
///
/// # Errors
///
/// Invalid options, or a [`graphlay::LayoutError`] from the engine.
pub fn layout<M: Visualize + ?Sized>(
    model: &M,
    options: &M::Options,
) -> Result<Layout, VisualizationError> {
    layout_request(model, options)?.request.lay_out()
}

/// The drawing of `model` over `layout`, e.g., one the user rearranged, with `overrides` applied.
///
/// # Errors
///
/// [`VisualizationError::LayoutMismatch`] if `layout` is not of `model`'s graph under `options`.
pub fn scene_from_layout<M: Visualize + ?Sized>(
    model: &M,
    layout: &Layout,
    options: &M::Options,
    style: &M::Style,
    overrides: &StyleOverrides,
) -> Result<Scene, VisualizationError> {
    let (graph, nodes, edges) = model.to_graph(options, &heuristic_label_width);
    check_layout(layout, nodes.len(), edges.len())?;
    let base = M::base(style);
    let context = model.context(layout, &nodes, options)?;
    let scene_nodes = layout
        .nodes
        .iter()
        .zip(&nodes)
        .enumerate()
        .map(|(i, (placement, node))| {
            let mut n = model.draw_node(node, placement, &context, options, style);
            style_node_at(&mut n, i, overrides, base);
            n
        })
        .collect();
    let scene_edges = layout
        .edges
        .iter()
        .zip(&graph.edges)
        .zip(&edges)
        .enumerate()
        .filter_map(|(i, ((route, graph_edge), edge))| {
            let mut e = model.draw_edge(edge, graph_edge, route, &context, options, style)?;
            style_edge_at(&mut e, i, overrides, base);
            Some(e)
        })
        .collect();
    let mut drawing = scene(base, scene_nodes, scene_edges);
    drawing.order = M::draw_order();
    Ok(drawing)
}

/// Lays `model` out and draws it in `style`, as a standalone SVG document.
///
/// # Errors
///
/// As [`layout`].
pub fn svg<M: Visualize + ?Sized>(
    model: &M,
    options: &M::Options,
    style: &M::Style,
) -> Result<String, VisualizationError> {
    let layout = layout(model, options)?;
    let scene = scene_from_layout(model, &layout, options, style, &StyleOverrides::default())?;
    Ok(render_scene_svg(&scene, None))
}

/// The direction and engine patch every model's options have, flattened into them in JSON.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(JsonSchema))]
#[serde(default)]
pub struct LayoutSettings {
    /// Flow direction. `None` is the model's own default.
    pub direction: Option<Direction>,
    /// Fields of the engine options to change, as a partial
    /// [`LayoutOptions`](graphlay::LayoutOptions) object deep-merged over the model's preset and
    /// the direction, e.g., `{"routing": {"style": "diagonal"}}`. `null` changes nothing.
    #[cfg_attr(
        feature = "bindings",
        schemars(with = "Option<graphlay::LayoutOptions>")
    )]
    pub layout_patch: Value,
}

impl LayoutSettings {
    /// A layered request for `graph`: `preset`, then the direction (`default` if unset), then
    /// `model_patch`, then [`LayoutSettings::layout_patch`].
    ///
    /// # Errors
    ///
    /// [`VisualizationError::InvalidOptions`] if the patches do not describe valid options.
    pub fn layered(
        &self,
        graph: Graph,
        preset: Preset,
        default: Option<Direction>,
        model_patch: Value,
    ) -> Result<LayoutRequest, VisualizationError> {
        let mut patch = direction_patch(self.direction.or(default));
        merge_patch(&mut patch, &model_patch);
        layered_request(graph, preset, patch, &self.layout_patch)
    }
}

/// Labels, layout weights and line widths by model element that replace the model's own, e.g.,
/// durations instead of counts. In JSON, both maps are lists of `[key, annotation]` pairs.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, bound(serialize = "N: Serialize + Ord, E: Serialize + Ord"))]
#[serde(bound(deserialize = "N: Deserialize<'de> + Ord, E: Deserialize<'de> + Ord"))]
pub struct Annotations<N, E> {
    /// By node, as one more line in the node.
    #[serde_as(as = "Vec<(_, _)>")]
    pub nodes: BTreeMap<N, NodeAnnotation>,
    /// By edge.
    #[serde_as(as = "Vec<(_, _)>")]
    pub edges: BTreeMap<E, EdgeAnnotation>,
}

impl<N, E> Default for Annotations<N, E> {
    fn default() -> Self {
        Annotations {
            nodes: BTreeMap::new(),
            edges: BTreeMap::new(),
        }
    }
}

/// The JSON form of [`Annotations`].
#[derive(JsonSchema)]
#[schemars(rename = "Annotations_for_{N}")]
#[allow(dead_code)]
struct AnnotationsJson<N, E> {
    /// By node, as one more line in the node.
    nodes: Vec<(N, NodeAnnotation)>,
    /// By edge.
    edges: Vec<(E, EdgeAnnotation)>,
}

impl<N: JsonSchema, E: JsonSchema> JsonSchema for Annotations<N, E> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        AnnotationsJson::<N, E>::schema_name()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        AnnotationsJson::<N, E>::json_schema(generator)
    }
}
