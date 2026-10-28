//! Laying out and drawing an [`AcceptingPetriNet`], with initial tokens as dots and final ones as
//! squares. Markings do not change the layout.

use graphlay::json::LayoutRequest;
use graphlay::{Edge, Graph, Layout, Node, Preset, Shape, Size};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::core::process_models::petri_net::{
    AcceptingPetriNet, Arc, ArcType, NodeID, PlaceID, TransitionID,
};
use crate::visualization::annotation::{
    edge_text, edge_thickness, edge_weight, node_text, EdgeAnnotation,
};
use crate::visualization::engine::VisualizationError;
use crate::visualization::graph_build::{
    edge_label_size_with, GraphAcc, LabelWidth, LABELED_TRANSITION_SIZE, PLACE_SIZE,
    SILENT_TRANSITION_SIZE,
};
use crate::visualization::model::{Annotations, LayoutSettings, ModelLayoutRequest, Visualize};
use crate::visualization::styling::BaseStyle;

/// A place or transition of a Petri net drawing, e.g., `{"kind": "place", "index": 3}`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "kind", content = "index", rename_all = "snake_case")]
pub enum PetriNodeRef {
    /// A place, by its id.
    Place(PlaceID),
    /// A transition, by its id.
    Transition(TransitionID),
}

impl From<NodeID> for PetriNodeRef {
    fn from(node: NodeID) -> Self {
        match node {
            NodeID::Place(p) => PetriNodeRef::Place(p),
            NodeID::Transition(t) => PetriNodeRef::Transition(t),
        }
    }
}

impl From<PetriNodeRef> for NodeID {
    fn from(node: PetriNodeRef) -> Self {
        match node {
            PetriNodeRef::Place(p) => NodeID::Place(p),
            PetriNodeRef::Transition(t) => NodeID::Transition(t),
        }
    }
}

/// An arc of a Petri net drawing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct PetriArcRef {
    /// Where the arc starts.
    pub source: PetriNodeRef,
    /// Where the arc ends.
    pub target: PetriNodeRef,
    /// How many tokens the arc moves.
    pub weight: u32,
}

impl From<Arc> for PetriArcRef {
    fn from(arc: Arc) -> Self {
        let (source, target) = match arc.from_to {
            ArcType::PlaceTransition(p, t) => (PetriNodeRef::Place(p), PetriNodeRef::Transition(t)),
            ArcType::TransitionPlace(t, p) => (PetriNodeRef::Transition(t), PetriNodeRef::Place(p)),
        };
        PetriArcRef {
            source,
            target,
            weight: arc.weight,
        }
    }
}

/// Labels, layout weights and line widths of a Petri net drawing, e.g., durations on the arcs.
/// A transition widens to fit its line, and a place keeps its size. Arcs are keyed by source and
/// target.
pub type PetriAnnotations = Annotations<PetriNodeRef, (PetriNodeRef, PetriNodeRef)>;

/// How a Petri net is turned into a [`Graph`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(JsonSchema))]
#[serde(default)]
pub struct PetriBuildOptions {
    /// Direction (left to right by default) and engine patch over the layered preset.
    #[serde(flatten)]
    pub layout: LayoutSettings,
    /// Labels, layout weights and line widths by node and arc. An arc's label replaces its
    /// weight, and a node's is one more line.
    pub annotations: PetriAnnotations,
}

/// The layout request of a Petri net.
pub type PetriLayoutRequest = ModelLayoutRequest<LayoutRequest, PetriNodeRef, PetriArcRef>;

/// Colors a Petri net drawing uses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct PetriStyle {
    /// Colors and paddings shared with every other model.
    pub base: BaseStyle,
    /// Fill of a silent transition.
    pub silent_fill: String,
    /// Color of the tokens and their counts.
    pub token: String,
}

impl Default for PetriStyle {
    fn default() -> Self {
        let base = BaseStyle::default();
        PetriStyle {
            silent_fill: "#111827".to_string(),
            token: base.text.clone(),
            base,
        }
    }
}

/// A transition box, widened to fit its annotated line.
pub(crate) fn transition_size(labeled: bool, note: Option<&str>, measure: LabelWidth<'_>) -> Size {
    let size = if labeled {
        LABELED_TRANSITION_SIZE
    } else {
        SILENT_TRANSITION_SIZE
    };
    match note {
        Some(note) => Size::new(size.width.max(measure(note)), size.height),
        None => size,
    }
}

/// An arc of `width` with room for its [`arc_text`], unless `annotation` changes them.
pub(crate) fn arc_edge(
    source: graphlay::NodeId,
    target: graphlay::NodeId,
    weight: u32,
    width: f64,
    annotation: Option<&EdgeAnnotation>,
    measure: LabelWidth<'_>,
) -> Edge {
    Edge {
        weight: edge_weight(annotation, 1.0),
        thickness: edge_thickness(annotation, width),
        label_size: arc_text(weight, annotation).map(|text| edge_label_size_with(&text, measure)),
        ..Edge::new(source, target)
    }
}

/// Width of an arc's line, and the room the layout keeps for it.
pub(crate) const ARC_WIDTH: f64 = 2.0;

/// Shared by the graph and the scene, so the layout reserves room for exactly the drawn labels:
/// the annotation's label, or the weight unless it is 1.
pub(crate) fn arc_text(weight: u32, annotation: Option<&EdgeAnnotation>) -> Option<String> {
    edge_text(annotation, || (weight != 1).then(|| weight.to_string()))
}

mod draw {
    use super::*;
    use graphlay::layout::{EdgeRoute, Placement};
    use graphlay::svg::{
        FitLayout, Glyph, GlyphCluster, GlyphLayout, GlyphRow, NodeContent, NumeralStyle,
        SceneEdge, SceneNode, Stroke,
    };

    use crate::visualization::styling::{
        edge_label, node_label, push_extra_line, scene_edge, scene_node,
    };

    /// More tokens in a place than this show as counts. About four is what reads at a glance.
    const MAX_TOKEN_GLYPHS: u64 = 4;

    fn tokens(initial: u32, final_: u32, width: f64, style: &PetriStyle) -> NodeContent {
        let cluster = |glyph: Glyph, count: u32| GlyphCluster {
            glyph,
            count: u64::from(count),
            color: Some(style.token.clone()),
            ..GlyphCluster::default()
        };
        NodeContent::Glyphs(GlyphRow {
            clusters: vec![
                cluster(Glyph::Dot, initial),
                cluster(
                    Glyph::Square {
                        scale: 0.85,
                        corner_radius: 1.5,
                        opacity: 1.0,
                    },
                    final_,
                ),
            ],
            layout: GlyphLayout::Fit(FitLayout {
                max_width: width - 14.0,
                max_glyphs: Some(MAX_TOKEN_GLYPHS),
                numeral: Some(NumeralStyle {
                    size: style.base.label_size,
                    weight: 600.0,
                    color: Some(style.token.clone()),
                    counted: true,
                }),
                ..FitLayout::default()
            }),
            ..GlyphRow::default()
        })
    }

    /// A place with its initial tokens as dots and its final ones as squares.
    pub(crate) fn place_node(
        placement: &Placement,
        initial: u32,
        final_: u32,
        style: &PetriStyle,
    ) -> SceneNode {
        let base = &style.base;
        let mut node = scene_node(
            placement,
            Shape::Ellipse,
            Some(base.node_fill.clone()),
            Some(Stroke::new(&base.node_border, base.border_width)),
            base,
        );
        if initial > 0 || final_ > 0 {
            node.content
                .push(tokens(initial, final_, placement.size.width, style));
        }
        node
    }

    /// A transition with its label, or a filled bar if it is silent.
    pub(crate) fn transition_node(
        placement: &Placement,
        label: Option<&str>,
        style: &PetriStyle,
    ) -> SceneNode {
        let base = &style.base;
        let fill = match label {
            None => style.silent_fill.clone(),
            Some(_) => base.node_fill.clone(),
        };
        let mut node = scene_node(
            placement,
            Shape::Box,
            Some(fill),
            Some(Stroke::new(&base.node_border, base.border_width)),
            base,
        );
        if let Some(label) = label {
            node.content
                .push(node_label(label, placement.size.width, 2, base));
        }
        node
    }

    impl Visualize for AcceptingPetriNet {
        type NodeRef = PetriNodeRef;
        type EdgeRef = PetriArcRef;
        type Options = PetriBuildOptions;
        type Style = PetriStyle;
        type Request = LayoutRequest;
        type Context = ();

        fn to_graph(
            &self,
            options: &PetriBuildOptions,
            measure: LabelWidth<'_>,
        ) -> (Graph, Vec<PetriNodeRef>, Vec<PetriArcRef>) {
            let net = &self.net;
            let mut acc: GraphAcc<PetriNodeRef, PetriArcRef> = GraphAcc::new();
            let places: Vec<_> = net
                .places()
                .map(|(id, _)| {
                    let node = Node {
                        size: PLACE_SIZE,
                        shape: Shape::Ellipse,
                        ..Node::default()
                    };
                    acc.add_node(format!("p:{}", id.index()), node, || {
                        PetriNodeRef::Place(id)
                    })
                })
                .collect();
            let transitions: Vec<_> = net
                .transitions()
                .map(|(id, t)| {
                    let meta = PetriNodeRef::Transition(id);
                    let note = node_text(options.annotations.nodes.get(&meta));
                    let node = Node {
                        size: transition_size(t.label.is_some(), note, measure),
                        shape: Shape::Box,
                        ..Node::default()
                    };
                    acc.add_node(format!("t:{}", id.index()), node, || meta)
                })
                .collect();
            for arc in net.arcs() {
                let (source, target) = match arc.from_to {
                    ArcType::PlaceTransition(p, t) => (places[p.index()], transitions[t.index()]),
                    ArcType::TransitionPlace(t, p) => (transitions[t.index()], places[p.index()]),
                };
                let arc = PetriArcRef::from(arc);
                let annotation = options.annotations.edges.get(&(arc.source, arc.target));
                let edge = arc_edge(source, target, arc.weight, ARC_WIDTH, annotation, measure);
                acc.add_edge(edge, arc);
            }
            acc.finish()
        }

        fn request(
            graph: Graph,
            options: &PetriBuildOptions,
        ) -> Result<LayoutRequest, VisualizationError> {
            options
                .layout
                .layered(graph, Preset::Layered, None, Value::Null)
        }

        fn base(style: &PetriStyle) -> &BaseStyle {
            &style.base
        }

        fn context(
            &self,
            _layout: &Layout,
            _nodes: &[PetriNodeRef],
            _options: &PetriBuildOptions,
        ) -> Result<(), VisualizationError> {
            Ok(())
        }

        fn draw_node(
            &self,
            node: &PetriNodeRef,
            placement: &Placement,
            _context: &(),
            options: &PetriBuildOptions,
            style: &PetriStyle,
        ) -> SceneNode {
            let mut n = match *node {
                PetriNodeRef::Place(place) => place_node(
                    placement,
                    self.initial_marking.tokens(place),
                    self.final_marking.tokens(place),
                    style,
                ),
                PetriNodeRef::Transition(transition) => transition_node(
                    placement,
                    self.net.transition(transition).label.as_deref(),
                    style,
                ),
            };
            if let Some(note) = node_text(options.annotations.nodes.get(node)) {
                push_extra_line(&mut n, note, &style.base);
            }
            n
        }

        fn draw_edge(
            &self,
            arc: &PetriArcRef,
            graph_edge: &Edge,
            route: &EdgeRoute,
            _context: &(),
            options: &PetriBuildOptions,
            style: &PetriStyle,
        ) -> Option<SceneEdge> {
            let base = &style.base;
            let mut e = scene_edge(route, &base.edge, graph_edge.thickness)?;
            let annotation = options.annotations.edges.get(&(arc.source, arc.target));
            if let Some(text) = arc_text(arc.weight, annotation) {
                e.labels.push(edge_label(&text, route, base));
            }
            Some(e)
        }
    }
}

pub(crate) use draw::{place_node, transition_node};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::process_models::petri_net::Marking;
    use crate::test_utils::{assert_xml_well_formed, get_test_data_path};
    use crate::visualization::graph_build::heuristic_label_width;
    use crate::visualization::model::{self, Visualize};
    use crate::visualization::StyleOverrides;
    use crate::{Importable, PetriNet};

    fn accepting(net: PetriNet) -> AcceptingPetriNet {
        AcceptingPetriNet::new(net, Marking::new(), Marking::new())
    }

    #[test]
    fn every_place_transition_and_arc_is_drawn_once() {
        let path = get_test_data_path().join("petri-net").join("pn.pnml");
        let net = AcceptingPetriNet::import_from_path(path).unwrap();
        let (graph, nodes, arcs) =
            net.to_graph(&PetriBuildOptions::default(), &heuristic_label_width);
        let ids: Vec<PetriNodeRef> = net.net.nodes().map(PetriNodeRef::from).collect();
        assert_eq!(nodes, ids);
        assert_eq!(graph.nodes.len(), nodes.len());
        let net_arcs: Vec<PetriArcRef> = net.net.arcs().map(PetriArcRef::from).collect();
        assert_eq!(arcs, net_arcs);
    }

    #[test]
    fn the_layout_request_names_places_and_transitions_by_index() {
        use crate::visualization::engine::{layout_graph, layout_options_of};
        let path = get_test_data_path().join("petri-net").join("pn.pnml");
        let net = AcceptingPetriNet::import_from_path(path).unwrap();
        let options = PetriBuildOptions::default();
        let request = model::layout_request(&net, &options).unwrap();
        assert_eq!(
            layout_options_of(&request.request).unwrap(),
            graphlay::LayoutOptions::layered_default()
        );
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(
            json["nodes"][0],
            serde_json::json!({"kind": "place", "index": 0})
        );
        let back: PetriLayoutRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back.nodes, request.nodes);
        assert_eq!(back.edges, request.edges);
        for (edge, arc) in request.request.graph.edges.iter().zip(&request.edges) {
            assert_eq!(request.nodes[edge.source.index()], arc.source);
            assert_eq!(request.nodes[edge.target.index()], arc.target);
        }

        let layout = layout_graph(&request.request).unwrap();
        assert_eq!(layout, model::layout(&net, &options).unwrap());
    }

    /// The aggregated alignment view: transitions colored by deviation rate, with the rate as an
    /// extra line, and the transitions that never fired dimmed.
    #[test]
    fn a_heatmap_overlay_restyles_only_the_given_transitions() {
        use crate::visualization::styling::ElementStyle;
        use graphlay::svg::{NodeContent, Paint};
        let path = get_test_data_path().join("petri-net").join("pn.pnml");
        let net = AcceptingPetriNet::import_from_path(path).unwrap();
        let options = PetriBuildOptions::default();
        let request = model::layout_request(&net, &options).unwrap();
        let layout = model::layout(&net, &options).unwrap();
        let transitions: Vec<usize> = (0..request.nodes.len())
            .filter(|&i| matches!(request.nodes[i], PetriNodeRef::Transition(_)))
            .collect();
        let (hot, unfired) = (transitions[0], transitions[1]);
        let mut overrides = StyleOverrides::default();
        overrides.nodes.insert(
            hot,
            ElementStyle {
                fill: Some("#dc26262e".to_string()),
                stroke: Some("#dc2626".to_string()),
                stroke_width: Some(3.0),
                extra_label: Some("85%".to_string()),
                ..ElementStyle::default()
            },
        );
        overrides.nodes.insert(
            unfired,
            ElementStyle {
                opacity: Some(0.35),
                ..ElementStyle::default()
            },
        );
        let style = PetriStyle::default();
        let scene = model::scene_from_layout(&net, &layout, &options, &style, &overrides).unwrap();
        let plain =
            model::scene_from_layout(&net, &layout, &options, &style, &StyleOverrides::default())
                .unwrap();
        let stroke = scene.nodes[hot].stroke.as_ref().unwrap();
        assert_eq!(
            (&stroke.paint, stroke.width),
            (&Paint::color("#dc2626"), 3.0)
        );
        let texts: Vec<&str> = scene.nodes[hot]
            .content
            .iter()
            .filter_map(|c| match c {
                NodeContent::Text(run) => Some(run.text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(texts.last(), Some(&"85%"));
        assert_eq!(scene.nodes[unfired].attrs.extra[0].1, "0.35");
        for i in (0..scene.nodes.len()).filter(|i| ![hot, unfired].contains(i)) {
            assert_eq!(scene.nodes[i].fill, plain.nodes[i].fill);
            assert_eq!(scene.nodes[i].stroke, plain.nodes[i].stroke);
        }
    }

    #[test]
    fn annotations_label_weight_and_widen_arcs_and_add_a_node_line() {
        use crate::visualization::engine::layout_graph;
        use crate::visualization::graph_build::edge_label_size;
        use crate::visualization::{EdgeAnnotation, NodeAnnotation};
        use graphlay::svg::NodeContent;
        let path = get_test_data_path().join("petri-net").join("pn.pnml");
        let net = AcceptingPetriNet::import_from_path(path).unwrap();
        let plain = model::layout_request(&net, &PetriBuildOptions::default()).unwrap();
        let arc = plain.edges[0];
        let transition = plain
            .nodes
            .iter()
            .position(|n| matches!(n, PetriNodeRef::Transition(_)))
            .unwrap();
        let mut options = PetriBuildOptions::default();
        options.annotations.edges.insert(
            (arc.source, arc.target),
            EdgeAnnotation {
                label: Some("4 h".to_string()),
                weight: Some(5.0),
                thickness: Some(6.0),
            },
        );
        options.annotations.nodes.insert(
            plain.nodes[transition],
            NodeAnnotation {
                label: Some("85%".to_string()),
            },
        );
        let back: PetriBuildOptions =
            serde_json::from_str(&serde_json::to_string(&options).unwrap()).unwrap();
        assert_eq!(back, options);

        let request = model::layout_request(&net, &options).unwrap();
        let edge = &request.request.graph.edges[0];
        assert_eq!(edge.label_size, Some(edge_label_size("4 h")));
        assert_eq!((edge.weight, edge.thickness), (5.0, 6.0));
        assert_eq!(
            request.request.graph.edges[1..],
            plain.request.graph.edges[1..]
        );
        assert_eq!(request.request.graph.nodes, plain.request.graph.nodes);

        let layout = layout_graph(&request.request).unwrap();
        let style = PetriStyle::default();
        let overrides = StyleOverrides::default();
        let scene = model::scene_from_layout(&net, &layout, &options, &style, &overrides).unwrap();
        assert_eq!(scene.edges[0].labels[0].text.text, "4 h");
        assert_eq!(scene.edges[0].stroke.width, 6.0);
        assert!(scene.edges[1..].iter().all(|e| e.stroke.width == ARC_WIDTH));
        let last = scene.nodes[transition].content.last();
        assert!(matches!(last, Some(NodeContent::Text(run)) if run.text == "85%"));
    }

    /// `(initial, final, dots, squares, counts)`. Past four tokens, each kind shows once with
    /// its count.
    #[test]
    fn tokens_are_dots_for_the_initial_and_squares_for_the_final_marking() {
        for (initial, final_, dots, squares, counts) in [
            (0, 2, 0, 2, &[][..]),
            (2, 1, 2, 1, &[]),
            (12, 132, 1, 1, &["\u{d7}12", "\u{d7}132"]),
        ] {
            let mut net = PetriNet::new();
            let p = net.add_place(None);
            let net = AcceptingPetriNet::new(
                net,
                Marking::from([(p, initial)]),
                Marking::from([(p, final_)]),
            );
            let svg =
                model::svg(&net, &PetriBuildOptions::default(), &PetriStyle::default()).unwrap();
            assert_xml_well_formed(&svg);
            // The place itself is an ellipse and the background a rect.
            assert_eq!(svg.matches("<ellipse").count(), 1);
            assert_eq!(svg.matches("<circle").count(), dots, "{svg}");
            assert_eq!(svg.matches("<rect").count(), 1 + squares, "{svg}");
            for count in counts {
                assert!(svg.contains(&format!(">{count}</text>")), "{svg}");
            }
        }
    }

    /// The meta tables hand out the same refs as the layout request, and a scene of a layout of
    /// another net is an error instead of a panic.
    #[test]
    fn the_meta_tables_are_the_requests_refs_and_a_mismatched_layout_is_an_error() {
        let path = get_test_data_path().join("petri-net").join("pn.pnml");
        let net = AcceptingPetriNet::import_from_path(path).unwrap();
        let options = PetriBuildOptions::default();
        let (_, nodes, edges) = net.to_graph(&options, &heuristic_label_width);
        let request = model::layout_request(&net, &options).unwrap();
        assert_eq!((nodes, edges), (request.nodes, request.edges));

        let mut small = PetriNet::new();
        small.add_place(None);
        let layout = model::layout(&accepting(small), &options).unwrap();
        let style = PetriStyle::default();
        let overrides = StyleOverrides::default();
        let err = model::scene_from_layout(&net, &layout, &options, &style, &overrides);
        assert!(matches!(
            err,
            Err(VisualizationError::LayoutMismatch { .. })
        ));
    }

    #[test]
    fn a_transition_widens_to_fit_its_annotated_line() {
        use crate::visualization::NodeAnnotation;
        let mut net = PetriNet::new();
        let labeled = net.add_transition(Some("pay".to_string()), None);
        let silent = net.add_transition(None, None);
        let note = "avg 12.5 hours, 85% on time";
        let mut options = PetriBuildOptions::default();
        for t in [labeled, silent] {
            options.annotations.nodes.insert(
                PetriNodeRef::Transition(t),
                NodeAnnotation {
                    label: Some(note.to_string()),
                },
            );
        }
        let net = accepting(net);
        let (graph, _, _) = net.to_graph(&options, &heuristic_label_width);
        for node in &graph.nodes {
            assert_eq!(node.size.width, heuristic_label_width(note));
        }
        let (plain, _, _) = net.to_graph(&PetriBuildOptions::default(), &heuristic_label_width);
        assert_eq!(plain.nodes[0].size, LABELED_TRANSITION_SIZE);
        assert_eq!(plain.nodes[1].size, SILENT_TRANSITION_SIZE);
    }
}
