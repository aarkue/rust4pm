//! Laying out and drawing a [`PetriNet`], with initial tokens as dots and final ones as squares.
//!
//! Markings do not change the layout, so the graph and the layout take a [`PetriNet`], and the
//! drawing takes an [`AcceptingPetriNet`] to show its tokens.

use graphlay::{Direction, Edge, Graph, LayoutError, LayoutOptions, Node, Shape};
#[cfg(feature = "bindings")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::process_models::petri_net::{AcceptingPetriNet, Arc, ArcType, NodeID, PetriNet};
use crate::visualization::graph_build::{
    edge_label_size, GraphAcc, LABELED_TRANSITION_SIZE, PLACE_SIZE, SILENT_TRANSITION_SIZE,
};
use crate::visualization::styling::BaseStyle;

/// How a [`PetriNet`] is turned into a [`Graph`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(JsonSchema))]
#[serde(default)]
pub struct PetriBuildOptions {
    /// Flow direction. `None` means left to right.
    pub direction: Option<Direction>,
}

impl PetriBuildOptions {
    /// The engine options for these build options.
    pub fn layout_options(&self) -> LayoutOptions {
        let mut options = LayoutOptions::layered_default();
        if let Some(direction) = self.direction {
            options.direction = direction;
        }
        options
    }
}

/// Colors a [`PetriNet`] drawing uses.
#[derive(Debug, Clone, PartialEq)]
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

/// The [`Graph`] of `net`, the node behind each graph node, and the arc behind each edge.
pub fn petri_net_to_graph(
    net: &PetriNet,
    options: &PetriBuildOptions,
) -> (Graph, Vec<NodeID>, Vec<Arc>) {
    // The direction only affects the engine, not the graph.
    let _ = options;
    let mut acc: GraphAcc<NodeID, Arc> = GraphAcc::new();
    let places: Vec<_> = net
        .places()
        .map(|(id, _)| {
            let node = Node {
                size: PLACE_SIZE,
                shape: Shape::Ellipse,
                ..Node::default()
            };
            acc.add_node(format!("p:{}", id.index()), node, || NodeID::Place(id))
        })
        .collect();
    let transitions: Vec<_> = net
        .transitions()
        .map(|(id, t)| {
            let node = Node {
                size: if t.label.is_none() {
                    SILENT_TRANSITION_SIZE
                } else {
                    LABELED_TRANSITION_SIZE
                },
                shape: Shape::Box,
                ..Node::default()
            };
            acc.add_node(format!("t:{}", id.index()), node, || NodeID::Transition(id))
        })
        .collect();
    for arc in net.arcs() {
        let (source, target) = match arc.from_to {
            ArcType::PlaceTransition(p, t) => (places[p.index()], transitions[t.index()]),
            ArcType::TransitionPlace(t, p) => (transitions[t.index()], places[p.index()]),
        };
        let edge = Edge {
            label_size: arc_weight_label(arc.weight).map(|text| edge_label_size(&text)),
            ..Edge::new(source, target)
        };
        acc.add_edge(edge, arc);
    }
    acc.finish()
}

/// Lay `net` out.
///
/// # Errors
///
/// [`LayoutError`] from the engine.
pub fn layout_petri_net(
    net: &PetriNet,
    options: &PetriBuildOptions,
) -> Result<graphlay::Layout, LayoutError> {
    let (graph, _, _) = petri_net_to_graph(net, options);
    graphlay::layout(&graph, &options.layout_options())
}

/// Shared by the graph and the scene, so the layout reserves room for exactly the drawn labels.
fn arc_weight_label(weight: u32) -> Option<String> {
    (weight != 1).then(|| weight.to_string())
}

mod draw {
    use super::*;
    use graphlay::svg::{
        FitLayout, Glyph, GlyphCluster, GlyphLayout, GlyphRow, NodeContent, NumeralStyle, Scene,
        Stroke,
    };
    use graphlay::Layout;

    use crate::visualization::styling::{edge_label, node_label, scene, scene_edge, scene_node};

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

    /// The drawing of `net` over an existing [`Layout`].
    pub fn petri_net_scene(
        net: &AcceptingPetriNet,
        layout: &Layout,
        nodes: &[NodeID],
        edges: &[Arc],
        style: &PetriStyle,
    ) -> Scene {
        let base = &style.base;
        let mut scene_nodes = Vec::with_capacity(nodes.len());
        for (placement, node) in layout.nodes.iter().zip(nodes) {
            match *node {
                NodeID::Place(place) => {
                    let mut scene_node = scene_node(
                        placement,
                        Shape::Ellipse,
                        Some(base.node_fill.clone()),
                        Some(Stroke::new(&base.node_border, base.border_width)),
                        base,
                    );
                    let initial = net.initial_marking.tokens(place);
                    let final_ = net.final_marking.tokens(place);
                    if initial > 0 || final_ > 0 {
                        scene_node.content.push(tokens(
                            initial,
                            final_,
                            placement.size.width,
                            style,
                        ));
                    }
                    scene_nodes.push(scene_node);
                }
                NodeID::Transition(transition) => {
                    let label = &net.net.transition(transition).label;
                    let silent = label.is_none();
                    let mut scene_node = scene_node(
                        placement,
                        Shape::Box,
                        Some(if silent {
                            style.silent_fill.clone()
                        } else {
                            base.node_fill.clone()
                        }),
                        Some(Stroke::new(&base.node_border, base.border_width)),
                        base,
                    );
                    if let Some(label) = label {
                        scene_node
                            .content
                            .push(node_label(label, placement.size.width, 2, base));
                    }
                    scene_nodes.push(scene_node);
                }
            }
        }

        let mut scene_edges = Vec::with_capacity(layout.edges.len());
        for (route, arc) in layout.edges.iter().zip(edges) {
            let Some(mut edge) = scene_edge(route, &base.edge, 2.0) else {
                continue;
            };
            if let Some(text) = arc_weight_label(arc.weight) {
                edge.labels.push(edge_label(&text, route, base));
            }
            scene_edges.push(edge);
        }
        scene(base, scene_nodes, scene_edges)
    }

    /// Lay `net` out and draw it, as one standalone SVG document.
    ///
    /// # Errors
    ///
    /// As [`layout_petri_net`].
    pub fn export_petri_net_svg(
        net: &AcceptingPetriNet,
        options: &PetriBuildOptions,
    ) -> Result<String, LayoutError> {
        let (graph, nodes, edges) = petri_net_to_graph(&net.net, options);
        let layout = graphlay::layout(&graph, &options.layout_options())?;
        Ok(crate::visualization::styling::render_scene(
            &petri_net_scene(net, &layout, &nodes, &edges, &PetriStyle::default()),
        ))
    }
}

pub use draw::{export_petri_net_svg, petri_net_scene};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::process_models::petri_net::Marking;
    use crate::test_utils::{assert_xml_well_formed, get_test_data_path};
    use crate::Importable;

    #[test]
    fn every_place_transition_and_arc_is_drawn_once() {
        let path = get_test_data_path().join("petri-net").join("pn.pnml");
        let net = PetriNet::import_from_path(path).unwrap();
        let (graph, nodes, arcs) = petri_net_to_graph(&net, &PetriBuildOptions::default());
        assert_eq!(nodes, net.nodes().collect::<Vec<_>>());
        assert_eq!(graph.nodes.len(), nodes.len());
        assert_eq!(arcs, net.arcs().collect::<Vec<_>>());
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
            let svg = export_petri_net_svg(&net, &PetriBuildOptions::default()).unwrap();
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
}
