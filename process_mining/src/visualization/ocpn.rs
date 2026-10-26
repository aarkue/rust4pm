//! Laying out and drawing an object-centric Petri net in its
//! [flat JSON form](ObjectCentricPetriNetJson), as one Petri net of all its places and transitions.
//!
//! Places and arcs take their object type's color ([`key_color`] in the `"objectType"` scope), an
//! arc that of the place it connects. Variable arcs are bold and dashed. Transitions, silent
//! transitions and markings look as in a [`super::petri_net`] drawing.
//!
//! [`key_color`]: crate::visualization::key_color

use std::collections::{BTreeMap, HashMap};

use graphlay::json::LayoutRequest;
use graphlay::layout::{EdgeRoute, Placement};
use graphlay::svg::{SceneEdge, SceneNode, Stroke};
use graphlay::{Edge, Graph, Layout, Node, Preset, Shape};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::core::process_models::object_centric::ocpn::json::{
    JsonArc, JsonArcType, ObjectCentricPetriNetJson,
};
use crate::visualization::annotation::{annotated_thickness, node_text};
use crate::visualization::engine::VisualizationError;
use crate::visualization::graph_build::{GraphAcc, LabelWidth, PLACE_SIZE};
use crate::visualization::model::{Annotations, LayoutSettings, ModelLayoutRequest, Visualize};
use crate::visualization::petri_net::{
    arc_edge, arc_text, place_node, transition_node, transition_size, PetriStyle, ARC_WIDTH,
};
use crate::visualization::styling::{
    edge_label, key_color, lighten, push_extra_line, scene_edge, BaseStyle,
};

/// A place or transition of an object-centric Petri net drawing, by its index in the flat JSON's
/// `places` or `transitions`, e.g., `{"kind": "place", "index": 3}`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "kind", content = "index", rename_all = "snake_case")]
pub enum OcpnNodeRef {
    /// A place, by its index in `places`.
    Place(usize),
    /// A transition, by its index in `transitions`.
    Transition(usize),
}

/// An arc of an object-centric Petri net drawing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct OcpnArcRef {
    /// Index of the arc in the flat JSON's `arcs`.
    pub arc: usize,
    /// Where the arc starts.
    pub source: OcpnNodeRef,
    /// Where the arc ends.
    pub target: OcpnNodeRef,
}

/// Labels, layout weights and line widths of an object-centric Petri net drawing, e.g., durations
/// on the arcs. A transition widens to fit its line, and a place keeps its size. Arcs are keyed by
/// their index in the flat JSON's `arcs`.
pub type OcpnAnnotations = Annotations<OcpnNodeRef, usize>;

/// How an object-centric Petri net is turned into a [`Graph`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(JsonSchema))]
#[serde(default)]
pub struct OcpnBuildOptions {
    /// Direction (left to right by default) and engine patch over the layered preset.
    #[serde(flatten)]
    pub layout: LayoutSettings,
    /// Labels, layout weights and line widths by node and arc. An arc's label replaces its
    /// weight, and a node's is one more line.
    pub annotations: OcpnAnnotations,
}

/// The layout request of an object-centric Petri net.
pub type OcpnLayoutRequest = ModelLayoutRequest<LayoutRequest, OcpnNodeRef, OcpnArcRef>;

/// The room the layout keeps for a variable arc's line, the default
/// [`OcpnStyle::variable_arc_width`].
const VARIABLE_ARC_WIDTH: f64 = 4.0;

/// Colors and sizes an object-centric Petri net drawing uses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct OcpnStyle {
    /// Transitions, silent transitions, markings and the colors shared with every other model.
    pub petri: PetriStyle,
    /// How far towards white a place's fill is from its object type's color.
    pub place_fill_lightness: f64,
    /// Line width of a variable arc. The layout keeps room for the default width.
    pub variable_arc_width: f64,
    /// SVG dash pattern of a variable arc, e.g., `"6 3"`. Empty draws it solid.
    pub variable_arc_dash: String,
}

impl Default for OcpnStyle {
    fn default() -> Self {
        OcpnStyle {
            petri: PetriStyle::default(),
            place_fill_lightness: 0.85,
            variable_arc_width: VARIABLE_ARC_WIDTH,
            variable_arc_dash: "6 3".to_string(),
        }
    }
}

fn is_variable(arc: &JsonArc) -> bool {
    arc.arc_type == JsonArcType::Variable
}

fn default_arc_width(arc: &JsonArc) -> f64 {
    if is_variable(arc) {
        VARIABLE_ARC_WIDTH
    } else {
        ARC_WIDTH
    }
}

fn type_color(object_type: &str) -> String {
    key_color("objectType", object_type)
}

/// The object type of the place an arc connects.
fn arc_object_type<'a>(net: &'a ObjectCentricPetriNetJson, arc: &OcpnArcRef) -> Option<&'a str> {
    match (arc.source, arc.target) {
        (OcpnNodeRef::Place(i), _) | (_, OcpnNodeRef::Place(i)) => {
            net.places.get(i).map(|p| p.object_type.as_str())
        }
        _ => None,
    }
}

/// Places come first, then transitions, each in JSON order. An arc naming a place or transition the
/// net does not have is left out.
impl Visualize for ObjectCentricPetriNetJson {
    type NodeRef = OcpnNodeRef;
    type EdgeRef = OcpnArcRef;
    type Options = OcpnBuildOptions;
    type Style = OcpnStyle;
    type Request = LayoutRequest;
    type Context = ();

    fn to_graph(
        &self,
        options: &OcpnBuildOptions,
        measure: LabelWidth<'_>,
    ) -> (Graph, Vec<OcpnNodeRef>, Vec<OcpnArcRef>) {
        let net = self;
        let mut acc: GraphAcc<OcpnNodeRef, OcpnArcRef> = GraphAcc::new();
        let mut by_name = HashMap::new();
        for (i, place) in net.places.iter().enumerate() {
            let node = Node {
                size: PLACE_SIZE,
                shape: Shape::Ellipse,
                ..Node::default()
            };
            let id = acc.add_node(format!("p:{i}"), node, || OcpnNodeRef::Place(i));
            by_name
                .entry(place.name.as_str())
                .or_insert((id, OcpnNodeRef::Place(i)));
        }
        for (i, transition) in net.transitions.iter().enumerate() {
            let meta = OcpnNodeRef::Transition(i);
            let note = node_text(options.annotations.nodes.get(&meta));
            let node = Node {
                size: transition_size(transition.label.is_some(), note, measure),
                shape: Shape::Box,
                ..Node::default()
            };
            let id = acc.add_node(format!("t:{i}"), node, || meta);
            // A name shared by a place and a transition means the place, as in `from_json_form`.
            by_name
                .entry(transition.name.as_str())
                .or_insert((id, meta));
        }
        for (i, arc) in net.arcs.iter().enumerate() {
            let (Some(&(source, source_ref)), Some(&(target, target_ref))) = (
                by_name.get(arc.source.as_str()),
                by_name.get(arc.target.as_str()),
            ) else {
                continue;
            };
            let annotation = options.annotations.edges.get(&i);
            let width = default_arc_width(arc);
            let edge = arc_edge(source, target, arc.weight, width, annotation, measure);
            let meta = OcpnArcRef {
                arc: i,
                source: source_ref,
                target: target_ref,
            };
            acc.add_edge(edge, meta);
        }
        acc.finish()
    }

    fn request(
        graph: Graph,
        options: &OcpnBuildOptions,
    ) -> Result<LayoutRequest, VisualizationError> {
        options
            .layout
            .layered(graph, Preset::Layered, None, Value::Null)
    }

    fn base(style: &OcpnStyle) -> &BaseStyle {
        &style.petri.base
    }

    fn context(
        &self,
        _layout: &Layout,
        _nodes: &[OcpnNodeRef],
        _options: &OcpnBuildOptions,
    ) -> Result<(), VisualizationError> {
        Ok(())
    }

    fn draw_node(
        &self,
        node: &OcpnNodeRef,
        placement: &Placement,
        _context: &(),
        options: &OcpnBuildOptions,
        style: &OcpnStyle,
    ) -> SceneNode {
        let base = &style.petri.base;
        let mut n = match *node {
            OcpnNodeRef::Place(p) => {
                let place = &self.places[p];
                let tokens = |marking: &BTreeMap<String, u32>| {
                    marking.get(&place.name).copied().unwrap_or(0)
                };
                let mut n = place_node(
                    placement,
                    tokens(&self.initial_marking),
                    tokens(&self.final_marking),
                    &style.petri,
                );
                let color = type_color(&place.object_type);
                n.fill = Some(lighten(&color, style.place_fill_lightness));
                n.stroke = Some(Stroke::new(&color, base.border_width));
                n
            }
            OcpnNodeRef::Transition(t) => transition_node(
                placement,
                self.transitions[t].label.as_deref(),
                &style.petri,
            ),
        };
        if let Some(note) = node_text(options.annotations.nodes.get(node)) {
            push_extra_line(&mut n, note, base);
        }
        n
    }

    fn draw_edge(
        &self,
        edge: &OcpnArcRef,
        graph_edge: &Edge,
        route: &EdgeRoute,
        _context: &(),
        options: &OcpnBuildOptions,
        style: &OcpnStyle,
    ) -> Option<SceneEdge> {
        let base = &style.petri.base;
        let arc = &self.arcs[edge.arc];
        let annotation = options.annotations.edges.get(&edge.arc);
        let width = match annotated_thickness(annotation) {
            None if is_variable(arc) => style.variable_arc_width,
            _ => graph_edge.thickness,
        };
        let color = arc_object_type(self, edge).map_or_else(|| base.edge.clone(), type_color);
        let mut e = scene_edge(route, &color, width)?;
        if is_variable(arc) && !style.variable_arc_dash.is_empty() {
            e.stroke.dash = Some(style.variable_arc_dash.clone());
        }
        if let Some(text) = arc_text(arc.weight, annotation) {
            e.labels.push(edge_label(&text, route, base));
        }
        Some(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::visualization::engine::{layout_graph, layout_options_of};
    use crate::visualization::graph_build::{LABELED_TRANSITION_SIZE, SILENT_TRANSITION_SIZE};
    use crate::visualization::model;
    use crate::visualization::styling::{key_color, lighten, ElementStyle};
    use crate::visualization::{EdgeAnnotation, NodeAnnotation, StyleOverrides};
    use graphlay::svg::{NodeContent, Paint, Scene};
    use serde_json::json;
    use OcpnNodeRef::{Place, Transition};

    /// Orders and items meet at "place order", whose item arcs are variable. A silent transition
    /// moves an item on, and one arc weighs 2.
    fn example() -> ObjectCentricPetriNetJson {
        serde_json::from_value(json!({
            "places": [
                {"name": "orders p0", "object_type": "orders"},
                {"name": "orders p1", "object_type": "orders"},
                {"name": "items p0", "object_type": "items"},
                {"name": "items p1", "object_type": "items"},
                {"name": "items p2", "object_type": "items"},
            ],
            "transitions": [
                {"name": "place order", "label": "place order"},
                {"name": "items tau0", "label": null},
            ],
            "arcs": [
                {"source": "orders p0", "target": "place order"},
                {"source": "place order", "target": "orders p1", "weight": 2},
                {"source": "items p0", "target": "place order", "type": "variable"},
                {"source": "place order", "target": "items p1", "type": "variable"},
                {"source": "items p1", "target": "items tau0"},
                {"source": "items tau0", "target": "items p2"},
            ],
            "initial_marking": {"orders p0": 1, "items p0": 3},
            "final_marking": {"orders p1": 1, "items p2": 1},
        }))
        .unwrap()
    }

    fn scene_of(
        net: &ObjectCentricPetriNetJson,
        options: &OcpnBuildOptions,
        overrides: &StyleOverrides,
    ) -> (OcpnLayoutRequest, Scene) {
        let request = model::layout_request(net, options).unwrap();
        let layout = layout_graph(&request.request).unwrap();
        let style = OcpnStyle::default();
        let scene = model::scene_from_layout(net, &layout, options, &style, overrides).unwrap();
        (request, scene)
    }

    fn place_of<'a>(net: &'a ObjectCentricPetriNetJson, arc: &OcpnArcRef) -> &'a str {
        match (arc.source, arc.target) {
            (Place(i), _) | (_, Place(i)) => &net.places[i].object_type,
            _ => panic!("every arc of the example connects a place"),
        }
    }

    #[test]
    fn nodes_and_edges_map_back_to_the_json() {
        let net = example();
        let options = OcpnBuildOptions::default();
        let request = model::layout_request(&net, &options).unwrap();
        assert_eq!(
            layout_options_of(&request.request).unwrap(),
            graphlay::LayoutOptions::layered_default()
        );
        let places = (0..net.places.len()).map(Place);
        let transitions = (0..net.transitions.len()).map(Transition);
        assert_eq!(request.nodes, places.chain(transitions).collect::<Vec<_>>());

        let name = |node: OcpnNodeRef| match node {
            Place(i) => net.places[i].name.as_str(),
            Transition(i) => net.transitions[i].name.as_str(),
        };
        assert_eq!(request.edges.len(), net.arcs.len());
        let graph_edges = request.request.graph.edges.iter();
        for (i, (edge, arc)) in graph_edges.zip(&request.edges).enumerate() {
            assert_eq!(arc.arc, i);
            assert_eq!(request.nodes[edge.source.index()], arc.source);
            assert_eq!(request.nodes[edge.target.index()], arc.target);
            assert_eq!(name(arc.source), net.arcs[i].source);
            assert_eq!(name(arc.target), net.arcs[i].target);
        }

        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["nodes"][0], json!({"kind": "place", "index": 0}));
        assert_eq!(
            json["edges"][0],
            json!({
                "arc": 0,
                "source": {"kind": "place", "index": 0},
                "target": {"kind": "transition", "index": 0},
            })
        );
        let back: OcpnLayoutRequest = serde_json::from_value(json).unwrap();
        assert_eq!((back.nodes, back.edges), (request.nodes, request.edges));

        let layout = layout_graph(&request.request).unwrap();
        assert_eq!(layout, model::layout(&net, &options).unwrap());
    }

    #[test]
    fn an_arc_with_an_unknown_end_is_left_out() {
        let mut net = example();
        let mut stray = net.arcs[0].clone();
        stray.source = "nowhere".to_string();
        net.arcs.insert(1, stray);
        let request = model::layout_request(&net, &OcpnBuildOptions::default()).unwrap();
        let arcs: Vec<usize> = request.edges.iter().map(|e| e.arc).collect();
        assert_eq!(arcs, [0, 2, 3, 4, 5, 6]);
        let layout = layout_graph(&request.request).unwrap();
        let style = OcpnStyle::default();
        let options = OcpnBuildOptions::default();
        let overrides = StyleOverrides::default();
        let scene = model::scene_from_layout(&net, &layout, &options, &style, &overrides).unwrap();
        assert_eq!(scene.edges.len(), 6);
    }

    #[test]
    fn places_and_arcs_take_their_object_types_color_and_variable_arcs_are_bold_and_dashed() {
        let net = example();
        let style = OcpnStyle::default();
        let (request, scene) = scene_of(&net, &OcpnBuildOptions::default(), &Default::default());
        for (node, n) in request.nodes.iter().zip(&scene.nodes) {
            let stroke = &n.stroke.as_ref().unwrap().paint;
            match *node {
                Place(i) => {
                    let color = key_color("objectType", &net.places[i].object_type);
                    assert_eq!(n.fill, Some(lighten(&color, style.place_fill_lightness)));
                    assert_eq!(stroke, &Paint::color(color));
                }
                Transition(i) => {
                    let silent = net.transitions[i].label.is_none();
                    let (fill, width) = match silent {
                        true => (&style.petri.silent_fill, SILENT_TRANSITION_SIZE.width),
                        false => (&style.petri.base.node_fill, LABELED_TRANSITION_SIZE.width),
                    };
                    assert_eq!((n.fill.as_ref(), n.rect.w), (Some(fill), width));
                    assert_eq!(stroke, &Paint::color(style.petri.base.node_border.clone()));
                }
            }
        }

        let graph_edges = &request.request.graph.edges;
        for ((arc, e), edge) in request.edges.iter().zip(&scene.edges).zip(graph_edges) {
            let color = key_color("objectType", place_of(&net, arc));
            assert_eq!(e.stroke.paint, Paint::color(color));
            let variable = net.arcs[arc.arc].arc_type == JsonArcType::Variable;
            let width = if variable {
                VARIABLE_ARC_WIDTH
            } else {
                ARC_WIDTH
            };
            assert_eq!((e.stroke.width, edge.thickness), (width, width));
            assert_eq!(
                e.stroke.dash.as_deref(),
                variable.then_some(style.variable_arc_dash.as_str())
            );
        }
        assert_eq!(scene.edges[1].labels[0].text.text, "2");
    }

    #[test]
    fn markings_show_as_dots_and_squares() {
        let net = example();
        let (request, scene) = scene_of(&net, &OcpnBuildOptions::default(), &Default::default());
        for (node, n) in request.nodes.iter().zip(&scene.nodes) {
            let Place(i) = *node else { continue };
            let name = &net.places[i].name;
            let tokens = |marking: &BTreeMap<String, u32>| u64::from(marking[name]);
            let counts: Option<Vec<u64>> = n.content.iter().find_map(|c| match c {
                NodeContent::Glyphs(row) => Some(row.clusters.iter().map(|c| c.count).collect()),
                _ => None,
            });
            let expected = match name.as_str() {
                "orders p0" | "items p0" => Some(vec![tokens(&net.initial_marking), 0]),
                "orders p1" | "items p2" => Some(vec![0, tokens(&net.final_marking)]),
                _ => None,
            };
            assert_eq!(counts, expected, "{name}");
        }
    }

    #[test]
    fn host_overrides_win_over_the_type_colors() {
        let net = example();
        let options = OcpnBuildOptions::default();
        let request = model::layout_request(&net, &options).unwrap();
        let place = request.nodes.iter().position(|n| matches!(n, Place(_)));
        let variable = request
            .edges
            .iter()
            .position(|e| net.arcs[e.arc].arc_type == JsonArcType::Variable);
        let (place, variable) = (place.unwrap(), variable.unwrap());
        let mut overrides = StyleOverrides::default();
        overrides.nodes.insert(
            place,
            ElementStyle {
                fill: Some("#ffffff".to_string()),
                stroke: Some("#dc2626".to_string()),
                ..ElementStyle::default()
            },
        );
        overrides.edges.insert(
            variable,
            ElementStyle {
                stroke: Some("#dc2626".to_string()),
                stroke_width: Some(1.0),
                dash: Some(String::new()),
                ..ElementStyle::default()
            },
        );
        let (_, scene) = scene_of(&net, &options, &overrides);
        let node = &scene.nodes[place];
        assert_eq!(node.fill.as_deref(), Some("#ffffff"));
        assert_eq!(node.stroke.as_ref().unwrap().paint, Paint::color("#dc2626"));
        let edge = &scene.edges[variable];
        assert_eq!(edge.stroke.paint, Paint::color("#dc2626"));
        assert_eq!(
            (edge.stroke.width, edge.stroke.dash.as_deref()),
            (1.0, None)
        );
    }

    #[test]
    fn annotations_label_weight_and_widen_arcs_and_add_a_node_line() {
        use crate::visualization::graph_build::edge_label_size;
        let net = example();
        let plain = model::layout_request(&net, &OcpnBuildOptions::default()).unwrap();
        let arc = plain.edges[2];
        let mut options = OcpnBuildOptions::default();
        options.annotations.edges.insert(
            arc.arc,
            EdgeAnnotation {
                label: Some("4 h".to_string()),
                weight: Some(5.0),
                thickness: Some(6.0),
            },
        );
        options.annotations.nodes.insert(
            Transition(0),
            NodeAnnotation {
                label: Some("85%".to_string()),
            },
        );
        let back: OcpnBuildOptions =
            serde_json::from_str(&serde_json::to_string(&options).unwrap()).unwrap();
        assert_eq!(back, options);

        let (request, scene) = scene_of(&net, &options, &Default::default());
        let edge = &request.request.graph.edges[2];
        assert_eq!(edge.label_size, Some(edge_label_size("4 h")));
        assert_eq!((edge.weight, edge.thickness), (5.0, 6.0));
        assert_eq!(request.request.graph.nodes, plain.request.graph.nodes);
        assert_eq!(scene.edges[2].labels[0].text.text, "4 h");
        assert_eq!(scene.edges[2].stroke.width, 6.0);
        assert_eq!(
            scene.edges[2].stroke.dash.as_deref(),
            Some(OcpnStyle::default().variable_arc_dash.as_str())
        );
        let transition = request.nodes.iter().position(|n| *n == Transition(0));
        let last = scene.nodes[transition.unwrap()].content.last();
        assert!(matches!(last, Some(NodeContent::Text(run)) if run.text == "85%"));
    }

    /// A smoke test on a discovered net, written out to look at by hand.
    #[test]
    fn writes_the_svg_export_of_a_discovered_net() {
        use crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;
        use crate::discovery::object_centric::ocpn::{discover_ocpn_from, OcpnDiscoveryOptions};
        use crate::test_utils::{get_test_data_path, write_export};
        use crate::{Importable, OCEL};

        let path = get_test_data_path()
            .join("ocel")
            .join("order-management.json");
        let locel = SlimLinkedOCEL::from_ocel(OCEL::import_from_path(path).unwrap());
        let types = ["items", "orders", "packages"];
        let options = OcpnDiscoveryOptions::default().with_object_types(types);
        let net = discover_ocpn_from(&locel, options).to_json_form();
        assert!(net.arcs.iter().any(is_variable));

        let svg = model::svg(&net, &OcpnBuildOptions::default(), &OcpnStyle::default()).unwrap();
        assert_eq!(svg.matches("<ellipse").count(), net.places.len());
        for object_type in types {
            assert!(svg.contains(&key_color("objectType", object_type)));
        }
        let dash = OcpnStyle::default().variable_arc_dash;
        assert!(svg.contains(&format!("stroke-dasharray=\"{dash}\"")));
        write_export("order-management-ocpn.svg", &svg);
    }

    /// Two arcs between the same place and transition are annotated apart, by their index.
    #[test]
    fn parallel_arcs_take_their_own_annotation() {
        let mut net = example();
        net.arcs.push(net.arcs[0].clone());
        let mut options = OcpnBuildOptions::default();
        options.annotations.edges.insert(
            6,
            EdgeAnnotation {
                label: Some("second".to_string()),
                thickness: Some(7.0),
                ..EdgeAnnotation::default()
            },
        );
        let json = serde_json::to_value(&options.annotations).unwrap();
        assert_eq!(json["edges"][0][0], json!(6));
        let (request, scene) = scene_of(&net, &options, &Default::default());
        let (first, second) = (&request.edges[0], &request.edges[6]);
        assert_eq!((first.source, first.target), (second.source, second.target));
        assert!(scene.edges[0].labels.is_empty());
        assert_eq!(scene.edges[0].stroke.width, ARC_WIDTH);
        assert_eq!(scene.edges[6].labels[0].text.text, "second");
        assert_eq!(scene.edges[6].stroke.width, 7.0);
    }

    /// An annotated line makes a transition wider, also a silent one.
    #[test]
    fn a_transition_widens_to_fit_its_annotated_line() {
        use crate::visualization::graph_build::heuristic_label_width;
        let net = example();
        let note = "avg 12.5 hours, 85% on time";
        let mut options = OcpnBuildOptions::default();
        for t in [Transition(0), Transition(1)] {
            options.annotations.nodes.insert(
                t,
                NodeAnnotation {
                    label: Some(note.to_string()),
                },
            );
        }
        let (request, scene) = scene_of(&net, &options, &Default::default());
        for (node, n) in request.nodes.iter().zip(&scene.nodes) {
            if let Transition(_) = node {
                assert_eq!(n.rect.w, heuristic_label_width(note), "{node:?}");
            }
        }
    }
}
