//! The annotated object-centric Petri net: discovered from the flow
//! projection, with the non-flow cells as annotations of its transitions and, optionally, as
//! places.

use std::collections::{BTreeMap, HashMap, HashSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::event_data::object_centric::linked_ocel::{LinkedOCELAccess, SlimLinkedOCEL};
use crate::core::process_models::case_centric::petri_net::{
    AcceptingPetriNet, ArcType, Marking, PetriNet, PlaceID, TransitionID,
};
use crate::core::process_models::object_centric::oc_declare::ObjectInvolvementCounts;
use crate::core::process_models::object_centric::ocpn::ObjectCentricPetriNet;
use crate::discovery::object_centric::ocpn::{discover_ocpn_from, OcpnDiscoveryOptions};

use super::{flow_projection, CellState, ReflowOptions, ReflowResult};

/// Settings of the annotated object-centric Petri net.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct AnnotatedOcpnOptions {
    /// How the net is discovered from the flow projection.
    pub discovery: OcpnDiscoveryOptions,
    /// Also draw the non-flow cells as places: an involved cell as a self-loop place at its
    /// transition, an implied cell as a copy of the determining flow type's places.
    pub places: bool,
}

/// A non-flow cell, drawn at the transition of its activity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CellAnnotation {
    /// The object type of the cell.
    pub object_type: String,
    /// For an implied cell, the type it is recomputed from (drawn as `from -> object_type`).
    /// `None` for an involved cell.
    pub from: Option<String>,
}

/// Why a place was added to the discovered net.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum PlaceRole {
    /// A self-loop place of an involved cell.
    Involved,
    /// A copy of a place of `carrier`, the flow type an implied cell is recomputed from.
    Mapped {
        /// The flow type.
        carrier: String,
    },
}

/// A place added to the discovered net, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AddedPlace {
    /// The object type, i.e., the net in [`ObjectCentricPetriNet::nets`].
    pub object_type: String,
    /// The place in that net.
    pub place: PlaceID,
    /// Why it was added.
    pub role: PlaceRole,
}

/// An object-centric Petri net discovered from a flow projection, with its non-flow cells.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AnnotatedOcpn {
    /// The net. With [`AnnotatedOcpnOptions::places`], it also has the places of `places`.
    pub ocpn: ObjectCentricPetriNet,
    /// The non-flow cells by activity, sorted by object type.
    pub annotations: BTreeMap<String, Vec<CellAnnotation>>,
    /// Places added for non-flow cells.
    pub places: Vec<AddedPlace>,
    /// The fewest and most objects per event of every cell, by activity and object type. See
    /// [`ReflowResult::involvements`].
    pub involvements: BTreeMap<String, BTreeMap<String, ObjectInvolvementCounts>>,
}

/// Discover the annotated net of a tagged log, reading the annotation off the tags
/// ([`reflow_from_tagged`](super::reflow_from_tagged)) under the determination threshold `theta`
/// ([`ReflowOptions::theta`]).
pub fn annotated_ocpn(
    tagged: &SlimLinkedOCEL,
    theta: f64,
    options: &AnnotatedOcpnOptions,
) -> AnnotatedOcpn {
    let reflow_options = ReflowOptions {
        theta,
        ..ReflowOptions::default()
    };
    let result = super::reflow_from_tagged(tagged, &reflow_options);
    build(&flow_projection(tagged), tagged, &result, options)
}

impl ReflowResult {
    /// The fewest and most objects per event of every cell, by activity and object type, as
    /// [`ReflowCell::min_objects`](super::ReflowCell::min_objects) and `max_objects` count them.
    pub fn involvements(&self) -> BTreeMap<String, BTreeMap<String, ObjectInvolvementCounts>> {
        let mut out: BTreeMap<String, BTreeMap<String, ObjectInvolvementCounts>> = BTreeMap::new();
        for c in &self.cells {
            out.entry(c.activity.clone()).or_default().insert(
                c.object_type.clone(),
                ObjectInvolvementCounts {
                    min: c.min_objects,
                    max: c.max_objects,
                },
            );
        }
        out
    }

    /// Discover the annotated net of `locel` under this result's flow layer.
    pub fn annotated_ocpn(
        &self,
        locel: &SlimLinkedOCEL,
        options: &AnnotatedOcpnOptions,
    ) -> AnnotatedOcpn {
        build(&self.flow_log(locel), locel, self, options)
    }
}

/// Name of an involvement place. Roles are read off the names, which survive the renumbering of
/// silent-transition reduction.
const INVOLVED_PLACE: &str = "reflow:involved";
/// Name prefix of a mapped place, followed by its carrier.
const MAPPED_PLACE: &str = "reflow:mapped:";

fn build(
    flow_log: &SlimLinkedOCEL,
    full_log: &SlimLinkedOCEL,
    result: &ReflowResult,
    options: &AnnotatedOcpnOptions,
) -> AnnotatedOcpn {
    // A type without flow cells would get a net of silent transitions only.
    let flowing: Vec<String> = result
        .object_types
        .iter()
        .filter(|t| result.flow_layer().any(|(_, f)| f == t.as_str()))
        .filter(|t| options.discovery.uses_object_type(t))
        .cloned()
        .collect();
    let discovery = options.discovery.clone().with_object_types(flowing);
    let mut ocpn = discover_ocpn_from(flow_log, discovery);
    let mut annotations: BTreeMap<String, Vec<CellAnnotation>> = BTreeMap::new();
    for cell in result.cells.iter().filter(|c| c.state != CellState::Flow) {
        annotations
            .entry(cell.activity.clone())
            .or_default()
            .push(CellAnnotation {
                object_type: cell.object_type.clone(),
                from: cell.determined_by.as_ref().map(|d| d.from.clone()),
            });
    }
    for cells in annotations.values_mut() {
        cells.sort();
    }
    let mut places = Vec::new();
    if options.places {
        add_places(&mut ocpn, full_log, result);
        for (object_type, net) in &ocpn.nets {
            for (id, place) in net.net.places() {
                let role = match place.name.as_deref() {
                    Some(INVOLVED_PLACE) => PlaceRole::Involved,
                    Some(name) => match name.strip_prefix(MAPPED_PLACE) {
                        Some(carrier) => PlaceRole::Mapped {
                            carrier: carrier.to_string(),
                        },
                        None => continue,
                    },
                    None => continue,
                };
                places.push(AddedPlace {
                    object_type: object_type.clone(),
                    place: id,
                    role,
                });
            }
        }
        places.sort_by(|x, y| (&x.object_type, x.place).cmp(&(&y.object_type, y.place)));
    }
    AnnotatedOcpn {
        ocpn,
        annotations,
        places,
        involvements: result.involvements(),
    }
}

/// The flow type an implied cell is recomputed from at the end of its chain.
fn carrier(result: &ReflowResult, activity: &str, object_type: &str) -> Option<String> {
    let mut at = result.cell(activity, object_type)?;
    for _ in 0..result.object_types.len() {
        let from = &at.determined_by.as_ref()?.from;
        at = result.cell(activity, from)?;
        if at.state == CellState::Flow {
            return Some(from.clone());
        }
    }
    None
}

/// Per object type and activity, whether an object of the type takes part in the activity more
/// than once.
fn repeated(log: &SlimLinkedOCEL) -> HashSet<(String, String)> {
    let mut seen: HashMap<(u32, usize), u32> = HashMap::new();
    let mut out = HashSet::new();
    for e in log.get_all_evs() {
        let ev = e.get_ev(log);
        let mut last = None;
        for (_, o) in &ev.relationships {
            if last == Some(*o) {
                continue;
            }
            last = Some(*o);
            let n = seen.entry((o.into_inner(), ev.event_type)).or_default();
            *n += 1;
            if *n == 2 {
                out.insert((
                    log.get_ob_type_of(*o).to_string(),
                    log.get_ev_type_of(e).to_string(),
                ));
            }
        }
    }
    out
}

/// The activities of one type's involved cells, and of its implied ones by carrier.
type NonFlow<'a> = (Vec<&'a str>, BTreeMap<String, Vec<&'a str>>);

/// Add an involvement place per involved cell and a copy of the carrier's net per implied cell,
/// joined with what the type's own net has into one token.
fn add_places(ocpn: &mut ObjectCentricPetriNet, full_log: &SlimLinkedOCEL, result: &ReflowResult) {
    let repeats = repeated(full_log);
    let orders: HashSet<usize> = result.orderings.iter().map(|(t, _, _)| *t).collect();
    let mut by_type: BTreeMap<&str, NonFlow<'_>> = BTreeMap::new();
    for cell in result.cells.iter().filter(|c| c.state != CellState::Flow) {
        let entry = by_type.entry(cell.object_type.as_str()).or_default();
        let t = result
            .object_types
            .iter()
            .position(|t| *t == cell.object_type);
        // A mapped copy states the carrier's ordering for the type, so it needs a type that
        // orders something. Every other non-flow cell is drawn as an involvement.
        let carrier = carrier(result, &cell.activity, &cell.object_type)
            .filter(|_| t.is_some_and(|t| orders.contains(&t)));
        match carrier {
            Some(c) => entry.1.entry(c).or_default().push(cell.activity.as_str()),
            None => entry.0.push(cell.activity.as_str()),
        }
    }
    let flow_nets = ocpn.nets.clone();
    for (object_type, (involved, mapped)) in by_type {
        let mut net = flow_nets
            .get(object_type)
            .filter(|n| n.net.transitions().any(|(_, t)| t.label.is_some()))
            .cloned()
            .unwrap_or_else(|| {
                AcceptingPetriNet::new(PetriNet::new(), Marking::new(), Marking::new())
            });
        let mut branches: Vec<(Vec<PlaceID>, Vec<PlaceID>)> = Vec::new();
        if net.net.num_places() > 0 {
            branches.push((
                net.initial_marking.iter().map(|(p, _)| p).collect(),
                net.final_marking.iter().map(|(p, _)| p).collect(),
            ));
        }
        for (carrier, activities) in &mapped {
            let Some(source) = flow_nets.get(carrier) else {
                continue;
            };
            let repeatable = activities
                .iter()
                .any(|a| repeats.contains(&(object_type.to_string(), a.to_string())));
            branches.push(copy_projected(
                &mut net.net,
                source,
                activities,
                carrier,
                repeatable,
            ));
            let variable = ocpn
                .variable_arcs
                .entry(object_type.to_string())
                .or_default();
            variable.extend(activities.iter().map(|a| a.to_string()));
        }
        let (initial, final_) = bracket(&mut net.net, branches);
        net.initial_marking = initial;
        net.final_marking = final_;
        for a in involved {
            let transition = net.net.add_transition(Some(a.to_string()), None);
            let place = net.net.add_place(Some(INVOLVED_PLACE.to_string()));
            net.net
                .add_arc(ArcType::place_to_transition(place, transition), None);
            net.net
                .add_arc(ArcType::transition_to_place(transition, place), None);
            net.initial_marking.add(place, 1);
            net.final_marking.add(place, 1);
        }
        net.reduce_silent_transitions();
        ocpn.nets.insert(object_type.to_string(), net);
    }
}

fn transition_by_label(net: &PetriNet, label: &str) -> Option<TransitionID> {
    net.transitions()
        .find(|(_, t)| t.label.as_deref() == Some(label))
        .map(|(id, _)| id)
}

/// Copy `source` into `net`, with every transition outside `observable` silent, and return the
/// copy's initial and final places.
///
/// Every place is kept, so the copy states the source's language projected onto `observable`. An
/// observable transition the net already has is reused, so the flow and the copy synchronize.
/// If `repeatable`, a silent transition leads from the end back to the start, as one object of
/// the type may gather several carrier objects.
fn copy_projected(
    net: &mut PetriNet,
    source: &AcceptingPetriNet,
    observable: &[&str],
    carrier: &str,
    repeatable: bool,
) -> (Vec<PlaceID>, Vec<PlaceID>) {
    let name = format!("{MAPPED_PLACE}{carrier}");
    let places: Vec<PlaceID> = source
        .net
        .places()
        .map(|_| net.add_place(Some(name.clone())))
        .collect();
    let transitions: Vec<TransitionID> = source
        .net
        .transitions()
        .map(|(_, t)| match t.label.as_deref() {
            Some(label) if observable.contains(&label) => transition_by_label(net, label)
                .unwrap_or_else(|| net.add_transition(Some(label.to_string()), None)),
            _ => net.add_transition(None, None),
        })
        .collect();
    for arc in source.net.arcs() {
        let from_to = match arc.from_to {
            ArcType::PlaceTransition(p, t) => {
                ArcType::place_to_transition(places[p.index()], transitions[t.index()])
            }
            ArcType::TransitionPlace(t, p) => {
                ArcType::transition_to_place(transitions[t.index()], places[p.index()])
            }
        };
        net.add_arc(from_to, Some(arc.weight));
    }
    let starts: Vec<PlaceID> = source
        .initial_marking
        .iter()
        .map(|(p, _)| places[p.index()])
        .collect();
    let ends: Vec<PlaceID> = source
        .final_marking
        .iter()
        .map(|(p, _)| places[p.index()])
        .collect();
    if repeatable && !starts.is_empty() && !ends.is_empty() {
        let back = net.add_transition(None, None);
        for p in &ends {
            net.add_arc(ArcType::place_to_transition(*p, back), None);
        }
        for p in &starts {
            net.add_arc(ArcType::transition_to_place(back, *p), None);
        }
    }
    (starts, ends)
}

/// Join branches into one component with one start and one end place: a silent fork starts all
/// of them and a silent join waits for all of them, so one object runs every branch.
fn bracket(
    net: &mut PetriNet,
    mut branches: Vec<(Vec<PlaceID>, Vec<PlaceID>)>,
) -> (Marking, Marking) {
    let marking = |places: &[PlaceID]| places.iter().map(|p| (*p, 1)).collect::<Marking>();
    match branches.len() {
        0 => (Marking::new(), Marking::new()),
        1 => {
            let (starts, ends) = branches.pop().expect("one branch");
            (marking(&starts), marking(&ends))
        }
        _ => {
            let source = net.add_place(None);
            let sink = net.add_place(None);
            let fork = net.add_transition(None, None);
            let join = net.add_transition(None, None);
            net.add_arc(ArcType::place_to_transition(source, fork), None);
            net.add_arc(ArcType::transition_to_place(join, sink), None);
            for (starts, ends) in branches {
                for p in starts {
                    net.add_arc(ArcType::transition_to_place(fork, p), None);
                }
                for p in ends {
                    net.add_arc(ArcType::place_to_transition(p, join), None);
                }
            }
            (marking(&[source]), marking(&[sink]))
        }
    }
}

#[cfg(feature = "visualization")]
mod drawing {
    use super::*;
    use crate::core::process_models::object_centric::ocpn::json::ObjectCentricPetriNetJson;
    use crate::visualization::badges::{InvolvementCard, TypeBadge};
    use crate::visualization::graphlay::svg::Palette;
    use crate::visualization::ocpn::{OcpnBuildOptions, OcpnStyle};
    use crate::visualization::{ElementStyle, StyleOverrides, VisualizationError};

    /// What [`crate::visualization::ocpn`] draws an [`AnnotatedOcpn`] from: the net, build
    /// options with a badge per non-flow cell, and dashed borders for the added places.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[cfg_attr(feature = "bindings", derive(JsonSchema))]
    pub struct AnnotatedOcpnDrawing {
        /// The net in its flat JSON form.
        pub ocpn: ObjectCentricPetriNetJson,
        /// Build options with the badges.
        pub options: OcpnBuildOptions,
        /// Style of the added places, by node index.
        pub overrides: StyleOverrides,
    }

    impl AnnotatedOcpn {
        /// The net, its badges and the style of its added places, for the
        /// [`crate::visualization::ocpn`] functions and bindings.
        pub fn drawing(&self) -> AnnotatedOcpnDrawing {
            let (ocpn, place_index) = self.ocpn.to_json_form_indexed();
            let badges = self
                .annotations
                .iter()
                .map(|(activity, cells)| {
                    let here = self.involvements.get(activity);
                    let card = |t: &str| here.and_then(|h| h.get(t)).map(InvolvementCard::from);
                    let badges = cells
                        .iter()
                        .map(|c| TypeBadge {
                            object_type: c.object_type.clone(),
                            from: c.from.clone(),
                            card: card(&c.object_type),
                            from_card: c.from.as_deref().and_then(card),
                        })
                        .collect();
                    (activity.clone(), badges)
                })
                .collect();
            let mut overrides = StyleOverrides::default();
            for added in &self.places {
                // Places come first among the drawing's nodes, in JSON order.
                if let Some(i) = place_index.get(&(added.object_type.clone(), added.place)) {
                    let dash = match added.role {
                        PlaceRole::Involved => "2 3",
                        PlaceRole::Mapped { .. } => "6 3",
                    };
                    overrides.nodes.insert(
                        *i,
                        ElementStyle {
                            dash: Some(dash.to_string()),
                            ..ElementStyle::default()
                        },
                    );
                }
            }
            AnnotatedOcpnDrawing {
                ocpn,
                options: OcpnBuildOptions {
                    badges,
                    ..OcpnBuildOptions::default()
                },
                overrides,
            }
        }

        /// Draw the net as a standalone SVG document, with `palette` for the colors of the
        /// default theme.
        ///
        /// # Errors
        ///
        /// A [`VisualizationError`] from the layout.
        pub fn svg(
            &self,
            style: &OcpnStyle,
            palette: Option<&Palette>,
        ) -> Result<String, VisualizationError> {
            let drawing = self.drawing();
            let layout = crate::visualization::layout(&drawing.ocpn, &drawing.options)?;
            let scene = crate::visualization::scene_from_layout(
                &drawing.ocpn,
                &layout,
                &drawing.options,
                style,
                &drawing.overrides,
            )?;
            Ok(crate::visualization::engine::render_scene_svg(
                &scene, palette,
            ))
        }
    }
}

#[cfg(feature = "visualization")]
pub use drawing::AnnotatedOcpnDrawing;
