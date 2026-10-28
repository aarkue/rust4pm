//! Laying out and drawing OC-Declare models (i.e., multiple [`OCDeclareArc`]s).

use std::collections::{BTreeMap, BTreeSet};

use graphlay::json::LayoutRequest;
use graphlay::{
    Direction, Edge, Graph, LabelReservation, LayoutOptions, Node, Preset, Shape, Size,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;
use crate::core::process_models::oc_declare::{
    get_activity_object_involvements, OCDeclareArc, OCDeclareArcLabel, OCDeclareArcType,
    ObjectInvolvementCounts, ObjectTypeAssociation,
};
use crate::visualization::engine::{json_of, layout_options_of, VisualizationError};
use crate::visualization::graph_build::{dfg_width_with, GraphAcc, LabelWidth, DECLARE_NODE_SIZE};
use crate::visualization::model::{LayoutSettings, ModelLayoutRequest, Visualize};
use crate::visualization::styling::BaseStyle;

/// An activity of an OC-DECLARE model.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct OCDeclareNodeRef(pub String);

impl OCDeclareNodeRef {
    /// The activity name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Which arcs a laid-out edge draws.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
pub struct OCDeclareEdgeRef {
    /// Index of the arc the edge draws.
    pub arc: usize,
    /// The EP (DP) arc mirroring an EF (DF) `arc`, merged into this edge.
    pub mirrored: Option<usize>,
}

/// How an arc shows its object types.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum OCDeclareLabels {
    /// A chip naming the types, e.g., `Each(orders), Any(items)`.
    #[default]
    Names,
    /// Discs on the line: one for each, three for all, one filled and two hollow for any.
    Dots,
}

/// How a slice of [`OCDeclareArc`]s is turned into a [`Graph`]. Missing fields take the values of
/// [`OCDeclareBuildOptions::new`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(JsonSchema))]
#[serde(default)]
pub struct OCDeclareBuildOptions {
    /// Direction (left to right by default) and engine patch over the process-map preset with room
    /// reserved for every label and labels kept by their line.
    #[serde(flatten)]
    pub layout: LayoutSettings,
    /// Widen each activity box to fit its label.
    pub fit_node_width: bool,
    /// Rank EP and DP arcs backwards, so the drawing reads forwards in time.
    pub reverse_backward_arcs: bool,
    /// Merge an EF (DF) arc and an EP (DP) arc mirroring it with the same object types into one
    /// edge, which shows the EF (DF) arc's count bounds.
    pub merge_mirrored_arcs: bool,
    /// How the arcs show their object types.
    pub labels: OCDeclareLabels,
    /// Per activity, how many objects of each type one of its events involves, shown under its
    /// name.
    pub involvements: BTreeMap<String, BTreeMap<String, ObjectInvolvementCounts>>,
    /// Activities that become nodes even if no arc connects them, e.g., one just added in an
    /// editor.
    pub extra_activities: Vec<String>,
}

impl Default for OCDeclareBuildOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl OCDeclareBuildOptions {
    /// Backward arcs reversed, mirrored arcs merged, types as names.
    pub fn new() -> Self {
        OCDeclareBuildOptions {
            layout: LayoutSettings::default(),
            fit_node_width: false,
            reverse_backward_arcs: true,
            merge_mirrored_arcs: true,
            labels: OCDeclareLabels::Names,
            involvements: BTreeMap::new(),
            extra_activities: Vec::new(),
        }
    }

    /// These options with the involvements of every activity of `locel`.
    pub fn with_involvements_of(mut self, locel: &SlimLinkedOCEL) -> Self {
        self.involvements = oc_declare_involvements(locel);
        self
    }
}

/// Per activity of `locel`, how many objects of each type one of its events involves, as
/// [`OCDeclareBuildOptions::involvements`] takes them.
pub fn oc_declare_involvements(
    locel: &SlimLinkedOCEL,
) -> BTreeMap<String, BTreeMap<String, ObjectInvolvementCounts>> {
    get_activity_object_involvements(locel)
        .into_iter()
        .map(|(activity, types)| (activity, types.into_iter().collect()))
        .collect()
}

const SIDE_STEPS: [f64; 4] = [1.0; 4];

/// The layout request of OC-DECLARE arcs.
pub type OCDeclareLayoutRequest =
    ModelLayoutRequest<LayoutRequest, OCDeclareNodeRef, OCDeclareEdgeRef>;

/// Whether an arc type points against temporal order.
pub fn points_backwards(arc_type: OCDeclareArcType) -> bool {
    matches!(arc_type, OCDeclareArcType::EP | OCDeclareArcType::DP)
}

fn is_negated(counts: (Option<usize>, Option<usize>)) -> bool {
    counts.1 == Some(0)
}

fn count_label(counts: (Option<usize>, Option<usize>)) -> Option<String> {
    let min = counts.0.filter(|&m| m > 0);
    match (min, counts.1) {
        (None | Some(1), None) => None,
        (Some(a), Some(b)) if a == b => Some(format!("= {a}")),
        (Some(a), None) => Some(format!("\u{2265} {a}")),
        (None, Some(b)) => Some(format!("\u{2264} {b}")),
        (Some(a), Some(b)) => Some(format!("{a} \u{2013} {b}")),
    }
}

fn chip_text(arc: &OCDeclareArc) -> Option<String> {
    if is_negated(arc.counts) {
        None
    } else {
        count_label(arc.counts)
    }
}

struct LabelPart<'a> {
    text: &'a str,
    object_type: Option<&'a str>,
}

/// Joins to [`OCDeclareArcLabel::as_template_string`].
fn name_parts(label: &OCDeclareArcLabel) -> Vec<LabelPart<'_>> {
    fn plain(text: &str) -> LabelPart<'_> {
        LabelPart {
            text,
            object_type: None,
        }
    }
    fn named(object_type: &str) -> LabelPart<'_> {
        LabelPart {
            text: object_type,
            object_type: Some(object_type),
        }
    }
    let mut parts = Vec::new();
    for (open, refs) in [
        ("Each(", &label.each),
        ("All(", &label.all),
        ("Any(", &label.any),
    ] {
        if refs.is_empty() {
            continue;
        }
        if !parts.is_empty() {
            parts.push(plain(", "));
        }
        parts.push(plain(open));
        for (i, reference) in refs.iter().enumerate() {
            if i > 0 {
                parts.push(plain(","));
            }
            match reference {
                ObjectTypeAssociation::Simple { object_type } => parts.push(named(object_type)),
                ObjectTypeAssociation::O2O {
                    first,
                    second,
                    reversed,
                } => {
                    parts.push(named(first));
                    parts.push(plain(if *reversed { "<" } else { ">" }));
                    parts.push(named(second));
                }
            }
        }
        parts.push(plain(")"));
    }
    parts
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Quantifier {
    Each,
    Any,
    All,
}

/// Each, then any, then all.
fn references(label: &OCDeclareArcLabel) -> Vec<(Quantifier, &ObjectTypeAssociation)> {
    [
        (Quantifier::Each, &label.each),
        (Quantifier::Any, &label.any),
        (Quantifier::All, &label.all),
    ]
    .into_iter()
    .flat_map(|(q, refs)| refs.iter().map(move |r| (q, r)))
    .collect()
}

/// An O2O reference takes its first type's color.
fn reference_type(reference: &ObjectTypeAssociation) -> &str {
    match reference {
        ObjectTypeAssociation::Simple { object_type } => object_type,
        ObjectTypeAssociation::O2O { first, .. } => first,
    }
}

fn slot_width(q: Quantifier) -> f64 {
    match q {
        Quantifier::Each => 10.0,
        Quantifier::Any | Quantifier::All => 17.0,
    }
}

const SLOT_GAP: f64 = 2.0;
/// At the names label's 10px semibold.
const NAME_CHAR_WIDTH: f64 = 6.0;
const CHIP_HEIGHT: f64 = 15.0;
const CHIP_ROW: f64 = 16.0;

fn label_size(arc: &OCDeclareArc, labels: OCDeclareLabels) -> Option<Size> {
    let (width, height) = match labels {
        OCDeclareLabels::Names => {
            let chars: usize = name_parts(&arc.label)
                .iter()
                .map(|p| p.text.chars().count())
                .sum();
            if chars == 0 {
                (0.0, 0.0)
            } else {
                (NAME_CHAR_WIDTH * chars as f64 + 10.0, CHIP_HEIGHT)
            }
        }
        OCDeclareLabels::Dots => {
            let width: f64 = references(&arc.label)
                .iter()
                .map(|(q, _)| slot_width(*q) + SLOT_GAP)
                .sum();
            (width, if width > 0.0 { 16.0 } else { 0.0 })
        }
    };
    match chip_text(arc) {
        Some(_) => Some(Size::new(width.max(30.0), height + CHIP_ROW)),
        None if width > 0.0 => Some(Size::new(width, height)),
        None => None,
    }
}

/// The room an arc needs along its line: its label, and for a negated arc also the `//`, which
/// sits [`draw::SLASH_DISTANCE`] before the target end. The label stays centered, so the same
/// room is reserved on the source side. The `margin` the layout adds around a reserved label
/// counts towards that room.
fn reserved_size(arc: &OCDeclareArc, labels: OCDeclareLabels, margin: f64) -> Option<Size> {
    let size = label_size(arc, labels);
    if !is_negated(arc.counts) {
        return size;
    }
    let slashes = 2.0 * (draw::SLASH_DISTANCE + draw::slash_half() + draw::SLASH_CLEARANCE);
    let (width, height) = size.map_or((0.0, 0.0), |s| (s.width, s.height));
    Some(Size::new(
        width + (slashes - margin).max(0.0),
        height.max(draw::SLASH_LENGTH),
    ))
}

fn edge_refs(arcs: &[OCDeclareArc], merge: bool) -> Vec<OCDeclareEdgeRef> {
    let mut mirrored: Vec<Option<usize>> = vec![None; arcs.len()];
    let mut merged_away = vec![false; arcs.len()];
    if merge {
        // Labels compared as sets, so the order of the references does not matter.
        let labels: Vec<OCDeclareArcLabel> = arcs
            .iter()
            .map(|a| {
                let mut label = a.label.clone();
                label.each.sort();
                label.any.sort();
                label.all.sort();
                label
            })
            .collect();
        // Filled in reverse, so `pop` hands out the first of several mirrors.
        let mut backward: BTreeMap<_, Vec<usize>> = BTreeMap::new();
        for (i, arc) in arcs.iter().enumerate().rev() {
            if points_backwards(arc.arc_type) {
                let key = (
                    arc.arc_type,
                    arc.from.as_str(),
                    arc.to.as_str(),
                    &labels[i],
                    is_negated(arc.counts),
                );
                backward.entry(key).or_default().push(i);
            }
        }
        for (i, arc) in arcs.iter().enumerate() {
            let back = match arc.arc_type {
                OCDeclareArcType::EF => OCDeclareArcType::EP,
                OCDeclareArcType::DF => OCDeclareArcType::DP,
                _ => continue,
            };
            let key = (
                back,
                arc.to.as_str(),
                arc.from.as_str(),
                &labels[i],
                is_negated(arc.counts),
            );
            if let Some(j) = backward.get_mut(&key).and_then(Vec::pop) {
                mirrored[i] = Some(j);
                merged_away[j] = true;
            }
        }
    }
    (0..arcs.len())
        .filter(|&i| !merged_away[i])
        .map(|i| OCDeclareEdgeRef {
            arc: i,
            mirrored: mirrored[i],
        })
        .collect()
}

const MARKER_COLOR: &str = "#4b5563";

/// Colors and sizes of an OC-DECLARE drawing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct OCDeclareStyle {
    /// Colors and paddings shared with every other model.
    pub base: BaseStyle,
    /// Draw the activities in the base node colors instead of tinting each one.
    pub plain_activities: bool,
    /// How far towards white an activity's color is washed for its fill.
    pub fill_lightness: f64,
    /// Color of the markers, and of an arc that names no object type.
    pub marker: String,
    /// Width of an arc's line.
    pub arc_width: f64,
    /// Side of a marker's box.
    pub marker_size: f64,
}

impl Default for OCDeclareStyle {
    fn default() -> Self {
        OCDeclareStyle {
            base: BaseStyle::default(),
            plain_activities: false,
            fill_lightness: 0.85,
            marker: MARKER_COLOR.to_string(),
            arc_width: 2.5,
            marker_size: 15.0,
        }
    }
}

mod draw {
    use super::*;
    use graphlay::layout::{EdgeRoute, Placement};
    use graphlay::svg::{
        Attrs, BuiltinMarker, Chip, Decoration, DrawOrder, EdgeAnchor, EdgeLabel, ExactLayout,
        Glyph, GlyphCluster, GlyphLayout, GlyphRow, Gradient, GradientStop, LineCap, MarkerRef,
        NodeContent, Paint, Point as SPoint, SceneEdge, SceneNode, Shape as SShape, Stroke,
        TextAnchor, TextRun, TextSpan, TextWrap,
    };
    use graphlay::Layout;

    use crate::core::process_models::oc_declare::{EXIT_EVENT_PREFIX, INIT_EVENT_PREFIX};
    use crate::visualization::styling::{darken, key_color, lighten, scene_node, scene_point};

    pub(super) const NEGATION_COLOR: &str = "#dc2626";
    pub(super) const SLASH_LENGTH: f64 = 13.0;
    const SLASH_SLANT: f64 = -22.6;
    const SLASH_GAP: f64 = 5.0;
    /// How far before the target end the `//` sits, clear of every target marker.
    pub(super) const SLASH_DISTANCE: f64 = 27.0;
    /// Space kept between the `//` and a label.
    pub(super) const SLASH_CLEARANCE: f64 = 3.0;
    const END_CLEARANCE: f64 = 20.0;
    const CHIP_TEXT: &str = "#1c2024";
    const CHIP_BORDER: &str = "#d9d9e0";
    const INVOLVEMENT_TEXT: &str = "#8b8d98";
    const NODE_RADIUS: f64 = 16.0;
    const DISC_RADIUS: f64 = 4.33;
    /// Less than a diameter, so the discs of a cluster overlap.
    const DISC_STEP: f64 = 3.1;

    fn object_type_color(object_type: &str) -> String {
        key_color("objectType", object_type)
    }

    /// Init and exit pseudo-activities take their object type's color and a dashed border.
    fn node_color(name: &str) -> (String, bool) {
        for prefix in [INIT_EVENT_PREFIX, EXIT_EVENT_PREFIX] {
            if let Some(object_type) = name.strip_prefix(prefix).and_then(|r| r.strip_prefix(' ')) {
                return (object_type_color(object_type), true);
            }
        }
        (key_color("activity", name), false)
    }

    fn markers(
        arc_type: OCDeclareArcType,
        merged: bool,
    ) -> (Option<BuiltinMarker>, Option<BuiltinMarker>) {
        use BuiltinMarker::*;
        if merged {
            return (Some(Ball), Some(ArrowBall));
        }
        match arc_type {
            OCDeclareArcType::AS => (Some(Ball), None),
            OCDeclareArcType::EF => (Some(Ball), Some(ArrowCentered)),
            OCDeclareArcType::DF => (Some(Ball), Some(ArrowBar)),
            OCDeclareArcType::EP => (Some(BallArrow), None),
            OCDeclareArcType::DP => (Some(BallBarArrow), None),
        }
    }

    /// Several types give a gradient, weighted 4 per each or all reference and 1 per any.
    fn arc_paint(label: &OCDeclareArcLabel, points: &[SPoint], style: &OCDeclareStyle) -> Paint {
        let mut weights: Vec<(&str, f64)> = Vec::new();
        for (q, reference) in references(label) {
            let w = if q == Quantifier::Any { 1.0 } else { 4.0 };
            let object_type = reference_type(reference);
            match weights.iter_mut().find(|(t, _)| *t == object_type) {
                Some((_, total)) => *total += w,
                None => weights.push((object_type, w)),
            }
        }
        match weights.as_slice() {
            [] => Paint::Color(style.marker.clone()),
            [(object_type, _)] => Paint::Color(object_type_color(object_type)),
            _ => {
                let total: f64 = weights.iter().map(|(_, w)| w).sum();
                let mut before = 0.0;
                let stops = weights
                    .iter()
                    .map(|(object_type, w)| {
                        let offset = (before + w / 2.0) / total;
                        before += w;
                        GradientStop {
                            offset,
                            color: object_type_color(object_type),
                        }
                    })
                    .collect();
                Paint::Gradient(Gradient {
                    from: points[0],
                    to: points[points.len() - 1],
                    stops,
                })
            }
        }
    }

    fn dist(a: SPoint, b: SPoint) -> f64 {
        (a.x - b.x).hypot(a.y - b.y)
    }

    fn polyline_length(points: &[SPoint]) -> f64 {
        points.windows(2).map(|w| dist(w[0], w[1])).sum()
    }

    fn position_of(points: &[SPoint], p: SPoint) -> f64 {
        let (mut best, mut at, mut walked) = (f64::INFINITY, 0.0, 0.0);
        for w in points.windows(2) {
            let (a, b) = (w[0], w[1]);
            let len = dist(a, b);
            let t = if len > 0.0 {
                (((p.x - a.x) * (b.x - a.x) + (p.y - a.y) * (b.y - a.y)) / (len * len))
                    .clamp(0.0, 1.0)
            } else {
                0.0
            };
            let d = dist(p, SPoint::new(a.x + t * (b.x - a.x), a.y + t * (b.y - a.y)));
            if d < best {
                (best, at) = (d, walked + t * len);
            }
            walked += len;
        }
        at
    }

    /// Returns the part of the line the discs cover, relative to its middle.
    fn push_discs(edge: &mut SceneEdge, label: &OCDeclareArcLabel) -> (f64, f64) {
        let refs = references(label);
        let total: f64 = refs.iter().map(|(q, _)| slot_width(*q) + SLOT_GAP).sum();
        let mut offset = -total / 2.0;
        for (q, reference) in refs {
            let c = offset + slot_width(q) / 2.0;
            offset += slot_width(q) + SLOT_GAP;
            let color = object_type_color(reference_type(reference));
            // `(offset, filled)` from the target side, so the source-side disc ends up on top.
            let discs: &[(f64, bool)] = match q {
                Quantifier::Each => &[(0.0, true)],
                Quantifier::All => &[(DISC_STEP, true), (0.0, true), (-DISC_STEP, true)],
                Quantifier::Any => &[(DISC_STEP, false), (0.0, false), (-DISC_STEP, true)],
            };
            for &(d, filled) in discs {
                edge.decorations.push(Decoration::Dot {
                    at: EdgeAnchor::Along {
                        fraction: 0.5,
                        offset: c + d,
                    },
                    radius: DISC_RADIUS,
                    fill: Some(if filled {
                        color.clone()
                    } else {
                        "#ffffff".to_string()
                    }),
                    stroke: Some(Stroke::new(darken(&color, 0.35), 1.0)),
                    attrs: Attrs::default(),
                });
            }
        }
        (-total / 2.0, total / 2.0)
    }

    fn chip() -> Chip {
        Chip {
            fill: "#ffffff".to_string(),
            padding: 2.5,
            corner_radius: 4.0,
            stroke: Some(Stroke::new(CHIP_BORDER, 1.0)),
        }
    }

    /// Half of the `//` along the line: half its gap plus the lean of a stroke.
    pub(super) fn slash_half() -> f64 {
        SLASH_GAP / 2.0 + SLASH_LENGTH / 2.0 * SLASH_SLANT.to_radians().sin().abs()
    }

    /// Before the target end, or beside the discs or label (`taken`, relative to the middle)
    /// when they are there.
    fn push_slashes(edge: &mut SceneEdge, length: f64, taken: Option<(f64, f64)>) {
        let half = slash_half();
        let room = length / 2.0 - END_CLEARANCE - half;
        let default = length / 2.0 - SLASH_DISTANCE;
        let (fraction, center) = match taken {
            Some((lo, hi))
                if default - half < hi + SLASH_CLEARANCE
                    && default + half > lo - SLASH_CLEARANCE =>
            {
                let after = hi + SLASH_CLEARANCE + half;
                let before = lo - SLASH_CLEARANCE - half;
                if after <= room || before < -room {
                    (0.5, after)
                } else {
                    (0.5, before)
                }
            }
            _ => (1.0, -SLASH_DISTANCE),
        };
        for offset in [-SLASH_GAP / 2.0, SLASH_GAP / 2.0] {
            edge.decorations.push(Decoration::Tick {
                at: EdgeAnchor::Along {
                    fraction,
                    offset: center + offset,
                },
                length: SLASH_LENGTH,
                slant: SLASH_SLANT,
                stroke: Stroke {
                    linecap: LineCap::Round,
                    ..Stroke::new(NEGATION_COLOR, 2.0)
                },
                attrs: Attrs::default(),
            });
        }
    }

    fn names_label(label: &OCDeclareArcLabel, at: EdgeAnchor) -> Option<EdgeLabel> {
        let parts = name_parts(label);
        if parts.is_empty() {
            return None;
        }
        Some(EdgeLabel {
            text: TextRun {
                spans: parts
                    .iter()
                    .map(|p| TextSpan {
                        text: p.text.to_string(),
                        color: p.object_type.map(object_type_color),
                        weight: None,
                    })
                    .collect(),
                size: Some(10.0),
                weight: Some(600.0),
                color: Some(CHIP_TEXT.to_string()),
                ..TextRun::default()
            },
            at,
            chip: Some(chip()),
            attrs: Attrs::default(),
        })
    }

    fn count_chip(arc: &OCDeclareArc, at: EdgeAnchor) -> Option<EdgeLabel> {
        Some(EdgeLabel {
            text: TextRun {
                text: chip_text(arc)?,
                dy: CHIP_ROW,
                size: Some(10.0),
                weight: Some(700.0),
                color: Some(CHIP_TEXT.to_string()),
                ..TextRun::default()
            },
            at,
            chip: Some(chip()),
            attrs: Attrs::default(),
        })
    }

    // The strip's room inside the node, its gap between items and its height in rows. The rest
    // of the object types go into a "+k" at the end of the second row.
    const STRIP_INSET: f64 = 16.0;
    const STRIP_GAP: f64 = 6.0;
    const STRIP_ROWS: usize = 2;
    // Width of a 9px character, the cap on a name and the width of its `0..1`.
    const STRIP_CHAR: f64 = 5.4;
    const STRIP_NAME_MAX: f64 = 52.0;
    const STRIP_CARD: f64 = 18.0;
    const DOTS_DY: f64 = 8.5;
    const NAMES_DY: f64 = 9.0;
    const NAMES_LINE: f64 = 11.0;

    fn plus_width(hidden: usize) -> f64 {
        format!("+{hidden}").len() as f64 * STRIP_CHAR + 2.0
    }

    /// `widths` packed greedily into rows of at most `usable`, as item indices per row.
    fn strip_rows(widths: &[f64], usable: f64) -> Vec<Vec<usize>> {
        let mut rows: Vec<Vec<usize>> = Vec::new();
        let mut row = 0.0;
        for (i, &w) in widths.iter().enumerate() {
            match rows.last_mut() {
                Some(last) if row + STRIP_GAP + w <= usable => {
                    last.push(i);
                    row += STRIP_GAP + w;
                }
                _ => {
                    rows.push(vec![i]);
                    row = w;
                }
            }
        }
        rows
    }

    /// How many items of `widths` fit the strip, in order. Fewer than all leaves room for a "+k".
    fn strip_shown(widths: &[f64], usable: f64) -> usize {
        if strip_rows(widths, usable).len() <= STRIP_ROWS {
            return widths.len();
        }
        (1..widths.len())
            .rev()
            .find(|&shown| {
                let mut packed = widths[..shown].to_vec();
                packed.push(plus_width(widths.len() - shown));
                strip_rows(&packed, usable).len() <= STRIP_ROWS
            })
            .unwrap_or(0)
    }

    fn involvement_cluster(object_type: &str, c: &ObjectInvolvementCounts) -> GlyphCluster {
        let color = object_type_color(object_type);
        let optional = c.min == 0;
        GlyphCluster {
            glyph: Glyph::Dot,
            count: if c.max > 1 { 3 } else { 1 },
            outline: Some(Stroke {
                paint: Paint::Color(darken(&color, 0.35)),
                width: if optional { 1.3 } else { 1.0 },
                dash: optional.then(|| "2.2 1.6".to_string()),
                ..Stroke::default()
            }),
            color: Some(color),
            radius: DISC_RADIUS,
            step: Some(DISC_STEP),
            ..GlyphCluster::default()
        }
    }

    /// A name shortened to the strip's cap (truncated).
    fn strip_name(name: &str) -> String {
        let max = (STRIP_NAME_MAX / STRIP_CHAR) as usize;
        if name.chars().count() <= max {
            return name.to_string();
        }
        let mut short: String = name.chars().take(max.saturating_sub(1)).collect();
        short.push('\u{2026}');
        short
    }

    fn plus_text(hidden: usize, dx: f64, dy: f64, anchor: TextAnchor) -> NodeContent {
        NodeContent::Text(TextRun {
            text: format!("+{hidden}"),
            dx,
            dy,
            size: Some(9.0),
            weight: Some(600.0),
            color: Some(INVOLVEMENT_TEXT.to_string()),
            anchor,
            ..TextRun::default()
        })
    }

    /// The object types under an activity's name, in at most two rows. A disc for at most one
    /// object, three for more, and a dashed ring when an event may involve none.
    pub(super) fn involvement_strip(
        involved: &[(&String, &ObjectInvolvementCounts)],
        labels: OCDeclareLabels,
        node_width: f64,
    ) -> Vec<NodeContent> {
        let usable = node_width - STRIP_INSET;
        match labels {
            OCDeclareLabels::Dots => {
                let clusters: Vec<GlyphCluster> = involved
                    .iter()
                    .map(|(object_type, c)| involvement_cluster(object_type, c))
                    .collect();
                let widths: Vec<f64> = clusters.iter().map(GlyphCluster::width).collect();
                let shown = strip_shown(&widths, usable);
                let row = |clusters: Vec<GlyphCluster>, dx: f64, dy: f64, wrap_width: f64| {
                    NodeContent::Glyphs(GlyphRow {
                        clusters,
                        dx,
                        dy,
                        layout: GlyphLayout::Exact(ExactLayout {
                            wrap_width,
                            cluster_gap: STRIP_GAP,
                            ..ExactLayout::default()
                        }),
                        ..GlyphRow::default()
                    })
                };
                if shown == clusters.len() {
                    return vec![row(clusters, 0.0, DOTS_DY, usable)];
                }
                // The second row and its "+k" are centered together.
                let hidden = clusters.len() - shown;
                let rows = strip_rows(&widths[..shown], usable);
                let row_h = 2.0 * DISC_RADIUS + ExactLayout::default().row_gap;
                let mut out = Vec::new();
                for (ri, items) in rows.iter().enumerate() {
                    let dy = DOTS_DY + ri as f64 * row_h;
                    let members: Vec<GlyphCluster> =
                        items.iter().map(|&i| clusters[i].clone()).collect();
                    if ri + 1 < rows.len() {
                        out.push(row(members, 0.0, dy, f64::INFINITY));
                        continue;
                    }
                    let width: f64 = items.iter().map(|&i| widths[i]).sum::<f64>()
                        + STRIP_GAP * items.len().saturating_sub(1) as f64;
                    let tail = STRIP_GAP + plus_width(hidden);
                    out.push(row(members, -tail / 2.0, dy, f64::INFINITY));
                    out.push(plus_text(
                        hidden,
                        (width - tail) / 2.0 + STRIP_GAP,
                        dy,
                        TextAnchor::Start,
                    ));
                }
                out
            }
            OCDeclareLabels::Names => {
                let card = |c: &ObjectInvolvementCounts| {
                    format!(
                        " {}..{}",
                        if c.min >= 1 { "1" } else { "0" },
                        if c.max > 1 { "*" } else { "1" }
                    )
                };
                let widths: Vec<f64> = involved
                    .iter()
                    .map(|(t, _)| {
                        (t.chars().count() as f64 * STRIP_CHAR).min(STRIP_NAME_MAX)
                            + 2.0
                            + STRIP_CARD
                    })
                    .collect();
                let shown = strip_shown(&widths, usable);
                let hidden = involved.len() - shown;
                let rows = strip_rows(&widths[..shown], usable);
                let last = rows.len().saturating_sub(1);
                rows.iter()
                    .enumerate()
                    .map(|(ri, items)| {
                        let mut spans = Vec::new();
                        for (k, &i) in items.iter().enumerate() {
                            let (object_type, c) = involved[i];
                            if k > 0 {
                                spans.push(TextSpan {
                                    text: "  ".to_string(),
                                    ..TextSpan::default()
                                });
                            }
                            spans.push(TextSpan {
                                text: strip_name(object_type),
                                color: Some(object_type_color(object_type)),
                                weight: Some(600.0),
                            });
                            spans.push(TextSpan {
                                text: card(c),
                                ..TextSpan::default()
                            });
                        }
                        if ri == last && hidden > 0 {
                            spans.push(TextSpan {
                                text: format!("  +{hidden}"),
                                weight: Some(600.0),
                                ..TextSpan::default()
                            });
                        }
                        NodeContent::Text(TextRun {
                            spans,
                            dy: NAMES_DY + ri as f64 * NAMES_LINE,
                            size: Some(9.0),
                            color: Some(INVOLVEMENT_TEXT.to_string()),
                            ..TextRun::default()
                        })
                    })
                    .collect()
            }
        }
    }

    impl Visualize for [OCDeclareArc] {
        type NodeRef = OCDeclareNodeRef;
        type EdgeRef = OCDeclareEdgeRef;
        type Options = OCDeclareBuildOptions;
        type Style = OCDeclareStyle;
        type Request = LayoutRequest;
        type Context = ();

        fn to_graph(
            &self,
            options: &OCDeclareBuildOptions,
            measure: LabelWidth<'_>,
        ) -> (Graph, Vec<OCDeclareNodeRef>, Vec<OCDeclareEdgeRef>) {
            let mut acc: GraphAcc<OCDeclareNodeRef, OCDeclareEdgeRef> = GraphAcc::new();

            let mut activities: BTreeSet<&str> = options
                .extra_activities
                .iter()
                .map(String::as_str)
                .collect();
            for arc in self {
                activities.insert(arc.from.as_str());
                activities.insert(arc.to.as_str());
            }
            for activity in &activities {
                let width = if options.fit_node_width {
                    dfg_width_with(activity, measure).max(DECLARE_NODE_SIZE.width)
                } else {
                    DECLARE_NODE_SIZE.width
                };
                acc.add_node(
                    *activity,
                    Node {
                        size: Size::new(width, DECLARE_NODE_SIZE.height),
                        shape: Shape::Box,
                        ..Node::default()
                    },
                    || OCDeclareNodeRef((*activity).to_string()),
                );
            }

            let mut edges = edge_refs(self, options.merge_mirrored_arcs);
            edges.sort_by(|a, b| {
                let (x, y) = (&self[a.arc], &self[b.arc]);
                (x.from.as_str(), x.to.as_str(), x.arc_type, &x.label).cmp(&(
                    y.from.as_str(),
                    y.to.as_str(),
                    y.arc_type,
                    &y.label,
                ))
            });
            // With the patch, so a patched margin is the one subtracted. An invalid patch fails later, in
            // the layout request.
            let margin = <[OCDeclareArc] as Visualize>::request(Graph::default(), options)
                .and_then(|request| layout_options_of(&request))
                .unwrap_or_else(|_| LayoutOptions::process_map())
                .spacing
                .edge_label_margin;
            for edge in edges {
                let arc = &self[edge.arc];
                let (Some(source), Some(target)) =
                    (acc.node_id(arc.from.as_str()), acc.node_id(arc.to.as_str()))
                else {
                    continue;
                };
                acc.add_edge(
                    Edge {
                        label_size: reserved_size(arc, options.labels, margin),
                        reverse_for_ranking: options.reverse_backward_arcs
                            && edge.mirrored.is_none()
                            && points_backwards(arc.arc_type),
                        ..Edge::new(source, target)
                    },
                    edge,
                );
            }

            acc.finish()
        }

        fn request(
            graph: Graph,
            options: &OCDeclareBuildOptions,
        ) -> Result<LayoutRequest, VisualizationError> {
            let patch = serde_json::json!({
                // The default reserves room only for a label too large across the line, and a
                // names chip or the `//` of a negated arc is small across it but long along it.
                "labels": {"reservation": json_of(LabelReservation::Always)},
                // A label pushed further than just clear of its line no longer reads as its arc's.
                "tuning": {"labels": {"side_steps": SIDE_STEPS}},
            });
            options
                .layout
                .layered(graph, Preset::ProcessMap, Some(Direction::LeftRight), patch)
        }

        fn base(style: &OCDeclareStyle) -> &BaseStyle {
            &style.base
        }

        fn context(
            &self,
            _layout: &Layout,
            _nodes: &[OCDeclareNodeRef],
            _options: &OCDeclareBuildOptions,
        ) -> Result<(), VisualizationError> {
            Ok(())
        }

        fn draw_order() -> DrawOrder {
            DrawOrder::EdgesOverNodes
        }

        fn draw_node(
            &self,
            node: &OCDeclareNodeRef,
            placement: &Placement,
            _context: &(),
            options: &OCDeclareBuildOptions,
            style: &OCDeclareStyle,
        ) -> SceneNode {
            let base = &style.base;
            let (color, dashed) = node_color(node.as_str());
            let (fill, border, border_opacity, text) = if style.plain_activities {
                (
                    base.node_fill.clone(),
                    base.node_border.clone(),
                    None,
                    base.text.clone(),
                )
            } else {
                (
                    lighten(&color, style.fill_lightness),
                    color.clone(),
                    Some(0.8),
                    darken(&color, 0.25),
                )
            };
            let mut n = scene_node(
                placement,
                Shape::Box,
                Some(fill),
                Some(Stroke {
                    paint: Paint::Color(border),
                    width: 2.0,
                    dash: dashed.then(|| "8 5".to_string()),
                    opacity: border_opacity,
                    ..Stroke::default()
                }),
                base,
            );
            n.shape = SShape::Rect {
                corner_radius: NODE_RADIUS,
            };
            let involved: Vec<(&String, &ObjectInvolvementCounts)> = options
                .involvements
                .get(node.as_str())
                .map(|types| types.iter().filter(|(_, c)| c.max > 0).collect())
                .unwrap_or_default();
            n.content.push(NodeContent::Text(TextRun {
                text: node.as_str().to_string(),
                dy: if involved.is_empty() { 0.0 } else { -7.0 },
                size: Some(12.0),
                weight: Some(600.0),
                color: Some(text),
                wrap: Some(TextWrap {
                    max_width: placement.size.width - 16.0,
                    max_lines: if involved.is_empty() { 2 } else { 1 },
                    line_height: 15.0,
                }),
                ..TextRun::default()
            }));
            if !involved.is_empty() {
                n.content.extend(involvement_strip(
                    &involved,
                    options.labels,
                    placement.size.width,
                ));
            }
            n
        }

        fn draw_edge(
            &self,
            edge: &OCDeclareEdgeRef,
            _graph_edge: &Edge,
            route: &EdgeRoute,
            _context: &(),
            options: &OCDeclareBuildOptions,
            style: &OCDeclareStyle,
        ) -> Option<SceneEdge> {
            let marker = |kind: Option<BuiltinMarker>| match kind {
                Some(marker) => MarkerRef::Builtin {
                    marker,
                    color: Some(style.marker.clone()),
                    size: Some(style.marker_size),
                },
                None => MarkerRef::None,
            };
            let arc = self.get(edge.arc)?;
            if route.points.len() < 2 {
                return None;
            }
            let points: Vec<SPoint> = route.points.iter().copied().map(scene_point).collect();
            let mirror = edge.mirrored.and_then(|i| self.get(i));
            let (start, end) = markers(arc.arc_type, mirror.is_some());
            let mut attrs = Attrs::default().data("template", arc.as_template_string());
            attrs = match mirror {
                Some(m) => attrs
                    .data(
                        "arc-type",
                        format!("{}+{}", arc.arc_type.get_name(), m.arc_type.get_name()),
                    )
                    .data("mirrored-template", m.as_template_string()),
                None => attrs.data("arc-type", arc.arc_type.get_name()),
            };
            let mut e = SceneEdge {
                stroke: Stroke {
                    paint: arc_paint(&arc.label, &points, style),
                    width: style.arc_width,
                    ..Stroke::default()
                },
                corner_radius: NODE_RADIUS,
                marker_start: marker(start),
                marker_end: marker(end),
                attrs,
                points,
                ..SceneEdge::default()
            };

            let length = polyline_length(&e.points);
            let (anchor, taken) = match (options.labels, route.label_anchor) {
                (OCDeclareLabels::Names, Some(p)) => {
                    let p = scene_point(p);
                    // A conservative guess: the chip's whole width, as if it lay along the line.
                    let half = label_size(arc, options.labels).map_or(0.0, |s| s.width / 2.0);
                    let at = position_of(&e.points, p) - length / 2.0;
                    let label = names_label(&arc.label, EdgeAnchor::At(p));
                    let taken = label.as_ref().map(|_| (at - half, at + half));
                    e.labels.extend(label);
                    (EdgeAnchor::At(p), taken)
                }
                (OCDeclareLabels::Names, None) => {
                    e.labels
                        .extend(names_label(&arc.label, EdgeAnchor::Fraction(0.5)));
                    (EdgeAnchor::Fraction(0.5), None)
                }
                (OCDeclareLabels::Dots, _) => {
                    let taken = push_discs(&mut e, &arc.label);
                    (EdgeAnchor::Fraction(0.5), Some(taken))
                }
            };
            e.labels.extend(count_chip(arc, anchor));
            if is_negated(arc.counts) && length > 0.0 {
                push_slashes(&mut e, length, taken);
            }
            Some(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::draw::NEGATION_COLOR;
    use super::*;
    use crate::core::process_models::oc_declare::OCDeclareNode;
    use crate::visualization::graph_build::heuristic_label_width;
    use crate::visualization::model::{self, Visualize};
    use crate::visualization::styling::{key_color, lighten};
    use crate::visualization::StyleOverrides;
    use graphlay::svg::{
        BuiltinMarker, Decoration, EdgeAnchor, MarkerRef, NodeContent, Paint, Scene, SceneEdge,
    };
    use OCDeclareArcType::*;

    fn arc(
        (from, to): (&str, &str),
        arc_type: OCDeclareArcType,
        [each, any, all]: [&[&str]; 3],
        counts: (Option<usize>, Option<usize>),
    ) -> OCDeclareArc {
        let refs = |types: &[&str]| -> Vec<ObjectTypeAssociation> {
            types
                .iter()
                .map(|t| ObjectTypeAssociation::new_simple(*t))
                .collect()
        };
        OCDeclareArc {
            from: OCDeclareNode::new(from),
            to: OCDeclareNode::new(to),
            arc_type,
            label: OCDeclareArcLabel {
                each: refs(each),
                any: refs(any),
                all: refs(all),
            },
            counts,
        }
    }

    fn orders(from: &str, to: &str, arc_type: OCDeclareArcType) -> OCDeclareArc {
        arc(
            (from, to),
            arc_type,
            [&["orders"], &[], &["items"]],
            (Some(1), None),
        )
    }

    fn sample_arcs() -> Vec<OCDeclareArc> {
        serde_json::from_str(
            r#"[
                {"arc_type":"EF","counts":[1,null],"from":"place order","to":"ship",
                 "label":{"all":[{"type":"Simple","object_type":"item"}],"any":[],
                          "each":[{"type":"Simple","object_type":"order"}]}},
                {"arc_type":"DF","counts":[1,1],"from":"ship","to":"invoice",
                 "label":{"all":[{"type":"Simple","object_type":"item"}],
                          "any":[{"type":"Simple","object_type":"worker"}],
                          "each":[{"type":"Simple","object_type":"order"}]}},
                {"arc_type":"EP","counts":[0,1],"from":"invoice","to":"pay",
                 "label":{"all":[{"type":"Simple","object_type":"item"}],"any":[],
                          "each":[{"type":"Simple","object_type":"order"}]}},
                {"arc_type":"AS","counts":[1,null],"from":"pay","to":"place order",
                 "label":{"each":[],"any":[{"type":"Simple","object_type":"order"}],"all":[]}},
                {"arc_type":"EP","counts":[1,null],"from":"ship","to":"place order",
                 "label":{"all":[{"type":"Simple","object_type":"item"}],"any":[],
                          "each":[{"type":"Simple","object_type":"order"}]}},
                {"arc_type":"EF","counts":[0,0],"from":"invoice","to":"place order",
                 "label":{"each":[{"type":"Simple","object_type":"order"}],"any":[],"all":[]}},
                {"arc_type":"DF","counts":[2,null],"from":"place order","to":"invoice",
                 "label":{"each":[],"all":[],
                          "any":[{"type":"O2O","first":"order","second":"item","reversed":false}]}}
            ]"#,
        )
        .unwrap()
    }

    #[test]
    fn partial_options_keep_the_defaults() {
        let options: OCDeclareBuildOptions =
            serde_json::from_value(serde_json::json!({ "extra_activities": ["x"] })).unwrap();
        assert!(options.reverse_backward_arcs && options.merge_mirrored_arcs);
        assert_eq!(options.extra_activities, ["x"]);
    }

    fn with(change: impl FnOnce(&mut OCDeclareBuildOptions)) -> OCDeclareBuildOptions {
        let mut options = OCDeclareBuildOptions::new();
        change(&mut options);
        options
    }

    fn scene_of(
        arcs: &[OCDeclareArc],
        options: &OCDeclareBuildOptions,
    ) -> (Scene, Vec<OCDeclareNodeRef>) {
        styled_scene_of(arcs, options, &OCDeclareStyle::default())
    }

    fn styled_scene_of(
        arcs: &[OCDeclareArc],
        options: &OCDeclareBuildOptions,
        style: &OCDeclareStyle,
    ) -> (Scene, Vec<OCDeclareNodeRef>) {
        let request = model::layout_request(arcs, options).unwrap();
        let layout = crate::visualization::engine::layout_graph(&request.request).unwrap();
        let overrides = StyleOverrides::default();
        let scene = model::scene_from_layout(arcs, &layout, options, style, &overrides);
        (scene.unwrap(), request.nodes)
    }

    fn texts(edge: &SceneEdge) -> Vec<String> {
        edge.labels
            .iter()
            .map(|l| match l.text.spans.as_slice() {
                [] => l.text.text.clone(),
                spans => spans.iter().map(|s| s.text.as_str()).collect(),
            })
            .collect()
    }

    /// How far from its start an anchor lands, measured on the edge's polyline.
    fn position(edge: &SceneEdge, at: &EdgeAnchor) -> f64 {
        let length: f64 = edge
            .points
            .windows(2)
            .map(|w| (w[1].x - w[0].x).hypot(w[1].y - w[0].y))
            .sum();
        match *at {
            EdgeAnchor::Fraction(f) => f * length,
            EdgeAnchor::Along { fraction, offset } => fraction * length + offset,
            EdgeAnchor::At(_) => panic!("decorations sit along the line"),
        }
    }

    fn discs(edge: &SceneEdge) -> Vec<(f64, String)> {
        let dots = edge.decorations.iter().filter_map(|d| match d {
            Decoration::Dot { at, fill, .. } => Some((position(edge, at), fill.clone().unwrap())),
            _ => None,
        });
        dots.collect()
    }

    fn slashes(edge: &SceneEdge) -> Vec<f64> {
        let ticks = edge.decorations.iter().filter_map(|d| match d {
            Decoration::Tick { at, stroke, .. } => {
                assert_eq!(stroke.paint, Paint::color(NEGATION_COLOR));
                Some(position(edge, at))
            }
            _ => None,
        });
        ticks.collect()
    }

    #[test]
    fn mirrored_arcs_merge_unless_their_types_or_negation_differ() {
        let (ef, ep) = (orders("a", "b", EF), orders("b", "a", EP));
        let options = OCDeclareBuildOptions::new();
        let (graph, _, edges) = [ef.clone(), ep.clone()].to_graph(&options, &heuristic_label_width);
        assert_eq!(
            edges,
            [OCDeclareEdgeRef {
                arc: 0,
                mirrored: Some(1)
            }]
        );
        assert!(!graph.edges[0].reverse_for_ranking);

        let other_types = arc(("b", "a"), EP, [&["items"], &[], &[]], (Some(1), None));
        let negated = OCDeclareArc {
            counts: (Some(0), Some(0)),
            ..ep
        };
        for mirror in [other_types, negated] {
            let (_, _, edges) = [ef.clone(), mirror].to_graph(&options, &heuristic_label_width);
            assert_eq!(edges.len(), 2);
        }
    }

    #[test]
    fn the_layout_request_resolves_to_the_layout_options() {
        use crate::visualization::engine::layout_options_of;
        for options in [
            OCDeclareBuildOptions::new(),
            with(|o| o.layout.direction = Some(Direction::TopBottom)),
            with(|o| o.labels = OCDeclareLabels::Dots),
        ] {
            let request = model::layout_request(sample_arcs().as_slice(), &options).unwrap();
            let resolved = layout_options_of(&request.request).unwrap();
            let mut expected = LayoutOptions::process_map();
            expected.direction = options.layout.direction.unwrap_or(Direction::LeftRight);
            expected.labels.reservation = LabelReservation::Always;
            expected.tuning.labels.side_steps = SIDE_STEPS;
            assert_eq!(resolved, expected);
            let json = serde_json::to_value(&request).unwrap();
            assert!(json["nodes"][0].is_string());
            assert!(json["edges"][0]["arc"].is_number());
        }
    }

    #[test]
    fn markers_follow_the_arc_type() {
        use BuiltinMarker::*;
        let kind = |m: &MarkerRef| match m {
            MarkerRef::Builtin { marker, .. } => Some(*marker),
            _ => None,
        };
        let options = OCDeclareBuildOptions::new();
        for (arc_type, expected) in [
            (AS, (Some(Ball), None)),
            (EF, (Some(Ball), Some(ArrowCentered))),
            (DF, (Some(Ball), Some(ArrowBar))),
            (EP, (Some(BallArrow), None)),
            (DP, (Some(BallBarArrow), None)),
        ] {
            let edge = &scene_of(&[orders("a", "b", arc_type)], &options).0.edges[0];
            assert_eq!((kind(&edge.marker_start), kind(&edge.marker_end)), expected);
        }
        let merged = scene_of(&[orders("a", "b", EF), orders("b", "a", EP)], &options).0;
        assert_eq!(kind(&merged.edges[0].marker_end), Some(ArrowBall));
    }

    /// The ball marks the reference activity, so a backward arc must still start there.
    #[test]
    fn a_backward_arc_starts_at_its_source() {
        let (scene, nodes) = scene_of(&[orders("a", "b", EP)], &OCDeclareBuildOptions::new());
        let first = scene.edges[0].points[0];
        let distance = |name: &str| {
            let i = nodes.iter().position(|n| n.as_str() == name).unwrap();
            let c = scene.nodes[i].rect.center();
            (c.x - first.x).hypot(c.y - first.y)
        };
        assert!(distance("a") < distance("b"));
    }

    #[test]
    fn an_arc_takes_the_color_of_its_object_types() {
        let paint = |each: &[&str], any: &[&str]| {
            let arcs = [arc(("a", "b"), EF, [each, any, &[]], (Some(1), None))];
            scene_of(&arcs, &OCDeclareBuildOptions::new()).0.edges[0]
                .stroke
                .paint
                .clone()
        };
        let color = |t| key_color("objectType", t);
        assert_eq!(paint(&[], &[]), Paint::color(MARKER_COLOR));
        assert_eq!(paint(&["orders"], &[]), Paint::Color(color("orders")));
        // Each weighs 4 and any 1, so orders spans 0..0.8 and items 0.8..1.
        let Paint::Gradient(g) = paint(&["orders"], &["items"]) else {
            panic!("two types give a gradient");
        };
        let stops: Vec<(f64, String)> = g
            .stops
            .iter()
            .map(|s| (s.offset, s.color.clone()))
            .collect();
        assert_eq!(stops, [(0.4, color("orders")), (0.9, color("items"))]);
    }

    #[test]
    fn the_names_label_is_the_template_in_type_colors() {
        let mut arc = arc(
            ("a", "b"),
            EF,
            [&["orders"], &["packages"], &["items"]],
            (Some(1), None),
        );
        arc.label.any.push(ObjectTypeAssociation::O2O {
            first: "orders".into(),
            second: "items".into(),
            reversed: true,
        });
        let edge = &scene_of(&[arc.clone()], &OCDeclareBuildOptions::new())
            .0
            .edges[0];
        assert_eq!(texts(edge), [arc.label.as_template_string()]);
        let mut named = edge.labels[0]
            .text
            .spans
            .iter()
            .filter(|s| s.text == "orders");
        assert!(named.all(|s| s.color == Some(key_color("objectType", "orders"))));
    }

    #[test]
    fn count_labels_show_the_bounds() {
        for (counts, expected) in [
            ((Some(1), None), None),
            ((None, None), None),
            ((Some(0), None), None),
            ((Some(1), Some(1)), Some("= 1")),
            ((Some(2), None), Some("\u{2265} 2")),
            ((None, Some(1)), Some("\u{2264} 1")),
            ((Some(0), Some(1)), Some("\u{2264} 1")),
            ((Some(2), Some(5)), Some("2 \u{2013} 5")),
        ] {
            assert_eq!(count_label(counts).as_deref(), expected, "{counts:?}");
        }
        assert!(is_negated((Some(0), Some(0))) && is_negated((None, Some(0))));
        assert!(!is_negated((Some(0), Some(1))));
    }

    #[test]
    fn a_count_chip_sits_under_the_label_and_a_negated_arc_is_crossed_out() {
        let counted = [arc(
            ("a", "b"),
            EF,
            [&["orders"], &[], &[]],
            (Some(2), None),
        )];
        let edge = &scene_of(&counted, &OCDeclareBuildOptions::new()).0.edges[0];
        assert_eq!(texts(edge), ["Each(orders)", "\u{2265} 2"]);
        assert_eq!(edge.labels[1].text.dy, CHIP_ROW);

        let negated = [arc(
            ("a", "b"),
            EF,
            [&["orders"], &[], &[]],
            (Some(0), Some(0)),
        )];
        for labels in [OCDeclareLabels::Names, OCDeclareLabels::Dots] {
            let edge = &scene_of(&negated, &with(|o| o.labels = labels)).0.edges[0];
            assert!(texts(edge).iter().all(|t| t == "Each(orders)"));
            let (slashes, length) = (slashes(edge), position(edge, &EdgeAnchor::Fraction(1.0)));
            assert_eq!(slashes.len(), 2);
            assert!(
                slashes.iter().all(|&s| s > length / 2.0 && s < length),
                "{slashes:?}"
            );
        }
    }

    /// On a short arc, the end the `//` belongs at is taken by the discs, so it moves beside them.
    #[test]
    fn discs_sit_at_the_middle_and_clear_of_the_slashes() {
        let negated = [arc(
            ("a", "b"),
            EF,
            [&["orders"], &["items"], &["packages"]],
            (Some(0), Some(0)),
        )];
        let edge = &scene_of(&negated, &with(|o| o.labels = OCDeclareLabels::Dots))
            .0
            .edges[0];
        let discs = discs(edge);
        assert_eq!(discs.len(), 7);
        assert_eq!(
            discs.iter().filter(|(_, fill)| fill == "#ffffff").count(),
            2
        );
        let middle = position(edge, &EdgeAnchor::Fraction(0.5));
        for (d, _) in &discs {
            assert!((d - middle).abs() <= 30.0, "{d} is far from {middle}");
            assert!(slashes(edge).iter().all(|s| (s - d).abs() >= 8.0));
        }
    }

    #[test]
    fn a_negated_arc_has_room_for_its_slashes() {
        let negated = [arc(
            ("a", "b"),
            EF,
            [&["orders"], &["items"], &["packages"]],
            (Some(0), Some(0)),
        )];
        for labels in [OCDeclareLabels::Dots, OCDeclareLabels::Names] {
            let edge = &scene_of(&negated, &with(|o| o.labels = labels)).0.edges[0];
            let end = position(edge, &EdgeAnchor::Fraction(1.0));
            for s in slashes(edge) {
                assert!(
                    (s - (end - draw::SLASH_DISTANCE)).abs() <= draw::slash_half(),
                    "{labels:?}: the // at {s} left its spot before the end at {end}"
                );
            }
        }
        // No longer than the discs in the middle and the // beside them need.
        let edge = &scene_of(&negated, &with(|o| o.labels = OCDeclareLabels::Dots))
            .0
            .edges[0];
        let discs = label_size(&negated[0], OCDeclareLabels::Dots)
            .unwrap()
            .width;
        let needed =
            discs + 2.0 * (draw::SLASH_DISTANCE + draw::slash_half() + draw::SLASH_CLEARANCE);
        let end = position(edge, &EdgeAnchor::Fraction(1.0));
        assert!(
            end <= needed + 1.0,
            "{end} is longer than the {needed} needed"
        );
    }

    #[test]
    fn a_names_label_widens_the_gap_it_crosses() {
        let long = arc(
            ("a", "b"),
            EF,
            [
                &["orders", "customers"],
                &["items", "packages"],
                &["products"],
            ],
            (Some(1), None),
        );
        let gap = |labels| {
            let options = with(|o| o.labels = labels);
            let layout = model::layout(std::slice::from_ref(&long), &options).unwrap();
            let [a, b] = [&layout.nodes[0], &layout.nodes[1]];
            (b.center.x - a.center.x).abs() - (a.size.width + b.size.width) / 2.0
        };
        let chip = label_size(&long, OCDeclareLabels::Names).unwrap().width;
        assert!(gap(OCDeclareLabels::Names) >= chip);
        assert!(gap(OCDeclareLabels::Dots) < chip);
    }

    /// The layout may push a label off its line to avoid an overlap, but only until it just
    /// clears the line, which for a wide label beside an upright segment is half its width.
    #[test]
    fn every_label_stays_by_its_line() {
        let (scene, _) = scene_of(&sample_arcs(), &OCDeclareBuildOptions::new());
        for edge in &scene.edges {
            let widest = texts(edge)
                .iter()
                .map(|t| t.chars().count())
                .max()
                // With two rows, one item and the "+k" always fit, each on its own row.
                .unwrap_or(1);
            let reach = (NAME_CHAR_WIDTH * widest as f64 + 10.0) / 2.0 + 12.0;
            for label in &edge.labels {
                let EdgeAnchor::At(p) = label.at else {
                    continue;
                };
                let off = edge
                    .points
                    .windows(2)
                    .map(|w| {
                        let (a, b) = (w[0], w[1]);
                        let len2 = (b.x - a.x).powi(2) + (b.y - a.y).powi(2);
                        let t = (((p.x - a.x) * (b.x - a.x) + (p.y - a.y) * (b.y - a.y)) / len2)
                            .clamp(0.0, 1.0);
                        (p.x - a.x - t * (b.x - a.x)).hypot(p.y - a.y - t * (b.y - a.y))
                    })
                    .fold(f64::INFINITY, f64::min);
                assert!(off <= reach, "{} is {off} off its line", label.text.text);
            }
        }
    }

    #[test]
    fn involvements_show_under_the_activity() {
        let counts = |min, max| ObjectInvolvementCounts { min, max };
        let pay: BTreeMap<String, ObjectInvolvementCounts> = [
            ("orders", counts(1, 1)),
            ("items", counts(0, 5)),
            ("customers", counts(0, 0)),
        ]
        .into_iter()
        .map(|(t, c)| (t.to_string(), c))
        .collect();
        let arcs = [orders("place order", "pay", EF)];
        for labels in [OCDeclareLabels::Names, OCDeclareLabels::Dots] {
            let options = with(|o| {
                o.labels = labels;
                o.involvements.insert("pay".into(), pay.clone());
            });
            let (scene, nodes) = scene_of(&arcs, &options);
            let content = |name: &str| {
                &scene.nodes[nodes.iter().position(|n| n.as_str() == name).unwrap()].content
            };
            assert_eq!(content("place order").len(), 1);
            // By name, without customers. Items get three discs and, as an event may have none,
            // a dashed ring.
            match &content("pay")[1] {
                NodeContent::Glyphs(row) => {
                    let clusters: Vec<(u64, bool)> = row
                        .clusters
                        .iter()
                        .map(|c| (c.count, c.outline.as_ref().unwrap().dash.is_some()))
                        .collect();
                    assert_eq!(clusters, [(3, true), (1, false)]);
                }
                NodeContent::Text(run) => {
                    let text: String = run.spans.iter().map(|s| s.text.as_str()).collect();
                    assert_eq!(text, "items 0..*  orders 1..1");
                }
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn involvements_take_at_most_two_rows() {
        let many: BTreeMap<String, ObjectInvolvementCounts> = (0..30)
            .map(|i| {
                (
                    format!("t{i:02}"),
                    ObjectInvolvementCounts { min: 1, max: 1 },
                )
            })
            .collect();
        let arcs = [orders("place order", "pay", EF)];
        let options = with(|o| {
            o.labels = OCDeclareLabels::Dots;
            o.involvements.insert("pay".into(), many.clone());
        });
        let (scene, nodes) = scene_of(&arcs, &options);
        let pay = &scene.nodes[nodes.iter().position(|n| n.as_str() == "pay").unwrap()];
        // Name, two rows of discs and "+14"
        let rows: Vec<usize> = pay
            .content
            .iter()
            .filter_map(|c| match c {
                NodeContent::Glyphs(row) => Some(row.clusters.len()),
                _ => None,
            })
            .collect();
        assert_eq!(rows.iter().sum::<usize>(), 16);
        assert_eq!(rows.len(), 2);
        let texts: Vec<&str> = pay
            .content
            .iter()
            .filter_map(|c| match c {
                NodeContent::Text(run) => Some(run.text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(texts, ["pay", "+14"]);

        let options = with(|o| {
            o.labels = OCDeclareLabels::Names;
            o.involvements.insert("pay".into(), many.clone());
        });
        let (scene, nodes) = scene_of(&arcs, &options);
        let pay = &scene.nodes[nodes.iter().position(|n| n.as_str() == "pay").unwrap()];
        let lines: Vec<String> = pay.content[1..]
            .iter()
            .map(|c| match c {
                NodeContent::Text(run) => run.spans.iter().map(|s| s.text.as_str()).collect(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[1].ends_with("+25"), "{lines:?}");
    }

    #[test]
    fn extra_activities_become_nodes_without_arcs() {
        use crate::visualization::engine::layout_graph;
        let names = |request: &OCDeclareLayoutRequest| -> Vec<String> {
            request
                .nodes
                .iter()
                .map(|n| n.as_str().to_string())
                .collect()
        };
        let arcs = [orders("place order", "ship", EF)];
        let options = with(|o| o.extra_activities = vec!["review".into(), "ship".into()]);
        let request = model::layout_request(arcs.as_slice(), &options).unwrap();
        assert_eq!(names(&request), ["place order", "review", "ship"]);
        assert_eq!(request.edges.len(), 1);
        let layout = layout_graph(&request.request).unwrap();
        let style = OCDeclareStyle::default();
        let overrides = StyleOverrides::default();
        let scene =
            model::scene_from_layout(arcs.as_slice(), &layout, &options, &style, &overrides)
                .unwrap();
        assert_eq!((scene.nodes.len(), scene.edges.len()), (3, 1));

        let options = with(|o| o.extra_activities = vec!["review".into()]);
        let request = model::layout_request(&[][..], &options).unwrap();
        assert_eq!(names(&request), ["review"]);
        let layout = layout_graph(&request.request).unwrap();
        let scene =
            model::scene_from_layout(&[][..], &layout, &options, &style, &overrides).unwrap();
        assert_eq!(scene.nodes.len(), 1);
    }

    #[test]
    fn nodes_are_tinted_unless_plain() {
        let arcs = [orders("<init> orders", "pay", EF)];
        let base = BaseStyle::default();
        for plain in [false, true] {
            let style = OCDeclareStyle {
                plain_activities: plain,
                ..OCDeclareStyle::default()
            };
            let (scene, nodes) = styled_scene_of(&arcs, &OCDeclareBuildOptions::new(), &style);
            for (node, meta) in scene.nodes.iter().zip(&nodes) {
                let init = meta.as_str() != "pay";
                let color = match init {
                    true => key_color("objectType", "orders"),
                    false => key_color("activity", "pay"),
                };
                let fill = if plain {
                    base.node_fill.clone()
                } else {
                    lighten(&color, style.fill_lightness)
                };
                assert_eq!(node.fill, Some(fill), "{meta:?}");
                assert_eq!(node.stroke.as_ref().unwrap().dash.is_some(), init);
            }
        }
    }

    #[test]
    fn writes_the_svg_exports() {
        use crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;
        use crate::discovery::object_centric::oc_declare::discover_behavior_constraints;
        use crate::test_utils::{get_test_data_path, write_export};
        use crate::{Importable, OCEL};

        let counts = |min, max| ObjectInvolvementCounts { min, max };
        let involvements = [
            (
                "place order",
                [("order", counts(1, 1)), ("item", counts(1, 8))].as_slice(),
            ),
            (
                "ship",
                [("order", counts(0, 1)), ("item", counts(0, 5))].as_slice(),
            ),
            ("invoice", [("order", counts(1, 1))].as_slice()),
        ]
        .into_iter()
        .map(|(a, types)| {
            let types = types.iter().map(|(t, c)| (t.to_string(), c.clone()));
            (a.to_string(), types.collect())
        })
        .collect::<BTreeMap<_, _>>();
        for (labels, plain_activities, name) in [
            (OCDeclareLabels::Names, false, "oc-declare-sample.svg"),
            (OCDeclareLabels::Dots, false, "oc-declare-sample-dots.svg"),
            (OCDeclareLabels::Names, true, "oc-declare-sample-plain.svg"),
        ] {
            let options = with(|o| {
                o.labels = labels;
                o.involvements = involvements.clone();
            });
            let style = OCDeclareStyle {
                plain_activities,
                ..OCDeclareStyle::default()
            };
            let svg = model::svg(sample_arcs().as_slice(), &options, &style).unwrap();
            assert!(svg.contains("data-template"));
            write_export(name, &svg);
        }

        let path = get_test_data_path()
            .join("ocel")
            .join("pm4py-ocel20_example.jsonocel");
        let locel = SlimLinkedOCEL::from_ocel(OCEL::import_from_path(path).unwrap());
        let arcs = discover_behavior_constraints(&locel, Default::default());
        let options = OCDeclareBuildOptions::new().with_involvements_of(&locel);
        let svg = model::svg(arcs.as_slice(), &options, &OCDeclareStyle::default()).unwrap();
        write_export("pm4py-ocel20-OC-DECLARE.svg", &svg);
    }

    /// A node too narrow for even one involvement shows the first one and how many it leaves out.
    #[test]
    fn a_strip_without_room_for_any_item_still_counts_the_rest() {
        let counts = ObjectInvolvementCounts { min: 1, max: 1 };
        let types: Vec<String> = ["orders", "items", "packages"].map(String::from).to_vec();
        let involved: Vec<(&String, &ObjectInvolvementCounts)> =
            types.iter().map(|t| (t, &counts)).collect();
        let strip = draw::involvement_strip(&involved, OCDeclareLabels::Names, 22.0);
        let text: Vec<String> = strip
            .iter()
            .map(|c| match c {
                NodeContent::Text(run) => run.spans.iter().map(|s| s.text.as_str()).collect(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(text, ["orders 1..1  +2"]);
        let strip = draw::involvement_strip(&involved, OCDeclareLabels::Dots, 22.0);
        assert!(
            matches!(&strip[..], [NodeContent::Glyphs(row), NodeContent::Text(plus)]
            if row.clusters.len() == 1 && plus.text == "+2")
        );
    }

    #[test]
    fn involvements_are_read_per_activity() {
        use crate::test_utils::get_test_data_path;
        use crate::{Importable, OCEL};
        let path = get_test_data_path()
            .join("ocel")
            .join("order-management.json");
        let locel = SlimLinkedOCEL::from_ocel(OCEL::import_from_path(path).unwrap());
        let involvements = oc_declare_involvements(&locel);
        assert!(involvements.contains_key("place order"), "{involvements:?}");
        let options = OCDeclareBuildOptions::new().with_involvements_of(&locel);
        assert_eq!(options.involvements, involvements);
    }
}
