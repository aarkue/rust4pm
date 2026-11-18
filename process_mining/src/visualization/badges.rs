//! Object types drawn as a row of badges at the bottom of an activity's node, in the style of
//! OC-DECLARE's involvement discs. Object-centric Petri nets, DFGs and OC-DFGs take them in their
//! build options, by activity.
//!
//! A badge is a disc in its object type's color ([`key_color`] in the `"objectType"` scope) with
//! a border mixed [`BADGE_OUTLINE_MIX`] towards the text color: three discs [`BADGE_STEP`] apart
//! when an event may have several objects of the type, and a dashed border when it may have none
//! ([`InvolvementCard`]). Badges with a `from` type are grouped by it, in first-seen order: the
//! source's disc, an arrow, then the group's types. The badges without one come last, as one
//! group of plain discs.
//!
//! One geometry sizes the nodes and places the discs, so a host drawing the badges itself can
//! follow the same numbers. All widths include the border, as on screen:
//!
//! - a disc is `2 * BADGE_RADIUS + 2 * BADGE_OUTLINE_WIDTH` wide and high, with
//!   [`BADGE_DASHED_OUTLINE_WIDTH`] for a dashed one, plus `2 * BADGE_STEP` for three discs
//!   ([`badge_disc_width`]);
//! - in a group, [`BADGE_GAP`] separates two discs, and a disc from the arrow, which is
//!   [`BADGE_ARROW_WIDTH`] wide;
//! - [`BADGE_GROUP_GAP`] separates two groups, and the groups are centered in the node
//!   ([`badge_row`], [`badge_row_width`]);
//! - a node with badges is [`BADGE_ROW_HEIGHT`] higher and at least the row's width plus
//!   `2 * BADGE_PADDING` wide ([`badge_node_size`]). Its other content moves up by half the row
//!   height, and the discs are centered [`BADGE_ROW_BOTTOM`] above its bottom edge;
//! - a node with badges and an annotated line is [`BADGE_NOTE_HEIGHT`] higher still. The line
//!   goes at the bottom, the discs that much higher, and the other content up by half as much.
//!
//! [`key_color`]: crate::visualization::key_color

use std::collections::BTreeMap;

use graphlay::svg::{
    ExactLayout, GlyphCluster, GlyphLayout, GlyphRow, NodeContent, Paint, SceneNode, Separator,
    Stroke, SYMBOL_MAPSTO,
};
use graphlay::Size;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::process_models::object_centric::oc_declare::ObjectInvolvementCounts;
use crate::visualization::styling::{extra_line, key_color, mix, push_extra_line, BaseStyle};

/// How many objects of a type an event has, as OC-DECLARE's involvement classes: at least none
/// or one, at most one or several.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
pub enum InvolvementCard {
    /// At most one, maybe none.
    #[serde(rename = "0..1")]
    ZeroOrOne,
    /// Any number, maybe none.
    #[serde(rename = "0..*")]
    ZeroOrMore,
    /// Exactly one.
    #[serde(rename = "1..1")]
    One,
    /// At least one.
    #[serde(rename = "1..*")]
    OneOrMore,
}

impl InvolvementCard {
    /// The class of events with between `min` and `max` objects of a type.
    pub fn of(min: usize, max: usize) -> Self {
        match (min == 0, max > 1) {
            (true, false) => InvolvementCard::ZeroOrOne,
            (true, true) => InvolvementCard::ZeroOrMore,
            (false, false) => InvolvementCard::One,
            (false, true) => InvolvementCard::OneOrMore,
        }
    }

    /// Whether an event may have none, drawn as a dashed border.
    pub fn optional(self) -> bool {
        matches!(
            self,
            InvolvementCard::ZeroOrOne | InvolvementCard::ZeroOrMore
        )
    }

    /// Whether an event may have several, drawn as three discs.
    pub fn many(self) -> bool {
        matches!(
            self,
            InvolvementCard::ZeroOrMore | InvolvementCard::OneOrMore
        )
    }
}

impl From<&ObjectInvolvementCounts> for InvolvementCard {
    fn from(counts: &ObjectInvolvementCounts) -> Self {
        InvolvementCard::of(counts.min, counts.max)
    }
}

/// An object type drawn as a badge in the node of an activity, e.g., one that takes part without
/// places or edges of its own, alone or after the badge of the type it follows from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TypeBadge {
    /// The object type the badge stands for.
    pub object_type: String,
    /// The object type the badge follows from, drawn as a disc and an arrow before the badges
    /// that follow from it.
    #[serde(default)]
    pub from: Option<String>,
    /// How many objects of `object_type` an event has. `None` draws one disc with a solid
    /// border, as `"1..1"`.
    #[serde(default)]
    pub card: Option<InvolvementCard>,
    /// How many objects of `from` an event has. A group takes that of its first badge.
    #[serde(default)]
    pub from_card: Option<InvolvementCard>,
}

/// Radius of a disc.
pub const BADGE_RADIUS: f64 = 4.33;
/// Center-to-center distance of the three discs of a type an event may have several of.
pub const BADGE_STEP: f64 = 3.1;
/// Border width of a solid disc, also the room it keeps for its border.
pub const BADGE_OUTLINE_WIDTH: f64 = 1.0;
/// Border width of a dashed disc, also the room it keeps for its border.
pub const BADGE_DASHED_OUTLINE_WIDTH: f64 = 1.3;
/// SVG dash pattern of the border of a type an event may have none of.
pub const BADGE_DASH: &str = "2.2 1.6";
/// How far a disc's border is mixed from its fill towards the text color.
pub const BADGE_OUTLINE_MIX: f64 = 0.35;
/// Gap between two discs of a group, and between a disc and the arrow.
pub const BADGE_GAP: f64 = 2.0;
/// Gap between two groups.
pub const BADGE_GROUP_GAP: f64 = 10.0;
/// Width of the arrow after a group's source disc.
pub const BADGE_ARROW_WIDTH: f64 = 9.0;
/// Height of that arrow, centered on the discs.
pub const BADGE_ARROW_HEIGHT: f64 = 8.0;
/// Height a row of badges adds to a node.
pub const BADGE_ROW_HEIGHT: f64 = 16.0;
/// Distance from a node's bottom edge to the center of its discs.
pub const BADGE_ROW_BOTTOM: f64 = 10.0;
/// Least room left and right of the row in its node.
pub const BADGE_PADDING: f64 = 8.0;
/// Height an annotated line under the badges adds to a node, for the default edge label size.
pub const BADGE_NOTE_HEIGHT: f64 = 14.0;

/// Width of the badge of `card`, border included.
pub fn badge_disc_width(card: Option<InvolvementCard>) -> f64 {
    let many = card.is_some_and(InvolvementCard::many);
    2.0 * BADGE_RADIUS + 2.0 * outline_width(card) + if many { 2.0 * BADGE_STEP } else { 0.0 }
}

fn outline_width(card: Option<InvolvementCard>) -> f64 {
    if card.is_some_and(InvolvementCard::optional) {
        BADGE_DASHED_OUTLINE_WIDTH
    } else {
        BADGE_OUTLINE_WIDTH
    }
}

/// A group of a [`BadgeRow`]: the badges following from one type, or those following from none.
#[derive(Debug, Clone, PartialEq)]
pub struct BadgeGroup<'a> {
    /// The type the badges follow from and how many objects of it an event has, drawn as a disc
    /// and an arrow first. `None` for the badges without a `from` type.
    pub from: Option<(&'a str, Option<InvolvementCard>)>,
    /// The badges, in input order.
    pub badges: Vec<&'a TypeBadge>,
    /// Left edge of the group, from the left edge of the row.
    pub x: f64,
    /// Width of the group.
    pub width: f64,
}

/// Where the badges of a node go, left to right.
#[derive(Debug, Clone, PartialEq)]
pub struct BadgeRow<'a> {
    /// The groups: those with a `from` type in first-seen order, then the others.
    pub groups: Vec<BadgeGroup<'a>>,
    /// Width of the whole row.
    pub width: f64,
}

/// The groups of `badges` and where they go.
pub fn badge_row(badges: &[TypeBadge]) -> BadgeRow<'_> {
    let mut groups: Vec<BadgeGroup<'_>> = Vec::new();
    for badge in badges {
        let Some(from) = badge.from.as_deref() else {
            continue;
        };
        match groups
            .iter_mut()
            .find(|g| g.from.is_some_and(|(f, _)| f == from))
        {
            Some(g) => g.badges.push(badge),
            None => groups.push(BadgeGroup {
                from: Some((from, badge.from_card)),
                badges: vec![badge],
                x: 0.0,
                width: 0.0,
            }),
        }
    }
    let involved: Vec<&TypeBadge> = badges.iter().filter(|b| b.from.is_none()).collect();
    if !involved.is_empty() {
        groups.push(BadgeGroup {
            from: None,
            badges: involved,
            x: 0.0,
            width: 0.0,
        });
    }
    let mut x = 0.0;
    for (i, g) in groups.iter_mut().enumerate() {
        if i > 0 {
            x += BADGE_GROUP_GAP;
        }
        let discs: f64 = g.badges.iter().map(|b| badge_disc_width(b.card)).sum();
        let source = g.from.map_or(0.0, |(_, card)| {
            badge_disc_width(card) + BADGE_GAP + BADGE_ARROW_WIDTH + BADGE_GAP
        });
        g.x = x;
        g.width = source + discs + BADGE_GAP * g.badges.len().saturating_sub(1) as f64;
        x += g.width;
    }
    BadgeRow { groups, width: x }
}

/// Width of the row of `badges`.
pub fn badge_row_width(badges: &[TypeBadge]) -> f64 {
    badge_row(badges).width
}

/// A node of `size` with room for a row of `badges`, and with `note` for an annotated line under
/// them.
pub fn badge_node_size(size: Size, badges: &[TypeBadge], note: bool) -> Size {
    if badges.is_empty() {
        return size;
    }
    Size::new(
        size.width
            .max(badge_row_width(badges) + 2.0 * BADGE_PADDING),
        size.height + BADGE_ROW_HEIGHT + note_height(note),
    )
}

fn note_height(note: bool) -> f64 {
    if note {
        BADGE_NOTE_HEIGHT
    } else {
        0.0
    }
}

/// The badges of `activity`, none for a node without one.
pub(crate) fn badges_of<'a>(
    badges: &'a BTreeMap<String, Vec<TypeBadge>>,
    activity: Option<&str>,
) -> &'a [TypeBadge] {
    activity
        .and_then(|a| badges.get(a))
        .map_or(&[], Vec::as_slice)
}

/// One disc, or three, of `object_type`.
fn disc(
    object_type: &str,
    card: Option<InvolvementCard>,
    text: &str,
    separator: Option<Separator>,
) -> GlyphCluster {
    let color = key_color("objectType", object_type);
    let optional = card.is_some_and(InvolvementCard::optional);
    GlyphCluster {
        count: if card.is_some_and(InvolvementCard::many) {
            3
        } else {
            1
        },
        radius: BADGE_RADIUS,
        step: Some(BADGE_STEP),
        outline: Some(Stroke {
            paint: Paint::Color(mix(&color, text, BADGE_OUTLINE_MIX)),
            width: outline_width(card),
            dash: optional.then(|| BADGE_DASH.to_string()),
            ..Stroke::default()
        }),
        color: Some(color),
        separator,
        ..GlyphCluster::default()
    }
}

/// The row of `badges` and the annotated line `note` at the bottom of `node`, whose other content
/// moves up to make room. The node must be [`badge_node_size`] large.
pub(crate) fn push_badges(
    node: &mut SceneNode,
    badges: &[TypeBadge],
    note: Option<&str>,
    base: &BaseStyle,
) {
    if badges.is_empty() {
        if let Some(note) = note {
            push_extra_line(node, note, base);
        }
        return;
    }
    let below = note_height(note.is_some());
    let shift = (BADGE_ROW_HEIGHT + below) / 2.0;
    for content in &mut node.content {
        match content {
            NodeContent::Text(run) => run.dy -= shift,
            NodeContent::Glyphs(row) => row.dy -= shift,
            _ => {}
        }
    }
    let text = base.text.as_str();
    let row = badge_row(badges);
    let dy = node.rect.h / 2.0 - BADGE_ROW_BOTTOM - below;
    // A glyph cluster is as wide as its discs without their borders, so the gaps between
    // clusters take the borders on.
    let gap = BADGE_GAP + BADGE_OUTLINE_WIDTH;
    for group in &row.groups {
        let mut clusters = Vec::new();
        if let Some((from, card)) = group.from {
            clusters.push(disc(from, card, text, None));
        }
        for (i, badge) in group.badges.iter().enumerate() {
            let arrow = (i == 0 && group.from.is_some()).then(|| Separator {
                symbol: SYMBOL_MAPSTO.to_string(),
                width: BADGE_ARROW_WIDTH,
                height: BADGE_ARROW_HEIGHT,
                gap_before: gap,
                gap_after: gap,
                color: None,
            });
            clusters.push(disc(&badge.object_type, badge.card, text, arrow));
        }
        node.content.push(NodeContent::Glyphs(GlyphRow {
            clusters,
            dx: group.x + group.width / 2.0 - row.width / 2.0,
            dy,
            layout: GlyphLayout::Exact(ExactLayout {
                cluster_gap: gap + BADGE_OUTLINE_WIDTH,
                ..ExactLayout::default()
            }),
            ..GlyphRow::default()
        }));
    }
    if let Some(note) = note {
        node.content.push(extra_line(node, note, base));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphlay::svg::Rect;

    fn badge(object_type: &str, from: Option<&str>, card: &str) -> TypeBadge {
        serde_json::from_value(serde_json::json!({
            "object_type": object_type,
            "from": from,
            "card": card,
            "from_card": "1..*",
        }))
        .unwrap()
    }

    fn rows(node: &SceneNode) -> Vec<&GlyphRow> {
        node.content
            .iter()
            .filter_map(|c| match c {
                NodeContent::Glyphs(row) => Some(row),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn cards_are_the_involvement_classes() {
        for (min, max, json) in [
            (0, 1, "0..1"),
            (0, 5, "0..*"),
            (1, 1, "1..1"),
            (2, 3, "1..*"),
        ] {
            let card = InvolvementCard::of(min, max);
            assert_eq!(serde_json::to_value(card).unwrap(), json);
            assert_eq!(card.optional(), min == 0);
            assert_eq!(card.many(), max > 1);
        }
    }

    /// Groups by source in first-seen order, then the involved types, each width from the shared
    /// geometry.
    #[test]
    fn badges_group_by_source_and_the_involved_ones_come_last() {
        let badges = [
            badge("customers", None, "1..1"),
            badge("items", Some("orders"), "1..*"),
            badge("employees", Some("packages"), "0..1"),
            badge("products", Some("orders"), "0..*"),
        ];
        let row = badge_row(&badges);
        let names = |g: &BadgeGroup<'_>| {
            g.badges
                .iter()
                .map(|b| b.object_type.clone())
                .collect::<Vec<_>>()
        };
        let froms: Vec<_> = row.groups.iter().map(|g| g.from.map(|f| f.0)).collect();
        assert_eq!(froms, [Some("orders"), Some("packages"), None]);
        assert_eq!(names(&row.groups[0]), ["items", "products"]);
        assert_eq!(names(&row.groups[2]), ["customers"]);

        let single = 2.0 * BADGE_RADIUS + 2.0 * BADGE_OUTLINE_WIDTH;
        let many = single + 2.0 * BADGE_STEP;
        let single_dashed = 2.0 * BADGE_RADIUS + 2.0 * BADGE_DASHED_OUTLINE_WIDTH;
        let many_dashed = single_dashed + 2.0 * BADGE_STEP;
        let arrow = BADGE_GAP + BADGE_ARROW_WIDTH + BADGE_GAP;
        let widths: Vec<f64> = row.groups.iter().map(|g| g.width).collect();
        let expected = [
            many + arrow + many + BADGE_GAP + many_dashed,
            many + arrow + single_dashed,
            single,
        ];
        for (w, e) in widths.iter().zip(expected) {
            assert!((w - e).abs() < 1e-9, "{widths:?} {expected:?}");
        }
        assert_eq!(row.groups[1].x, widths[0] + BADGE_GROUP_GAP);
        assert_eq!(
            row.width,
            widths.iter().sum::<f64>() + 2.0 * BADGE_GROUP_GAP
        );
        let size = badge_node_size(Size::new(20.0, 52.0), &badges, false);
        assert_eq!(size.width, row.width + 2.0 * BADGE_PADDING);
        assert_eq!(size.height, 52.0 + BADGE_ROW_HEIGHT);
        let noted = badge_node_size(Size::new(20.0, 52.0), &badges, true);
        assert_eq!(noted.height, size.height + BADGE_NOTE_HEIGHT);
        assert_eq!(
            badge_node_size(Size::new(20.0, 52.0), &[], true),
            Size::new(20.0, 52.0)
        );
    }

    /// Three discs for several objects, a dashed border for none, and an arrow only after the
    /// source of a group. Each group's glyphs span its place in the row, borders aside. The gaps
    /// between glyphs leave room for solid borders, so a dashed disc's wider border is spare room.
    #[test]
    fn discs_follow_the_cards_and_groups_sit_where_the_geometry_says() {
        let badges = [
            badge("customers", None, "0..1"),
            badge("items", Some("orders"), "1..*"),
        ];
        let row = badge_row(&badges);
        let mut node = SceneNode {
            rect: Rect::new(0.0, 0.0, row.width + 2.0 * BADGE_PADDING, 68.0),
            ..SceneNode::default()
        };
        let base = BaseStyle {
            text: "#111827".to_string(),
            ..BaseStyle::default()
        };
        push_badges(&mut node, &badges, None, &base);
        let glyphs = rows(&node);
        assert_eq!(glyphs.len(), 2);
        let (implied, involved) = (glyphs[0], glyphs[1]);
        assert_eq!(implied.clusters.len(), 2);
        assert_eq!(implied.clusters[0].count, 3);
        assert!(implied.clusters[0].separator.is_none());
        let arrow = implied.clusters[1].separator.as_ref().unwrap();
        assert_eq!(arrow.symbol, SYMBOL_MAPSTO);
        assert!(involved.clusters.iter().all(|c| c.separator.is_none()));
        let outline = |c: &GlyphCluster| c.outline.clone().unwrap();
        assert_eq!(involved.clusters[0].count, 1);
        assert_eq!(
            outline(&involved.clusters[0]).dash.as_deref(),
            Some(BADGE_DASH)
        );
        assert_eq!(
            outline(&involved.clusters[0]).width,
            BADGE_DASHED_OUTLINE_WIDTH
        );
        assert_eq!(outline(&implied.clusters[1]).dash, None);
        let color = key_color("objectType", "customers");
        assert_eq!(
            outline(&involved.clusters[0]).paint,
            Paint::Color(mix(&color, "#111827", BADGE_OUTLINE_MIX))
        );
        assert_eq!(involved.dy, 34.0 - BADGE_ROW_BOTTOM);

        for (g, glyph_row) in row.groups.iter().zip(&glyphs) {
            let GlyphLayout::Exact(exact) = &glyph_row.layout else {
                panic!("an exact row");
            };
            let inner: f64 = glyph_row
                .clusters
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let gap = match &c.separator {
                        Some(s) => s.gap_before + s.width + s.gap_after,
                        None if i > 0 => exact.cluster_gap,
                        None => 0.0,
                    };
                    c.width() + gap
                })
                .sum();
            let dashed: f64 = g
                .from
                .map(|(_, card)| card)
                .into_iter()
                .chain(g.badges.iter().map(|b| b.card))
                .map(|card| 2.0 * (outline_width(card) - BADGE_OUTLINE_WIDTH))
                .sum();
            let borders = 2.0 * BADGE_OUTLINE_WIDTH + dashed;
            assert!((inner - (g.width - borders)).abs() < 1e-9);
            let center = g.x + g.width / 2.0 - row.width / 2.0;
            assert!((glyph_row.dx - center).abs() < 1e-9);
        }
    }

    /// The annotated line goes at the bottom, the discs right above it, and the label stays
    /// centered in the rest of the node.
    #[test]
    fn badges_sit_above_an_annotated_line() {
        use graphlay::svg::TextRun;
        let badges = [badge("items", Some("orders"), "0..*")];
        let base = BaseStyle::default();
        let size = badge_node_size(Size::new(80.0, 52.0), &badges, true);
        let mut node = SceneNode {
            rect: Rect::new(0.0, 0.0, size.width, size.height),
            content: vec![NodeContent::Text(TextRun {
                text: "label".to_string(),
                ..TextRun::default()
            })],
            ..SceneNode::default()
        };
        push_badges(&mut node, &badges, Some("2 days"), &base);
        let texts: Vec<&TextRun> = node
            .content
            .iter()
            .filter_map(|c| match c {
                NodeContent::Text(run) => Some(run),
                _ => None,
            })
            .collect();
        let (label, note) = (texts[0], texts[1]);
        assert_eq!(note.text, "2 days");
        let top = -size.height / 2.0;
        assert_eq!(label.dy, top + 52.0 / 2.0);
        let disc_bottom = rows(&node)[0].dy + BADGE_RADIUS + BADGE_DASHED_OUTLINE_WIDTH;
        let note_top = note.dy - base.edge_label_size / 2.0;
        assert!(disc_bottom < note_top, "{disc_bottom} {note_top}");
        assert!(note.dy + base.edge_label_size / 2.0 < size.height / 2.0);
    }
}
