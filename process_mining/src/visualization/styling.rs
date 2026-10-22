//! Colors and [`Scene`](graphlay::svg::Scene) helpers shared by all model drawings.

/// `color` moved `amount` (0 to 1) towards white. A color that is not `#rrggbb` is kept.
pub fn lighten(color: &str, amount: f64) -> String {
    let Some(rgb) = parse_hex(color) else {
        return color.to_string();
    };
    let amount = amount.clamp(0.0, 1.0);
    let mix = |v: u8| (v as f64 + (255.0 - v as f64) * amount).round() as u8;
    format!("#{:02x}{:02x}{:02x}", mix(rgb.0), mix(rgb.1), mix(rgb.2))
}

/// `color` moved `amount` (0 to 1) towards black. A color that is not `#rrggbb` is kept.
pub fn darken(color: &str, amount: f64) -> String {
    let Some(rgb) = parse_hex(color) else {
        return color.to_string();
    };
    let keep = 1.0 - amount.clamp(0.0, 1.0);
    let mix = |v: u8| (v as f64 * keep).round() as u8;
    format!("#{:02x}{:02x}{:02x}", mix(rgb.0), mix(rgb.1), mix(rgb.2))
}

/// The color propel's `colorForKey` gives `key` in `scope` (`"objectType"` or `"activity"`).
pub fn key_color(scope: &str, key: &str) -> String {
    const SEED: u32 = 0x0101_cedc;
    const L_TIERS: [f64; 3] = [0.62, 0.7, 0.55];
    const C_TIERS: [f64; 2] = [0.15, 0.11];
    let mut h = format!("{scope}:{key}")
        .encode_utf16()
        .fold(SEED, |h, c| h.wrapping_mul(31).wrapping_add(u32::from(c)));
    // Seeds that differ only in their last character would otherwise get neighboring hues.
    h ^= h >> 16;
    h = h.wrapping_mul(0x045d_9f3b);
    h ^= h >> 16;
    oklch_hex(
        L_TIERS[(h >> 9) as usize % 3],
        C_TIERS[(h >> 12) as usize % 2],
        f64::from(h % 360),
    )
}

/// OKLCH to `#rrggbb`, with Ottosson's matrices. Out-of-gamut channels are clamped.
fn oklch_hex(l: f64, c: f64, hue_deg: f64) -> String {
    let hr = hue_deg * std::f64::consts::PI / 180.0;
    let (a, b) = (c * hr.cos(), c * hr.sin());
    let l_ = (l + 0.396_337_777_4 * a + 0.215_803_757_3 * b).powi(3);
    let m_ = (l - 0.105_561_345_8 * a - 0.063_854_172_8 * b).powi(3);
    let s_ = (l - 0.089_484_177_5 * a - 1.291_485_548 * b).powi(3);
    let byte = |x: f64| {
        let encoded = if x <= 0.003_130_8 {
            12.92 * x
        } else {
            1.055 * x.powf(1.0 / 2.4) - 0.055
        };
        (encoded * 255.0).round().clamp(0.0, 255.0) as u8
    };
    format!(
        "#{:02x}{:02x}{:02x}",
        byte(4.076_741_662_1 * l_ - 3.307_711_591_3 * m_ + 0.230_969_929_2 * s_),
        byte(-1.268_438_004_6 * l_ + 2.609_757_401_1 * m_ - 0.341_319_396_5 * s_),
        byte(-0.004_196_086_3 * l_ - 0.703_418_614_7 * m_ + 1.707_614_701 * s_),
    )
}

fn parse_hex(color: &str) -> Option<(u8, u8, u8)> {
    let hex = color.strip_prefix('#')?;
    if hex.len() != 6 {
        return None;
    }
    Some((
        u8::from_str_radix(&hex[0..2], 16).ok()?,
        u8::from_str_radix(&hex[2..4], 16).ok()?,
        u8::from_str_radix(&hex[4..6], 16).ok()?,
    ))
}

/// Colors, sizes and paddings shared by all model drawings.
#[derive(Debug, Clone, PartialEq)]
pub struct BaseStyle {
    /// Page background. `None` leaves the SVG transparent.
    pub background: Option<String>,
    /// Space kept around the drawing's bounding box, in px.
    pub padding: f64,
    /// Default node border color.
    pub node_border: String,
    /// Default node fill.
    pub node_fill: String,
    /// Default label color.
    pub text: String,
    /// Default edge (and arrowhead) color.
    pub edge: String,
    /// Node label size in px.
    pub label_size: f64,
    /// Edge label size in px.
    pub edge_label_size: f64,
    /// Corner radius of a box-shaped node.
    pub corner_radius: f64,
    /// Node border width in px.
    pub border_width: f64,
}

impl Default for BaseStyle {
    fn default() -> Self {
        BaseStyle {
            background: Some("#ffffff".to_string()),
            padding: 24.0,
            node_border: "#1f2937".to_string(),
            node_fill: "#ffffff".to_string(),
            text: "#111827".to_string(),
            edge: "#374151".to_string(),
            label_size: 12.5,
            edge_label_size: 10.5,
            corner_radius: 4.0,
            border_width: 1.75,
        }
    }
}

pub use scene_build::*;

mod scene_build {
    use super::BaseStyle;
    use graphlay::layout::{EdgeRoute, Placement};
    use graphlay::svg::{
        render, Attrs, BuiltinMarker, Chip, DrawOrder, EdgeAnchor, EdgeLabel, FontSpec,
        HeuristicMeasurer, MarkerRef, NodeContent, Point as SPoint, Rect as SRect, Scene,
        SceneEdge, SceneNode, Shape as SShape, Stroke, TextRun, TextWrap, Theme,
    };
    use graphlay::{Point, Shape, Size};

    /// A layout point as a scene point.
    pub fn scene_point(p: Point) -> SPoint {
        SPoint::new(p.x, p.y)
    }

    fn scene_rect(center: Point, size: Size) -> SRect {
        SRect::new(
            center.x - size.width / 2.0,
            center.y - size.height / 2.0,
            size.width,
            size.height,
        )
    }

    fn scene_shape(shape: Shape, corner_radius: f64) -> SShape {
        match shape {
            Shape::Ellipse => SShape::Ellipse,
            Shape::Box => SShape::Rect { corner_radius },
        }
    }

    /// A scene node covering the placement's box.
    pub fn scene_node(
        placement: &Placement,
        shape: Shape,
        fill: Option<String>,
        stroke: Option<Stroke>,
        base: &BaseStyle,
    ) -> SceneNode {
        SceneNode {
            rect: scene_rect(placement.center, placement.size),
            shape: scene_shape(shape, base.corner_radius),
            fill,
            stroke,
            content: Vec::new(),
            attrs: Attrs::default(),
        }
    }

    /// A centered node label, wrapped into at most `lines` lines.
    pub fn node_label(text: &str, width: f64, lines: usize, base: &BaseStyle) -> NodeContent {
        NodeContent::Text(TextRun {
            text: text.to_string(),
            size: Some(base.label_size),
            weight: Some(500.0),
            color: Some(base.text.clone()),
            wrap: Some(TextWrap {
                max_width: (width - 12.0).max(24.0),
                max_lines: lines.max(1),
                line_height: base.label_size + 1.5,
            }),
            ..TextRun::default()
        })
    }

    /// A play triangle for a start node or a stop square for an end node, `size` across.
    pub fn terminal_mark(start: bool, size: f64, color: &str) -> NodeContent {
        let r = size / 2.0;
        let points = if start {
            vec![SPoint::new(-r, -r), SPoint::new(-r, r), SPoint::new(r, 0.0)]
        } else {
            vec![
                SPoint::new(-r, -r),
                SPoint::new(r, -r),
                SPoint::new(r, r),
                SPoint::new(-r, r),
            ]
        };
        NodeContent::Polygon {
            points,
            fill: Some(color.to_string()),
            stroke: None,
            attrs: Attrs::default(),
        }
    }

    /// A scene edge along the route, with an arrowhead at the target.
    pub fn scene_edge(route: &EdgeRoute, color: &str, width: f64) -> Option<SceneEdge> {
        if route.points.len() < 2 {
            return None;
        }
        Some(SceneEdge {
            points: route.points.iter().copied().map(scene_point).collect(),
            stroke: Stroke::new(color, width),
            corner_radius: 4.0,
            marker_end: MarkerRef::builtin(BuiltinMarker::Arrow),
            ..SceneEdge::default()
        })
    }

    /// A mid-edge label on a chip, where the layout reserved room for it.
    pub fn edge_label(text: &str, route: &EdgeRoute, base: &BaseStyle) -> EdgeLabel {
        EdgeLabel {
            text: TextRun {
                text: text.to_string(),
                size: Some(base.edge_label_size),
                color: Some(base.text.clone()),
                ..TextRun::default()
            },
            at: match route.label_anchor {
                Some(p) => EdgeAnchor::At(scene_point(p)),
                None => EdgeAnchor::Fraction(0.5),
            },
            // With a glyph halo, an edge under the text shows between the letters and looks
            // dotted. A filled chip hides it.
            chip: Some(Chip {
                fill: "#ffffff".to_string(),
                ..Chip::default()
            }),
            attrs: Attrs::default(),
        }
    }

    /// The scene holding `nodes` and `edges` in this style.
    pub fn scene(base: &BaseStyle, nodes: Vec<SceneNode>, edges: Vec<SceneEdge>) -> Scene {
        Scene {
            background: base.background.clone(),
            padding: base.padding,
            nodes,
            edges,
            order: DrawOrder::EdgesUnderNodes,
            font: FontSpec::default(),
            ..Scene::default()
        }
    }

    /// Draw a scene to a standalone SVG document.
    pub fn render_scene(scene: &Scene) -> String {
        render(scene, &Theme::default(), &HeuristicMeasurer::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lighten_and_darken_mix_towards_white_and_black() {
        assert_eq!(lighten("#000000", 0.5), "#808080");
        assert_eq!(darken("#ffffff", 0.25), "#bfbfbf");
        assert_eq!(lighten("currentColor", 0.5), "currentColor");
    }

    /// The values propel's `colorForKey` gives, so a name has the same color in both.
    #[test]
    fn key_colors_match_propel() {
        for (scope, key, hex) in [
            ("objectType", "orders", "#d48a5c"),
            ("objectType", "items", "#007abe"),
            ("objectType", "Ünï", "#c685be"),
            ("activity", "place order", "#00838e"),
            ("activity", "pay", "#e58038"),
        ] {
            assert_eq!(key_color(scope, key), hex, "{scope}:{key}");
        }
    }
}
