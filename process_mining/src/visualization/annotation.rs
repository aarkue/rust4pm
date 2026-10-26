//! Labels, layout weights and line widths a host gives model elements before the layout, e.g.,
//! durations instead of counts.
//!
//! The build options of a model key them by model element, e.g., a DFG edge by its source and
//! target node, so a host can set them before it knows the graph's indices. In JSON, each map is a
//! list of `[key, annotation]` pairs. The layout reserves room for an annotated label exactly as
//! for the model's own.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A line a node shows under its label, e.g., an average duration.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct NodeAnnotation {
    /// The line's text. An empty string draws no line.
    pub label: Option<String>,
}

/// How an edge is labeled, weighted and drawn, instead of the model's defaults.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct EdgeAnnotation {
    /// Replaces the edge's label, e.g., its count. An empty string draws no label.
    pub label: Option<String>,
    /// Layout weight. A heavier edge is drawn shorter and straighter. A weight that is not finite
    /// and `> 0` is ignored, so the edge keeps the model's weight.
    pub weight: Option<f64>,
    /// Line width in px, which the layout also keeps between edges sharing a port. A width that is
    /// not finite and `>= 0` is ignored, so the edge keeps the model's width.
    pub thickness: Option<f64>,
}

/// The label `annotation` gives an edge, or `default` if it sets none.
pub(crate) fn edge_text(
    annotation: Option<&EdgeAnnotation>,
    default: impl FnOnce() -> Option<String>,
) -> Option<String> {
    match annotation.and_then(|a| a.label.as_deref()) {
        Some("") => None,
        Some(label) => Some(label.to_string()),
        None => default(),
    }
}

/// The line `annotation` adds under a node's label, if any.
pub(crate) fn node_text(annotation: Option<&NodeAnnotation>) -> Option<&str> {
    annotation
        .and_then(|a| a.label.as_deref())
        .filter(|label| !label.is_empty())
}

/// The layout weight `annotation` gives an edge, or `default` if it gives none or an invalid one.
pub(crate) fn edge_weight(annotation: Option<&EdgeAnnotation>, default: f64) -> f64 {
    annotation
        .and_then(|a| a.weight)
        .filter(|w| w.is_finite() && *w > 0.0)
        .unwrap_or(default)
}

/// The line width `annotation` gives an edge, or `default` if it gives none or an invalid one.
pub(crate) fn edge_thickness(annotation: Option<&EdgeAnnotation>, default: f64) -> f64 {
    annotated_thickness(annotation).unwrap_or(default)
}

/// The valid line width `annotation` gives an edge, if any.
pub(crate) fn annotated_thickness(annotation: Option<&EdgeAnnotation>) -> Option<f64> {
    annotation
        .and_then(|a| a.thickness)
        .filter(|t| t.is_finite() && *t >= 0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_invalid_weight_or_width_keeps_the_default() {
        let annotation = |weight, thickness| EdgeAnnotation {
            weight: Some(weight),
            thickness: Some(thickness),
            ..EdgeAnnotation::default()
        };
        for (weight, thickness) in [
            (0.0, -1.0),
            (-2.0, f64::NAN),
            (f64::INFINITY, f64::INFINITY),
        ] {
            let a = annotation(weight, thickness);
            assert_eq!(edge_weight(Some(&a), 1.5), 1.5);
            assert_eq!(edge_thickness(Some(&a), 2.0), 2.0);
        }
        let a = annotation(0.5, 0.0);
        assert_eq!(
            (edge_weight(Some(&a), 1.5), edge_thickness(Some(&a), 2.0)),
            (0.5, 0.0)
        );
    }
}
