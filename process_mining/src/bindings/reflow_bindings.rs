//! Bindings for [Reflow](crate::analysis::object_centric::reflow).
//!
//! [`reflow`](crate::analysis::object_centric::reflow::reflow) stores its result as a
//! `ReflowResult` handle, which every other binding reads instead of searching again: tagging a
//! log, editing the flow layer, and the annotated object-centric Petri net. Logs go in and come
//! out as `SlimLinkedOCEL` handles.

use macros_process_mining::register_binding;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::analysis::object_centric::reflow::{
    flow_projection, full_projection, untag, AnnotatedOcpn, AnnotatedOcpnOptions, ReflowCell,
    ReflowResult, ReflowStats,
};
use crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;
use crate::core::process_models::object_centric::ocpn::ObjectCentricPetriNet;

/// A cell, by activity and object type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReflowCellRef {
    /// The activity.
    pub activity: String,
    /// The object type.
    pub object_type: String,
}

/// The cells and counts of a [`ReflowResult`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReflowSummary {
    /// Activities, sorted.
    pub activities: Vec<String>,
    /// Object types, sorted.
    pub object_types: Vec<String>,
    /// Every recorded cell and every added one, with the fewest and most objects per event.
    pub cells: Vec<ReflowCell>,
    /// Sizes and how the search went.
    pub stats: ReflowStats,
}

/// The cells of a result with their states, and its counts.
#[register_binding]
pub fn reflow_cells(#[bind(handle)] result: &ReflowResult) -> ReflowSummary {
    ReflowSummary {
        activities: result.activities.clone(),
        object_types: result.object_types.clone(),
        cells: result.cells.clone(),
        stats: result.stats.clone(),
    }
}

/// A result for another flow layer of the same log, e.g., an edited one, repaired so that every
/// recorded cell of an ordering flows or is determined.
#[register_binding(returns_handle, stringify_error)]
pub fn reflow_with_flow_layer(
    #[bind(handle)] result: &ReflowResult,
    flow: Vec<ReflowCellRef>,
) -> Result<ReflowResult, String> {
    result
        .with_flow_layer(flow.into_iter().map(|c| (c.activity, c.object_type)))
        .map_err(|e| e.to_string())
}

/// Tag a log with a result's flow layer: non-flow participations get the `!` qualifier prefix,
/// participations added by expansion the qualifier `+`.
#[register_binding]
pub fn reflow_tag(ocel: &SlimLinkedOCEL, #[bind(handle)] result: &ReflowResult) -> SlimLinkedOCEL {
    result.tag(ocel)
}

/// The flow projection of a log under a result, which is what discovery reads: the same as
/// `reflow_flow_projection` of `reflow_tag`, without the tagged log in between.
#[register_binding]
pub fn reflow_flow_log(
    ocel: &SlimLinkedOCEL,
    #[bind(handle)] result: &ReflowResult,
) -> SlimLinkedOCEL {
    result.flow_log(ocel)
}

/// The flowing participations of a tagged log, which is what discovery reads.
#[register_binding]
pub fn reflow_flow_projection(ocel: &SlimLinkedOCEL) -> SlimLinkedOCEL {
    flow_projection(ocel).into_owned()
}

/// Every participation of a tagged log, with the `!` prefix dropped.
#[register_binding]
pub fn reflow_full_projection(ocel: &SlimLinkedOCEL) -> SlimLinkedOCEL {
    full_projection(ocel).into_owned()
}

/// The log a tagged log tags: added participations removed and the `!` prefix dropped.
#[register_binding]
pub fn reflow_untag(ocel: &SlimLinkedOCEL) -> SlimLinkedOCEL {
    untag(ocel).into_owned()
}

/// Discover an object-centric Petri net from the flow projection of a log under a result, with
/// the non-flow cells annotated at their transitions, and store it.
#[register_binding(returns_handle)]
pub fn reflow_annotated_ocpn(
    ocel: &SlimLinkedOCEL,
    #[bind(handle)] result: &ReflowResult,
    #[bind(default)] options: AnnotatedOcpnOptions,
) -> AnnotatedOcpn {
    result.annotated_ocpn(ocel, &options)
}

/// The object-centric Petri net of an annotated net, as a handle for other bindings.
#[register_binding(returns_handle)]
pub fn reflow_annotated_ocpn_net(#[bind(handle)] model: &AnnotatedOcpn) -> ObjectCentricPetriNet {
    model.ocpn.clone()
}

#[cfg(feature = "visualization")]
mod drawing {
    use super::*;
    use crate::analysis::object_centric::reflow::AnnotatedOcpnDrawing;
    use crate::visualization::graphlay::svg::Palette;
    use crate::visualization::ocpn::OcpnStyle;
    use crate::visualization::VisualizationError;

    /// The flat net, build options with a badge per non-flow cell, and the style of the added
    /// places, for the `ocpn_*` visualization bindings.
    #[register_binding]
    pub fn reflow_annotated_ocpn_drawing(
        #[bind(handle)] model: &AnnotatedOcpn,
    ) -> AnnotatedOcpnDrawing {
        model.drawing()
    }

    /// Draw an annotated net as a standalone SVG document.
    #[register_binding(stringify_error)]
    pub fn reflow_annotated_ocpn_svg(
        #[bind(handle)] model: &AnnotatedOcpn,
        #[bind(default)] style: OcpnStyle,
        #[bind(default)] palette: Option<Palette>,
    ) -> Result<String, VisualizationError> {
        model.svg(&style, palette.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use crate::bindings::{
        call_resolved, call_with_outcome, list_functions, AppState, Binding, CallError,
        RegistryItem,
    };
    use crate::test_utils::get_test_data_path;
    use crate::Control;

    fn binding(name: &str) -> &'static Binding {
        list_functions()
            .into_iter()
            .find(|b| b.name == name)
            .unwrap_or_else(|| panic!("no binding {name}"))
    }

    fn call(name: &str, args: Value, state: &AppState) -> Value {
        let out =
            call_resolved(binding(name), &args, state).unwrap_or_else(|e| panic!("{name}: {e}"));
        serde_json::from_slice(&out).unwrap()
    }

    fn log_path() -> String {
        get_test_data_path()
            .join("ocel")
            .join("order-management.json")
            .to_string_lossy()
            .to_string()
    }

    #[test]
    fn a_result_handle_is_reused_by_every_later_call() {
        let state = AppState::default();
        state.add(
            "om",
            RegistryItem::load_from_path(&"SlimLinkedOCEL".parse().unwrap(), log_path()).unwrap(),
        );
        let result = call("reflow", json!({ "ocel": "om" }), &state);
        let result = result.as_str().unwrap().to_string();
        let summary = call("reflow_cells", json!({ "result": result }), &state);
        assert_eq!(summary["stats"]["flow"], 14);
        assert_eq!(summary["stats"]["involved"], 3);
        assert_eq!(summary["stats"]["implied"], 23);
        let cell = summary["cells"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["activity"] == "confirm order" && c["objectType"] == "items")
            .unwrap();
        assert_eq!(
            (&cell["minObjects"], &cell["maxObjects"]),
            (&json!(1), &json!(16))
        );

        let tagged = call(
            "reflow_tag",
            json!({ "ocel": "om", "result": result }),
            &state,
        );
        let tagged = tagged.as_str().unwrap().to_string();
        let read = call("reflow_from_tagged", json!({ "ocel": tagged }), &state);
        let read = call("reflow_cells", json!({ "result": read }), &state);
        assert_eq!(read["cells"], summary["cells"]);

        let flow = call("reflow_flow_projection", json!({ "ocel": tagged }), &state);
        let direct = call(
            "reflow_flow_log",
            json!({ "ocel": "om", "result": result }),
            &state,
        );
        let item = |id: &Value| state.read()[id.as_str().unwrap()].to_value().unwrap();
        assert_eq!(item(&direct), item(&flow));
        let untagged = call("reflow_untag", json!({ "ocel": tagged }), &state);
        let again = call("reflow", json!({ "ocel": untagged }), &state);
        let again = call("reflow_cells", json!({ "result": again }), &state);
        assert_eq!(again["cells"], summary["cells"]);

        let model = call(
            "reflow_annotated_ocpn",
            json!({ "ocel": "om", "result": result, "options": { "places": true } }),
            &state,
        );
        let net = call(
            "reflow_annotated_ocpn_net",
            json!({ "model": model }),
            &state,
        );
        assert!(state.contains_key(net.as_str().unwrap()));
        #[cfg(feature = "visualization")]
        {
            let drawing = call(
                "reflow_annotated_ocpn_drawing",
                json!({ "model": model }),
                &state,
            );
            assert!(!drawing["options"]["badges"].as_object().unwrap().is_empty());
            let svg = call(
                "reflow_annotated_ocpn_svg",
                json!({ "model": model }),
                &state,
            );
            assert!(svg.as_str().unwrap().contains("<svg"));
            let ocpn_svg = call(
                "ocpn_svg",
                json!({ "ocpn": drawing["ocpn"], "options": drawing["options"] }),
                &state,
            );
            assert!(ocpn_svg.as_str().unwrap().contains("gl-mapsto"));
        }
    }

    #[test]
    fn an_edited_flow_layer_is_a_new_handle_and_unknown_cells_are_errors() {
        let state = AppState::default();
        let result = call("reflow", json!({ "ocel": log_path() }), &state);
        let edited = call(
            "reflow_with_flow_layer",
            json!({
                "result": result,
                "flow": [{ "activity": "place order", "objectType": "orders" }],
            }),
            &state,
        );
        assert_ne!(edited, result);
        let cells = call("reflow_cells", json!({ "result": edited }), &state);
        assert!(cells["stats"]["repaired"].as_u64().unwrap() > 0);
        assert!(cells["stats"]["seed"].is_null());

        let err = call_resolved(
            binding("reflow_with_flow_layer"),
            &json!({ "result": result, "flow": [{ "activity": "nope", "objectType": "orders" }] }),
            &state,
        )
        .unwrap_err();
        assert!(err.contains("nope"), "{err}");
    }

    #[test]
    fn results_round_trip_through_json() {
        let state = AppState::default();
        let result = call(
            "reflow",
            json!({ "ocel": log_path(), "options": { "maxWork": 0 } }),
            &state,
        );
        let item = state.read()[result.as_str().unwrap()].to_value().unwrap();
        assert_eq!(item["stats"]["limitReached"], true);
        let kind = "ReflowResult".parse().unwrap();
        let back = RegistryItem::from_json_value(&kind, &item).unwrap();
        assert_eq!(back.to_value().unwrap(), item);
        let mut broken = item;
        broken["grid"][0]["present"][0] = json!(99);
        assert!(RegistryItem::from_json_value(&kind, &broken).is_err());
        let bytes = serde_json::to_vec(&broken).unwrap();
        assert!(RegistryItem::load_from_bytes(&kind, &bytes, "json").is_err());
    }

    #[test]
    fn a_cancelled_discovery_stores_nothing() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let state = AppState::default();
        state.add(
            "om",
            RegistryItem::load_from_path(&"SlimLinkedOCEL".parse().unwrap(), log_path()).unwrap(),
        );
        for name in ["reflow", "reflow_from_tagged"] {
            let binding = binding(name);
            assert!(!(binding.args)().iter().any(|(name, _)| name == "ctl"));
            let flag = AtomicBool::new(false);
            let ctl = Control::new()
                .on_progress(|p| {
                    if p.stage == "Building activity grid" {
                        flag.store(true, Ordering::Relaxed);
                    }
                })
                .cancel_flag(&flag);
            let out = call_with_outcome(binding, &json!({ "ocel": "om" }), &state, &ctl);
            assert_eq!(out, Err(CallError::Cancelled), "{name}");
            assert_eq!(state.read().len(), 1);
        }
    }
}
