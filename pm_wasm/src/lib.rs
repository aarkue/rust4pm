use std::panic;

use pm_rust::{
    alphappp::full::{alphappp_discover_petri_net, AlphaPPPConfig},
    event_log::{activity_projection::EventLogActivityProjection, import_xes::import_xes_str},
};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn wasm_discover_alphappp_petri_net(xes_str: &str) -> String {
    panic::set_hook(Box::new(console_error_panic_hook::hook));
    let log = import_xes_str(xes_str);
    let log_proj: EventLogActivityProjection = (&log).into();
    let pn = alphappp_discover_petri_net(
        &log_proj,
        AlphaPPPConfig {
            balance_thresh: 0.1,
            fitness_thresh: 0.8,
            log_repair_skip_df_thresh: 25,
            log_repair_loop_df_thresh: 25,
            absolute_df_clean_thresh: 5,
            relative_df_clean_thresh: 0.05,
        },
    );
    pn.to_json()
}
