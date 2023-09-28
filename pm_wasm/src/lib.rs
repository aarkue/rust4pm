use std::panic;

use wasm_bindgen::prelude::*;
use pm_rust::{event_log::{import_xes::import_xes_str, activity_projection::EventLogActivityProjection}, alphappp::full::alphappp_discover_petri_net};


#[wasm_bindgen]
pub fn wasm_discover_alphappp_petri_net(xes_str: &str) -> String {
    panic::set_hook(Box::new(console_error_panic_hook::hook));
    let log = import_xes_str(xes_str);
    let log_proj: EventLogActivityProjection = (&log).into();
    let pn = alphappp_discover_petri_net(&log_proj);
    pn.to_json()
}
