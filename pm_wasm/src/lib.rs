use std::panic;

use process_mining::{
    alphappp::full::{alphappp_discover_petri_net, AlphaPPPConfig, alphappp_discover_petri_net_with_timing_fn},
    event_log::{activity_projection::EventLogActivityProjection, import_xes::import_xes_str},
};
use wasm_bindgen::prelude::*;

pub use wasm_bindgen_rayon::init_thread_pool;
extern crate console_error_panic_hook;

#[wasm_bindgen]
pub fn wasm_discover_alphappp_petri_net(xes_str: &str) -> String {
    console_error_panic_hook::set_once();
    let log = import_xes_str(xes_str, None);
    let log_proj: EventLogActivityProjection = (&log).into();
    let (pn,_) = alphappp_discover_petri_net_with_timing_fn(
        &log_proj,
        AlphaPPPConfig {
            balance_thresh: 0.1,
            fitness_thresh: 0.8,
            replay_thresh: 0.3,
            log_repair_skip_df_thresh_rel: 4.0,
            log_repair_loop_df_thresh_rel: 4.0,
            absolute_df_clean_thresh: 5,
            relative_df_clean_thresh: 0.05,
        },
        &|| {
            return 0;
        }
    );
    pn.to_json()
}
