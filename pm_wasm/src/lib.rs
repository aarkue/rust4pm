use process_mining::{
    alphappp::full::{alphappp_discover_petri_net_with_timing_fn, AlphaPPPConfig},
    event_log::{
        activity_projection::EventLogActivityProjection,
        constants::ACTIVITY_NAME,
        import_xes::{build_ignore_attributes, import_xes_str, XESImportOptions},
        ocel::xml_ocel_import::import_ocel_xml_slice,
        stream_xes::{stream_xes_slice, stream_xes_slice_gz},
    },
    petri_net::image_export::{export_petri_net_to_dot_graph, graph_to_dot},
    PetriNet, OCEL,
};
use wasm_bindgen::prelude::*;
// pub use wasm_bindgen_rayon::init_thread_pool;
extern crate console_error_panic_hook;

#[wasm_bindgen]
extern "C" {
    // Use `js_namespace` here to bind `console.log(..)` instead of just
    // `log(..)`
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);

    // The `console.log` is quite polymorphic, so we can bind it with multiple
    // signatures. Note that we need to use `js_name` to ensure we always call
    // `log` in JS.
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn log_u32(a: u32);

    // Multiple arguments too!
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn log_many(a: &str, b: &str);
}

macro_rules! console_log {
    // Note that this is using the `log` function imported above during
    // `bare_bones`
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}

#[wasm_bindgen]
pub fn wasm_discover_alphappp_petri_net_from_xes_string(xes_str: &str) -> Vec<u8> {
    console_error_panic_hook::set_once();
    let log = import_xes_str(
        xes_str,
        XESImportOptions {
            ignore_trace_attributes_except: Some(build_ignore_attributes(vec!["concept:name"])),
            ignore_event_attributes_except: Some(build_ignore_attributes(vec!["concept:name"])),
            ignore_log_attributes_except: Some(build_ignore_attributes(Vec::<&str>::new())),
            ..XESImportOptions::default()
        },
    )
    .unwrap();
    console_log!("Got log: {}", log.traces.len());
    let log_proj: EventLogActivityProjection = (&log).into();
    let (pn, _) = alphappp_discover_petri_net_with_timing_fn(
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
        &|| 0,
    );
    serde_json::to_vec(&pn).unwrap()
}

#[wasm_bindgen]
pub fn wasm_discover_alphappp_petri_net_from_xes_vec(
    xes_data: &[u8],
    is_compressed_gz: bool,
) -> Vec<u8> {
    console_error_panic_hook::set_once();
    console_log!("Got data: {}", xes_data.len());
    let options = XESImportOptions {
        ignore_event_attributes_except: Some(build_ignore_attributes(vec![ACTIVITY_NAME])),
        ignore_trace_attributes_except: Some(build_ignore_attributes(Vec::<&str>::new())),
        ignore_log_attributes_except: Some(build_ignore_attributes(Vec::<&str>::new())),
        ..XESImportOptions::default()
    };
    let (mut stream, _log_data) = if is_compressed_gz {
        stream_xes_slice_gz(xes_data, options)
    } else {
        stream_xes_slice(xes_data, options)
    }
    .unwrap();
    // let now = Instant::now();

    web_sys::console::time_with_label("xes-import");
    // let log = import_xes_slice(
    //     xes_data,
    //     is_compressed_gz,
    //     XESImportOptions {
    //         ignore_trace_attributes_except: Some(build_ignore_attributes(vec!["concept:name"])),
    //         ignore_event_attributes_except: Some(build_ignore_attributes(vec!["concept:name"])),
    //         ignore_log_attributes_except: Some(build_ignore_attributes(Vec::<&str>::new())),
    //         ..XESImportOptions::default()
    //     },
    // )
    // .unwrap();
    // web_sys::console::time_end_with_label("xes-import");
    // console_log!("Got Log: {}", log.traces.len());
    let log_proj: EventLogActivityProjection = (&mut stream).into();
    console_log!("Got Log Activity Projection: {}", log_proj.traces.len());
    let (pn, _) = alphappp_discover_petri_net_with_timing_fn(
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
        &|| 0,
    );
    serde_json::to_vec(&pn).unwrap()
}

#[wasm_bindgen]
pub fn wasm_parse_ocel2_json(json_data: &[u8]) -> Vec<u8> {
    console_error_panic_hook::set_once();
    console_log!("Got data: {}", json_data.len());
    let ocel: OCEL = serde_json::from_slice(json_data).unwrap();
    console_log!("Got Log: {}", ocel.events.len());
    serde_json::to_vec(&ocel).unwrap()
}

// Some first measurements for Angular OCEL2 log:
//  6.187s
//  6.451s
//  6.472s
// Chromium: 15.627s
#[wasm_bindgen]
pub fn wasm_parse_ocel2_xml(ocel_data: &[u8]) -> JsValue {
    let ocel = import_ocel_xml_slice(ocel_data);
    serde_wasm_bindgen::to_value(&ocel).unwrap()
}

//  5.064s
//  5.358s
//  5.636s
// Chromium: 10.519s
#[wasm_bindgen]
pub fn wasm_parse_ocel2_xml_to_json_str(ocel_data: &[u8]) -> String {
    let ocel = import_ocel_xml_slice(ocel_data);
    serde_json::to_string(&ocel).unwrap()
}
// 5.101s
// 4.96s
// 5.854s

// Chromium: 9.934s // Second test in Chromium: 11.334
#[wasm_bindgen]
pub fn wasm_parse_ocel2_xml_to_json_vec(ocel_data: &[u8]) -> Vec<u8> {
    let ocel = import_ocel_xml_slice(ocel_data);
    serde_json::to_vec(&ocel).unwrap()
}

/// Parse OCEL XML from byte slice and keep it in WASM memory
///
/// __Note: Memory will leak if it is not cleaned up manually (e.g., by caliing [`wasm_destroy_ocel_pointer`])__  
#[wasm_bindgen]
pub fn wasm_parse_ocel2_xml_keep_state_in_wasm(ocel_data: &[u8]) -> JsValue {
    let ocel = import_ocel_xml_slice(ocel_data);
    let boxed_ocel = Box::new(ocel);
    let memory_addr = Box::into_raw(boxed_ocel) as usize;
    memory_addr.into()
}

/// Get number of events in OCEL at given memory location
///
/// # Safety
/// Assumes that there is an valid OCEL stored at the given memory location
#[wasm_bindgen]
pub unsafe fn wasm_get_ocel_num_events_from_pointer(addr: usize) -> JsValue {
    let boxed_ocel = Box::from_raw(addr as *mut OCEL);
    let len = boxed_ocel.events.len();
    // Into raw: do not destroy/deallocate OCEL
    let _ = Box::into_raw(boxed_ocel);
    len.into()
}

/// Destroy OCEL at memory location
///
/// # Safety
/// Assumes that there is an valid OCEL stored at the given memory location
#[wasm_bindgen]
pub unsafe fn wasm_destroy_ocel_pointer(addr: usize) -> JsValue {
    let _boxed_ocel = Box::from_raw(addr as *mut OCEL);
    // OCEL implicitly detroyed/deallocated here
    true.into()
}

#[wasm_bindgen]
pub fn wasm_petri_net_dot(pn: &str) -> String {
    let pn: PetriNet = serde_json::from_str(pn).unwrap();
    let g = export_petri_net_to_dot_graph(&pn, None);
    graph_to_dot(&g)
}
