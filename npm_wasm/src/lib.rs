mod utils;

use process_mining::{event_log::ocel::xml_ocel_import::import_ocel_xml_slice, OCEL};
use utils::set_panic_hook;
use wasm_bindgen::prelude::*;

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
pub fn wasm_parse_ocel2_json(json_data: &[u8]) -> Vec<u8> {
    set_panic_hook();
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
    set_panic_hook();
    let ocel = import_ocel_xml_slice(ocel_data);
    serde_wasm_bindgen::to_value(&ocel).unwrap()
}

//  5.064s
//  5.358s
//  5.636s
// Chromium: 10.519s
#[wasm_bindgen]
pub fn wasm_parse_ocel2_xml_to_json_str(ocel_data: &[u8]) -> String {
    set_panic_hook();
    let ocel = import_ocel_xml_slice(ocel_data);
    serde_json::to_string(&ocel).unwrap()
}
// 5.101s
// 4.96s
// 5.854s

// Chromium: 9.934s // Second test in Chromium: 11.334
#[wasm_bindgen]
pub fn wasm_parse_ocel2_xml_to_json_vec(ocel_data: &[u8]) -> Vec<u8> {
    set_panic_hook();
    let ocel = import_ocel_xml_slice(ocel_data);
    serde_json::to_vec(&ocel).unwrap()
}

/// Parse OCEL XML from byte slice and keep it in WASM memory
///
/// __Note: Memory will leak if it is not cleaned up manually (e.g., by caliing [`wasm_destroy_ocel_pointer`])__  
#[wasm_bindgen]
pub fn wasm_parse_ocel2_xml_keep_state_in_wasm(ocel_data: &[u8]) -> JsValue {
    set_panic_hook();
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
    set_panic_hook();
    let boxed_ocel = Box::from_raw(addr as *mut OCEL);
    let len = boxed_ocel.events.len();
    // Into raw: do not destroy/deallocate OCEL
    Box::into_raw(boxed_ocel);
    len.into()
}

/// Destroy OCEL at memory location
///
/// # Safety
/// Assumes that there is an valid OCEL stored at the given memory location
#[wasm_bindgen]
pub unsafe fn wasm_destroy_ocel_pointer(addr: usize) -> JsValue {
    set_panic_hook();
    let _boxed_ocel = Box::from_raw(addr as *mut OCEL);
    // OCEL implicitly detroyed/deallocated here
    true.into()
}
