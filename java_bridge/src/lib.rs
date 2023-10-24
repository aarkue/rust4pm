#![allow(non_snake_case)]

use std::collections::HashMap;

pub use copy_log::copy_log_shared::JEvent;
mod copy_log {
    pub mod copy_log_shared;
    mod java_log_to_rust;
    mod rust_log_to_java;
}
use jni::{
    objects::{JClass, JIntArray, JString},
    sys::jlong,
    JNIEnv,
};

use jni_fn::jni_fn;
use pm_rust::{
    add_start_end_acts,
    alphappp::full::{alphappp_discover_petri_net, AlphaPPPConfig},
    event_log::{activity_projection::EventLogActivityProjection, import_xes::import_xes_file},
    petrinet_to_json, Attribute, AttributeValue, EventLog,
};

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.RustBridge")]
pub unsafe fn addStartEndToRustLog<'local>(mut _env: JNIEnv<'local>, _: JClass, pointer: jlong) {
    let mut log_pointer = Box::from_raw(pointer as *mut EventLog);
    add_start_end_acts(&mut log_pointer);
    let _proj: EventLogActivityProjection = log_pointer.as_ref().into();
    let _log_pointer = Box::into_raw(log_pointer);
}

/// Get attributes of (boxed) [EventLog] referenced by `pointer`
///
/// Attributes are converted to JSON String (encoding a [HashMap<String,String>])
#[jni_fn("org.processmining.alpharevisitexperiments.bridge.RustBridge")]
pub unsafe fn getRustLogAttributes<'local>(
    mut _env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
) -> JString<'local> {
    let mut log_pointer = Box::from_raw(pointer as *mut EventLog);
    let (k, a) = Attribute::new_with_key(
        "__NUM_TRACES__".to_string(),
        AttributeValue::Int(log_pointer.traces.len() as i64),
    );
    log_pointer.attributes.insert(k, a);
    let attributes_json = serde_json::to_string(&log_pointer.attributes).unwrap();
    // memory of log_pointer should _not_ be destroyed!
    let _log_pointer = Box::into_raw(log_pointer);
    _env.new_string(attributes_json).unwrap()
}

/// Get the lengths of all traces in (boxed) [EventLog] referenced by `pointer`
///
/// The lengths are returned as a [JIntArray] of size of `EventLog.traces`,
/// where each entry contains the length of the trace (i.e., the length of `Trace.events`) at the corresponding index
#[jni_fn("org.processmining.alpharevisitexperiments.bridge.RustBridge")]
pub unsafe fn getRustTraceLengths<'local>(
    mut _env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
) -> JIntArray<'local> {
    let log_pointer = Box::from_raw(pointer as *mut EventLog);
    let trace_lengths: Vec<i32> = log_pointer
        .traces
        .iter()
        .map(|t| t.events.len() as i32)
        .collect();
    let trace_lengths_j: JIntArray = _env.new_int_array(trace_lengths.len() as i32).unwrap();
    _env.set_int_array_region(&trace_lengths_j, 0, &trace_lengths)
        .unwrap();
    // memory of log_pointer should _not_ be destroyed!
    let _log_pointer = Box::into_raw(log_pointer);
    trace_lengths_j
}

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.RustBridge")]
pub unsafe fn discoverPetriNetAlphaPPP<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    log_pointer: jlong,
    algo_config: JString,
) -> JString<'local> {
    let algo_config =
        AlphaPPPConfig::from_json(&env.get_string(&algo_config).unwrap().to_str().unwrap());
    println!("[Rust] Got config {:?}", algo_config);
    let log_boxed = Box::from_raw(log_pointer as *mut EventLog);
    let (net, _duration) = alphappp_discover_petri_net(&(log_boxed.as_ref()).into(), algo_config);
    env.new_string(petrinet_to_json(&net)).unwrap()
}

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.RustBridge")]
pub unsafe fn discoverPetriNetAlphaPPPFromActProj<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    variants_json: JString,
    activities_json: JString,
    algo_config_json: JString,
) -> JString<'local> {
    let algo_config =
        AlphaPPPConfig::from_json(&env.get_string(&algo_config_json).unwrap().to_str().unwrap());
    println!("[Rust] Got config {:?}", algo_config);
    let acts: Vec<String> =
        serde_json::from_str(&env.get_string(&activities_json).unwrap().to_str().unwrap()).unwrap();
    let variants: HashMap<String, u64> =
        serde_json::from_str(&env.get_string(&variants_json).unwrap().to_str().unwrap()).unwrap();
    let mut log_proj = EventLogActivityProjection {
        activities: acts.clone(),
        act_to_index: acts
            .into_iter()
            .enumerate()
            .map(|(i, a)| (a.clone(), i))
            .collect(),
        traces: Vec::new(),
    };

    log_proj.traces = variants
        .iter()
        .map(|(var, count)| {
            (
                var.split(",").into_iter()
                    .map(|a| *log_proj.act_to_index.get(a).unwrap())
                    .collect(),
                *count,
            )
        })
        .collect();
    let (net, _duration) = alphappp_discover_petri_net(&log_proj, algo_config);
    env.new_string(petrinet_to_json(&net)).unwrap()
}


#[jni_fn("org.processmining.alpharevisitexperiments.bridge.RustBridge")]
pub unsafe fn importXESLog<'local>(mut env: JNIEnv<'local>, _: JClass, path: JString) -> jlong {
    let log: EventLog = import_xes_file(&env.get_string(&path).unwrap().to_str().unwrap());
    let log_box = Box::new(log);
    let pointer = Box::into_raw(log_box) as jlong;
    pointer
}
