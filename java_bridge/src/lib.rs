#![allow(non_snake_case, clippy::missing_safety_doc)]

use std::collections::HashMap;

pub use copy_log::copy_log_shared::JEvent;
mod copy_log {
    pub mod copy_log_shared;
    mod java_log_to_rust;
    mod rust_log_to_java;
}
use jni::{
    objects::{AutoLocal, JClass, JIntArray, JObject, JString},
    sys::{jint, jlong},
    JNIEnv,
};

use jni_fn::jni_fn;
use process_mining::{
    alphappp::{
        auto_parameters::alphappp_discover_with_auto_parameters,
        full::{alphappp_discover_petri_net, AlphaPPPConfig},
    },
    event_log::{
        activity_projection::{add_start_end_acts, EventLogActivityProjection},
        event_log_struct::HashMapAttribute,
        import_xes::{import_xes_file, XESImportOptions},
        Attribute, XESEditableAttribute, AttributeValue, Attributes, EventLog,
    },
    petri_net::petri_net_struct::PetriNet,
    petrinet_to_json, stream_xes_from_path,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.RustBridge")]
pub unsafe fn addStartEndToRustLog(mut _env: JNIEnv<'_>, _: JClass, pointer: jlong) {
    let mut log_pointer = Box::from_raw(pointer as *mut EventLog);
    add_start_end_acts(&mut log_pointer);
    let _proj: EventLogActivityProjection = log_pointer.as_ref().into();
    let _log_pointer = Box::into_raw(log_pointer);
}

/// Get attributes of (boxed) [`EventLog`] referenced by `pointer`
///
/// Attributes are converted to JSON String (encoding a [`HashMap`])
#[jni_fn("org.processmining.alpharevisitexperiments.bridge.RustBridge")]
pub unsafe fn getRustLogAttributes<'local>(
    mut _env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
) -> JString<'local> {
    let mut log_pointer = Box::from_raw(pointer as *mut EventLog);
    log_pointer.attributes.add_to_attributes(
        "__NUM_TRACES__".to_string(),
        AttributeValue::Int(log_pointer.traces.len() as i64),
    );
    let attributes_json = serde_json::to_string(&log_pointer.attributes.as_hash_map()).unwrap();
    // memory of log_pointer should _not_ be destroyed!
    let _log_pointer = Box::into_raw(log_pointer);
    _env.new_string(attributes_json).unwrap()
}

/// Get the lengths of all traces in (boxed) [`EventLog`] referenced by `pointer`
///
/// The lengths are returned as a [`JIntArray`] of size of `EventLog.traces`,
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
        AlphaPPPConfig::from_json(env.get_string(&algo_config).unwrap().to_str().unwrap());
    println!("[Rust] Got config {:?}", algo_config);
    let log_boxed = Box::from_raw(log_pointer as *mut EventLog);
    let (net, _duration) = alphappp_discover_petri_net(&(log_boxed.as_ref()).into(), algo_config);
    env.new_string(petrinet_to_json(&net)).unwrap()
}

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.RustBridge")]
pub unsafe fn discoverPetriNetAlphaPPPFromActProjAuto<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    variants_json: JString,
    activities_json: JString,
) -> JString<'local> {
    let acts: Vec<String> =
        serde_json::from_str(env.get_string(&activities_json).unwrap().to_str().unwrap()).unwrap();
    let variants: HashMap<String, u64> =
        serde_json::from_str(env.get_string(&variants_json).unwrap().to_str().unwrap()).unwrap();
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
                var.split(',')
                    .map(|a| *log_proj.act_to_index.get(a).unwrap())
                    .collect(),
                *count,
            )
        })
        .collect();
    let (config, net) = alphappp_discover_with_auto_parameters(&log_proj);
    #[derive(Serialize, Deserialize)]
    struct AutoDiscoveryResult {
        petri_net: PetriNet,
        config: AlphaPPPConfig,
    }
    let res = AutoDiscoveryResult {
        petri_net: net,
        config,
    };
    env.new_string(serde_json::to_string(&res).unwrap())
        .unwrap()
}

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.RustBridge")]
pub unsafe fn testActProjPassPerformance<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    variants_json: JString,
    activities_json: JString,
) -> JString<'local> {
    let acts: Vec<String> =
        serde_json::from_str(env.get_string(&activities_json).unwrap().to_str().unwrap()).unwrap();
    let variants: HashMap<String, u64> =
        serde_json::from_str(env.get_string(&variants_json).unwrap().to_str().unwrap()).unwrap();
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
                var.split(',')
                    .map(|a| *log_proj.act_to_index.get(a).unwrap())
                    .collect(),
                *count,
            )
        })
        .collect();
    let num_cases: u64 = log_proj.traces.iter().map(|(_, count)| count).sum();
    env.new_string(format!("#Cases: {}", num_cases)).unwrap()
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
        AlphaPPPConfig::from_json(env.get_string(&algo_config_json).unwrap().to_str().unwrap());
    println!("[Rust] Got config {:?}", algo_config);
    let acts: Vec<String> =
        serde_json::from_str(env.get_string(&activities_json).unwrap().to_str().unwrap()).unwrap();
    let variants: HashMap<String, u64> =
        serde_json::from_str(env.get_string(&variants_json).unwrap().to_str().unwrap()).unwrap();
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
                var.split(',')
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
pub unsafe fn importXESLog(mut env: JNIEnv<'_>, _: JClass, path: JString) -> jlong {
    let log: EventLog = import_xes_file(
        env.get_string(&path).unwrap().to_str().unwrap(),
        XESImportOptions::default(),
    )
    .unwrap();
    let log_box = Box::new(log);

    Box::into_raw(log_box) as jlong
}

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.RustBridge")]
pub unsafe fn importXESLogStream(
    mut env: JNIEnv<'_>,
    _: JClass,
    path: JString,
    callback: JObject,
) -> jlong {
    let (mut stream, _log_data) = stream_xes_from_path(
        env.get_string(&path).unwrap().to_str().unwrap(),
        XESImportOptions::default(),
    )
    .unwrap();
    // let ss = env.byte_array_from_slice(&[3]).unwrap();
    for trace in &mut stream {
        let mut events_json: Vec<HashMap<String, HashMapAttribute>> =
            Vec::with_capacity(1 + trace.events.len());
        events_json.push(trace.attributes.as_hash_map());
        trace.events.iter().for_each(|e| {
            let mut attrs: Attributes = e.attributes.clone();
            attrs.add_to_attributes("__UUID__".into(), AttributeValue::ID(Uuid::new_v4()));
            events_json.push(attrs.as_hash_map())
        });
        let all_json: String = serde_json::to_string(&events_json).unwrap();
        let auto_dropped_all_json = env.auto_local(env.new_string(all_json).unwrap());
        match env.call_method(
            &callback,
            "rustCallback",
            "(Ljava/lang/String;)V",
            &[(&auto_dropped_all_json).into()],
        ) {
            Ok(_) => {}
            Err(e) => {
                eprintln!("!Java Error!: {}", e);
            }
        };
    }
    jlong::from(42)
    // let log_box = Box::new(log);
}

fn create_attribute_map<'a>(
    env: &mut JNIEnv<'a>,
    attribute_map_class: &JClass,
    size: usize,
) -> AutoLocal<'a, JObject<'a>> {
    let obj = env
        .new_object(attribute_map_class, "(I)V", &[(size as i32).into()])
        .unwrap();
    env.auto_local(obj)
}
fn create_trace<'a>(
    env: &mut JNIEnv<'a>,
    trace_class: &JClass,
    j_attribute_map: &JObject,
) -> AutoLocal<'a, JObject<'a>> {
    let obj = env
        .new_object(
            trace_class,
            "(Lorg/deckfour/xes/model/XAttributeMap;)V",
            &[(&j_attribute_map).into()],
        )
        .unwrap();
    env.auto_local(obj)
}
fn create_event<'a>(
    env: &mut JNIEnv<'a>,
    event_class: &JClass,
    j_attribute_map: &JObject,
) -> AutoLocal<'a, JObject<'a>> {
    let obj = env
        .new_object(
            event_class,
            "(Lorg/deckfour/xes/model/XAttributeMap;)V",
            &[(&j_attribute_map).into()],
        )
        .unwrap();
    env.auto_local(obj)
}

fn add_to_list<'a>(env: &mut JNIEnv<'a>, list: &JObject, to_add: &JObject) {
    let res = env
        .call_method(&list, "add", "(Ljava/lang/Object;)Z", &[(&to_add).into()])
        .unwrap();
}

fn put_in_map<'a>(env: &mut JNIEnv<'a>, map: &JObject, key: &str, value: &JObject) {
    let j_key = env.auto_local(env.new_string(key).unwrap());
    env.call_method(
        &map,
        "put",
        "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
        &[(&j_key).into(), (&value).into()],
    )
    .unwrap();
}

fn new_attribute_value<'a>(
    env: &mut JNIEnv<'a>,
    attribute_string_class: &JClass,
    attr: &Attribute,
) -> AutoLocal<'a, JObject<'a>> {
    let j_attr_key = env.auto_local(env.new_string(&attr.key).unwrap());
    let j_attr_value = env.auto_local(env.new_string(&format!("{:?}", attr.value)).unwrap());
    let j_attr_str = env
        .new_object(
            attribute_string_class,
            "(Ljava/lang/String;Ljava/lang/String;)V",
            &[(&j_attr_key).into(), (&j_attr_value).into()],
        )
        .unwrap();
    return env.auto_local(j_attr_str);
}
///
/// Experimental function for constructing the event log in Java directly
///
#[jni_fn("org.processmining.alpharevisitexperiments.bridge.RustBridge")]
pub unsafe fn constructInRustTest(
    mut env: JNIEnv<'_>,
    _: JClass,
    path: JString,
    xlog: JObject,
    trace_class: JClass,
    event_class: JClass,
    attribute_map_class: JClass,
    attribute_string_class: JClass,
) -> jlong {
    let (mut stream, _log_data) = stream_xes_from_path(
        env.get_string(&path).unwrap().to_str().unwrap(),
        XESImportOptions::default(),
    )
    .unwrap();
    for trace in &mut stream {
        // println!("[Rust]: Next Trace!");
        let j_trace_attribute_map =
            create_attribute_map(&mut env, &attribute_map_class, trace.attributes.len());
        for attr in trace.attributes {
            if let Some(_) = attr.value.try_get_string() {
                let j_attr_str = new_attribute_value(&mut env, &attribute_string_class, &attr);
                put_in_map(&mut env, &j_trace_attribute_map, &attr.key, &j_attr_str);
            }
        }
        let jtrace = create_trace(&mut env, &trace_class, &j_trace_attribute_map);
        for event in trace.events {
            let j_attribute_map =
                create_attribute_map(&mut env, &attribute_map_class, event.attributes.len());
            for attr in event.attributes {
                if let Some(_) = attr.value.try_get_string() {
                    let j_attr_str = new_attribute_value(&mut env, &attribute_string_class, &attr);
                    put_in_map(&mut env, &j_attribute_map, &attr.key, &j_attr_str);
                }
            }
            let jevent = create_event(&mut env, &event_class, &j_attribute_map);
            add_to_list(&mut env, &jtrace, &jevent);
        }
        add_to_list(&mut env, &xlog, &jtrace);
    }
    jlong::from(42)
}
