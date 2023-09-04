#![allow(non_snake_case)]

use jni::{
    objects::{JByteArray, JClass, JIntArray, JString},
    sys::{jboolean, jint, jlong},
    JNIEnv,
};
use std::{collections::HashMap, time::Instant};

use jni_fn::jni_fn;
use pm_rust::{
    add_start_end_acts, export_log, export_log_to_byte_vec, import_log, import_log_from_byte_array,
    Event, EventLog, EventLogActivityProjection, Trace,
};

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub fn addArtificialActs<'local>(
    env: JNIEnv<'local>,
    _: JClass,
    data: JByteArray<'local>,
) -> JByteArray<'local> {
    let mut now = Instant::now();
    let d: Vec<u8> = env.convert_byte_array(&data).unwrap();
    println!(
        "Got byte array of size {:.2} in {:.2?}",
        d.len(),
        now.elapsed()
    );
    now = Instant::now();
    let mut log: EventLog = import_log_from_byte_array(&d);

    println!("Time until struct ready: {:.2?}", now.elapsed());
    now = Instant::now();
    log.attributes.insert(
        "name".to_string(),
        "Transformed Rust Log from byte[]".into(),
    );
    println!("Time until into EventLog: {:.2?}", now.elapsed());
    now = Instant::now();
    add_start_end_acts(&mut log);
    println!("Time until start/end added: {:.2?}", now.elapsed());
    now = Instant::now();
    let log_projection: EventLogActivityProjection<usize> = log.into();
    let log_again: EventLog = log_projection.into();
    println!("Time until into/from: {:.2?}", now.elapsed());
    now = Instant::now();
    let export_vec = export_log_to_byte_vec(&log_again);
    println!("ExportVec to byte array: {:.2?}", now.elapsed());
    return env.byte_array_from_slice(&export_vec).unwrap();
}

// #[jni_fn("HelloProcessMining")]
// pub fn addArtificialActsAvro<'local>(
//     env: JNIEnv<'local>,
//     _: JClass,
//     data: JByteArray<'local>,
// ) -> JByteArray<'local> {
//     let mut now = Instant::now();
//     let d: Vec<u8>  = env.convert_byte_array(data).unwrap();
//     println!("Got byte array of size {:.2} in {:.2?}",d.len(),now.elapsed());
//     now = Instant::now();
//     let mut log: EventLog = import_log_from_byte_vec_avro(&d).unwrap();
// }

struct EventLogConstruction {
    traces: Vec<Box<Trace>>,
    attributes: HashMap<String, String>,
}

// Promise to free later!
#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub unsafe fn createRustEventLogPar<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    num_traces: jint,
    attributes: JString<'local>,
) -> jlong {
    let attribute_str = env
        .get_string(&attributes)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let attributes: HashMap<String, String> = serde_json::from_str(&attribute_str).unwrap();
    let traces: Vec<Box<Trace>> = (0..num_traces)
        .map(|_| {
            Box::new(Trace {
                attributes: HashMap::default(),
                events: Vec::new(),
            })
        })
        .collect();
    let log_constr = EventLogConstruction { attributes, traces };
    let pointer = Box::into_raw(Box::new(log_constr)) as jlong;
    pointer
}

/**
 * Add trace attributes for [EventLogConstruction] at Trace index _index_
 */
#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub unsafe fn setTraceAttributesPar<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
    index: jint,
    attributes: JString<'local>,
) -> jlong {
    let mut log_constr_pointer = Box::from_raw(pointer as *mut EventLogConstruction);
    let attribute_str = env
        .get_string(&attributes)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let attributes: HashMap<String, String> = serde_json::from_str(&attribute_str).unwrap();
    log_constr_pointer
        .traces
        .get_mut(index as usize)
        .unwrap()
        .attributes = attributes;
    let pointer = Box::into_raw(log_constr_pointer) as jlong;
    pointer
}

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub unsafe fn addEventToTracePar<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
    index: jint,
    event_attrs: JString<'local>,
) -> jlong {
    let mut log_constr_pointer = Box::from_raw(pointer as *mut EventLogConstruction);
    let attribute_str = env
        .get_string(&event_attrs)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let event_attributes: HashMap<String, String> = serde_json::from_str(&attribute_str).unwrap();
    log_constr_pointer
        .traces
        .get_mut(index as usize)
        .unwrap()
        .events
        .push(Event {
            attributes: event_attributes,
        });
    let pointer = Box::into_raw(log_constr_pointer) as jlong;
    pointer
}

// Promise to free later!
#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub unsafe fn finishLogConstructionPar<'local>(
    mut _env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
) -> jlong {
    let log_constr_pointer = Box::from_raw(pointer as *mut EventLogConstruction);
    // log_constr_pointer is released and thereby destroyed!
    let log: EventLog = EventLog {
        attributes: log_constr_pointer.attributes.clone(),
        traces: log_constr_pointer
            .traces
            .into_iter()
            .map(|t_box| *t_box)
            .collect(),
    };
    let log_box = Box::new(log);
    let pointer = Box::into_raw(log_box) as jlong;
    pointer
}

// Promise to free later!
#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub unsafe fn createRustEventLog<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    attributes: JString<'local>,
) -> jlong {
    let attribute_str = env
        .get_string(&attributes)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let attributes: HashMap<String, String> = serde_json::from_str(&attribute_str).unwrap();
    let log = EventLog {
        attributes: attributes,
        traces: Vec::new(),
    };
    let pointer = Box::into_raw(Box::new(log)) as jlong;
    pointer
}

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub unsafe fn appendTrace<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
    attributes: JString<'local>,
) -> jlong {
    let mut log_pointer = Box::from_raw(pointer as *mut EventLog);
    let attribute_str = env
        .get_string(&attributes)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let attributes: HashMap<String, String> = serde_json::from_str(&attribute_str).unwrap();
    log_pointer.traces.push(Trace {
        attributes,
        events: Vec::new(),
    });
    let pointer = Box::into_raw(log_pointer) as jlong;
    pointer
}

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub unsafe fn appendEventToLastTrace<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
    attributes: JString<'local>,
) -> jlong {
    let mut log_pointer = Box::from_raw(pointer as *mut EventLog);
    let attribute_str = env
        .get_string(&attributes)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let attributes: HashMap<String, String> = serde_json::from_str(&attribute_str).unwrap();
    log_pointer
        .traces
        .last_mut()
        .unwrap()
        .events
        .push(Event { attributes });
    let pointer = Box::into_raw(log_pointer) as jlong;
    pointer
}

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub unsafe fn addStartEndToRustLog<'local>(mut _env: JNIEnv<'local>, _: JClass, pointer: jlong) {
    let mut log_pointer = Box::from_raw(pointer as *mut EventLog);
    add_start_end_acts(&mut log_pointer);
    let _log_pointer = Box::into_raw(log_pointer);
}

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
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

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub unsafe fn getRustLogAttributes<'local>(
    mut _env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
) -> JString<'local> {
    let log_pointer = Box::from_raw(pointer as *mut EventLog);
    let attributes_json = serde_json::to_string(&log_pointer.attributes).unwrap();
    // memory of log_pointer should _not_ be destroyed!
    let _log_pointer = Box::into_raw(log_pointer);
    _env.new_string(attributes_json).unwrap()
}

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub unsafe fn getRustTraceAttributes<'local>(
    mut _env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
    index: jint,
) -> JString<'local> {
    let log_pointer = Box::from_raw(pointer as *mut EventLog);
    let attributes_json =
        serde_json::to_string(&log_pointer.traces.get(index as usize).unwrap().attributes).unwrap();
    // memory of log_pointer should _not_ be destroyed!
    let _log_pointer = Box::into_raw(log_pointer);
    _env.new_string(attributes_json).unwrap()
}

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub unsafe fn getRustEventAttributes<'local>(
    mut _env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
    trace_index: jint,
    event_index: jint,
) -> JString<'local> {
    let log_pointer = Box::from_raw(pointer as *mut EventLog);
    let attributes_json = serde_json::to_string(
        &log_pointer
            .traces
            .get(trace_index as usize)
            .unwrap()
            .events
            .get(event_index as usize)
            .unwrap()
            .attributes,
    )
    .unwrap();
    // memory of log_pointer should _not_ be destroyed!
    let _log_pointer = Box::into_raw(log_pointer);
    _env.new_string(attributes_json).unwrap()
}

// Frees memory
#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub unsafe fn destroyRustEventLog<'local>(
    mut _env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
) -> jboolean {
    let log_pointer = Box::from_raw(pointer as *mut EventLog);
    println!(
        "Destroying {} Event Log with address {}, which contained {} traces (first trace with {} events)",
        log_pointer
            .attributes
            .get("name")
            .unwrap_or(&"NO NAME".to_string()),
        pointer,
        log_pointer.traces.len(),
        log_pointer.traces.first().unwrap().events.len()
    );

    log_pointer
        .traces
        .first()
        .unwrap()
        .events
        .iter()
        .for_each(|e| {
            println!(
                "Events in first case: {}",
                e.attributes.get("concept:name").unwrap()
            );
        });
    // Deconstruct!
    true.into()
}

#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub fn addArtificialActsUsingFiles<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    importPath: JString<'local>,
    exportPath: JString<'local>,
) -> JString<'local> {
    let mut now = Instant::now();
    let import_path: String = env
        .get_string(&importPath)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let export_path: String = env
        .get_string(&exportPath)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    println!("Time until paths ready: {:.2?}", now.elapsed());
    now = Instant::now();
    let mut log: EventLog = import_log(import_path);
    println!("Time until into EventLog: {:.2?}", now.elapsed());
    now = Instant::now();
    log.attributes
        .insert("name".to_string(), "Transformed Rust Log from file".into());
    add_start_end_acts(&mut log);
    println!("Time until start/end added: {:.2?}", now.elapsed());
    now = Instant::now();
    let log_projection: EventLogActivityProjection<usize> = log.into();
    let log_again: EventLog = log_projection.into();
    println!("Time until into/from: {:.2?}", now.elapsed());
    export_log(export_path.clone(), &log_again);
    return env.new_string(export_path).unwrap();
}
