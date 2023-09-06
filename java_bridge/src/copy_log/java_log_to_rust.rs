use jni::{
    objects::{JClass, JString},
    sys::{jboolean, jint, jlong},
    JNIEnv,
};
use std::collections::HashMap;

use jni_fn::jni_fn;
use pm_rust::{Attributes, Event, EventLog, Trace};

use super::copy_log_shared::JTrace;

/// Construction struct used when copying an XLog from Java (i.e., creating a [EventLog] from it)
///
/// User must guarantee, that same trace is not modified concurrently
/// (This is achieved by only parallelizing on the trace ids)
///
/// This struct is heavily used in unsafe code, to allow efficient copying of XLogs to [EventLog]s
struct EventLogConstruction {
    traces: Vec<Box<Trace>>,
    attributes: Attributes,
}

/// Intialize [EventLogConstruction] stub for (parallel) copying of Java XLog
///
/// __Warning:__ Returned jlong points to (boxed) [EventLogConstruction] struct which __must be manually destroyed__
///
/// The __caller must guarantee__ to (eventually) call __[finishLogConstructionPar]__ and then __[destroyRustEventLog]__
///
/// Otherwise, memory is leaked.
///
/// TODO: Add destroyRustEventLogConstruction binding
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
    let attributes: Attributes = serde_json::from_str(&attribute_str).unwrap();
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

/// Given a pointer ([jlong]) to a [EventLogConstruction], add the passed trace information to the trace at index `trace_index`
///
/// The trace information is made up of:
/// - `trace_attributes_json`: JSON-encoded string containing [HashMap<String,String]-like trace attributes
/// - `event_attributes_json`: JSON-encoed string containing [Vec<HashMap<String,String>]-like event attributes (i.e., one entry for each event)
///
/// Note: The passed (referenced) [EventLogConstruction] _is not_ destroyed, freed or finalized by this function but __trace at index `trace_index` is modified__
#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub unsafe fn setTracePar<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
    trace_index: jint,
    trace_attributes_json: JString,
    event_attributes_json: JString,
) {
    let mut log_constr_pointer = Box::from_raw(pointer as *mut EventLogConstruction);
    let trace_attributes_str: String = env.get_string(&trace_attributes_json).unwrap().into();
    let trace = log_constr_pointer
        .traces
        .get_mut(trace_index as usize)
        .unwrap();
    let trace_attributes: Attributes = serde_json::from_str(&trace_attributes_str).unwrap();
    trace.attributes = trace_attributes;
    let event_attrs: Vec<Attributes> = serde_json::from_str(
        env.get_string(&event_attributes_json)
            .unwrap()
            .to_str()
            .unwrap(),
    )
    .unwrap();
    trace.events = event_attrs
        .into_iter()
        .map(|e_attrs| Event {
            attributes: e_attrs,
        })
        .collect();
    // Should not be freed! Thus convert back.
    let _pointer = Box::into_raw(log_constr_pointer);
}

/// Similar to [setTracePar] function, but use JSON string of XTrace-compatible [JTrace] object
///
/// i.e., Passed `trace_json` is assumed to be valid JSON-serialization of the [JTrace] struct
///
/// `trace_index` indicates trace on `EventLogConstruction` which to _replace_ with converted [JTrace] (first converted to a [Trace])
///
/// Note: The passed (referenced) [EventLogConstruction] _is not_ destroyed, freed or finalized by this function but __trace at index `trace_index` is modified__
#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub unsafe fn setTraceParJsonCompatible<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
    trace_index: jint,
    trace_json: JString,
) {
    let mut log_constr_pointer = Box::from_raw(pointer as *mut EventLogConstruction);
    let trace_json_str: String = env.get_string(&trace_json).unwrap().into();
    let jtrace: JTrace = serde_json::from_str(&trace_json_str).unwrap();
    log_constr_pointer.traces[trace_index as usize] = Box::new(jtrace.into());
    // Should not be freed! Thus convert back.
    let _pointer = Box::into_raw(log_constr_pointer);
}

/// Converts a (populated) [EventLogConstruction] to an [EventLog]
///
/// The [EventLogConstruction] can be created using [createRustEventLogPar]
///
/// This frees/destroys the [EventLogConstruction] (given by box reference `pointer`)
///
/// __Warning:__ Returned jlong points to (boxed) [EventLog] struct which __must be manually destroyed__
///
/// The __caller must guarantee__ to (eventually) call __[destroyRustEventLog]__ with the returned pointer!
///
/// Otherwise, memory is leaked.
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

/// Destroys the (boxed) [EventLog] referenced by `pointer` and frees associated memory
///
/// This function __must__ be called for each created [EventLog], which is behind a pointer (e.g., `long` in Java)
#[jni_fn("org.processmining.alpharevisitexperiments.bridge.HelloProcessMining")]
pub unsafe fn destroyRustEventLog<'local>(
    mut _env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
) -> jboolean {
    let _log_pointer = Box::from_raw(pointer as *mut EventLog);
    // Deconstruct!
    true.into()
}
