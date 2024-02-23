use jni::{
    objects::{JClass, JString},
    sys::{jint, jlong},
    JNIEnv,
};

use uuid::Uuid;

use jni_fn::jni_fn;
use process_mining::event_log::{AttributeAddable, AttributeValue, Attributes, EventLog, Trace};

use super::copy_log_shared::{JEventLog, JTrace};

/// Given the passed reference to a (boxed) [EventLog] and the given `index`, retrieve all attributes of the trace and events
///
/// The returned String is a JSON-encoding of [Vec<Attributes>], where:
/// - 0: Contains the trace attributes (of trace at index _index_)
/// - i (1 to (n-1)): Contains the event attributes of the i'th event in the trace
///
/// Note: This does not free/destroy the passed boxed [EventLog]
#[jni_fn("org.processmining.alpharevisitexperiments.bridge.RustBridge")]
pub unsafe fn getCompleteRustTraceAsString<'local>(
    mut _env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
    index: jint,
) -> JString<'local> {
    let log_pointer = Box::from_raw(pointer as *mut EventLog);
    let trace = log_pointer.traces.get(index as usize).unwrap();
    let mut events_json: Vec<Attributes> = Vec::with_capacity(1 + trace.events.len());
    events_json.push(trace.attributes.clone());
    trace.events.iter().for_each(|e| {
        let mut attrs: Attributes = e.attributes.clone();
        attrs.add_to_attributes("__UUID__".into(), AttributeValue::ID(Uuid::new_v4()));
        events_json.push(attrs)
    });
    let all_json: String = serde_json::to_string(&events_json).unwrap();
    // memory of log_pointer should _not_ be destroyed!
    let _log_pointer = Box::into_raw(log_pointer);
    return _env.new_string(&all_json).unwrap();
}

/// Given the passed reference to a (boxed) [EventLog] and the given `index`, retrieve the indicated [Trace] as compatible JSON String
///
/// The returned String is a JSON-encoding of a [JTrace], which should be compatible with a `XTrace` in java
///
/// All events contained in the [Trace]/[JTrace] are also included (see [JTrace] struct)
///
/// Note: This does not free/destroy the passed boxed [EventLog]
#[jni_fn("org.processmining.alpharevisitexperiments.bridge.RustBridge")]
pub unsafe fn getCompleteRustTraceAsStringJsonCompatible<'local>(
    mut _env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
    index: jint,
) -> JString<'local> {
    let log_pointer = Box::from_raw(pointer as *mut EventLog);
    let trace: &Trace = log_pointer.traces.get(index as usize).unwrap();
    let jtrace: JTrace = trace.into();
    let trace_json: String = serde_json::to_string(&jtrace).unwrap();
    // memory of log_pointer should _not_ be destroyed!
    let _log_pointer = Box::into_raw(log_pointer);
    return _env.new_string(&trace_json).unwrap();
}

/// Given the passed reference to a (boxed) [EventLog] retrieve the (complete) [EventLog] as compatible JSON String
///
/// The returned String is a JSON-encoding of a [JEventLog], which should be compatible with a `XLog` in Java
///
/// All traces (and recursively events) contained in the [EventLog] are also included (see [JEventLog] struct)
///
/// Note: This does not free/destroy the passed boxed [EventLog]
#[jni_fn("org.processmining.alpharevisitexperiments.bridge.RustBridge")]
pub unsafe fn getCompleteRustLogAsStringJsonCompatible<'local>(
    mut _env: JNIEnv<'local>,
    _: JClass,
    pointer: jlong,
) -> JString<'local> {
    let log_pointer = Box::from_raw(pointer as *mut EventLog);
    let log: JEventLog = log_pointer.as_ref().into();
    let log_json: String = serde_json::to_string(&log).unwrap();
    // memory of log_pointer should _not_ be destroyed!
    let _log_pointer = Box::into_raw(log_pointer);
    return _env.new_string(&log_json).unwrap();
}
