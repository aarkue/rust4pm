use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use pm_rust::{Event, EventLog, Trace};

/// Used for Java-compatible XLog JSON generation
/// 
/// Corresponds to XAttribute (attributeType is currently unused & always 'string')
#[derive(Debug, Serialize, Deserialize)]
struct JAttribute {
    key: String,
    attributeType: String,
    value: String,
}

/// Used for Java-compatible XLog JSON generation
///
/// Corresponds to XEvent
#[derive(Debug, Serialize, Deserialize)]
struct JEvent {
    uuid: String,
    attributes: HashMap<String, JAttribute>,
}

/// Used for Java-compatible XLog JSON generation
/// 
/// Corresponds to XTrace
#[derive(Debug, Serialize, Deserialize)]
pub struct JTrace {
    attributes: HashMap<String, JAttribute>,
    events: Vec<JEvent>,
}
/// Used for Java-compatible XLog JSON generation <p>
///
/// Corresponds to XLog
#[derive(Debug, Serialize, Deserialize)]
pub struct JEventLog {
    attributes: HashMap<String, JAttribute>,
    traces: Vec<JTrace>,
}

/// Converts a [HashMap<String,String>] to a [HashMap<String,JAttribute>] (i.e., to a mapping to [JAttribute]s)
///
/// Currently required for converting an [EventLog] to an [JEventLog] (+ all subtypes)
fn stringMapToJAttributeMap(map: &HashMap<String, String>) -> HashMap<String, JAttribute> {
    map.iter()
        .map(|(key, value)| {
            (
                key.clone(),
                JAttribute {
                    key: key.clone(),
                    attributeType: "String".into(),
                    value: value.clone(),
                },
            )
        })
        .collect()
}

/// Converts as [HashMap<String, JAttribute>] to a [HashMap<String, String>] (i.e., from a mapping of [JAttribute]s)
///
/// Currently, [JAttribute]s can only hold Strings, so this just un-wraps that container
/// 
/// Required for converting a [JEventLog] to an [EventLog] (+ all subtypes)
fn jAttributeMapToStringMap(map: &HashMap<String, JAttribute>) -> HashMap<String, String> {
    map.iter()
        .map(|(key, value)| (key.clone(), value.value.clone()))
        .collect()
}

impl From<&Event> for JEvent {
    /// Note: Generates new uuids for the resulting [JEvent]!
    fn from(value: &Event) -> Self {
        JEvent {
            attributes: stringMapToJAttributeMap(&value.attributes),
            uuid: Uuid::new_v4().to_string(),
        }
    }
}

impl From<&Trace> for JTrace {
    fn from(value: &Trace) -> Self {
        JTrace {
            attributes: stringMapToJAttributeMap(&value.attributes),
            events: value.events.iter().map(|e| e.into()).collect(),
        }
    }
}

impl From<&EventLog> for JEventLog {
    fn from(value: &EventLog) -> Self {
        JEventLog {
            attributes: stringMapToJAttributeMap(&value.attributes),
            traces: value.traces.iter().map(|e| e.into()).collect(),
        }
    }
}

impl Into<Event> for &JEvent {
    fn into(self) -> Event {
        Event {
            attributes: jAttributeMapToStringMap(&self.attributes),
        }
    }
}

impl Into<Trace> for JTrace {
    fn into(self) -> Trace {
        Trace {
            attributes: jAttributeMapToStringMap(&self.attributes),
            events: self.events.iter().map(|e| e.into()).collect(),
        }
    }
}
