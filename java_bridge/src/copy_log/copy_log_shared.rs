use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use process_mining::event_log::{Attribute, Attributes, Event, EventLog, Trace};

/// Used for Java-compatible XLog JSON generation
///
/// Corresponds to XEvent
#[derive(Debug, Serialize, Deserialize)]
pub struct JEvent {
    uuid: String,
    pub attributes: HashMap<String, Attribute>,
}

/// Used for Java-compatible XLog JSON generation
///
/// Corresponds to XTrace
#[derive(Debug, Serialize, Deserialize)]
pub struct JTrace {
    attributes: HashMap<String, Attribute>,
    events: Vec<JEvent>,
}
/// Used for Java-compatible XLog JSON generation <p>
///
/// Corresponds to XLog
#[derive(Debug, Serialize, Deserialize)]
pub struct JEventLog {
    attributes: HashMap<String, Attribute>,
    traces: Vec<JTrace>,
}

/// Converts a [HashMap<String,String>] to a [HashMap<String,Attribute>] (i.e., to a mapping to [Attribute]s)
///
/// Currently required for converting an [EventLog] to an [JEventLog] (+ all subtypes)
fn stringMapToAttributeMap(map: &Attributes) -> HashMap<String, Attribute> {
    map.iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// Converts as [HashMap<String, Attribute>] to a [Attributes] (i.e., from a mapping of [Attribute]s)
///
/// Currently, [Attribute]s can only hold Strings, so this just un-wraps that container
///
/// Required for converting a [JEventLog] to an [EventLog] (+ all subtypes)
fn AttributeMapToStringMap(map: &HashMap<String, Attribute>) -> Attributes {
    map.iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

impl From<&Event> for JEvent {
    /// Note: Generates new uuids for the resulting [JEvent]!
    fn from(value: &Event) -> Self {
        JEvent {
            attributes: stringMapToAttributeMap(&value.attributes),
            uuid: Uuid::new_v4().to_string(),
        }
    }
}

impl From<&Trace> for JTrace {
    fn from(value: &Trace) -> Self {
        JTrace {
            attributes: stringMapToAttributeMap(&value.attributes),
            events: value.events.iter().map(|e| e.into()).collect(),
        }
    }
}

impl From<&EventLog> for JEventLog {
    fn from(value: &EventLog) -> Self {
        JEventLog {
            attributes: stringMapToAttributeMap(&value.attributes),
            traces: value.traces.iter().map(|e| e.into()).collect(),
        }
    }
}

impl Into<Event> for &JEvent {
    fn into(self) -> Event {
        Event {
            attributes: AttributeMapToStringMap(&self.attributes),
        }
    }
}

impl Into<Trace> for JTrace {
    fn into(self) -> Trace {
        Trace {
            attributes: AttributeMapToStringMap(&self.attributes),
            events: self.events.iter().map(|e| e.into()).collect(),
        }
    }
}
