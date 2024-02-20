use std::collections::HashMap;

use chrono::{serde::ts_milliseconds, DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::constants::{ACTIVITY_NAME, TRACE_ID_NAME};

///
/// Possible attribute values according to the XES Standard
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "content")]
pub enum AttributeValue {
    String(String),
    #[serde(with = "ts_milliseconds")]
    Date(DateTime<Utc>),
    Int(i64),
    Float(f64),
    Boolean(bool),
    ID(Uuid),
    List(Vec<Attribute>),
    Container(Attributes),
    None(),
}

///
/// Attribute made up of the key and value
///
/// Depending on usage, the key field might be redundant but useful for some implementations
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Attribute {
    pub key: String,
    pub value: AttributeValue,
    pub own_attributes: Option<Attributes>,
}
impl Attribute {
    ///
    /// Helper to create a new attribute
    ///
    pub fn new(key: String, attribute_val: AttributeValue) -> Self {
        Self {
            key,
            value: attribute_val,
            own_attributes: None,
        }
    }
    ///
    /// Helper to create a new attribute, while returning the key String additionally
    ///
    /// This is useful for directly inserting the attribute in a [HashMap] afterwards
    ///
    pub fn new_with_key(key: String, attribute_val: AttributeValue) -> (String, Self) {
        (
            key.clone(),
            Self {
                key,
                value: attribute_val,
                own_attributes: None,
            },
        )
    }
}

///
/// Attributes are [HashMap] mapping a key ([String]) to an [Attribute]
///
pub type Attributes = HashMap<String, Attribute>;

///
/// Trait to easily add a new attribute
pub trait AttributeAddable {
    fn add_to_attributes(&mut self, key: String, value: AttributeValue) -> Option<&mut Attribute>;
}
impl AttributeAddable for Attributes {
    ///
    /// Add a new attribute (with key and value)
    ///
    fn add_to_attributes(&mut self, key: String, value: AttributeValue) -> Option<&mut Attribute> {
        let (k, v) = Attribute::new_with_key(key, value);
        self.insert(k.clone(), v);
        return self.get_mut(&k);
    }
}

pub fn to_attributes(from: HashMap<String, AttributeValue>) -> Attributes {
    from.into_iter()
        .map(|(key, value)| {
            (
                key.clone(),
                Attribute {
                    key: key.clone(),
                    value,
                    own_attributes: None,
                },
            )
        })
        .collect()
}

///
/// An event consists of multiple (event) attributes ([Attributes])
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub attributes: Attributes,
}
impl Event {
    pub fn new(activity: String) -> Self {
        Event {
            attributes: to_attributes(
                vec![(ACTIVITY_NAME.to_string(), AttributeValue::String(activity))]
                    .into_iter()
                    .collect(),
            ),
        }
    }
}

///
/// A trace consists of a list of events and trace attributes (See also [Event] and [Attributes])
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    pub attributes: Attributes,
    pub events: Vec<Event>,
}
impl Trace {
    pub fn new(activity: String) -> Self {
        Trace {
            events: Vec::new(),
            attributes: to_attributes(
                vec![(TRACE_ID_NAME.to_string(), AttributeValue::String(activity))]
                    .into_iter()
                    .collect(),
            ),
        }
    }
}

///
/// Event log consisting of a list of [Trace]s and log [Attributes]
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventLog {
    pub attributes: Attributes,
    pub traces: Vec<Trace>,
    pub extensions: Option<Vec<EventLogExtension>>,
    pub classifiers: Option<Vec<EventLogClassifier>>,
}

impl EventLog {
    ///
    /// Try to get the [EventLogClassifier] with the associated name
    ///
    pub fn get_classifier_by_name<S>(&self, name: S) -> Option<EventLogClassifier>
    where
        std::string::String: PartialEq<S>,
    {
        self.classifiers
            .as_ref()
            .and_then(|classifiers| classifiers.iter().find(|c| c.name == name).cloned())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventLogExtension {
    pub name: String,
    pub prefix: String,
    pub uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventLogClassifier {
    pub name: String,
    pub keys: Vec<String>,
}
impl EventLogClassifier {
    const DELIMITER: &str = "+";
    ///
    /// Get the class identity (joined with [EventLogClassifier::DELIMITER])
    /// 
    /// Missing attributes and attributes with a type different than [AttributeValue::String] are represented by an empty String.
    ///
    pub fn get_class_identity(&self, ev: &Event) -> String {
        let mut ret: String = String::new();
        let mut first = true;
        for k in &self.keys {
            let s = match ev.attributes.get(k).map(|at| at.value.clone()) {
                Some(AttributeValue::String(s)) => s,
                _ => String::new(),
            };
            if !first {
                ret.push_str(EventLogClassifier::DELIMITER)
            } else {
                first = false;
            }
            ret.push_str(&s);
        }
        ret
    }
}
