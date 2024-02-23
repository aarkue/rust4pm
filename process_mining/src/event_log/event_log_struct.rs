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
    #[deprecated(
        since = "0.2.0",
        note = "This function will be removed soon as Attributes are now backed by Vec instead of HashMap"
    )]
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
pub type Attributes = Vec<Attribute>;

///
/// Trait to easily add and update attributes
///
pub trait AttributeAddable {
    fn add_to_attributes(&mut self, key: String, value: AttributeValue);
    fn get_by_key(&self, key: &str) -> Option<&Attribute>;
    fn get_by_key_mut(&mut self, key: &str) -> Option<&mut Attribute>;
    fn add_attribute(&mut self, attr: Attribute);
    fn remove_with_key(&mut self, key: &str) -> bool;
}
impl AttributeAddable for Attributes {
    ///
    /// Add a new attribute (with key and value)
    ///
    /// Note: Does _not_ check if attribute was already present and does _not_ sort attributes wrt. key.
    ///
    fn add_to_attributes(&mut self, key: String, value: AttributeValue) {
        let a = Attribute::new(key, value);
        self.push(a);
    }

    ///
    /// Add a new attribute
    ///
    fn add_attribute(&mut self, a: Attribute) {
        self.push(a);
    }

    ///
    /// Get an attribute by key
    ///
    /// _Complexity_: Does linear lookup (i.e., in O(n)). If you need faster lookup, consider manually sorting the attributes by key and utilizing binary search.
    fn get_by_key(&self, key: &str) -> Option<&Attribute> {
        self.iter().find(|attr| attr.key == key)
    }

    ///
    /// Get an attribute as mutable by key
    ///
    /// _Complexity_: Does linear lookup (i.e., in O(n)). If you need faster lookup, consider manually sorting the attributes by key and utilizing binary search.
    fn get_by_key_mut(&mut self, key: &str) -> Option<&mut Attribute> {
        self.iter_mut().find(|attr| attr.key == key)
    }

    ///
    /// Remove attribute with given key
    ///
    /// Returns `true` if the attribute was present and `false` otherwise
    ///
    /// _Complexity_: Does linear lookup (i.e., in O(n)). If you need faster lookup, consider manually sorting the attributes by key and utilizing binary search.
    fn remove_with_key(&mut self, key: &str) -> bool {
        let index_opt = self.iter().position(|a| a.key == key);
        if let Some(index) = index_opt {
            self.remove(index);
            return true;
        }
        false
    }
}

pub fn to_attributes(from: HashMap<String, AttributeValue>) -> Attributes {
    from.into_iter()
        .map(|(key, value)| Attribute {
            key: key.clone(),
            value,
            own_attributes: None,
        })
        .collect()
}

///
/// An event consists of multiple (event) attributes ([Attributes])
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventLog {
    pub attributes: Attributes,
    pub traces: Vec<Trace>,
    pub extensions: Option<Vec<EventLogExtension>>,
    pub classifiers: Option<Vec<EventLogClassifier>>,
    pub global_event_attrs: Option<Attributes>,
    pub global_trace_attrs: Option<Attributes>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EventLogExtension {
    pub name: String,
    pub prefix: String,
    pub uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventLogClassifier {
    pub name: String,
    pub keys: Vec<String>,
}
impl EventLogClassifier {
    pub const DELIMITER: &'static str = "+";
    ///
    /// Get the class identity (joined with [EventLogClassifier::DELIMITER])
    ///
    /// Missing attributes and attributes with a type different than [AttributeValue::String] are represented by an empty String.
    ///
    pub fn get_class_identity(&self, ev: &Event) -> String {
        let mut ret: String = String::new();
        let mut first = true;
        for k in &self.keys {
            let s = match ev.attributes.get_by_key(k).map(|at| at.value.clone()) {
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
