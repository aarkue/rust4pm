use std::collections::HashMap;

use chrono::{serde::ts_milliseconds, DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::constants::ACTIVITY_NAME;

///
/// Possible attribute values according to the XES Standard
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "content")]
pub enum AttributeValue {
    /// String values
    String(String),
    #[serde(with = "ts_milliseconds")]
    /// DateTime values
    Date(DateTime<Utc>),
    /// Integer values
    Int(i64),
    /// Float values
    Float(f64),
    /// Boolean values
    Boolean(bool),
    /// IDs (UUIDs)
    ID(Uuid),
    /// List of other Attributes (where order matters; might contain multiple child attributes with the same key)
    List(Vec<Attribute>),
    /// List of other Attributes (where order does not matter)
    Container(Attributes),
    /// Used to represent invalid values (e.g., DateTime which could not be parsed)
    None(),
}

impl AttributeValue {
    ///
    /// Try to get attribute value as String
    ///
    /// Returns inner value if self is of variant [`AttributeValue::String`]
    ///
    /// Otherwise, returns None

    pub fn try_get_string(&self) -> Option<&String> {
        match self {
            AttributeValue::String(s) => Some(s),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
///
/// Attribute made up of the key and value
///
/// Depending on usage, the key field might be redundant but useful for some implementations
///
pub struct Attribute {
    /// Attribute key
    pub key: String,
    /// Attribute value
    pub value: AttributeValue,
    /// Child attributes (nested)
    pub own_attributes: Option<Attributes>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// Version of [`Attribute`] which represents underlying nested attributes as a [`HashMap`]
///
/// Only used for easier JSON-interop with `ProM`
pub struct HashMapAttribute {
    /// Attribute key
    pub key: String,
    /// Attribute value
    pub value: AttributeValue,
    /// Child attributes (nested)
    pub own_attributes: Option<HashMap<String, HashMapAttribute>>,
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
    /// This is useful for directly inserting the attribute in a [`HashMap`] afterwards
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
/// Attributes are [`Vec`] of [`Attribute`]s
///
pub type Attributes = Vec<Attribute>;

///
/// Trait to easily add and update attributes
///
pub trait AttributeAddable {
    ///
    /// Add a new attribute (with key and value)
    ///
    /// Note: Does _not_ check if attribute was already present and does _not_ sort attributes wrt. key.
    ///
    fn add_to_attributes(&mut self, key: String, value: AttributeValue);
    ///
    /// Add a new attribute
    ///
    fn add_attribute(&mut self, attr: Attribute);
    ///
    /// Get an attribute by key
    ///
    /// _Complexity_: Does linear lookup (i.e., in O(n)). If you need faster lookup, consider manually sorting the attributes by key and utilizing binary search.
    fn get_by_key(&self, key: &str) -> Option<&Attribute>;
    ///
    /// Get an attribute as mutable by key
    ///
    /// _Complexity_: Does linear lookup (i.e., in O(n)). If you need faster lookup, consider manually sorting the attributes by key and utilizing binary search.
    fn get_by_key_mut(&mut self, key: &str) -> Option<&mut Attribute>;
    ///
    /// Get an attribute by key or the default value (e.g., provided by global event or trace attributes)
    ///
    /// _Complexity_: Does linear lookup (i.e., in O(n)). If you need faster lookup, consider manually sorting the attributes by key and utilizing binary search.
    fn get_by_key_or_global<'a>(
        &'a self,
        key: &str,
        global_attrs: &'a Option<&'a Attributes>,
    ) -> Option<&'a Attribute>;
    ///
    /// Remove attribute with given key
    ///
    /// Returns `true` if the attribute was present and `false` otherwise
    ///
    /// _Complexity_: Does linear lookup (i.e., in O(n)). If you need faster lookup, consider manually sorting the attributes by key and utilizing binary search.
    fn remove_with_key(&mut self, key: &str) -> bool;

    ///
    /// Convert Attributes to [`HashMap`]-backed version
    ///
    /// Used for creating attribute structures that are more easily compatible with other JSON representations of [`Attributes`].
    ///
    /// __Usage is generally discouraged__
    ///
    /// _Warning_: Currently, nested attributes are stripped.
    ///
    ///
    fn as_hash_map(&self) -> HashMap<String, HashMapAttribute>;
}
impl AttributeAddable for Attributes {
    fn add_to_attributes(&mut self, key: String, value: AttributeValue) {
        let a = Attribute::new(key, value);
        self.push(a);
    }

    fn add_attribute(&mut self, a: Attribute) {
        self.push(a);
    }

    fn get_by_key(&self, key: &str) -> Option<&Attribute> {
        self.iter().find(|attr| attr.key == key)
    }

    fn get_by_key_mut(&mut self, key: &str) -> Option<&mut Attribute> {
        self.iter_mut().find(|attr| attr.key == key)
    }

    fn get_by_key_or_global<'a>(
        &'a self,
        key: &str,
        global_attrs: &'a Option<&'a Attributes>,
    ) -> Option<&'a Attribute> {
        // TODO
        if let Some(attr) = self.iter().find(|attr| attr.key == key) {
            return Some(attr);
        }
        if let Some(global_attrs) = global_attrs {
            if let Some(attr) = global_attrs.get_by_key(key) {
                return Some(attr);
            }
        }
        None
    }

    fn remove_with_key(&mut self, key: &str) -> bool {
        let index_opt = self.iter().position(|a| a.key == key);
        if let Some(index) = index_opt {
            self.remove(index);
            return true;
        }
        false
    }
    fn as_hash_map(&self) -> HashMap<String, HashMapAttribute> {
        self.iter()
            .map(|a| {
                let a_clone = HashMapAttribute {
                    key: a.key.clone(),
                    value: a.value.clone(),
                    own_attributes: None,
                };
                (a.key.clone(), a_clone)
            })
            .collect()
    }
}

/// Covert a [`HashMap`] of attributes to a [`Attributes`] representation
pub fn to_attributes(from: HashMap<String, AttributeValue>) -> Attributes {
    from.into_iter()
        .map(|(key, value)| Attribute {
            key,
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
    /// Event attributes
    pub attributes: Attributes,
}
impl Event {
    /// Create a new event with the provided activity
    ///
    /// Implicitly assumes usage of the concept XES extension (i.e., uses [`ACTIVITY_NAME`] as key)
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
/// A trace consists of a list of events and trace attributes (See also [`Event`] and [`Attributes`])
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Trace {
    /// Trace-level attributes
    pub attributes: Attributes,
    /// Events contained in trace
    pub events: Vec<Event>,
}

///
/// Event log consisting of a list of [`Trace`]s and log [`Attributes`]
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventLog {
    /// Top-level attributes
    pub attributes: Attributes,
    /// Traces contained in log
    pub traces: Vec<Trace>,
    /// XES Extensions
    pub extensions: Option<Vec<EventLogExtension>>,
    /// XES Event classifiers
    pub classifiers: Option<Vec<EventLogClassifier>>,
    /// Global trace attributes
    pub global_trace_attrs: Option<Attributes>,
    ///  Global event attributes
    pub global_event_attrs: Option<Attributes>,
}

impl EventLog {
    ///
    /// Try to get the [`EventLogClassifier`] with the associated name
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
/// An XES Extension
pub struct EventLogExtension {
    /// Extension name
    pub name: String,
    /// Prefix of attributes defined by the extension
    pub prefix: String,
    /// URI pointing to XESEXT of the XES extension 
    pub uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// Event classifier
/// 
/// Enables classifying events by a set of attributes to consider for the _class identity_
pub struct EventLogClassifier {
    /// Name of the classifier
    pub name: String,
    /// List of attribute keys to consider for the _class identity_
    ///
    /// Note: Currently keys might not be correctly parsed (i.e., they are just split at a " " while the XES standard defined more complicated detection)
    ///
    /// TODO: Investigate aligning parsing implementation with XES standard
    pub keys: Vec<String>,
}
impl EventLogClassifier {
    /// Delimiter for combining the values defined by the classifer to form a single class identity string
    pub const DELIMITER: &'static str = "+";
    ///
    /// Get the class identity (joined with [`EventLogClassifier::DELIMITER`])
    ///
    /// Missing attributes and attributes with a type different than [`AttributeValue::String`] are represented by an empty String.
    ///
    ///
    /// Note: Currently classifier keys might not be correctly parsed (i.e., they are just split at a " " while the XES standard defined more complicated detection)
    ///
    /// TODO: Investigate aligning parsing implementation with XES standard
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
