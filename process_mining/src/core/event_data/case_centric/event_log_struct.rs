use chrono::{DateTime, FixedOffset};
use macros_process_mining::RegistryEntity;
use ordered_float::OrderedFloat;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use uuid::Uuid;

#[cfg(feature = "dataframes")]
use polars::error::PolarsError;
#[cfg(feature = "dataframes")]
use polars::frame::DataFrame;

#[cfg(feature = "dataframes")]
use crate::core::event_data::case_centric::dataframe::convert_log_to_dataframe;
use crate::core::event_data::case_centric::xes::XESOuterLogData;

use super::constants::ACTIVITY_NAME;

///
/// Possible attribute values according to the XES Standard
///
/// Tip: If you know the expected `AttributeValue` type, make use of the `try_as_xxx` functions (e.g., [`AttributeValue::try_as_string`])
///
/// ```rust
/// use process_mining::core::event_data::case_centric::{Attribute, AttributeValue, XESEditableAttribute};
/// let v = AttributeValue::Float(42.0);
///
/// let f = v.try_as_float().unwrap();
/// assert_eq!(*f,42.0);
/// ````
///
///
/// [`AttributeValue`] implements [`Display`] and thus `to_string()`, with the following design decisions:
///
/// For container/list attribute values, a debug representation String is returned.
/// This could, for example, look like this: `[Attribute { key: "test", value: Float(0.3), own_attributes: None }]`.
/// For None attribute vaues, the String `"None"` is returned.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(tag = "type", content = "content")]
pub enum AttributeValue {
    /// String values
    String(String),
    // #[serde(with = "ts_milliseconds")]
    /// `DateTime` values
    Date(DateTime<FixedOffset>),
    /// Integer values
    Int(i64),
    /// Float values
    Float(f64),
    /// Boolean values
    Boolean(bool),
    /// IDs (UUIDs)
    ID(Uuid),
    /// List of other Attributes (where order matters; might contain multiple child attributes with the same key)
    ///
    /// _Note_: Lists should _not_ have nested attributes in the `own_attributes` field, but ONLY in the inner [`Vec<Attribute>`]
    List(Vec<Attribute>),
    /// Container of other Attributes (where order does not matter)
    ///
    /// _Note_: Containers should _not_ have nested attributes in the `own_attributes` field, but ONLY in the inner [`Attributes`]
    Container(Attributes),
    /// Used to represent invalid values (e.g., `DateTime` which could not be parsed)
    None(),
}

impl Display for AttributeValue {
    /// Get String representation of an [`AttributeValue`]
    ///
    /// Note: For container/list attribute values, this returns a debug representation string.
    ///
    /// For None attribute vaues, the String `"None"` is returned.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            AttributeValue::String(s) => s.to_string(),
            AttributeValue::Date(date_time) => date_time.to_string(),
            AttributeValue::Int(i) => i.to_string(),
            AttributeValue::Float(f) => f.to_string(),
            AttributeValue::Boolean(b) => b.to_string(),
            AttributeValue::ID(uuid) => uuid.to_string(),
            AttributeValue::List(attributes) => format!("{:?}", attributes),
            AttributeValue::Container(attributes) => format!("{:?}", attributes),
            AttributeValue::None() => String::from("None"),
        };
        write!(f, "{}", s)
    }
}

impl From<&str> for AttributeValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<String> for AttributeValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl<T> From<DateTime<T>> for AttributeValue
where
    T: chrono::TimeZone,
{
    fn from(value: DateTime<T>) -> Self {
        Self::Date(value.fixed_offset())
    }
}

impl From<i64> for AttributeValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<f64> for AttributeValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<bool> for AttributeValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<Uuid> for AttributeValue {
    fn from(value: Uuid) -> Self {
        Self::ID(value)
    }
}

impl From<Vec<Attribute>> for AttributeValue {
    fn from(value: Vec<Attribute>) -> Self {
        Self::List(value)
    }
}

///
/// [`Hash`] trait implementation for [`AttributeValue`]
///
impl Hash for AttributeValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            AttributeValue::String(value) => value.hash(state),
            AttributeValue::Date(value) => value.hash(state),
            AttributeValue::Int(value) => value.hash(state),
            AttributeValue::Float(value) => OrderedFloat::from(*value).hash(state),
            AttributeValue::Boolean(value) => value.hash(state),
            AttributeValue::ID(value) => value.hash(state),
            AttributeValue::List(value) => value.hash(state),
            AttributeValue::Container(value) => value.hash(state),
            AttributeValue::None() => {}
        }
    }
}

///
/// [`Eq`] trait implementation for [`AttributeValue`]
///
impl Eq for AttributeValue {}

impl AttributeValue {
    ///
    /// Try to get attribute value as String
    ///
    /// Returns `Some()` of inner value if value is of variant [`AttributeValue::String`] and `None` otherwise
    ///
    pub fn try_as_string(&self) -> Option<&String> {
        match self {
            AttributeValue::String(v) => Some(v),
            _ => None,
        }
    }
    ///
    /// Try to get attribute value as date
    ///
    /// Returns `Some()` of inner value if value is of variant [`AttributeValue::Date`] and `None` otherwise
    ///
    pub fn try_as_date(&self) -> Option<&DateTime<FixedOffset>> {
        match self {
            AttributeValue::Date(v) => Some(v),
            _ => None,
        }
    }
    ///
    /// Try to get attribute value as int
    ///
    /// Returns `Some()` of inner value if value is of variant [`AttributeValue::Int`] and `None` otherwise
    ///
    pub fn try_as_int(&self) -> Option<&i64> {
        match self {
            AttributeValue::Int(v) => Some(v),
            _ => None,
        }
    }

    ///
    /// Try to get attribute value as float
    ///
    /// Returns `Some()` of inner value if value is of variant [`AttributeValue::Float`] and `None` otherwise
    ///
    pub fn try_as_float(&self) -> Option<&f64> {
        match self {
            AttributeValue::Float(v) => Some(v),
            _ => None,
        }
    }
    ///
    /// Try to get attribute value as bool
    ///
    /// Returns `Some()` of inner value if value is of variant [`AttributeValue::Boolean`] and `None` otherwise
    ///
    pub fn try_as_bool(&self) -> Option<&bool> {
        match self {
            AttributeValue::Boolean(v) => Some(v),
            _ => None,
        }
    }
    ///
    /// Try to get attribute value as [`Uuid`]
    ///
    /// Returns `Some()` of inner value if value is of variant [`AttributeValue::ID`] and `None` otherwise
    ///
    pub fn try_as_uuid(&self) -> Option<&Uuid> {
        match self {
            AttributeValue::ID(v) => Some(v),
            _ => None,
        }
    }

    ///
    /// Try to get attribute value as list (i.e., nested XES attribute list)
    ///
    /// Returns `Some()` of inner value if value is of variant [`AttributeValue::List`] and `None` otherwise
    ///
    pub fn try_as_list(&self) -> Option<&Vec<Attribute>> {
        match self {
            AttributeValue::List(v) => Some(v),
            _ => None,
        }
    }
    ///
    /// Try to get attribute value as container (i.e., nested XES attributes)
    ///
    /// Returns `Some()` of inner value if value is of variant [`AttributeValue::Container`] and `None` otherwise
    ///
    pub fn try_as_container(&self) -> Option<&Vec<Attribute>> {
        match self {
            AttributeValue::Container(v) => Some(v),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash, Eq, JsonSchema)]
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
/// Attributes are [`Vec`]s of [`Attribute`]s
///
/// See the [`XESEditableAttribute`] trait for convenient functions to add, edit or remove attribute by keys.
///
/// In particular, see [`XESEditableAttribute::get_by_key`], [`XESEditableAttribute::add_attribute`] and [`XESEditableAttribute::get_by_key_or_global`].
///
/// Tip: If you know the expected attribute type, make use of the `try_as_xxx` functions (e.g., [`AttributeValue::try_as_string`])
/// ```rust
/// use process_mining::core::event_data::case_centric::{Attribute, AttributeValue, XESEditableAttribute};
/// let attrs = vec![Attribute::new("key".to_string(), AttributeValue::Float(42.0))];
///
/// let f = attrs.get_by_key("key").and_then(|a| a.value.try_as_float()).unwrap();
/// assert_eq!(*f,42.0);
/// ````
pub type Attributes = Vec<Attribute>;

///
/// Trait to easily add and update attributes
///
pub trait XESEditableAttribute {
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
        global_attrs: &'a Option<Attributes>,
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
impl XESEditableAttribute for Attributes {
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
        global_attrs: &'a Option<Attributes>,
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
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Hash, Eq, JsonSchema)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, JsonSchema)]
pub struct Trace {
    /// Trace-level attributes
    pub attributes: Attributes,
    /// Events contained in trace
    pub events: Vec<Event>,
}

impl Trace {
    /// Initializes a new trace with no attributes and events
    pub fn new() -> Self {
        Self::default()
    }

    ///
    /// Clones a new `Trace` that contains the same attributes but does initially not contain any
    /// events.
    ///
    pub fn clone_without_events(&self) -> Self {
        Self {
            attributes: self.attributes.clone(),
            events: vec![],
        }
    }
}

///
/// Event log consisting of a list of [`Trace`]s and log [`Attributes`]
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, RegistryEntity, JsonSchema)]

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
    /// Initializes a new event log with no attributes, an empty trace list, no extensions, no
    /// classifiers, no global trace attributes, and no global event attributes.
    pub fn new() -> Self {
        Self::default()
    }

    ///
    /// Clones a new `EventLog` that contains the same attributes, extensions, classifiers,
    /// global trace attributes, and global event attributes but does initially not contain any
    /// traces.
    ///
    pub fn clone_without_traces(&self) -> Self {
        Self {
            attributes: self.attributes.clone(),
            traces: vec![],
            extensions: self.extensions.clone(),
            classifiers: self.classifiers.clone(),
            global_trace_attrs: self.global_trace_attrs.clone(),
            global_event_attrs: self.global_event_attrs.clone(),
        }
    }

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

    ///
    /// Get a trace attribute value using a key
    ///
    /// Uses global trace attributes of the event log (if any) as fallback
    /// (i.e., uses the [`XESEditableAttribute::get_by_key_or_global`] function of [`Attributes`] internall)
    ///
    pub fn get_trace_attribute<'a>(&'a self, trace: &'a Trace, key: &str) -> Option<&'a Attribute> {
        trace
            .attributes
            .get_by_key_or_global(key, &self.global_trace_attrs)
    }

    ///
    /// Get an event attribute value using a key
    ///
    /// Uses global event attributes of the event log (if any) as fallback
    /// (i.e., uses the [`XESEditableAttribute::get_by_key_or_global`] function of [`Attributes`] internall)
    ///
    pub fn get_event_attribute<'a>(&'a self, event: &'a Event, key: &str) -> Option<&'a Attribute> {
        event
            .attributes
            .get_by_key_or_global(key, &self.global_trace_attrs)
    }

    #[cfg(feature = "dataframes")]
    ///
    /// Convert this [`EventLog`] to a Polars [`DataFrame`]
    ///
    /// Flattens event log and adds trace-level attributes to events with prefixed attribute key.
    ///
    /// Note: This function is only available if the `dataframes` feature is enabled.
    ///
    pub fn to_dataframe(&self) -> Result<DataFrame, PolarsError> {
        convert_log_to_dataframe(self, false)
    }

    ///
    /// Construct an [`EventLog`] from list of [`Trace`]s and [`XESOuterLogData`]
    ///
    /// This is useful in combination with streaming XES import when traces are filtered/pre-processed directly
    ///
    pub fn from_traces_and_log_data(traces: Vec<Trace>, log_data: XESOuterLogData) -> Self {
        EventLog {
            attributes: log_data.log_attributes,
            traces,
            extensions: Some(log_data.extensions),
            classifiers: Some(log_data.classifiers),
            // Only put global_trace_attrs / global_event_attrs to log data if it is not empty
            global_trace_attrs: if log_data.global_trace_attrs.is_empty() {
                None
            } else {
                Some(log_data.global_trace_attrs)
            },
            global_event_attrs: if log_data.global_event_attrs.is_empty() {
                None
            } else {
                Some(log_data.global_event_attrs)
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, JsonSchema)]
/// An XES Extension
pub struct EventLogExtension {
    /// Extension name
    pub name: String,
    /// Prefix of attributes defined by the extension
    pub prefix: String,
    /// URI pointing to XESEXT of the XES extension
    pub uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
/// Event classifier
///
/// Enables classifying events by a set of attributes to consider for the _class identity_
pub struct EventLogClassifier {
    /// Name of the classifier
    pub name: String,
    /// List of attribute keys to consider for the _class identity_
    ///
    pub keys: Vec<String>,
}

impl Default for EventLogClassifier {
    fn default() -> Self {
        Self {
            name: "Default".to_string(),
            keys: vec![ACTIVITY_NAME.to_string()],
        }
    }
}
impl EventLogClassifier {
    /// Delimiter for combining the values defined by the classifer to form a single class identity string
    pub const DELIMITER: &'static str = "+";
    ///
    /// Get the class identity (joined with [`EventLogClassifier::DELIMITER`])
    ///
    /// Missing attributes and attributes with a type different than [`AttributeValue::String`] are represented by an empty String.
    ///
    pub fn get_class_identity(&self, ev: &Event) -> String {
        self.get_class_identity_with_globals(ev, &None)
    }
    ///
    /// Get the class identity (joined with [`EventLogClassifier::DELIMITER`]) using the global event attributes for default values
    ///
    /// Missing attributes and attributes with a type different than [`AttributeValue::String`] are represented by an empty String.
    ///
    pub fn get_class_identity_with_globals(
        &self,
        ev: &Event,
        global_attrs: &Option<Vec<Attribute>>,
    ) -> String {
        let mut ret: String = String::new();
        let mut first = true;
        for k in &self.keys {
            let s = match ev
                .attributes
                .get_by_key_or_global(k, global_attrs)
                .map(|at| at.value.clone())
            {
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
