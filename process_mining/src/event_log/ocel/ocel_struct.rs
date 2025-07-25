use std::collections::HashSet;
use std::fmt::Display;

use crate::ocel::linked_ocel::index_linked_ocel::ObjectIndex;
use crate::ocel::linked_ocel::IndexLinkedOCEL;
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};

///
/// Object-centric Event Log
///
/// Consists of multiple [`OCELEvent`]s and [`OCELObject`]s with corresponding event and object [`OCELType`]s
///
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct OCEL {
    /// Event Types in OCEL
    #[serde(rename = "eventTypes")]
    pub event_types: Vec<OCELType>,
    /// Object Types in OCEL
    #[serde(rename = "objectTypes")]
    pub object_types: Vec<OCELType>,
    /// Events contained in OCEL
    #[serde(default)]
    pub events: Vec<OCELEvent>,
    /// Objects contained in OCEL
    #[serde(default)]
    pub objects: Vec<OCELObject>,
}

impl OCEL {
    ///
    /// Removes all [`OCELObject`] that do not have an e2o relation
    ///
    pub fn remove_orphan_objects(self) -> OCEL {
        let locel: IndexLinkedOCEL = IndexLinkedOCEL::from(self);

        let objects_with_e2o = locel
            .e2o_rev_et
            .iter()
            .flat_map(|(_, o2e_set)| o2e_set.keys().cloned())
            .collect::<HashSet<_>>();

        let mut underlying_ocel = locel.into_inner();

        underlying_ocel.objects = underlying_ocel
            .objects
            .iter()
            .enumerate()
            .filter_map(
                |(index, obj)| match objects_with_e2o.contains(&ObjectIndex::from(index)) {
                    true => Some(obj.clone()),
                    false => None,
                },
            )
            .collect::<Vec<_>>();

        underlying_ocel
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
/// OCEL Event/Object Type
pub struct OCELType {
    /// Name
    pub name: String,
    /// Attributes (defining the _type_ of values)
    #[serde(default)]
    pub attributes: Vec<OCELTypeAttribute>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
/// OCEL Attribute types
pub struct OCELTypeAttribute {
    /// Name of attribute
    pub name: String,
    /// Type of attribute
    #[serde(rename = "type")]
    pub value_type: String,
}

impl OCELTypeAttribute {
    /// Construct a type attribute based on a given name and type
    pub fn new<S: AsRef<str>>(name: S, value_type: &OCELAttributeType) -> Self {
        Self {
            name: name.as_ref().to_string(),
            value_type: value_type.to_type_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
/// OCEL Event Attributes
pub struct OCELEventAttribute {
    /// Name of event attribute
    pub name: String,
    /// Value of attribute
    pub value: OCELAttributeValue,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
/// OCEL Event
pub struct OCELEvent {
    /// Event ID
    pub id: String,
    /// Event Type (referring back to the `name` of an [`OCELType`])
    #[serde(rename = "type")]
    pub event_type: String,
    /// `DateTime` when event occured
    pub time: DateTime<FixedOffset>,
    /// Event attributes
    #[serde(default)]
    pub attributes: Vec<OCELEventAttribute>,
    /// E2O (Event-to-Object) relationships
    #[serde(default)]
    pub relationships: Vec<OCELRelationship>,
}

impl OCELEvent {
    /// Construct a new OCEL Event
    pub fn new<S1: AsRef<str>, S2: AsRef<str>, T: Into<DateTime<FixedOffset>>>(
        id: S1,
        event_type: S2,
        time: T,
        attributes: Vec<OCELEventAttribute>,
        relationships: Vec<OCELRelationship>,
    ) -> Self {
        Self {
            id: id.as_ref().to_string(),
            event_type: event_type.as_ref().to_string(),
            time: time.into(),
            attributes,
            relationships,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
/// OCEL Relationship (qualified; referring back to an [`OCELObject`])
pub struct OCELRelationship {
    /// ID of referenced [`OCELObject`]
    #[serde(rename = "objectId")]
    pub object_id: String,
    /// Qualifier of relationship
    pub qualifier: String,
}

impl OCELRelationship {
    /// Construct a new OCEL Relationship
    pub fn new<S: AsRef<str>, Q: AsRef<str>>(to_object_id: S, qualifier: Q) -> Self {
        Self {
            object_id: to_object_id.as_ref().to_string(),
            qualifier: qualifier.as_ref().to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
/// OCEL Object
pub struct OCELObject {
    /// Object ID
    pub id: String,
    /// Object Type (referring back to thte `name` of an [`OCELType`])
    #[serde(rename = "type")]
    pub object_type: String,
    /// Object attributes
    #[serde(default)]
    pub attributes: Vec<OCELObjectAttribute>,
    /// O2O (Object-to-Object) relationships
    #[serde(default)]
    pub relationships: Vec<OCELRelationship>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
/// OCEL Object Attribute
///
/// Describing a named value _at a certain point in time_
pub struct OCELObjectAttribute {
    /// Name of attribute
    pub name: String,
    /// Value of attribute
    pub value: OCELAttributeValue,
    /// Time of attribute value
    #[serde(deserialize_with = "robust_timestamp_parsing")]
    pub time: DateTime<FixedOffset>,
}

fn robust_timestamp_parsing<'de, D>(deserializer: D) -> Result<DateTime<FixedOffset>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let time: String = Deserialize::deserialize(deserializer)?;
    if let Ok(dt) = DateTime::parse_from_rfc3339(&time) {
        return Ok(dt);
    }
    if let Ok(dt) = DateTime::parse_from_rfc2822(&time) {
        return Ok(dt);
    }
    // eprintln!("Encountered weird datetime format: {:?}", time);

    // Some logs have this date: "2023-10-06 09:30:21.890421"
    // Assuming that this is UTC
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&time, "%F %T%.f") {
        return Ok(dt.and_utc().into());
    }

    // Also handle "2024-10-02T07:55:15.348555" as well as "2022-01-09T15:00:00"
    // Assuming UTC time zone
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&time, "%FT%T%.f") {
        return Ok(dt.and_utc().into());
    }

    // export_path
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&time, "%F %T UTC") {
        return Ok(dt.and_utc().into());
    }

    // Who made me do this? 🫣
    // Some logs have this date: "Mon Apr 03 2023 12:08:18 GMT+0200 (Mitteleuropäische Sommerzeit)"
    // Below ignores the first "Mon " part (%Z) parses the rest (only if "GMT") and then parses the timezone (+0200)
    // The rest of the input is ignored
    if let Ok((dt, _)) = DateTime::parse_and_remainder(&time, "%Z %b %d %Y %T GMT%z") {
        return Ok(dt);
    }
    Err(serde::de::Error::custom("Unexpected Date Format"))
}

impl OCELObjectAttribute {
    /// Construct a new object attribute given its name, value, and time
    pub fn new<S: AsRef<str>, V: Into<OCELAttributeValue>, T: Into<DateTime<FixedOffset>>>(
        name: S,
        value: V,
        time: T,
    ) -> Self {
        Self {
            name: name.as_ref().to_string(),
            value: value.into(),
            time: time.into(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
/// OCEL Attribute Values
pub enum OCELAttributeValue {
    /// `DateTime`
    Time(DateTime<FixedOffset>),
    /// Integer
    Integer(i64),
    /// Float
    Float(f64),
    /// Boolean
    Boolean(bool),
    /// String
    String(String),
    /// Placeholder for invalid values
    Null,
}

impl Display for OCELAttributeValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            OCELAttributeValue::Time(dt) => dt.to_rfc3339(),
            OCELAttributeValue::Integer(i) => i.to_string(),
            OCELAttributeValue::Float(f) => f.to_string(),
            OCELAttributeValue::Boolean(b) => b.to_string(),
            OCELAttributeValue::String(s) => s.clone(),
            OCELAttributeValue::Null => String::default(), //"INVALID_VALUE".to_string(),
        };
        write!(f, "{s}")
    }
}

impl From<i64> for OCELAttributeValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}
impl From<f64> for OCELAttributeValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<usize> for OCELAttributeValue {
    fn from(value: usize) -> Self {
        Self::Integer(value as i64)
    }
}
impl From<bool> for OCELAttributeValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}
impl From<String> for OCELAttributeValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&String> for OCELAttributeValue {
    fn from(value: &String) -> Self {
        Self::String(value.clone())
    }
}

impl From<&str> for OCELAttributeValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}
impl From<DateTime<FixedOffset>> for OCELAttributeValue {
    fn from(value: DateTime<FixedOffset>) -> Self {
        Self::Time(value)
    }
}

impl<T: Into<OCELAttributeValue>> From<Option<T>> for OCELAttributeValue {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Self::Null,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
/// _Types_ of attribute values in OCEL2
pub enum OCELAttributeType {
    /// String
    String,
    /// `DateTime`
    Time,
    /// Integer
    Integer,
    /// Float
    Float,
    /// Boolean
    Boolean,
    /// Placeholder for invalid types
    Null,
}

impl OCELAttributeType {
    ///
    /// Returns the OCEL 2.0 string names of the data types as used in the XML format.
    ///
    /// For instance "string", "time" or "float"
    ///
    /// See [`OCELAttributeType::from_type_str`] for the reverse functionality.
    ///
    pub fn to_type_string(&self) -> String {
        match self {
            OCELAttributeType::String => "string",
            OCELAttributeType::Float => "float",
            OCELAttributeType::Boolean => "boolean",
            OCELAttributeType::Integer => "integer",
            OCELAttributeType::Time => "time",
            //  Null is not a real attribute type
            OCELAttributeType::Null => "string",
        }
        .to_string()
    }

    ///
    /// Returns the [`OCELAttributeType`] corresponding to the given attribute type string.
    ///
    /// For instance "string" yields [`OCELAttributeType::String`]
    ///
    /// See [`OCELAttributeType::to_type_string`] for the reverse functionality.
    ///  
    pub fn from_type_str(s: &str) -> Self {
        match s {
            "string" => OCELAttributeType::String,
            "float" => OCELAttributeType::Float,
            "boolean" => OCELAttributeType::Boolean,
            "integer" => OCELAttributeType::Integer,
            "time" => OCELAttributeType::Time,
            _ => OCELAttributeType::Null,
        }
    }
}

impl From<&OCELAttributeValue> for OCELAttributeType {
    fn from(value: &OCELAttributeValue) -> Self {
        match value {
            OCELAttributeValue::Time(_) => OCELAttributeType::Time,
            OCELAttributeValue::Integer(_) => OCELAttributeType::Integer,
            OCELAttributeValue::Float(_) => OCELAttributeType::Float,
            OCELAttributeValue::Boolean(_) => OCELAttributeType::Boolean,
            OCELAttributeValue::String(_) => OCELAttributeType::String,
            OCELAttributeValue::Null => OCELAttributeType::Null,
        }
    }
}
