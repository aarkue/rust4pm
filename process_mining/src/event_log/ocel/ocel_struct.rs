use std::fmt::Display;

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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
/// OCEL Relationship (qualified; referring back to an [`OCELObject`])
pub struct OCELRelationship {
    /// ID of referenced [`OCELObject`]
    #[serde(rename = "objectId")]
    pub object_id: String,
    /// Qualifier of relationship
    pub qualifier: String,
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
    pub time: DateTime<FixedOffset>,
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
