use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

///
/// Object-centric Event Log
///
/// Consists of multiple [`OCELEvent`]s and [`OCELObject`]s with corresponding event and object [`OCELType`]s
///
#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
/// OCEL Event/Object Type
pub struct OCELType {
    /// Name
    pub name: String,
    /// Attributes (defining the _type_ of values)
    #[serde(default)]
    pub attributes: Vec<OCELTypeAttribute>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
/// OCEL Attribute types
pub struct OCELTypeAttribute {
    /// Name of attribute
    pub name: String,
    /// Type of attribute
    #[serde(rename = "type")]
    pub value_type: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
/// OCEL Event Attributes
pub struct OCELEventAttribute {
    /// Name of event attribute
    pub name: String,
    /// Value of attribute
    pub value: OCELAttributeValue,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
/// OCEL Event
pub struct OCELEvent {
    /// Event ID
    pub id: String,
    /// Event Type (referring back to the `name` of an [`OCELType`])
    #[serde(rename = "type")]
    pub event_type: String,
    /// DateTime when event occured
    pub time: DateTime<Utc>,
    /// Event attributes
    #[serde(default)]
    pub attributes: Vec<OCELEventAttribute>,
    /// E2O (Event-to-Object) relationships
    pub relationships: Option<Vec<OCELRelationship>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
/// OCEL Relationship (qualified; referring back to an [`OCELObject`])
pub struct OCELRelationship {
    /// ID of referenced [`OCELObject`]
    #[serde(rename = "objectId")]
    pub object_id: String,
    /// Qualifier of relationship
    pub qualifier: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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
    pub relationships: Option<Vec<OCELRelationship>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
/// OCEL Object Attribute
///
/// Describing a named value _at a certain point in time_
pub struct OCELObjectAttribute {
    /// Name of attribute
    pub name: String,
    /// Value of attribute
    pub value: OCELAttributeValue,
    /// Time of attribute value
    pub time: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
/// OCEL Attribute Values
pub enum OCELAttributeValue {
    /// String
    String(String),
    /// DateTime
    Time(DateTime<Utc>),
    /// Integer
    Integer(i64),
    /// Float
    Float(f64),
    /// Boolean
    Boolean(bool),
    /// Placeholder for invalid values
    Null,
}
