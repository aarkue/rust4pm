use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

///
/// Object-centric Event Log
///
/// Consists of multiple [OCELEvent]s and [OCELObject]s with corresponding event and object [OCELType]s
///
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OCEL {
    #[serde(rename = "eventTypes")]
    pub event_types: Vec<OCELType>,
    #[serde(rename = "objectTypes")]
    pub object_types: Vec<OCELType>,
    pub events: Vec<OCELEvent>,
    pub objects: Vec<OCELObject>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OCELType {
    pub name: String,
    pub attributes: Vec<OCELTypeAttribute>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OCELTypeAttribute {
    pub name: String,
    #[serde(rename = "type")]
    pub value_type: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OCELEventAttribute {
    pub name: String,
    pub value: OCELAttributeValue,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OCELEvent {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub time: DateTime<Utc>,
    pub attributes: Vec<OCELEventAttribute>,
    pub relationships: Option<Vec<OCELRelationship>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OCELRelationship {
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub qualifier: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OCELObject {
    pub id: String,
    #[serde(rename = "type")]
    pub object_type: String,
    pub attributes: Vec<OCELObjectAttribute>,
    pub relationships: Option<Vec<OCELRelationship>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OCELObjectAttribute {
    pub name: String,
    pub value: OCELAttributeValue,
    pub time: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum OCELAttributeValue {
    String(String),
    Time(DateTime<Utc>),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}
