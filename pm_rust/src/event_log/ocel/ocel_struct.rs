use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct OCEL {
    #[serde(rename = "eventTypes")]
    pub event_types: Vec<OCELType>,
    #[serde(rename = "objectTypes")]
    pub object_types: Vec<OCELType>,
    pub events: Vec<OCELEvent>,
    pub objects: Vec<OCELObject>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OCELType {
    pub name: String,
    pub attributes: Vec<OCELTypeAttribute>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OCELTypeAttribute {
    pub name: String,
    #[serde(rename = "type")]
    pub value_type: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OCELEventAttribute {
    pub name: String,
    pub value: OCELAttributeValue,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OCELEvent {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub time: DateTime<Utc>,
    pub attributes: Vec<OCELEventAttribute>,
    pub relationships: Option<Vec<OCELRelationship>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OCELRelationship {
    #[serde(rename = "objectId")]
    object_id: String,
    qualifier: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OCELObject {
    id: String,
    #[serde(rename = "type")]
    object_type: String,
    attributes: Vec<OCELObjectAttribute>,
    relationships: Option<Vec<OCELRelationship>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OCELObjectAttribute {
    name: String,
    value: OCELAttributeValue,
    time: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum OCELAttributeValue {
    String(String),
    Time(DateTime<Utc>),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}
