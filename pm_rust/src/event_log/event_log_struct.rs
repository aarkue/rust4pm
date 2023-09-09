use std::collections::HashMap;

use chrono::{DateTime, Utc, serde::ts_milliseconds};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::constants::{ACTIVITY_NAME, TRACE_ID_NAME};



#[derive(Debug, Deserialize, Serialize, Clone)]
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Attribute {
    pub key: String,
    pub value: AttributeValue,
}
impl Attribute {
    pub fn new(key: String, attribute_val: AttributeValue) -> Self {
        Self {
            key,
            value: attribute_val,
        }
    }
    pub fn new_with_key(key: String, attribute_val: AttributeValue) -> (String, Self) {
        (
            key.clone(),
            Self {
                key,
                value: attribute_val,
            },
        )
    }
}
pub type Attributes = HashMap<String, Attribute>;
pub trait AttributeAddable {
    fn add_to_attributes(self: &mut Self, key: String, value: AttributeValue);
}

impl AttributeAddable for Attributes {
    fn add_to_attributes(self: &mut Self, key: String, value: AttributeValue) {
        let (k, v) = Attribute::new_with_key(key, value);
        self.insert(k, v);
    }
}
pub fn add_to_attributes(attributes: &mut Attributes, key: String, value: AttributeValue) {
    let (k, v) = Attribute::new_with_key(key, value);
    attributes.insert(k, v);
}

pub fn to_attributes(from: HashMap<String, AttributeValue>) -> Attributes {
    from.into_iter()
        .map(|(key, value)| {
            (
                key.clone(),
                Attribute {
                    key: key.clone(),
                    value,
                },
            )
        })
        .collect()
}

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
pub struct EventLog {
    pub attributes: Attributes,
    pub traces: Vec<Trace>,
}