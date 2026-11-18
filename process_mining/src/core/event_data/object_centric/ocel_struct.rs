use std::collections::{HashMap, HashSet};
use std::fmt::Display;

use chrono::{DateTime, FixedOffset};
use macros_process_mining::RegistryEntity;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::event_data::{
    case_centric::AttributeValue,
    object_centric::linked_ocel::{index_linked_ocel::ObjectIndex, IndexLinkedOCEL},
    timestamp_utils::parse_timestamp,
};

///
/// Object-centric Event Log
///
/// Consists of multiple [`OCELEvent`]s and [`OCELObject`]s with corresponding event and object [`OCELType`]s
///
/// Deserializing reads a string attribute value as a timestamp (if it is one) only when its event
/// or object type does not declare the attribute with another type.
#[derive(Serialize, Debug, Clone, Default, PartialEq, JsonSchema, RegistryEntity)]
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
            .values()
            .flat_map(|o2e_set| o2e_set.keys().cloned())
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

impl<'de> Deserialize<'de> for OCEL {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct JsonOCEL {
            #[serde(rename = "eventTypes")]
            event_types: Vec<OCELType>,
            #[serde(rename = "objectTypes")]
            object_types: Vec<OCELType>,
            #[serde(default)]
            events: Vec<JsonOCELEvent>,
            #[serde(default)]
            objects: Vec<JsonOCELObject>,
        }
        let json = JsonOCEL::deserialize(deserializer)?;
        let event_attribute_types = DeclaredAttributeTypes::from_types(&json.event_types);
        let object_attribute_types = DeclaredAttributeTypes::from_types(&json.object_types);
        Ok(OCEL {
            events: json
                .events
                .into_iter()
                .map(|e| e.resolve(&event_attribute_types))
                .collect(),
            objects: json
                .objects
                .into_iter()
                .map(|o| o.resolve(&object_attribute_types))
                .collect(),
            event_types: json.event_types,
            object_types: json.object_types,
        })
    }
}

/// Attribute types declared per event or object type, to read attribute values with
#[derive(Debug, Default)]
pub(crate) struct DeclaredAttributeTypes(HashMap<String, HashMap<String, OCELAttributeType>>);

impl DeclaredAttributeTypes {
    fn from_types(types: &[OCELType]) -> Self {
        let mut ret = Self::default();
        types.iter().for_each(|t| ret.declare(t));
        ret
    }

    /// Declare (or re-declare) the attributes of `t`
    pub(crate) fn declare(&mut self, t: &OCELType) {
        self.0.insert(
            t.name.clone(),
            t.attributes
                .iter()
                .map(|a| {
                    (
                        a.name.clone(),
                        OCELAttributeType::from_type_str(&a.value_type),
                    )
                })
                .collect(),
        );
    }

    /// Declare (or re-declare) one attribute of type `type_name`
    pub(crate) fn declare_attribute(
        &mut self,
        type_name: &str,
        attribute: &str,
        attribute_type: OCELAttributeType,
    ) {
        if let Some(attributes) = self.0.get_mut(type_name) {
            attributes.insert(attribute.to_string(), attribute_type);
        } else {
            self.0.insert(
                type_name.to_string(),
                HashMap::from([(attribute.to_string(), attribute_type)]),
            );
        }
    }

    pub(crate) fn get(&self, type_name: &str, attribute: &str) -> Option<OCELAttributeType> {
        self.0.get(type_name)?.get(attribute).copied()
    }
}

/// An [`OCELAttributeValue`] as read from OCEL JSON, where a timestamp is a string
///
/// Read separately because untagged [`OCELAttributeValue`] would turn every string that parses as
/// a timestamp into [`OCELAttributeValue::Time`], even for an attribute declared as a string.
#[derive(Deserialize)]
#[serde(untagged)]
enum JsonAttributeValue {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    String(String),
    Null,
}

impl JsonAttributeValue {
    /// A string is read as the declared type if it parses as one (the OCEL 2.0 JSON schema types
    /// every value as a string), and a whole number declared `float` (e.g., `5` written by
    /// JavaScript for `5.0`) as a float.
    fn resolve(self, declared: impl FnOnce() -> Option<OCELAttributeType>) -> OCELAttributeValue {
        match self {
            Self::Integer(i) => match declared() {
                Some(OCELAttributeType::Float) => OCELAttributeValue::Float(i as f64),
                _ => OCELAttributeValue::Integer(i),
            },
            Self::Float(f) => OCELAttributeValue::Float(f),
            Self::Boolean(b) => OCELAttributeValue::Boolean(b),
            Self::Null => OCELAttributeValue::Null,
            Self::String(s) => match declared() {
                Some(OCELAttributeType::Null) | None => {
                    // Same parsing as the `Deserialize` of `DateTime` in untagged `OCELAttributeValue`
                    match s.parse::<DateTime<FixedOffset>>() {
                        Ok(dt) => OCELAttributeValue::Time(dt),
                        Err(_) => OCELAttributeValue::String(s),
                    }
                }
                Some(OCELAttributeType::String) => OCELAttributeValue::String(s),
                Some(t) => {
                    let s = OCELAttributeValue::String(s);
                    s.try_coerce_to(t).unwrap_or(s)
                }
            },
        }
    }
}

#[derive(Deserialize)]
struct JsonOCELEventAttribute {
    name: String,
    value: JsonAttributeValue,
}

/// An [`OCELEvent`] as read from OCEL JSON, before attribute values are resolved against the
/// declared types (see [`JsonAttributeValue`])
#[derive(Deserialize)]
pub(crate) struct JsonOCELEvent {
    id: String,
    #[serde(rename = "type")]
    event_type: String,
    #[serde(deserialize_with = "robust_timestamp_parsing")]
    time: DateTime<FixedOffset>,
    #[serde(default)]
    attributes: Vec<JsonOCELEventAttribute>,
    #[serde(default)]
    relationships: Vec<OCELRelationship>,
}

impl JsonOCELEvent {
    pub(crate) fn resolve(self, types: &DeclaredAttributeTypes) -> OCELEvent {
        let JsonOCELEvent {
            id,
            event_type,
            time,
            attributes,
            relationships,
        } = self;
        let attributes = attributes
            .into_iter()
            .map(|a| OCELEventAttribute {
                value: a.value.resolve(|| types.get(&event_type, &a.name)),
                name: a.name,
            })
            .collect();
        OCELEvent {
            id,
            event_type,
            time,
            attributes,
            relationships,
        }
    }
}

#[derive(Deserialize)]
struct JsonOCELObjectAttribute {
    name: String,
    value: JsonAttributeValue,
    #[serde(deserialize_with = "robust_timestamp_parsing")]
    time: DateTime<FixedOffset>,
}

/// An [`OCELObject`] as read from OCEL JSON, before attribute values are resolved against the
/// declared types (see [`JsonAttributeValue`])
#[derive(Deserialize)]
pub(crate) struct JsonOCELObject {
    id: String,
    #[serde(rename = "type")]
    object_type: String,
    #[serde(default)]
    attributes: Vec<JsonOCELObjectAttribute>,
    #[serde(default)]
    relationships: Vec<OCELRelationship>,
}

impl JsonOCELObject {
    pub(crate) fn resolve(self, types: &DeclaredAttributeTypes) -> OCELObject {
        let JsonOCELObject {
            id,
            object_type,
            attributes,
            relationships,
        } = self;
        let attributes = attributes
            .into_iter()
            .map(|a| OCELObjectAttribute {
                value: a.value.resolve(|| types.get(&object_type, &a.name)),
                name: a.name,
                time: a.time,
            })
            .collect();
        OCELObject {
            id,
            object_type,
            attributes,
            relationships,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
/// OCEL Event/Object Type
pub struct OCELType {
    /// Name
    pub name: String,
    /// Attributes (defining the _type_ of values)
    #[serde(default)]
    pub attributes: Vec<OCELTypeAttribute>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, JsonSchema)]
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
/// OCEL Event Attributes
pub struct OCELEventAttribute {
    /// Name of event attribute
    pub name: String,
    /// Value of attribute
    pub value: OCELAttributeValue,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
/// OCEL Event
pub struct OCELEvent {
    /// Event ID
    pub id: String,
    /// Event Type (referring back to the `name` of an [`OCELType`])
    #[serde(rename = "type")]
    pub event_type: String,
    /// `DateTime` when event occured
    #[serde(deserialize_with = "robust_timestamp_parsing")]
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, JsonSchema)]
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
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

/// Deserialize a timestamp field, accepting every format understood by
/// [`parse_timestamp`] instead of only RFC3339.
fn robust_timestamp_parsing<'de, D>(deserializer: D) -> Result<DateTime<FixedOffset>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let time: String = Deserialize::deserialize(deserializer)?;
    parse_timestamp(&time, None).map_err(serde::de::Error::custom)
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, Default)]
#[serde(untagged)]
/// OCEL Attribute Values
pub enum OCELAttributeValue {
    /// Integer
    Integer(i64),
    /// Float
    Float(f64),
    /// Boolean
    Boolean(bool),
    /// `DateTime`
    Time(DateTime<FixedOffset>),
    /// String
    String(String),
    /// Placeholder for invalid values
    #[default]
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

impl OCELAttributeValue {
    /// Get attribute type of an attribute value
    pub fn get_type(&self) -> OCELAttributeType {
        match self {
            OCELAttributeValue::Time(_) => OCELAttributeType::Time,
            OCELAttributeValue::Integer(_) => OCELAttributeType::Integer,
            OCELAttributeValue::Float(_) => OCELAttributeType::Float,
            OCELAttributeValue::Boolean(_) => OCELAttributeType::Boolean,
            OCELAttributeValue::String(_) => OCELAttributeType::String,
            OCELAttributeValue::Null => OCELAttributeType::Null,
        }
    }

    /// Try to coerce this value so its variant matches `target`. Returns `Some(coerced)`
    /// when a lossless or parse-based conversion is defined, `None` otherwise.
    ///
    /// Defined conversions:
    ///   anything -> String    (via `Display` / RFC3339)
    ///   Integer  -> Float     (lossless within `i64` -> `f64`)
    ///   Float    -> Integer   (only if `fract() == 0.0` and within `i64` range)
    ///   String   -> Integer   (parse via `i64::from_str`)
    ///   String   -> Float     (parse via `f64::from_str`)
    ///   String   -> Boolean   (case-insensitive `"true"`/`"false"`)
    ///   String   -> Time      (parse via [`parse_timestamp`], i.e. RFC3339 and fallbacks)
    pub fn try_coerce_to(&self, target: OCELAttributeType) -> Option<OCELAttributeValue> {
        use OCELAttributeType as T;
        use OCELAttributeValue::*;
        match (target, self) {
            (T::String, v @ (Integer(_) | Float(_) | Boolean(_) | Time(_))) => {
                Some(String(v.to_string()))
            }
            (T::Float, Integer(i)) => Some(Float(*i as f64)),
            // `as` saturates, and `i64::MAX as f64` rounds up to 2^63, so a round-trip check
            // alone would accept 2^63 as `i64::MAX`
            (T::Integer, Float(f))
                if f.fract() == 0.0 && (-(2f64.powi(63))..2f64.powi(63)).contains(f) =>
            {
                Some(Integer(*f as i64))
            }
            (T::Integer, String(s)) => s.parse::<i64>().ok().map(Integer),
            (T::Float, String(s)) => s.parse::<f64>().ok().map(Float),
            (T::Boolean, String(s)) => match s.to_ascii_lowercase().as_str() {
                "true" => Some(Boolean(true)),
                "false" => Some(Boolean(false)),
                _ => None,
            },
            (T::Time, String(s)) => parse_timestamp(s, None).ok().map(Time),
            _ => None,
        }
    }

    /// Convert this value to `target`, as the SQL exporters store it in a column of that type.
    /// `Null` stays `Null`, and an unknown `target` (`Null`) is treated as `String`. Returns
    /// `None` if the value has no reading as `target`.
    ///
    /// Extends [`Self::try_coerce_to`] with lossy conversions, and trims text before parsing:
    ///   Float          -> Integer  (rounded half away from zero, if within `i64` range)
    ///   Integer, Float -> Boolean  (nonzero is `true`)
    ///   String         -> Boolean  (also `"1"`/`"0"`)
    ///   String         -> Integer  (also a whole float such as `"3.0"`)
    pub fn normalize_to(&self, target: OCELAttributeType) -> Option<OCELAttributeValue> {
        use OCELAttributeType as T;
        use OCELAttributeValue::*;
        let target = if target == T::Null { T::String } else { target };
        if matches!(self, Null) || self.get_type() == target {
            return Some(self.clone());
        }
        // Same range check as in `try_coerce_to`.
        let to_integer = |f: f64| {
            (-(2f64.powi(63))..2f64.powi(63))
                .contains(&f)
                .then_some(Integer(f as i64))
        };
        match (target, self) {
            (T::Integer, Float(f)) => to_integer(f.round()),
            (T::Boolean, Integer(i)) => Some(Boolean(*i != 0)),
            (T::Boolean, Float(f)) if !f.is_nan() => Some(Boolean(*f != 0.0)),
            (T::Integer, Boolean(b)) => Some(Integer(i64::from(*b))),
            (T::Float, Boolean(b)) => Some(Float(f64::from(u8::from(*b)))),
            (T::Integer, String(s)) => {
                let s = s.trim();
                s.parse::<i64>().ok().map(Integer).or_else(|| {
                    s.parse::<f64>()
                        .ok()
                        .filter(|f| f.fract() == 0.0)
                        .and_then(to_integer)
                })
            }
            (T::Float, String(s)) => s.trim().parse::<f64>().ok().map(Float),
            (T::Boolean, String(s)) => match s.trim().to_ascii_lowercase().as_str() {
                "true" | "1" => Some(Boolean(true)),
                "false" | "0" => Some(Boolean(false)),
                _ => None,
            },
            (T::Time, String(s)) => parse_timestamp(s, None).ok().map(Time),
            _ => self.try_coerce_to(target),
        }
    }
}

impl From<AttributeValue> for OCELAttributeValue {
    /// Convert an [`AttributeValue`] to an [`OCELAttributeValue`]
    ///
    /// The conversion is not entirely lossless, in particular values that cannot be represented
    /// as OCEL attributes are converted to String (either directly, or as debug string).
    fn from(value: AttributeValue) -> Self {
        match value {
            AttributeValue::String(s) => Self::String(s),
            AttributeValue::Date(date_time) => Self::Time(date_time),
            AttributeValue::Int(i) => Self::Integer(i),
            AttributeValue::Float(f) => Self::Float(f),
            AttributeValue::Boolean(b) => Self::Boolean(b),
            AttributeValue::ID(uuid) => Self::String(uuid.to_string()),
            AttributeValue::List(attributes) => Self::String(format!("{:?}", attributes)),
            AttributeValue::Container(attributes) => Self::String(format!("{:?}", attributes)),
            AttributeValue::None() => Self::Null,
        }
    }
}

impl From<OCELAttributeValue> for AttributeValue {
    /// Convert an [`OCELAttributeValue`] to an [`AttributeValue`]
    fn from(value: OCELAttributeValue) -> AttributeValue {
        match value {
            OCELAttributeValue::String(s) => AttributeValue::String(s),
            OCELAttributeValue::Integer(i) => AttributeValue::Int(i),
            OCELAttributeValue::Float(f) => AttributeValue::Float(f),
            OCELAttributeValue::Boolean(b) => AttributeValue::Boolean(b),
            OCELAttributeValue::Time(date_time) => AttributeValue::Date(date_time),
            OCELAttributeValue::Null => AttributeValue::None(),
        }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
        self.as_type_str().to_string()
    }

    /// Same as [`Self::to_type_string`] but returns a `&'static str`, suitable for
    /// hot-path comparisons that would otherwise allocate a `String` per call.
    pub fn as_type_str(&self) -> &'static str {
        match self {
            OCELAttributeType::String => "string",
            OCELAttributeType::Float => "float",
            OCELAttributeType::Boolean => "boolean",
            OCELAttributeType::Integer => "integer",
            OCELAttributeType::Time => "time",
            // Null is not a real OCEL attribute type; map to "string" for compatibility.
            OCELAttributeType::Null => "string",
        }
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
            // pm4py writes `date` for timestamp attributes
            "time" | "date" => OCELAttributeType::Time,
            _ => OCELAttributeType::Null,
        }
    }

    /// The narrowest type that can hold values of both `self` and `other`: identical types
    /// stay put, `Null` yields the other type, `Integer` + `Float` widens to `Float`, and
    /// anything else falls back to `String`.
    ///
    /// Used where one attribute name carries different types (e.g. across event types, or
    /// across rows of an untyped source) and a single column type must cover all of them.
    pub fn coalesce(self, other: Self) -> Self {
        use OCELAttributeType::*;
        if self == other {
            return self;
        }
        match (self, other) {
            (Null, o) | (o, Null) => o,
            (Integer, Float) | (Float, Integer) => Float,
            _ => String,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_to_integer_coercion_rejects_out_of_range() {
        let two_pow_63 = 2f64.powi(63);
        assert_eq!(
            OCELAttributeValue::Float(two_pow_63).try_coerce_to(OCELAttributeType::Integer),
            None
        );
        assert_eq!(
            OCELAttributeValue::Float(-two_pow_63).try_coerce_to(OCELAttributeType::Integer),
            Some(OCELAttributeValue::Integer(i64::MIN))
        );
        assert_eq!(
            OCELAttributeValue::Float(3.0).try_coerce_to(OCELAttributeType::Integer),
            Some(OCELAttributeValue::Integer(3))
        );
        for f in [0.5, f64::NAN, f64::INFINITY] {
            assert_eq!(
                OCELAttributeValue::Float(f).try_coerce_to(OCELAttributeType::Integer),
                None
            );
        }
    }

    #[test]
    fn normalize_to_converts_to_the_declared_type() {
        use OCELAttributeType as T;
        use OCELAttributeValue as V;
        let s = |s: &str| V::String(s.to_string());
        for (v, target, want) in [
            (V::Integer(2), T::Float, Some(V::Float(2.0))),
            (V::Float(2.5), T::Integer, Some(V::Integer(3))),
            (V::Float(-2.5), T::Integer, Some(V::Integer(-3))),
            (V::Float(f64::NAN), T::Integer, None),
            (V::Float(1e19), T::Integer, None),
            (V::Integer(0), T::Boolean, Some(V::Boolean(false))),
            (V::Float(0.1), T::Boolean, Some(V::Boolean(true))),
            (s(" TRUE "), T::Boolean, Some(V::Boolean(true))),
            (s("0"), T::Boolean, Some(V::Boolean(false))),
            (s("yes"), T::Boolean, None),
            (s(" 3.0"), T::Integer, Some(V::Integer(3))),
            (s("3.5"), T::Integer, None),
            (s("n/a"), T::Integer, None),
            (s(" 2.5 "), T::Float, Some(V::Float(2.5))),
            (V::Boolean(true), T::String, Some(s("true"))),
            (V::Boolean(true), T::Integer, Some(V::Integer(1))),
            (V::Boolean(false), T::Float, Some(V::Float(0.0))),
            (V::Integer(1), T::Null, Some(s("1"))),
            (V::Null, T::Integer, Some(V::Null)),
            (V::Boolean(true), T::Time, None),
        ] {
            assert_eq!(v.normalize_to(target), want, "{v:?} as {target:?}");
        }
        assert!(matches!(
            s("2024-01-01T10:00:00+00:00").normalize_to(T::Time),
            Some(V::Time(_))
        ));
    }
}
