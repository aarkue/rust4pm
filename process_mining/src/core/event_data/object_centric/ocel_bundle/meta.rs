//! `ocel-meta.json`: what a bundled container declares about itself.

use std::collections::BTreeMap;

use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};

use crate::core::event_data::object_centric::{OCELAttributeType, OCELTypeAttribute};

/// The manifest's name inside a container. Every other filename is declared by this file,
/// never inferred from a path.
pub const META_FILE_NAME: &str = "ocel-meta.json";

/// The bundled-format revision this build reads and writes.
pub const BUNDLE_FORMAT_VERSION: &str = "1.0";

/// The OCEL revision the bundled format carries.
pub const OCEL_VERSION: &str = "2.0";

/// Fixed column names, identical in both storage formats.
pub mod columns {
    /// An event's or object's own id.
    pub const ID: &str = "ocel_id";
    /// When an event happened, or when an object attribute took its value.
    pub const TIME: &str = "ocel_time";
    /// Which attribute an object-change row changes.
    pub const CHANGED_FIELD: &str = "ocel_changed_field";
    /// `e2o`: the event.
    pub const EVENT_ID: &str = "ocel_event_id";
    /// `e2o`: the object.
    pub const OBJECT_ID: &str = "ocel_object_id";
    /// `o2o`: the referring object.
    pub const SOURCE_ID: &str = "ocel_source_id";
    /// `o2o`: the referred-to object.
    pub const TARGET_ID: &str = "ocel_target_id";
    /// A relation's qualifier.
    pub const QUALIFIER: &str = "ocel_qualifier";
}

/// Which physical storage a container uses. One per container: a `csv` container holds no
/// Parquet files and vice versa.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageFormat {
    /// Every cell is text, read according to the declared attribute type.
    #[default]
    Csv,
    /// Attribute types are carried by the file's own schema.
    Parquet,
}

impl StorageFormat {
    /// The filename extension for this storage.
    #[must_use]
    pub fn extension(self) -> &'static str {
        match self {
            StorageFormat::Csv => "csv",
            StorageFormat::Parquet => "parquet",
        }
    }
}

/// One of OCEL 2.0's primitive attribute types, as `ocel-meta.json` spells it.
///
/// A separate enum rather than [`OCELAttributeType`] directly: that type carries a `Null`
/// variant which is not an OCEL type, and its `from_type_str` maps anything unrecognised
/// onto it, so a typo in a manifest would be read as a valid declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AttributeType {
    /// Text.
    String,
    /// A timezone-aware instant.
    Time,
    /// A 64-bit integer.
    Integer,
    /// A double.
    Float,
    /// `true` or `false`.
    Boolean,
}

impl From<AttributeType> for OCELAttributeType {
    fn from(t: AttributeType) -> Self {
        match t {
            AttributeType::String => OCELAttributeType::String,
            AttributeType::Time => OCELAttributeType::Time,
            AttributeType::Integer => OCELAttributeType::Integer,
            AttributeType::Float => OCELAttributeType::Float,
            AttributeType::Boolean => OCELAttributeType::Boolean,
        }
    }
}

/// One cell's value, as one of the format's primitive types.
#[derive(Debug, Clone)]
pub enum Value {
    /// Text.
    Text(String),
    /// A 64-bit integer.
    Integer(i64),
    /// A double.
    Float(f64),
    /// `true` or `false`.
    Boolean(bool),
    /// A timezone-aware instant.
    Time(DateTime<FixedOffset>),
}

impl std::fmt::Display for Value {
    /// The cell's text, as CSV storage holds it.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Text(s) => f.write_str(s),
            Value::Integer(i) => write!(f, "{i}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::Boolean(b) => write!(f, "{b}"),
            Value::Time(t) => write!(f, "{}", t.to_rfc3339()),
        }
    }
}

/// The instant an object table's values are in force from.
#[must_use]
pub fn epoch() -> DateTime<FixedOffset> {
    DateTime::from_timestamp_nanos(0).into()
}

/// One declared attribute: the column that holds it, and how to read it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttributeDecl {
    /// Attribute name, which is also the column name.
    pub name: String,
    /// Primitive type.
    #[serde(rename = "type")]
    pub value_type: AttributeType,
}

/// An event type's table and attributes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventTypeDecl {
    /// Path inside the container.
    pub file: String,
    /// Attribute columns beyond `ocel_id` and `ocel_time`.
    #[serde(default)]
    pub attributes: Vec<AttributeDecl>,
}

/// A declared attribute list as OCEL type attributes.
#[must_use]
pub fn type_attributes(attrs: &[AttributeDecl]) -> Vec<OCELTypeAttribute> {
    attrs
        .iter()
        .map(|a| OCELTypeAttribute::new(&a.name, &OCELAttributeType::from(a.value_type)))
        .collect()
}

/// An object type's tables and attributes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectTypeDecl {
    /// Path inside the container.
    pub file: String,
    /// Path to this type's object-change table.
    ///
    /// The format requires one per object type even when it has no changes, but this stays
    /// optional so a container that omits an empty one still imports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub changes_file: Option<String>,
    /// Attribute columns beyond `ocel_id`.
    #[serde(default)]
    pub attributes: Vec<AttributeDecl>,
}

/// The two relation tables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationFiles {
    /// Event-to-object relations.
    pub e2o: String,
    /// Object-to-object relations.
    pub o2o: String,
}

/// A container's `ocel-meta.json`.
///
/// Deliberately not `deny_unknown_fields`: the bundled format is young, and a container written
/// against a later revision should still import for everything this build understands rather
/// than be rejected wholesale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BundleMeta {
    /// OCEL version, `"2.0"`.
    pub ocel_version: String,
    /// Bundled-format revision.
    pub bundle_format_version: String,
    /// Which physical storage the tables use.
    pub storage_format: StorageFormat,
    /// Event types, by exact type name. `BTreeMap` so a generated blueprint and an exported
    /// manifest are byte-identical across runs.
    #[serde(default)]
    pub event_types: BTreeMap<String, EventTypeDecl>,
    /// Object types, by exact type name.
    #[serde(default)]
    pub object_types: BTreeMap<String, ObjectTypeDecl>,
    /// The relation tables.
    pub relations: RelationFiles,
}

impl BundleMeta {
    /// Every declared file, paired with the logical table name that reads it.
    ///
    /// This is the single place where the two halves of an import agree: the container opens
    /// exactly these paths under exactly these names, and the generated blueprint's `Source`
    /// nodes name the same ones.
    pub fn tables(&self) -> Vec<(String, &str)> {
        let mut out = Vec::new();
        for (ty, d) in &self.event_types {
            out.push((event_table(ty), d.file.as_str()));
        }
        for (ty, d) in &self.object_types {
            out.push((object_table(ty), d.file.as_str()));
            if let Some(f) = &d.changes_file {
                out.push((object_changes_table(ty), f.as_str()));
            }
        }
        out.push((E2O_TABLE.to_string(), self.relations.e2o.as_str()));
        out.push((O2O_TABLE.to_string(), self.relations.o2o.as_str()));
        out
    }
}

/// The `e2o` table's logical name.
pub const E2O_TABLE: &str = "e2o";
/// The `o2o` table's logical name.
pub const O2O_TABLE: &str = "o2o";

// A colon after `event`/`object` and an underscore in `object_changes` means the three prefixes
// differ at a fixed position, so no type name can make two of these collide, nor equal the bare
// `e2o`/`o2o`.
/// Logical table name for an event type's table.
#[must_use]
pub fn event_table(event_type: &str) -> String {
    format!("event:{event_type}")
}
/// Logical table name for an object type's table.
#[must_use]
pub fn object_table(object_type: &str) -> String {
    format!("object:{object_type}")
}
/// Logical table name for an object type's change table.
#[must_use]
pub fn object_changes_table(object_type: &str) -> String {
    format!("object_changes:{object_type}")
}

/// Percent-encode a type name for use in a filename, per the format's naming rule:
/// `A-Z`, `a-z`, `0-9`, `.`, `_` and `-` stay, every other byte becomes `%HH` with uppercase hex.
///
/// Import never needs this, since `ocel-meta.json` is authoritative and its declared paths are
/// used verbatim, but an exporter does, and keeping both here keeps the two consistent.
#[must_use]
pub fn encode_type_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for b in name.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-') {
            out.push(*b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_names_are_percent_encoded_as_the_format_specifies() {
        assert_eq!(encode_type_name("place order"), "place%20order");
        assert_eq!(encode_type_name("pay/order"), "pay%2Forder");
        assert_eq!(encode_type_name("orders"), "orders");
        assert_eq!(encode_type_name("sales person"), "sales%20person");
        // Multi-byte UTF-8 is encoded byte by byte.
        assert_eq!(encode_type_name("café"), "caf%C3%A9");
    }

    #[test]
    fn no_type_name_can_make_two_logical_table_names_collide() {
        let names = [
            event_table("changes:x"),
            object_table("changes:x"),
            object_changes_table("x"),
            object_table("e2o"),
            E2O_TABLE.to_string(),
        ];
        let unique: std::collections::BTreeSet<_> = names.iter().collect();
        assert_eq!(unique.len(), names.len(), "{names:?}");
    }

    #[test]
    fn the_specs_example_manifest_parses() {
        let meta: BundleMeta = serde_json::from_str(
            r#"{
              "ocelVersion": "2.0",
              "bundleFormatVersion": "1.0",
              "storageFormat": "csv",
              "eventTypes": {
                "place order": { "file": "events/event_place%20order.csv",
                                 "attributes": [{ "name": "resource", "type": "string" }] },
                "pay order": { "file": "events/event_pay%20order.csv", "attributes": [] }
              },
              "objectTypes": {
                "orders": { "file": "objects/object_orders.csv",
                            "changesFile": "object_changes/object_changes_orders.csv",
                            "attributes": [{ "name": "price", "type": "float" }] },
                "sales person": { "file": "objects/object_sales%20person.csv",
                                  "changesFile": "object_changes/object_changes_sales%20person.csv",
                                  "attributes": [] }
              },
              "relations": { "e2o": "relations/e2o.csv", "o2o": "relations/o2o.csv" }
            }"#,
        )
        .expect("parse");

        assert_eq!(meta.storage_format, StorageFormat::Csv);
        assert_eq!(
            meta.event_types["place order"].attributes[0].name,
            "resource"
        );
        assert_eq!(
            meta.object_types["orders"].attributes[0].value_type,
            AttributeType::Float
        );
        // 2 event tables + 2 object tables + 2 change tables + e2o + o2o.
        assert_eq!(meta.tables().len(), 8);
    }

    #[test]
    fn an_unknown_attribute_type_is_rejected_rather_than_read_as_null() {
        let err = serde_json::from_str::<AttributeDecl>(r#"{"name":"x","type":"strng"}"#)
            .expect_err("typo rejected");
        assert!(err.to_string().contains("strng"), "{err}");
    }
}
