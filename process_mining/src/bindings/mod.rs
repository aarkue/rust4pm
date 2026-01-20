#![cfg(feature = "bindings")]

//! # Bindings Module
//!
//! This module provides a framework for exposing Rust functions to dynamic environments
//! such as CLIs, Python bindings, or visual editors.
//!
//! ## Architecture
//!
//! - **Registry**: A global collection of `Binding` structs, collected via `inventory`.
//! - **AppState**: A thread-safe storage for "Big Types" (e.g., EventLogs)
//!   that are passed by reference (ID) rather than serialized.
//! - **Execution**: Functions are invoked via `call()`, which handles argument extraction
//!   and result storage.
//!
//! ## Usage
//!
//! 1. Define a function and annotate it with `#[register_binding]`.
//! 2. Use `list_functions()` to discover available commands.
//! 3. Use `call()` to execute them.
//!
//! ## Type Handling
//!
//! - **Simple Types**: Serialized/Deserialized via `serde_json`.
//! - **Big Types**: Stored in `AppState`. Arguments are string IDs pointing to the state.
//!   Return values are stored in state, and their new ID is returned.
//!
//! ## Helper Features
//!
//! - **Auto-Loading**: The `resolve_argument` function can automatically load "Big Types" from
//!   file paths if the argument schema indicates a registry reference.

use crate::core::{
    event_data::{
        case_centric::utils::activity_projection::EventLogActivityProjection,
        object_centric::{
            linked_ocel::{IndexLinkedOCEL, LinkedOCELAccess, SlimLinkedOCEL},
            ocel_struct::OCEL,
        },
    },
    EventLog,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, fmt::Display};
use std::{str::FromStr, sync::RwLock};

/// Manually maintained Registry enum of 'big' types
///
/// NOTE: When extending this with a new variant, make sure to also update `BIG_TYPES_NAMES` in the macro crate.
#[derive(Debug)]
#[allow(clippy::large_enum_variant, missing_docs)]
pub enum RegistryItem {
    EventLogActivityProjection(EventLogActivityProjection),
    IndexLinkedOCEL(IndexLinkedOCEL),
    SlimLinkedOCEL(SlimLinkedOCEL),
    EventLog(EventLog),
    OCEL(OCEL),
}

impl From<EventLog> for RegistryItem {
    fn from(value: EventLog) -> Self {
        Self::EventLog(value)
    }
}
impl From<EventLogActivityProjection> for RegistryItem {
    fn from(value: EventLogActivityProjection) -> Self {
        Self::EventLogActivityProjection(value)
    }
}
impl From<IndexLinkedOCEL> for RegistryItem {
    fn from(value: IndexLinkedOCEL) -> Self {
        Self::IndexLinkedOCEL(value)
    }
}
impl From<OCEL> for RegistryItem {
    fn from(value: OCEL) -> Self {
        Self::OCEL(value)
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum RegistryItemKind {
    EventLogActivityProjection,
    IndexLinkedOCEL,
    SlimLinkedOCEL,
    EventLog,
    OCEL,
}

impl Display for RegistryItemKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            RegistryItemKind::EventLogActivityProjection => "EventLogActivityProjection",
            RegistryItemKind::IndexLinkedOCEL => "IndexLinkedOCEL",
            RegistryItemKind::SlimLinkedOCEL => "SlimLinkedOCEL",
            RegistryItemKind::EventLog => "EventLog",
            RegistryItemKind::OCEL => "OCEL",
        };
        write!(f, "{}", s)
    }
}

impl RegistryItemKind {
    /// Get all kinds of `RegistryItemKind`
    pub fn all_kinds() -> &'static [Self] {
        &[
            RegistryItemKind::OCEL,
            RegistryItemKind::EventLogActivityProjection,
            RegistryItemKind::EventLog,
            RegistryItemKind::IndexLinkedOCEL,
            RegistryItemKind::SlimLinkedOCEL,
        ]
    }
}

impl std::str::FromStr for RegistryItemKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "EventLogActivityProjection" => Ok(RegistryItemKind::EventLogActivityProjection),
            "IndexLinkedOCEL" => Ok(RegistryItemKind::IndexLinkedOCEL),
            "EventLog" => Ok(RegistryItemKind::EventLog),
            "OCEL" => Ok(RegistryItemKind::OCEL),
            "SlimLinkedOCEL" => Ok(RegistryItemKind::SlimLinkedOCEL),
            _ => Err(format!("Unknown RegistryItemKind: {}", s)),
        }
    }
}

use crate::core::io::{Exportable, Importable};

impl RegistryItem {
    /// Convert the registry item to a JSON value
    ///
    /// For "Big Types", this performs a full serialization.
    pub fn to_value(&self) -> Result<Value, String> {
        match self {
            RegistryItem::EventLog(log) => serde_json::to_value(log).map_err(|e| e.to_string()),
            RegistryItem::OCEL(ocel) => serde_json::to_value(ocel).map_err(|e| e.to_string()),
            RegistryItem::IndexLinkedOCEL(locel) => {
                serde_json::to_value(locel).map_err(|e| e.to_string())
            }
            RegistryItem::SlimLinkedOCEL(locel) => {
                serde_json::to_value(locel).map_err(|e| e.to_string())
            }
            RegistryItem::EventLogActivityProjection(proj) => {
                serde_json::to_value(proj).map_err(|e| e.to_string())
            }
        }
    }

    /// Try to load a registry item from a file path based on the expected type name
    pub fn load_from_path(item_kind: &RegistryItemKind, path: &str) -> Result<Self, String> {
        let path = std::path::Path::new(path);

        match item_kind {
            RegistryItemKind::EventLog => Ok(RegistryItem::EventLog(
                EventLog::import_from_path(path).map_err(|e| e.to_string())?,
            )),
            RegistryItemKind::OCEL => Ok(RegistryItem::OCEL(
                OCEL::import_from_path(path).map_err(|e| e.to_string())?,
            )),
            RegistryItemKind::SlimLinkedOCEL => Ok(RegistryItem::SlimLinkedOCEL({
                OCEL::import_from_path(path)
                    .map(SlimLinkedOCEL::from_ocel)
                    .map_err(|e| e.to_string())?
            })),
            RegistryItemKind::IndexLinkedOCEL => Ok(RegistryItem::IndexLinkedOCEL(
                IndexLinkedOCEL::import_from_path(path).map_err(|e| e.to_string())?,
            )),
            RegistryItemKind::EventLogActivityProjection => {
                Ok(RegistryItem::EventLogActivityProjection(
                    EventLogActivityProjection::import_from_path(path)
                        .map_err(|e| e.to_string())?,
                ))
            }
        }
    }

    /// Try to load a registry item from bytes based on the expected type name and format
    pub fn load_from_bytes(
        item_kind: &RegistryItemKind,
        data: &[u8],
        format: &str,
    ) -> Result<Self, String> {
        match item_kind {
            RegistryItemKind::EventLog => Ok(RegistryItem::EventLog(
                EventLog::import_from_bytes(data, format).map_err(|e| e.to_string())?,
            )),
            RegistryItemKind::OCEL => Ok(RegistryItem::OCEL(
                OCEL::import_from_bytes(data, format).map_err(|e| e.to_string())?,
            )),
            RegistryItemKind::IndexLinkedOCEL => Ok(RegistryItem::IndexLinkedOCEL(
                IndexLinkedOCEL::import_from_bytes(data, format).map_err(|e| e.to_string())?,
            )),
            RegistryItemKind::SlimLinkedOCEL => Ok(RegistryItem::SlimLinkedOCEL({
                OCEL::import_from_bytes(data, format)
                    .map(SlimLinkedOCEL::from_ocel)
                    .map_err(|e| e.to_string())?
            })),
            RegistryItemKind::EventLogActivityProjection => {
                Ok(RegistryItem::EventLogActivityProjection(
                    EventLogActivityProjection::import_from_bytes(data, format)
                        .map_err(|e| e.to_string())?,
                ))
            }
        }
    }

    /// Get the kind of the registry item
    pub fn kind(&self) -> RegistryItemKind {
        match self {
            RegistryItem::EventLogActivityProjection(_) => {
                RegistryItemKind::EventLogActivityProjection
            }
            RegistryItem::IndexLinkedOCEL(_) => RegistryItemKind::IndexLinkedOCEL,
            RegistryItem::EventLog(_) => RegistryItemKind::EventLog,
            RegistryItem::OCEL(_) => RegistryItemKind::OCEL,
            RegistryItem::SlimLinkedOCEL(_) => RegistryItemKind::SlimLinkedOCEL,
        }
    }

    /// Export the registry item to a file path
    pub fn export_to_path(&self, path: impl AsRef<std::path::Path>) -> Result<(), String> {
        let path = path.as_ref();
        match self {
            RegistryItem::EventLog(x) => x.export_to_path(path).map_err(|e| e.to_string()),
            RegistryItem::OCEL(x) => x.export_to_path(path).map_err(|e| e.to_string()),
            RegistryItem::IndexLinkedOCEL(x) => x.export_to_path(path).map_err(|e| e.to_string()),
            RegistryItem::SlimLinkedOCEL(x) => x
                .construct_ocel()
                .export_to_path(path)
                .map_err(|e| e.to_string()),
            RegistryItem::EventLogActivityProjection(x) => {
                x.export_to_path(path).map_err(|e| e.to_string())
            }
        }
    }

    /// Export the registry item to a byte vector
    pub fn export_to_bytes(&self, format: &str) -> Result<Vec<u8>, String> {
        let mut bytes = Vec::new();
        match self {
            RegistryItem::EventLog(x) => x
                .export_to_writer(&mut bytes, format)
                .map_err(|e| e.to_string())?,
            RegistryItem::OCEL(x) => x
                .export_to_writer(&mut bytes, format)
                .map_err(|e| e.to_string())?,
            RegistryItem::SlimLinkedOCEL(x) => x
                .construct_ocel()
                .export_to_writer(&mut bytes, format)
                .map_err(|e| e.to_string())?,
            RegistryItem::IndexLinkedOCEL(x) => x
                .export_to_writer(&mut bytes, format)
                .map_err(|e| e.to_string())?,
            RegistryItem::EventLogActivityProjection(x) => x
                .export_to_writer(&mut bytes, format)
                .map_err(|e| e.to_string())?,
        };
        Ok(bytes)
    }

    /// Convert the registry item to another kind
    pub fn convert(&self, target_kind: RegistryItemKind) -> Result<Self, String> {
        match (self, target_kind) {
            (RegistryItem::EventLog(log), RegistryItemKind::EventLogActivityProjection) => {
                Ok(RegistryItem::EventLogActivityProjection(log.into()))
            }
            (RegistryItem::OCEL(ocel), RegistryItemKind::IndexLinkedOCEL) => Ok(
                RegistryItem::IndexLinkedOCEL(IndexLinkedOCEL::from_ocel(ocel.clone())),
            ),
            (RegistryItem::IndexLinkedOCEL(locel), RegistryItemKind::OCEL) => {
                Ok(RegistryItem::OCEL(locel.get_ocel_ref().clone()))
            }
            _ => Err(format!("Cannot convert {} to {}", self.kind(), target_kind)),
        }
    }
}

/// Inner App State
pub type InnerAppState = HashMap<String, RegistryItem>;
/// State that can store 'big' types
#[derive(Debug, Default)]
pub struct AppState {
    /// Stored items
    pub items: RwLock<InnerAppState>,
}
impl AppState {
    /// Add the passed registry item
    pub fn add(&self, id: impl Into<String>, item: impl Into<RegistryItem>) {
        self.items.write().unwrap().insert(id.into(), item.into());
    }
    /// Check if the state contains the passed key
    pub fn contains_key(&self, id: &str) -> bool {
        self.items.read().unwrap().contains_key(id)
    }
}

/// Function Binding
#[derive(Debug)]
pub struct Binding {
    /// Unique ID of the function
    pub id: &'static str,
    /// Name of the function
    pub name: &'static str,
    /// Function handler (executing the function with (de-)serializing inputs/outputs)
    pub handler: fn(&Value, &AppState) -> Result<Value, String>,
    /// Documentation of function
    pub docs: fn() -> Vec<String>,
    /// Module path of declared function
    pub module: &'static str,
    /// File path of declared function
    pub source_path: &'static str,
    /// Line number of function in `source_path`
    pub source_line: u32,
    /// Get arguments of the function with the corresponding JSON schema
    pub args: fn() -> Vec<(String, Value)>,
    /// Get a list of all required arguments
    pub required_args: fn() -> Vec<String>,
    /// JSON Schema of return type
    pub return_type: fn() -> Value,
}
inventory::collect!(Binding);

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
/// Metadata of a function binding
pub struct BindingMeta {
    /// Unique ID of the function
    pub id: String,
    /// Name of the function
    pub name: String,
    /// Documentation of function
    pub docs: Vec<String>,
    /// Module path of declared function
    pub module: String,
    /// File path of declared function
    pub source_path: String,
    /// Line number of function in `source_path`
    pub source_line: u32,
    /// Get arguments of the function with the corresponding JSON schema
    pub args: Vec<(String, Value)>,
    /// Get a list of all required arguments
    pub required_args: Vec<String>,
    /// JSON Schema of return type
    pub return_type: Value,
}

impl From<&Binding> for BindingMeta {
    fn from(value: &Binding) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name.to_string(),
            docs: (value.docs)(),
            module: value.module.to_string(),
            source_path: value.source_path.to_string(),
            source_line: value.source_line,
            args: (value.args)(),
            required_args: (value.required_args)(),
            return_type: (value.return_type)(),
        }
    }
}

// Helper functions

/// Derive Value from Context
pub trait FromContext<'a>: Sized {
    /// Get value from context
    fn from_context(v: &Value, s: &'a InnerAppState) -> Result<Self, String>;
}

/// Try to extract function args (used in macro)
pub fn extract_param<'a, T: FromContext<'a>>(
    m: &serde_json::Map<String, Value>,
    k: &str,
    s: &'a InnerAppState,
    default: impl FnOnce() -> Option<T>,
) -> Result<T, String> {
    if let Some(x) = m.get(k) {
        // If argument is null in JSON, check if a default is given
        // when yes: Use that, otherwise, fallback to standard parsing
        if x.is_null() {
            let d = default();
            if let Some(d) = d {
                return Ok(d);
            }
        }
        T::from_context(x, s).map_err(|e| format!("Invalid Argument: {k}\n{e}"))
    } else {
        let r = default();
        r.ok_or_else(|| format!("Missing required argument {k}"))
    }
}

// Runtime Extraction
// If a type is Deserialize, we can extract it from JSON.
impl<'a, T> FromContext<'a> for T
where
    T: serde::de::DeserializeOwned,
{
    fn from_context(v: &Value, _: &'a InnerAppState) -> Result<Self, String> {
        serde_json::from_value(v.clone()).map_err(|e| e.to_string())
    }
}

/// Resolve an argument value based on its schema and the current state.
///
/// This function handles:
/// 1. Loading "Big Types" from file paths if the schema indicates a registry reference.
/// 2. Loading JSON objects from files if the value is a path ending in `.json`.
/// 3. Parsing JSON strings if the value is a string but the schema expects an object/array.
pub fn resolve_argument(
    arg_name: &str,
    value: Value,
    schema: &Value,
    state: &mut AppState,
) -> Result<Value, String> {
    let schema_obj = schema.as_object().ok_or("Invalid schema")?;

    // Case 1: Registry Reference
    if let Some(arg_ref) = schema_obj.get("x-registry-ref").and_then(|r| r.as_str()) {
        // If the value is already a string ID that exists in the registry, use it.
        if let Some(id) = value.as_str() {
            let mut items = state.items.write().map_err(|e| e.to_string())?;
            if let Some(item) = items.get(id) {
                if item.kind().to_string() == arg_ref {
                    return Ok(value);
                }

                // Try conversion
                use std::str::FromStr;
                let target_kind = RegistryItemKind::from_str(arg_ref)?;
                match item.convert(target_kind) {
                    Ok(converted) => {
                        let new_id = format!("{}_as_{}", id, arg_ref);
                        items.insert(new_id.clone(), converted);
                        return Ok(serde_json::Value::String(new_id));
                    }
                    Err(e) => {
                        return Err(format!(
                        "Type mismatch for ID '{}': expected {}, found {}. Conversion failed: {}",
                        id,
                        arg_ref,
                        item.kind(),
                        e
                    ))
                    }
                }
            }
            drop(items);

            // Otherwise, try to load it from file
            let item = RegistryItem::load_from_path(&RegistryItemKind::from_str(arg_ref)?, id)?;
            let stored_name = format!("A{}_{}", arg_name, uuid::Uuid::new_v4());
            state.add(&stored_name, item);
            return Ok(serde_json::Value::String(stored_name));
        }
    }

    // Case 2: Load JSON from file
    if let Some(val_str) = value.as_str() {
        if schema_obj.get("type") == Some(&serde_json::json!("object"))
            && val_str.ends_with(".json")
        {
            let file = std::fs::File::open(val_str)
                .map_err(|e| format!("Failed to open JSON file: {}", e))?;
            let reader = std::io::BufReader::new(file);
            let loaded_val: Value = serde_json::from_reader(reader)
                .map_err(|e| format!("Failed to parse JSON file: {}", e))?;
            return Ok(loaded_val);
        }
    }

    // Case 3: Parse JSON string (if needed)
    // If the schema expects an object/array but we got a string, try to parse it.
    if let Some(val_str) = value.as_str() {
        let type_field = schema_obj.get("type").and_then(|t| t.as_str());
        if matches!(type_field, Some("object") | Some("array")) {
            if let Ok(parsed) = serde_json::from_str::<Value>(val_str) {
                return Ok(parsed);
            }
        }
    }

    Ok(value)
}

/// Call the specified function with the passed arguments
pub fn call(binding: &Binding, args: &Value, state: &AppState) -> Result<Value, String> {
    (binding.handler)(args, state)
}

/// Get a list of all functions available through bindings
pub fn list_functions() -> Vec<&'static Binding> {
    inventory::iter::<Binding>.into_iter().collect()
}
/// Get a list of all function metadata available through bindings
pub fn list_functions_meta() -> Vec<BindingMeta> {
    inventory::iter::<Binding>
        .into_iter()
        .map(BindingMeta::from)
        .collect()
}

/// Get the binding information of an function by its name
pub fn get_fn_binding(id: &str) -> Option<&'static Binding> {
    inventory::iter::<Binding>.into_iter().find(|b| b.id == id)
}

/// Get the number of objects in an [`OCEL`]
#[binding_macros::register_binding]
pub fn num_objects(ocel: &IndexLinkedOCEL) -> usize {
    ocel.get_ocel_ref().objects.len()
}
/// Get the number of events in an [`OCEL`]
#[binding_macros::register_binding]
pub fn num_events(ocel: &IndexLinkedOCEL) -> usize {
    ocel.get_ocel_ref().events.len()
}

/// Convert an [`OCEL`] to an [`IndexLinkedOCEL`]
#[binding_macros::register_binding]
pub fn index_link_ocel(ocel: &OCEL) -> IndexLinkedOCEL {
    IndexLinkedOCEL::from_ocel(ocel.clone())
}

#[binding_macros::register_binding]
/// This is a test function.
///
/// **This should be bold**, *this is italic*, `and this code`.
///
pub fn test_some_inputs(s: String, n: usize, i: i32, f: f64, b: bool) -> String {
    format!("s={},n={},i={},f={},b={}", s, n, i, f, b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_consistent_registry_item_variants() {
        // Ensure that we have the expected variants
        let variants = RegistryItemKind::all_kinds();
        let variant_names: HashSet<String> = variants.iter().map(|v| v.to_string()).collect();

        // Get the list of types from the macro crate
        let macro_types: &[&str] = binding_macros::big_types_list!();
        let macro_type_names: HashSet<String> = macro_types.iter().map(|s| s.to_string()).collect();

        // Check for consistency
        // 1. All types in macro must be in RegistryItem
        for macro_type in &macro_type_names {
            assert!(
                variant_names.contains(macro_type),
                "Macro expects type '{}' which is missing in RegistryItem enum",
                macro_type
            );
        }

        // 2. All types in RegistryItem must be in macro
        for variant in &variant_names {
            assert!(
                macro_type_names.contains(variant),
                "RegistryItem has variant '{}' which is missing in binding_macros::BIG_TYPES_NAMES",
                variant
            );
        }

        assert_eq!(
            variant_names.len(),
            macro_type_names.len(),
            "Mismatch in number of types between RegistryItem and binding_macros"
        );
    }
}
