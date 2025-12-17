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
        object_centric::linked_ocel::IndexLinkedOCEL,
    },
    EventLog,
};
use serde_json::Value;
use std::sync::RwLock;
use std::{collections::HashMap, fmt::Display};

/// Manually maintained Registry enum of 'big' types
///
/// NOTE: When extending this with a new variant, make sure to also update `BIG_TYPES_NAMES` in the macro crate.
#[derive(Debug)]
#[allow(clippy::large_enum_variant, missing_docs)]
pub enum RegistryItem {
    EventLogActivityProjection(EventLogActivityProjection),
    IndexLinkedOCEL(IndexLinkedOCEL),
    EventLog(EventLog),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[allow(missing_docs)]
pub enum RegistryItemKind {
    EventLogActivityProjection,
    IndexLinkedOCEL,
    EventLog,
}

impl Display for RegistryItemKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            RegistryItemKind::EventLogActivityProjection => "EventLogActivityProjection",
            RegistryItemKind::IndexLinkedOCEL => "IndexLinkedOCEL",
            RegistryItemKind::EventLog => "EventLog",
        };
        write!(f, "{}", s)
    }
}

impl RegistryItemKind {
    /// Get all kinds of `RegistryItemKind`
    pub fn all_kinds() -> &'static [Self] {
        &[
            RegistryItemKind::EventLogActivityProjection,
            RegistryItemKind::IndexLinkedOCEL,
            RegistryItemKind::EventLog,
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
            _ => Err(format!("Unknown RegistryItemKind: {}", s)),
        }
    }
}

impl RegistryItem {
    /// Try to load a registry item from a file path based on the expected type name
    pub fn load_from_path(type_name: impl AsRef<str>, path: &str) -> Result<Self, String> {
        use std::str::FromStr;
        let item_type = RegistryItemKind::from_str(type_name.as_ref())
            .map_err(|_| format!("Unknown registry type: {}", type_name.as_ref()))?;

        match item_type {
            RegistryItemKind::IndexLinkedOCEL => {
                let ocel =
                    crate::core::event_data::object_centric::ocel_json::import_ocel_json_from_path(
                        path,
                    )
                    .map_err(|e| e.to_string())?;
                let locel = IndexLinkedOCEL::from_ocel(ocel);
                Ok(RegistryItem::IndexLinkedOCEL(locel))
            }
            RegistryItemKind::EventLogActivityProjection => {
                let xes = crate::core::event_data::case_centric::xes::import_xes_file(
                    path,
                    crate::core::event_data::case_centric::xes::XESImportOptions::default(),
                )
                .map_err(|e| e.to_string())?;
                Ok(RegistryItem::EventLogActivityProjection((&xes).into()))
            }
            RegistryItemKind::EventLog => {
                let xes = crate::core::event_data::case_centric::xes::import_xes_file(
                    path,
                    crate::core::event_data::case_centric::xes::XESImportOptions::default(),
                )
                .map_err(|e| e.to_string())?;
                Ok(RegistryItem::EventLog(xes))
            }
        }
    }

    /// Try to load a registry item from bytes based on the expected type name and format
    pub fn load_from_bytes(
        item_type: RegistryItemKind,
        data: &[u8],
        format: &str,
    ) -> Result<Self, String> {
        match item_type {
            RegistryItemKind::IndexLinkedOCEL => {
                let ocel = match format {
                    "json" => crate::core::event_data::object_centric::ocel_json::import_ocel_json_from_slice(data)
                        .map_err(|e| e.to_string())?,
                    "xml" => crate::core::event_data::object_centric::ocel_xml::xml_ocel_import::import_ocel_xml_slice(data),
                    #[cfg(feature = "ocel-sqlite")]
                    "sqlite" => crate::core::event_data::object_centric::ocel_sql::sqlite::sqlite_ocel_import::import_ocel_sqlite_from_slice(data)
                        .map_err(|e| e.to_string())?,
                     _ => return Err(format!("Unknown or unsupported format for IndexLinkedOCEL: {}", format)),
                };
                let locel = IndexLinkedOCEL::from_ocel(ocel);
                Ok(RegistryItem::IndexLinkedOCEL(locel))
            }
            RegistryItemKind::EventLogActivityProjection => {
                // Assume XES
                let xes = crate::core::event_data::case_centric::xes::import_xes_slice(
                    data,
                    format.ends_with(".gz"),
                    Default::default(),
                )
                .map_err(|e| e.to_string())?;
                Ok(RegistryItem::EventLogActivityProjection((&xes).into()))
            }
            RegistryItemKind::EventLog => {
                // Assume XES
                let xes = crate::core::event_data::case_centric::xes::import_xes_slice(
                    data,
                    format.ends_with(".gz"),
                    Default::default(),
                )
                .map_err(|e| e.to_string())?;
                Ok(RegistryItem::EventLog(xes))
            }
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
    pub fn add(&self, id: &str, item: RegistryItem) {
        self.items.write().unwrap().insert(id.to_string(), item);
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

// Helper functions

/// Derive Value from Context
pub trait FromContext<'a>: Sized {
    /// Ger value from context
    fn from_context(v: &Value, s: &'a InnerAppState) -> Result<Self, String>;
}

/// Try to extract function args (used in macro)
pub fn extract_param<'a, T: FromContext<'a>>(
    m: &serde_json::Map<String, Value>,
    k: &str,
    s: &'a InnerAppState,
) -> Result<T, String> {
    T::from_context(m.get(k).ok_or("Missing Arg")?, s)
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
            if state.contains_key(id) {
                return Ok(value);
            }

            // Otherwise, try to load it from file
            let item = RegistryItem::load_from_path(arg_ref, id)?;
            let stored_name = format!(
                "A{}_{}",
                arg_name,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
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

/// Get the binding information of an function by its name
pub fn get_fn_binding(id: &str) -> Option<&'static Binding> {
    inventory::iter::<Binding>.into_iter().find(|b| b.id == id)
}

/// Test Binding
#[binding_macros::register_binding]
pub fn test(log: &EventLog) -> EventLog {
    log.clone()
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
