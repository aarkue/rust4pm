#![cfg(feature = "bindings")]

use crate::core::{
    event_data::{
        case_centric::utils::activity_projection::EventLogActivityProjection,
        object_centric::linked_ocel::IndexLinkedOCEL,
    },
    EventLog,
};
use serde_json::Value;
use std::collections::HashMap;

/// Manually maintained Registry enum of 'big' types
///
/// Big types should not be serialized/deserialized and instead refered to by their name/ID, assuming
/// that they are available in some (global) state
#[derive(Debug)]
#[allow(missing_docs, clippy::large_enum_variant)]
pub enum RegistryItem {
    EventLogActivityProjection(EventLogActivityProjection),
    IndexLinkedOCEL(IndexLinkedOCEL),
    EventLog(EventLog),
}

/// State that can store 'big' types
#[derive(Debug, Default)]
pub struct AppState {
    /// Stored items
    pub items: HashMap<String, RegistryItem>,
}
impl AppState {
    /// Add the passed registry item
    pub fn add(&mut self, id: &str, item: RegistryItem) {
        self.items.insert(id.to_string(), item);
    }
}

/// Function Binding
#[derive(Debug)]
pub struct Binding {
    /// Name of the function
    pub name: &'static str,
    /// Function handler (executing the function with (de-)serializing inputs/outputs)
    pub handler: fn(&Value, &AppState) -> Result<Value, String>,
    /// Documentation of function
    pub docs: fn() -> Vec<String>,
    /// Get arguments of the function
    pub args: fn() -> HashMap<String, Value>,
    /// Retrieve the JSON Schema of the function
    pub schema: fn() -> Value,
}
inventory::collect!(Binding);

// Helper functions

/// Derive Value from Context
pub trait FromContext<'a>: Sized {
    /// Ger value from context
    fn from_context(v: &Value, s: &'a AppState) -> Result<Self, String>;
}
/// JSON Schema provider with some overwrites
pub trait SchemaProvider {
    /// Get JSON Schema
    fn get_schema_gen() -> Value;
}
/// Try to extract function args (used in macro)
pub fn extract_param<'a, T: FromContext<'a>>(
    m: &serde_json::Map<String, Value>,
    k: &str,
    s: &'a AppState,
) -> Result<T, String> {
    T::from_context(m.get(k).ok_or("Missing Arg")?, s)
}

// Runtime Extraction
// If a type is Deserialize, we can extract it from JSON.
impl<'a, T> FromContext<'a> for T
where
    T: serde::de::DeserializeOwned,
{
    fn from_context(v: &Value, _: &'a AppState) -> Result<Self, String> {
        serde_json::from_value(v.clone()).map_err(|e| e.to_string())
    }
}

// Schema Generation
impl<T> SchemaProvider for T
where
    T: schemars::JsonSchema,
{
    fn get_schema_gen() -> Value {
        serde_json::to_value(schemars::schema_for!(T)).unwrap()
    }
}

/// Call the specified function with the passed arguments
pub fn call(func_name: &str, args: &Value, state: &AppState) -> Result<Value, String> {
    for binding in inventory::iter::<Binding> {
        if binding.name == func_name {
            // Found the function! Execute it.
            return (binding.handler)(args, state);
        }
    }
    Err(format!("Function '{}' not found in registry", func_name))
}

/// Get a list of all functions available through bindings
pub fn list_functions() -> Vec<String> {
    inventory::iter::<Binding>
        .into_iter()
        .map(|b| b.name.to_string())
        .collect()
}

/// Get the binding information of an function by its name
pub fn get_fn_binding(name: &str) -> Option<&'static Binding> {
    inventory::iter::<Binding>
        .into_iter()
        .find(|b| b.name == name)
}

#[cfg(test)]
mod tests {
    use crate::core::event_data::{
        case_centric::{
            utils::activity_projection::EventLogActivityProjection,
            xes::{import_xes_file, XESImportOptions},
        },
        object_centric::ocel_json::import_ocel_json_from_path,
    };

    #[test]
    fn test_bindings() {
        use crate::bindings::{AppState, Binding, RegistryItem};

        let log = import_xes_file(
            "/home/aarkue/dow/Sepsis Cases - Event Log.xes.gz",
            XESImportOptions::default(),
        )
        .unwrap();
        // 1. Setup
        let mut state = AppState::default();
        state.add(
            "L1",
            RegistryItem::EventLogActivityProjection(EventLogActivityProjection::from(&log)),
        );
        state.add(
            "O1",
            RegistryItem::IndexLinkedOCEL(
                import_ocel_json_from_path("/home/aarkue/dow/ocel/order-management.json")
                    .unwrap()
                    .into(),
            ),
        );

        // 2. Inspect Schema
        println!("--- Schema ---");
        for b in inventory::iter::<Binding> {
            let schema = (b.schema)();
            println!("{}", serde_json::to_string_pretty(&schema).unwrap());
        }

        // 3. Run
        println!("\n--- Execution ---");

        let input = serde_json::json!({
        "log_proj": "L1",
        "config": {
            "balance_thresh": 0.1,
            "fitness_thresh": 0.8,
            "replay_thresh": 0.0,
            "log_repair_skip_df_thresh_rel": 4.0,
            "log_repair_loop_df_thresh_rel": 4.0,
            "absolute_df_clean_thresh": 1,
            "relative_df_clean_thresh": 0.01,
        }
                });

        match super::call("alphappp_discover_petri_net", &input, &state) {
            Ok(res) => println!("Go result: {:?}", res),
            Err(e) => println!("Caught error: {}", e),
        }
        match super::call(
            "discover_dfg_from_locel",
            &serde_json::json!({"locel": "O1"}),
            &state,
        ) {
            Ok(res) => println!("Got OC-DFG:\n {:?}", res),
            Err(e) => println!("Caught error: {}", e),
        }
    }
}
