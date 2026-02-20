//! Object Attribute Change History
//!
//! Extracts the time-stamped history of attribute value changes for a single
//! OCEL object, grouped by attribute name.

use std::collections::HashMap;

use chrono::{DateTime, FixedOffset};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use macros_process_mining::register_binding;

use crate::core::event_data::object_centric::{
    linked_ocel::LinkedOCELAccess, ocel_struct::OCELAttributeValue,
};

/// A single attribute value change at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AttributeChange {
    /// Timestamp of the change.
    pub time: DateTime<FixedOffset>,
    /// Attribute value at this point in time.
    pub value: OCELAttributeValue,
}

/// Time-stamped attribute change history for a single OCEL object.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ObjectAttributeChanges {
    /// Attribute change traces keyed by attribute name.
    ///
    /// Each entry contains the chronological list of value changes
    /// for that attribute.
    pub traces: HashMap<String, Vec<AttributeChange>>,
}

#[register_binding(stringify_error)]
/// Extract the attribute change history for a specific object.
///
/// Returns one trace per attribute, each containing the timestamped values
/// in the order they appear in the log. Returns an error if the object ID
/// is not found.
pub fn get_object_attribute_changes<'a>(
    ocel: &'a impl LinkedOCELAccess<'a>,
    object_id: impl AsRef<str>,
) -> Result<ObjectAttributeChanges, String> {
    let object_id = object_id.as_ref();
    let ob = ocel
        .get_ob_by_id(object_id)
        .ok_or_else(|| format!("Object with ID '{object_id}' not found."))?;

    let attr_names: Vec<_> = ocel.get_ob_attrs(&ob).map(str::to_string).collect();

    let mut traces: HashMap<String, Vec<AttributeChange>> = attr_names
        .iter()
        .map(|name| (name.clone(), Vec::new()))
        .collect();

    for attr_name in &attr_names {
        if let Some(trace) = traces.get_mut(attr_name) {
            for (time, value) in ocel.get_ob_attr_vals(&ob, attr_name) {
                trace.push(AttributeChange {
                    time: *time,
                    value: value.clone(),
                });
            }
        }
    }

    Ok(ObjectAttributeChanges { traces })
}
