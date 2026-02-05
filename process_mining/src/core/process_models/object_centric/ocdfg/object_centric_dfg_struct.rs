use macros_process_mining::register_binding;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::core::{
    event_data::object_centric::{linked_ocel::LinkedOCELAccess, utils::flatten::flatten_ocel_on},
    process_models::case_centric::dfg::dfg_struct::DirectlyFollowsGraph,
    EventLog,
};

///
/// An object-centric directly-follows graph containing a [`DirectlyFollowsGraph`] for each object
/// type involved.
///
#[derive(Debug, Serialize, Deserialize, Default, JsonSchema)]
pub struct OCDirectlyFollowsGraph<'a> {
    /// The DFG per object type
    pub object_type_to_dfg: HashMap<String, DirectlyFollowsGraph<'a>>,
}

impl<'a> OCDirectlyFollowsGraph<'a> {
    ///
    /// Create new [`OCDirectlyFollowsGraph`] with no object types and no [`DirectlyFollowsGraph`]s.
    ///
    pub fn new() -> Self {
        Self {
            object_type_to_dfg: HashMap::new(),
        }
    }

    ///
    /// Construct a [`OCDirectlyFollowsGraph`] from an [`IndexLinkedOCEL`]
    ///
    pub fn create_from_ocel(locel: &'a impl LinkedOCELAccess<'a>) -> Self {
        discover_dfg_from_ocel(locel)
    }

    ///
    /// Serialize to JSON string.
    ///
    pub fn to_json(self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

///
/// Construct a [`OCDirectlyFollowsGraph`] from an [`IndexLinkedOCEL`]
///
#[register_binding]
pub fn discover_dfg_from_ocel<'a>(
    ocel: &'a impl LinkedOCELAccess<'a>,
) -> OCDirectlyFollowsGraph<'a> {
    let mut result = OCDirectlyFollowsGraph::new();

    // For each object type: flatten the OCEL on the object type and discover its DFG
    ocel.get_ob_types().for_each(|ob_type| {
        let event_log: EventLog = flatten_ocel_on(ocel, ob_type);

        let object_type_dfg = DirectlyFollowsGraph::discover(&event_log);

        result
            .object_type_to_dfg
            .insert(ob_type.to_string(), object_type_dfg);
    });

    result
}
