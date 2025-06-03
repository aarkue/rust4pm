use crate::dfg::DirectlyFollowsGraph;
use crate::event_log::event_log_struct::EventLogClassifier;
use crate::ocel::flatten::flatten_ocel_on;
use crate::ocel::linked_ocel::{IndexLinkedOCEL, LinkedOCELAccess};
use crate::EventLog;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// An object-centric directly-follows graph containing a [`DirectlyFollowsGraph`] for each object
/// type involved.
#[derive(Debug, Serialize, Deserialize)]
pub struct OCDirectlyFollowsGraph<'a> {
    pub object_type_to_dfg: HashMap<String, DirectlyFollowsGraph<'a>>,
}

impl Default for OCDirectlyFollowsGraph<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> OCDirectlyFollowsGraph<'a> {
    /// Create new [`OCDirectlyFollowsGraph`] with no object types and no [`DirectlyFollowsGraph`]s.
    pub fn new() -> Self {
        Self {
            object_type_to_dfg: HashMap::new(),
        }
    }

    /// Construct a [`OCDirectlyFollowsGraph`] from an [`IndexLinkedOCEL`]
    pub fn create_from_locel(locel: &IndexLinkedOCEL) -> Self {
        let mut result = Self::new();

        locel.get_ob_types().for_each(|ob_type| {
            let event_log: EventLog = flatten_ocel_on(locel, &ob_type.to_string());

            let object_type_dfg =
                DirectlyFollowsGraph::create_from_log(&event_log, &EventLogClassifier::default());

            result
                .object_type_to_dfg
                .insert(ob_type.to_string(), object_type_dfg);
        });

        result
    }

    /// Serialize to JSON string.
    pub fn to_json(self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}
