//! Discover [`DirectlyFollowsGraph`]s from Data

use binding_macros::register_binding;

use crate::core::{
    event_data::case_centric::EventLogClassifier,
    process_models::case_centric::dfg::DirectlyFollowsGraph, EventLog,
};

/// Discover a [`DirectlyFollowsGraph`] from an [`EventLog`] using the specified [`EventLogClassifier`] to derive the 'activity' names
///
/// If there is no special classifier to be used, the default (`&EventLogClassifier::default()`) can also simply be passed in
pub fn discover_dfg_with_classifier<'a, 'b>(
    event_log: &'b EventLog,
    classifier: &'b EventLogClassifier,
) -> DirectlyFollowsGraph<'a> {
    let mut result = DirectlyFollowsGraph::new();
    event_log.traces.iter().for_each(|t| {
        let mut last_event_identity: Option<String> = None;
        t.events.iter().for_each(|e| {
            let curr_event_identity = classifier.get_class_identity(e);
            result.add_activity(curr_event_identity.clone(), 1);

            if let Some(last_ev_id) = last_event_identity.take() {
                result.add_df_relation(last_ev_id.into(), curr_event_identity.clone().into(), 1)
            } else {
                result.add_start_activity(curr_event_identity.clone());
            }

            last_event_identity = Some(curr_event_identity);
        });
        if let Some(last_ev_id) = last_event_identity.take() {
            result.add_end_activity(last_ev_id);
        }
    });

    result
}

/// Discover [`DirectlyFollowsGraph`] with default classifier
#[register_binding]
pub fn discover_dfg<'b>(event_log: &EventLog) -> DirectlyFollowsGraph<'b> {
    discover_dfg_with_classifier(event_log, &EventLogClassifier::default())
}
