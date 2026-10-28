//! Discover [`DirectlyFollowsGraph`]s from Data

use macros_process_mining::register_binding;

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
                result.add_start_activity(curr_event_identity.clone(), 1);
            }

            last_event_identity = Some(curr_event_identity);
        });
        if let Some(last_ev_id) = last_event_identity.take() {
            result.add_end_activity(last_ev_id, 1);
        }
    });

    result
}

/// Discover [`DirectlyFollowsGraph`] with default classifier
#[register_binding]
pub fn discover_dfg<'b>(event_log: &EventLog) -> DirectlyFollowsGraph<'b> {
    discover_dfg_with_classifier(event_log, &EventLogClassifier::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_log;

    #[test]
    fn start_and_end_activities_count_their_traces() {
        let log = event_log!(["a", "b"], ["a", "c"], ["b"], []);
        let dfg = discover_dfg(&log);
        let counts = |map: &std::collections::HashMap<String, u32>| {
            let mut counts: Vec<(String, u32)> = map.iter().map(|(a, &c)| (a.clone(), c)).collect();
            counts.sort();
            counts
        };
        assert_eq!(
            counts(&dfg.start_activities),
            [("a".to_string(), 2), ("b".to_string(), 1)]
        );
        assert_eq!(
            counts(&dfg.end_activities),
            [("b".to_string(), 2), ("c".to_string(), 1)]
        );
    }
}
