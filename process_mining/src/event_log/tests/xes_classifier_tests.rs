#[cfg(test)]
use std::{collections::HashSet, time::Instant};

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::event_log::import_xes::{import_xes_slice, XESImportOptions};

#[test]
pub fn test_get_class_identity() {
    let log_bytes = include_bytes!("test_data/Road_Traffic_Fine_Management_Process.xes.gz");
    let log = import_xes_slice(log_bytes, true, XESImportOptions::default()).unwrap();
    let now = Instant::now();
    let event_name_classifier = log.get_classifier_by_name("Event Name");
    assert!(event_name_classifier.is_some());

    if let Some(classifier) = event_name_classifier {
        // Gather unique variants of traces (wrt. the event name classifier above)
        let trace_variants: HashSet<Vec<String>> = log
            .traces
            .iter()
            .map(|t| {
                t.events
                    .par_iter()
                    .map(|e| classifier.get_class_identity(e))
                    .collect()
            })
            .collect();

        println!(
            "Took: {:?}; got {} unique variants",
            now.elapsed(),
            trace_variants.len()
        );
        assert_eq!(trace_variants.len(), 231);

        // Variants should contain example variant
        let example_variant: Vec<String> = vec![
            "Create Fine",
            "Send Fine",
            "Insert Fine Notification",
            "Add penalty",
            "Insert Date Appeal to Prefecture",
            "Send Appeal to Prefecture",
            "Receive Result Appeal from Prefecture",
            "Notify Result Appeal to Offender",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect();
        assert!(trace_variants.contains(&example_variant))
    }
}
