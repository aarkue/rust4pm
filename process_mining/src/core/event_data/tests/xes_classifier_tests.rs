#[cfg(test)]
use std::{collections::HashSet, time::Instant};

use rayon::prelude::*;

use crate::{
    core::event_data::case_centric::xes::import_xes::{import_xes_file, XESImportOptions},
    test_utils::get_test_data_path,
};

#[test]
pub fn test_get_class_identity() {
    let path = get_test_data_path()
        .join("xes")
        .join("Road_Traffic_Fine_Management_Process.xes.gz");
    let log = import_xes_file(&path, XESImportOptions::default()).unwrap();
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

#[test]
pub fn test_get_class_identity_complex() {
    let path = get_test_data_path().join("xes").join("AN1-example.xes");
    let log = import_xes_file(&path, XESImportOptions::default()).unwrap();
    let now = Instant::now();
    let classifier = log.get_classifier_by_name("classifier1");
    assert!(classifier.is_some());

    if let Some(classifier) = classifier {
        // Gather unique variants of traces (wrt. the event name classifier above)
        let trace_variants: HashSet<Vec<String>> = log
            .traces
            .iter()
            .map(|t| {
                t.events
                    .par_iter()
                    .map(|e| classifier.get_class_identity_with_globals(e, &log.global_event_attrs))
                    .collect()
            })
            .collect();

        println!(
            "Took: {:?}; got {} unique variants",
            now.elapsed(),
            trace_variants.len()
        );
        assert_eq!(trace_variants.len(), 5);

        let example_variant: Vec<String> = vec![
            "TEST+TEST2",
            "TEST+TEST2",
            "TEST+TEST2",
            "TEST+TEST2",
            "TEST+TEST2",
            "TEST+TEST2",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect();
        assert!(trace_variants.contains(&example_variant))
    }
}
