use crate::{import_xes_file, utils::test_utils::get_test_data_path, XESImportOptions};

use super::XESEditableAttribute;

mod ocel_xml_import_tests;
mod xes_classifier_tests;
mod xes_import_tests;

#[test]
fn test_event_log_attribute_helpers() {
    let path = get_test_data_path().join("xes").join("RepairExample.xes");
    let mut log = import_xes_file(&path, XESImportOptions::default()).unwrap();
    // Global trace attribute for "concept:name" is set to "__INVALID__"
    let trace = log.traces.last_mut().unwrap();
    // Last trace has a "concept:name" value 999
    let concept_name = trace
        .attributes
        .get_by_key_or_global("concept:name", &log.global_trace_attrs)
        .and_then(|a| a.value.try_as_string())
        .unwrap();
    assert_eq!(concept_name, "999");
    // ...but if we remove this attribute...
    trace.attributes.remove_with_key("concept:name");
    // ...the global attribute value ("__INVALID__") will be returned :)
    let concept_name_after = trace
        .attributes
        .get_by_key_or_global("concept:name", &log.global_trace_attrs)
        .and_then(|a| a.value.try_as_string())
        .unwrap();
    assert_eq!(concept_name_after, "__INVALID__");
}
