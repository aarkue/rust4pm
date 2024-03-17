use chrono::DateTime;

use crate::{
    event_log::{
        import_xes::{import_xes_slice, XESImportOptions, XESParseError},
        AttributeValue, Trace, XESEditableAttribute,
    },
    import_xes_file,
};

#[test]
fn test_xes_gz_import() {
    let x = include_bytes!("test_data/Sepsis Cases - Event Log.xes.gz");
    let log = import_xes_slice(x, true, XESImportOptions::default()).unwrap();

    // Log has 1050 cases total
    assert_eq!(log.traces.len(), 1050);

    // Case with concept:name "A" has 22 events
    let case_a = log.traces.iter().find(|t| {
        t.attributes
            .get_by_key("concept:name")
            .is_some_and(|c| c.value == AttributeValue::String("A".to_string()))
    });
    assert!(case_a.is_some());
    assert_eq!(case_a.unwrap().events.len(), 22);

    // Test if activity trace of case "A" is correct
    let case_a_act_trace: Vec<AttributeValue> = case_a
        .unwrap()
        .events
        .iter()
        .map(|ev| {
            ev.attributes
                .get_by_key("concept:name")
                .unwrap()
                .value
                .clone()
        })
        .collect();
    let case_a_correct_act_trace: Vec<AttributeValue> = vec![
        "ER Registration",
        "Leucocytes",
        "CRP",
        "LacticAcid",
        "ER Triage",
        "ER Sepsis Triage",
        "IV Liquid",
        "IV Antibiotics",
        "Admission NC",
        "CRP",
        "Leucocytes",
        "Leucocytes",
        "CRP",
        "Leucocytes",
        "CRP",
        "CRP",
        "Leucocytes",
        "Leucocytes",
        "CRP",
        "CRP",
        "Leucocytes",
        "Release A",
    ]
    .into_iter()
    .map(|act| AttributeValue::String(act.into()))
    .collect();
    assert_eq!(case_a_act_trace, case_a_correct_act_trace);

    // Test if first event of case "A" has correct timestamp
    assert_eq!(
        case_a.unwrap().events[0]
            .attributes
            .get_by_key("time:timestamp")
            .unwrap()
            .value,
        AttributeValue::Date(
            DateTime::parse_from_rfc3339("2014-10-22 09:15:41+00:00")
                .unwrap()
                .into()
        )
    );

    // Test if log name is correct
    let log_name = match &log.attributes.get_by_key("concept:name").unwrap().value {
        AttributeValue::String(s) => Some(s.as_str()),
        _ => None,
    };
    assert_eq!(log_name, Some("Sepsis Cases - Event Log"));
}
#[test]
pub fn test_invalid_xes_non_existing_file_gz() {
    let res_gz = import_xes_file(
        "this-file-does-not-exist.xes.gz",
        XESImportOptions::default(),
    );
    assert!(matches!(res_gz, Err(XESParseError::IOError(_))));
}
#[test]
pub fn test_invalid_xes_non_existing_file() {
    let res_gz = import_xes_file("this-file-does-not-exist.xes", XESImportOptions::default());
    assert!(matches!(res_gz, Err(XESParseError::IOError(_))));
}

#[test]
pub fn test_invalid_xes_file_gz_expected() {
    // Example XML (non-XES) file; Error should only involve missing .gz headers, so the other  content does not matter
    let x = include_bytes!("test_data/BPI_Challenge_2019_sampled_3000cases_model_alphappp.pnml");
    let res_gz = import_xes_slice(x, true, XESImportOptions::default());
    assert!(matches!(res_gz, Err(XESParseError::XMLParsingError(_))));
}

#[test]
pub fn test_invalid_xes_file_gz_unexpected() {
    let x = include_bytes!("test_data/Sepsis Cases - Event Log.xes.gz");
    let res = import_xes_slice(x, false, XESImportOptions::default());
    assert!(matches!(res, Err(XESParseError::NoTopLevelLog)));
}

#[test]
pub fn test_invalid_xes_file_zero() {
    // File with ~1MB of zeros (dd if=/dev/zero of=zero.file bs=1024 count=1024)
    let x = include_bytes!("test_data/zero.file");
    let res = import_xes_slice(x, false, XESImportOptions::default());
    assert!(matches!(res, Err(XESParseError::NoTopLevelLog)));
}

#[test]
pub fn test_invalid_xes_file_zero_gz() {
    // File with ~1MB of zeros (dd if=/dev/zero of=zero.file bs=1024 count=1024)
    let x = include_bytes!("test_data/zero.file");
    let res = import_xes_slice(x, true, XESImportOptions::default());
    assert!(matches!(res, Err(XESParseError::XMLParsingError(_))));
}

#[test]
pub fn test_invalid_xes_file_pnml() {
    let x = include_bytes!("test_data/BPI_Challenge_2019_sampled_3000cases_model_alphappp.pnml");
    let res = import_xes_slice(x, false, XESImportOptions::default());
    assert!(matches!(res, Err(XESParseError::NoTopLevelLog)));
}

#[test]
pub fn test_invalid_xes_file_json() {
    let x = include_bytes!("test_data/order-management.json");
    let res = import_xes_slice(x, false, XESImportOptions::default());
    assert!(matches!(res, Err(XESParseError::NoTopLevelLog)));
}

#[test]
pub fn test_invalid_xes_file_empty() {
    let x: &'static [u8] = &[];
    let res = import_xes_slice(x, false, XESImportOptions::default());
    assert!(matches!(res, Err(XESParseError::NoTopLevelLog)));
}

#[test]
pub fn test_nested_global_event_attr() {
    let x = include_bytes!("test_data/small-example.xes");
    let res = import_xes_slice(
        x,
        false,
        XESImportOptions {
            ..XESImportOptions::default()
        },
    )
    .unwrap();
    let a = res
        .global_event_attrs
        .as_ref()
        .unwrap()
        .get_by_key("nested-global-string-attr")
        .unwrap();
    assert!(a.own_attributes.is_some());
    assert_eq!(
        a.own_attributes
            .as_ref()
            .unwrap()
            .get_by_key("was-correctly-parsed")
            .unwrap()
            .value,
        AttributeValue::Boolean(true)
    );
}

#[test]
pub fn test_xes_unsorted_traces() {
    fn trace_to_test_id_vec(trace: &Trace) -> Vec<&String> {
        trace
            .events
            .iter()
            .map(|e| {
                e.attributes
                    .get_by_key("testid")
                    .unwrap()
                    .value
                    .try_as_string()
                    .unwrap()
            })
            .collect()
    }

    let x = include_bytes!("test_data/small-example.xes");
    let res = import_xes_slice(
        x,
        false,
        XESImportOptions {
            ..XESImportOptions::default()
        },
    )
    .unwrap();

    assert_eq!(
        trace_to_test_id_vec(res.traces.last().unwrap()),
        vec![
            "Second",
            "First",
            "With invalid time:timestamp",
            "With default timestamp",
        ]
    );

    let res_sorted_events = import_xes_slice(
        x,
        false,
        XESImportOptions {
            sort_events_with_timestamp_key: Some("time:timestamp".into()),
            ..XESImportOptions::default()
        },
    )
    .unwrap();
    // In this test log, the default timestamp (global event attribute) is set to a later date than all other timestamps
    // Thus, events without a valid timestamp (either none or invalid type) should appear after all other events (but in stable ordering)
    // If no default _would_ be provided, the events would be sorted as first
    assert_eq!(
        trace_to_test_id_vec(res_sorted_events.traces.last().unwrap()),
        vec![
            "First",
            "Second",
            "With invalid time:timestamp",
            "With default timestamp",
        ]
    );
}
