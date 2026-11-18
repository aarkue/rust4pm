use std::{fs::File, io::Read};

use chrono::DateTime;

use crate::{
    core::event_data::case_centric::{
        event_log_struct::{AttributeValue, Trace, XESEditableAttribute},
        io::EventLogIOError,
        xes::import_xes::{XESImportOptions, XESParseError},
    },
    test_utils::get_test_data_path,
    Control, EventLog, Exportable, Importable,
};

fn import_str(xes: &str) -> Result<EventLog, EventLogIOError> {
    EventLog::import_from_bytes(xes.as_bytes(), "xes")
}

fn import_str_with(xes: &str, options: XESImportOptions) -> Result<EventLog, EventLogIOError> {
    EventLog::import_from_bytes_with(xes.as_bytes(), "xes", options, &Control::new())
}

#[test]
fn control_imports_xes_with_progress_and_cancel() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;
    let path = get_test_data_path()
        .join("xes")
        .join("Sepsis Cases - Event Log.xes.gz");
    let bytes = std::fs::read(&path).unwrap();
    let len = bytes.len() as u64;
    let seen = Mutex::new(Vec::new());
    let ctl = Control::new().on_progress(|p| seen.lock().unwrap().push(p));
    let from_path =
        EventLog::import_from_path_with(&path, XESImportOptions::default(), &ctl).unwrap();
    let from_slice =
        EventLog::import_from_bytes_with(&bytes, "xes.gz", XESImportOptions::default(), &ctl)
            .unwrap();
    assert_eq!(from_path.traces.len(), 1050);
    assert_eq!(from_slice.traces.len(), 1050);
    drop(ctl);
    let seen = seen.into_inner().unwrap();
    assert!(seen
        .iter()
        .all(|p| p.stage == "Importing" && p.total == Some(len)));
    assert_eq!(seen.iter().filter(|p| p.done == len).count(), 2, "{seen:?}");

    let flag = AtomicBool::new(false);
    let ctl = Control::new()
        .on_progress(|_| flag.store(true, Ordering::Relaxed))
        .cancel_flag(&flag);
    assert!(matches!(
        EventLog::import_from_path_with(&path, XESImportOptions::default(), &ctl),
        Err(EventLogIOError::Cancelled)
    ));
    flag.store(false, Ordering::Relaxed);
    assert!(matches!(
        EventLog::import_from_bytes_with(&bytes, "xes.gz", XESImportOptions::default(), &ctl),
        Err(EventLogIOError::Cancelled)
    ));
}

/// A reader is not wrapped to count bytes, so the parser itself checks for a cancel.
#[test]
fn a_cancelled_reader_import_fails_with_cancelled() {
    let flag = std::sync::atomic::AtomicBool::new(true);
    let ctl = Control::new().cancel_flag(&flag);
    let file = File::open(get_test_data_path().join("xes").join("RepairExample.xes")).unwrap();
    assert!(matches!(
        EventLog::import_from_reader_with(file, "xes", XESImportOptions::default(), &ctl),
        Err(EventLogIOError::Cancelled)
    ));
}

#[test]
fn test_xes_gz_import() {
    let path = get_test_data_path()
        .join("xes")
        .join("Sepsis Cases - Event Log.xes.gz");
    let log = EventLog::import_from_path(&path).unwrap();

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
    let case_a_first_ev_time = case_a
        .unwrap()
        .events
        .first()
        .unwrap()
        .attributes
        .get_by_key("time:timestamp")
        .unwrap()
        .value
        .try_as_date()
        .unwrap();
    println!("{case_a_first_ev_time:?}");
    assert_eq!(
        case_a_first_ev_time,
        &DateTime::parse_from_rfc3339("2014-10-22T11:15:41.000+02:00")
            .unwrap()
            .fixed_offset()
    );
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
        AttributeValue::Date(DateTime::parse_from_rfc3339("2014-10-22 09:15:41+00:00").unwrap())
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
    let res_gz = EventLog::import_from_path("this-file-does-not-exist.xes.gz");
    assert!(matches!(res_gz, Err(EventLogIOError::Io(_))));
}
#[test]
pub fn test_invalid_xes_non_existing_file() {
    let res_gz = EventLog::import_from_path("this-file-does-not-exist.xes");
    assert!(matches!(res_gz, Err(EventLogIOError::Io(_))));
}

#[test]
pub fn test_invalid_xes_file_gz_expected() {
    // Example XML (non-XES) file; Error should only involve missing .gz headers, so the other  content does not matter
    let path = get_test_data_path()
        .join("petri-net")
        .join("BPI_Challenge_2019_sampled_3000cases_model_alphappp.pnml");
    let mut bytes = Vec::new();
    File::open(&path).unwrap().read_to_end(&mut bytes).unwrap();
    let res_gz = EventLog::import_from_bytes(&bytes, "xes.gz");
    assert!(matches!(res_gz, Err(EventLogIOError::Io(_))));
}

/// Import an uncompressed XES file as `xes.gz`
#[test]
pub fn test_normal_xes_file_gz_expected() {
    let path = get_test_data_path().join("xes").join("RepairExample.xes");
    let mut bytes = Vec::new();
    File::open(&path).unwrap().read_to_end(&mut bytes).unwrap();
    let res_gz = EventLog::import_from_bytes(&bytes, "xes.gz");
    assert!(matches!(res_gz, Err(EventLogIOError::Io(_))));
}

#[test]
pub fn test_invalid_xes_file_gz_unexpected() {
    let path = get_test_data_path()
        .join("xes")
        .join("Sepsis Cases - Event Log.xes.gz");
    let mut bytes = Vec::new();
    File::open(&path).unwrap().read_to_end(&mut bytes).unwrap();
    let res = EventLog::import_from_bytes(&bytes, "xes");
    assert!(matches!(
        res,
        Err(EventLogIOError::Xml(quick_xml::Error::Encoding(_)))
    ));
}

#[test]
pub fn test_invalid_xes_file_zero() {
    // File with ~1MB of zeros (dd if=/dev/zero of=zero.file bs=1024 count=1024)
    let path = get_test_data_path().join("zero.file");
    let mut bytes = Vec::new();
    File::open(&path).unwrap().read_to_end(&mut bytes).unwrap();
    let res = EventLog::import_from_bytes(&bytes, "xes");
    assert!(matches!(
        res,
        Err(EventLogIOError::Xes(XESParseError::NoTopLevelLog))
    ));
}

#[test]
pub fn test_invalid_xes_file_zero_gz() {
    // File with ~1MB of zeros (dd if=/dev/zero of=zero.file bs=1024 count=1024)
    let path = get_test_data_path().join("zero.file");
    let mut bytes = Vec::new();
    File::open(&path).unwrap().read_to_end(&mut bytes).unwrap();
    let res = EventLog::import_from_bytes(&bytes, "xes.gz");
    assert!(matches!(res, Err(EventLogIOError::Io(_))));
}

#[test]
pub fn test_invalid_xes_file_pnml() {
    let path = get_test_data_path()
        .join("petri-net")
        .join("BPI_Challenge_2019_sampled_3000cases_model_alphappp.pnml");
    let res = EventLog::import_from_bytes(&std::fs::read(&path).unwrap(), "xes");
    assert!(matches!(
        res,
        Err(EventLogIOError::Xes(XESParseError::NoTopLevelLog))
    ));
}

#[test]
pub fn test_invalid_xes_file_json() {
    let path = get_test_data_path()
        .join("ocel")
        .join("order-management.json");
    let res = EventLog::import_from_bytes(&std::fs::read(&path).unwrap(), "xes");
    assert!(matches!(
        res,
        Err(EventLogIOError::Xes(XESParseError::NoTopLevelLog))
    ));
}

#[test]
pub fn test_invalid_xes_file_empty() {
    let x: &'static [u8] = &[];
    let res = EventLog::import_from_bytes(x, "xes");
    assert!(matches!(
        res,
        Err(EventLogIOError::Xes(XESParseError::NoTopLevelLog))
    ));
}

#[test]
pub fn test_nested_global_event_attr() {
    let path = get_test_data_path().join("xes").join("small-example.xes");
    let res = EventLog::import_from_path(&path).unwrap();
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

    let path = get_test_data_path().join("xes").join("small-example.xes");
    let res = EventLog::import_from_path(&path).unwrap();

    assert_eq!(
        trace_to_test_id_vec(res.traces.last().unwrap()),
        vec![
            "Second",
            "First",
            "With invalid time:timestamp",
            "With default timestamp",
        ]
    );

    let res_sorted_events = EventLog::import_from_path_with(
        &path,
        XESImportOptions {
            sort_events_with_timestamp_key: Some("time:timestamp".into()),
            ..XESImportOptions::default()
        },
        &Control::new(),
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

#[test]
// Test roundway for nested
pub fn test_xes_nested_attrs() {
    let path = get_test_data_path().join("xes").join("nested-attrs.xes");
    let mut xes_bytes = Vec::new();
    File::open(&path)
        .unwrap()
        .read_to_end(&mut xes_bytes)
        .unwrap();
    let log = EventLog::import_from_bytes(&xes_bytes, "xes").unwrap();
    let out_xes_bytes = log.export_to_bytes("xes").unwrap();
    // The fixture writes list items without `<values>`; the export uses the standard form
    let out_xes = String::from_utf8(out_xes_bytes).unwrap();
    assert_eq!(out_xes.matches("<values>").count(), 3);
    let log2 = import_str(&out_xes).unwrap();
    assert_eq!(log, log2);

    let event = &log.traces[0].events[0];
    let AttributeValue::List(list1) = &event.attributes.get_by_key("list1").unwrap().value else {
        panic!("list1 is not a list");
    };
    let keys: Vec<&str> = list1.iter().map(|a| a.key.as_str()).collect();
    assert_eq!(keys, ["list1:s1", "list1:s2", "list1:s4", "list1:list2"]);
    assert!(matches!(&list1[3].value, AttributeValue::List(l) if l.len() == 2));
}

#[test]
pub fn test_xes_empty_trace_and_event_tags() {
    let xes =
        r#"<log xes.version="2.0"><trace/><trace><event/><event></event></trace><trace/></log>"#;
    let log = import_str(xes).unwrap();
    assert_eq!(log.traces.len(), 3);
    assert!(log.traces[0].events.is_empty());
    assert_eq!(log.traces[1].events.len(), 2);
    assert!(log.traces[1].events.iter().all(|e| e.attributes.is_empty()));
    assert!(log.traces[2].events.is_empty());
}

#[test]
pub fn test_xes_deeply_nested_attributes_are_an_error() {
    use crate::core::event_data::case_centric::xes::stream_xes::MAX_ATTRIBUTE_DEPTH;
    let nested = |depth: usize| {
        format!(
            r#"<log><trace><event>{}{}</event></trace></log>"#,
            r#"<container key="c">"#.repeat(depth),
            "</container>".repeat(depth)
        )
    };
    let log = import_str(&nested(MAX_ATTRIBUTE_DEPTH)).unwrap();
    assert_eq!(log.traces[0].events[0].attributes.len(), 1);
    // Deep enough to overflow the stack when the attribute tree is dropped
    let res = import_str(&nested(1_000_000));
    assert!(
        matches!(
            res,
            Err(EventLogIOError::Xes(XESParseError::AttributeNestingTooDeep))
        ),
        "{res:?}"
    );
}

#[test]
pub fn test_xes_truncated_is_an_error() {
    let complete =
        r#"<log xes.version="2.0"><trace><event><string key="a" value="b"/></event></trace></log>"#;
    assert!(import_str(complete).is_ok());
    for cut in ["</log>", "</trace>", "</event>", "<string"] {
        let truncated = &complete[..complete.find(cut).unwrap()];
        let res = import_str(truncated);
        assert!(
            matches!(
                res,
                Err(EventLogIOError::Xml(quick_xml::Error::IllFormed(
                    quick_xml::errors::IllFormedError::MissingEndTag(_)
                )))
            ),
            "{truncated:?} gave {res:?}"
        );
    }
    let in_attribute = r#"<log><trace><event><container key="c"><string key="a" value="b"/>"#;
    assert!(import_str(in_attribute).is_err());
}

#[test]
pub fn test_xes_gz_multiple_members() {
    use crate::core::event_data::case_centric::xes::stream_xes::stream_xes;
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    let xes = r#"<log xes.version="2.0"><trace><event/></trace><trace><event/></trace></log>"#;
    let (head, tail) = xes.split_at(xes.find("</trace>").unwrap());
    let mut bytes = Vec::new();
    for part in [head, tail] {
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(part.as_bytes()).unwrap();
        bytes.extend(enc.finish().unwrap());
    }
    let log = EventLog::import_from_bytes(&bytes, "xes.gz").unwrap();
    assert_eq!(log.traces.len(), 2);
    let (mut stream, _) =
        stream_xes(bytes.as_slice(), "xes.gz", XESImportOptions::default()).unwrap();
    assert_eq!(stream.count(), 2);
}

#[test]
pub fn test_xes_standard_list_inside_container() {
    let xes = r#"<log xes.version="2.0"><trace><event>
        <container key="c">
            <list key="l">
                <string key="meta" value="m"/>
                <values>
                    <string key="item" value="1"/>
                    <string key="item" value="2"/>
                </values>
            </list>
            <string key="after" value="x"/>
        </container>
        <string key="ev" value="y"/>
    </event></trace></log>"#;
    let log = import_str(xes).unwrap();
    let event = &log.traces[0].events[0];
    let keys: Vec<&str> = event.attributes.iter().map(|a| a.key.as_str()).collect();
    assert_eq!(keys, ["c", "ev"]);
    let AttributeValue::Container(c) = &event.attributes[0].value else {
        panic!("c is not a container");
    };
    let keys: Vec<&str> = c.iter().map(|a| a.key.as_str()).collect();
    assert_eq!(keys, ["l", "after"]);
    let AttributeValue::List(items) = &c[0].value else {
        panic!("l is not a list");
    };
    assert_eq!(items.len(), 2);
    let meta = c[0].own_attributes.as_ref().unwrap();
    assert_eq!(meta.len(), 1);
    assert_eq!(meta[0].key, "meta");

    let out = String::from_utf8(log.export_to_bytes("xes").unwrap()).unwrap();
    assert!(out.contains(r#"<list key="l"><string key="meta" value="m"/><values>"#));
    assert_eq!(import_str(&out).unwrap(), log);
}

#[test]
pub fn test_xes_ignored_attribute_children_are_skipped() {
    use crate::core::event_data::case_centric::xes::import_xes::build_ignore_attributes;

    let xes = r#"<log xes.version="2.0"><trace><event>
        <string key="drop" value="1"><string key="child" value="2"/></string>
        <string key="keep" value="3"/>
    </event></trace></log>"#;
    let options = XESImportOptions {
        ignore_event_attributes_except: Some(build_ignore_attributes(["keep"])),
        ..XESImportOptions::default()
    };
    let log = import_str_with(xes, options).unwrap();
    let keys: Vec<&str> = log.traces[0].events[0]
        .attributes
        .iter()
        .map(|a| a.key.as_str())
        .collect();
    assert_eq!(keys, ["keep"]);
}

#[test]
pub fn test_get_event_attribute_falls_back_to_global_event_attrs() {
    let xes = r#"<log xes.version="2.0">
        <global scope="trace"><string key="k" value="trace-default"/></global>
        <global scope="event"><string key="k" value="event-default"/></global>
        <trace><event/></trace></log>"#;
    let log = import_str(xes).unwrap();
    let event = &log.traces[0].events[0];
    assert_eq!(
        log.get_event_attribute(event, "k").unwrap().value,
        AttributeValue::String("event-default".to_string())
    );
}

#[test]
pub fn test_xes_global_scope_defaults_to_event() {
    let xes = r#"<log xes.version="2.0">
        <global><string key="concept:name" value="__INVALID__"/></global>
        <global scope="trace"/>
        <classifier name="Activity" keys="concept:name"/>
        <trace/></log>"#;
    let log = import_str(xes).unwrap();
    assert_eq!(log.global_event_attrs.as_ref().unwrap().len(), 1);
    assert!(log.global_trace_attrs.is_none_or(|a| a.is_empty()));
    assert!(log.attributes.is_empty());
}

#[test]
pub fn test_xes_value_lexical_forms() {
    let xes = r#"<log xes.version="2.0"><trace><event>
        <boolean key="b1" value="1"/><boolean key="b0" value="0"/>
        <boolean key="bt" value=" true "/><boolean key="bT" value="True"/>
        <boolean key="bx" value="yes"/>
        <int key="i" value=" 42 "/><int key="ix" value="1.0"/>
        <int key="iover" value="9223372036854775808"/>
        <float key="f" value=" -INF"/><float key="fx" value="abc"/>
        <date key="d" value=" 2020-01-01T10:00:00Z "/>
        <id key="idx" value="not-a-uuid"/>
        <string key="s" value=" kept "/>
    </event></trace></log>"#;
    let log = import_str(xes).unwrap();
    let ev = &log.traces[0].events[0];
    let v = |k: &str| ev.attributes.get_by_key(k).unwrap().value.clone();
    assert_eq!(v("b1"), AttributeValue::Boolean(true));
    assert_eq!(v("b0"), AttributeValue::Boolean(false));
    assert_eq!(v("bt"), AttributeValue::Boolean(true));
    assert_eq!(v("bT"), AttributeValue::Boolean(true));
    assert_eq!(v("i"), AttributeValue::Int(42));
    assert_eq!(v("f"), AttributeValue::Float(f64::NEG_INFINITY));
    assert_eq!(
        v("d"),
        AttributeValue::Date(DateTime::parse_from_rfc3339("2020-01-01T10:00:00Z").unwrap())
    );
    assert_eq!(v("s"), AttributeValue::String(" kept ".to_string()));
    for k in ["bx", "ix", "iover", "fx", "idx"] {
        assert_eq!(v(k), AttributeValue::None(), "{k}");
    }
}

#[test]
pub fn test_xes_dates_use_the_shared_lenient_parser() {
    let xes = r#"<log xes.version="2.0"><trace><event>
        <date key="time:timestamp" value="2023-10-06"/>
        <date key="offset" value="2023-10-06 09:30:21+0200"/>
        <date key="colon" value="2023-10-06T09:30:21:5+02:00"/>
        <date key="custom" value="06/10/2023 09:30"/>
    </event></trace></log>"#;
    let options = XESImportOptions {
        date_format: Some("%d/%m/%Y %H:%M".to_string()),
        ..Default::default()
    };
    let log = import_str_with(xes, options).unwrap();
    let ev = &log.traces[0].events[0];
    let date = |k: &str| {
        *ev.attributes
            .get_by_key(k)
            .unwrap()
            .value
            .try_as_date()
            .unwrap()
    };
    let rfc = |s: &str| DateTime::parse_from_rfc3339(s).unwrap();
    assert_eq!(date("time:timestamp"), rfc("2023-10-06T00:00:00Z"));
    assert_eq!(date("offset"), rfc("2023-10-06T09:30:21+02:00"));
    assert_eq!(date("colon"), rfc("2023-10-06T09:30:21.5+02:00"));
    assert_eq!(date("custom"), rfc("2023-10-06T09:30:00Z"));
}

#[test]
pub fn test_xes_format_names() {
    use crate::core::event_data::case_centric::io::EventLogFormat;
    for (name, gz) in [
        ("xes", false),
        ("XES", false),
        (".xes", false),
        ("xes.gz", true),
        ("XES.GZ", true),
    ] {
        assert_eq!(
            EventLogFormat::parse(name).unwrap(),
            EventLogFormat::Xes { gz }
        );
        assert_eq!(EventLogFormat::parse_xes_gz(name).unwrap(), gz);
    }
    for name in ["gz", "myxes", "xes.zip", "json"] {
        assert!(matches!(
            EventLogFormat::parse_xes_gz(name),
            Err(EventLogIOError::UnsupportedFormat(_))
        ));
    }
}

#[test]
pub fn test_xes_log_level_events_are_skipped() {
    let xes = r#"<log xes.version="2.0"><trace><event/></trace>
        <event><string key="a" value="b"><string key="meta" value="c"/></string></event>
    </log>"#;
    let log = import_str(xes).unwrap();
    assert_eq!(log.traces.len(), 1);
    assert_eq!(log.traces[0].events.len(), 1);
}

#[test]
pub fn test_xes_export_float_spelling() {
    use crate::core::event_data::case_centric::event_log_struct::{Attribute, Event, EventLog};
    let floats = [f64::INFINITY, f64::NEG_INFINITY, 1e300, -0.0, 0.1];
    let mut log = EventLog::default();
    log.traces.push(Trace {
        attributes: vec![],
        events: vec![Event {
            attributes: floats
                .iter()
                .enumerate()
                .map(|(i, f)| Attribute::new(i.to_string(), AttributeValue::Float(*f)))
                .collect(),
        }],
    });
    let out = String::from_utf8(log.export_to_bytes("xes").unwrap()).unwrap();
    for s in [
        r#"value="INF""#,
        r#"value="-INF""#,
        r#"value="1e300""#,
        r#"value="-0.0""#,
        r#"value="0.1""#,
    ] {
        assert!(out.contains(s), "{s} not in {out}");
    }
    assert_eq!(import_str(&out).unwrap().traces, log.traces);
}

#[cfg(target_os = "linux")]
#[test]
pub fn test_xes_export_reports_write_errors_at_flush() {
    use crate::core::event_data::case_centric::xes::{export_xes, XESOuterLogData};

    // Small enough to sit in the write buffer until the final flush; writes to /dev/full fail
    // with ENOSPC
    let log = import_str(r#"<log xes.version="2.0"><trace><event/></trace></log>"#).unwrap();
    for format in ["xes", "xes.gz"] {
        let full = File::create("/dev/full").unwrap();
        let header = XESOuterLogData::default();
        assert!(export_xes(full, format, &header, log.traces.iter()).is_err());
    }
    for format in ["xes", "xes.gz", "json"] {
        assert!(log.export_to_path_as("/dev/full", format, ()).is_err());
    }
}

#[test]
fn malformed_attribute_warns_through_control() {
    use std::sync::Mutex;
    let xes =
        r#"<log xes.version="2.0"><trace><event><int key="n" value="x"/></event></trace></log>"#;
    let seen = Mutex::new(Vec::new());
    let ctl = Control::new().on_warning(|w| seen.lock().unwrap().push(w.to_string()));
    let log =
        EventLog::import_from_bytes_with(xes.as_bytes(), "xes", Default::default(), &ctl).unwrap();
    assert_eq!(log.traces.len(), 1);
    drop(ctl);
    let seen = seen.into_inner().unwrap();
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(seen[0].contains("int"), "{seen:?}");
}
