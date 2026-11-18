use crate::core::{
    event_data::case_centric::{
        event_log_struct::{
            Attribute, AttributeValue, Attributes, EventLog, EventLogClassifier, EventLogExtension,
            Trace,
        },
        io::{EventLogFormat, EventLogIOError},
        xes::stream_xes::XESOuterLogData,
    },
    io::xml_chars,
};
use flate2::{write::GzEncoder, Compression};
use quick_xml::{events::BytesDecl, Writer};
use std::{
    borrow::Borrow,
    io::{BufWriter, Write},
};
const OK: Result<(), std::io::Error> = Ok(());

/// The log-level part of an XES file, borrowed from an [`EventLog`] or [`XESOuterLogData`]
pub(crate) struct XesHeader<'a> {
    extensions: &'a [EventLogExtension],
    classifiers: &'a [EventLogClassifier],
    global_trace_attrs: &'a [Attribute],
    global_event_attrs: &'a [Attribute],
    log_attributes: &'a Attributes,
}

impl<'a> XesHeader<'a> {
    pub(crate) fn of(log: &'a EventLog) -> Self {
        Self {
            extensions: log.extensions.as_deref().unwrap_or_default(),
            classifiers: log.classifiers.as_deref().unwrap_or_default(),
            global_trace_attrs: log.global_trace_attrs.as_deref().unwrap_or_default(),
            global_event_attrs: log.global_event_attrs.as_deref().unwrap_or_default(),
            log_attributes: &log.attributes,
        }
    }
}

impl<'a> From<&'a XESOuterLogData> for XesHeader<'a> {
    fn from(data: &'a XESOuterLogData) -> Self {
        Self {
            extensions: &data.extensions,
            classifiers: &data.classifiers,
            global_trace_attrs: &data.global_trace_attrs,
            global_event_attrs: &data.global_event_attrs,
            log_attributes: &data.log_attributes,
        }
    }
}

/// Export XES (from log data and traces) to `writer` in `format` (`xes` or `xes.gz`, in any case)
///
/// Writes the traces as they come, so they can be streamed, e.g., from
/// [`stream_xes`](super::stream_xes::stream_xes). Empty global attribute lists are left out.
/// Flushes `writer` at the end.
pub fn export_xes<T: Borrow<Trace>>(
    writer: impl Write,
    format: &str,
    log_data: &XESOuterLogData,
    traces: impl IntoIterator<Item = T>,
) -> Result<(), EventLogIOError> {
    let gz = EventLogFormat::parse_xes_gz(format)?;
    write_xes(writer, gz, log_data.into(), traces)
}

/// Write XES to `writer`, gzipped if `gz` is set, and flush it
pub(crate) fn write_xes<T: Borrow<Trace>>(
    mut writer: impl Write,
    gz: bool,
    header: XesHeader<'_>,
    traces: impl IntoIterator<Item = T>,
) -> Result<(), EventLogIOError> {
    if gz {
        let mut encoder = GzEncoder::new(writer, Compression::fast());
        write_xes_xml(&mut encoder, header, traces)?;
        encoder.finish()?.flush()?;
    } else {
        write_xes_xml(&mut writer, header, traces)?;
        writer.flush()?;
    }
    Ok(())
}

fn write_xes_xml<T: Borrow<Trace>>(
    writer: impl Write,
    header: XesHeader<'_>,
    traces: impl IntoIterator<Item = T>,
) -> Result<(), EventLogIOError> {
    // Buffered, as the XML writer writes each tag and attribute separately
    let mut buffered = BufWriter::new(writer);
    let mut writer = Writer::new(&mut buffered);
    writer.write_event(quick_xml::events::Event::Decl(BytesDecl::new(
        "1.0",
        Some("UTF-8"),
        None,
    )))?;
    writer
        .create_element("log")
        .with_attributes(vec![
            ("xes.version", "2.0"),
            // nested-attributes are not always present, but they might be so let's just say we use them
            ("xes.features", "nested-attributes"),
            ("xmlns", "http://www.xes-standard.org/"),
        ])
        .write_inner_content(|w| {
            for ext in header.extensions {
                w.create_element("extension")
                    .with_attributes(vec![
                        ("name", xml_chars(&ext.name).as_ref()),
                        ("prefix", xml_chars(&ext.prefix).as_ref()),
                        ("uri", xml_chars(&ext.uri).as_ref()),
                    ])
                    .write_empty()?;
            }
            for (scope, attrs) in [
                ("trace", header.global_trace_attrs),
                ("event", header.global_event_attrs),
            ] {
                if attrs.is_empty() {
                    continue;
                }
                w.create_element("global")
                    .with_attribute(("scope", scope))
                    .write_inner_content(|w| {
                        for a in attrs {
                            write_xes_attribute(w, a)?;
                        }
                        OK
                    })?;
            }
            for cl in header.classifiers {
                w.create_element("classifier")
                    .with_attributes(vec![
                        ("name", xml_chars(&cl.name).as_ref()),
                        ("keys", xml_chars(&serialize_classifier(&cl.keys)).as_ref()),
                    ])
                    .write_empty()?;
            }
            for a in header.log_attributes {
                write_xes_attribute(w, a)?;
            }
            for t in traces {
                w.create_element("trace").write_inner_content(|w| {
                    for a in &t.borrow().attributes {
                        write_xes_attribute(w, a)?;
                    }
                    for e in &t.borrow().events {
                        w.create_element("event").write_inner_content(|w| {
                            for a in &e.attributes {
                                write_xes_attribute(w, a)?;
                            }
                            OK
                        })?;
                    }
                    OK
                })?;
            }
            OK
        })?;
    // Not flushing the inner writer, which a gz encoder would answer with a sync block
    buffered.into_inner().map_err(|e| e.into_error())?;
    Ok(())
}

fn write_xes_attribute<T>(w: &mut Writer<T>, a: &Attribute) -> Result<(), std::io::Error>
where
    T: Write,
{
    let (tag_name, value_opt): (&str, Option<String>) = match &a.value {
        AttributeValue::String(s) => ("string", Some(xml_chars(s).into_owned())),
        AttributeValue::Date(d) => ("date", Some(d.to_rfc3339())),
        AttributeValue::Int(i) => ("int", Some(i.to_string())),
        AttributeValue::Float(f) => ("float", Some(format_xs_double(*f))),
        AttributeValue::Boolean(b) => ("boolean", Some(b.to_string())),
        AttributeValue::ID(id) => ("id", Some(id.to_string())),
        AttributeValue::List(_) => ("list", None),
        AttributeValue::Container(_) => ("container", None),
        AttributeValue::None() => ("string", None),
    };
    let key = xml_chars(&a.key);
    let e = match value_opt {
        Some(value) => w
            .create_element(tag_name)
            .with_attributes(vec![("key", key.as_ref()), ("value", &value)]),
        None => w
            .create_element(tag_name)
            .with_attribute(("key", key.as_ref())),
    };
    if let AttributeValue::List(c) = &a.value {
        // XES standard: meta-attributes first, then the items wrapped in `<values>`
        e.write_inner_content(|inner_w| {
            for own_attr in a.own_attributes.iter().flatten() {
                write_xes_attribute(inner_w, own_attr)?;
            }
            let values = inner_w.create_element("values");
            if c.is_empty() {
                values.write_empty()?;
            } else {
                values.write_inner_content(|values_w| {
                    for attr in c {
                        write_xes_attribute(values_w, attr)?;
                    }
                    OK
                })?;
            }
            OK
        })?;
    } else if let AttributeValue::Container(c) = &a.value {
        e.write_inner_content(|inner_w| {
            for attr in c {
                write_xes_attribute(inner_w, attr)?;
            }
            OK
        })?;
    } else if let Some(own_nested_attrs) = &a.own_attributes {
        e.write_inner_content(|inner_w| {
            for own_attr in own_nested_attrs {
                write_xes_attribute(inner_w, own_attr)?;
            }
            OK
        })?;
    } else {
        e.write_empty()?;
    }

    OK
}

/// `f` as xs:double, which spells infinity `INF` and takes `1e300` instead of 301 digits
fn format_xs_double(f: f64) -> String {
    match f {
        f64::INFINITY => "INF".to_string(),
        f64::NEG_INFINITY => "-INF".to_string(),
        _ => format!("{f:?}"),
    }
}

fn serialize_classifier(classifier_keys: &[String]) -> String {
    let should_quote = classifier_keys.iter().any(|key| key.contains(' '));
    if should_quote {
        classifier_keys
            .iter()
            .map(|k| format!("'{k}'"))
            .collect::<Vec<String>>()
            .join(" ")
    } else {
        classifier_keys.join(" ")
    }
}

#[cfg(test)]
mod export_xes_tests {
    use std::{collections::HashSet, fs::File, time::Instant};

    use crate::{
        core::event_data::case_centric::{
            event_log_struct::EventLogExtension,
            xes::{
                export_xes::{export_xes, serialize_classifier},
                import_xes::XESImportOptions,
                stream_xes::{parse_classifier_key, stream_xes, XESOuterLogData},
            },
        },
        test_utils::get_test_data_path,
        EventLog, Exportable, Importable,
    };

    #[test]
    fn test_xes_export_roundtrip() {
        let path = get_test_data_path()
            .join("xes")
            .join("Sepsis Cases - Event Log.xes.gz");
        let log = EventLog::import_from_path(&path).unwrap();
        for format in ["xes", "xes.gz"] {
            let data = log.export_to_bytes(format).unwrap();
            let log2 = EventLog::import_from_bytes(&data, format).unwrap();
            assert_eq!(log.traces.len(), log2.traces.len());
            assert_eq!(log.attributes.len(), log2.attributes.len());
            assert_eq!(
                log.classifiers
                    .as_ref()
                    .map(|c| c.len())
                    .unwrap_or_default(),
                log2.classifiers
                    .as_ref()
                    .map(|c| c.len())
                    .unwrap_or_default(),
            );
            assert_eq!(
                log.extensions
                    .as_ref()
                    .unwrap()
                    .iter()
                    .collect::<HashSet<&EventLogExtension>>(),
                log2.extensions
                    .as_ref()
                    .unwrap()
                    .iter()
                    .collect::<HashSet<&EventLogExtension>>()
            );

            // The below assumes that also all orders of events, traces, log attributes, extensions etc. must be the same
            // In reality, we would also accept a weaker equality relation (e.g., ignoring the order of attributes)
            assert!(log2 == log);
        }
    }

    #[test]
    fn stream_export_matches_log_export() {
        let log = crate::event_log!(["a", "b"], ["c"]);
        let bytes = log.export_to_bytes("xes").unwrap();
        let (mut stream, log_data) =
            stream_xes(bytes.as_slice(), "xes", XESImportOptions::default()).unwrap();
        let mut streamed = Vec::new();
        export_xes(&mut streamed, "xes", &log_data, &mut stream).unwrap();
        assert!(stream.error.is_none());
        assert_eq!(streamed, bytes);
        assert!(!String::from_utf8(streamed).unwrap().contains("<global"));
        assert!(matches!(
            export_xes(Vec::new(), "json", &log_data, log.traces.iter()),
            Err(crate::core::event_data::case_centric::io::EventLogIOError::UnsupportedFormat(_))
        ));
    }

    #[test]
    fn test_stream_from_gz_to_gz() {
        let now = Instant::now();
        let path = get_test_data_path()
            .join("xes")
            .join("Road_Traffic_Fine_Management_Process.xes.gz");

        let (mut stream, mut log_data) = stream_xes(
            File::open(&path).unwrap(),
            "xes.gz",
            XESImportOptions::default(),
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let export_path = dir.path().join("streaming-export.xes.gz");

        let traces = stream.map(|mut t| {
            for a in t.attributes.iter_mut() {
                a.key = a.key.to_uppercase();
            }
            for e in t.events.iter_mut() {
                for a in e.attributes.iter_mut() {
                    a.key = a.key.to_uppercase();
                }
            }
            t
        });
        for a in log_data.global_trace_attrs.iter_mut() {
            a.key = a.key.to_uppercase();
        }

        for a in log_data.global_event_attrs.iter_mut() {
            a.key = a.key.to_uppercase();
        }
        for a in log_data.log_attributes.iter_mut() {
            a.key = a.key.to_uppercase();
        }
        for c in log_data.classifiers.iter_mut() {
            for k in c.keys.iter_mut() {
                *k = k.to_uppercase();
            }
        }

        export_xes(
            File::create(&export_path).unwrap(),
            "xes.gz",
            &log_data,
            traces,
        )
        .unwrap();
        println!("Streamed from .xes.gz to .xes.gz in {:?}", now.elapsed());
        let back = EventLog::import_from_path(&export_path).unwrap();
        assert_eq!(back.traces.len(), 150_370);
    }

    #[test]
    fn invalid_xml_chars_are_dropped() {
        let log = crate::event_log!(
            {"log\u{1}" => "l\u{FFFE}"};
            ["a\u{1}b\u{FFFE}"; {"k\u{1}" => "v\u{FFFE}"}]
        );
        let data = log.export_to_bytes("xes").unwrap();
        crate::core::io::assert_xml_well_formed(&data);
        let back = EventLog::import_from_bytes(&data, "xes").unwrap();
        let expected = crate::event_log!(
            {"log" => "l"};
            ["ab"; {"k" => "v"}]
        );
        assert_eq!(back.attributes, expected.attributes);
        assert_eq!(back.traces, expected.traces);
    }

    #[test]
    fn test_classifier_serialization() {
        // Basic tests
        assert_eq!(
            serialize_classifier(
                &["testing", "123"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            ),
            "testing 123".to_string()
        );
        assert_eq!(
            serialize_classifier(
                &["testing 123"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            ),
            "'testing 123'".to_string()
        );

        // Round-trip test (together with parse_classifier_key)
        let test_keys: Vec<Vec<String>> = vec![
            vec!["test", "key", "without", "quotes"]
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
            vec!["test", "key", "with some", "quotes"]
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
            parse_classifier_key(
                "'testing 123' test key single test koo naa aaa bbb ccc ddd aaa bbb ccc dd was"
                    .to_string(),
                &XESOuterLogData::default(),
            ),
        ];
        for keys in test_keys {
            assert_eq!(
                keys,
                parse_classifier_key(serialize_classifier(&keys), &XESOuterLogData::default())
            );
        }
    }
}
