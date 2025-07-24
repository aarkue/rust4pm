use super::{
    event_log_struct::{EventLogClassifier, EventLogExtension},
    stream_xes::XESOuterLogData,
    Attribute, AttributeValue, Attributes, Trace,
};
use crate::{utils::xml_utils::XMLWriterWrapper, EventLog};
use flate2::{write::GzEncoder, Compression};
use quick_xml::{events::BytesDecl, Writer};
use std::{
    borrow::Borrow,
    fs::File,
    io::{BufWriter, Write},
};
const OK: Result<(), std::io::Error> = Ok(());

///
/// Export XES (from log data and an iterator over traces) to a XML writer
///
pub fn export_xes<'a, 'b, W, T: Borrow<Trace>, I>(
    writer: impl Into<XMLWriterWrapper<'b, W>>,
    log_extensions: &'a Option<&'a Vec<EventLogExtension>>,
    log_global_trace_attrs: &'a Option<&'a Attributes>,
    log_global_event_attrs: &'a Option<&'a Attributes>,
    log_classifiers: &'a Option<&'a Vec<EventLogClassifier>>,
    log_attributes: &'a Attributes,
    traces: I,
) -> Result<(), quick_xml::Error>
where
    I: Iterator<Item = T>,
    W: Write + 'b,
{
    let mut xml_writer = writer.into();
    let writer: &mut Writer<_> = xml_writer.to_xml_writer();
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
            // Extensions
            if let Some(extensions) = log_extensions {
                for ext in extensions.iter() {
                    w.create_element("extension")
                        .with_attributes(vec![
                            ("name", ext.name.as_str()),
                            ("prefix", ext.prefix.as_str()),
                            ("uri", ext.uri.as_str()),
                        ])
                        .write_empty()?;
                }
            }
            // Global trace attributes
            if let Some(global_trace_attrs) = log_global_trace_attrs {
                w.create_element("global")
                    .with_attribute(("scope", "trace"))
                    .write_inner_content(|w| {
                        for a in global_trace_attrs.iter() {
                            write_xes_attribute(w, a)?;
                        }
                        OK
                    })?;
            }
            // Global event attributes
            if let Some(global_event_attrs) = log_global_event_attrs {
                w.create_element("global")
                    .with_attribute(("scope", "event"))
                    .write_inner_content(|w| {
                        for a in global_event_attrs.iter() {
                            write_xes_attribute(w, a)?;
                        }
                        OK
                    })?;
            }
            // Classifiers
            if let Some(classifiers) = log_classifiers {
                for cl in classifiers.iter() {
                    w.create_element("classifier")
                        .with_attributes(vec![
                            ("name", cl.name.as_str()),
                            ("keys", &serialize_classifier(&cl.keys)),
                        ])
                        .write_empty()?;
                }
            }
            // Log attributes
            for a in log_attributes {
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
    Ok(())
}

fn write_xes_attribute<T>(w: &mut Writer<T>, a: &Attribute) -> Result<(), std::io::Error>
where
    T: Write,
{
    let (tag_name, value_opt): (&str, Option<String>) = match &a.value {
        AttributeValue::String(s) => ("string", Some(s.clone())),
        AttributeValue::Date(d) => ("date", Some(d.to_rfc3339())),
        AttributeValue::Int(i) => ("int", Some(i.to_string())),
        AttributeValue::Float(f) => ("float", Some(f.to_string())),
        AttributeValue::Boolean(b) => ("boolean", Some(b.to_string())),
        AttributeValue::ID(id) => ("id", Some(id.to_string())),
        AttributeValue::List(_) => ("list", None),
        AttributeValue::Container(_) => ("container", None),
        AttributeValue::None() => ("string", None),
    };
    let e = match value_opt {
        Some(value) => w
            .create_element(tag_name)
            .with_attributes(vec![("key", a.key.as_str()), ("value", &value)]),
        None => w
            .create_element(tag_name)
            .with_attribute(("key", a.key.as_str())),
    };
    if let AttributeValue::List(c) = &a.value {
        e.write_inner_content(|inner_w| {
            for attr in c {
                write_xes_attribute(inner_w, attr)?;
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

///
/// Export an [`EventLog`] to a writer
///
/// Both [`quick_xml::Writer`] as well as [`std::io::Write`] are accepted
//
pub fn export_xes_event_log<'a, W>(
    writer: impl Into<XMLWriterWrapper<'a, W>>,
    log: &'a EventLog,
) -> Result<(), quick_xml::Error>
where
    W: Write + 'a,
{
    export_xes(
        writer,
        &log.extensions.as_ref(),
        &log.global_trace_attrs.as_ref(),
        &log.global_event_attrs.as_ref(),
        &log.classifiers.as_ref(),
        &log.attributes,
        log.traces.iter(),
    )
}

/// Export an [`EventLog`] to a [`File`]
pub fn export_xes_event_log_to_file(
    log: &EventLog,
    file: File,
    compress_gz: bool,
) -> Result<(), quick_xml::Error> {
    if compress_gz {
        let encoder = GzEncoder::new(BufWriter::new(file), Compression::fast());
        return export_xes_event_log(&mut Writer::new(BufWriter::new(encoder)), log);
    }
    export_xes_event_log(&mut Writer::new(BufWriter::new(file)), log)
}

/// Export an [`EventLog`] to a filepath
///
/// Automatically selects gz-compression if filepath ends with `.gz`
///
/// See also [`export_xes_event_log_to_file`], which accepts a [`File`] and boolean flag for gz-compression.
pub fn export_xes_event_log_to_file_path<P: AsRef<std::path::Path>>(
    log: &EventLog,
    path: P,
) -> Result<(), quick_xml::Error> {
    let is_gz = path
        .as_ref()
        .as_os_str()
        .to_str()
        .is_some_and(|p| p.ends_with(".gz"));
    let file = File::create(path)?;
    export_xes_event_log_to_file(log, file, is_gz)
}

/// Export a trace stream (i.e., [`Iterator`] over [`Trace`]) and [`XESOuterLogData`] to a XML [`Writer`]
pub fn export_xes_trace_stream<'a, W, T: Borrow<Trace>, I>(
    writer: impl Into<XMLWriterWrapper<'a, W>>,
    trace_stream: I,
    log_data: XESOuterLogData,
) -> Result<(), quick_xml::Error>
where
    W: Write + 'a,
    I: Iterator<Item = T>,
{
    export_xes(
        writer,
        &Some(log_data.extensions.as_ref()),
        &Some(log_data.global_trace_attrs.as_ref()),
        &Some(log_data.global_event_attrs.as_ref()),
        &Some(log_data.classifiers.as_ref()),
        &log_data.log_attributes,
        trace_stream,
    )
}

/// Export a trace stream (i.e., [`Iterator`] over [`Trace`]) and [`XESOuterLogData`] to a [`File`]
///
/// If `compress_gz` is `true`, the XES will be compressed to a `.xes.gz` file before writing to file
pub fn export_xes_trace_stream_to_file<T: Borrow<Trace>, I>(
    trace_stream: I,
    log_data: XESOuterLogData,
    file: File,
    compress_gz: bool,
) -> Result<(), quick_xml::Error>
where
    I: Iterator<Item = T>,
{
    if compress_gz {
        let encoder = GzEncoder::new(BufWriter::new(file), Compression::fast());
        return export_xes_trace_stream(
            &mut Writer::new(BufWriter::new(encoder)),
            trace_stream,
            log_data,
        );
    }
    export_xes_trace_stream(
        &mut Writer::new(BufWriter::new(file)),
        trace_stream,
        log_data,
    )
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
    use std::{collections::HashSet, fs::File, io::BufWriter, time::Instant};

    use quick_xml::Writer;

    use crate::{
        event_log::{
            event_log_struct::EventLogExtension,
            export_xes::{export_xes_event_log, serialize_classifier},
            stream_xes::{parse_classifier_key, XESOuterLogData},
        },
        import_xes_file, stream_xes_from_path,
        utils::test_utils::get_test_data_path,
        XESImportOptions,
    };

    use super::export_xes_trace_stream_to_file;

    #[test]
    fn test_xes_export_std_writer() {
        let path = get_test_data_path()
            .join("xes")
            .join("Sepsis Cases - Event Log.xes.gz");
        let log = import_xes_file(&path, XESImportOptions::default()).unwrap();
        let exported_xes_data: Vec<u8> = Vec::new();
        // let mut writer = Writer::new(exported_xes_data);
        let mut buf_writer = BufWriter::new(exported_xes_data);
        export_xes_event_log(&mut buf_writer, &log).unwrap();
        let data = buf_writer.into_inner().unwrap();
        let log2 = crate::import_xes_slice(&data, false, XESImportOptions::default()).unwrap();
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

    #[test]
    fn test_xes_export_xml_writer() {
        let path = get_test_data_path()
            .join("xes")
            .join("Sepsis Cases - Event Log.xes.gz");
        let log = import_xes_file(&path, XESImportOptions::default()).unwrap();
        let exported_xes_data: Vec<u8> = Vec::new();
        let mut writer = Writer::new(exported_xes_data);
        export_xes_event_log(&mut writer, &log).unwrap();
        let data = writer.into_inner();
        let log2 =
            crate::import_xes_slice(&data, false, crate::XESImportOptions::default()).unwrap();
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

    #[test]
    fn test_stream_from_gz_to_plain() {
        let now = Instant::now();
        let path = get_test_data_path()
            .join("xes")
            .join("Road_Traffic_Fine_Management_Process.xes.gz");

        let (mut stream, mut log_data) =
            stream_xes_from_path(&path, XESImportOptions::default()).unwrap();
        let export_path = get_test_data_path()
            .join("export")
            .join("streaming-export.xes.gz");

        let traces = stream.map(|mut t| {
            for a in t.attributes.iter_mut() {
                a.key = a.key.to_uppercase().to_string().to_string();
            }
            for e in t.events.iter_mut() {
                for a in e.attributes.iter_mut() {
                    a.key = a.key.to_uppercase().to_string().to_string();
                }
            }
            t
        });
        for a in log_data.global_trace_attrs.iter_mut() {
            a.key = a.key.to_uppercase().to_string().to_string();
        }

        for a in log_data.global_event_attrs.iter_mut() {
            a.key = a.key.to_uppercase().to_string().to_string();
        }
        for a in log_data.log_attributes.iter_mut() {
            a.key = a.key.to_uppercase().to_string().to_string();
        }
        for c in log_data.classifiers.iter_mut() {
            for k in c.keys.iter_mut() {
                *k = k.to_uppercase().to_string().to_string();
            }
        }

        export_xes_trace_stream_to_file(
            traces,
            log_data,
            File::create(&export_path).unwrap(),
            true,
        )
        .unwrap();
        println!("Streamed from .xes.gz to .xes.gz in {:?}", now.elapsed());
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
