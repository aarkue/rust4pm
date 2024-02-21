use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::str::FromStr;

use crate::event_log::event_log_struct::{
    Attribute, AttributeAddable, AttributeValue, Attributes, Event, EventLog, EventLogClassifier,
    EventLogExtension, Trace,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use flate2::bufread::GzDecoder;
use quick_xml::escape::unescape;
use quick_xml::events::BytesStart;
use quick_xml::Error as QuickXMLError;
use quick_xml::Reader;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

///
/// Current Parsing Mode (i.e., which tag is currently open / being parsed)
///
#[derive(Clone, Copy, Debug)]
enum Mode {
    Trace,
    Event,
    Attribute,
    Global,
    Log,
    None,
}

#[derive(Debug)]
///
/// Error encountered while parsing XES
///
pub enum XESParseError {
    AttributeOutsideLog(),
    NoTopLevelLog(),
    MissingLastEvent(),
    MissingLastTrace(),
    IOError(std::io::Error),
    XMLParsingError(QuickXMLError),
}

impl From<std::io::Error> for XESParseError {
    fn from(e: std::io::Error) -> Self {
        Self::IOError(e)
    }
}

impl From<QuickXMLError> for XESParseError {
    fn from(e: QuickXMLError) -> Self {
        Self::XMLParsingError(e)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
///
///
/// Options for XES Import
///
/// See also [build_ignore_attributes] for easy construction of attributes set to not ignore
pub struct XESImportOptions {
    pub ignore_log_attributes_except: Option<HashSet<Vec<u8>>>,
    pub ignore_trace_attributes_except: Option<HashSet<Vec<u8>>>,
    pub ignore_event_attributes_except: Option<HashSet<Vec<u8>>>,
    pub date_format: Option<String>,
}
///
/// Construct a `HashSet<Vec<u8>>` from a _collection_ of String, &str, ...
///
/// Example usage: `XESImportOptions::build_ignore_attributes(vec!["concept:name"])`
///
pub fn build_ignore_attributes<I, S: AsRef<str>>(keys: I) -> HashSet<Vec<u8>>
where
    I: IntoIterator<Item = S>,
{
    keys.into_iter()
        .map(|s| s.as_ref().as_bytes().clone().to_vec())
        .collect()
}

///
/// Import a XES [EventLog] from a [Reader]
///
pub fn import_xes<T>(
    reader: &mut Reader<T>,
    options: XESImportOptions,
) -> Result<EventLog, XESParseError>
where
    T: BufRead,
{
    reader.trim_text(true);
    let mut buf: Vec<u8> = Vec::new();

    let mut current_mode: Mode = Mode::Log;
    let mut last_mode_before_attr: Mode = Mode::Log;

    let mut log = EventLog {
        attributes: Attributes::new(),
        traces: Vec::new(),
        extensions: Some(Vec::new()),
        classifiers: Some(Vec::new()),
    };
    let mut encountered_log = false;
    let mut current_nested_attributes: Vec<Attribute> = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(r) => {
                match r {
                    quick_xml::events::Event::Start(t) => match t.name().as_ref() {
                        b"trace" => {
                            current_mode = Mode::Trace;
                            log.traces.push(Trace {
                                attributes: Attributes::new(),
                                events: Vec::new(),
                            });
                        }
                        b"event" => {
                            current_mode = Mode::Event;
                            match log.traces.last_mut() {
                                Some(t) => {
                                    t.events.push(Event {
                                        attributes: Attributes::new(),
                                    });
                                }
                                None => {
                                    eprintln!("Invalid XES format: Event without trace")
                                }
                            }
                        }
                        b"global" => {
                            match current_mode {
                                Mode::Global => {}
                                Mode::Attribute => {}
                                m => {
                                    last_mode_before_attr = m;
                                }
                            }
                            current_mode = Mode::Global;
                        }
                        b"log" => {
                            encountered_log = true;
                            current_mode = Mode::Log
                        }
                        _x => {
                            if !encountered_log {
                                return Err(XESParseError::NoTopLevelLog());
                            }
                            {
                                // Nested attribute!
                                let (key, value) =
                                    parse_attribute_from_tag(&t, current_mode, &options);
                                if !(key.is_empty() && matches!(value, AttributeValue::None())) {
                                    current_nested_attributes.push(Attribute {
                                        key,
                                        value,
                                        own_attributes: Some(Attributes::new()),
                                    });
                                    match current_mode {
                                        Mode::Attribute => {}
                                        Mode::Global => {}
                                        m => {
                                            last_mode_before_attr = m;
                                        }
                                    }
                                    current_mode = Mode::Attribute;
                                }
                            }
                        }
                    },
                    quick_xml::events::Event::Empty(t) => match t.name().as_ref() {
                        b"extension" => {
                            let mut name = String::new();
                            let mut prefix = String::new();
                            let mut uri = String::new();
                            t.attributes().for_each(|a| {
                                let x = a.unwrap();
                                match x.key.as_ref() {
                                    b"name" => {
                                        x.value.as_ref().read_to_string(&mut name).unwrap();
                                    }
                                    b"prefix" => {
                                        x.value.as_ref().read_to_string(&mut prefix).unwrap();
                                    }
                                    b"uri" => {
                                        x.value.as_ref().read_to_string(&mut uri).unwrap();
                                    }
                                    _ => {}
                                }
                            });
                            log.extensions.as_mut().unwrap().push(EventLogExtension {
                                name,
                                prefix,
                                uri,
                            })
                        }
                        b"classifier" => {
                            let mut name = String::new();
                            let mut keys = String::new();
                            t.attributes().for_each(|a| {
                                let x = a.unwrap();
                                match x.key.as_ref() {
                                    b"name" => {
                                        x.value.as_ref().read_to_string(&mut name).unwrap();
                                    }
                                    b"keys" => {
                                        x.value.as_ref().read_to_string(&mut keys).unwrap();
                                    }
                                    _ => {}
                                }
                            });
                            log.classifiers.as_mut().unwrap().push(EventLogClassifier {
                                name,
                                keys: keys.split(' ').map(|s| s.to_string()).collect(),
                            })
                        }
                        b"log" => {
                            // Empty log, but still a log
                            encountered_log = true;
                            current_mode = Mode::None
                        }
                        _ => {
                            if !encountered_log {
                                return Err(XESParseError::NoTopLevelLog());
                            }
                            if !add_attribute_from_tag(
                                &t,
                                current_mode,
                                &mut log,
                                &mut current_nested_attributes,
                                &options,
                            ) {
                                return Err(XESParseError::AttributeOutsideLog());
                            }
                        }
                    },
                    quick_xml::events::Event::End(t) => {
                        let mut t_string = String::new();
                        t.as_ref().read_to_string(&mut t_string).unwrap();
                        match t_string.as_str() {
                            "event" => current_mode = Mode::Trace,
                            "trace" => current_mode = Mode::Log,
                            "log" => current_mode = Mode::None,
                            "global" => current_mode = last_mode_before_attr,
                            _ => match current_mode {
                                Mode::Attribute => {
                                    if !current_nested_attributes.is_empty() {
                                        let attr = current_nested_attributes.pop().unwrap();
                                        if !current_nested_attributes.is_empty() {
                                            current_nested_attributes
                                                .last_mut()
                                                .unwrap()
                                                .own_attributes
                                                .as_mut()
                                                .unwrap()
                                                .insert(attr.key.clone(), attr);
                                        } else {
                                            match last_mode_before_attr {
                                                Mode::Trace => {
                                                    if let Some(last_trace) = log.traces.last_mut()
                                                    {
                                                        last_trace
                                                            .attributes
                                                            .insert(attr.key.clone(), attr);
                                                    } else {
                                                        return Err(
                                                            XESParseError::MissingLastTrace(),
                                                        );
                                                    }
                                                }
                                                Mode::Event => {
                                                    if let Some(last_trace) = log.traces.last_mut()
                                                    {
                                                        if let Some(last_event) =
                                                            last_trace.events.last_mut()
                                                        {
                                                            last_event
                                                                .attributes
                                                                .insert(attr.key.clone(), attr);
                                                        } else {
                                                            return Err(
                                                                XESParseError::MissingLastEvent(),
                                                            );
                                                        }
                                                    } else {
                                                        return Err(
                                                            XESParseError::MissingLastTrace(),
                                                        );
                                                    }
                                                }
                                                Mode::Log => {
                                                    log.attributes.insert(attr.key.clone(), attr);
                                                }
                                                x => {
                                                    panic!("Invalid Mode! {:?}; This should not happen!",x);
                                                }
                                            }
                                            current_mode = last_mode_before_attr;
                                        }
                                    } else {
                                        // This means there was no current nested attribute but the mode indicated otherwise
                                        // Should thus not happen, but execution can continue.
                                        eprintln!("[Rust] Warning: Attribute mode but no open nested attributes!");
                                        current_mode = last_mode_before_attr;
                                    }
                                }
                                _ => current_mode = Mode::Log,
                            },
                        }
                    }
                    quick_xml::events::Event::Eof => break,
                    _ => {}
                }
            }
            Err(e) => {
                eprintln!("[Rust] Error occured when parsing XES: {:?}", e);
                return Err(XESParseError::XMLParsingError(e));
            }
        }
    }
    buf.clear();
    if encountered_log {
        Ok(log)
    } else {
        Err(XESParseError::NoTopLevelLog())
    }
}

///
/// Import a XES [EventLog] from a file path
///
pub fn import_xes_file(path: &str, options: XESImportOptions) -> Result<EventLog, XESParseError> {
    if path.ends_with(".gz") {
        let file = File::open(path)?;
        let dec: GzDecoder<BufReader<&File>> = GzDecoder::new(BufReader::new(&file));
        let reader = BufReader::new(dec);
        import_xes(&mut Reader::from_reader(reader), options)
    } else {
        let mut reader: Reader<BufReader<std::fs::File>> = Reader::from_file(path)?;
        import_xes(&mut reader, options)
    }
}

///
/// Import a XES [EventLog] directly from a string
///
pub fn import_xes_str(xes_str: &str, options: XESImportOptions) -> Result<EventLog, XESParseError> {
    let mut reader: Reader<&[u8]> = Reader::from_str(xes_str);
    import_xes(&mut reader, options)
}

///
/// Import a XES [EventLog] from a byte slice (&\[u8\])
///
/// * `is_compressed_gz`: Parse the passed `xes_data` as a compressed .gz archive
///
pub fn import_xes_slice(
    xes_data: &[u8],
    is_compressed_gz: bool,
    options: XESImportOptions,
) -> Result<EventLog, XESParseError> {
    // let buf_reader = BufReader::new(reader);
    if is_compressed_gz {
        let gz: GzDecoder<&[u8]> = GzDecoder::new(xes_data);
        let reader = BufReader::new(gz);
        return import_xes(&mut Reader::from_reader(reader), options);
    }
    import_xes(&mut Reader::from_reader(BufReader::new(xes_data)), options)
}

fn parse_attribute_from_tag(
    t: &BytesStart,
    mode: Mode,
    options: &XESImportOptions,
) -> (String, AttributeValue) {
    let mut value = String::new();
    let mut key = String::new();
    t.attributes().for_each(|a| {
        let x = a.unwrap();
        match x.key.as_ref() {
            b"key" => {
                x.value.as_ref().read_to_string(&mut key).unwrap();
            }
            b"value" => {
                x.value.as_ref().read_to_string(&mut value).unwrap();
            }
            _ => {}
        }
    });
    let attribute_val: Option<AttributeValue> = match t.name().as_ref() {
        b"string" => Some(AttributeValue::String(
            unescape(value.as_str())
                .unwrap_or(value.clone().into())
                .into(),
        )),
        b"date" => match &options.date_format {
            // If a format is specified, try parsing with this format: First as DateTime (has to include a time zone)
            //   If this fails, retry parsing as NaiveDateTime (without time zone, assuming UTC)
            Some(dt_format) => match DateTime::parse_from_str(&value, dt_format) {
                Ok(dt) => Some(AttributeValue::Date(dt.into())),
                Err(dt_error) => Some(AttributeValue::Date(
                    match NaiveDateTime::parse_from_str(&value, "%Y-%m-%dT%H:%M:%S%.f") {
                        Ok(dt) => dt.and_local_timezone(Utc).unwrap(),
                        Err(ndt_error) => {
                            eprintln!("Could not parse datetime '{}' with provided format '{}'. Will use datetime epoch 0 instead.\nError (when parsing as DateTime): {:?}\nError (when parsing as NaiveDateTime, without TZ): {:?}", value, dt_format, dt_error, ndt_error);
                            DateTime::default()
                        }
                    },
                )),
            },
            // If no format is specified try two very common formats (rfc3339 standardized and one without timezone)
            None => Some(AttributeValue::Date(
                match DateTime::parse_from_rfc3339(&value) {
                    Ok(dt) => dt.into(),
                    Err(_e) => {
                        match NaiveDateTime::parse_from_str(&value, "%Y-%m-%dT%H:%M:%S%.f") {
                            Ok(dt) => dt.and_local_timezone(Utc).unwrap(),
                            Err(e) => {
                                eprintln!("Could not parse datetime '{}'. Will use datetime epoch 0 instead.\nError {:?}",value,e);
                                DateTime::default()
                            }
                        }
                    }
                },
            )),
        },
        b"int" => {
            let parsed_val = match value.parse::<i64>() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("Could not parse integer {:?}: Error {}", value, e);
                    i64::default()
                }
            };
            Some(AttributeValue::Int(parsed_val))
        }
        b"float" => {
            let parsed_val = match value.parse::<f64>() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("Could not parse float {:?}: Error {}", value, e);
                    f64::default()
                }
            };
            Some(AttributeValue::Float(parsed_val))
        }
        b"boolean" => {
            let parsed_val = match value.parse::<bool>() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("Could not parse boolean {:?}: Error {}", value, e);
                    bool::default()
                }
            };
            Some(AttributeValue::Boolean(parsed_val))
        }
        b"id" => {
            let parsed_val = match Uuid::from_str(&value) {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("Could not parse UUID {:?}: Error {}", value, e);
                    Uuid::default()
                }
            };

            Some(AttributeValue::ID(parsed_val))
        }
        b"container" => Some(AttributeValue::Container(HashMap::new())),
        b"list" => Some(AttributeValue::List(Vec::new())),
        _ => match mode {
            Mode::Log => None,
            m => {
                let mut name_str = String::new();
                t.name().as_ref().read_to_string(&mut name_str).unwrap();
                eprintln!(
                    "Attribute type not implemented '{}' in mode {:?}",
                    name_str, m
                );
                None
            }
        },
    };
    (key, attribute_val.unwrap_or(AttributeValue::None()))
}

///
/// Parse an attribute from a tag (reading the "key" and "value" fields) and parsing the inner value
///
fn add_attribute_from_tag(
    t: &BytesStart,
    mode: Mode,
    log: &mut EventLog,
    current_nested_attributes: &mut [Attribute],
    options: &XESImportOptions,
) -> bool {
    if options.ignore_event_attributes_except.is_some()
        || options.ignore_trace_attributes_except.is_some()
        || options.ignore_log_attributes_except.is_some()
    {
        let key = t.try_get_attribute("key").unwrap().unwrap().value;
        if matches!(mode, Mode::Event)
            && options
                .ignore_event_attributes_except
                .as_ref()
                .is_some_and(|not_ignored| !not_ignored.contains(key.as_ref()))
        {
            return true;
        }
        if matches!(mode, Mode::Trace)
            && options
                .ignore_trace_attributes_except
                .as_ref()
                .is_some_and(|not_ignored| !not_ignored.contains(key.as_ref()))
        {
            return true;
        }

        if matches!(mode, Mode::Log)
            && options
                .ignore_log_attributes_except
                .as_ref()
                .is_some_and(|ignored| !ignored.contains(key.as_ref()))
        {
            return true;
        }
    }

    let (key, val) = parse_attribute_from_tag(t, mode, options);
    match mode {
        Mode::Trace => match log.traces.last_mut() {
            Some(t) => {
                t.attributes.add_to_attributes(key, val);
            }
            None => {
                eprintln!(
                    "No current trace when parsing trace attribute: Key {:?}, Value {:?}",
                    key, val
                );
            }
        },
        Mode::Event => match log.traces.last_mut() {
            Some(t) => match t.events.last_mut() {
                Some(e) => {
                    e.attributes.add_to_attributes(key, val);
                }
                None => {
                    eprintln!(
                        "No current event when parsing event attribute: Key {:?}, Value {:?}",
                        key, val
                    )
                }
            },
            None => {
                eprintln!(
                    "No current trace when parsing event attribute: Key {:?}, Value {:?}",
                    key, val
                );
            }
        },

        Mode::Log => {
            log.attributes.add_to_attributes(key, val);
        }
        Mode::None => return false,
        Mode::Attribute => {
            let last_attr = current_nested_attributes.last_mut().unwrap();
            last_attr.value = match last_attr.value.clone() {
                AttributeValue::List(mut l) => {
                    l.push(Attribute {
                        key,
                        value: val,
                        own_attributes: None,
                    });
                    AttributeValue::List(l)
                }
                AttributeValue::Container(mut c) => {
                    c.add_to_attributes(key, val);
                    AttributeValue::Container(c)
                }
                x => {
                    if let Some(own_attributes) = &mut last_attr.own_attributes {
                        own_attributes.add_to_attributes(key, val);
                    } else {
                        return false;
                    }
                    x
                }
            };
        }
        Mode::Global => {}
    }
    true
}
