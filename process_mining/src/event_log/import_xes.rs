use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::str::FromStr;

use chrono::{DateTime, NaiveDateTime, Utc};
use flate2::bufread::GzDecoder;
use quick_xml::escape::unescape;
use quick_xml::events::BytesStart;
use quick_xml::Reader;
use uuid::Uuid;

use crate::event_log::event_log_struct::{
    Attribute, AttributeAddable, AttributeValue, Attributes, Event, EventLog, EventLogClassifier,
    EventLogExtension, Trace,
};

///
/// Current Parsing Mode (i.e., which tag is currently open / being parsed)
///
#[derive(Clone, Copy, Debug)]
enum Mode {
    Trace,
    Event,
    Attribute,
    Global,
    None,
}


fn parse_attribute_from_tag(
    t: &BytesStart,
    mode: Mode,
    date_format: Option<&str>,
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
        b"date" => match date_format {
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
                            Ok(dt) => dt.and_local_timezone(Utc).unwrap().into(),
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
            Mode::None => None,
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
    return (key, attribute_val.unwrap_or(AttributeValue::None()));
}

///
/// Parse an attribute from a tag (reading the "key" and "value" fields) and parsing the inner value
///
fn add_attribute_from_tag(
    t: &BytesStart,
    mode: Mode,
    log: &mut EventLog,
    current_nested_attributes: &mut Vec<Attribute>,
    date_format: Option<&str>,
) {
    let (key, val) = parse_attribute_from_tag(t, mode, date_format);
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

        Mode::None => {
            log.attributes.add_to_attributes(key, val);
        }
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
                    last_attr
                        .own_attributes
                        .as_mut()
                        .unwrap()
                        .add_to_attributes(key, val);
                    x
                }
            };
        }
        Mode::Global => {}
    }
}

///
/// Import an XES [EventLog] from a [Reader]
///
pub fn import_xes<T>(reader: &mut Reader<T>, date_format: Option<&str>) -> EventLog
where
    T: BufRead,
{
    reader.trim_text(true);
    let mut buf: Vec<u8> = Vec::new();

    let mut current_mode: Mode = Mode::None;
    let mut last_mode_before_attr: Mode = Mode::None;

    let mut log = EventLog {
        attributes: Attributes::new(),
        traces: Vec::new(),
        extensions: Some(Vec::new()),
        classifiers: Some(Vec::new()),
    };
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
                        x => {
                            match x {
                                b"log" => {}
                                _ => {
                                    // Nested attribute!
                                    let (key, value) =
                                        parse_attribute_from_tag(&t, current_mode, date_format);
                                    if !(key == "" && matches!(value, AttributeValue::None())) {
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
                                keys: keys.split(" ").map(|s| s.to_string()).collect(),
                            })
                        }
                        _ => add_attribute_from_tag(
                            &t,
                            current_mode,
                            &mut log,
                            &mut current_nested_attributes,
                            date_format
                        ),
                    },
                    quick_xml::events::Event::End(t) => {
                        let mut t_string = String::new();
                        t.as_ref().read_to_string(&mut t_string).unwrap();
                        match t_string.as_str() {
                            "event" => current_mode = Mode::Trace,
                            "trace" => current_mode = Mode::None,
                            "log" => current_mode = Mode::None,
                            "global" => current_mode = last_mode_before_attr,
                            _ => match current_mode {
                                Mode::Attribute => {
                                    if current_nested_attributes.len() >= 1 {
                                        let attr = current_nested_attributes.pop().unwrap();
                                        if current_nested_attributes.len() >= 1 {
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
                                                    log.traces
                                                        .last_mut()
                                                        .unwrap()
                                                        .attributes
                                                        .insert(attr.key.clone(), attr);
                                                }
                                                Mode::Event => {
                                                    log.traces
                                                        .last_mut()
                                                        .unwrap()
                                                        .events
                                                        .last_mut()
                                                        .unwrap()
                                                        .attributes
                                                        .insert(attr.key.clone(), attr);
                                                }
                                                Mode::None => {
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
                                _ => current_mode = Mode::None,
                            },
                        }
                    }
                    quick_xml::events::Event::Eof => break,
                    _ => {}
                }
            }
            Err(e) => {
                eprintln!("[Rust] Error occured when parsing XES: {:?}", e);
            }
        }
    }
    buf.clear();
    return log;
}

///
/// Import an XES [EventLog] from a file path
///
pub fn import_xes_file(path: &str, date_format: Option<&str>) -> EventLog {
    if path.ends_with(".gz") {
        let file = File::open(path).unwrap();
        let reader = BufReader::new(&file);
        let mut dec = GzDecoder::new(reader);
        let mut s = String::new();
        dec.read_to_string(&mut s).unwrap();
        let mut reader: Reader<&[u8]> = Reader::from_str(&s);
        return import_xes(&mut reader, date_format);
    } else {
        let mut reader: Reader<BufReader<std::fs::File>> = Reader::from_file(path).unwrap();
        return import_xes(&mut reader, date_format);
    }
}

///
/// Import an XES [EventLog] directly from a string
///
pub fn import_xes_str(xes_str: &str, date_format: Option<&str>) -> EventLog {
    let mut reader: Reader<&[u8]> = Reader::from_str(&xes_str);
    return import_xes(&mut reader, date_format);
}
