use std::io::Read;
use std::str::FromStr;
use std::time::Instant;

use chrono::DateTime;
use quick_xml::events::BytesStart;
use quick_xml::Reader;
use uuid::Uuid;

use crate::event_log::event_log_struct::{AttributeValue, Attributes, Event, EventLog, Trace, AttributeAddable};

#[derive(Clone, Copy)]
enum Mode {
    Trace,
    Event,
    None,
}

fn add_attribute_from_tag(t: &BytesStart, mode: Mode, log: &mut EventLog) {
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
        b"string" => Some(AttributeValue::String(value)),
        b"date" => Some(AttributeValue::Date(
            DateTime::parse_from_rfc3339(&value).unwrap().into(),
        )),
        b"int" => Some(AttributeValue::Int(value.parse::<i64>().unwrap())),
        b"float" => Some(AttributeValue::Float(value.parse::<f64>().unwrap())),
        b"boolean" => Some(AttributeValue::Boolean(value.parse::<bool>().unwrap())),
        b"id" => Some(AttributeValue::ID(Uuid::from_str(&value).unwrap())),
        _ => {
            let mut name_str = String::new();
            t.name().as_ref().read_to_string(&mut name_str).unwrap();
            // todo!("Name ('{}') not implementede yet",name_str);
            None
        }
    };
    match attribute_val {
        Some(val) => match mode {
            Mode::Trace => {
                log.traces
                    .last_mut()
                    .unwrap()
                    .attributes
                    .add_to_attributes(key, val);
            }
            Mode::Event => {
                log.traces
                    .last_mut()
                    .unwrap()
                    .events
                    .last_mut()
                    .unwrap()
                    .attributes
                    .add_to_attributes(key, val);
            }

            Mode::None => {
                log.attributes.add_to_attributes(key, val);
            }
        },
        None => {}
    }
}

pub fn import_log_xes(path: &str) -> EventLog {
    let now = Instant::now();
    let mut reader = Reader::from_file(path).unwrap();

    reader.trim_text(true);
    let mut buf: Vec<u8> = Vec::new();

    let mut current_mode: Mode = Mode::None;

    let mut log = EventLog {
        attributes: Attributes::new(),
        traces: Vec::new(),
    };

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(r) => match r {
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
                        log.traces.last_mut().unwrap().events.push(Event {
                            attributes: Attributes::new(),
                        });
                    }
                    _ => {
                        add_attribute_from_tag(&t, current_mode, &mut log);
                    }
                },
                quick_xml::events::Event::Empty(t) => match current_mode {
                    Mode::None => {
                        // let mut name = String::new();
                        // t.name().as_ref().read_to_string(&mut name).unwrap();
                        // Maybe add event log attributes?
                        match t.name().as_ref() {
                            b"event" => {
                                println!("EVENT!!!!");
                            }

                            b"trace" => {
                                println!("TRACE!!!!");
                            }
                            _ => {}
                        }
                        // println!("Name {}", name);
                    }
                    mode => {
                        add_attribute_from_tag(&t, mode, &mut log);
                    }
                },
                quick_xml::events::Event::Eof => break,
                _ => {}
            },
            Err(_) => todo!(),
        }
    }
    buf.clear();
    println!("Parsing XES took {:.2?}", now.elapsed());
    // println!("Read event log with {:?} traces.", log.traces.len());
    return log;
}
