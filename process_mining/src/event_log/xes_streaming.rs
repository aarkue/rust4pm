use std::io::{BufRead, BufReader, Read};

use flate2::bufread::GzDecoder;
use quick_xml::{events::BytesStart, Reader};

use crate::XESImportOptions;

use super::{
    event_log_struct::{EventLogClassifier, EventLogExtension},
    import_xes::{parse_attribute_from_tag, Mode, XESParseError},
    Attribute, AttributeAddable, AttributeValue, Attributes, Event, Trace,
};

pub struct XESTraceStream<T>
where
    T: BufRead,
{
    reader: Reader<T>,
    buf: Vec<u8>,
    current_mode: Mode,
    current_trace: Option<Trace>,
    last_mode_before_attr: Mode,
    encountered_log: bool,
    current_nested_attributes: Vec<Attribute>,
    options: XESImportOptions,
    extensions: Vec<EventLogExtension>,
    classifiers: Vec<EventLogClassifier>,
    log_attributes: Attributes,
}

impl<T> Iterator for XESTraceStream<T>
where
    T: BufRead,
{
    type Item = Trace;

    fn next(&mut self) -> Option<Self::Item> {
        // let mut buf = Vec::<u8>::new();
        loop {
            match self.reader.read_event_into(&mut self.buf) {
                Ok(r) => {
                    match r {
                        quick_xml::events::Event::Start(t) => match t.name().as_ref() {
                            b"trace" => {
                                self.current_mode = Mode::Trace;
                                self.current_trace = Some(Trace {
                                    attributes: Attributes::new(),
                                    events: Vec::new(),
                                });
                            }
                            b"event" => {
                                self.current_mode = Mode::Event;
                                match &mut self.current_trace {
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
                                match self.current_mode {
                                    Mode::Global => {}
                                    Mode::Attribute => {}
                                    m => {
                                        self.last_mode_before_attr = m;
                                    }
                                }
                                self.current_mode = Mode::Global;
                            }
                            b"log" => {
                                self.encountered_log = true;
                                self.current_mode = Mode::Log
                            }
                            _x => {
                                if !self.encountered_log {
                                    panic!("{:?}", XESParseError::NoTopLevelLog());
                                    // Err(XESParseError::NoTopLevelLog());
                                    return None;
                                }
                                {
                                    // Nested attribute!
                                    let (key, value) = parse_attribute_from_tag(
                                        &t,
                                        self.current_mode,
                                        &self.options,
                                    );
                                    if !(key.is_empty() && matches!(value, AttributeValue::None()))
                                    {
                                        self.current_nested_attributes.push(Attribute {
                                            key,
                                            value,
                                            own_attributes: Some(Attributes::new()),
                                        });
                                        match self.current_mode {
                                            Mode::Attribute => {}
                                            Mode::Global => {}
                                            m => {
                                                self.last_mode_before_attr = m;
                                            }
                                        }
                                        self.current_mode = Mode::Attribute;
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
                                self.extensions
                                    .push(EventLogExtension { name, prefix, uri })
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
                                self.classifiers.push(EventLogClassifier {
                                    name,
                                    keys: keys.split(' ').map(|s| s.to_string()).collect(),
                                })
                            }
                            b"log" => {
                                // Empty log, but still a log
                                self.encountered_log = true;
                                self.current_mode = Mode::None
                            }
                            _ => {
                                if !self.encountered_log {
                                    panic!("{:?}", XESParseError::NoTopLevelLog());
                                    return None;
                                }
                                // if !self.add_attribute_from_tag(&t) {
                                //     panic!("{:?}", XESParseError::AttributeOutsideLog());
                                //     return None;
                                // }
                            }
                        },
                        quick_xml::events::Event::End(t) => {
                            let mut t_string = String::new();
                            t.as_ref().read_to_string(&mut t_string).unwrap();
                            match t_string.as_str() {
                                "event" => self.current_mode = Mode::Trace,
                                "trace" => {
                                    self.current_mode = Mode::Log;
                                    return Some(self.current_trace.clone().unwrap());
                                }
                                "log" => self.current_mode = Mode::None,
                                "global" => self.current_mode = self.last_mode_before_attr,
                                _ => {
                                    match self.current_mode {
                                        Mode::Attribute => {
                                            if !self.current_nested_attributes.is_empty() {
                                                let attr =
                                                    self.current_nested_attributes.pop().unwrap();
                                                if !self.current_nested_attributes.is_empty() {
                                                    self.current_nested_attributes
                                                        .last_mut()
                                                        .unwrap()
                                                        .own_attributes
                                                        .as_mut()
                                                        .unwrap()
                                                        .insert(attr.key.clone(), attr);
                                                } else {
                                                    match self.last_mode_before_attr {
                                                        Mode::Trace => {
                                                            if let Some(last_trace) =
                                                                &mut self.current_trace
                                                            {
                                                                last_trace
                                                                    .attributes
                                                                    .insert(attr.key.clone(), attr);
                                                            } else {
                                                                panic!(
                                                                    "{:?}",
                                                                    XESParseError::MissingLastTrace(
                                                                    )
                                                                );
                                                                return None;
                                                                // return Err(
                                                                //     XESParseError::MissingLastTrace(),
                                                                // );
                                                            }
                                                        }
                                                        Mode::Event => {
                                                            if let Some(last_trace) =
                                                                &mut self.current_trace
                                                            {
                                                                if let Some(last_event) =
                                                                    last_trace.events.last_mut()
                                                                {
                                                                    last_event.attributes.insert(
                                                                        attr.key.clone(),
                                                                        attr,
                                                                    );
                                                                } else {
                                                                    panic!("{:?}",XESParseError::MissingLastEvent());
                                                                    return None;
                                                                    // return Err(
                                                                    //     XESParseError::MissingLastEvent(
                                                                    //     ),
                                                                    // );
                                                                }
                                                            } else {
                                                                return None;
                                                                // return Err(
                                                                //     XESParseError::MissingLastTrace(),
                                                                // );
                                                            }
                                                        }
                                                        Mode::Log => {
                                                            self.log_attributes
                                                                .insert(attr.key.clone(), attr);
                                                        }
                                                        x => {
                                                            panic!("Invalid Mode! {:?}; This should not happen!",x);
                                                        }
                                                    }
                                                    self.current_mode = self.last_mode_before_attr;
                                                }
                                            } else {
                                                // This means there was no current nested attribute but the mode indicated otherwise
                                                // Should thus not happen, but execution can continue.
                                                eprintln!("[Rust] Warning: Attribute mode but no open nested attributes!");
                                                self.current_mode = self.last_mode_before_attr;
                                            }
                                        }
                                        _ => self.current_mode = Mode::Log,
                                    }
                                }
                            }
                        }
                        quick_xml::events::Event::Eof => {
                            // panic!("End of file");
                            return None;
                        }
                        _ => {}
                    }
                }
                Err(e) => panic!("{:?}: {}", XESParseError::AttributeOutsideLog(), e),
            }
        }
    }
}

impl<T> XESTraceStream<T>
where
    T: BufRead,
{
    fn add_attribute_from_tag(self: &mut Self, t: &BytesStart) -> bool {
        if self.options.ignore_event_attributes_except.is_some()
            || self.options.ignore_trace_attributes_except.is_some()
            || self.options.ignore_log_attributes_except.is_some()
        {
            let key = t.try_get_attribute("key").unwrap().unwrap().value;
            if matches!(self.current_mode, Mode::Event)
                && self
                    .options
                    .ignore_event_attributes_except
                    .as_ref()
                    .is_some_and(|not_ignored| !not_ignored.contains(key.as_ref()))
            {
                return true;
            }
            if matches!(self.current_mode, Mode::Trace)
                && self
                    .options
                    .ignore_trace_attributes_except
                    .as_ref()
                    .is_some_and(|not_ignored| !not_ignored.contains(key.as_ref()))
            {
                return true;
            }

            if matches!(self.current_mode, Mode::Log)
                && self
                    .options
                    .ignore_log_attributes_except
                    .as_ref()
                    .is_some_and(|ignored| !ignored.contains(key.as_ref()))
            {
                return true;
            }
        }

        let (key, val) = parse_attribute_from_tag(t, self.current_mode, &self.options);
        match self.current_mode {
            Mode::Trace => match &mut self.current_trace {
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
            Mode::Event => match &mut self.current_trace {
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
                self.log_attributes.add_to_attributes(key, val);
            }
            Mode::None => return false,
            Mode::Attribute => {
                let last_attr = self.current_nested_attributes.last_mut().unwrap();
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
}

pub fn stream_xes_slice(
    xes_data: &[u8],
    options: XESImportOptions,
) -> XESTraceStream<BufReader<&[u8]>> {
    let reader = Reader::from_reader(BufReader::new(xes_data));
    XESTraceStream {
        reader: reader,
        current_mode: Mode::Log,
        current_trace: None,
        last_mode_before_attr: Mode::Log,
        encountered_log: false,
        current_nested_attributes: Vec::new(),
        options,
        extensions: Vec::new(),
        classifiers: Vec::new(),
        log_attributes: Attributes::new(),
        buf: Vec::new(),
    }
}

pub fn stream_xes_slice_gz<'a>(
    xes_data: &'a [u8],
    options: XESImportOptions,
) -> XESTraceStream<BufReader<flate2::bufread::GzDecoder<&'a [u8]>>> {
    let gz: GzDecoder<&[u8]> = GzDecoder::new(xes_data);
    let reader = BufReader::new(gz);
    XESTraceStream {
        reader: Reader::from_reader(reader),
        current_mode: Mode::Log,
        current_trace: None,
        last_mode_before_attr: Mode::Log,
        encountered_log: false,
        current_nested_attributes: Vec::new(),
        options,
        extensions: Vec::new(),
        classifiers: Vec::new(),
        log_attributes: Attributes::new(),
        buf: Vec::new(),
    }
}

#[test]
fn test_xes_stream() {
    let x = include_bytes!("tests/test_data/RepairExample.xes");
    let num_traces = stream_xes_slice(x, XESImportOptions::default()).count();
    println!("Num. traces: {}", num_traces);
    assert_eq!(num_traces,1104);
}
