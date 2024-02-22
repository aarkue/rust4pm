use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
};

use flate2::read::GzDecoder;
use quick_xml::{events::BytesStart, Reader};

use crate::XESImportOptions;

use super::{
    event_log_struct::{EventLogClassifier, EventLogExtension},
    import_xes::{parse_attribute_from_tag, Mode, XESParseError},
    Attribute, AttributeAddable, AttributeValue, Attributes, Event, Trace,
};

pub struct XESTraceStreamIter<'a>(XESTraceStream<'a>);

impl<'a> Iterator for XESTraceStreamIter<'a> {
    type Item = Trace;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next_trace()
    }
}

pub struct XESTraceStreamIterResult<'a>(XESTraceStream<'a>);

impl<'a> Iterator for XESTraceStreamIterResult<'a> {
    type Item = Result<Trace, XESParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next_trace() {
            Some(t) => Some(Ok(t)),
            None => self.0.terminated_on_error.take().map(Err),
        }
    }
}

///
/// Streaming Parser for [Trace]s
///
/// Experimental implementation
///
pub struct XESTraceStream<'a> {
    reader: Box<Reader<Box<dyn BufRead + 'a>>>,
    buf: Vec<u8>,
    current_mode: Mode,
    current_trace: Option<Trace>,
    last_mode_before_attr: Mode,
    current_nested_attributes: Vec<Attribute>,
    extensions: Vec<EventLogExtension>,
    classifiers: Vec<EventLogClassifier>,
    log_attributes: Attributes,
    options: XESImportOptions,
    encountered_log: bool,
    terminated_on_error: Option<XESParseError>,
    trace_cached: Option<Trace>,
}

impl<'a> XESTraceStream<'a> {
    ///
    /// Iterate over the parsed [Trace]s as a `Result<Trace,XESParseError`
    ///
    /// The resulting iterator will return a single `Err(...)` item and no items after that if an [XESParseError] is encountered.
    /// For a Iterator over the [Trace] type directly see the `stream`` function instead
    ///
    /// Experimental implementation!
    ///
    ///
    pub fn stream_results(self) -> XESTraceStreamIterResult<'a> {
        XESTraceStreamIterResult(self)
    }
    ///
    /// Iterate over the parsed [Trace]s
    ///
    /// The resulting iterator will simply report None when error are encountered.
    /// For a Iterator over Result types see the `stream_results`` function instead
    ///
    /// Experimental implementation!
    ///
    ///
    pub fn stream(self) -> XESTraceStreamIter<'a> {
        XESTraceStreamIter(self)
    }

    fn next_trace(&mut self) -> Option<Trace> {
        if self.terminated_on_error.is_some() {
            return None;
        }
        if let Some(t) = self.trace_cached.take() {
            self.trace_cached = None;
            return Some(t);
        }
        loop {
            match self.reader.read_event_into(&mut self.buf) {
                Ok(r) => {
                    match r {
                        quick_xml::events::Event::Start(t) => match t.name().as_ref() {
                            b"trace" => {
                                self.current_mode = Mode::Trace;
                                match &mut self.current_trace {
                                    Some(trace) => {
                                        trace.attributes.clear();
                                        trace.events.clear();
                                    }
                                    None => {
                                        self.current_trace = Some(Trace {
                                            attributes: Attributes::new(),
                                            events: Vec::new(),
                                        });
                                    }
                                }
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
                                    self.terminated_on_error = Some(XESParseError::NoTopLevelLog);
                                    return None;
                                }
                                {
                                    // Nested attribute!
                                    let (key, value) = parse_attribute_from_tag(
                                        &t,
                                        &self.current_mode,
                                        &self.options,
                                    );
                                    if !(key.is_empty() && matches!(value, AttributeValue::None()))
                                    {
                                        self.current_nested_attributes.push(Attribute {
                                            key,
                                            value,
                                            own_attributes: None,
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
                                    self.terminated_on_error = Some(XESParseError::NoTopLevelLog);
                                    return None;
                                }
                                if !XESTraceStream::add_attribute_from_tag(
                                    &self.current_mode,
                                    &mut self.current_trace,
                                    &mut self.log_attributes,
                                    &mut self.current_nested_attributes,
                                    &self.options,
                                    &t,
                                ) {
                                    self.terminated_on_error =
                                        Some(XESParseError::AttributeOutsideLog);
                                    return None;
                                }
                            }
                        },
                        quick_xml::events::Event::End(t) => {
                            match t.as_ref() {
                                b"event" => self.current_mode = Mode::Trace,
                                b"trace" => {
                                    self.current_mode = Mode::Log;
                                    let trace = self.current_trace.take().unwrap();
                                    self.current_trace = None;
                                    return Some(trace);
                                }
                                b"log" => self.current_mode = Mode::None,
                                b"global" => self.current_mode = self.last_mode_before_attr,
                                _ => {
                                    match self.current_mode {
                                        Mode::Attribute => {
                                            if !self.current_nested_attributes.is_empty() {
                                                let attr =
                                                    self.current_nested_attributes.pop().unwrap();
                                                if !self.current_nested_attributes.is_empty() {
                                                    if let Some(own_attrs) = self
                                                        .current_nested_attributes
                                                        .last_mut()
                                                        .unwrap()
                                                        .own_attributes
                                                        .as_mut()
                                                    {
                                                        own_attrs.insert(attr.key.clone(), attr);
                                                    } else {
                                                        let mut own_attrs = Attributes::new();
                                                        own_attrs.insert(attr.key.clone(), attr);
                                                        self.current_nested_attributes
                                                            .last_mut()
                                                            .unwrap()
                                                            .own_attributes = Some(own_attrs)
                                                    }
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
                                                                self.terminated_on_error = Some(
                                                                    XESParseError::MissingLastTrace,
                                                                );
                                                                return None;
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
                                                                    self.terminated_on_error = Some(XESParseError::MissingLastEvent);
                                                                    return None;
                                                                }
                                                            } else {
                                                                self.terminated_on_error = Some(
                                                                    XESParseError::MissingLastTrace,
                                                                );
                                                                return None;
                                                            }
                                                        }
                                                        Mode::Log => {
                                                            self.log_attributes
                                                                .insert(attr.key.clone(), attr);
                                                        }
                                                        _x => {
                                                            self.terminated_on_error =
                                                                Some(XESParseError::InvalidMode);
                                                            return None;
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
                            if !self.encountered_log {
                                self.terminated_on_error = Some(XESParseError::NoTopLevelLog);
                                return None;
                            }
                            return None;
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    self.terminated_on_error = Some(XESParseError::XMLParsingError(e));
                    return None;
                }
            }
            self.buf.clear();
        }
    }
}

impl<'a> XESTraceStream<'a> {
    pub fn new(reader: Box<Reader<Box<dyn BufRead + 'a>>>, options: XESImportOptions) -> Self {
        XESTraceStream {
            reader,
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
            terminated_on_error: None,
            trace_cached: None,
        }
    }

    pub fn try_new(
        reader: Box<Reader<Box<dyn BufRead + 'a>>>,
        options: XESImportOptions,
    ) -> Result<Self, XESParseError> {
        let mut s = XESTraceStream {
            reader,
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
            terminated_on_error: None,
            trace_cached: None,
        };
        let t = s.next_trace();
        match t {
            Some(trace) => {
                s.trace_cached = Some(trace);
                Ok(s)
            }
            None => {
                if let Some(e) = s.terminated_on_error {
                    Err(e)
                } else {
                    Ok(s)
                }
            }
        }
    }

    fn add_attribute_from_tag(
        current_mode: &Mode,
        current_trace: &mut Option<Trace>,
        log_attributes: &mut Attributes,
        current_nested_attributes: &mut [Attribute],
        options: &XESImportOptions,
        t: &BytesStart,
    ) -> bool {
        if options.ignore_event_attributes_except.is_some()
            || options.ignore_trace_attributes_except.is_some()
            || options.ignore_log_attributes_except.is_some()
        {
            let key = t.try_get_attribute("key").unwrap().unwrap().value;
            if matches!(current_mode, Mode::Event)
                && options
                    .ignore_event_attributes_except
                    .as_ref()
                    .is_some_and(|not_ignored| !not_ignored.contains(key.as_ref()))
            {
                return true;
            }
            if matches!(current_mode, Mode::Trace)
                && options
                    .ignore_trace_attributes_except
                    .as_ref()
                    .is_some_and(|not_ignored| !not_ignored.contains(key.as_ref()))
            {
                return true;
            }

            if matches!(current_mode, Mode::Log)
                && options
                    .ignore_log_attributes_except
                    .as_ref()
                    .is_some_and(|ignored| !ignored.contains(key.as_ref()))
            {
                return true;
            }
        }

        let (key, val) = parse_attribute_from_tag(t, current_mode, options);
        match current_mode {
            Mode::Trace => match current_trace {
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
            Mode::Event => match current_trace {
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
                log_attributes.add_to_attributes(key, val);
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
                            let mut new_own_attrs = Attributes::new();
                            new_own_attrs.add_to_attributes(key, val);
                            last_attr.own_attributes = Some(new_own_attrs);
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

///
/// Stream XES Traces from byte slice
///
/// __Warning:__ XES streams are currently still unstable and incomplete
///
/// Note, that currently events outside of a trace and log attributes, classifiers and extensions are not exposed
///
pub fn stream_xes_slice(
    xes_data: &[u8],
    options: XESImportOptions,
) -> Result<XESTraceStream, XESParseError> {
    XESTraceStream::try_new(
        Box::new(Reader::from_reader(Box::new(BufReader::new(xes_data)))),
        options,
    )
}

///
/// Stream XES Traces from gzipped byte slice
///
/// __Warning:__ XES streams are currently still unstable and incomplete
///
/// Note, that currently events outside of a trace and log attributes, classifiers and extensions are not exposed
///
pub fn stream_xes_slice_gz(
    xes_data: &[u8],
    options: XESImportOptions,
) -> Result<XESTraceStream, XESParseError> {
    let gz: GzDecoder<&[u8]> = GzDecoder::new(xes_data);
    let reader = BufReader::new(gz);
    XESTraceStream::try_new(Box::new(Reader::from_reader(Box::new(reader))), options)
}

///
/// Stream XES Traces from a file
///
/// __Warning:__ XES streams are currently still unstable and incomplete
///
/// Note, that currently events outside of a trace and log attributes, classifiers and extensions are not exposed
///
fn stream_xes_file<'a>(
    file: File,
    options: XESImportOptions,
) -> Result<XESTraceStream<'a>, XESParseError> {
    XESTraceStream::try_new(
        Box::new(Reader::from_reader(Box::new(BufReader::new(file)))),
        options,
    )
}

///
/// Stream XES Traces from a gzipped file
///
/// __Warning:__ XES streams are currently still unstable and incomplete
///
/// Note, that currently events outside of a trace and log attributes, classifiers and extensions are not exposed
///
fn stream_xes_file_gz<'a>(
    file: File,
    options: XESImportOptions,
) -> Result<XESTraceStream<'a>, XESParseError> {
    let dec = GzDecoder::new(file);
    XESTraceStream::try_new(
        Box::new(Reader::from_reader(Box::new(BufReader::new(dec)))),
        options,
    )
}

///
/// Stream XES Traces from path (auto-detecting gz compression)
///
/// __Warning:__ XES streams are currently still unstable and incomplete
///
/// Note, that currently events outside of a trace and log attributes, classifiers and extensions are not exposed
///
pub fn stream_xes_from_path(
    path: &str,
    options: XESImportOptions,
) -> Result<XESTraceStream<'_>, XESParseError> {
    let file = File::open(path)?;
    if path.ends_with(".gz") {
        stream_xes_file_gz(file, options)
    } else {
        stream_xes_file(file, options)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Instant};

    use crate::{
        event_log::{
            event_log_struct::EventLogClassifier,
            import_xes::build_ignore_attributes,
            xes_streaming::{stream_xes_slice, stream_xes_slice_gz},
        },
        XESImportOptions,
    };

    #[test]
    fn test_xes_stream() {
        let x = include_bytes!("tests/test_data/RepairExample.xes");
        let num_traces = stream_xes_slice(x, XESImportOptions::default())
            .unwrap()
            .stream()
            .count();
        println!("Num. traces: {}", num_traces);
        assert_eq!(num_traces, 1104);
    }

    #[test]
    pub fn test_streaming_variants() {
        let log_bytes =
            include_bytes!("tests/test_data/Road_Traffic_Fine_Management_Process.xes.gz");
        // Hardcoded event log classifier as log attributes are not available in streaming (at least for now)
        let classifier = EventLogClassifier {
            name: "Name".to_string(),
            keys: vec!["concept:name".to_string()],
        };
        let now = Instant::now();
        let log_stream = stream_xes_slice_gz(
            log_bytes,
            XESImportOptions {
                ignore_event_attributes_except: Some(build_ignore_attributes(&classifier.keys)),
                ignore_trace_attributes_except: Some(build_ignore_attributes(vec!["concept:name"])),
                ignore_log_attributes_except: Some(build_ignore_attributes(Vec::<&str>::new())),
                ..XESImportOptions::default()
            },
        )
        .unwrap();

        // Gather unique variants of traces (wrt. the hardcoded )
        let trace_variants: HashSet<Vec<String>> = log_stream
            .stream()
            .map(|t| {
                t.events
                    .iter()
                    .map(|ev| classifier.get_class_identity(ev))
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
