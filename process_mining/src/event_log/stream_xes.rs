use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    iter::FusedIterator,
    str::FromStr,
};

use chrono::{DateTime, NaiveDateTime, Utc};
use flate2::read::GzDecoder;
use quick_xml::{escape::unescape, events::BytesStart, Reader};
use uuid::Uuid;

use crate::XESImportOptions;

use super::{
    event_log_struct::{EventLogClassifier, EventLogExtension},
    import_xes::XESParseError,
    Attribute, AttributeAddable, AttributeValue, Attributes, Event, Trace,
};

#[derive(Default, Debug)]

/// (Global) log data parsed during streaming
///
///
/// According to the state machine flow in XES standard (<https://xes-standard.org/_media/xes/xesstandarddefinition-2.0.pdf#page=11>) those must occur before the first trace
///
/// Thus, __for XES-compliant logs it is guaranteed that this data is already complete once the first trace is parsed__.
#[derive(Clone)]
pub struct XESOuterLogData {
    pub extensions: Vec<EventLogExtension>,
    pub classifiers: Vec<EventLogClassifier>,
    pub log_attributes: Attributes,
    pub global_trace_attrs: Attributes,
    pub global_event_attrs: Attributes,
}

#[derive(Clone, Copy, Debug)]
///
/// Current Parsing Mode (i.e., which tag is currently open / being parsed)
///
pub enum Mode {
    Trace,
    Event,
    Attribute,
    GlobalTraceAttributes,
    GlobalEventAttributes,
    Log,
    None,
}

///
/// Streaming XES Parser over [Trace]s
///
/// Can be initiated using any of the streaming functions (e.g. [stream_xes_from_path], [stream_xes_slice], ...)
pub struct StreamingXESParser<'a> {
    ///
    /// Boxed [quick_xml::reader::Reader] to read XML from
    ///
    /// (2x Boxed to prevent making [XESTraceStream] generic, which for example is inconvenient for both gz- and non-gz-readers)
    reader: Box<Reader<Box<dyn BufRead + 'a>>>,
    /// Buffer to read xml into
    buf: Vec<u8>,
    /// Current parsing mode
    current_mode: Mode,
    /// Currently active (=open) trace in current XML parsing position
    current_trace: Option<Trace>,
    // Last mode before nested attribute parsing
    last_mode_before_attr: Mode,
    /// Current nested attributes (used for nested attribute parsing)
    current_nested_attributes: Vec<Attribute>,
    /// XES Import options (see [XESImportOptions])
    options: XESImportOptions,
    /// Whether a (top-level) log tag was encountered yet (top-level log tag is required for XES files, see [XESParseError::NoTopLevelLog])
    encountered_log: bool,
    // [XESOuterLogData] parsed from the log (this will be emitted once the first trace is encountered or the file ends)
    log_data: XESOuterLogData,
    // Whether or not log data was already emitted (i.e., true after the start of the first trace is encountered)
    log_data_emitted: bool,
    /// Whether the parsing was terminated (either by encountering an error or reaching the Eof)
    finished: bool,
}

#[derive(Debug)]
///
/// Enum of possible data streamed by [StreamingXESParser]
pub enum XESNextStreamElement {
    Trace(Trace),
    Error(XESParseError),
    LogData(XESOuterLogData),
}

impl<'a> StreamingXESParser<'a> {
    /// Try to parse a next [XESNextStreamElement] from the current position
    ///
    /// Returns [None] if it encountered an error previously or there are no more traces left
    ///
    /// Otherwise returns [Some] wrapping a [XESNextStreamElement]
    ///
    /// * `XESNextStreamElement:LogData` will be at most emitted once at the beginning (it is emitted before parsing the first trace)
    /// * `XESNextStreamElement:Trace` will be emitted for every trace found in the underlying XES
    /// * `XESNextStreamElement:Error` will be emitted at most once and will end the iterator (i.e., it will only return None afterwards)

    pub fn next_trace(&mut self) -> Option<XESNextStreamElement> {
        // Helper function to terminate parsing and set the error fields
        fn terminate_with_error(
            myself: &mut StreamingXESParser,
            error: XESParseError,
        ) -> Option<XESNextStreamElement> {
            myself.finished = true;
            Some(XESNextStreamElement::Error(error))
        }

        fn emit_log_data(myself: &mut StreamingXESParser) -> Option<XESNextStreamElement> {
            myself.log_data_emitted = true;

            Some(XESNextStreamElement::LogData(myself.log_data.clone()))
        }

        fn emit_trace_data(myself: &mut StreamingXESParser) -> Option<XESNextStreamElement> {
            if let Some(mut trace) = myself.current_trace.take() {
                if let Some(event_timestamp_key) = &myself.options.sort_events_with_timestamp_key {
                    trace.events.sort_by_key(|e| {
                        if let Some(dt_attr) = e.attributes.get_by_key(event_timestamp_key) {
                            if let AttributeValue::Date(d) = dt_attr.value {
                                return Some(d);
                            }
                        }
                        if let Some(x) = myself
                            .log_data
                            .global_event_attrs
                            .get_by_key(event_timestamp_key)
                        {
                            if let AttributeValue::Date(d) = x.value {
                                return Some(d);
                            }
                        }

                        None
                    });
                }
                trace.events.shrink_to_fit();
                trace.attributes.shrink_to_fit();
                trace
                    .events
                    .iter_mut()
                    .for_each(|e| e.attributes.shrink_to_fit());
                return Some(XESNextStreamElement::Trace(trace));
            }
            terminate_with_error(myself, XESParseError::MissingLastTrace)
        }

        // After an error is encountered do not continue parsing
        if self.finished {
            return None;
        }
        self.reader.trim_text(true);

        loop {
            match self.reader.read_event_into(&mut self.buf) {
                Ok(r) => {
                    match r {
                        quick_xml::events::Event::Start(t) => match t.name().as_ref() {
                            b"trace" => {
                                self.current_mode = Mode::Trace;
                                self.current_trace = Some(Trace {
                                    attributes: Attributes::with_capacity(10),
                                    events: Vec::with_capacity(10),
                                });
                                if !self.log_data_emitted {
                                    return emit_log_data(self);
                                }
                            }
                            b"event" => {
                                self.current_mode = Mode::Event;
                                match &mut self.current_trace {
                                    Some(t) => {
                                        t.events.push(Event {
                                            attributes: Attributes::with_capacity(10),
                                        });
                                    }
                                    None => {
                                        eprintln!("Invalid XES format: Event without trace")
                                    }
                                }
                            }
                            b"global" => match t.try_get_attribute("scope") {
                                Ok(Some(a)) => match a.value.as_ref() {
                                    b"trace" => self.current_mode = Mode::GlobalTraceAttributes,
                                    b"event" => self.current_mode = Mode::GlobalEventAttributes,
                                    _ => {
                                        return terminate_with_error(
                                            self,
                                            XESParseError::InvalidKeyValue("scope"),
                                        )
                                    }
                                },
                                Ok(None) => {
                                    return terminate_with_error(
                                        self,
                                        XESParseError::MissingKey("scope"),
                                    );
                                }
                                Err(e) => {
                                    return terminate_with_error(
                                        self,
                                        XESParseError::XMLParsingError(e),
                                    );
                                }
                            },
                            b"log" => {
                                self.encountered_log = true;
                                self.current_mode = Mode::Log
                            }
                            _x => {
                                if !self.encountered_log {
                                    return terminate_with_error(
                                        self,
                                        XESParseError::NoTopLevelLog,
                                    );
                                }
                                {
                                    // Nested attribute!
                                    let key = get_attribute_string(&t, "key");
                                    let value = parse_attribute_value_from_tag(
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
                                self.log_data.extensions.push(EventLogExtension {
                                    name: get_attribute_string(&t, "name"),
                                    prefix: get_attribute_string(&t, "prefix"),
                                    uri: get_attribute_string(&t, "uri"),
                                });
                            }
                            b"classifier" => {
                                self.log_data.classifiers.push(EventLogClassifier {
                                    name: get_attribute_string(&t, "name"),
                                    // TODO: This is not strictly correct according to XES standard, as also strings _inside_ a classifier key are allowed
                                    // See https://xes-standard.org/_media/xes/xesstandarddefinition-2.0.pdf#page=8
                                    keys: get_attribute_string(&t, "keys")
                                        .split(' ')
                                        .map(|s| s.to_string())
                                        .collect(),
                                })
                            }
                            b"log" => {
                                // Empty log, but still a log
                                self.encountered_log = true;
                                self.current_mode = Mode::None;
                                // Send (empty) log_data anyways
                                if !self.log_data_emitted {
                                    return emit_log_data(self);
                                }
                            }
                            b"trace" => {
                                return emit_trace_data(self);
                            }
                            _ => {
                                if !self.encountered_log {
                                    return terminate_with_error(
                                        self,
                                        XESParseError::NoTopLevelLog,
                                    );
                                }
                                if !StreamingXESParser::add_attribute_from_tag(
                                    &self.current_mode,
                                    &mut self.current_trace,
                                    &mut self.log_data,
                                    &mut self.current_nested_attributes,
                                    &self.options,
                                    &t,
                                ) {
                                    return terminate_with_error(
                                        self,
                                        XESParseError::AttributeOutsideLog,
                                    );
                                }
                            }
                        },
                        quick_xml::events::Event::End(t) => {
                            match t.as_ref() {
                                b"event" => self.current_mode = Mode::Trace,
                                b"trace" => {
                                    self.current_mode = Mode::Log;
                                    return emit_trace_data(self);
                                }
                                b"log" => self.current_mode = Mode::None,
                                b"global" => self.current_mode = Mode::Log,
                                _ => {
                                    match self.current_mode {
                                        Mode::Attribute => {
                                            if let Some(attr) = self.current_nested_attributes.pop()
                                            {
                                                if let Some(current_nested) =
                                                    self.current_nested_attributes.last_mut()
                                                {
                                                    if let Some(own_attrs) =
                                                        &mut current_nested.own_attributes
                                                    {
                                                        own_attrs.push(attr);
                                                    } else {
                                                        current_nested.own_attributes =
                                                            Some(vec![attr])
                                                    }
                                                } else {
                                                    match self.last_mode_before_attr {
                                                        Mode::Trace => {
                                                            if let Some(last_trace) =
                                                                &mut self.current_trace
                                                            {
                                                                last_trace
                                                                    .attributes
                                                                    .add_attribute(attr);
                                                            } else {
                                                                return terminate_with_error(
                                                                    self,
                                                                    XESParseError::MissingLastTrace,
                                                                );
                                                            }
                                                        }
                                                        Mode::Event => {
                                                            if let Some(last_trace) =
                                                                &mut self.current_trace
                                                            {
                                                                if let Some(last_event) =
                                                                    last_trace.events.last_mut()
                                                                {
                                                                    last_event
                                                                        .attributes
                                                                        .add_attribute(attr);
                                                                } else {
                                                                    return terminate_with_error(self,XESParseError::MissingLastEvent);
                                                                }
                                                            } else {
                                                                return terminate_with_error(
                                                                    self,
                                                                    XESParseError::MissingLastTrace,
                                                                );
                                                            }
                                                        }
                                                        Mode::Log => {
                                                            self.log_data
                                                                .log_attributes
                                                                .add_attribute(attr);
                                                        }
                                                        Mode::GlobalTraceAttributes => {
                                                            self.log_data
                                                                .global_trace_attrs
                                                                .add_attribute(attr);
                                                        }
                                                        Mode::GlobalEventAttributes => {
                                                            self.log_data
                                                                .global_event_attrs
                                                                .add_attribute(attr);
                                                        }
                                                        _x => {
                                                            return terminate_with_error(
                                                                self,
                                                                XESParseError::InvalidMode,
                                                            );
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
                            // Finished!
                            if !self.encountered_log {
                                // If there was no (top-level) log tag, this was not a valid XES file!
                                return terminate_with_error(self, XESParseError::NoTopLevelLog);
                            }
                            if !self.log_data_emitted {
                                return emit_log_data(self);
                            }
                            self.finished = true;
                            //
                            return None;
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    return terminate_with_error(self, XESParseError::XMLParsingError(e));
                }
            }
            self.buf.clear();
        }
    }
}

impl<'a> StreamingXESParser<'a> {
    ///
    /// Add XES attribute from tag to the currently active element (indicated by `current_mode`)
    ///
    fn add_attribute_from_tag(
        current_mode: &Mode,
        current_trace: &mut Option<Trace>,
        log_data: &mut XESOuterLogData,
        current_nested_attributes: &mut [Attribute],
        options: &XESImportOptions,
        t: &BytesStart,
    ) -> bool {
        let key = get_attribute_string(t, "key");
        if options.ignore_event_attributes_except.is_some()
            || options.ignore_trace_attributes_except.is_some()
            || options.ignore_log_attributes_except.is_some()
        {
            if matches!(current_mode, Mode::Event)
                && options
                    .ignore_event_attributes_except
                    .as_ref()
                    .is_some_and(|not_ignored| !not_ignored.contains(&key))
            {
                return true;
            }
            if matches!(current_mode, Mode::Trace)
                && options
                    .ignore_trace_attributes_except
                    .as_ref()
                    .is_some_and(|not_ignored| !not_ignored.contains(&key))
            {
                return true;
            }

            if matches!(current_mode, Mode::Log)
                && options
                    .ignore_log_attributes_except
                    .as_ref()
                    .is_some_and(|ignored| !ignored.contains(&key))
            {
                return true;
            }
        }

        let val = parse_attribute_value_from_tag(t, current_mode, options);
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
                log_data.log_attributes.add_to_attributes(key, val);
            }
            Mode::None => return false,
            Mode::Attribute => {
                if let Some(last_attr) = current_nested_attributes.last_mut() {
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
                } else {
                    return false;
                }
            }
            Mode::GlobalTraceAttributes => {
                log_data.global_trace_attrs.add_to_attributes(key, val);
            }
            Mode::GlobalEventAttributes => {
                log_data.global_event_attrs.add_to_attributes(key, val);
            }
        }
        true
    }
}

pub struct XESParsingTraceStream<'a> {
    inner: StreamingXESParser<'a>,
    pub error: Option<XESParseError>,
}

pub type XESParsingStreamAndLogData<'a> = (XESParsingTraceStream<'a>, XESOuterLogData);

impl<'a> Iterator for &mut XESParsingTraceStream<'a> {
    type Item = Trace;

    fn next(&mut self) -> Option<Self::Item> {
        if self.error.is_some() {
            return None;
        }
        match self.inner.next_trace() {
            Some(XESNextStreamElement::Trace(t)) => Some(t),
            Some(XESNextStreamElement::Error(e)) => {
                self.error = Some(e);
                None
            }
            Some(XESNextStreamElement::LogData(_)) => {
                self.error = Some(XESParseError::ExpectedTraceData);
                None
            }
            None => None,
        }
    }
}

impl<'a> FusedIterator for &mut XESParsingTraceStream<'a> {}

impl<'a> XESParsingTraceStream<'a> {
    /// Check if any errors occured
    pub fn check_for_errors(&self) -> Option<XESParseError> {
        self.error.clone()
    }

    /// Set or update parsing options
    pub fn set_options(&mut self, options: XESImportOptions) {
        self.inner.options = options;
    }

    /// Set or update parsing options
    pub fn get_options(&self) -> &XESImportOptions {
        &self.inner.options
    }

    ///
    /// Try to construct a new [XESParsingTraceStream] and directly try to parse until the first trace
    ///
    /// As all log attributes must occur before the first trace, this already returns the parsed [XESOuterLogData]
    ///
    pub fn try_new(
        reader: Box<Reader<Box<dyn BufRead + 'a>>>,
        options: XESImportOptions,
    ) -> Result<(Self, XESOuterLogData), XESParseError> {
        let log_data = XESOuterLogData::default();
        let mut s = StreamingXESParser {
            reader,
            current_mode: Mode::Log,
            current_trace: None,
            last_mode_before_attr: Mode::Log,
            encountered_log: false,
            current_nested_attributes: Vec::new(),
            options,
            log_data,
            log_data_emitted: false,
            buf: Vec::new(),
            finished: false,
        };
        let next = s.next_trace();
        match next {
            Some(el) => match el {
                XESNextStreamElement::Error(e) => Err(e),
                XESNextStreamElement::Trace(_) => {
                    eprintln!("Encountered trace before LogData; This should not happen!");
                    Err(XESParseError::ExpectedLogData)
                }
                XESNextStreamElement::LogData(d) => Ok((
                    (Self {
                        inner: s,
                        error: None,
                    }),
                    d,
                )),
            },
            None => {
                // No log data and no error returned: This should not happen!
                eprintln!(
                    "Iterator initially empty. Expected log data or error; This should not happen!"
                );
                Err(XESParseError::ExpectedLogData)
            }
        }
    }
}

///
/// Stream XES [Trace]s from byte slice
///
/// The returned [XESParsingStreamAndLogData] contains the [XESOuterLogData] and can be used to iterate over [Trace]s
///
pub fn stream_xes_slice(
    xes_data: &[u8],
    options: XESImportOptions,
) -> Result<XESParsingStreamAndLogData<'_>, XESParseError> {
    XESParsingTraceStream::try_new(
        Box::new(Reader::from_reader(Box::new(BufReader::new(xes_data)))),
        options,
    )
}

///
/// Stream XES [Trace]s from gzipped byte slice
///
/// The returned [XESParsingStreamAndLogData] contains the [XESOuterLogData] and can be used to iterate over [Trace]s
///
pub fn stream_xes_slice_gz(
    xes_data: &[u8],
    options: XESImportOptions,
) -> Result<XESParsingStreamAndLogData<'_>, XESParseError> {
    let gz: GzDecoder<&[u8]> = GzDecoder::new(xes_data);
    let reader = BufReader::new(gz);
    XESParsingTraceStream::try_new(Box::new(Reader::from_reader(Box::new(reader))), options)
}

///
/// Stream XES [Trace]s from a file
///
/// The returned [XESParsingStreamAndLogData] contains the [XESOuterLogData] and can be used to iterate over [Trace]s
///
pub fn stream_xes_file<'a>(
    file: File,
    options: XESImportOptions,
) -> Result<XESParsingStreamAndLogData<'a>, XESParseError> {
    XESParsingTraceStream::try_new(
        Box::new(Reader::from_reader(Box::new(BufReader::new(file)))),
        options,
    )
}

///
/// Stream XES [Trace]s from a gzipped file
///
/// The returned [XESParsingStreamAndLogData] contains the [XESOuterLogData] and can be used to iterate over [Trace]s
///
pub fn stream_xes_file_gz<'a>(
    file: File,
    options: XESImportOptions,
) -> Result<XESParsingStreamAndLogData<'a>, XESParseError> {
    let dec = GzDecoder::new(file);
    XESParsingTraceStream::try_new(
        Box::new(Reader::from_reader(Box::new(BufReader::new(dec)))),
        options,
    )
}

///
/// Stream XES [Trace]s from path (auto-detecting gz compression from file extension)
///
/// The returned [XESParsingStreamAndLogData] contains the [XESOuterLogData] and can be used to iterate over [Trace]s
///
pub fn stream_xes_from_path<'a>(
    path: &str,
    options: XESImportOptions,
) -> Result<XESParsingStreamAndLogData<'a>, XESParseError> {
    let file = File::open(path)?;
    if path.ends_with(".gz") {
        stream_xes_file_gz(file, options)
    } else {
        stream_xes_file(file, options)
    }
}

fn get_attribute_string(t: &BytesStart, key: &'static str) -> String {
    if let Ok(Some(attr)) = t.try_get_attribute(key) {
        return String::from_utf8_lossy(&attr.value).to_string();
    }
    eprintln!(
        "Did not find expected XML attribute with key {}. Will assume empty string as value.",
        key
    );
    String::new()
}

pub fn parse_attribute_value_from_tag(
    t: &BytesStart,
    mode: &Mode,
    options: &XESImportOptions,
) -> AttributeValue {
    let value = get_attribute_string(t, "value");
    let attribute_val: Option<AttributeValue> = match t.name().as_ref() {
        b"string" => Some(AttributeValue::String(
            unescape(value.as_str())
                .unwrap_or(value.as_str().into())
                .into(),
        )),
        b"date" => match parse_date_from_str(&value, &options.date_format) {
            Some(dt) => Some(AttributeValue::Date(dt)),
            None => {
                eprintln!("Failed to parse data from {:?}", value);
                None
            }
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
        b"container" => Some(AttributeValue::Container(Attributes::new())),
        b"list" => Some(AttributeValue::List(Vec::new())),
        _ => match mode {
            Mode::Log => None,
            m => {
                let mut name_str = String::new();
                t.name()
                    .as_ref()
                    .read_to_string(&mut name_str)
                    .unwrap_or_default();
                eprintln!(
                    "Attribute type not implemented '{}' in mode {:?}",
                    name_str, m
                );
                None
            }
        },
    };
    attribute_val.unwrap_or(AttributeValue::None())
}

fn parse_date_from_str(value: &str, date_format: &Option<String>) -> Option<DateTime<Utc>> {
    // Is a date_format string provided?
    if let Some(date_format) = &date_format {
        if let Ok(dt) = DateTime::parse_from_str(value, date_format) {
            return Some(dt.into());
        }
        // If parsing with DateTime with provided date format fail, try to parse NaiveDateTime using format (i.e., without time-zone, assuming UTC)
        if let Ok(dt) = NaiveDateTime::parse_from_str(value, date_format) {
            return Some(dt.and_utc());
        }
    }

    // Default parsing options for commonly used formats

    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Some(dt.into());
    }

    if let Ok(dt) = DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S:%f%:z") {
        return Some(dt.into());
    }

    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(dt.and_utc());
    }

    None
}

#[cfg(test)]
mod stream_test {
    use std::{collections::HashSet, time::Instant};

    use crate::{
        event_log::{
            import_xes::build_ignore_attributes,
            stream_xes::{stream_xes_slice, stream_xes_slice_gz},
        },
        XESImportOptions,
    };

    #[test]
    fn test_xes_stream() {
        let x = include_bytes!("tests/test_data/RepairExample.xes");
        let (mut stream, _log_data) = stream_xes_slice(x, XESImportOptions::default()).unwrap();
        let num_traces = stream.count();
        println!("Num. traces: {}", num_traces);
        assert_eq!(num_traces, 1104);
    }

    #[test]
    pub fn test_streaming_variants() {
        let log_bytes =
            include_bytes!("tests/test_data/Road_Traffic_Fine_Management_Process.xes.gz");
        let now = Instant::now();
        let (mut log_stream, log_data) =
            stream_xes_slice_gz(log_bytes, XESImportOptions::default()).unwrap();
        let classifier = log_data
            .classifiers
            .iter()
            .find(|c| c.name == "Event Name")
            .unwrap();
        log_stream.set_options(XESImportOptions {
            ignore_event_attributes_except: Some(build_ignore_attributes(&classifier.keys)),
            ignore_trace_attributes_except: Some(build_ignore_attributes(Vec::<&str>::new())),
            ..XESImportOptions::default()
        });
        // Gather unique variants of traces (wrt. the hardcoded )
        let trace_variants: HashSet<Vec<String>> = log_stream
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
