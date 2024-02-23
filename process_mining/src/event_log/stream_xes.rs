use std::{
    cell::RefCell,
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

///
/// Iterator directly yielding items of type [Trace]
///
/// Does no error checking by itself. Consumers should use [XESTraceStreamLogData::terminated_on_error] after iterator ends (i.e., first [None] is returned from `next()`)
pub struct XESTraceStreamIter<'a>(XESTraceStreamParser<'a>);

impl<'a> Iterator for XESTraceStreamIter<'a> {
    type Item = Trace;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next_trace()
    }
}

/// Fused iterator (once None, always None)
/// Holds when file ends but also when an [XESParseError] is encountered
impl<'a> FusedIterator for XESTraceStreamIter<'a> {}

///
/// Iterator yielding items of type [Result<Trace, XESParseError>]
///
/// Incoporates error checking: When an error is encountered while streaming, one last `Some(Result<...>)` will be emitted (containing an [XESParseError])
///
/// In most cases, [XESTraceStreamIter] should be preferred and used instead, because of better performance and ergonomics.
///
pub struct XESTraceStreamIterResult<'a>(XESTraceStreamParser<'a>);

impl<'a> Iterator for XESTraceStreamIterResult<'a> {
    type Item = Result<Trace, XESParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next_trace() {
            Some(t) => Some(Ok(t)),
            None => self.0.terminated_on_error.take().map(Err),
        }
    }
}

/// Fused iterator (once None, always None)
/// Holds when file ends but also when an [XESParseError] is encountered
impl<'a> FusedIterator for XESTraceStreamIterResult<'a> {}

#[derive(Default, Debug)]
/// (Global) log data parsed during streaming
///
/// Also includes `terminated_on_error` for convenient error detection
///
/// According to the state machine flow in XES standard (<https://xes-standard.org/_media/xes/xesstandarddefinition-2.0.pdf#page=11>) those must occur before the first trace.
/// The current streaming parser will however also allow them afterwards.
///
/// Thus, __for XES-compliant logs it is guaranteed that this data is already complete once the first trace is emitted from the iterator__.
pub struct XESTraceStreamLogData {
    pub extensions: Vec<EventLogExtension>,
    pub classifiers: Vec<EventLogClassifier>,
    pub log_attributes: Attributes,
    pub global_trace_attrs: Attributes,
    pub global_event_attrs: Attributes,
    /// Whether an error (wrapped in [Some]) was encountered or not ([None])
    pub terminated_on_error: Option<XESParseError>,
}

/// A RefCell over [XESTraceStreamLogData] to pass inside a [XESTraceStreamParser]
pub type XESTraceStreamLogDataRefCell = RefCell<XESTraceStreamLogData>;

/// Construct a new [XESTraceStreamLogDataRefCell]
///
/// Convenient function for constructing a [XESTraceStreamParser]
pub fn construct_log_data_cell() -> XESTraceStreamLogDataRefCell {
    RefCell::new(XESTraceStreamLogData::default())
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
pub struct XESTraceStreamParser<'a> {
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
    /// Whether the parsing was terminated by encountering an error: If yes, parsing will stop
    terminated_on_error: Option<XESParseError>,
    /// Cached [Trace] (already parsed but not yet emitted) used for initially checking if any valid trace is contained, see also [XESTraceStream::try_new]
    trace_cached: Option<Trace>,
    log_data: &'a XESTraceStreamLogDataRefCell,
}

impl<'a> XESTraceStreamParser<'a> {
    ///
    /// Move [XESTraceStreamLogData] log data out of self
    ///
    pub fn take_log_data(self) -> XESTraceStreamLogData {
        self.log_data.take()
    }

    ///
    ///  Borrow the [XESTraceStreamLogData] log data
    ///
    pub fn get_log_data(&self) -> std::cell::Ref<'a, XESTraceStreamLogData> {
        self.log_data.borrow()
    }

    ///
    ///  Borrow the [XESTraceStreamLogData] log data as mut
    ///
    pub fn get_log_data_mut(&self) -> std::cell::RefMut<'a, XESTraceStreamLogData> {
        self.log_data.borrow_mut()
    }

    ///
    /// Iterate over the parsed [Trace]s as a `Result<Trace,XESParseError`
    ///
    /// The resulting iterator will return a single `Err(...)` item (and no items after that) if an [XESParseError] is encountered.
    /// For a Iterator over the [Trace] type directly see the `stream`` function instead
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
    pub fn stream(self) -> XESTraceStreamIter<'a> {
        XESTraceStreamIter(self)
    }

    /// Try to parse a next [Trace] from the current position
    ///
    /// Returns [None] if there are no more traces or an error was encountered (errors are detectable using [XESTraceStreamParser::log_data] and [XESTraceStreamLogData::terminated_on_error])
    ///
    /// Otherwise returns [Some] wrapping a [Trace] with next trace in the data
    fn next_trace(&mut self) -> Option<Trace> {
        // Helper function to terminate parsing and set the error fields
        fn terminate_with_error(
            myself: &mut XESTraceStreamParser,
            error: XESParseError,
        ) -> Option<Trace> {
            myself.log_data.borrow_mut().terminated_on_error = Some(error.clone());
            myself.terminated_on_error = Some(error);
            None
        }
        // After an error is encountered do not continue parsing
        if self.terminated_on_error.is_some() {
            return None;
        }
        // If there is a cached trace available, return that (and unset trace_cached)
        if let Some(t) = self.trace_cached.take() {
            self.trace_cached = None;
            return Some(t);
        }

        self.reader.trim_text(true);

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
                                            attributes: Attributes::with_capacity(10),
                                            events: Vec::with_capacity(10),
                                        });
                                    }
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
                                self.log_data
                                    .borrow_mut()
                                    .extensions
                                    .push(EventLogExtension { name, prefix, uri });
                                // self.extensions
                                //     .push(EventLogExtension { name, prefix, uri })
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
                                self.log_data
                                    .borrow_mut()
                                    .classifiers
                                    .push(EventLogClassifier {
                                        name,
                                        // TODO: This is not strictly correct according to XES standard, as also strings _inside_ a classifier key are allowed
                                        // See https://xes-standard.org/_media/xes/xesstandarddefinition-2.0.pdf#page=8
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
                                    return terminate_with_error(
                                        self,
                                        XESParseError::NoTopLevelLog,
                                    );
                                }
                                if !XESTraceStreamParser::add_attribute_from_tag(
                                    &self.current_mode,
                                    &mut self.current_trace,
                                    &mut self.log_data.borrow_mut(),
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
                                    let mut trace = self.current_trace.take().unwrap();
                                    trace
                                        .events
                                        .iter_mut()
                                        .for_each(|e| e.attributes.shrink_to_fit());
                                    trace.events.shrink_to_fit();
                                    trace.attributes.shrink_to_fit();
                                    self.current_trace = None;
                                    return Some(trace);
                                }
                                b"log" => self.current_mode = Mode::None,
                                b"global" => self.current_mode = Mode::Log,
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
                                                        own_attrs.push(attr);
                                                    } else {
                                                        self.current_nested_attributes
                                                            .last_mut()
                                                            .unwrap()
                                                            .own_attributes = Some(vec![attr])
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
                                                                .borrow_mut()
                                                                .log_attributes
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

/// Iterate over traces in streamed log
///
/// Note: Errors are not handled by the iterator itself. See [XESTraceStreamIter].
impl<'a> IntoIterator for XESTraceStreamParser<'a> {
    type Item = Trace;

    type IntoIter = XESTraceStreamIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.stream()
    }
}

impl<'a> XESTraceStreamParser<'a> {
    ///
    /// Construct a new [XESTraceStreamParser] (without parsing anything)
    ///
    /// To directly parse the first trace (and thus quickly checking if the content _could_ be valid XES) use [XESTraceStreamParser::try_new] instead.
    ///
    pub fn new(
        reader: Box<Reader<Box<dyn BufRead + 'a>>>,
        options: XESImportOptions,
        log_data: &'a XESTraceStreamLogDataRefCell,
    ) -> Self {
        XESTraceStreamParser {
            reader,
            current_mode: Mode::Log,
            current_trace: None,
            last_mode_before_attr: Mode::Log,
            encountered_log: false,
            current_nested_attributes: Vec::new(),
            options,
            log_data,
            buf: Vec::new(),
            terminated_on_error: None,
            trace_cached: None,
        }
    }

    ///
    /// Try to construct a new [XESTraceStreamParser] and directly try to parse the first trace
    ///
    /// This allows quickly checking if the content _could_ be valid XES (See [XESTraceStreamParser::new] for a version which does no initial parsing)
    ///
    pub fn try_new(
        reader: Box<Reader<Box<dyn BufRead + 'a>>>,
        options: XESImportOptions,
        log_data: &'a XESTraceStreamLogDataRefCell,
    ) -> Result<Self, XESParseError> {
        let mut s = XESTraceStreamParser {
            reader,
            current_mode: Mode::Log,
            current_trace: None,
            last_mode_before_attr: Mode::Log,
            encountered_log: false,
            current_nested_attributes: Vec::new(),
            options,
            log_data,
            buf: Vec::new(),
            terminated_on_error: None,
            trace_cached: None,
        };
        let t = s.next_trace();
        match t {
            Some(trace) => {
                // If a trace was parsed, the XML looks to be valid XES for now
                // -> Cache the parsed trace and return the XESTraceStreamParser
                s.trace_cached = Some(trace);
                Ok(s)
            }
            None => {
                // No trace returned, this could either be an error or just an empty log
                if let Some(e) = s.terminated_on_error {
                    Err(e)
                } else {
                    Ok(s)
                }
            }
        }
    }

    ///
    /// Add XES attribute from tag to the currently active element (indicated by `current_mode`)
    ///
    fn add_attribute_from_tag(
        current_mode: &Mode,
        current_trace: &mut Option<Trace>,
        log_data: &mut XESTraceStreamLogData,
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
                log_data.log_attributes.add_to_attributes(key, val);
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

///
/// Stream XES [Trace]s from byte slice
///
/// The returned [XESTraceStreamParser] can be used to iterate over [Trace]s
///
pub fn stream_xes_slice<'a>(
    xes_data: &'a [u8],
    options: XESImportOptions,
    log_data: &'a XESTraceStreamLogDataRefCell,
) -> Result<XESTraceStreamParser<'a>, XESParseError> {
    XESTraceStreamParser::try_new(
        Box::new(Reader::from_reader(Box::new(BufReader::new(xes_data)))),
        options,
        log_data,
    )
}

///
/// Stream XES [Trace]s from gzipped byte slice
///
/// The returned [XESTraceStreamParser] can be used to iterate over [Trace]s
///
pub fn stream_xes_slice_gz<'a>(
    xes_data: &'a [u8],
    options: XESImportOptions,
    log_data: &'a XESTraceStreamLogDataRefCell,
) -> Result<XESTraceStreamParser<'a>, XESParseError> {
    let gz: GzDecoder<&[u8]> = GzDecoder::new(xes_data);
    let reader = BufReader::new(gz);
    XESTraceStreamParser::try_new(
        Box::new(Reader::from_reader(Box::new(reader))),
        options,
        log_data,
    )
}

///
/// Stream XES [Trace]s from a file
///
/// The returned [XESTraceStreamParser] can be used to iterate over [Trace]s
///
pub fn stream_xes_file(
    file: File,
    options: XESImportOptions,
    log_data: &XESTraceStreamLogDataRefCell,
) -> Result<XESTraceStreamParser<'_>, XESParseError> {
    XESTraceStreamParser::try_new(
        Box::new(Reader::from_reader(Box::new(BufReader::new(file)))),
        options,
        log_data,
    )
}

///
/// Stream XES [Trace]s from a gzipped file
///
/// The returned [XESTraceStreamParser] can be used to iterate over [Trace]s
///
pub fn stream_xes_file_gz(
    file: File,
    options: XESImportOptions,
    log_data: &XESTraceStreamLogDataRefCell,
) -> Result<XESTraceStreamParser<'_>, XESParseError> {
    let dec = GzDecoder::new(file);
    XESTraceStreamParser::try_new(
        Box::new(Reader::from_reader(Box::new(BufReader::new(dec)))),
        options,
        log_data,
    )
}

///
/// Stream XES [Trace]s from path (auto-detecting gz compression from file extension)
///
/// The returned [XESTraceStreamParser] can be used to iterate over [Trace]s
///
pub fn stream_xes_from_path<'a>(
    path: &str,
    options: XESImportOptions,
    log_data: &'a XESTraceStreamLogDataRefCell,
) -> Result<XESTraceStreamParser<'a>, XESParseError> {
    let file = File::open(path)?;
    if path.ends_with(".gz") {
        stream_xes_file_gz(file, options, log_data)
    } else {
        stream_xes_file(file, options, log_data)
    }
}

pub fn parse_attribute_from_tag(
    t: &BytesStart,
    mode: &Mode,
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
                .unwrap_or(value.as_str().into())
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
        b"container" => Some(AttributeValue::Container(Attributes::new())),
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

#[cfg(test)]
mod stream_test {
    use std::{collections::HashSet, time::Instant};

    use crate::{
        event_log::{
            event_log_struct::EventLogClassifier,
            import_xes::build_ignore_attributes,
            stream_xes::{construct_log_data_cell, stream_xes_slice, stream_xes_slice_gz},
        },
        XESImportOptions,
    };

    #[test]
    fn test_xes_stream() {
        let x = include_bytes!("tests/test_data/RepairExample.xes");
        let log_data = construct_log_data_cell();
        let num_traces = stream_xes_slice(x, XESImportOptions::default(), &log_data)
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
        let log_data = construct_log_data_cell();
        let log_stream = stream_xes_slice_gz(
            log_bytes,
            XESImportOptions {
                ignore_event_attributes_except: Some(build_ignore_attributes(&classifier.keys)),
                ignore_trace_attributes_except: Some(build_ignore_attributes(vec!["concept:name"])),
                ignore_log_attributes_except: Some(build_ignore_attributes(Vec::<&str>::new())),
                ..XESImportOptions::default()
            },
            &log_data,
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
