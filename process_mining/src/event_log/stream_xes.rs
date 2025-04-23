use super::{
    event_log_struct::{EventLogClassifier, EventLogExtension},
    import_xes::XESParseError,
    Attribute, AttributeValue, Attributes, Event, Trace, XESEditableAttribute,
};
use crate::XESImportOptions;
use chrono::{DateTime, FixedOffset, NaiveDateTime};
use flate2::read::GzDecoder;
use quick_xml::{escape::unescape, events::BytesStart, Reader};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    fs::File,
    io::{BufRead, BufReader, Read},
    iter::FusedIterator,
    str::FromStr,
};
use uuid::Uuid;

/// (Global) log data parsed during streaming
///
///
/// According to the state machine flow in XES standard (<https://xes-standard.org/_media/xes/xesstandarddefinition-2.0.pdf#page=11>) those must occur before the first trace
///
/// Thus, __for XES-compliant logs it is guaranteed that this data is already complete once the first trace is parsed__.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct XESOuterLogData {
    /// XES Extensions of event log
    pub extensions: Vec<EventLogExtension>,
    /// Event Classifiers of event log
    pub classifiers: Vec<EventLogClassifier>,
    /// Log-level attributes of event log
    pub log_attributes: Attributes,
    /// Global trace attributes of event log
    pub global_trace_attrs: Attributes,
    /// Global event attributes of event log
    pub global_event_attrs: Attributes,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
///
/// Current Parsing Mode (i.e., which tag is currently open / being parsed)
///
pub enum Mode {
    /// Parsing trace
    Trace,
    /// Parsing event
    Event,
    /// Parsing nested attributes
    Attribute,
    /// Parsing global trace attributes
    GlobalTraceAttributes,
    /// Parsing global event attributes
    GlobalEventAttributes,
    /// Parsing log
    Log,
    /// No currently open tags
    None,
}

///
/// Streaming XES Parser over [`Trace`]s
///
/// Can be initiated using any of the streaming functions (e.g. [`stream_xes_from_path`], [`stream_xes_slice`], ...)
pub struct StreamingXESParser<'a> {
    ///
    /// Boxed [`quick_xml::reader::Reader`] to read XML from
    ///
    /// (2x Boxed to prevent making [`XESTraceStream`] generic, which for example is inconvenient for both gz- and non-gz-readers)
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
    /// XES Import options (see [`XESImportOptions`])
    options: XESImportOptions,
    /// Whether a (top-level) log tag was encountered yet (top-level log tag is required for XES files, see [`XESParseError::NoTopLevelLog`])
    encountered_log: bool,
    // [XESOuterLogData] parsed from the log (this will be emitted once the first trace is encountered or the file ends)
    log_data: XESOuterLogData,
    // Whether or not log data was already emitted (i.e., true after the start of the first trace is encountered)
    log_data_emitted: bool,
    /// Whether the parsing was terminated (either by encountering an error or reaching the Eof)
    finished: bool,
}

impl Debug for StreamingXESParser<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingXESParser")
            .field("reader", &"[Boxed Reader]")
            .field("buf", &self.buf)
            .field("current_mode", &self.current_mode)
            .field("current_trace", &self.current_trace)
            .field("last_mode_before_attr", &self.last_mode_before_attr)
            .field("current_nested_attributes", &self.current_nested_attributes)
            .field("options", &self.options)
            .field("encountered_log", &self.encountered_log)
            .field("log_data", &self.log_data)
            .field("log_data_emitted", &self.log_data_emitted)
            .field("finished", &self.finished)
            .finish()
    }
}

#[derive(Debug)]
///
/// Enum of possible data streamed by [`StreamingXESParser`]
pub enum XESNextStreamElement {
    /// Log data
    LogData(XESOuterLogData),
    /// Trace data
    Trace(Trace),
    /// Encountered error
    Error(XESParseError),
}

impl StreamingXESParser<'_> {
    /// Try to parse a next [`XESNextStreamElement`] from the current position
    ///
    /// Returns [`None`] if it encountered an error previously or there are no more traces left
    ///
    /// Otherwise returns [`Some`] wrapping a [`XESNextStreamElement`]
    ///
    /// * `XESNextStreamElement:LogData` will be at most emitted once at the beginning (it is emitted before parsing the first trace)
    /// * `XESNextStreamElement:Trace` will be emitted for every trace found in the underlying XES
    /// * `XESNextStreamElement:Error` will be emitted at most once and will end the iterator (i.e., it will only return None afterwards)
    pub fn next_trace(&mut self) -> Option<XESNextStreamElement> {
        // Helper function to terminate parsing and set the error fields
        fn terminate_with_error(
            myself: &mut StreamingXESParser<'_>,
            error: XESParseError,
        ) -> Option<XESNextStreamElement> {
            myself.finished = true;
            Some(XESNextStreamElement::Error(error))
        }

        fn emit_log_data(myself: &mut StreamingXESParser<'_>) -> Option<XESNextStreamElement> {
            myself.log_data_emitted = true;

            Some(XESNextStreamElement::LogData(myself.log_data.clone()))
        }

        fn emit_trace_data(myself: &mut StreamingXESParser<'_>) -> Option<XESNextStreamElement> {
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
        self.reader.config_mut().trim_text(true);

        fn parse_classifier(t: &BytesStart<'_>, log_data: &mut XESOuterLogData) {
            log_data.classifiers.push(EventLogClassifier {
                name: get_attribute_string(t, "name").unwrap_or_default(),
                keys: parse_classifier_key(
                    get_attribute_string(t, "keys").unwrap_or_default(),
                    log_data,
                ),
            })
        }

        fn parse_extension(t: &BytesStart<'_>, log_data: &mut XESOuterLogData) {
            log_data.extensions.push(EventLogExtension {
                name: get_attribute_string(t, "name").unwrap_or_default(),
                prefix: get_attribute_string(t, "prefix").unwrap_or_default(),
                uri: get_attribute_string(t, "uri").unwrap_or_default(),
            });
        }

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
                                        XESParseError::XMLParsingError(
                                            quick_xml::Error::InvalidAttr(e),
                                        ),
                                    );
                                }
                            },
                            b"log" => {
                                if self.encountered_log {
                                    eprintln!(
                                        "Encountered two log tags. This is not a valid XES file"
                                    )
                                }
                                self.encountered_log = true;
                                self.current_mode = Mode::Log
                            }
                            b"extension" => {
                                parse_extension(&t, &mut self.log_data);
                            }
                            b"classifier" => {
                                parse_classifier(&t, &mut self.log_data);
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
                                    let key = get_attribute_string(&t, "key").unwrap_or_default();
                                    if !should_ignore_attribute(
                                        &self.options,
                                        &self.current_mode,
                                        &key,
                                    ) {
                                        let value = parse_attribute_value_from_tag(
                                            &t,
                                            &self.current_mode,
                                            &self.options,
                                        );
                                        if !(key.is_empty()
                                            && matches!(value, AttributeValue::None()))
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
                            }
                        },
                        quick_xml::events::Event::Empty(t) => match t.name().as_ref() {
                            b"extension" => {
                                parse_extension(&t, &mut self.log_data);
                            }
                            b"classifier" => {
                                parse_classifier(&t, &mut self.log_data);
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
                            b"event" => {
                                todo!("Empty event not handled?!");
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
                                                    match &mut current_nested.value {
                                                        AttributeValue::Container(c) => {
                                                            c.push(attr);
                                                        }
                                                        AttributeValue::List(l) => {
                                                            l.add_attribute(attr);
                                                        }
                                                        _ => {
                                                            if let Some(own_attrs) =
                                                                &mut current_nested.own_attributes
                                                            {
                                                                own_attrs.push(attr);
                                                            } else {
                                                                current_nested.own_attributes =
                                                                    Some(vec![attr])
                                                            }
                                                        }
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
                                        _ => {
                                            // We might end up here if there are nested, ignored attributes
                                            // Noop
                                        }
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

///
/// Parse classifier key in accordance with XES Standard (which allows spaces under certain conditions)
///
/// For reference, see <https://xes-standard.org/_media/xes/xesstandarddefinition-2.0.pdf#page=8>
pub fn parse_classifier_key(t: String, log_data: &XESOuterLogData) -> Vec<String> {
    let mut ret: Vec<String> = Vec::new();
    let mut buffer: Vec<char> = Vec::new();
    let chars: Vec<char> = t.chars().collect();
    let mut i = 0;
    let mut is_inside_quotes = false;
    while i < chars.len() {
        let c = chars[i];
        if c == '\'' {
            if is_inside_quotes {
                ret.push(String::from_iter(buffer.iter()));
                buffer.clear();
                is_inside_quotes = false;
            } else {
                is_inside_quotes = true;
            }
        } else if !is_inside_quotes && c == ' ' {
            let mut s = String::from_iter(buffer.iter());
            if log_data.global_event_attrs.iter().any(|attr| attr.key == s) {
                // Test if there is a global event attribute with the same name (otherwise, we might want to try expand with space)?
                ret.push(String::from_iter(buffer.iter()));
                buffer.clear();
            } else if log_data
                .global_event_attrs
                .iter()
                .any(|attr| attr.key.starts_with(&(s.clone() + " ")))
            {
                // Otherwise, is there a global event attribute with s as a prefix?
                let j = i;
                let mut found_attr = false;
                let prev_buffer = buffer.clone();
                while i < chars.len()
                    && log_data
                        .global_event_attrs
                        .iter()
                        .any(|attr| attr.key.starts_with(&s))
                {
                    let c = chars[i];
                    buffer.push(c);
                    s = String::from_iter(buffer.iter());
                    if log_data.global_event_attrs.iter().any(|attr| attr.key == s)
                        && (chars.get(i + 1).is_none()
                            || chars.get(i + 1).is_some_and(|next_c| *next_c == ' '))
                    {
                        ret.push(s.clone());
                        buffer.clear();
                        found_attr = true;
                        break;
                    }
                    i += 1;
                }
                // Did the look-forward find a matching global attribute?
                if !found_attr {
                    // If not, reset to the last position...
                    buffer = prev_buffer;
                    if !buffer.is_empty() {
                        ret.push(String::from_iter(buffer.iter()));
                        buffer.clear();
                    }
                    // ...skipping the space
                    i = j + 1;
                    continue;
                }
            } else {
                // i.e., c is a space, not inside quotes
                //  and no global attribute matching
                // In this case: Push the current buffer to ret
                if !buffer.is_empty() {
                    ret.push(String::from_iter(buffer.iter()));
                    buffer.clear();
                }
            }
        } else {
            buffer.push(c);
        }
        i += 1;
    }
    // After loop: push the last buffer to ret (if it is not empty, which happens when encountering single quotes)
    if !buffer.is_empty() {
        ret.push(String::from_iter(buffer.iter()))
    }
    ret
}
fn should_ignore_attribute(options: &XESImportOptions, mode: &Mode, key: &str) -> bool {
    if options.ignore_event_attributes_except.is_some()
        || options.ignore_trace_attributes_except.is_some()
        || options.ignore_log_attributes_except.is_some()
    {
        if matches!(mode, Mode::Event)
            && options
                .ignore_event_attributes_except
                .as_ref()
                .is_some_and(|not_ignored| !not_ignored.contains(key))
        {
            return true;
        }
        if matches!(mode, Mode::Trace)
            && options
                .ignore_trace_attributes_except
                .as_ref()
                .is_some_and(|not_ignored| !not_ignored.contains(key))
        {
            return true;
        }

        if matches!(mode, Mode::Log)
            && options
                .ignore_log_attributes_except
                .as_ref()
                .is_some_and(|not_ignored| !not_ignored.contains(key))
        {
            return true;
        }
    }
    false
}
#[test]
fn test_classifier_parse() {
    let data = XESOuterLogData {
        extensions: Vec::new(),
        classifiers: Vec::new(),
        log_attributes: Vec::new(),
        global_trace_attrs: Vec::new(),
        global_event_attrs: vec![
            Attribute {
                key: "test key".into(),
                value: AttributeValue::String("test value".into()),
                own_attributes: None,
            },
            Attribute {
                key: "aaa bbb ccc ddd".into(),
                value: AttributeValue::String("test value".into()),
                own_attributes: None,
            },
            Attribute {
                key: "aaa bbb ccc ddd eee".into(),
                value: AttributeValue::String("test value".into()),
                own_attributes: None,
            },
        ],
    };
    assert_eq!(
        parse_classifier_key(
            "'testing 123' test key single test koo naa aaa bbb ccc ddd aaa bbb ccc dd was".into(),
            &data
        ),
        vec![
            "testing 123",
            "test key",
            "single",
            "test",
            "koo",
            "naa",
            "aaa bbb ccc ddd",
            "aaa",
            "bbb",
            "ccc",
            "dd",
            "was"
        ]
    );

    assert_eq!(
        parse_classifier_key("test this is".into(), &data),
        vec!["test", "this", "is"]
    );

    assert_eq!(
        parse_classifier_key("'test key fake' test key 'test key' test koo".into(), &data),
        vec!["test key fake", "test key", "test key", "test", "koo"]
    );

    assert_eq!(
        parse_classifier_key("aa bb 'xx yy' cc dd".into(), &data),
        vec!["aa", "bb", "xx yy", "cc", "dd"]
    );

    assert_eq!(
        parse_classifier_key("test ke".into(), &data),
        vec!["test", "ke"]
    );
    assert_eq!(
        parse_classifier_key("test key".into(), &data),
        vec!["test key"]
    );

    // This might be ambigious:
    // Currently, the first matching global key is accepted, even if a longer match is possible
    assert_eq!(
        parse_classifier_key("aaa bbb ccc ddd eee".into(), &data),
        vec!["aaa bbb ccc ddd", "eee"]
    );
}
impl StreamingXESParser<'_> {
    ///
    /// Add XES attribute from tag to the currently active element (indicated by `current_mode`)
    ///
    fn add_attribute_from_tag(
        current_mode: &Mode,
        current_trace: &mut Option<Trace>,
        log_data: &mut XESOuterLogData,
        current_nested_attributes: &mut [Attribute],
        options: &XESImportOptions,
        t: &BytesStart<'_>,
    ) -> bool {
        let key = get_attribute_string(t, "key").unwrap_or_default();
        if should_ignore_attribute(options, current_mode, &key) {
            return true;
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
                    match &mut last_attr.value {
                        AttributeValue::List(l) => l.push(Attribute::new(key, val)),
                        AttributeValue::Container(c) => {
                            c.add_to_attributes(key, val);
                        }
                        _ => {
                            if last_attr.own_attributes.is_none() {
                                last_attr.own_attributes = Some(Attributes::new());
                            }
                            last_attr
                                .own_attributes
                                .as_mut()
                                .unwrap()
                                .add_to_attributes(key, val);
                        }
                    }
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

#[derive(Debug)]
/// XES Parsing Trace Stream
///
/// Allows iterating over [`Trace`]s
///
/// Parses traces laziliy (i.e., only when they are requested)
pub struct XESParsingTraceStream<'a> {
    inner: StreamingXESParser<'a>,
    /// Error encountered while parsing XES
    pub error: Option<XESParseError>,
}
/// [`XESParsingTraceStream`] and [`XESOuterLogData`]
///
/// First component is trace stream lazily parsed, second component provides top-level log information (eagerly parsed at the beginning)
pub type XESParsingStreamAndLogData<'a> = (XESParsingTraceStream<'a>, XESOuterLogData);

impl Iterator for &mut XESParsingTraceStream<'_> {
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

impl FusedIterator for &mut XESParsingTraceStream<'_> {}

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
    /// Try to construct a new [`XESParsingTraceStream`] and directly try to parse until the first trace
    ///
    /// As all log attributes must occur before the first trace, this already returns the parsed [`XESOuterLogData`]
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
/// Stream XES [`Trace`]s from a byte slice
///
/// The returned [`XESParsingStreamAndLogData`] contains the [`XESOuterLogData`] and can be used to iterate over [`Trace`]s
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
/// Stream XES [`Trace`]s from a gzipped byte slice
///
/// The returned [`XESParsingStreamAndLogData`] contains the [`XESOuterLogData`] and can be used to iterate over [`Trace`]s
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
/// Stream XES [`Trace`]s from a file
///
/// The returned [`XESParsingStreamAndLogData`] contains the [`XESOuterLogData`] and can be used to iterate over [`Trace`]s
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
/// Stream XES [`Trace`]s from a gzipped file
///
/// The returned [`XESParsingStreamAndLogData`] contains the [`XESOuterLogData`] and can be used to iterate over [`Trace`]s
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
/// Stream XES [`Trace`]s from path (auto-detecting gz compression from file extension)
///
/// The returned [`XESParsingStreamAndLogData`] contains the [`XESOuterLogData`] and can be used to iterate over [`Trace`]s
///
pub fn stream_xes_from_path<'a, P: AsRef<std::path::Path>>(
    path: P,
    options: XESImportOptions,
) -> Result<XESParsingStreamAndLogData<'a>, XESParseError> {
    let is_gz = path
        .as_ref()
        .as_os_str()
        .to_str()
        .is_some_and(|p| p.ends_with(".gz"));
    let file = File::open(path)?;
    if is_gz {
        stream_xes_file_gz(file, options)
    } else {
        stream_xes_file(file, options)
    }
}

fn get_attribute_string(t: &BytesStart<'_>, key: &'static str) -> Option<String> {
    if let Ok(Some(attr)) = t.try_get_attribute(key) {
        return Some(String::from_utf8_lossy(&attr.value).to_string());
    }
    // eprintln!(
    //     "Did not find expected XML attribute with key {:?}. Will assume empty string as value.",
    //     key
    // );
    None
}

fn parse_attribute_value_from_tag(
    t: &BytesStart<'_>,
    mode: &Mode,
    options: &XESImportOptions,
) -> AttributeValue {
    let attribute_val: Option<AttributeValue> = match t.name().as_ref() {
        b"container" => Some(AttributeValue::Container(Attributes::new())),
        b"list" => Some(AttributeValue::List(Vec::new())),
        _ => {
            let value = get_attribute_string(t, "value");
            if let Some(value) = value {
                match t.name().as_ref() {
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
                }
            } else {
                None
            }
        }
    };
    attribute_val.unwrap_or(AttributeValue::None())
}

fn parse_date_from_str(value: &str, date_format: &Option<String>) -> Option<DateTime<FixedOffset>> {
    // Is a date_format string provided?
    if let Some(date_format) = &date_format {
        if let Ok(dt) = DateTime::parse_from_str(value, date_format) {
            return Some(dt);
        }
        // If parsing with DateTime with provided date format fail, try to parse NaiveDateTime using format (i.e., without time-zone, assuming UTC)
        if let Ok(dt) = NaiveDateTime::parse_from_str(value, date_format) {
            return Some(dt.and_utc().fixed_offset());
        }
    }

    // Default parsing options for commonly used formats

    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Some(dt);
    }

    if let Ok(dt) = DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S:%f%:z") {
        return Some(dt);
    }

    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(dt.and_utc().fixed_offset());
    }

    None
}

#[cfg(test)]
mod stream_test {
    use std::{collections::HashSet, time::Instant};

    use crate::{
        event_log::import_xes::build_ignore_attributes, stream_xes_from_path,
        utils::test_utils::get_test_data_path, XESImportOptions,
    };

    #[test]
    fn test_xes_stream() {
        let path = get_test_data_path().join("xes").join("RepairExample.xes");
        let (mut stream, _log_data) =
            stream_xes_from_path(&path, XESImportOptions::default()).unwrap();
        let num_traces = stream.count();
        println!("Num. traces: {}", num_traces);
        assert_eq!(num_traces, 1104);
    }

    #[test]
    pub fn test_streaming_variants() {
        let path = get_test_data_path()
            .join("xes")
            .join("Road_Traffic_Fine_Management_Process.xes.gz");
        let now = Instant::now();
        let (mut log_stream, log_data) =
            stream_xes_from_path(&path, XESImportOptions::default()).unwrap();
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

    #[test]
    pub fn test_stream_ignoring_attributes() {
        let path = get_test_data_path().join("xes").join("nested-attrs.xes");
        let (_log_stream, log_data) = stream_xes_from_path(
            &path,
            XESImportOptions {
                ignore_event_attributes_except: Some(HashSet::new()),
                ignore_trace_attributes_except: Some(HashSet::new()),
                ignore_log_attributes_except: Some(HashSet::new()),
                ..Default::default()
            },
        )
        .unwrap();
        println!("{:#?}", log_data.log_attributes);
        assert!(log_data.log_attributes.is_empty());
    }
}
