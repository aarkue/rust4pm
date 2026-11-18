use crate::core::{
    event_data::{
        case_centric::{
            io::{EventLogFormat, EventLogIOError},
            xes::import_xes::XESImportOptions,
        },
        timestamp_utils::parse_timestamp,
    },
    io::read_xml_text_unescaped,
};
use crate::utils::control::Control;

use super::{
    super::event_log_struct::{
        Attribute, AttributeValue, Attributes, Event, EventLogClassifier, EventLogExtension, Trace,
        XESEditableAttribute,
    },
    import_xes::XESParseError,
};
use chrono::{DateTime, FixedOffset};
use flate2::read::MultiGzDecoder;
use quick_xml::{errors::IllFormedError, events::BytesStart, Reader};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
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
    /// The XES version associated with the log tag
    pub xes_version: Option<String>,
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
pub(crate) enum Mode {
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

/// How deep attributes may nest in an XES file, as `serde_json` allows for JSON.
///
/// Dropping, cloning and exporting an [`Attribute`] recurse into its children, so a file nesting
/// far deeper would overflow the stack, which aborts the process.
pub const MAX_ATTRIBUTE_DEPTH: usize = 128;

/// A tag opened below a trace, event, log or global element (i.e., an attribute or part of one)
#[derive(Clone, Copy, Debug)]
enum OpenAttrTag {
    /// An attribute pushed onto `current_nested_attributes`
    ///
    /// `has_values` records whether a list has a `<values>` child, which decides whether its
    /// direct children are list items (no `<values>`, as written by older exporters) or
    /// meta-attributes (XES standard).
    Attribute { has_values: bool },
    /// The `<values>` element of an open list
    ListValues,
    /// A tag that is not an attribute itself; its children belong to the enclosing element
    Transparent,
    /// An ignored attribute; it and everything inside it is skipped
    Skipped,
}

/// Streaming XES Parser over [`Trace`]s
pub(crate) struct StreamingXESParser<'a> {
    /// The source is boxed so that [`XESParsingTraceStream`] is not generic over gz- and
    /// non-gz-readers
    reader: Reader<Box<dyn BufRead + 'a>>,
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
    /// Currently open attribute-level tags, innermost last
    open_attr_tags: Vec<OpenAttrTag>,
    /// Whether `current_trace` is complete and must be emitted on the next call (an empty
    /// `<trace/>` read while the log data was emitted first)
    emit_current_trace_next: bool,
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
    ctl: &'a Control<'a>,
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
            .field("open_attr_tags", &self.open_attr_tags)
            .field("emit_current_trace_next", &self.emit_current_trace_next)
            .field("options", &self.options)
            .field("encountered_log", &self.encountered_log)
            .field("log_data", &self.log_data)
            .field("log_data_emitted", &self.log_data_emitted)
            .field("finished", &self.finished)
            .finish()
    }
}

#[derive(Debug)]
/// Enum of possible data streamed by [`StreamingXESParser`]
pub(crate) enum XESNextStreamElement {
    /// Log data
    LogData(XESOuterLogData),
    /// Trace data
    Trace(Trace),
    /// Encountered error
    Error(EventLogIOError),
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
    pub(crate) fn next_trace(&mut self) -> Option<XESNextStreamElement> {
        // Helper function to terminate parsing and set the error fields
        fn terminate_with_error(
            myself: &mut StreamingXESParser<'_>,
            error: impl Into<EventLogIOError>,
        ) -> Option<XESNextStreamElement> {
            myself.finished = true;
            Some(XESNextStreamElement::Error(error.into()))
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
        if let Err(cancelled) = self.ctl.check() {
            return terminate_with_error(self, cancelled);
        }
        if self.emit_current_trace_next {
            self.emit_current_trace_next = false;
            return emit_trace_data(self);
        }
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
                            "trace" => {
                                self.current_mode = Mode::Trace;
                                self.current_trace = Some(Trace {
                                    attributes: Attributes::with_capacity(10),
                                    events: Vec::with_capacity(10),
                                });
                                if !self.log_data_emitted {
                                    return emit_log_data(self);
                                }
                            }
                            "event" => {
                                self.current_mode = Mode::Event;
                                match &mut self.current_trace {
                                    Some(t) => {
                                        t.events.push(Event {
                                            attributes: Attributes::with_capacity(10),
                                        });
                                    }
                                    None => {
                                        self.ctl.warn("Invalid XES format: Event without trace");
                                    }
                                }
                            }
                            "global" => match t.try_get_attribute("scope") {
                                Ok(Some(a)) => match a.value.as_ref() {
                                    "trace" => self.current_mode = Mode::GlobalTraceAttributes,
                                    "event" => self.current_mode = Mode::GlobalEventAttributes,
                                    _ => {
                                        return terminate_with_error(
                                            self,
                                            XESParseError::InvalidKeyValue("scope"),
                                        )
                                    }
                                },
                                // IEEE 1849: `scope` is optional and defaults to `event`
                                Ok(None) => self.current_mode = Mode::GlobalEventAttributes,
                                Err(e) => {
                                    return terminate_with_error(self, quick_xml::Error::from(e));
                                }
                            },
                            "log" => {
                                if self.encountered_log {
                                    self.ctl.warn(
                                        "Encountered two log tags. This is not a valid XES file",
                                    );
                                }
                                self.log_data.xes_version = get_attribute_string(&t, "xes.version");
                                self.encountered_log = true;
                                self.current_mode = Mode::Log
                            }
                            "extension" => {
                                parse_extension(&t, &mut self.log_data);
                            }
                            "classifier" => {
                                parse_classifier(&t, &mut self.log_data);
                            }
                            _x => {
                                if !self.encountered_log {
                                    return terminate_with_error(
                                        self,
                                        XESParseError::NoTopLevelLog,
                                    );
                                }
                                let tag = if matches!(
                                    self.open_attr_tags.last(),
                                    Some(OpenAttrTag::Skipped)
                                ) {
                                    OpenAttrTag::Skipped
                                } else if t.name().as_ref() == "values" {
                                    match (
                                        self.open_attr_tags.last_mut(),
                                        self.current_nested_attributes.last(),
                                    ) {
                                        (
                                            Some(OpenAttrTag::Attribute { has_values }),
                                            Some(Attribute {
                                                value: AttributeValue::List(_),
                                                ..
                                            }),
                                        ) => {
                                            *has_values = true;
                                            OpenAttrTag::ListValues
                                        }
                                        _ => OpenAttrTag::Transparent,
                                    }
                                } else {
                                    // Nested attribute!
                                    let key = get_attribute_string(&t, "key").unwrap_or_default();
                                    if should_ignore_attribute(
                                        &self.options,
                                        &self.current_mode,
                                        &key,
                                    ) {
                                        OpenAttrTag::Skipped
                                    } else {
                                        let value = parse_attribute_value_from_tag(
                                            &t,
                                            &self.current_mode,
                                            &self.options,
                                            self.ctl,
                                        );
                                        if key.is_empty() && matches!(value, AttributeValue::None())
                                        {
                                            OpenAttrTag::Transparent
                                        } else if self.current_nested_attributes.len()
                                            >= MAX_ATTRIBUTE_DEPTH
                                        {
                                            return terminate_with_error(
                                                self,
                                                XESParseError::AttributeNestingTooDeep,
                                            );
                                        } else {
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
                                            OpenAttrTag::Attribute { has_values: false }
                                        }
                                    }
                                };
                                self.open_attr_tags.push(tag);
                            }
                        },
                        quick_xml::events::Event::Empty(t) => match t.name().as_ref() {
                            "global" => {}
                            "extension" => {
                                parse_extension(&t, &mut self.log_data);
                            }
                            "classifier" => {
                                parse_classifier(&t, &mut self.log_data);
                            }
                            "log" => {
                                // Empty log, but still a log
                                self.encountered_log = true;
                                self.current_mode = Mode::None;
                                // Send (empty) log_data anyways
                                if !self.log_data_emitted {
                                    return emit_log_data(self);
                                }
                            }
                            "trace" => {
                                self.current_trace = Some(Trace {
                                    attributes: Attributes::new(),
                                    events: Vec::new(),
                                });
                                if !self.log_data_emitted {
                                    self.emit_current_trace_next = true;
                                    return emit_log_data(self);
                                }
                                return emit_trace_data(self);
                            }
                            "event" => match &mut self.current_trace {
                                Some(t) => {
                                    t.events.push(Event {
                                        attributes: Attributes::new(),
                                    });
                                }
                                None => {
                                    self.ctl.warn("Invalid XES format: Event without trace");
                                }
                            },
                            _ => {
                                if !self.encountered_log {
                                    return terminate_with_error(
                                        self,
                                        XESParseError::NoTopLevelLog,
                                    );
                                }
                                match self.open_attr_tags.last_mut() {
                                    Some(OpenAttrTag::Skipped) => {}
                                    // `<values/>`: an empty list in XES standard form
                                    Some(OpenAttrTag::Attribute { has_values })
                                        if t.name().as_ref() == "values" =>
                                    {
                                        *has_values = true;
                                    }
                                    _ if t.name().as_ref() == "values" => {}
                                    top => {
                                        let in_list_values =
                                            matches!(top, Some(OpenAttrTag::ListValues));
                                        if !StreamingXESParser::add_attribute_from_tag(
                                            &self.current_mode,
                                            &mut self.current_trace,
                                            &mut self.log_data,
                                            &mut self.current_nested_attributes,
                                            in_list_values,
                                            &self.options,
                                            &t,
                                            self.ctl,
                                        ) {
                                            return terminate_with_error(
                                                self,
                                                XESParseError::AttributeOutsideLog,
                                            );
                                        }
                                    }
                                }
                            }
                        },
                        quick_xml::events::Event::End(t) => {
                            match t.name().as_ref() {
                                "event" => self.current_mode = Mode::Trace,
                                "trace" => {
                                    self.current_mode = Mode::Log;
                                    return emit_trace_data(self);
                                }
                                "log" => self.current_mode = Mode::None,
                                "global" => self.current_mode = Mode::Log,
                                "extension" | "classifier" => {}
                                _ => {
                                    match self.open_attr_tags.pop() {
                                        Some(OpenAttrTag::Attribute { has_values }) => {
                                            if let Some(mut attr) =
                                                self.current_nested_attributes.pop()
                                            {
                                                if !has_values {
                                                    if let AttributeValue::List(items) =
                                                        &mut attr.value
                                                    {
                                                        // No `<values>`: the direct children
                                                        // collected as meta-attributes are the
                                                        // list items
                                                        if let Some(children) =
                                                            attr.own_attributes.take()
                                                        {
                                                            *items = children;
                                                        }
                                                    }
                                                }
                                                if let Some(current_nested) =
                                                    self.current_nested_attributes.last_mut()
                                                {
                                                    add_nested_attribute(
                                                        current_nested,
                                                        attr,
                                                        matches!(
                                                            self.open_attr_tags.last(),
                                                            Some(OpenAttrTag::ListValues)
                                                        ),
                                                    );
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
                                                                self.ctl.warn("Skipping attribute of an event outside a trace");
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
                                                self.ctl.warn(
                                                    "Attribute mode but no open nested attributes",
                                                );
                                                self.current_mode = self.last_mode_before_attr;
                                            }
                                        }
                                        Some(
                                            OpenAttrTag::ListValues
                                            | OpenAttrTag::Transparent
                                            | OpenAttrTag::Skipped,
                                        )
                                        | None => {}
                                    }
                                }
                            }
                        }
                        quick_xml::events::Event::Eof => {
                            if !self.encountered_log {
                                // If there was no (top-level) log tag, this was not a valid XES file!
                                return terminate_with_error(self, XESParseError::NoTopLevelLog);
                            }
                            let unclosed = match self.current_mode {
                                Mode::None => None,
                                Mode::Log => Some("log"),
                                Mode::Trace => Some("trace"),
                                Mode::Event => Some("event"),
                                Mode::GlobalTraceAttributes | Mode::GlobalEventAttributes => {
                                    Some("global")
                                }
                                Mode::Attribute => Some("attribute"),
                            };
                            if let Some(tag) = unclosed {
                                // Truncated input: quick-xml reports Eof without checking for
                                // tags still open
                                return terminate_with_error(
                                    self,
                                    quick_xml::Error::from(IllFormedError::MissingEndTag(
                                        tag.to_string(),
                                    )),
                                );
                            }
                            if !self.log_data_emitted {
                                return emit_log_data(self);
                            }
                            self.finished = true;
                            return None;
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    return terminate_with_error(self, e);
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
            let s = String::from_iter(buffer.iter());
            if log_data.global_event_attrs.iter().any(|attr| attr.key == s) {
                // Test if there is a global event attribute with the same name (otherwise, we might want to try expand with space)?
                ret.push(s);
                buffer.clear();
            } else if let Some((len, key)) = global_key_continuing(&s, &chars[i..], log_data) {
                ret.push(key.to_string());
                buffer.clear();
                i += len;
                continue;
            } else {
                // i.e., c is a space, not inside quotes
                //  and no global attribute matching
                // In this case: Push the current buffer to ret
                if !buffer.is_empty() {
                    ret.push(s);
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

/// The shortest global event attribute key that is `prefix` followed by the start of `rest`
/// (which starts with a space) up to a space or its end, with how many chars of `rest` it takes.
///
/// Compares each key once, so a long key of spaces cannot make classifier parsing cubic.
fn global_key_continuing<'a>(
    prefix: &str,
    rest: &[char],
    log_data: &'a XESOuterLogData,
) -> Option<(usize, &'a str)> {
    log_data
        .global_event_attrs
        .iter()
        .filter_map(|attr| {
            let tail = attr.key.strip_prefix(prefix)?;
            let len = tail.chars().count();
            (tail.starts_with(' ')
                && rest.len() >= len
                && rest[..len].iter().copied().eq(tail.chars())
                && rest.get(len).is_none_or(|c| *c == ' '))
            .then_some((len, attr.key.as_str()))
        })
        .min_by_key(|(len, _)| *len)
}

/// Add `child` to `parent`: as an element of a container, as an item of a list when read inside
/// its `<values>`, and as a meta-attribute otherwise (see [`OpenAttrTag::Attribute`] for list
/// children outside `<values>`).
fn add_nested_attribute(parent: &mut Attribute, child: Attribute, in_list_values: bool) {
    match &mut parent.value {
        AttributeValue::Container(c) => c.push(child),
        AttributeValue::List(l) if in_list_values => l.push(child),
        _ => parent
            .own_attributes
            .get_or_insert_with(Attributes::new)
            .push(child),
    }
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
        xes_version: None,
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

#[test]
fn test_classifier_parse_with_a_long_global_key_is_not_cubic() {
    let words = "a ".repeat(5_000);
    let data = XESOuterLogData {
        xes_version: None,
        extensions: Vec::new(),
        classifiers: Vec::new(),
        log_attributes: Vec::new(),
        global_trace_attrs: Vec::new(),
        global_event_attrs: vec![Attribute {
            key: format!("{words}Z"),
            value: AttributeValue::String(String::new()),
            own_attributes: None,
        }],
    };
    // A long global key must not be rescanned from every space of the classifier keys
    let keys = parse_classifier_key(words.trim_end().to_string(), &data);
    assert_eq!(keys.len(), 5_000);
}

impl StreamingXESParser<'_> {
    ///
    /// Add XES attribute from tag to the currently active element (indicated by `current_mode`)
    ///
    #[allow(clippy::too_many_arguments)]
    fn add_attribute_from_tag(
        current_mode: &Mode,
        current_trace: &mut Option<Trace>,
        log_data: &mut XESOuterLogData,
        current_nested_attributes: &mut [Attribute],
        in_list_values: bool,
        options: &XESImportOptions,
        t: &BytesStart<'_>,
        ctl: &Control<'_>,
    ) -> bool {
        let key = get_attribute_string(t, "key").unwrap_or_default();
        if should_ignore_attribute(options, current_mode, &key) {
            return true;
        }

        let val = parse_attribute_value_from_tag(t, current_mode, options, ctl);
        match current_mode {
            Mode::Trace => match current_trace {
                Some(t) => {
                    t.attributes.add_to_attributes(key, val);
                }
                None => {
                    ctl.warn(format_args!(
                        "No current trace when parsing trace attribute: Key {key:?}, Value {val:?}"
                    ));
                }
            },
            Mode::Event => match current_trace {
                Some(t) => match t.events.last_mut() {
                    Some(e) => {
                        e.attributes.add_to_attributes(key, val);
                    }
                    None => {
                        ctl.warn(format_args!(
                            "No current event when parsing event attribute: Key {key:?}, Value {val:?}"
                        ));
                    }
                },
                None => {
                    ctl.warn(format_args!(
                        "No current trace when parsing event attribute: Key {key:?}, Value {val:?}"
                    ));
                }
            },

            Mode::Log => {
                log_data.log_attributes.add_to_attributes(key, val);
            }
            Mode::None => return false,
            Mode::Attribute => {
                if let Some(last_attr) = current_nested_attributes.last_mut() {
                    add_nested_attribute(last_attr, Attribute::new(key, val), in_list_values);
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
    /// Error encountered while parsing XES, which ends the iteration
    pub error: Option<EventLogIOError>,
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
                self.error = Some(XESParseError::ExpectedTraceData.into());
                None
            }
            None => None,
        }
    }
}

impl FusedIterator for &mut XESParsingTraceStream<'_> {}

impl<'a> XESParsingTraceStream<'a> {
    /// Set or update parsing options
    pub fn set_options(&mut self, options: XESImportOptions) {
        self.inner.options = options;
    }

    /// The parsing options
    pub fn get_options(&self) -> &XESImportOptions {
        &self.inner.options
    }

    /// Try to construct a new [`XESParsingTraceStream`] and directly try to parse until the first trace
    ///
    /// As all log attributes must occur before the first trace, this already returns the parsed [`XESOuterLogData`]
    pub(crate) fn try_new(
        reader: Box<dyn BufRead + 'a>,
        options: XESImportOptions,
        ctl: &'a Control<'a>,
    ) -> Result<(Self, XESOuterLogData), EventLogIOError> {
        let mut reader = Reader::from_reader(reader);
        reader.config_mut().trim_text(true);
        // Text is ignored, so a lone `&` in it is no error
        reader.config_mut().allow_dangling_amp = true;
        let log_data = XESOuterLogData::default();
        let mut s = StreamingXESParser {
            reader,
            current_mode: Mode::Log,
            current_trace: None,
            last_mode_before_attr: Mode::Log,
            encountered_log: false,
            current_nested_attributes: Vec::new(),
            open_attr_tags: Vec::new(),
            emit_current_trace_next: false,
            options,
            log_data,
            log_data_emitted: false,
            buf: Vec::new(),
            finished: false,
            ctl,
        };
        let next = s.next_trace();
        match next {
            Some(el) => match el {
                XESNextStreamElement::Error(e) => Err(e),
                XESNextStreamElement::Trace(_) => {
                    s.ctl
                        .warn("Encountered trace before LogData; This should not happen!");
                    Err(XESParseError::ExpectedLogData.into())
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
                s.ctl.warn(
                    "Iterator initially empty. Expected log data or error; This should not happen!",
                );
                Err(XESParseError::ExpectedLogData.into())
            }
        }
    }
}

/// `reader` as a buffered reader of XES, gunzipping it if `gz` is set
pub(crate) fn xes_reader<'a>(reader: impl Read + 'a, gz: bool) -> Box<dyn BufRead + 'a> {
    if gz {
        Box::new(BufReader::new(MultiGzDecoder::new(reader)))
    } else {
        Box::new(BufReader::new(reader))
    }
}

/// Stream XES [`Trace`]s from `reader` in `format` (`xes` or `xes.gz`, in any case)
///
/// The returned [`XESParsingStreamAndLogData`] contains the [`XESOuterLogData`] and can be used
/// to iterate over [`Trace`]s. An error while iterating ends the iteration and is kept in
/// [`XESParsingTraceStream::error`].
pub fn stream_xes<'a>(
    reader: impl Read + 'a,
    format: &str,
    options: XESImportOptions,
) -> Result<XESParsingStreamAndLogData<'a>, EventLogIOError> {
    static NO_CONTROL: Control<'static> = Control::new();
    let gz = EventLogFormat::parse_xes_gz(format)?;
    XESParsingTraceStream::try_new(xes_reader(reader, gz), options, &NO_CONTROL)
}

fn get_attribute_string(t: &BytesStart<'_>, key: &'static str) -> Option<String> {
    if let Ok(Some(attr)) = t.try_get_attribute(key) {
        return Some(read_xml_text_unescaped(&attr.value));
    }
    None
}

fn parse_attribute_value_from_tag(
    t: &BytesStart<'_>,
    mode: &Mode,
    options: &XESImportOptions,
    ctl: &Control<'_>,
) -> AttributeValue {
    let tag = t.name();
    let tag = tag.as_ref();
    match tag {
        "container" => return AttributeValue::Container(Attributes::new()),
        "list" => return AttributeValue::List(Vec::new()),
        _ => {}
    }
    let Some(value) = get_attribute_string(t, "value") else {
        return AttributeValue::None();
    };
    if tag == "string" {
        return AttributeValue::String(value);
    }
    // Leading and trailing whitespace is insignificant for the XSD types of the other values
    let trimmed = value.trim();
    let parsed = match tag {
        "date" => parse_date_from_str(trimmed, &options.date_format).map(AttributeValue::Date),
        "int" => trimmed.parse::<i64>().ok().map(AttributeValue::Int),
        "float" => trimmed.parse::<f64>().ok().map(AttributeValue::Float),
        "boolean" => match trimmed {
            "true" | "1" => Some(AttributeValue::Boolean(true)),
            "false" | "0" => Some(AttributeValue::Boolean(false)),
            // Not xs:boolean, but written by some tools and accepted by OpenXES
            s if s.eq_ignore_ascii_case("true") => Some(AttributeValue::Boolean(true)),
            s if s.eq_ignore_ascii_case("false") => Some(AttributeValue::Boolean(false)),
            _ => None,
        },
        "id" => Uuid::from_str(trimmed).ok().map(AttributeValue::ID),
        _ => {
            if !matches!(mode, Mode::Log) {
                ctl.warn(format_args!(
                    "Attribute type not implemented '{tag}' in mode {mode:?}"
                ));
            }
            return AttributeValue::None();
        }
    };
    parsed.unwrap_or_else(|| {
        ctl.warn(format_args!("Could not parse {tag} from {value:?}"));
        AttributeValue::None()
    })
}

fn parse_date_from_str(value: &str, date_format: &Option<String>) -> Option<DateTime<FixedOffset>> {
    parse_timestamp(value, date_format.as_deref())
        .ok()
        .or_else(|| {
            let dotted = colon_fraction_to_dot(value)?;
            parse_timestamp(&dotted, date_format.as_deref()).ok()
        })
}

/// Some logs put a colon before the fraction (`09:30:21:123`); this turns it into a dot.
fn colon_fraction_to_dot(value: &str) -> Option<String> {
    let t = value.find('T')?;
    let mut colons = value[t..].match_indices(':').map(|(i, _)| t + i);
    let (_, seconds, fraction) = (colons.next()?, colons.next()?, colons.next()?);
    let is_seconds = fraction == seconds + 3
        && value[seconds + 1..fraction]
            .bytes()
            .all(|b| b.is_ascii_digit());
    is_seconds.then(|| format!("{}.{}", &value[..fraction], &value[fraction + 1..]))
}

#[cfg(test)]
mod stream_test {
    use std::{collections::HashSet, fs::File, time::Instant};

    use crate::{
        core::event_data::case_centric::xes::{
            import_xes::{build_ignore_attributes, XESImportOptions},
            stream_xes::stream_xes,
        },
        test_utils::get_test_data_path,
    };

    #[test]
    fn test_xes_stream() {
        let path = get_test_data_path().join("xes").join("RepairExample.xes");
        let (mut stream, log_data) = stream_xes(
            File::open(&path).unwrap(),
            "xes",
            XESImportOptions::default(),
        )
        .unwrap();
        assert_eq!(log_data.xes_version, Some("1.0".to_string()));
        let num_traces = stream.count();
        println!("Num. traces: {num_traces}");
        assert_eq!(num_traces, 1104);
    }

    #[test]
    pub fn test_streaming_variants() {
        let path = get_test_data_path()
            .join("xes")
            .join("Road_Traffic_Fine_Management_Process.xes.gz");
        let now = Instant::now();
        let (mut log_stream, log_data) = stream_xes(
            File::open(&path).unwrap(),
            "xes.gz",
            XESImportOptions::default(),
        )
        .unwrap();
        assert_eq!(log_data.xes_version, Some("1.0".to_string()));
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
        // Gather unique variants of traces (wrt. the hardcoded)
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
        let (_log_stream, log_data) = stream_xes(
            File::open(&path).unwrap(),
            "XES",
            XESImportOptions {
                ignore_event_attributes_except: Some(HashSet::new()),
                ignore_trace_attributes_except: Some(HashSet::new()),
                ignore_log_attributes_except: Some(HashSet::new()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(log_data.xes_version, Some("2.0".to_string()));
        println!("{:#?}", log_data.log_attributes);
        assert!(log_data.log_attributes.is_empty());
    }
}
