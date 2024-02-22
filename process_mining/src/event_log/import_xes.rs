use std::collections::HashSet;

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::str::FromStr;

use crate::event_log::event_log_struct::{AttributeValue, Attributes, EventLog, Trace};
use chrono::{DateTime, NaiveDateTime, Utc};
use flate2::bufread::GzDecoder;
use quick_xml::escape::unescape;
use quick_xml::events::BytesStart;
use quick_xml::Error as QuickXMLError;
use quick_xml::Reader;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::stream_xes::{construct_log_data_cell, XESTraceStreamLogDataRefCell};

#[derive(Clone, Copy, Debug)]
///
/// Current Parsing Mode (i.e., which tag is currently open / being parsed)
///
pub enum Mode {
    Trace,
    Event,
    Attribute,
    Global,
    Log,
    None,
}

///
/// Error encountered while parsing XES
///
#[derive(Debug, Clone)]
pub enum XESParseError {
    AttributeOutsideLog,
    NoTopLevelLog,
    MissingLastEvent,
    MissingLastTrace,
    InvalidMode,
    IOError(std::rc::Rc<std::io::Error>),
    XMLParsingError(QuickXMLError),
}

impl std::fmt::Display for XESParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to parse XES: {:?}", self)
    }
}

impl std::error::Error for XESParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            XESParseError::IOError(e) => Some(e.as_ref()),
            XESParseError::XMLParsingError(e) => Some(e),
            _ => None,
        }
    }

    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}

impl From<std::io::Error> for XESParseError {
    fn from(e: std::io::Error) -> Self {
        Self::IOError(std::rc::Rc::new(e))
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
        .map(|s| s.as_ref().as_bytes().to_vec())
        .collect()
}

pub fn import_xes<T>(reader: T, options: XESImportOptions) -> Result<EventLog, XESParseError>
where
    T: BufRead,
{
    let log_data: XESTraceStreamLogDataRefCell = construct_log_data_cell();
    let trace_stream = super::stream_xes::XESTraceStreamParser::try_new(
        Box::new(Reader::from_reader(Box::new(reader))),
        options,
        &log_data,
    )?;
    let traces: Vec<Trace> = trace_stream.stream().collect();
    let log_data_owned = log_data.take();

    if let Some(e) = log_data_owned.terminated_on_error {
        return Err(e);
    }

    Ok(EventLog {
        attributes: log_data_owned.log_attributes,
        traces,
        extensions: Some(log_data_owned.extensions),
        classifiers: Some(log_data_owned.classifiers),
    })
}

///
/// Import a XES [EventLog] from a file path
///
pub fn import_xes_file(path: &str, options: XESImportOptions) -> Result<EventLog, XESParseError> {
    if path.ends_with(".gz") {
        let file = File::open(path)?;
        let dec: GzDecoder<BufReader<&File>> = GzDecoder::new(BufReader::new(&file));
        let reader = BufReader::new(dec);
        import_xes(reader, options)
    } else {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        import_xes(reader, options)
    }
}

///
/// Import a XES [EventLog] directly from a string
///
pub fn import_xes_str(xes_str: &str, options: XESImportOptions) -> Result<EventLog, XESParseError> {
    let reader = BufReader::new(xes_str.as_bytes());
    import_xes(reader, options)
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
        return import_xes(reader, options);
    }
    import_xes(BufReader::new(xes_data), options)
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

// ///
// /// Parse an attribute from a tag (reading the "key" and "value" fields) and parsing the inner value
// ///
// fn add_attribute_from_tag(
//     t: &BytesStart,
//     mode: Mode,
//     log: &mut EventLog,
//     current_nested_attributes: &mut [Attribute],
//     options: &XESImportOptions,
// ) -> bool {
//     if options.ignore_event_attributes_except.is_some()
//         || options.ignore_trace_attributes_except.is_some()
//         || options.ignore_log_attributes_except.is_some()
//     {
//         let key = t.try_get_attribute("key").unwrap().unwrap().value;
//         if matches!(mode, Mode::Event)
//             && options
//                 .ignore_event_attributes_except
//                 .as_ref()
//                 .is_some_and(|not_ignored| !not_ignored.contains(key.as_ref()))
//         {
//             return true;
//         }
//         if matches!(mode, Mode::Trace)
//             && options
//                 .ignore_trace_attributes_except
//                 .as_ref()
//                 .is_some_and(|not_ignored| !not_ignored.contains(key.as_ref()))
//         {
//             return true;
//         }

//         if matches!(mode, Mode::Log)
//             && options
//                 .ignore_log_attributes_except
//                 .as_ref()
//                 .is_some_and(|ignored| !ignored.contains(key.as_ref()))
//         {
//             return true;
//         }
//     }

//     let (key, val) = parse_attribute_from_tag(t, &mode, options);
//     match mode {
//         Mode::Trace => match log.traces.last_mut() {
//             Some(t) => {
//                 t.attributes.add_to_attributes(key, val);
//             }
//             None => {
//                 eprintln!(
//                     "No current trace when parsing trace attribute: Key {:?}, Value {:?}",
//                     key, val
//                 );
//             }
//         },
//         Mode::Event => match log.traces.last_mut() {
//             Some(t) => match t.events.last_mut() {
//                 Some(e) => {
//                     e.attributes.add_to_attributes(key, val);
//                 }
//                 None => {
//                     eprintln!(
//                         "No current event when parsing event attribute: Key {:?}, Value {:?}",
//                         key, val
//                     )
//                 }
//             },
//             None => {
//                 eprintln!(
//                     "No current trace when parsing event attribute: Key {:?}, Value {:?}",
//                     key, val
//                 );
//             }
//         },

//         Mode::Log => {
//             log.attributes.add_to_attributes(key, val);
//         }
//         Mode::None => return false,
//         Mode::Attribute => {
//             let last_attr = current_nested_attributes.last_mut().unwrap();
//             last_attr.value = match last_attr.value.clone() {
//                 AttributeValue::List(mut l) => {
//                     l.push(Attribute {
//                         key,
//                         value: val,
//                         own_attributes: None,
//                     });
//                     AttributeValue::List(l)
//                 }
//                 AttributeValue::Container(mut c) => {
//                     c.add_to_attributes(key, val);
//                     AttributeValue::Container(c)
//                 }
//                 x => {
//                     if let Some(own_attributes) = &mut last_attr.own_attributes {
//                         own_attributes.add_to_attributes(key, val);
//                     } else {
//                         return false;
//                     }
//                     x
//                 }
//             };
//         }
//         Mode::Global => {}
//     }
//     true
// }
