use std::collections::HashSet;

use std::fs::File;
use std::io::{BufRead, BufReader};

use crate::event_log::event_log_struct::{EventLog, Trace};

use flate2::bufread::GzDecoder;

use quick_xml::Error as QuickXMLError;
use quick_xml::Reader;
use serde::{Deserialize, Serialize};



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
    MissingKey(&'static str),
    InvalidKeyValue(&'static str),
    ExpectedLogData,
    ExpectedTraceData,
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
    let (mut trace_stream, log_data) = super::stream_xes::XESParsingTraceStream::try_new(
        Box::new(Reader::from_reader(Box::new(reader))),
        options
    )?;

    let traces: Vec<Trace> = trace_stream.collect();

    if let Some(e) = trace_stream.error {
        return Err(e);
    }

    Ok(EventLog {
        attributes: log_data.log_attributes,
        traces,
        extensions: Some(log_data.extensions),
        classifiers: Some(log_data.classifiers),
        global_trace_attrs: Some(log_data.global_trace_attrs),
        global_event_attrs: Some(log_data.global_event_attrs),
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
