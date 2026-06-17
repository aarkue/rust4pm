use std::{
    collections::HashMap,
    convert::Infallible,
    io::{BufRead, BufReader},
};

use chrono::{DateTime, FixedOffset};
use quick_xml::{events::BytesStart, Reader};
use serde::{Deserialize, Serialize};

use crate::core::{
    event_data::{
        object_centric::{
            appendable::AppendableOCEL,
            io::OCELIOError,
            ocel_struct::{
                OCELAttributeType, OCELAttributeValue, OCELEventAttribute, OCELObjectAttribute,
                OCELRelationship, OCELType, OCELTypeAttribute, OCEL,
            },
        },
        timestamp_utils::parse_timestamp,
    },
    io::read_xml_text_unescaped,
};

///
/// Options for OCEL Import
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OCELImportOptions {
    /// Verbosely log errors or warnings (e.g., for missing referenced objects or invalid attribute values)
    pub verbose: bool,
    /// Optional date format to use when parsing `DateTimes` (first trying [`chrono::DateTime`] then falling back to [`chrono::NaiveDateTime`] with UTC timezone).
    ///
    /// See <https://docs.rs/chrono/latest/chrono/format/strftime/index.html> for all available Specifiers.
    ///
    /// Will fall back to default formats (e.g., rfc3339) if parsing fails using passed `date_format`
    pub date_format: Option<String>,
}

impl Default for OCELImportOptions {
    fn default() -> Self {
        Self {
            verbose: true,
            date_format: None,
        }
    }
}

///
/// Current Parsing Mode (i.e., which tag is currently open / being parsed)
///
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
enum Mode {
    Objects,
    Events,
    Object,
    Event,
    ObjectTypes,
    ObjectType,
    ObjectTypeAttributes,
    EventTypes,
    EventType,
    EventTypeAttributes,
    Log,
    None,
}

impl From<Infallible> for OCELIOError {
    fn from(x: Infallible) -> Self {
        match x {}
    }
}

struct PartialEvent {
    id: String,
    event_type: String,
    time: DateTime<FixedOffset>,
    attributes: Vec<OCELEventAttribute>,
    relationships: Vec<OCELRelationship>,
}

struct PartialObject {
    id: String,
    object_type: String,
    attributes: Vec<OCELObjectAttribute>,
    relationships: Vec<OCELRelationship>,
}

/// Parse an `<attribute name=".." time="..">` tag and append a Null-valued attribute
/// to `current_object`. Skips on time parse failure (with optional warning).
fn append_object_attr_decl(
    t: &BytesStart<'_>,
    current_object: &mut Option<PartialObject>,
    options: &OCELImportOptions,
) -> Result<(), OCELIOError> {
    let name = get_attribute_value(t, "name")?;
    let time_str = get_attribute_value(t, "time")?;
    match parse_timestamp(&time_str, options.date_format.as_deref(), options.verbose) {
        Ok(time) => {
            current_object
                .as_mut()
                .unwrap()
                .attributes
                .push(OCELObjectAttribute {
                    name,
                    value: OCELAttributeValue::Null,
                    time,
                });
        }
        Err(e) => {
            if options.verbose {
                eprintln!("Failed to parse time value of attribute: {e}. Will skip this attribute completely for now.");
            }
        }
    }
    Ok(())
}

/// Parse an `<attribute name="..">` tag and append a Null-valued attribute to `current_event`.
fn append_event_attr_decl(
    t: &BytesStart<'_>,
    current_event: &mut Option<PartialEvent>,
) -> Result<(), OCELIOError> {
    let name = get_attribute_value(t, "name")?;
    current_event
        .as_mut()
        .unwrap()
        .attributes
        .push(OCELEventAttribute {
            name,
            value: OCELAttributeValue::Null,
        });
    Ok(())
}

fn get_attribute_value(t: &BytesStart<'_>, key: &str) -> Result<String, quick_xml::Error> {
    match t.try_get_attribute(key)? {
        Some(attr) => Ok(read_xml_text_unescaped(&mut attr.value.as_ref())),
        None => Err(quick_xml::Error::Io(std::sync::Arc::new(
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Attribute {} not found", key),
            ),
        ))),
    }
}

fn parse_attribute_value(
    attribute_type: &OCELAttributeType,
    value: String,
    options: &OCELImportOptions,
) -> OCELAttributeValue {
    let res = match attribute_type {
        OCELAttributeType::String => Ok(OCELAttributeValue::String(value.clone())),
        OCELAttributeType::Integer => value
            .parse::<i64>()
            .map_err(|e| format!("{e}"))
            .map(OCELAttributeValue::Integer),
        OCELAttributeType::Float => value
            .parse::<f64>()
            .or_else(|e| {
                if value == "null" {
                    Ok(f64::NAN)
                } else {
                    Err(e)
                }
            })
            .map_err(|e| format!("{e}"))
            .map(OCELAttributeValue::Float),
        OCELAttributeType::Boolean => value
            .parse::<bool>()
            .map_err(|e| format!("{e}"))
            .map(OCELAttributeValue::Boolean),
        OCELAttributeType::Null => Ok(OCELAttributeValue::Null),
        OCELAttributeType::Time => {
            parse_timestamp(&value, options.date_format.as_deref(), options.verbose)
                .map_err(|e| e.to_string())
                .map(OCELAttributeValue::Time)
        }
    };
    match res {
        Ok(attribute_val) => attribute_val,
        Err(e) => {
            if options.verbose {
                eprintln!(
                    "Failed to parse attribute value {value:?} with supposed type {attribute_type:?}\n{e}"
                );
            }
            OCELAttributeValue::Null
        }
    }
}

///
/// Import an OCEL XML stream into an [`AppendableOCEL`]
///
/// Type declarations, events and objects are appended to `ocel` as they are
/// parsed. The caller is responsible for invoking [`AppendableOCEL::finalize`]
/// afterwards if the implementation requires it.
///
pub fn import_ocel_xml_into<R, A>(
    reader: &mut Reader<R>,
    ocel: &mut A,
    options: OCELImportOptions,
) -> Result<(), OCELIOError>
where
    R: BufRead,
    A: AppendableOCEL,
    A::Error: Into<OCELIOError>,
{
    reader.config_mut().trim_text(true);
    let mut buf: Vec<u8> = Vec::new();
    let mut current_mode: Mode = Mode::None;

    let mut current_ev_type: Option<OCELType> = None;
    let mut current_ob_type: Option<OCELType> = None;
    let mut current_event: Option<PartialEvent> = None;
    let mut current_object: Option<PartialObject> = None;

    let mut object_attribute_types: HashMap<(String, String), OCELAttributeType> = HashMap::new();
    let mut event_attribute_types: HashMap<(String, String), OCELAttributeType> = HashMap::new();
    let mut has_object_or_event_types_decl = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(r) => {
                match r {
                    quick_xml::events::Event::Start(t) => match current_mode {
                        Mode::None if t.name().as_ref() == b"log" => {
                            current_mode = Mode::Log;
                        }
                        Mode::Log => match t.name().as_ref() {
                            b"object-types" => {
                                current_mode = Mode::ObjectTypes;
                                has_object_or_event_types_decl = true;
                            }
                            b"event-types" => {
                                current_mode = Mode::EventTypes;
                                has_object_or_event_types_decl = true;
                            }
                            b"objects" => current_mode = Mode::Objects,
                            b"events" => current_mode = Mode::Events,
                            _ => {}
                        },
                        Mode::ObjectTypes if t.name().as_ref() == b"object-type" => {
                            let name = get_attribute_value(&t, "name")?;
                            current_ob_type = Some(OCELType {
                                name,
                                attributes: Vec::new(),
                            });
                            current_mode = Mode::ObjectType;
                        }
                        Mode::ObjectType if t.name().as_ref() == b"attributes" => {
                            current_mode = Mode::ObjectTypeAttributes;
                        }
                        Mode::EventType if t.name().as_ref() == b"attributes" => {
                            current_mode = Mode::EventTypeAttributes;
                        }
                        Mode::EventTypes if t.name().as_ref() == b"event-type" => {
                            let name = get_attribute_value(&t, "name")?;
                            current_ev_type = Some(OCELType {
                                name,
                                attributes: Vec::new(),
                            });
                            current_mode = Mode::EventType;
                        }
                        Mode::Objects if t.name().as_ref() == b"object" => {
                            let id = get_attribute_value(&t, "id")?;
                            let object_type = get_attribute_value(&t, "type")?;
                            current_object = Some(PartialObject {
                                id,
                                object_type,
                                attributes: Vec::new(),
                                relationships: Vec::new(),
                            });
                            current_mode = Mode::Object;
                        }
                        Mode::Object => match t.name().as_ref() {
                            b"attributes" | b"objects" => {}
                            b"attribute" => {
                                append_object_attr_decl(&t, &mut current_object, &options)?;
                            }
                            _ => {}
                        },
                        Mode::Events if t.name().as_ref() == b"event" => {
                            let id = get_attribute_value(&t, "id")?;
                            let event_type = get_attribute_value(&t, "type")?;
                            let time_str = get_attribute_value(&t, "time")?;
                            let time = parse_timestamp(
                                &time_str,
                                options.date_format.as_deref(),
                                options.verbose,
                            )
                            .map_err(|e| {
                                OCELIOError::Xml(quick_xml::Error::Io(std::sync::Arc::new(
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        format!("Invalid date: {}", e),
                                    ),
                                )))
                            })?;
                            current_event = Some(PartialEvent {
                                id,
                                event_type,
                                time,
                                attributes: Vec::new(),
                                relationships: Vec::new(),
                            });
                            current_mode = Mode::Event;
                        }
                        Mode::Event => match t.name().as_ref() {
                            b"attributes" | b"objects" => {}
                            b"attribute" => {
                                append_event_attr_decl(&t, &mut current_event)?;
                            }
                            _ => {}
                        },
                        _ => {}
                    },
                    quick_xml::events::Event::End(t) => match current_mode {
                        Mode::ObjectTypeAttributes if t.name().as_ref() == b"attributes" => {
                            current_mode = Mode::ObjectType;
                        }
                        Mode::ObjectType if t.name().as_ref() == b"object-type" => {
                            if let Some(ot) = current_ob_type.take() {
                                ocel.declare_object_type(ot).map_err(Into::into)?;
                            }
                            current_mode = Mode::ObjectTypes;
                        }
                        Mode::ObjectTypes if t.name().as_ref() == b"object-types" => {
                            current_mode = Mode::Log;
                        }
                        Mode::EventTypes if t.name().as_ref() == b"event-types" => {
                            current_mode = Mode::Log;
                        }
                        Mode::EventType if t.name().as_ref() == b"event-type" => {
                            if let Some(et) = current_ev_type.take() {
                                ocel.declare_event_type(et).map_err(Into::into)?;
                            }
                            current_mode = Mode::EventTypes;
                        }
                        Mode::EventTypeAttributes if t.name().as_ref() == b"attributes" => {
                            current_mode = Mode::EventType;
                        }
                        Mode::Log if t.name().as_ref() == b"log" => {
                            current_mode = Mode::None;
                        }
                        Mode::Objects if t.name().as_ref() == b"objects" => {
                            current_mode = Mode::Log;
                        }
                        Mode::Events if t.name().as_ref() == b"events" => {
                            current_mode = Mode::Log;
                        }
                        Mode::Object if t.name().as_ref() == b"object" => {
                            if let Some(o) = current_object.take() {
                                ocel.append_object(
                                    o.id,
                                    &o.object_type,
                                    o.attributes,
                                    o.relationships,
                                )
                                .map_err(Into::into)?;
                            }
                            current_mode = Mode::Objects;
                        }
                        Mode::Event if t.name().as_ref() == b"event" => {
                            if let Some(e) = current_event.take() {
                                ocel.append_event(
                                    e.id,
                                    &e.event_type,
                                    e.time,
                                    e.attributes,
                                    e.relationships,
                                )
                                .map_err(Into::into)?;
                            }
                            current_mode = Mode::Events;
                        }
                        _ => {}
                    },
                    quick_xml::events::Event::Empty(t) => match current_mode {
                        Mode::ObjectTypeAttributes if t.name().as_ref() == b"attribute" => {
                            let name = get_attribute_value(&t, "name")?;
                            let value_type = get_attribute_value(&t, "type")?;
                            let ot = current_ob_type.as_mut().unwrap();
                            object_attribute_types.insert(
                                (ot.name.clone(), name.clone()),
                                OCELAttributeType::from_type_str(&value_type),
                            );
                            ot.attributes.push(OCELTypeAttribute { name, value_type });
                        }
                        Mode::Object => match t.name().as_ref() {
                            b"relationship" | b"relobj" => {
                                let object_id = get_attribute_value(&t, "object-id")?;
                                let qualifier = get_attribute_value(&t, "qualifier")?;
                                current_object.as_mut().unwrap().relationships.push(
                                    OCELRelationship {
                                        object_id,
                                        qualifier,
                                    },
                                );
                            }
                            b"attributes" | b"objects" => {}
                            b"attribute" => {
                                append_object_attr_decl(&t, &mut current_object, &options)?;
                            }
                            _ => {}
                        },
                        Mode::Event => match t.name().as_ref() {
                            b"attributes" | b"objects" => {}
                            b"relationship" | b"object" | b"relobj" => {
                                let object_id = get_attribute_value(&t, "object-id")?;
                                let qualifier = get_attribute_value(&t, "qualifier")?;
                                current_event.as_mut().unwrap().relationships.push(
                                    OCELRelationship {
                                        object_id,
                                        qualifier,
                                    },
                                );
                            }
                            b"attribute" => {
                                append_event_attr_decl(&t, &mut current_event)?;
                            }
                            _ => {}
                        },
                        Mode::ObjectType | Mode::EventType => {
                            // Empty <attributes/> tag, no-op
                        }
                        Mode::EventTypeAttributes if t.name().as_ref() == b"attribute" => {
                            let name = get_attribute_value(&t, "name")?;
                            let value_type = get_attribute_value(&t, "type")?;
                            let et = current_ev_type.as_mut().unwrap();
                            event_attribute_types.insert(
                                (et.name.clone(), name.clone()),
                                OCELAttributeType::from_type_str(&value_type),
                            );
                            et.attributes.push(OCELTypeAttribute { name, value_type });
                        }
                        _ => {}
                    },
                    quick_xml::events::Event::Text(t) => match current_mode {
                        Mode::Object => {
                            let str_val = read_xml_text_unescaped(&mut t.as_ref());
                            let o = current_object.as_mut().unwrap();
                            let attr = o.attributes.last_mut().unwrap();
                            attr.value = parse_attribute_value(
                                object_attribute_types
                                    .get(&(o.object_type.clone(), attr.name.clone()))
                                    .unwrap_or(&OCELAttributeType::String),
                                str_val,
                                &options,
                            );
                        }
                        Mode::Event => {
                            let str_val = read_xml_text_unescaped(&mut t.as_ref());
                            let e = current_event.as_mut().unwrap();
                            let attr = e.attributes.last_mut().unwrap();
                            attr.value = parse_attribute_value(
                                event_attribute_types
                                    .get(&(e.event_type.clone(), attr.name.clone()))
                                    .unwrap_or(&OCELAttributeType::String),
                                str_val,
                                &options,
                            );
                        }
                        _ => {}
                    },
                    quick_xml::events::Event::Eof => break,
                    _ => {}
                }
            }
            Err(err) => return Err(err.into()),
        }
        buf.clear();
    }
    if !has_object_or_event_types_decl {
        return Err(OCELIOError::Other("No object or event types".to_string()));
    }
    Ok(())
}

///
/// Import an [`OCEL`] XML file from the given reader
///
pub fn import_ocel_xml<T>(
    reader: &mut Reader<T>,
    options: OCELImportOptions,
) -> Result<OCEL, OCELIOError>
where
    T: BufRead,
{
    let mut ocel = OCEL {
        event_types: Vec::new(),
        object_types: Vec::new(),
        events: Vec::new(),
        objects: Vec::new(),
    };
    import_ocel_xml_into(reader, &mut ocel, options)?;
    Ok(ocel)
}

///
/// Import an [`OCEL`] XML from a byte slice __with__ _custom options_
///
pub fn import_ocel_xml_slice_with(
    xes_data: &[u8],
    options: OCELImportOptions,
) -> Result<OCEL, OCELIOError> {
    import_ocel_xml(&mut Reader::from_reader(BufReader::new(xes_data)), options)
}

///
/// Import an [`OCEL`] XML from a filepath __with__ _custom options_
///
pub fn import_ocel_xml_path_with<P: AsRef<std::path::Path>>(
    path: P,
    options: OCELImportOptions,
) -> Result<OCEL, OCELIOError> {
    let mut reader: Reader<BufReader<std::fs::File>> = Reader::from_file(path)?;
    import_ocel_xml(&mut reader, options)
}

///
/// Import an [`OCEL`] XML from a byte slice with default options
///
pub fn import_ocel_xml_slice(xes_data: &[u8]) -> Result<OCEL, OCELIOError> {
    import_ocel_xml_slice_with(xes_data, OCELImportOptions::default())
}

///
/// Import an [`OCEL`] XML from a filepath with default options
///
pub fn import_ocel_xml_path<P: AsRef<std::path::Path>>(path: P) -> Result<OCEL, OCELIOError> {
    import_ocel_xml_path_with(path, OCELImportOptions::default())
}
