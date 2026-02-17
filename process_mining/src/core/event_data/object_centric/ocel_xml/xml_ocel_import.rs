use std::{
    collections::HashMap,
    io::{BufRead, BufReader},
};

use quick_xml::{events::BytesStart, Reader};
use serde::{Deserialize, Serialize};

use crate::core::event_data::{
    object_centric::{
        io::OCELIOError,
        ocel_struct::{
            OCELAttributeType, OCELAttributeValue, OCELEvent, OCELEventAttribute, OCELObject,
            OCELObjectAttribute, OCELRelationship, OCELType, OCELTypeAttribute, OCEL,
        },
    },
    timestamp_utils::parse_timestamp,
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

fn read_to_string(x: &mut &[u8]) -> String {
    if let Ok(x_str) = std::str::from_utf8(x) {
        if let Ok(escaped) = quick_xml::escape::unescape(x_str) {
            return escaped.to_string();
        }
        return x_str.to_string();
    }
    String::from_utf8_lossy(x).to_string()
}

fn get_attribute_value(t: &BytesStart<'_>, key: &str) -> Result<String, quick_xml::Error> {
    match t.try_get_attribute(key)? {
        Some(attr) => Ok(read_to_string(&mut attr.value.as_ref())),
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
/// Import an [`OCEL`] XML file from the given reader
///
pub fn import_ocel_xml<T>(
    reader: &mut Reader<T>,
    options: OCELImportOptions,
) -> Result<OCEL, OCELIOError>
where
    T: BufRead,
{
    reader.config_mut().trim_text(true);
    let mut buf: Vec<u8> = Vec::new();

    let mut current_mode: Mode = Mode::None;

    let mut ocel = OCEL {
        event_types: Vec::new(),
        object_types: Vec::new(),
        events: Vec::new(),
        objects: Vec::new(),
    };
    // Object Type, Attribute Name => Attribute Type
    let mut object_attribute_types: HashMap<(String, String), OCELAttributeType> = HashMap::new();
    // Event Type, Attribute Name => Attribute Type
    let mut event_attribute_types: HashMap<(String, String), OCELAttributeType> = HashMap::new();
    let mut has_object_or_event_types_decl = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(r) => {
                match r {
                    quick_xml::events::Event::Start(t) => match current_mode {
                        Mode::None => match t.name().as_ref() {
                            // Start log parsing
                            b"log" => current_mode = Mode::Log,
                            _ => {} // mut x => print_to_string(&mut x, current_mode, "EventStart"),
                        },
                        Mode::Log => match t.name().as_ref() {
                            b"object-types" => {
                                current_mode = Mode::ObjectTypes;
                                has_object_or_event_types_decl = true
                            }
                            b"event-types" => {
                                current_mode = Mode::EventTypes;
                                has_object_or_event_types_decl = true
                            }
                            b"objects" => current_mode = Mode::Objects,
                            b"events" => current_mode = Mode::Events,
                            _ => {} // mut x => print_to_string(&mut x, current_mode, "EventStart"),
                        },
                        Mode::ObjectTypes => match t.name().as_ref() {
                            b"object-type" => {
                                let name = get_attribute_value(&t, "name")?;
                                ocel.object_types.push(OCELType {
                                    name,
                                    attributes: Vec::new(),
                                });
                                current_mode = Mode::ObjectType
                            }
                            _ => {} // mut x => print_to_string(&mut x, current_mode, "EventStart"),
                        },
                        Mode::ObjectType => match t.name().as_ref() {
                            b"attributes" => current_mode = Mode::ObjectTypeAttributes,
                            _ => {}
                        },
                        Mode::EventType => match t.name().as_ref() {
                            b"attributes" => current_mode = Mode::EventTypeAttributes,
                            // mut x => print_to_string(&mut x, current_mode, "EventStart"),
                            _ => {}
                        },
                        Mode::EventTypes => match t.name().as_ref() {
                            b"event-type" => {
                                let name = get_attribute_value(&t, "name")?;
                                ocel.event_types.push(OCELType {
                                    name,
                                    attributes: Vec::new(),
                                });
                                current_mode = Mode::EventType
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventStart"),
                            _ => {}
                        },
                        Mode::Objects => match t.name().as_ref() {
                            b"object" => {
                                let id = get_attribute_value(&t, "id")?;
                                let object_type = get_attribute_value(&t, "type")?;
                                ocel.objects.push(OCELObject {
                                    id,
                                    object_type,
                                    attributes: Vec::new(),
                                    relationships: Vec::new(),
                                });
                                current_mode = Mode::Object
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventStart"),
                            _ => {}
                        },
                        Mode::Object => match t.name().as_ref() {
                            b"attributes" => {
                                // Noop
                            }
                            b"objects" => {
                                // Begin O2O; Noop
                            }
                            b"attribute" => {
                                let name = get_attribute_value(&t, "name")?;
                                let time_str = get_attribute_value(&t, "time")?;
                                let time = parse_timestamp(
                                    &time_str,
                                    options.date_format.as_deref(),
                                    options.verbose,
                                );
                                match time {
                                    Ok(time_val) => {
                                        ocel.objects.last_mut().unwrap().attributes.push(
                                            OCELObjectAttribute {
                                                name,
                                                value: OCELAttributeValue::Null,
                                                time: time_val,
                                            },
                                        )
                                    }
                                    Err(e) => {
                                        if options.verbose {
                                            eprintln!("Failed to parse time value of attribute: {e}. Will skip this attribute completely for now.");
                                        }
                                    }
                                }
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventStart"),
                            _ => {}
                        },
                        Mode::Events => match t.name().as_ref() {
                            b"event" => {
                                let id = get_attribute_value(&t, "id")?;
                                let event_type = get_attribute_value(&t, "type")?;
                                let time = get_attribute_value(&t, "time")?;
                                let time_val = match parse_timestamp(
                                    &time,
                                    options.date_format.as_deref(),
                                    options.verbose,
                                ) {
                                    Ok(t) => t,
                                    Err(e) => {
                                        return Err(OCELIOError::Xml(quick_xml::Error::Io(
                                            std::sync::Arc::new(std::io::Error::new(
                                                std::io::ErrorKind::InvalidData,
                                                format!("Invalid date: {}", e),
                                            )),
                                        )));
                                    }
                                };
                                ocel.events.push(OCELEvent {
                                    id,
                                    event_type,
                                    attributes: Vec::new(),
                                    relationships: Vec::new(),
                                    time: time_val,
                                });
                                current_mode = Mode::Event
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventStart"),
                            _ => {}
                        },
                        Mode::Event => match t.name().as_ref() {
                            b"attributes" => {
                                // Noop
                            }
                            b"attribute" => {
                                let name = get_attribute_value(&t, "name")?;
                                ocel.events.last_mut().unwrap().attributes.push(
                                    OCELEventAttribute {
                                        name,
                                        value: OCELAttributeValue::Null,
                                    },
                                )
                            }
                            b"objects" => {
                                // Event-to-Object relations start now
                            }
                            _ => {}
                        },
                        _ => {}
                    },
                    quick_xml::events::Event::End(t) => match current_mode {
                        Mode::ObjectTypeAttributes => match t.name().as_ref() {
                            b"attributes" => current_mode = Mode::ObjectType,
                            _ => {}
                        },
                        Mode::ObjectType => match t.name().as_ref() {
                            b"object-type" => current_mode = Mode::ObjectTypes,
                            _ => {}
                        },
                        Mode::ObjectTypes => match t.name().as_ref() {
                            b"object-types" => {
                                // Finished parsing Object Types
                                current_mode = Mode::Log
                            }
                            _ => {}
                        },
                        Mode::EventTypes => match t.name().as_ref() {
                            b"event-types" => {
                                // Finished parsing Object Types
                                current_mode = Mode::Log
                            }
                            _ => {}
                        },
                        Mode::EventType => match t.name().as_ref() {
                            b"event-type" => current_mode = Mode::EventTypes,
                            _ => {}
                        },
                        Mode::EventTypeAttributes => match t.name().as_ref() {
                            b"attributes" => current_mode = Mode::EventType,
                            _ => {}
                        },
                        Mode::Log => match t.name().as_ref() {
                            b"log" => {
                                // Finished parsing Object Types
                                current_mode = Mode::None
                            }
                            _ => {}
                        },
                        Mode::Objects => match t.name().as_ref() {
                            b"objects" => current_mode = Mode::Log,
                            _ => {}
                        },
                        Mode::Events => match t.name().as_ref() {
                            b"events" => current_mode = Mode::Log,
                            _ => {}
                        },
                        Mode::Object => match t.name().as_ref() {
                            b"object" => current_mode = Mode::Objects,
                            b"attribute" => {}
                            b"attributes" => {}
                            b"objects" => {
                                // End O2O
                            }
                            _ => {}
                        },
                        Mode::Event => match t.name().as_ref() {
                            b"event" => current_mode = Mode::Events,
                            b"objects" => {
                                // End of E20 Relations
                                // Noop
                            }
                            b"attribute" => {}
                            b"attributes" => {}
                            _ => {}
                        },
                        _ => {}
                    },
                    quick_xml::events::Event::Empty(t) => match current_mode {
                        Mode::ObjectTypeAttributes => match t.name().as_ref() {
                            b"attribute" => {
                                let name = get_attribute_value(&t, "name")?;
                                let value_type = get_attribute_value(&t, "type")?;
                                let object_type = &ocel.object_types.last().unwrap().name;
                                object_attribute_types.insert(
                                    (object_type.clone(), name.clone()),
                                    OCELAttributeType::from_type_str(&value_type),
                                );
                                ocel.object_types
                                    .last_mut()
                                    .unwrap()
                                    .attributes
                                    .push(OCELTypeAttribute { name, value_type })
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventEmpty"),
                            _ => {}
                        },
                        Mode::Object => match t.name().as_ref() {
                            b"relationship" => {
                                let object_id = get_attribute_value(&t, "object-id")?;
                                let qualifier = get_attribute_value(&t, "qualifier")?;
                                let new_rel: OCELRelationship = OCELRelationship {
                                    object_id,
                                    qualifier,
                                };
                                ocel.objects.last_mut().unwrap().relationships.push(new_rel);
                            }
                            // P2P log uses relobj instead of relationship?
                            // TODO: Remove once fixed
                            b"relobj" => {
                                let object_id = get_attribute_value(&t, "object-id")?;
                                let qualifier = get_attribute_value(&t, "qualifier")?;
                                let new_rel: OCELRelationship = OCELRelationship {
                                    object_id,
                                    qualifier,
                                };
                                ocel.objects.last_mut().unwrap().relationships.push(new_rel);
                            }
                            b"objects" => {
                                // No O2O, that's fine!
                            }
                            b"attributes" => {
                                // No attributes, that's fine!
                            }

                            // Empty attributes => null value (?)
                            b"attribute" => {
                                let name = get_attribute_value(&t, "name")?;
                                let time_str = get_attribute_value(&t, "time")?;
                                let time = parse_timestamp(
                                    &time_str,
                                    options.date_format.as_deref(),
                                    options.verbose,
                                );
                                match time {
                                    Ok(time_val) => {
                                        ocel.objects.last_mut().unwrap().attributes.push(
                                            OCELObjectAttribute {
                                                name,
                                                value: OCELAttributeValue::Null,
                                                time: time_val,
                                            },
                                        )
                                    }
                                    Err(e) => {
                                        if options.verbose {
                                            eprintln!("Failed to parse time value of attribute: {e}. Will skip this attribute completely for now.");
                                        }
                                    }
                                }
                            }
                            _ => {}
                        },
                        Mode::Event => match t.name().as_ref() {
                            b"attributes" => {
                                // Noop
                            }
                            b"objects" => {
                                // If they are empty => Noop
                            }
                            b"relationship" => {
                                let object_id = get_attribute_value(&t, "object-id")?;
                                let qualifier = get_attribute_value(&t, "qualifier")?;
                                let new_rel: OCELRelationship = OCELRelationship {
                                    object_id,
                                    qualifier,
                                };
                                ocel.events.last_mut().unwrap().relationships.push(new_rel);
                            }
                            // Angular log uses object instead?
                            // TODO: Remove once example logs are updated
                            // Should use relationship instead
                            b"object" => {
                                let object_id = get_attribute_value(&t, "object-id")?;
                                let qualifier = get_attribute_value(&t, "qualifier")?;
                                let new_rel: OCELRelationship = OCELRelationship {
                                    object_id,
                                    qualifier,
                                };
                                ocel.events.last_mut().unwrap().relationships.push(new_rel);
                            }

                            // P2P log uses relobj instead of relationship?
                            // TODO: Remove once fixed
                            b"relobj" => {
                                let object_id = get_attribute_value(&t, "object-id")?;
                                let qualifier = get_attribute_value(&t, "qualifier")?;
                                let new_rel: OCELRelationship = OCELRelationship {
                                    object_id,
                                    qualifier,
                                };
                                ocel.events.last_mut().unwrap().relationships.push(new_rel);
                            }
                            // Empty attribute => Null value (?)
                            b"attribute" => {
                                let name = get_attribute_value(&t, "name")?;
                                ocel.events.last_mut().unwrap().attributes.push(
                                    OCELEventAttribute {
                                        name,
                                        value: OCELAttributeValue::Null,
                                    },
                                )
                            }
                            _ => {}
                        },
                        Mode::ObjectType => match t.name().as_ref() {
                            b"attributes" => {
                                // No attributes, that's fine!
                            }
                            _ => {}
                        },
                        Mode::EventType => match t.name().as_ref() {
                            b"attributes" => {
                                // No attributes, that's fine!
                            }
                            _ => {}
                        },
                        Mode::EventTypeAttributes => match t.name().as_ref() {
                            b"attribute" => {
                                let name = get_attribute_value(&t, "name")?;
                                let value_type = get_attribute_value(&t, "type")?;
                                let event_type = &ocel.event_types.last().unwrap().name;
                                event_attribute_types.insert(
                                    (event_type.clone(), name.clone()),
                                    OCELAttributeType::from_type_str(&value_type),
                                );
                                ocel.event_types
                                    .last_mut()
                                    .unwrap()
                                    .attributes
                                    .push(OCELTypeAttribute { name, value_type })
                            }
                            _ => {}
                        },
                        _ => {}
                    },
                    quick_xml::events::Event::Text(t) => match current_mode {
                        Mode::Object => {
                            let str_val = read_to_string(&mut t.as_ref());
                            let o = ocel.objects.last_mut().unwrap();
                            let attribute = o.attributes.last_mut().unwrap();
                            attribute.value = parse_attribute_value(
                                object_attribute_types
                                    .get(&(o.object_type.clone(), attribute.name.clone()))
                                    .unwrap_or(&OCELAttributeType::String),
                                str_val,
                                &options,
                            );
                            // parse_attribute_value
                        }
                        Mode::Event => {
                            let str_val = read_to_string(&mut t.as_ref());
                            let e = ocel.events.last_mut().unwrap();
                            let attribute = e.attributes.last_mut().unwrap();
                            attribute.value = parse_attribute_value(
                                event_attribute_types
                                    .get(&(e.event_type.clone(), attribute.name.clone()))
                                    .unwrap_or(&OCELAttributeType::String),
                                str_val,
                                &options,
                            );
                        }
                        _ => {
                            println!("Got text in unexpected mode {current_mode:?}");
                        }
                    },
                    quick_xml::events::Event::Eof => break,
                    _ => {}
                }
            }
            Err(err) => return Err(err.into()),
        }
    }
    if !has_object_or_event_types_decl {
        return Err(OCELIOError::Other("No object or event types".to_string()));
    }
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
