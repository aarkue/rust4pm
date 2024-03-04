use std::{
    collections::HashMap,
    io::{BufRead, BufReader},
};

use chrono::{DateTime, FixedOffset, NaiveDateTime};
use quick_xml::{events::BytesStart, Reader};
use serde::{Deserialize, Serialize};

use crate::{event_log::ocel::ocel_struct::OCELType, OCEL};

use super::ocel_struct::{
    OCELAttributeValue, OCELEvent, OCELEventAttribute, OCELObject, OCELObjectAttribute,
    OCELRelationship, OCELTypeAttribute,
};

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

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
/// _Types_ of attribute values in OCEL2
pub enum OCELAttributeType {
    /// String
    String,
    /// DateTime
    Time,
    /// Integer
    Integer,
    /// Float
    Float,
    /// Boolean
    Boolean,
    /// Placeholder for invalid types
    Null,
}

fn read_to_string(x: &mut &[u8]) -> String {
    String::from_utf8_lossy(x).to_string()
}

fn type_string_to_attribute_type(s: &str) -> OCELAttributeType {
    match s {
        "string" => OCELAttributeType::String,
        "float" => OCELAttributeType::Float,
        "boolean" => OCELAttributeType::Boolean,
        "integer" => OCELAttributeType::Integer,
        "time" => OCELAttributeType::Time,
        _ => OCELAttributeType::Null,
    }
}

fn get_attribute_value(t: &BytesStart<'_>, key: &str) -> String {
    read_to_string(&mut t.try_get_attribute(key).unwrap().unwrap().value.as_ref())
}

fn parse_attribute_value(attribute_type: &OCELAttributeType, value: String) -> OCELAttributeValue {
    let res = match attribute_type {
        OCELAttributeType::String => Ok(OCELAttributeValue::String(value.clone())),
        OCELAttributeType::Integer => value
            .parse::<i64>()
            .map_err(|e| format!("{}", e))
            .map(OCELAttributeValue::Integer),
        OCELAttributeType::Float => value
            .parse::<f64>()
            .map_err(|e| format!("{}", e))
            .map(OCELAttributeValue::Float),
        OCELAttributeType::Boolean => value
            .parse::<bool>()
            .map_err(|e| format!("{}", e))
            .map(OCELAttributeValue::Boolean),
        OCELAttributeType::Null => Ok(OCELAttributeValue::Null),
        OCELAttributeType::Time => parse_date(&value)
            .map_err(|e| e.to_string())
            .map(|v| OCELAttributeValue::Time(v.into())),
    };
    match res {
        Ok(attribute_val) => attribute_val,
        Err(e) => {
            eprintln!(
                "Failed to parse attribute value {:?} with supposed type {:?}\n{}",
                value, attribute_type, e
            );
            OCELAttributeValue::Null
        }
    }
}

fn parse_date(time: &str) -> Result<DateTime<FixedOffset>, &str> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(time) {
        return Ok(dt);
    }
    if let Ok(dt) = DateTime::parse_from_rfc2822(time) {
        return Ok(dt);
    }
    // eprintln!("Encountered weird datetime format: {:?}", time);

    // Some logs have this date: "2023-10-06 09:30:21.890421"
    // Assuming that this is UTC
    if let Ok(dt) = NaiveDateTime::parse_from_str(time, "%F %T%.f") {
        return Ok(dt.and_utc().into());
    }

    // Who made me do this? 🫣
    // Some logs have this date: "Mon Apr 03 2023 12:08:18 GMT+0200 (Mitteleuropäische Sommerzeit)"
    // Below ignores the first "Mon " part (%Z) parses the rest (only if "GMT") and then parses the timezone (+0200)
    // The rest of the input is ignored
    if let Ok((dt, _)) = DateTime::parse_and_remainder(time, "%Z %b %d %Y %T GMT%z") {
        return Ok(dt);
    }
    Err("Unexpected Date Format")
}

///
/// Import an [`OCEL`]2 XML file from the given reader
///
pub fn import_ocel_xml<T>(reader: &mut Reader<T>) -> OCEL
where
    T: BufRead,
{
    reader.trim_text(true);
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
                            b"object-types" => current_mode = Mode::ObjectTypes,
                            b"event-types" => current_mode = Mode::EventTypes,
                            b"objects" => current_mode = Mode::Objects,
                            b"events" => current_mode = Mode::Events,
                            _ => {} // mut x => print_to_string(&mut x, current_mode, "EventStart"),
                        },
                        Mode::ObjectTypes => match t.name().as_ref() {
                            b"object-type" => {
                                let name_attr = t.try_get_attribute("name").unwrap().unwrap();
                                let name = read_to_string(&mut name_attr.value.as_ref());
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
                            _ => {} // mut x => print_to_string(&mut x, current_mode, "EventStart"),
                        },
                        Mode::EventTypes => match t.name().as_ref() {
                            b"event-type" => {
                                let name = get_attribute_value(&t, "name");
                                ocel.event_types.push(OCELType {
                                    name,
                                    attributes: Vec::new(),
                                });
                                current_mode = Mode::EventType
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventStart"),
                            _ => {}
                        },
                        Mode::EventType => match t.name().as_ref() {
                            b"attributes" => current_mode = Mode::EventTypeAttributes,
                            // mut x => print_to_string(&mut x, current_mode, "EventStart"),
                            _ => {}
                        },
                        Mode::Objects => match t.name().as_ref() {
                            b"object" => {
                                let id = get_attribute_value(&t, "id");
                                let object_type = get_attribute_value(&t, "type");
                                ocel.objects.push(OCELObject {
                                    id,
                                    object_type,
                                    attributes: Vec::new(),
                                    relationships: None,
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
                                let name = get_attribute_value(&t, "name");
                                let time_str = get_attribute_value(&t, "time");
                                let time = parse_date(&time_str);
                                match time {
                                    Ok(time_val) => {
                                        ocel.objects.last_mut().unwrap().attributes.push(
                                            OCELObjectAttribute {
                                                name,
                                                value: super::ocel_struct::OCELAttributeValue::Null,
                                                time: time_val.into(),
                                            },
                                        )
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to parse time value of attribute: {}. Will skip this attribute completely for now.",e);
                                    }
                                }
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventStart"),
                            _ => {}
                        },
                        Mode::Events => match t.name().as_ref() {
                            b"event" => {
                                let id = get_attribute_value(&t, "id");
                                let event_type = get_attribute_value(&t, "type");
                                let time = get_attribute_value(&t, "time");
                                ocel.events.push(OCELEvent {
                                    id,
                                    event_type,
                                    attributes: Vec::new(),
                                    relationships: None,
                                    time: DateTime::parse_from_rfc3339(&time).unwrap().into(),
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
                                let name = get_attribute_value(&t, "name");
                                ocel.events.last_mut().unwrap().attributes.push(
                                    OCELEventAttribute {
                                        name,
                                        value: super::ocel_struct::OCELAttributeValue::Null,
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
                                let name = get_attribute_value(&t, "name");
                                let value_type = get_attribute_value(&t, "type");
                                let object_type = &ocel.object_types.last().unwrap().name;
                                object_attribute_types.insert(
                                    (object_type.clone(), name.clone()),
                                    type_string_to_attribute_type(&value_type),
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
                                let object_id = get_attribute_value(&t, "object-id");
                                let qualifier = get_attribute_value(&t, "qualifier");
                                let new_rel: OCELRelationship = OCELRelationship {
                                    object_id,
                                    qualifier,
                                };
                                match ocel.objects.last_mut().unwrap().relationships.as_mut() {
                                    Some(rels) => rels.push(new_rel),
                                    None => {
                                        ocel.objects.last_mut().unwrap().relationships =
                                            Some(vec![new_rel])
                                    }
                                }
                            }
                            b"objects" => {
                                // No O2O, that's fine!
                            }
                            b"attributes" => {
                                // No attributes, that's fine!
                            }
                            _ => {}
                        },
                        Mode::Event => match t.name().as_ref() {
                            b"attributes" => {
                                // Noop
                            }
                            b"objects" => {
                                // Angular OCEL uses <objects> tag for relationships
                                // If they are empty => Noop
                                // TODO: Remove once example logs are updated
                            }
                            b"relationship" => {
                                let object_id = get_attribute_value(&t, "object-id");
                                let qualifier = get_attribute_value(&t, "qualifier");
                                let new_rel: OCELRelationship = OCELRelationship {
                                    object_id,
                                    qualifier,
                                };
                                match ocel.events.last_mut().unwrap().relationships.as_mut() {
                                    Some(rels) => rels.push(new_rel),
                                    None => {
                                        ocel.events.last_mut().unwrap().relationships =
                                            Some(vec![new_rel])
                                    }
                                }
                            }
                            // Angular log uses object instead?
                            // TODO: Remove once example logs are updated
                            // Should use relationship instead
                            b"object" => {
                                let object_id = get_attribute_value(&t, "object-id");
                                let qualifier = get_attribute_value(&t, "qualifier");
                                let new_rel: OCELRelationship = OCELRelationship {
                                    object_id,
                                    qualifier,
                                };
                                match ocel.events.last_mut().unwrap().relationships.as_mut() {
                                    Some(rels) => rels.push(new_rel),
                                    None => {
                                        ocel.events.last_mut().unwrap().relationships =
                                            Some(vec![new_rel])
                                    }
                                }
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
                                let name = get_attribute_value(&t, "name");
                                let value_type = get_attribute_value(&t, "type");
                                let event_type = &ocel.event_types.last().unwrap().name;
                                event_attribute_types.insert(
                                    (event_type.clone(), name.clone()),
                                    type_string_to_attribute_type(&value_type),
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
                                    .unwrap(),
                                str_val,
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
                                    .unwrap(),
                                str_val,
                            );
                        }
                        _ => {
                            println!("Got text in unexpected mode {:?}", current_mode);
                        }
                    },
                    quick_xml::events::Event::Eof => break,
                    _ => {}
                }
            }
            Err(err) => eprintln!("Error: {:?}", err),
        }
    }

    ocel
}

///
/// Import an [`OCEL`]2 XML from a byte slice
///
pub fn import_ocel_xml_slice(xes_data: &[u8]) -> OCEL {
    import_ocel_xml(&mut Reader::from_reader(BufReader::new(xes_data)))
}

///
/// Import an [`OCEL`]2 XML from a filepath
///
pub fn import_ocel_xml_file(path: &str) -> OCEL {
    let mut reader: Reader<BufReader<std::fs::File>> = Reader::from_file(path).unwrap();
    import_ocel_xml(&mut reader)
}
