use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read},
    time::Instant,
};

use chrono::{DateTime, FixedOffset};
use quick_xml::{events::BytesStart, Reader};

use crate::{event_log::ocel::ocel_struct::OCELType, OCEL};

use super::ocel_struct::{
    OCELAttributeValue, OCELEvent, OCELEventAttribute, OCELObject, OCELObjectAttribute,
    OCELRelationship, OCELTypeAttribute,
};
///
/// Current Parsing Mode (i.e., which tag is currently open / being parsed)
///
#[derive(Clone, Copy, Debug)]
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
    Attribute,
    Log,
    None,
}

pub enum OCELAttributeType {
    String,
    Time,
    Integer,
    Float,
    Boolean,
    Null,
}

// fn print_to_string(x: &mut &[u8], mode: Mode, s: &str) {
//     // let mut str = String::new();
//     // x.read_to_string(&mut str).unwrap();
//     // println!("[{:?}] {}: {:?}", mode, s, str);
// }

fn read_to_string(x: &mut &[u8]) -> String {
    String::from_utf8_lossy(&x).to_string()
    // x.
    // let mut str = String::new();
    // x.read_to_string(&mut str).unwrap();
    // str
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
    match attribute_type {
        OCELAttributeType::String => OCELAttributeValue::String(value),
        OCELAttributeType::Integer => OCELAttributeValue::Integer(value.parse::<i64>().unwrap()),
        OCELAttributeType::Float => OCELAttributeValue::Float(value.parse::<f64>().unwrap()),
        OCELAttributeType::Boolean => OCELAttributeValue::Boolean(value.parse::<bool>().unwrap()),
        OCELAttributeType::Null => OCELAttributeValue::Null,
        OCELAttributeType::Time => todo!(),
    }
}

fn parse_date(time: &str) -> DateTime<FixedOffset> {
    match DateTime::parse_from_rfc3339(time) {
        Ok(dt) => dt,
        Err(_) => {
            match DateTime::parse_from_rfc2822(time) {
                Ok(dt) => dt,
                Err(_) => {
                    // Who made me do this? 🫣
                    // Some logs have this date: "Mon Apr 03 2023 12:08:18 GMT+0200 (Mitteleuropäische Sommerzeit)"
                    let replaced_time = &time[4..].replace(" GMT", "");
                    let s = replaced_time.split_once(" (").unwrap().0;
                    match DateTime::parse_from_str(s, "%b %d %Y %T%z") {
                        Ok(dt) => dt,
                        Err(_) => todo!("{}", time),
                    }
                }
            }
        }
    }
}

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
                                let time = get_attribute_value(&t, "time");
                                ocel.objects.last_mut().unwrap().attributes.push(
                                    OCELObjectAttribute {
                                        name,
                                        value: super::ocel_struct::OCELAttributeValue::Null,
                                        time: parse_date(&time).into(),
                                    },
                                )
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventStart"),
                            _ => {}
                        },
                        Mode::Events => match t.name().as_ref() {
                            b"event" => {
                                // TODO: Parse attributes
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
                            // mut x => print_to_string(&mut x, current_mode, "EventStart"),
                            _ => {}
                        },
                        _ => todo!("TODO: Implement EventStart for {:?}", current_mode),
                    },
                    quick_xml::events::Event::End(t) => match current_mode {
                        Mode::ObjectTypeAttributes => match t.name().as_ref() {
                            b"attributes" => current_mode = Mode::ObjectType,
                            _ => {} // mut x => print_to_string(&mut x, current_mode, "EventEnd"),
                        },
                        Mode::ObjectType => match t.name().as_ref() {
                            b"object-type" => current_mode = Mode::ObjectTypes,
                            // mut x => print_to_string(&mut x, current_mode, "EventEnd"),
                            _ => {}
                        },
                        Mode::ObjectTypes => match t.name().as_ref() {
                            b"object-types" => {
                                // Finished parsing Object Types
                                current_mode = Mode::Log
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventEnd"),
                            _ => {}
                        },
                        Mode::EventTypes => match t.name().as_ref() {
                            b"event-types" => {
                                // Finished parsing Object Types
                                current_mode = Mode::Log
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventEnd"),
                            _ => {}
                        },
                        Mode::EventType => match t.name().as_ref() {
                            b"event-type" => current_mode = Mode::EventTypes,
                            // mut x => print_to_string(&mut x, current_mode, "EventEnd"),
                            _ => {}
                        },
                        Mode::EventTypeAttributes => match t.name().as_ref() {
                            b"attributes" => current_mode = Mode::EventType,
                            // mut x => print_to_string(&mut x, current_mode, "EventEnd"),
                            _ => {}
                        },
                        Mode::Log => match t.name().as_ref() {
                            b"log" => {
                                // Finished parsing Object Types
                                current_mode = Mode::None
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventEnd"),
                            _ => {}
                        },
                        Mode::Objects => match t.name().as_ref() {
                            b"objects" => current_mode = Mode::Log,
                            // mut x => print_to_string(&mut x, current_mode, "EventEnd"),
                            _ => {}
                        },
                        Mode::Events => match t.name().as_ref() {
                            b"events" => current_mode = Mode::Log,
                            // mut x => print_to_string(&mut x, current_mode, "EventEnd"),
                            _ => {}
                        },
                        Mode::Object => match t.name().as_ref() {
                            b"object" => current_mode = Mode::Objects,
                            b"attribute" => {}
                            b"attributes" => {}
                            b"objects" => {
                                // End O2O
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventEnd"),
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
                            // mut x => print_to_string(&mut x, current_mode, "EventEnd"),
                            _ => {}
                        },
                        Mode::Attribute => todo!(),
                        _ => todo!("TODO: Implement EventEnd for {:?}", current_mode),
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
                                let new_rel : OCELRelationship = OCELRelationship {
                                    object_id,
                                    qualifier,
                                };
                                match ocel.objects
                                    .last_mut()
                                    .unwrap()
                                    .relationships
                                    .as_mut() {
                                        Some(rels) => rels.push(new_rel),
                                        None => ocel.objects
                                        .last_mut()
                                        .unwrap()
                                        .relationships = Some(vec![new_rel]),
                                    }
                            }
                            b"objects" => {
                                // No O2O, that's fine!
                            }
                            b"attributes" => {
                                // No attributes, that's fine!
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventEmpty"),
                            _ => {}
                        },
                        Mode::Event => match t.name().as_ref() {
                            b"attributes" => {
                                // Noop
                            }
                            b"objects" => {
                                // Angular OCEL uses <objects> tag for relationships
                                // If they are empty => Noop
                            }
                            b"relationship" => {
                                let object_id = get_attribute_value(&t, "object-id");
                                let qualifier = get_attribute_value(&t, "qualifier");
                                let new_rel : OCELRelationship = OCELRelationship {
                                    object_id,
                                    qualifier,
                                };
                                match ocel.events
                                    .last_mut()
                                    .unwrap()
                                    .relationships
                                    .as_mut() {
                                        Some(rels) => rels.push(new_rel),
                                        None => ocel.events
                                        .last_mut()
                                        .unwrap()
                                        .relationships = Some(vec![new_rel]),
                                    }
                            }
                            // Angular log uses object instead?
                            b"object" => {
                                let object_id = get_attribute_value(&t, "object-id");
                                let qualifier = get_attribute_value(&t, "qualifier");
                                let new_rel : OCELRelationship = OCELRelationship {
                                    object_id,
                                    qualifier,
                                };
                                match ocel.events
                                    .last_mut()
                                    .unwrap()
                                    .relationships
                                    .as_mut() {
                                        Some(rels) => rels.push(new_rel),
                                        None => ocel.events
                                        .last_mut()
                                        .unwrap()
                                        .relationships = Some(vec![new_rel]),
                                    }
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventEmpty"),  
                                                      _ => {}

                        },
                        // Mode::ObjectTypes => todo!(),
                        Mode::ObjectType => match t.name().as_ref() {
                            b"attributes" => {
                                // No attributes, that's fine!
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventEmpty"),      
                                                  _ => {}

                        },
                        // Mode::EventTypes => todo!(),
                        Mode::EventType => match t.name().as_ref() {
                            b"attributes" => {
                                // No attributes, that's fine!
                            }
                            // mut x => print_to_string(&mut x, current_mode, "EventEmpty"),   
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
                            // mut x => print_to_string(&mut x, current_mode, "EventEmpty"),             
                                           _ => {}

                        },
                        // Mode::Attribute => todo!(),
                        // Mode::Log => todo!(),
                        // Mode::None => todo!(),
                        _ => {}
                        // match t.name().as_ref() {
                        //     mut x => print_to_string(&mut x, current_mode, "EventEmpty"),
                        // },
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
                    _ => {} // x => {
                            //     println!("Unhandled: {:?}", x);
                            // }
                }
            }
            Err(err) => eprintln!("Error: {:?}", err),
        }
    }

    ocel
}

/// Import a XML [OCEL] from a byte slice (&\[u8\])
///
///
pub fn import_ocel_xml_slice(xes_data: &[u8]) -> OCEL {
    import_ocel_xml(&mut Reader::from_reader(BufReader::new(xes_data)))
}

/// Import a XML [OCEL] from a filepath (&\[u8\])
///
///
pub fn import_ocel_xml_file(path: &str) -> OCEL {
    let mut reader: Reader<BufReader<std::fs::File>> = Reader::from_file(path).unwrap();
    import_ocel_xml(&mut reader)
}

#[test]
fn test_ocel_xml() {
    let mut reader: Reader<BufReader<std::fs::File>> =
        Reader::from_file("/home/aarkue/dow/angular_github_commits_ocel.xml").unwrap();
    let now = Instant::now();
    let ocel = import_ocel_xml(&mut reader);
    println!(
        "Imported OCEL with {} objects and {} events in {:#?}",
        ocel.objects.len(),
        ocel.events.len(),
        now.elapsed()
    );
}
