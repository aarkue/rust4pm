use std::{convert::Infallible, io::BufRead};

use chrono::{DateTime, FixedOffset};
use quick_xml::{events::BytesStart, Reader};
use serde::{Deserialize, Serialize};

use crate::core::{
    event_data::{
        object_centric::{
            appendable::AppendableOCEL,
            io::{OCELIOError, OCELImportOptions},
            ocel_struct::{
                DeclaredAttributeTypes, OCELAttributeType, OCELAttributeValue, OCELEventAttribute,
                OCELObjectAttribute, OCELRelationship, OCELType, OCELTypeAttribute, OCEL,
            },
        },
        timestamp_utils::parse_timestamp,
    },
    io::{push_xml_general_ref, read_xml_text_unescaped},
};
use crate::utils::control::Control;

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

/// The type an attribute's value is read as. Attributes without a declared type are strings.
fn attribute_type(
    attribute_types: &DeclaredAttributeTypes,
    type_name: &str,
    attribute_name: &str,
) -> OCELAttributeType {
    attribute_types
        .get(type_name, attribute_name)
        .unwrap_or(OCELAttributeType::String)
}

/// The value of an `<attribute>` element without text, before any text is read: the empty
/// string for a string attribute, `Null` for any other type.
fn empty_attribute_value(attribute_type: OCELAttributeType) -> OCELAttributeValue {
    match attribute_type {
        OCELAttributeType::String => OCELAttributeValue::String(String::new()),
        _ => OCELAttributeValue::Null,
    }
}

/// Parse an `<attribute name=".." time="..">` tag and append an attribute with the value of an
/// empty element (see [`empty_attribute_value`]) to `current_object`, to be replaced by the
/// element's text if it has any. Skips on time parse failure (with a warning).
///
/// Returns the type of the appended attribute, or `None` if it was skipped.
fn append_object_attr_decl(
    t: &BytesStart<'_>,
    current_object: &mut Option<PartialObject>,
    object_attribute_types: &DeclaredAttributeTypes,
    options: &OCELImportOptions,
    ctl: &Control<'_>,
) -> Result<Option<OCELAttributeType>, OCELIOError> {
    let name = get_attribute_value(t, "name")?;
    let time = match get_optional_attribute_value(t, "time")? {
        Some(time_str) => match parse_timestamp(&time_str, options.date_format.as_deref()) {
            Ok(time) => time,
            Err(e) => {
                ctl.warn(format_args!(
                    "Skipping object attribute {name:?} with time {time_str:?}: {e}"
                ));
                return Ok(None);
            }
        },
        // OCEL 2.0 Sec. 4: a value assignment without a timestamp is at time 0
        None => DateTime::UNIX_EPOCH.into(),
    };
    let o = current_object.as_mut().unwrap();
    let ty = attribute_type(object_attribute_types, &o.object_type, &name);
    let value = empty_attribute_value(ty);
    o.attributes.push(OCELObjectAttribute { name, value, time });
    Ok(Some(ty))
}

/// Parse an `<attribute name="..">` tag and append an attribute with the value of an empty
/// element (see [`empty_attribute_value`]) to `current_event`, to be replaced by the element's
/// text if it has any.
///
/// Returns the type of the appended attribute.
fn append_event_attr_decl(
    t: &BytesStart<'_>,
    current_event: &mut Option<PartialEvent>,
    event_attribute_types: &DeclaredAttributeTypes,
) -> Result<OCELAttributeType, OCELIOError> {
    let name = get_attribute_value(t, "name")?;
    let e = current_event.as_mut().unwrap();
    let ty = attribute_type(event_attribute_types, &e.event_type, &name);
    let value = empty_attribute_value(ty);
    e.attributes.push(OCELEventAttribute { name, value });
    Ok(ty)
}

fn get_optional_attribute_value(
    t: &BytesStart<'_>,
    key: &str,
) -> Result<Option<String>, OCELIOError> {
    Ok(t.try_get_attribute(key)
        .map_err(quick_xml::Error::from)?
        .map(|attr| read_xml_text_unescaped(&attr.value)))
}

fn get_attribute_value(t: &BytesStart<'_>, key: &str) -> Result<String, OCELIOError> {
    get_optional_attribute_value(t, key)?.ok_or_else(|| {
        OCELIOError::Invalid(format!(
            "attribute {key:?} missing on <{}>",
            t.name().into_inner()
        ))
    })
}

/// The XSD makes `qualifier` optional, so a missing one is the empty qualifier
fn get_relationship(t: &BytesStart<'_>) -> Result<OCELRelationship, OCELIOError> {
    Ok(OCELRelationship {
        object_id: get_attribute_value(t, "object-id")?,
        qualifier: get_optional_attribute_value(t, "qualifier")?.unwrap_or_default(),
    })
}

/// Add the attribute declared by an `<attribute name=".." type="..">` tag to `ty`. The XSD makes
/// `type` optional, and the value of an attribute without one is read as a string.
fn declare_type_attribute(
    t: &BytesStart<'_>,
    ty: &mut OCELType,
    attribute_types: &mut DeclaredAttributeTypes,
) -> Result<(), OCELIOError> {
    let name = get_attribute_value(t, "name")?;
    let value_type = get_optional_attribute_value(t, "type")?
        .unwrap_or_else(|| OCELAttributeType::String.to_type_string());
    attribute_types.declare_attribute(
        &ty.name,
        &name,
        OCELAttributeType::from_type_str(&value_type),
    );
    ty.attributes.push(OCELTypeAttribute { name, value_type });
    Ok(())
}

/// `value` read as `attribute_type`. A value that does not parse as its type is kept as a string,
/// with a warning.
fn parse_attribute_value(
    attribute_type: &OCELAttributeType,
    value: String,
    options: &OCELImportOptions,
    ctl: &Control<'_>,
) -> OCELAttributeValue {
    // Only string values keep surrounding whitespace
    let trimmed = value.trim();
    let res = match attribute_type {
        // An undeclared type name is not a reason to drop the value
        OCELAttributeType::String | OCELAttributeType::Null => {
            return OCELAttributeValue::String(value);
        }
        OCELAttributeType::Integer => trimmed
            .parse::<i64>()
            .map_err(|e| format!("{e}"))
            .map(OCELAttributeValue::Integer),
        // Some logs (e.g., ContainerLogistics) write `null` where their JSON version has `null`
        OCELAttributeType::Float if trimmed == "null" => Ok(OCELAttributeValue::Null),
        OCELAttributeType::Float => trimmed
            .parse::<f64>()
            .map_err(|e| format!("{e}"))
            .map(OCELAttributeValue::Float),
        // `xs:boolean` also allows `1` and `0`, and pm4py writes `True` and `False`
        OCELAttributeType::Boolean => match trimmed.to_ascii_lowercase().as_str() {
            "true" | "1" => Ok(OCELAttributeValue::Boolean(true)),
            "false" | "0" => Ok(OCELAttributeValue::Boolean(false)),
            _ => Err(format!("{trimmed:?} is not a boolean")),
        },
        OCELAttributeType::Time => parse_timestamp(trimmed, options.date_format.as_deref())
            .map_err(|e| e.to_string())
            .map(OCELAttributeValue::Time),
    };
    res.unwrap_or_else(|e| {
        ctl.warn(format_args!(
            "Failed to parse attribute value {value:?} with supposed type {attribute_type:?}, \
             keeping it as a string\n{e}"
        ));
        OCELAttributeValue::String(value)
    })
}

fn partial_event(
    t: &BytesStart<'_>,
    options: &OCELImportOptions,
) -> Result<PartialEvent, OCELIOError> {
    let id = get_attribute_value(t, "id")?;
    let event_type = get_attribute_value(t, "type")?;
    let time_str = get_attribute_value(t, "time")?;
    let time = parse_timestamp(&time_str, options.date_format.as_deref())
        .map_err(|e| OCELIOError::Invalid(format!("event {id:?} has an invalid time: {e}")))?;
    Ok(PartialEvent {
        id,
        event_type,
        time,
        attributes: Vec::new(),
        relationships: Vec::new(),
    })
}

/// A type declared by an `<object-type>` or `<event-type>` tag, before its attributes.
fn type_decl(t: &BytesStart<'_>) -> Result<OCELType, OCELIOError> {
    Ok(OCELType {
        name: get_attribute_value(t, "name")?,
        attributes: Vec::new(),
    })
}

///
/// Import an OCEL XML stream into an [`AppendableOCEL`], warning through `ctl`.
///
/// Type declarations, events and objects are appended to `ocel` as they are parsed. The caller
/// is responsible for invoking [`AppendableOCEL::finalize`] afterwards if the implementation
/// requires it.
pub(crate) fn import_ocel_xml_into<R, A>(
    reader: &mut Reader<R>,
    ocel: &mut A,
    options: &OCELImportOptions,
    ctl: &Control<'_>,
) -> Result<(), OCELIOError>
where
    R: BufRead,
    A: AppendableOCEL,
    A::Error: Into<OCELIOError>,
{
    // Not trimmed, so that a string value keeps leading and trailing whitespace. Text outside an
    // open `<attribute>` (e.g., indentation) is ignored below.
    reader.config_mut().trim_text(false);
    // A lone `&` in text is kept as is, like an unknown entity
    reader.config_mut().allow_dangling_amp = true;
    let mut buf: Vec<u8> = Vec::new();
    // The type and the text read so far of the open `<attribute>` element, if one is open
    let mut open_attr: Option<(OCELAttributeType, String)> = None;
    let mut current_mode: Mode = Mode::None;

    let mut current_ev_type: Option<OCELType> = None;
    let mut current_ob_type: Option<OCELType> = None;
    let mut current_event: Option<PartialEvent> = None;
    let mut current_object: Option<PartialObject> = None;

    let mut object_attribute_types = DeclaredAttributeTypes::default();
    let mut event_attribute_types = DeclaredAttributeTypes::default();
    let mut has_object_or_event_types_decl = false;
    // Inside an O2O relationship written as `<object object-id=".." ..>` with a closing tag
    let mut in_o2o_object = false;

    loop {
        match reader.read_event_into(&mut buf)? {
            quick_xml::events::Event::Start(t) => match current_mode {
                Mode::None if t.name().as_ref() == "log" => {
                    current_mode = Mode::Log;
                }
                Mode::Log => match t.name().as_ref() {
                    "object-types" => {
                        current_mode = Mode::ObjectTypes;
                        has_object_or_event_types_decl = true;
                    }
                    "event-types" => {
                        current_mode = Mode::EventTypes;
                        has_object_or_event_types_decl = true;
                    }
                    "objects" => current_mode = Mode::Objects,
                    "events" => current_mode = Mode::Events,
                    _ => {}
                },
                Mode::ObjectTypes if t.name().as_ref() == "object-type" => {
                    current_ob_type = Some(type_decl(&t)?);
                    current_mode = Mode::ObjectType;
                }
                Mode::ObjectType if t.name().as_ref() == "attributes" => {
                    current_mode = Mode::ObjectTypeAttributes;
                }
                Mode::EventType if t.name().as_ref() == "attributes" => {
                    current_mode = Mode::EventTypeAttributes;
                }
                Mode::ObjectTypeAttributes if t.name().as_ref() == "attribute" => {
                    declare_type_attribute(
                        &t,
                        current_ob_type.as_mut().unwrap(),
                        &mut object_attribute_types,
                    )?;
                }
                Mode::EventTypeAttributes if t.name().as_ref() == "attribute" => {
                    declare_type_attribute(
                        &t,
                        current_ev_type.as_mut().unwrap(),
                        &mut event_attribute_types,
                    )?;
                }
                Mode::EventTypes if t.name().as_ref() == "event-type" => {
                    current_ev_type = Some(type_decl(&t)?);
                    current_mode = Mode::EventType;
                }
                Mode::Objects if t.name().as_ref() == "object" => {
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
                    "attributes" | "objects" => {}
                    "attribute" => {
                        open_attr = append_object_attr_decl(
                            &t,
                            &mut current_object,
                            &object_attribute_types,
                            options,
                            ctl,
                        )?
                        .map(|ty| (ty, String::new()));
                    }
                    "relationship" | "relobj" => {
                        let rel = get_relationship(&t)?;
                        current_object.as_mut().unwrap().relationships.push(rel);
                    }
                    "object" => {
                        let rel = get_relationship(&t)?;
                        current_object.as_mut().unwrap().relationships.push(rel);
                        in_o2o_object = true;
                    }
                    _ => {}
                },
                Mode::Events if t.name().as_ref() == "event" => {
                    current_event = Some(partial_event(&t, options)?);
                    current_mode = Mode::Event;
                }
                Mode::Event => match t.name().as_ref() {
                    "attributes" | "objects" => {}
                    "attribute" => {
                        let ty =
                            append_event_attr_decl(&t, &mut current_event, &event_attribute_types)?;
                        open_attr = Some((ty, String::new()));
                    }
                    "relationship" | "object" | "relobj" => {
                        let rel = get_relationship(&t)?;
                        current_event.as_mut().unwrap().relationships.push(rel);
                    }
                    _ => {}
                },
                _ => {}
            },
            quick_xml::events::Event::End(t) => match current_mode {
                Mode::Object if t.name().as_ref() == "attribute" => {
                    if let Some((ty, text)) = open_attr.take().filter(|(_, s)| !s.is_empty()) {
                        if let Some(attr) = current_object
                            .as_mut()
                            .and_then(|o| o.attributes.last_mut())
                        {
                            attr.value = parse_attribute_value(&ty, text, options, ctl);
                        }
                    }
                }
                Mode::Event if t.name().as_ref() == "attribute" => {
                    if let Some((ty, text)) = open_attr.take().filter(|(_, s)| !s.is_empty()) {
                        if let Some(attr) =
                            current_event.as_mut().and_then(|e| e.attributes.last_mut())
                        {
                            attr.value = parse_attribute_value(&ty, text, options, ctl);
                        }
                    }
                }
                Mode::ObjectTypeAttributes if t.name().as_ref() == "attributes" => {
                    current_mode = Mode::ObjectType;
                }
                Mode::ObjectType if t.name().as_ref() == "object-type" => {
                    if let Some(ot) = current_ob_type.take() {
                        ocel.declare_object_type(ot).map_err(Into::into)?;
                    }
                    current_mode = Mode::ObjectTypes;
                }
                Mode::ObjectTypes if t.name().as_ref() == "object-types" => {
                    current_mode = Mode::Log;
                }
                Mode::EventTypes if t.name().as_ref() == "event-types" => {
                    current_mode = Mode::Log;
                }
                Mode::EventType if t.name().as_ref() == "event-type" => {
                    if let Some(et) = current_ev_type.take() {
                        ocel.declare_event_type(et).map_err(Into::into)?;
                    }
                    current_mode = Mode::EventTypes;
                }
                Mode::EventTypeAttributes if t.name().as_ref() == "attributes" => {
                    current_mode = Mode::EventType;
                }
                Mode::Log if t.name().as_ref() == "log" => {
                    current_mode = Mode::None;
                }
                Mode::Objects if t.name().as_ref() == "objects" => {
                    current_mode = Mode::Log;
                }
                Mode::Events if t.name().as_ref() == "events" => {
                    current_mode = Mode::Log;
                }
                Mode::Object if t.name().as_ref() == "object" && in_o2o_object => {
                    in_o2o_object = false;
                }
                Mode::Object if t.name().as_ref() == "object" => {
                    if let Some(o) = current_object.take() {
                        ocel.append_object(o.id, &o.object_type, o.attributes, o.relationships)
                            .map_err(Into::into)?;
                    }
                    current_mode = Mode::Objects;
                }
                Mode::Event if t.name().as_ref() == "event" => {
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
                Mode::Log if matches!(t.name().as_ref(), "object-types" | "event-types") => {
                    has_object_or_event_types_decl = true;
                }
                // The XSD allows an object without `<attributes>` and `<objects>`
                Mode::Objects if t.name().as_ref() == "object" => {
                    let id = get_attribute_value(&t, "id")?;
                    let object_type = get_attribute_value(&t, "type")?;
                    ocel.append_object(id, &object_type, Vec::new(), Vec::new())
                        .map_err(Into::into)?;
                }
                Mode::Events if t.name().as_ref() == "event" => {
                    let e = partial_event(&t, options)?;
                    ocel.append_event(e.id, &e.event_type, e.time, Vec::new(), Vec::new())
                        .map_err(Into::into)?;
                }
                Mode::ObjectTypes if t.name().as_ref() == "object-type" => {
                    ocel.declare_object_type(type_decl(&t)?)
                        .map_err(Into::into)?;
                }
                Mode::EventTypes if t.name().as_ref() == "event-type" => {
                    ocel.declare_event_type(type_decl(&t)?)
                        .map_err(Into::into)?;
                }
                Mode::ObjectTypeAttributes if t.name().as_ref() == "attribute" => {
                    declare_type_attribute(
                        &t,
                        current_ob_type.as_mut().unwrap(),
                        &mut object_attribute_types,
                    )?;
                }
                Mode::Object => match t.name().as_ref() {
                    // `object` is the element the XSD names for an O2O relationship
                    "relationship" | "relobj" | "object" => {
                        let rel = get_relationship(&t)?;
                        current_object.as_mut().unwrap().relationships.push(rel);
                    }
                    "attributes" | "objects" => {}
                    "attribute" => {
                        append_object_attr_decl(
                            &t,
                            &mut current_object,
                            &object_attribute_types,
                            options,
                            ctl,
                        )?;
                    }
                    _ => {}
                },
                Mode::Event => match t.name().as_ref() {
                    "attributes" | "objects" => {}
                    "relationship" | "object" | "relobj" => {
                        let rel = get_relationship(&t)?;
                        current_event.as_mut().unwrap().relationships.push(rel);
                    }
                    "attribute" => {
                        append_event_attr_decl(&t, &mut current_event, &event_attribute_types)?;
                    }
                    _ => {}
                },
                Mode::ObjectType | Mode::EventType => {
                    // Empty <attributes/> tag, no-op
                }
                Mode::EventTypeAttributes if t.name().as_ref() == "attribute" => {
                    declare_type_attribute(
                        &t,
                        current_ev_type.as_mut().unwrap(),
                        &mut event_attribute_types,
                    )?;
                }
                _ => {}
            },
            quick_xml::events::Event::Text(t) => {
                if let Some((_, text)) = open_attr.as_mut() {
                    text.push_str(&t);
                }
            }
            quick_xml::events::Event::GeneralRef(r) => {
                if let Some((_, text)) = open_attr.as_mut() {
                    push_xml_general_ref(text, &r);
                }
            }
            quick_xml::events::Event::Eof => break,
            _ => {}
        }

        buf.clear();
    }
    if !has_object_or_event_types_decl {
        return Err(OCELIOError::Invalid(
            "no object or event types declared".to_string(),
        ));
    }
    Ok(())
}

/// Import an [`OCEL`] from OCEL XML, warning through `ctl`.
pub(crate) fn import_ocel_xml<T: BufRead>(
    reader: &mut Reader<T>,
    options: &OCELImportOptions,
    ctl: &Control<'_>,
) -> Result<OCEL, OCELIOError> {
    let mut ocel = OCEL::default();
    import_ocel_xml_into(reader, &mut ocel, options, ctl)?;
    Ok(ocel)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Importable;

    fn import(body: &str) -> OCEL {
        OCEL::import_from_bytes(
            format!("<?xml version=\"1.0\"?><log>{body}</log>").as_bytes(),
            "xml",
        )
        .unwrap()
    }

    /// Forms the XSD allows besides the self-closing ones the exporter writes.
    #[test]
    fn imports_xsd_valid_element_forms() {
        let ocel = import(
            r#"<object-types><object-type name="t"><attributes>
                <attribute name="a" type="integer"></attribute><attribute name="u"/>
            </attributes></object-type></object-types>
            <event-types><event-type name="x"><attributes/></event-type></event-types>
            <objects>
                <object id="o1" type="t"/>
                <object id="o2" type="t">
                    <attributes><attribute name="a">5</attribute></attributes>
                    <objects><object object-id="o1" qualifier="q1"></object><object object-id="o1"/></objects>
                </object>
            </objects>
            <events><event id="e1" type="x" time=" 2024-01-01T00:00:00Z ">
                <attributes/><objects><relationship object-id="o2" qualifier="q"></relationship></objects>
            </event></events>"#,
        );
        assert_eq!(
            ocel.object_types[0].attributes,
            vec![
                OCELTypeAttribute::new("a", &OCELAttributeType::Integer),
                OCELTypeAttribute::new("u", &OCELAttributeType::String),
            ]
        );
        let ids: Vec<_> = ocel.objects.iter().map(|o| o.id.as_str()).collect();
        assert_eq!(ids, ["o1", "o2"]);
        let o2 = &ocel.objects[1];
        assert_eq!(
            o2.attributes,
            vec![OCELObjectAttribute::new("a", 5i64, DateTime::UNIX_EPOCH)]
        );
        assert_eq!(
            o2.relationships,
            vec![
                OCELRelationship::new("o1", "q1"),
                OCELRelationship::new("o1", "")
            ]
        );
        assert_eq!(
            ocel.events[0].relationships,
            vec![OCELRelationship::new("o2", "q")]
        );
    }

    #[test]
    fn reads_values_as_other_tools_write_them() {
        let ocel = import(
            r#"<object-types/><event-types><event-type name="x"><attributes>
                <attribute name="b1" type="boolean"/><attribute name="b0" type="boolean"/>
                <attribute name="bt" type="boolean"/><attribute name="d" type="date"/>
                <attribute name="n" type="float"/><attribute name="k" type="unknown"/>
            </attributes></event-type></event-types><objects/>
            <events><event id="e1" type="x" time="2024-01-01T00:00:00Z"><attributes>
                <attribute name="b1">1</attribute><attribute name="b0">0</attribute>
                <attribute name="bt">True</attribute><attribute name="d">2024-01-01T10:00:00</attribute>
                <attribute name="n">null</attribute><attribute name="k">kept</attribute>
            </attributes><objects/></event></events>"#,
        );
        let values: Vec<_> = ocel.events[0]
            .attributes
            .iter()
            .map(|a| a.value.clone())
            .collect();
        assert_eq!(
            values,
            vec![
                OCELAttributeValue::Boolean(true),
                OCELAttributeValue::Boolean(false),
                OCELAttributeValue::Boolean(true),
                OCELAttributeValue::Time(
                    DateTime::parse_from_rfc3339("2024-01-01T10:00:00Z").unwrap()
                ),
                OCELAttributeValue::Null,
                OCELAttributeValue::String("kept".to_string()),
            ]
        );
    }

    #[test]
    fn empty_log_with_self_closing_type_lists() {
        let ocel = import("<object-types/><event-types/><objects/><events/>");
        assert!(ocel.events.is_empty() && ocel.object_types.is_empty());
    }

    #[test]
    fn self_closing_events_and_types() {
        let ocel = import(
            r#"<object-types><object-type name="t"/></object-types>
            <event-types><event-type name="x"/></event-types>
            <objects/>
            <events><event id="e1" type="x" time="2024-01-01T00:00:00Z"/></events>"#,
        );
        let names = |types: &[OCELType]| {
            types
                .iter()
                .map(|t| (t.name.clone(), t.attributes.len()))
                .collect::<Vec<_>>()
        };
        assert_eq!(names(&ocel.object_types), [("t".to_string(), 0)]);
        assert_eq!(names(&ocel.event_types), [("x".to_string(), 0)]);
        assert_eq!(ocel.events.len(), 1);
        let e = &ocel.events[0];
        assert_eq!((e.id.as_str(), e.event_type.as_str()), ("e1", "x"));
        assert_eq!(
            e.time,
            DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap()
        );
        assert!(e.attributes.is_empty() && e.relationships.is_empty());
    }

    #[test]
    fn a_value_not_of_its_type_is_kept_as_a_string() {
        let ocel = import(
            r#"<object-types><object-type name="t"><attributes>
                <attribute name="i" type="integer"/>
            </attributes></object-type></object-types>
            <event-types><event-type name="x"><attributes>
                <attribute name="b" type="boolean"/><attribute name="t" type="time"/>
            </attributes></event-type></event-types>
            <objects><object id="o1" type="t"><attributes>
                <attribute name="i"> 12x </attribute>
            </attributes></object></objects>
            <events><event id="e1" type="x" time="2024-01-01T00:00:00Z"><attributes>
                <attribute name="b">maybe</attribute><attribute name="t">soon</attribute>
            </attributes></event></events>"#,
        );
        assert_eq!(
            ocel.objects[0].attributes[0].value,
            OCELAttributeValue::String(" 12x ".to_string())
        );
        let values: Vec<_> = ocel.events[0]
            .attributes
            .iter()
            .map(|a| a.value.clone())
            .collect();
        assert_eq!(
            values,
            vec![
                OCELAttributeValue::String("maybe".to_string()),
                OCELAttributeValue::String("soon".to_string()),
            ]
        );
    }
}
