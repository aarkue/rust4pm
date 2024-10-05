use std::{collections::HashMap, time::UNIX_EPOCH};

use chrono::{Date, DateTime, FixedOffset};
use rusqlite::{Connection, Params, Row, Rows, Statement};
use serde::{Deserialize, Serialize};

use crate::{
    ocel::{
        ocel_struct::{OCELEvent, OCELObject, OCELObjectAttribute, OCELType, OCELTypeAttribute},
        xml_ocel_import::{parse_date, OCELImportOptions},
    },
    OCEL,
};

use super::ocel_struct::{
    ocel_type_string_to_attribute_type, OCELAttributeType, OCELAttributeValue,
};

#[derive(Debug, Serialize, Deserialize)]
struct MapTypeOCELRow {
    /// Name of the type (can contain spaces etc., e.g., `pay order`)
    ocel_type: String,
    /// Postfix name of the database (e.g., `PayOrder`)
    ocel_type_map: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct EventTableOCELRow {
    ocel_id: String,
    ocel_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PragmaRow {
    name: String,
    #[serde(rename = "type")]
    value_type: String,
}
const IGNORED_PRAGMA_COLUMNS: [&'static str; 3] = ["ocel_id", "ocel_time", "ocel_changed_field"];
const OCEL_CHANGED_FIELD: &'static str = "ocel_changed_field";
const OCEL_TIME_COLUMN: &'static str = "ocel_time";

pub fn import_ocel_sqlite(con: Connection) -> Result<OCEL, rusqlite::Error> {
    let mut ocel = OCEL {
        event_types: Vec::default(),
        object_types: Vec::default(),
        events: Vec::default(),
        objects: Vec::default(),
    };

    // Parse names of object types (and the table name postfixes)
    let mut s = con.prepare("SELECT * FROM event_map_type")?;
    let ev_map_type = query_all::<_>(&mut s, [])?;
    let ev_type_map: HashMap<String, String> = ev_map_type
        .and_then(|x| Ok::<_, rusqlite::Error>((x.get("ocel_type_map")?, x.get("ocel_type")?)))
        .flatten()
        .collect();

    let mut s = con.prepare("SELECT * FROM object_map_type")?;
    let ob_map_type = query_all::<_>(&mut s, [])?;

    let ev_type_map: HashMap<String, String> = ob_map_type
        .and_then(|x| Ok::<_, rusqlite::Error>((x.get("ocel_type_map")?, x.get("ocel_type")?)))
        .flatten()
        .collect();
    println!("{:?}", ev_type_map);
    println!("{:?}", ev_type_map);

    let mut object_map: HashMap<String, OCELObject> = HashMap::new();
    let mut event_map: HashMap<String, OCELEvent> = HashMap::new();

    for ob_type in ev_type_map.keys() {
        println!("{ob_type}");
        let mut s = con.prepare(format!("PRAGMA table_info(object_{ob_type})").as_str())?;
        let ob_attr_query = query_all::<_>(&mut s, [])?;
        let ob_type_attrs: Vec<OCELTypeAttribute> = ob_attr_query
            .and_then(|x| Ok::<(String, String), rusqlite::Error>((x.get("name")?, x.get("type")?)))
            .flatten()
            .filter(|(name, _)| !IGNORED_PRAGMA_COLUMNS.contains(&name.as_str()))
            .map(|(name, atype)| OCELTypeAttribute {
                name,
                value_type: sql_type_to_ocel(&atype).to_string(),
            })
            .collect();
        let mut s = con.prepare(
            format!("SELECT * FROM object_{ob_type} WHERE {OCEL_CHANGED_FIELD} is NULL").as_str(),
        )?;
        let objs = query_all::<_>(&mut s, [])?;
        objs.and_then(|x| {
            Ok::<(String, String, Vec<_>), rusqlite::Error>((
                x.get("ocel_id")?,
                x.get(OCEL_TIME_COLUMN)?,
                ob_type_attrs
                    .iter()
                    .map(|attr| Ok::<(&String, OCELAttributeValue),rusqlite::Error>((&attr.name, get_row_attribute_value(attr, x)?))).flatten()
                    .collect(),
            ))
        })
        .flatten()
        .for_each(|(ob_id, time, attrs)| {
            let time = parse_date(&time, &OCELImportOptions::default()).unwrap_or_default();
            let mut o = OCELObject {
                id: ob_id.clone(),
                object_type: ob_type.to_string(),
                attributes: Vec::default(),
                relationships: Vec::default(),
            };
            // Technically time should probably be set to UNIX epoch (1970-01-01 00:00 UTC) for these "initial" attribute values
            // however there are some OCEL logs for which this does not hold?
            if UNIX_EPOCH != time.into() {
                println!("Expected initial object attribute value to have UNIX epoch as time. Instead got {time:?}")
            }
            o.attributes.extend(attrs.into_iter().map(|(attr_name, attr_value)| OCELObjectAttribute {
                name: attr_name.clone(),

                value: attr_value,
                time,
            }));
            object_map.insert(ob_id, o);
        });

        let t = OCELType {
            name: ob_type.clone(),
            attributes: ob_type_attrs,
        };
        // Add object type to ocel
        ocel.object_types.push(t);
    }

    for ev_type in ev_type_map.keys() {
        println!("{ev_type}");
        let mut s = con.prepare(format!("PRAGMA table_info(event_{ev_type})").as_str())?;
        let ev_attr_query = query_all::<_>(&mut s, [])?;
        let ev_type_attrs: Vec<OCELTypeAttribute> = ev_attr_query
            .and_then(|x| Ok::<(String, String), rusqlite::Error>((x.get("name")?, x.get("type")?)))
            .flatten()
            .filter(|(name, _)| !IGNORED_PRAGMA_COLUMNS.contains(&name.as_str()))
            .map(|(name, atype)| OCELTypeAttribute {
                name,
                value_type: sql_type_to_ocel(&atype).to_string(),
            })
            .collect();
        let t = OCELType {
            name: ev_type.clone(),
            attributes: ev_type_attrs,
        };
        // con.prepare(format!("SELECT * FROM event_{}"))
        ocel.event_types.push(t);
    }

    ocel.objects = object_map.into_values().collect();
    // TODO: Continue SQLite importer
    Ok(ocel)
}

fn query_all<'a, P: Params>(s: &'a mut Statement<'_>, p: P) -> Result<Rows<'a>, rusqlite::Error> {
    let q = s.query(p)?;
    Ok(q)
}

fn sql_type_to_ocel(s: &str) -> OCELAttributeType {
    match s {
        "TEXT" => OCELAttributeType::String,
        "REAL" => OCELAttributeType::Float,
        "INTEGER" => OCELAttributeType::Integer,
        "BOOLEAN" => OCELAttributeType::Boolean,
        "TIMESTAMP" => OCELAttributeType::Time,
        _ => OCELAttributeType::String,
    }
}

fn get_row_attribute_value(
    a: &OCELTypeAttribute,
    r: &Row<'_>,
) -> Result<OCELAttributeValue, rusqlite::Error> {
    match ocel_type_string_to_attribute_type(&a.value_type) {
        OCELAttributeType::String => Ok(OCELAttributeValue::String(
            r.get::<_, String>(a.name.as_str())?,
        )),
        OCELAttributeType::Time => {
            let time_res = match r.get::<_, DateTime<FixedOffset>>(a.name.as_str()) {
                Ok(dt) => Ok(dt),
                Err(_) => parse_date(
                    r.get::<_, String>(a.name.as_str())?.as_str(),
                    &OCELImportOptions::default(),
                ).map_err(|e| rusqlite::Error::InvalidQuery),
            }?;
            Ok(OCELAttributeValue::Time(time_res))
        }
        OCELAttributeType::Integer => Ok(OCELAttributeValue::Integer(
            r.get::<_, i64>(a.name.as_str())?,
        )),
        OCELAttributeType::Float => {
            Ok(OCELAttributeValue::Float(r.get::<_, f64>(a.name.as_str())?))
        }
        OCELAttributeType::Boolean => Ok(OCELAttributeValue::Boolean(
            r.get::<_, bool>(a.name.as_str())?,
        )),
        // Or should Null be an Error result?
        OCELAttributeType::Null => Ok(OCELAttributeValue::Null),
    }
}

#[test]
fn test_sqlite_ocel() -> Result<(), rusqlite::Error> {
    let con = Connection::open("/home/aarkue/dow/order-management.sqlite").unwrap();
    let ocel = import_ocel_sqlite(con)?;
    println!("OCEL: {:#?}", ocel);

    Ok(())
}
