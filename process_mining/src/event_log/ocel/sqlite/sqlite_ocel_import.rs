use std::{collections::HashMap, time::UNIX_EPOCH};

use chrono::{DateTime, FixedOffset};
use rusqlite::{Connection, Params, Row, Rows, Statement};
use super::*;
use crate::{
    ocel::{
        ocel_struct::{
            OCELEvent, OCELEventAttribute, OCELObject, OCELObjectAttribute, OCELType,
            OCELTypeAttribute,
        },
        xml_ocel_import::{parse_date, OCELImportOptions},
    },
    OCEL,
};

use crate::ocel::ocel_struct::{OCELAttributeType, OCELAttributeValue, OCELRelationship};




fn try_get_column_date_val(
    r: &Row<'_>,
    column_name: &str,
) -> Result<DateTime<FixedOffset>, rusqlite::Error> {
    let dt = r.get::<_, DateTime<FixedOffset>>(column_name);
    dt.or_else(|_e| {
        r.get::<_, String>(column_name).and_then(|dt_str| {
            parse_date(&dt_str, &OCELImportOptions::default())
                .map_err(|_e| rusqlite::Error::InvalidQuery)
        })
    })
}

fn get_row_attribute_value(
    a: &OCELTypeAttribute,
    r: &Row<'_>,
) -> Result<OCELAttributeValue, rusqlite::Error> {
    match OCELAttributeType::from_type_str(&a.value_type) {
        OCELAttributeType::String => Ok(OCELAttributeValue::String(
            r.get::<_, String>(a.name.as_str())?,
        )),
        OCELAttributeType::Time => {
            let time_res = try_get_column_date_val(r, &a.name)?;
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


/// Import [`OCEL`] log from `SQLite` connection
///
/// If you want to import from a filepath, see [`import_ocel_sqlite_from_path`] instead.
///
pub fn import_ocel_sqlite_from_con(con: Connection) -> Result<OCEL, rusqlite::Error> {
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
        .and_then(|x| {
            Ok::<_, rusqlite::Error>((x.get(OCEL_TYPE_MAP_COLUMN)?, x.get(OCEL_TYPE_COLUMN)?))
        })
        .flatten()
        .collect();

    let mut s = con.prepare("SELECT * FROM object_map_type")?;
    let ob_map_type = query_all::<_>(&mut s, [])?;

    let ob_type_map: HashMap<String, String> = ob_map_type
        .and_then(|x| {
            Ok::<_, rusqlite::Error>((x.get(OCEL_TYPE_MAP_COLUMN)?, x.get(OCEL_TYPE_COLUMN)?))
        })
        .flatten()
        .collect();

    let mut object_map: HashMap<String, OCELObject> = HashMap::new();
    let mut event_map: HashMap<String, OCELEvent> = HashMap::new();

    for (ob_type, ob_type_ocel) in ob_type_map.iter() {
        let mut s = con.prepare(format!("PRAGMA table_info('object_{ob_type}')").as_str())?;
        let ob_attr_query = query_all::<_>(&mut s, [])?;
        let ob_type_attrs: Vec<OCELTypeAttribute> = ob_attr_query
            .and_then(|x| Ok::<(String, String), rusqlite::Error>((x.get("name")?, x.get("type")?)))
            .flatten()
            .filter(|(name, _)| !IGNORED_PRAGMA_COLUMNS.contains(&name.as_str()))
            .map(|(name, atype)| OCELTypeAttribute {
                name,
                value_type: sql_type_to_ocel(&atype).to_type_string(),
            })
            .collect();
        let mut s = con.prepare(
            format!("SELECT * FROM 'object_{ob_type}' WHERE {OCEL_CHANGED_FIELD} IS NULL").as_str(),
        )?;
        let objs = query_all::<_>(&mut s, [])?;
        objs.and_then(|x| {
            Ok::<(String, _, Vec<_>), rusqlite::Error>((
                x.get(OCEL_ID_COLUMN)?,
                try_get_column_date_val(x,OCEL_TIME_COLUMN)?,
                ob_type_attrs
                    .iter()
                    .flat_map(|attr| {
                        Ok::<(&String, OCELAttributeValue), rusqlite::Error>((
                            &attr.name,
                            get_row_attribute_value(attr, x)?,
                        ))
                    })
                    .collect(),
            ))
        })
        .flatten()
        .for_each(|(ob_id, mut time, attrs)| {
            let mut o = OCELObject {
                id: ob_id.clone(),
                object_type: ob_type_ocel.to_string(),
                attributes: Vec::default(),
                relationships: Vec::default(),
            };
            // Technically time should probably be set to UNIX epoch (1970-01-01 00:00 UTC) for these "initial" attribute values
            // however there are some OCEL logs for which this does not hold?
            if UNIX_EPOCH != time.into() {
                // eprintln!("Expected initial object attribute value to have UNIX epoch as time. Instead got {time:?}. Overwriting to UNIX epoch.");
                time = DateTime::UNIX_EPOCH.into();
            }
            o.attributes
                .extend(
                    attrs
                        .into_iter()
                        .map(|(attr_name, attr_value)| OCELObjectAttribute {
                            name: attr_name.clone(),

                            value: attr_value,
                            time,
                        }),
                );
            object_map.insert(ob_id, o);
        });
        // Get changed attributes
        let mut s = con.prepare(
            format!("SELECT * FROM 'object_{ob_type}' WHERE {OCEL_CHANGED_FIELD} IS NOT NULL")
                .as_str(),
        )?;
        let objs = query_all::<_>(&mut s, [])?;
        objs.and_then(|x| {
            let changed_field: String = x.get(OCEL_CHANGED_FIELD)?;
            let changed_val = ob_type_attrs
                .iter()
                .find(|at| at.name == changed_field)
                .ok_or(rusqlite::Error::InvalidQuery)
                .and_then(|attr| get_row_attribute_value(attr, x))?;
            Ok::<(String, _, String, OCELAttributeValue), rusqlite::Error>((
                x.get(OCEL_ID_COLUMN)?,
                try_get_column_date_val(x,OCEL_TIME_COLUMN)?,
                changed_field,
                changed_val,
            ))
        })
        .flatten()
        .for_each(|(ob_id, time, changed_field, changed_val)| {
            object_map
                .entry(ob_id.clone())
                .or_insert(OCELObject {
                    id: ob_id,
                    object_type: ob_type.clone(),
                    attributes: Vec::default(),
                    relationships: Vec::default(),
                })
                .attributes
                .push(OCELObjectAttribute {
                    name: changed_field.clone(),
                    value: changed_val,
                    time,
                });
        });

        let t = OCELType {
            name: ob_type_ocel.clone(),
            attributes: ob_type_attrs,
        };
        // Add object type to ocel
        ocel.object_types.push(t);
    }

    for (ev_type, ev_type_ocel) in ev_type_map.iter() {
        let mut s = con.prepare(format!("PRAGMA table_info('event_{ev_type}')").as_str())?;
        let ev_attr_query = query_all::<_>(&mut s, [])?;
        let ev_type_attrs: Vec<OCELTypeAttribute> = ev_attr_query
            .and_then(|x| Ok::<(String, String), rusqlite::Error>((x.get("name")?, x.get("type")?)))
            .flatten()
            .filter(|(name, _)| !IGNORED_PRAGMA_COLUMNS.contains(&name.as_str()))
            .map(|(name, atype)| OCELTypeAttribute {
                name,
                value_type: sql_type_to_ocel(&atype).to_type_string(),
            })
            .collect();
        // Next, query events
        let mut s = con.prepare(format!("SELECT * FROM 'event_{ev_type}'").as_str())?;
        let evs = query_all::<_>(&mut s, [])?;
        evs.and_then(|x| {
            Ok::<(String, _, Vec<_>), rusqlite::Error>((
                x.get(OCEL_ID_COLUMN)?,
                try_get_column_date_val(x,OCEL_TIME_COLUMN)?,
                ev_type_attrs
                    .iter()
                    .flat_map(|attr| {
                        Ok::<(&String, OCELAttributeValue), rusqlite::Error>((
                            &attr.name,
                            get_row_attribute_value(attr, x)?,
                        ))
                    })
                    .collect(),
            ))
        })
        .flatten()
        .for_each(|(ev_id, time, attrs)| {
            let mut e = OCELEvent {
                id: ev_id.clone(),
                event_type: ev_type_ocel.to_string(),
                time,
                attributes: Vec::default(),
                relationships: Vec::default(),
            };
            e.attributes
                .extend(
                    attrs
                        .into_iter()
                        .map(|(attr_name, attr_value)| OCELEventAttribute {
                            name: attr_name.clone(),
                            value: attr_value,
                        }),
                );
            event_map.insert(ev_id, e);
        });
        let t = OCELType {
            name: ev_type_ocel.clone(),
            attributes: ev_type_attrs,
        };
        ocel.event_types.push(t);
    }

    // E2O Relationships
    let mut s = con.prepare("SELECT * FROM event_object".to_string().as_str())?;
    let evs = query_all::<_>(&mut s, [])?;
    evs.and_then(|x| {
        Ok::<(String, String, String), rusqlite::Error>((
            x.get(OCEL_E2O_EVENT_ID_COLUMN)?,
            x.get(OCEL_E2O_OBJECT_ID_COLUMN)?,
            x.get(OCEL_REL_QUALIFIER_COLUMN)?,
        ))
    })
    .flatten()
    .for_each(|(ev_id, ob_id, qualifier)| {
        if let Some(ev) = event_map.get_mut(&ev_id) {
            ev.relationships.push(OCELRelationship {
                object_id: ob_id,
                qualifier,
            });
        } else {
            eprintln!(
                "Warning: E2O relationship not added as event with ID {ev_id} was not found."
            );
        }
    });

    // O2O Relationships
    let mut s = con.prepare("SELECT * FROM object_object".to_string().as_str())?;
    let evs = query_all::<_>(&mut s, [])?;
    evs.and_then(|x| {
            Ok::<(String, String, String), rusqlite::Error>((
                x.get(OCEL_O2O_SOURCE_ID_COLUMN)?,
                x.get(OCEL_O2O_TARGET_ID_COLUMN)?,
                x.get(OCEL_REL_QUALIFIER_COLUMN)?,
            ))
        })
        .flatten()
        .for_each(|(source_ob_id, target_ob_id, qualifier)| {
            if let Some(ev) = object_map.get_mut(&source_ob_id) {
                ev.relationships.push(OCELRelationship {
                    object_id: target_ob_id,
                    qualifier,
                });
            }else{
                eprintln!("Warning: O2O relationship not added as object with ID {source_ob_id} was not found.");
            }
        });

    ocel.objects = object_map.into_values().collect();
    ocel.events = event_map.into_values().collect();
    Ok(ocel)
}

fn query_all<'a, P: Params>(s: &'a mut Statement<'_>, p: P) -> Result<Rows<'a>, rusqlite::Error> {
    let q = s.query(p)?;
    Ok(q)
}


///
/// Import an [`OCEL`] `SQLite` file from the given path
///
pub fn import_ocel_sqlite_from_path<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<OCEL, rusqlite::Error> {
    let con = Connection::open(path)?;
    import_ocel_sqlite_from_con(con)
}

#[cfg(test)]
mod sqlite_tests {
    use std::{collections::HashSet, path::PathBuf};

    use chrono::DateTime;
    use rusqlite::Connection;

    use crate::{import_ocel_sqlite_from_con, ocel::ocel_struct::{OCELAttributeValue, OCELObjectAttribute, OCELRelationship}};

    #[test]
    fn test_sqlite_ocel() -> Result<(), rusqlite::Error> {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("src");
        path.push("event_log");
        path.push("tests");
        path.push("test_data");
        path.push("order-management.sqlite");

        let con = Connection::open(path).unwrap();
        let ocel = import_ocel_sqlite_from_con(con)?;

        assert_eq!(ocel.objects.len(), 10840);
        assert_eq!(ocel.events.len(), 21008);

        assert_eq!(ocel.event_types.len(), 11);
        assert_eq!(ocel.object_types.len(), 6);

        let po_1337 = ocel.events.iter().find(|e| e.id == "pay_o-991337").unwrap();
        assert_eq!(
            po_1337.time,
            DateTime::parse_from_rfc3339("2023-12-13T10:31:50+00:00").unwrap()
        );
        assert_eq!(
            po_1337
                .relationships
                .clone()
                .into_iter()
                .collect::<HashSet<_>>(),
            vec![
                ("Echo", "product"),
                ("iPad", "product"),
                ("iPad Pro", "product"),
                ("o-991337", "order"),
                ("i-885283", "item"),
                ("i-885284", "item"),
                ("i-885285", "item"),
            ]
            .into_iter()
            .map(|(o_id, q)| OCELRelationship {
                object_id: o_id.to_string(),
                qualifier: q.to_string()
            })
            .collect::<HashSet<_>>()
        );

        let o_1337 = ocel.objects.iter().find(|o| o.id == "o-991337").unwrap();
        assert_eq!(
            o_1337.attributes,
            vec![OCELObjectAttribute {
                name: "price".to_string(),
                value: OCELAttributeValue::Float(1909.04),
                time: DateTime::UNIX_EPOCH.into()
            }]
        );
        assert_eq!(
            o_1337
                .relationships
                .clone()
                .into_iter()
                .collect::<HashSet<_>>(),
            vec![
                ("i-885283", "comprises"),
                ("i-885284", "comprises"),
                ("i-885285", "comprises"),
            ]
            .into_iter()
            .map(|(o_id, q)| OCELRelationship {
                object_id: o_id.to_string(),
                qualifier: q.to_string()
            })
            .collect::<HashSet<_>>()
        );

        Ok(())
    }
}
