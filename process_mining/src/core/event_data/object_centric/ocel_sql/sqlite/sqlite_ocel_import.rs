use crate::core::event_data::object_centric::ocel_struct::OCEL;
use crate::core::event_data::object_centric::ocel_struct::{
    OCELAttributeValue, OCELEvent, OCELEventAttribute, OCELObject, OCELObjectAttribute,
    OCELRelationship, OCELTypeAttribute,
};
use crate::core::event_data::timestamp_utils::parse_timestamp;
use std::{collections::HashMap, ffi::CString};

use super::super::*;
use chrono::FixedOffset;
use rusqlite::{Connection, Params, Row, Rows, Statement};

fn try_get_column_date_val(
    r: &Row<'_>,
    column_name: &str,
) -> Result<DateTime<FixedOffset>, rusqlite::Error> {
    let dt = r.get::<_, DateTime<FixedOffset>>(column_name);
    dt.or_else(|_e| {
        r.get::<_, String>(column_name).and_then(|dt_str| {
            parse_timestamp(&dt_str, None, false).map_err(|_e| rusqlite::Error::InvalidQuery)
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
/// Rejects a file whose object-type table has no `ocel_changed_field` column. Use
/// [`import_ocel_sqlite_from_con_with_options`] to tolerate that instead.
///
/// Note: This function is only available if the `ocel-sqlite` feature is enabled.
///
pub fn import_ocel_sqlite_from_con(con: Connection) -> Result<OCEL, rusqlite::Error> {
    import_ocel_sqlite_from_con_with_options(con, SqlOcelImportOptions::default())
}

/// Import [`OCEL`] log from `SQLite` connection, with explicit [`SqlOcelImportOptions`].
///
/// Note: This function is only available if the `ocel-sqlite` feature is enabled.
pub fn import_ocel_sqlite_from_con_with_options(
    con: Connection,
    options: SqlOcelImportOptions,
) -> Result<OCEL, rusqlite::Error> {
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
        let table_name = format!("object_{ob_type}");
        let mut s =
            con.prepare(format!("PRAGMA table_info({})", quoted_str(&table_name)).as_str())?;
        let ob_attr_query = query_all::<_>(&mut s, [])?;
        let raw_ob_columns: Vec<(String, String)> = ob_attr_query
            .and_then(|x| Ok::<(String, String), rusqlite::Error>((x.get("name")?, x.get("type")?)))
            .flatten()
            .collect();
        let ObjectTablePlan {
            attributes: ob_type_attrs,
            initial_query,
            changed_query,
        } = plan_object_table(con.path(), ob_type, &table_name, raw_ob_columns, options)
            .map_err(rusqlite::Error::InvalidColumnName)?;
        let mut s = con.prepare(initial_query.as_str())?;
        let objs = query_all::<_>(&mut s, [])?;
        objs.and_then(|x| {
            Ok::<(String, Vec<_>), rusqlite::Error>((
                x.get(OCEL_ID_COLUMN)?,
                // We do not really need the time of initial attributes.
                // try_get_column_date_val(x, OCEL_TIME_COLUMN)?,
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
        .for_each(|(ob_id, attrs)| {
            let mut o = OCELObject {
                id: ob_id.clone(),
                object_type: ob_type_ocel.to_string(),
                attributes: Vec::default(),
                relationships: Vec::default(),
            };
            // Technically time should probably be set to UNIX epoch (1970-01-01 00:00 UTC) for these "initial" attribute values
            // however there are some OCEL logs for which this does not hold?
            let time = DateTime::UNIX_EPOCH.into();
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
        if let Some(changed_query) = &changed_query {
            let mut s = con.prepare(changed_query.as_str())?;
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
                    try_get_column_date_val(x, OCEL_TIME_COLUMN)?,
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
                        name: changed_field,
                        value: changed_val,
                        time,
                    });
            });
        }

        let t = OCELType {
            name: ob_type_ocel.clone(),
            attributes: ob_type_attrs,
        };
        // Add object type to ocel
        ocel.object_types.push(t);
    }

    for (ev_type, ev_type_ocel) in ev_type_map.iter() {
        let mut s = con.prepare(
            format!(
                "PRAGMA table_info({})",
                quoted_str(&format!("event_{ev_type}"))
            )
            .as_str(),
        )?;
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
        let mut s = con.prepare(
            format!(
                "SELECT * FROM {}",
                quoted_ident(&format!("event_{ev_type}"))
            )
            .as_str(),
        )?;
        let evs = query_all::<_>(&mut s, [])?;
        evs.and_then(|x| {
            Ok::<(String, _, Vec<_>), rusqlite::Error>((
                x.get(OCEL_ID_COLUMN)?,
                try_get_column_date_val(x, OCEL_TIME_COLUMN)?,
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
/// Note: This function is only available if the `ocel-sqlite` feature is enabled.
pub fn import_ocel_sqlite_from_path<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<OCEL, rusqlite::Error> {
    let con = Connection::open(path)?;
    import_ocel_sqlite_from_con(con)
}

///
/// Import an [`OCEL`] `SQLite` file from the given path, with custom options
///
/// Note: This function is only available if the `ocel-sqlite` feature is enabled.
pub fn import_ocel_sqlite_from_path_with_options<P: AsRef<std::path::Path>>(
    path: P,
    options: SqlOcelImportOptions,
) -> Result<OCEL, rusqlite::Error> {
    let con = Connection::open(path)?;
    import_ocel_sqlite_from_con_with_options(con, options)
}

///
/// Import an [`OCEL`] `SQLite` file from the given byte slice
///
/// Note: This function is only available if the `ocel-sqlite` feature is enabled.
pub fn import_ocel_sqlite_from_slice(bytes: &[u8]) -> Result<OCEL, rusqlite::Error> {
    let mut con = Connection::open_in_memory()?;
    deserialize_sqlite_slice(&mut con, bytes, false)?;
    import_ocel_sqlite_from_con(con)
}

fn deserialize_sqlite_slice(
    con: &mut Connection,
    data: &[u8],
    read_only: bool,
) -> Result<(), rusqlite::Error> {
    let schema = CString::new("main")?;
    let sz = data.len().try_into().unwrap();
    let flags = if read_only {
        rusqlite::ffi::SQLITE_DESERIALIZE_READONLY
    } else {
        rusqlite::ffi::SQLITE_DESERIALIZE_RESIZEABLE
    };
    let _rc = unsafe {
        rusqlite::ffi::sqlite3_deserialize(
            con.handle(),
            schema.as_ptr(),
            data.as_ptr() as *mut u8,
            sz,
            sz,
            flags,
        )
    };
    Ok(())
}
#[cfg(test)]
mod sqlite_tests {
    use std::collections::HashSet;

    use chrono::DateTime;
    use rusqlite::Connection;

    use crate::{
        core::event_data::object_centric::{
            ocel_sql::import_ocel_sqlite_from_con,
            ocel_struct::{OCELAttributeValue, OCELObjectAttribute, OCELRelationship},
        },
        test_utils::get_test_data_path,
    };

    #[test]
    fn test_sqlite_ocel() -> Result<(), rusqlite::Error> {
        let path = get_test_data_path()
            .join("ocel")
            .join("order-management.sqlite");

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

#[cfg(test)]
mod missing_changed_field_tests {
    use chrono::DateTime;
    use rusqlite::Connection;

    use crate::core::event_data::object_centric::{
        ocel_sql::{
            import_ocel_sqlite_from_con, import_ocel_sqlite_from_con_with_options,
            SqlOcelImportOptions,
        },
        ocel_struct::{OCELAttributeValue, OCELObjectAttribute},
    };

    /// A non-conforming `SQLite` file: object type `Truck` has no `ocel_changed_field` column,
    /// but does carry a misspelled `ocel_change_field` one whose value names a real attribute.
    fn build_fixture(path: &std::path::Path) {
        let con = Connection::open(path).unwrap();
        con.execute_batch(
            "
            CREATE TABLE event_map_type (ocel_type_map TEXT, ocel_type TEXT);
            CREATE TABLE object_map_type (ocel_type_map TEXT, ocel_type TEXT);
            CREATE TABLE event_object (ocel_event_id TEXT, ocel_object_id TEXT, ocel_qualifier TEXT);
            CREATE TABLE object_object (ocel_source_id TEXT, ocel_target_id TEXT, ocel_qualifier TEXT);
            CREATE TABLE event_Ev (ocel_id TEXT PRIMARY KEY, ocel_time TIMESTAMP);
            CREATE TABLE object_Truck (
                ocel_id TEXT PRIMARY KEY,
                ocel_time TIMESTAMP,
                driver TEXT,
                ocel_change_field TEXT
            );
            INSERT INTO object_map_type VALUES ('Truck', 'Truck');
            INSERT INTO event_map_type VALUES ('Ev', 'Ev');
            INSERT INTO event_Ev VALUES ('e1', '2020-01-01T00:00:00+00:00');
            INSERT INTO object_Truck VALUES ('t1', '2020-01-01T00:00:00+00:00', 'Alice', NULL);
            INSERT INTO object_Truck VALUES ('t2', '2020-01-02T00:00:00+00:00', 'Bob', 'driver');
            ",
        )
        .unwrap();
    }

    /// The column is only missing where there is no attribute change to record, so a file
    /// without it is read rather than refused.
    #[test]
    fn default_import_reads_a_file_without_the_changed_field_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no-changed-field.sqlite");
        build_fixture(&path);

        let con = Connection::open(&path).unwrap();
        let ocel =
            import_ocel_sqlite_from_con(con).expect("a file without the column must still be read");
        assert_eq!(ocel.objects.len(), 2, "both Truck objects must be present");
    }

    /// Holding the file to the specification is opt-in, and names what is wrong with it.
    #[test]
    fn strict_import_rejects_missing_changed_field_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no-changed-field.sqlite");
        build_fixture(&path);

        let con = Connection::open(&path).unwrap();
        let err = import_ocel_sqlite_from_con_with_options(
            con,
            SqlOcelImportOptions {
                allow_missing_changed_field: false,
            },
        )
        .unwrap_err();
        let msg = err.to_string();

        assert!(
            msg.contains("Truck"),
            "message should name the object type: {msg}"
        );
        assert!(
            msg.contains("object_Truck"),
            "message should name the table: {msg}"
        );
        assert!(
            msg.contains("ocel_changed_field"),
            "message should name the missing column: {msg}"
        );
        assert!(
            msg.contains("does not conform"),
            "message should say the file is non-conforming: {msg}"
        );
        assert!(
            msg.contains("ocel_change_field"),
            "message should mention the misspelled column it found: {msg}"
        );
    }

    #[test]
    fn a_file_without_the_changed_field_column_reads_as_initial_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no-changed-field.sqlite");
        build_fixture(&path);

        let con = Connection::open(&path).unwrap();
        let ocel = import_ocel_sqlite_from_con_with_options(
            con,
            SqlOcelImportOptions {
                allow_missing_changed_field: true,
            },
        )
        .unwrap();

        assert_eq!(ocel.objects.len(), 2, "both Truck objects must be present");

        let t1 = ocel.objects.iter().find(|o| o.id == "t1").unwrap();
        assert_eq!(t1.object_type, "Truck");
        assert_eq!(
            t1.attributes,
            vec![OCELObjectAttribute {
                name: "driver".to_string(),
                value: OCELAttributeValue::String("Alice".to_string()),
                time: DateTime::UNIX_EPOCH.into(),
            }],
            "t1's ocel_change_field cell is NULL, so it contributes no attribute; \
             it must not be invented as a change record"
        );

        let t2 = ocel.objects.iter().find(|o| o.id == "t2").unwrap();
        assert_eq!(t2.object_type, "Truck");
        let mut t2_attrs = t2.attributes.clone();
        t2_attrs.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(
            t2_attrs,
            vec![
                OCELObjectAttribute {
                    name: "driver".to_string(),
                    value: OCELAttributeValue::String("Bob".to_string()),
                    time: DateTime::UNIX_EPOCH.into(),
                },
                OCELObjectAttribute {
                    name: "ocel_change_field".to_string(),
                    value: OCELAttributeValue::String("driver".to_string()),
                    time: DateTime::UNIX_EPOCH.into(),
                },
            ],
            "no attribute-change row must be invented from the misspelled column"
        );

        let truck_type = ocel
            .object_types
            .iter()
            .find(|t| t.name == "Truck")
            .unwrap();
        let mut attr_names: Vec<_> = truck_type
            .attributes
            .iter()
            .map(|a| a.name.clone())
            .collect();
        attr_names.sort();
        assert_eq!(attr_names, vec!["driver", "ocel_change_field"]);
    }
}
