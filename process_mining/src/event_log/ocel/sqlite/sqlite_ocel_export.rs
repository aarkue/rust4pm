use std::collections::HashMap;

use super::*;
use crate::{ocel::ocel_struct::OCELTypeAttribute, OCEL};
use chrono::DateTime;
use rusqlite::Connection;

use crate::ocel::ocel_struct::OCELAttributeType;

fn clean_sql_name(type_name: &str) -> String {
    type_name
        .chars()
        .map(|c| if c != '\'' && c != '\\' { c } else { '-' })
        // .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

///
/// Export an [`OCEL`] to an `SQLite` file at the specified path
///
/// Note: This function is only available if the `ocel-sqlite` feature is enabled.
///
pub fn export_ocel_sqlite_to_path<P: AsRef<std::path::Path>>(
    ocel: &OCEL,
    path: P,
) -> Result<(), rusqlite::Error> {
    let con = Connection::open(path)?;
    export_ocel_sqlite_to_con(&con, ocel)
}

///
/// Export an [`OCEL`] to an `SQLite` to a byte array
///
/// Note: This function is only available if the `ocel-sqlite` feature is enabled.
pub fn export_ocel_sqlite_to_vec(ocel: &OCEL) -> Result<Vec<u8>, rusqlite::Error> {
    let con = Connection::open_in_memory()?;
    export_ocel_sqlite_to_con(&con, ocel)?;
    let data = con.serialize(rusqlite::DatabaseName::Main)?;
    Ok((*data).to_vec())
}

///
/// Export an [`OCEL`] log to a `SQLite` connection
///
/// Note: This function is only available if the `ocel-sqlite` feature is enabled.
///
pub fn export_ocel_sqlite_to_con(con: &Connection, ocel: &OCEL) -> Result<(), rusqlite::Error> {
    // event
    con.execute(&format!(r#"CREATE TABLE IF NOT EXISTS "event" ("{OCEL_ID_COLUMN}"	TEXT, "{OCEL_TYPE_COLUMN}"	TEXT, PRIMARY KEY("{OCEL_ID_COLUMN}"), FOREIGN KEY("{OCEL_TYPE_COLUMN}") REFERENCES "event_map_type" ("{OCEL_TYPE_COLUMN}"))"#), [])?;
    // object
    con.execute(&format!(r#"CREATE TABLE IF NOT EXISTS "object" ("{OCEL_ID_COLUMN}"	TEXT, "{OCEL_TYPE_COLUMN}"	TEXT, PRIMARY KEY("{OCEL_ID_COLUMN}"), FOREIGN KEY("{OCEL_TYPE_COLUMN}") REFERENCES "object_map_type" ("{OCEL_TYPE_COLUMN}"))"#), [])?;

    // event map type
    con.execute(&format!(r#"CREATE TABLE IF NOT EXISTS "event_map_type" ("{OCEL_TYPE_COLUMN}" TEXT, "{OCEL_TYPE_MAP_COLUMN}"	TEXT, PRIMARY KEY("{OCEL_TYPE_COLUMN}"))"#), [])?;
    // object map type
    con.execute(&format!(r#"CREATE TABLE IF NOT EXISTS "object_map_type" ("{OCEL_TYPE_COLUMN}" TEXT, "{OCEL_TYPE_MAP_COLUMN}"	TEXT, PRIMARY KEY("{OCEL_TYPE_COLUMN}"))"#), [])?;

    // O2O (object_object)
    // , FOREIGN KEY("{OCEL_O2O_SOURCE_ID_COLUMN}") REFERENCES "object"("{OCEL_ID_COLUMN}"), FOREIGN KEY("{OCEL_O2O_TARGET_ID_COLUMN}") REFERENCES "object"("{OCEL_ID_COLUMN}")
    con.execute(&format!(r#"CREATE TABLE IF NOT EXISTS "object_object" ("{OCEL_O2O_SOURCE_ID_COLUMN}" TEXT, "{OCEL_O2O_TARGET_ID_COLUMN}" TEXT, "{OCEL_REL_QUALIFIER_COLUMN}" TEXT, PRIMARY KEY("{OCEL_O2O_SOURCE_ID_COLUMN}", "{OCEL_O2O_TARGET_ID_COLUMN}", "{OCEL_REL_QUALIFIER_COLUMN}"))"#), [])?;
    // E2O (event_object)
    con.execute(&format!(r#"CREATE TABLE IF NOT EXISTS "event_object" ("{OCEL_E2O_EVENT_ID_COLUMN}" TEXT, "{OCEL_E2O_OBJECT_ID_COLUMN}" TEXT, "{OCEL_REL_QUALIFIER_COLUMN}" TEXT, PRIMARY KEY("{OCEL_E2O_EVENT_ID_COLUMN}", "{OCEL_E2O_OBJECT_ID_COLUMN}", "{OCEL_REL_QUALIFIER_COLUMN}"), FOREIGN KEY("{OCEL_E2O_EVENT_ID_COLUMN}") REFERENCES "event"("{OCEL_ID_COLUMN}"), FOREIGN KEY("{OCEL_E2O_OBJECT_ID_COLUMN}") REFERENCES "object"("{OCEL_ID_COLUMN}"))"#), [])?;

    con.execute(&format!("CREATE INDEX IF NOT EXISTS 'event_object_source' ON 'event_object' ('{OCEL_E2O_EVENT_ID_COLUMN}' ASC)"),[])?;
    con.execute(&format!("CREATE INDEX IF NOT EXISTS 'object_object_source' ON 'object_object' ('{OCEL_O2O_SOURCE_ID_COLUMN}' ASC)"),[])?;

    let mut et_attr_map: HashMap<&String, &Vec<OCELTypeAttribute>> = HashMap::new();
    // Tables for event types
    for et in &ocel.event_types {
        let mut attr_cols = et
            .attributes
            .iter()
            .map(|att| {
                format!(
                    "'{}' {}",
                    clean_sql_name(&att.name),
                    ocel_type_to_sql(&OCELAttributeType::from_type_str(&att.value_type))
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        if !attr_cols.is_empty() {
            attr_cols.push(',');
        }
        et_attr_map.insert(&et.name, &et.attributes);
        con.execute(&format!(r#"CREATE TABLE IF NOT EXISTS "event_{}" ("{OCEL_ID_COLUMN}"	TEXT, "{OCEL_TIME_COLUMN}"	TIMESTAMP,{attr_cols} PRIMARY KEY("{OCEL_ID_COLUMN}"))"#,clean_sql_name(&et.name)), [])?;

        con.execute(
            &format!(
                "INSERT INTO 'event_map_type' VALUES ('{}', '{}')",
                et.name,
                clean_sql_name(&et.name)
            ),
            [],
        )?;
    }

    let mut ot_attr_map: HashMap<&String, &Vec<OCELTypeAttribute>> = HashMap::new();

    // Tables for object types
    for ot in &ocel.object_types {
        let mut attr_cols = ot
            .attributes
            .iter()
            .map(|att| {
                format!(
                    "'{}' {}",
                    clean_sql_name(&att.name),
                    ocel_type_to_sql(&OCELAttributeType::from_type_str(&att.value_type))
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        if !attr_cols.is_empty() {
            attr_cols.insert_str(0, ", ");
            // attr_cols.push(',');
        }
        ot_attr_map.insert(&ot.name, &ot.attributes);
        con.execute(&format!(r#"CREATE TABLE IF NOT EXISTS "object_{}" ("{OCEL_ID_COLUMN}"	TEXT, "{OCEL_TIME_COLUMN}" TIMESTAMP, {OCEL_CHANGED_FIELD} TEXT{attr_cols})"#,clean_sql_name(&ot.name)), [])?;

        con.execute(
            &format!(
                "INSERT INTO 'object_map_type' VALUES ('{}', '{}')",
                ot.name,
                clean_sql_name(&ot.name)
            ),
            [],
        )?;
    }

    con.execute("BEGIN TRANSACTION", [])?;
    for o in &ocel.objects {
        con.execute(
            &format!(
                "INSERT INTO 'object' VALUES ('{}', '{}')",
                o.id, o.object_type
            ),
            [],
        )?;
        // Table for object type with initial attribute values
        let mut attr_vals = ot_attr_map
            .get(&o.object_type)
            .unwrap()
            .iter()
            .map(|a| {
                let initial_val = o
                    .attributes
                    .iter()
                    .find(|oa| oa.name == a.name && oa.time == DateTime::UNIX_EPOCH);
                if let Some(val) = initial_val {
                    format!("'{}'", val.value)
                } else {
                    "NULL".to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        if !attr_vals.is_empty() {
            attr_vals.insert_str(0, ", ");
        }
        con.execute(
            &format!(
                "INSERT INTO 'object_{}' VALUES ('{}','{}', NULL{})",
                clean_sql_name(&o.object_type),
                o.id,
                DateTime::UNIX_EPOCH,
                attr_vals
            ),
            [],
        )?;

        // Object attributes changes
        for attr in &o.attributes {
            if attr.time != DateTime::UNIX_EPOCH {
                let mut attr_vals = ot_attr_map
                    .get(&o.object_type)
                    .unwrap()
                    .iter()
                    .map(|a| {
                        if a.name == attr.name {
                            format!("'{}'", attr.value)
                        } else {
                            "NULL".to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                if !attr_vals.is_empty() {
                    attr_vals.insert_str(0, ", ");
                }
                con.execute(
                    &format!(
                        "INSERT INTO 'object_{}' VALUES ('{}','{}', '{}'{})",
                        clean_sql_name(&o.object_type),
                        o.id,
                        attr.time,
                        clean_sql_name(&attr.name),
                        attr_vals
                    ),
                    [],
                )?;
            }
        }
    }
    // Do O2O AFTER so that the referenced objects already exist
    for o in &ocel.objects {
        // O2O Relationships
        for rel in &o.relationships {
            con.execute(
                &format!(
                    "INSERT INTO 'object_object' VALUES ('{}', '{}', '{}')",
                    o.id, rel.object_id, rel.qualifier
                ),
                [],
            )?;
        }
    }

    for e in &ocel.events {
        con.execute(
            &format!(
                "INSERT INTO 'event' VALUES ('{}', '{}')",
                e.id, e.event_type,
            ),
            [],
        )?;
        // Table for event type with attribute values
        let mut attr_vals = et_attr_map
            .get(&e.event_type)
            .unwrap()
            .iter()
            .map(|a| {
                let value = e.attributes.iter().find(|oa| oa.name == a.name);
                if let Some(val) = value {
                    format!("'{}'", val.value)
                } else {
                    "NULL".to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        if !attr_vals.is_empty() {
            attr_vals.insert_str(0, ", ");
        }
        con.execute(
            &format!(
                "INSERT INTO 'event_{}' VALUES ('{}','{}'{})",
                clean_sql_name(&e.event_type),
                e.id,
                e.time,
                attr_vals
            ),
            [],
        )?;
        // E2O Relationships
        for rel in &e.relationships {
            con.execute(
                &format!(
                    "INSERT INTO 'event_object' VALUES ('{}', '{}', '{}')",
                    e.id, rel.object_id, rel.qualifier
                ),
                [],
            )?;
        }
    }
    con.execute("COMMIT", [])?;

    Ok(())
}

#[cfg(test)]
mod sqlite_export_tests {
    use std::fs::{self};

    use rusqlite::Connection;

    use crate::{import_ocel_sqlite_from_con, utils::test_utils::get_test_data_path};

    use super::export_ocel_sqlite_to_con;

    #[test]
    fn test_sqlite_export_order_management() {
        let path = get_test_data_path()
            .join("ocel")
            .join("order-management.sqlite");

        let in_con = Connection::open(path).unwrap();
        let ocel = import_ocel_sqlite_from_con(in_con).unwrap();

        let export_path = get_test_data_path()
            .join("export")
            .join("order-management-EXPORT.sqlite");
        if let Err(_e) = fs::remove_file(&export_path) {}
        let con = Connection::open(&export_path).unwrap();

        export_ocel_sqlite_to_con(&con, &ocel).unwrap();

        let in_con2 = Connection::open(export_path).unwrap();
        let ocel2 = import_ocel_sqlite_from_con(in_con2).unwrap();
        println!(
            "Got OCEL2 in round trip SQLite with {} events and {} objects",
            ocel2.events.len(),
            ocel2.objects.len(),
        );
        assert_eq!(ocel.events.len(), ocel2.events.len());
        assert_eq!(ocel.objects.len(), ocel2.objects.len());
        assert_eq!(ocel.event_types.len(), ocel2.event_types.len());
        assert_eq!(ocel.object_types.len(), ocel2.object_types.len());
        assert_eq!(
            ocel.events.iter().find(|e| e.id == "pay_o-990005").unwrap(),
            ocel2
                .events
                .iter()
                .find(|e| e.id == "pay_o-990005")
                .unwrap()
        );
        assert_eq!(
            ocel.objects.iter().find(|e| e.id == "o-990005").unwrap(),
            ocel2.objects.iter().find(|e| e.id == "o-990005").unwrap()
        );
    }

    #[test]
    fn test_sqlite_export_p2p() {
        let path = get_test_data_path()
            .join("ocel")
            .join("ocel2-p2p.sqlite");

        let in_con = Connection::open(path).unwrap();
        let ocel = import_ocel_sqlite_from_con(in_con).unwrap();

        let export_path = get_test_data_path()
            .join("export")
            .join("ocel2-p2p-EXPORT.sqlite");
        if let Err(_e) = fs::remove_file(&export_path) {}
        let con = Connection::open(&export_path).unwrap();

        export_ocel_sqlite_to_con(&con, &ocel).unwrap();

        let in_con2 = Connection::open(export_path).unwrap();
        let ocel2 = import_ocel_sqlite_from_con(in_con2).unwrap();
        println!(
            "Got OCEL2 in round trip SQLite with {} events and {} objects",
            ocel2.events.len(),
            ocel2.objects.len(),
        );
        assert_eq!(ocel.events.len(), ocel2.events.len());
        assert_eq!(ocel.objects.len(), ocel2.objects.len());
        assert_eq!(ocel.event_types.len(), ocel2.event_types.len());
        assert_eq!(ocel.object_types.len(), ocel2.object_types.len());
        assert_eq!(
            ocel.events.iter().find(|e| e.id == "event:741").unwrap(),
            ocel2.events.iter().find(|e| e.id == "event:741").unwrap()
        );
        assert_eq!(
            ocel.objects.iter().find(|e| e.id == "payment:629").unwrap(),
            ocel2
                .objects
                .iter()
                .find(|e| e.id == "payment:629")
                .unwrap()
        );
    }
}
