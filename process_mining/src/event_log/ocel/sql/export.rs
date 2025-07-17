use std::collections::HashMap;

use chrono::DateTime;

use crate::{ocel::ocel_struct::OCELTypeAttribute, OCEL};

use super::*;
///
/// Export an [`OCEL`] log to an SQL Database connection
///
/// Note: This function is only available if the `ocel-sqlite` or the `ocel-duckdb` feature is enabled.
///
pub fn export_ocel_to_sql_con<'a, DC: Into<DatabaseConnection<'a>>>(
    con: DC,
    ocel: &OCEL,
) -> Result<(), DatabaseError> {
    let con = con.into();
    // event map type
    con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "event_map_type" ("{OCEL_TYPE_COLUMN}" TEXT, "{OCEL_TYPE_MAP_COLUMN}"	TEXT, PRIMARY KEY("{OCEL_TYPE_COLUMN}"))"#))?;
    // object map type
    con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "object_map_type" ("{OCEL_TYPE_COLUMN}" TEXT, "{OCEL_TYPE_MAP_COLUMN}"	TEXT, PRIMARY KEY("{OCEL_TYPE_COLUMN}"))"#))?;

    // event
    con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "event" ("{OCEL_ID_COLUMN}"	TEXT, "{OCEL_TYPE_COLUMN}"	TEXT, PRIMARY KEY("{OCEL_ID_COLUMN}"), FOREIGN KEY("{OCEL_TYPE_COLUMN}") REFERENCES "event_map_type" ("{OCEL_TYPE_COLUMN}"))"#))?;
    // object
    con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "object" ("{OCEL_ID_COLUMN}"	TEXT, "{OCEL_TYPE_COLUMN}"	TEXT, PRIMARY KEY("{OCEL_ID_COLUMN}"), FOREIGN KEY("{OCEL_TYPE_COLUMN}") REFERENCES "object_map_type" ("{OCEL_TYPE_COLUMN}"))"#))?;

    // O2O (object_object)
    // , FOREIGN KEY("{OCEL_O2O_SOURCE_ID_COLUMN}") REFERENCES "object"("{OCEL_ID_COLUMN}"), FOREIGN KEY("{OCEL_O2O_TARGET_ID_COLUMN}") REFERENCES "object"("{OCEL_ID_COLUMN}")
    con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "object_object" ("{OCEL_O2O_SOURCE_ID_COLUMN}" TEXT, "{OCEL_O2O_TARGET_ID_COLUMN}" TEXT, "{OCEL_REL_QUALIFIER_COLUMN}" TEXT, PRIMARY KEY("{OCEL_O2O_SOURCE_ID_COLUMN}", "{OCEL_O2O_TARGET_ID_COLUMN}", "{OCEL_REL_QUALIFIER_COLUMN}"))"#))?;
    // E2O (event_object)
    con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "event_object" ("{OCEL_E2O_EVENT_ID_COLUMN}" TEXT, "{OCEL_E2O_OBJECT_ID_COLUMN}" TEXT, "{OCEL_REL_QUALIFIER_COLUMN}" TEXT, PRIMARY KEY("{OCEL_E2O_EVENT_ID_COLUMN}", "{OCEL_E2O_OBJECT_ID_COLUMN}", "{OCEL_REL_QUALIFIER_COLUMN}"), FOREIGN KEY("{OCEL_E2O_EVENT_ID_COLUMN}") REFERENCES "event"("{OCEL_ID_COLUMN}"), FOREIGN KEY("{OCEL_E2O_OBJECT_ID_COLUMN}") REFERENCES "object"("{OCEL_ID_COLUMN}"))"#))?;

    con.execute_no_params(&format!(
        r#"CREATE INDEX IF NOT EXISTS "event_id" ON "event" ("{OCEL_ID_COLUMN}" ASC)"#
    ))?;
    con.execute_no_params(&format!(
        r#"CREATE INDEX IF NOT EXISTS "object_id" ON "object" ("{OCEL_ID_COLUMN}" ASC)"#
    ))?;

    con.execute_no_params(&format!(r#"CREATE INDEX IF NOT EXISTS "event_object_source" ON "event_object" ("{OCEL_E2O_EVENT_ID_COLUMN}" ASC)"#))?;
    con.execute_no_params(&format!(r#"CREATE INDEX IF NOT EXISTS "event_object_target" ON "event_object" ("{OCEL_E2O_OBJECT_ID_COLUMN}" ASC)"#))?;
    con.execute_no_params(&format!(r#"CREATE INDEX IF NOT EXISTS "event_object_both" ON "event_object" ("{OCEL_E2O_EVENT_ID_COLUMN}","{OCEL_E2O_OBJECT_ID_COLUMN}" ASC)"#))?;

    con.execute_no_params(&format!(r#"CREATE INDEX IF NOT EXISTS "object_object_source" ON "object_object" ("{OCEL_O2O_SOURCE_ID_COLUMN}" ASC)"#))?;
    con.execute_no_params(&format!(r#"CREATE INDEX IF NOT EXISTS "object_object_target" ON "object_object" ("{OCEL_O2O_TARGET_ID_COLUMN}" ASC)"#))?;
    con.execute_no_params(&format!(r#"CREATE INDEX IF NOT EXISTS "object_object_both" ON "object_object" ("{OCEL_O2O_SOURCE_ID_COLUMN}","{OCEL_O2O_TARGET_ID_COLUMN}" ASC)"#))?;

    let mut et_attr_map: HashMap<&String, &Vec<OCELTypeAttribute>> = HashMap::new();
    // Tables for event types
    for et in &ocel.event_types {
        let mut attr_cols = et
            .attributes
            .iter()
            .map(|att| {
                format!(
                    r#""{}" {}"#,
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
        con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "event_{}" ("{OCEL_ID_COLUMN}"	TEXT, "{OCEL_TIME_COLUMN}"	TIMESTAMP,{attr_cols} PRIMARY KEY("{OCEL_ID_COLUMN}"))"#,clean_sql_name(&et.name)))?;

        con.execute(
            &format!(r#"INSERT INTO "event_map_type" VALUES (?, ?)"#,),
            [&et.name, &clean_sql_name(&et.name)],
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
                    r#""{}" {}"#,
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
        con.execute_no_params(&format!(r#"CREATE TABLE IF NOT EXISTS "object_{}" ("{OCEL_ID_COLUMN}"	TEXT, "{OCEL_TIME_COLUMN}" TIMESTAMP, {OCEL_CHANGED_FIELD} TEXT{attr_cols})"#,clean_sql_name(&ot.name)))?;

        con.execute(
            &format!(r#"INSERT INTO "object_map_type" VALUES (?, ?)"#,),
            [&ot.name, &clean_sql_name(&ot.name)],
        )?;
    }

    con.execute_no_params("BEGIN TRANSACTION")?;
    con.add_objects("object", ocel.objects.iter())?;
    // con.append_values(
    //     "object",
    //     ocel.objects.iter().map(|o| [&o.id, &o.object_type]),
    //     2,
    // )?;

    for ot in &ocel.object_types {
        let obs = ocel
            .objects
            .iter()
            .filter(|ob| ob.object_type == ot.name);

        con.add_object_changes_for_type(
            &clean_sql_name(&format!("object_{}", ot.name)),
            ot,
            obs,
        )?;
    }

    con.add_o2o_relationships("object_object",ocel.objects.iter())?;

    con.add_events("event",ocel.events.iter())?;
    
    for et in &ocel.event_types {
        let evs = ocel
            .events
            .iter()
            .filter(|ob| ob.event_type == et.name);

        con.add_event_attributes_for_type(
            &clean_sql_name(&format!("event_{}", et.name)),
            et,
            evs,
        )?;
    }
    con.add_e2o_relationships("event_object",ocel.events.iter())?;

    // for e in &ocel.events {
    //     con.execute(
    //         &format!(r#"INSERT INTO "event" VALUES (?, ?)"#,),
    //         [&e.id, &e.event_type],
    //     )?;
    //     // Table for event type with attribute values
    //     let mut attr_vals = et_attr_map
    //         .get(&e.event_type)
    //         .unwrap()
    //         .iter()
    //         .map(|a| {
    //             let value = e.attributes.iter().find(|oa| oa.name == a.name);
    //             if let Some(val) = value {
    //                 format!("'{}'", val.value)
    //             } else {
    //                 "NULL".to_string()
    //             }
    //         })
    //         .collect::<Vec<_>>()
    //         .join(", ");
    //     if !attr_vals.is_empty() {
    //         attr_vals.insert_str(0, ", ");
    //     }
    //     con.execute(
    //         &format!(
    //             r#"INSERT INTO "event_{}" VALUES (?, ? {})"#,
    //             clean_sql_name(&e.event_type),
    //             attr_vals
    //         ),
    //         [&e.id, &e.time.to_rfc3339()],
    //     )?;
    //     // E2O Relationships
    //     for rel in &e.relationships {
    //         con.execute(
    //             &format!(r#"INSERT INTO "event_object" VALUES (?, ?, ?)"#,),
    //             [&e.id, &rel.object_id, &rel.qualifier],
    //         )?;
    //     }
    // }

    for ot in &ocel.object_types {
        con.execute_no_params(&format!(
            r#"CREATE INDEX IF NOT EXISTS "{}_obid" ON "object_{}" ("{OCEL_ID_COLUMN}" ASC)"#,
            clean_sql_name(&ot.name),
            clean_sql_name(&ot.name)
        ))?;
    }
    for et in &ocel.event_types {
        con.execute_no_params(&format!(
            r#"CREATE INDEX IF NOT EXISTS "{}_evid" ON "event_{}" ("{OCEL_ID_COLUMN}" ASC)"#,
            clean_sql_name(&et.name),
            clean_sql_name(&et.name)
        ))?;
    }
    con.execute_no_params("COMMIT")?;

    Ok(())
}

fn clean_sql_name(type_name: &str) -> String {
    type_name
        .chars()
        .map(|c| {
            if c != '\'' && c != '\\' && c != ' ' {
                c
            } else {
                '_'
            }
        })
        // .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

#[cfg(test)]
mod test {
    use std::fs::remove_file;

    use crate::{import_ocel_json_from_path, utils::test_utils};

    use super::export_ocel_to_sql_con;

    #[test]
    #[cfg(feature = "ocel-duckdb")]
    fn test_duckdb_ocel_export_order() {
        let path = test_utils::get_test_data_path();
        let ocel =
            import_ocel_json_from_path(path.join("ocel").join("order-management.json")).unwrap();
        let export_path = path.join("export").join("duckdb-export.db");
        let _ = remove_file(&export_path);
        let conn = ::duckdb::Connection::open(&export_path).unwrap();
        export_ocel_to_sql_con(&conn, &ocel).unwrap();
    }

    #[test]
    #[cfg(feature = "ocel-sqlite")]
    fn test_sqlite_ocel_export_order() {
        let path: std::path::PathBuf = test_utils::get_test_data_path();
        let ocel =
            import_ocel_json_from_path(path.join("ocel").join("order-management.json")).unwrap();
        let export_path = path.join("export").join("sqlite-export.sqlite");
        let _ = remove_file(&export_path);
        let conn = rusqlite::Connection::open(&export_path).unwrap();
        export_ocel_to_sql_con(&conn, &ocel).unwrap();
    }
}
