use std::collections::HashMap;

use rusqlite::{ffi::Error, Connection, Params, Statement};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_rusqlite::{from_rows, DeserRows};

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
  value_type: String 
}

#[test]
fn test_sqlite_ocel() -> Result<(), rusqlite::Error> {
    let conn = Connection::open("/home/aarkue/dow/order-management.sqlite").unwrap();

    // Parse names of object types (and the table name postfixes)
    let mut s = conn.prepare("SELECT * FROM event_map_type")?;
    let ev_map_type = query_all::<MapTypeOCELRow,_>(&mut s,[])?;
    let ev_type_map: HashMap<_, _> = ev_map_type
        .into_iter()
        .flatten()
        .map(|x| (x.ocel_type_map, x.ocel_type))
        .collect();
    // Parse names of event types (and the table name postfixes)
    let mut s = conn.prepare("SELECT * FROM object_map_type")?;
    let ob_map_type = query_all::<MapTypeOCELRow,_>(&mut s,[])?;
    let ob_type_map: HashMap<_, _> = ob_map_type
        .into_iter()
        .flatten()
        .map(|x| (x.ocel_type_map, x.ocel_type))
        .collect();

    println!("{:?}", ev_type_map);
    println!("{:?}", ob_type_map);


    for ob_type in ob_type_map.keys() {
      println!("{ob_type}");
      let mut s = conn.prepare(format!("PRAGMA table_info(object_{ob_type})").as_str())?;
      let ob_attr = query_all::<PragmaRow,_>(&mut s,[])?;
      for o_atr in ob_attr.into_iter() {
        println!("{o_atr:?}")
      }
      
    }

    // TODO: Continue SQLite importer
    // Maybe also ditch https://github.com/twistedfall/serde_rusqlite in favor of simply using position-based approach?


    Ok(())
}

fn query_all<'a, T: DeserializeOwned, P: Params>(
    s: &'a mut Statement<'_>,
    p: P
) -> Result<DeserRows<'a, T>, rusqlite::Error> {
    let q = s.query(p)?;
    Ok(from_rows::<T>(q))
}
