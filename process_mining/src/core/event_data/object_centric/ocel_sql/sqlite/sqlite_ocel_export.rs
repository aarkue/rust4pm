use crate::core::event_data::object_centric::ocel_struct::OCEL;

use super::super::export::export_ocel_to_sql_con;
use super::super::*;
use rusqlite::Connection;

///
/// Export an [`OCEL`] to an `SQLite` file at the specified path
///
/// Note: This function is only available if the `ocel-sqlite` feature is enabled.
///
pub fn export_ocel_sqlite_to_path<P: AsRef<std::path::Path>>(
    ocel: &OCEL,
    path: P,
) -> Result<(), DatabaseError> {
    if path.as_ref().exists() {
        let _ = std::fs::remove_file(&path);
    }
    let con = Connection::open(path)?;
    export_ocel_to_sql_con(&con, ocel)
}

///
/// Export an [`OCEL`] to an `SQLite` to a byte array
///
/// Note: This function is only available if the `ocel-sqlite` feature is enabled.
pub fn export_ocel_sqlite_to_vec(ocel: &OCEL) -> Result<Vec<u8>, DatabaseError> {
    let con = Connection::open_in_memory()?;
    export_ocel_to_sql_con(&con, ocel)?;
    let data = con.serialize(rusqlite::DatabaseName::Main)?;
    Ok((*data).to_vec())
}

#[cfg(test)]
mod sqlite_export_tests {
    use std::fs::{self};

    use rusqlite::Connection;

    use crate::{
        core::event_data::object_centric::ocel_sql::import_ocel_sqlite_from_con,
        test_utils::get_test_data_path,
    };

    use super::export_ocel_to_sql_con;

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

        export_ocel_to_sql_con(&con, &ocel).unwrap();

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
        let path = get_test_data_path().join("ocel").join("ocel2-p2p.sqlite");

        let in_con = Connection::open(path).unwrap();
        let ocel = import_ocel_sqlite_from_con(in_con).unwrap();

        let export_path = get_test_data_path()
            .join("export")
            .join("ocel2-p2p-EXPORT.sqlite");
        if let Err(_e) = fs::remove_file(&export_path) {}
        let con = Connection::open(&export_path).unwrap();

        export_ocel_to_sql_con(&con, &ocel).unwrap();

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
