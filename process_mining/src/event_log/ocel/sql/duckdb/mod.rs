pub(crate) mod duckdb_ocel_export;
pub(crate) mod duckdb_ocel_import;

#[cfg(test)]
mod duckdb_tests {
    use std::collections::HashSet;

    use chrono::DateTime;

    use crate::{
        import_ocel_xml_file,
        ocel::{
            ocel_struct::{OCELAttributeValue, OCELRelationship},
            sql::duckdb::{
                duckdb_ocel_export::export_ocel_duckdb_to_path,
                duckdb_ocel_import::import_ocel_duckdb_from_path,
            },
        },
        utils::test_utils::get_test_data_path,
    };

    #[test]
    fn test_duckdb_round_trip_ocel() -> Result<(), ::duckdb::Error> {
        let path = get_test_data_path()
            .join("ocel")
            .join("order-management.xml");
        let ocel = import_ocel_xml_file(path);
        let export_path = get_test_data_path()
            .join("export")
            .join("order-management.duckdb");
        let _ = std::fs::remove_file(&export_path);
        export_ocel_duckdb_to_path(&ocel, &export_path).unwrap();
        let ocel2 = import_ocel_duckdb_from_path(&export_path).unwrap();

        assert_eq!(ocel.events.len(), ocel2.events.len());
        assert_eq!(ocel.objects.len(), ocel2.objects.len());
        assert_eq!(ocel.event_types.len(), ocel2.event_types.len());
        assert_eq!(ocel.object_types.len(), ocel2.object_types.len());
        drop(ocel);

        let po_1337 = ocel2
            .events
            .iter()
            .find(|e| e.id == "pay_o-991337")
            .unwrap();
        assert_eq!(
            po_1337.time,
            DateTime::parse_from_rfc3339("2023-12-13T09:31:50+00:00").unwrap()
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

        let o_1337 = ocel2.objects.iter().find(|o| o.id == "o-991337").unwrap();
        assert_eq!(o_1337.attributes.len(), 1);
        assert_eq!(o_1337.attributes.first().unwrap().name, "price");
        if let OCELAttributeValue::Float(f) = o_1337.attributes.first().unwrap().value {
            let diff = (f - 1909.04).abs();
            assert!(diff < 0.001);
        } else {
            panic!("Larger float difference")
        }

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

    #[test]
    fn test_duckdb_containers_round_trip_ocel() -> Result<(), ::duckdb::Error> {
        let path = get_test_data_path()
            .join("ocel")
            .join("ContainerLogistics.xml");
        let ocel = import_ocel_xml_file(path);
        let export_path = get_test_data_path()
            .join("export")
            .join("ContainerLogistics.duckdb");
        let _ = std::fs::remove_file(&export_path);
        export_ocel_duckdb_to_path(&ocel, &export_path).unwrap();
        let ocel2 = import_ocel_duckdb_from_path(&export_path).unwrap();

        assert_eq!(ocel.events.len(), ocel2.events.len());
        assert_eq!(ocel.objects.len(), ocel2.objects.len());
        assert_eq!(ocel.event_types.len(), ocel2.event_types.len());
        assert_eq!(ocel.object_types.len(), ocel2.object_types.len());
        drop(ocel);

        Ok(())
    }
}
