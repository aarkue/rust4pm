use std::time::Instant;

use crate::{
    import_ocel_xml_file,
    ocel::{dataframe::ocel_to_dataframes, linked_ocel::IndexLinkedOCEL},
    utils::test_utils::get_test_data_path,
};

use super::object_attribute_changes_to_df;

#[test]
fn test_ocel2_container_df() {
    let now = Instant::now();
    let path = get_test_data_path()
        .join("ocel")
        .join("ContainerLogistics.xml");
    let ocel = import_ocel_xml_file(path);
    let ocel_dfs = ocel_to_dataframes(&ocel);
    println!(
            "Got OCEL DF with {:?} objects in {:?}; Object change shape: {:?}; O2O shape: {:?}; E2O shape: {:?}",
            ocel_dfs.objects.shape(),
            now.elapsed(),
            ocel_dfs.object_changes.shape(),
            ocel_dfs.o2o.shape(),
            ocel_dfs.e2o.shape()
        );
    assert_eq!(ocel.objects.len(), 13910);
    assert_eq!(ocel.events.len(), 35413);
}

#[test]
fn test_ocel2_df() {
    let now = Instant::now();
    let path = get_test_data_path()
        .join("ocel")
        .join("order-management.xml");
    let ocel = import_ocel_xml_file(path);
    let ocel_dfs = ocel_to_dataframes(&ocel);
    println!(
            "Got OCEL DF with {:?} objects in {:?}; Object change shape: {:?}; O2O shape: {:?}; E2O shape: {:?}",
            ocel_dfs.objects.shape(),
            now.elapsed(),
            ocel_dfs.object_changes.shape(),
            ocel_dfs.o2o.shape(),
            ocel_dfs.e2o.shape()
        );

    // Assert DF shapes based on OCEL information
    assert_eq!(ocel.objects.len(), 10840);
    assert_eq!(ocel.objects.len(), ocel_dfs.objects.shape().0);

    assert_eq!(ocel.events.len(), 21008);
    assert_eq!(ocel.events.len(), ocel_dfs.events.shape().0);

    assert_eq!(
        ocel.events
            .iter()
            .flat_map(|ev| ev.relationships.clone())
            .count(),
        ocel_dfs.e2o.shape().0
    );
    assert_eq!(ocel.events.len(), ocel_dfs.events.shape().0);

    // Known DF-shapes (match PM4PY implementation)
    assert_eq!(ocel_dfs.objects.shape(), (10840, 2));
    assert_eq!(ocel_dfs.events.shape(), (21008, 3));
    assert_eq!(ocel_dfs.e2o.shape(), (147463, 6));
    assert_eq!(ocel_dfs.o2o.shape(), (28391, 3));
    assert_eq!(ocel_dfs.object_changes.shape(), (18604, 7));
}

#[test]
fn ocel_object_attribute_changes() {
    let ocel_path = get_test_data_path()
        .join("ocel")
        .join("order-management.xml");
    let ocel = import_ocel_xml_file(ocel_path);
    let locel: IndexLinkedOCEL = ocel.into();
    let df = object_attribute_changes_to_df(&locel, "products").unwrap();
    println!("{df:#?}");
    assert_eq!(
        df.get_column_names(),
        vec!["object_id", "from_time", "to_time", "weight", "price"]
    );
}
