use super::*;
use crate::core::event_data::object_centric::extraction::{validate, ExtractionCatalog};

/// The manifest for the spec's running example, cut down to what these tests need: an object
/// type with two attributes of which only one ever changes, which is the case a naive mapping
/// gets wrong.
fn meta() -> BundleMeta {
    serde_json::from_str(
        r#"{
          "ocelVersion": "2.0",
          "bundleFormatVersion": "1.0",
          "storageFormat": "csv",
          "eventTypes": {
            "Create Purchase Order": { "file": "events/event_Create%20Purchase%20Order.csv",
              "attributes": [{ "name": "po_creator", "type": "string" }] },
            "Change PO Quantity": { "file": "events/event_Change%20PO%20Quantity.csv",
              "attributes": [{ "name": "po_editor", "type": "string" }] }
          },
          "objectTypes": {
            "Purchase Order": { "file": "objects/object_Purchase%20Order.csv",
              "changesFile": "object_changes/object_changes_Purchase%20Order.csv",
              "attributes": [{ "name": "po_product", "type": "string" },
                             { "name": "po_quantity", "type": "integer" }] }
          },
          "relations": { "e2o": "relations/e2o.csv", "o2o": "relations/o2o.csv" }
        }"#,
    )
    .expect("parse manifest")
}

#[test]
fn every_declared_table_gets_a_source_node_reading_it() {
    let meta = meta();
    let bp = blueprint_for(&meta);
    let sources: Vec<&str> = bp
        .nodes
        .iter()
        .filter_map(|n| match &n.op {
            crate::core::event_data::object_centric::extraction::NodeOp::Source {
                table, ..
            } => Some(table.as_str()),
            _ => None,
        })
        .collect();
    let declared: Vec<String> = meta.tables().into_iter().map(|(t, _)| t).collect();
    for t in &declared {
        assert!(sources.contains(&t.as_str()), "{t} has no Source node");
    }
    assert_eq!(
        sources.len(),
        declared.len(),
        "no source node without a table"
    );
}

/// One filtered node and one mapping per changeable attribute. See `blueprint.rs` for why a
/// single mapping over the whole change table is wrong.
#[test]
fn a_change_table_is_read_once_per_attribute_behind_its_own_filter() {
    let bp = blueprint_for(&meta());
    let filters: Vec<&str> = bp
        .nodes
        .iter()
        .filter(|n| {
            matches!(
                n.op,
                crate::core::event_data::object_centric::extraction::NodeOp::Filter { .. }
            )
        })
        .map(|n| n.id.as_str())
        .collect();
    assert_eq!(
        filters,
        [
            "object_changes:Purchase Order#po_product",
            "object_changes:Purchase Order#po_quantity"
        ]
    );
}

#[test]
fn the_generated_blueprint_validates_against_the_schema_it_describes() {
    let meta = meta();
    let bp = blueprint_for(&meta);
    let errors = validate(&bp, &catalog(&meta));
    assert!(errors.is_empty(), "{errors:?}");
}

/// A catalog describing exactly the columns the format fixes, so validation sees the same shape
/// a real container would present.
fn catalog(meta: &BundleMeta) -> ExtractionCatalog {
    use crate::core::event_data::object_centric::extraction::TableSchema;
    use std::collections::BTreeMap;

    let mut tables: BTreeMap<String, TableSchema> = BTreeMap::new();
    let mut add = |name: String, cols: Vec<&str>| {
        let schema = TableSchema::new(&name, cols.into_iter().map(|c| (c, "TEXT", true)));
        tables.insert(name, schema);
    };

    for (ty, d) in &meta.event_types {
        let mut cols = vec![columns::ID, columns::TIME];
        cols.extend(d.attributes.iter().map(|a| a.name.as_str()));
        add(event_table(ty), cols);
    }
    for (ty, d) in &meta.object_types {
        let mut cols = vec![columns::ID];
        cols.extend(d.attributes.iter().map(|a| a.name.as_str()));
        add(object_table(ty), cols);
        if d.changes_file.is_some() {
            let mut cols = vec![columns::ID, columns::TIME, columns::CHANGED_FIELD];
            cols.extend(d.attributes.iter().map(|a| a.name.as_str()));
            add(object_changes_table(ty), cols);
        }
    }
    add(
        E2O_TABLE.to_string(),
        vec![columns::EVENT_ID, columns::OBJECT_ID, columns::QUALIFIER],
    );
    add(
        O2O_TABLE.to_string(),
        vec![columns::SOURCE_ID, columns::TARGET_ID, columns::QUALIFIER],
    );

    ExtractionCatalog {
        tables: BTreeMap::from([(SOURCE_ID.to_string(), tables)]),
        ..Default::default()
    }
}
