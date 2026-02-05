//! CSV-based OCEL 2.0 Format Import/Export
//!
//! # CSV Format Description
//!
//! The CSV format for OCEL 2.0 has the following structure:
//!
//! ```text
//! id,activity,timestamp,ot:order,ot:item,ea:billable,ea:area
//! e1,place order,2026-01-22T09:57:28+0000,o1,i1#part-of{"price": "5€"}/i2#part-of{"price": "15€"},no,
//! e2,pick item,2026-01-23T09:57:28+0000,,i1,no,outdoor
//! e3,produce item,2026-01-24T09:57:28+0000,,i2#target,no,indoor
//! ,,2026-01-25T09:57:28+0000,,i1{"price": "50€"},,
//! e4,send order,2026-01-26T09:57:28+0000,o1,i1/i2,yes,
//! o1,o2o,,,i1#has/i2#has,,
//! i2,o2o,,,i1#add-on,,
//! ```
//!
//! ## Column Types
//!
//! - **`id`**: Event ID or source object ID for O2O relationships
//! - **`activity`**: Event type name, or "o2o" (case insensitive) for object relationships
//! - **`timestamp`**: ISO 8601 formatted timestamp (empty for O2O relationships)
//! - **Columns prefixed with `ot:`** (case-insensitive): Object type columns defining object involvements
//! - **Columns prefixed with `ea:`** (case-insensitive): Event attribute columns
//!
//! ## Object References
//!
//! Object references in `ot:` columns support:
//! - Simple object ID: `o1`
//! - With qualifier: `i1#part-of`
//! - With JSON attributes: `i1{"price": "5€"}`
//! - With both: `i1#part-of{"price": "5€"}`
//! - Multiple objects separated by `/`: `i1/i2`
//!
//! Whitespace is trimmed from the start and end of object IDs and qualifiers,
//! but internal whitespace is preserved (e.g., `"my order"` is a valid ID).
//!
//! ## Special Row Types
//!
//! - **O2O relationships**: Activity is "o2o" (case insensitive), ID column contains source object,
//!   timestamp is empty (or ignored if present)
//! - **Object attribute changes**: Empty ID and activity columns indicate attribute-only updates
//!
//! # Export Behavior
//!
//! ## Object Attribute Handling
//!
//! - **At event time**: When an object has attribute values recorded at the exact same timestamp
//!   as an event involving that object, the attributes are embedded in the object reference
//!   (e.g., `i1#qualifier{"attr": "value"}`).
//!
//! - **Standalone changes**: When object attributes have timestamps that don't coincide with any
//!   event involving that object, a separate row is created with empty `id` and `activity` columns.
//!
//! - **Same-time combination**: All attribute changes for a single object at the same timestamp
//!   are combined into one JSON object. Different objects create separate rows.
//!
//! - **Unchanged values**: The format records all attribute values at their timestamps without
//!   detecting whether values actually changed. If the source OCEL has multiple entries with
//!   identical values at different times, all are preserved.
//!
//! ## O2O Relationship Handling
//!
//! - O2O relationships don't have timestamps in OCEL 2.0. During export, the timestamp column
//!   is left empty for O2O rows, and O2O rows are placed at the end of the file.
//!
//! - During import, the timestamp from an O2O row is ignored for the relationship itself.
//!   However, if an O2O row contains object attributes with JSON and has a timestamp,
//!   those attributes are recorded at that timestamp.
//!
//! ## Roundtrip Considerations
//!
//! Import → Export → Import may not be perfectly lossless:
//! - Event attribute types are inferred as strings during CSV import
//! - Column ordering may change (sorted alphabetically)
//! - Object types are inferred from which `ot:` column an object first appears in
//!
//! # Import Options
//!
//! The importer supports a `strict` mode via [`OCELCSVImportOptions`]:
//! - **`strict: false`** (default): Rows with missing timestamps are skipped with a warning
//! - **`strict: true`**: Missing timestamps cause an error

mod csv_ocel_export;
mod csv_ocel_import;

#[doc(inline)]
pub use csv_ocel_export::*;
#[doc(inline)]
pub use csv_ocel_import::*;
