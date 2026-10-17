//! The OCEL 2.0 bundled CSV/Parquet format: a `.ocel.zip` archive, or a directory with the same
//! layout, holding one table per event type, object type, object-change set, and relation kind.
//!
//! Split by what each part needs:
//!
//! - `meta` holds the `ocel-meta.json` manifest and the naming rules. Pure, always available.
//! - `export`/`import` write and read a container (`ocel-bundle`, plus `ocel-bundle-parquet` for
//!   Parquet storage).
//! - `blueprint` builds the extraction blueprint that turns a container's tables back into an OCEL
//!   (`extraction-blueprint`). Running it needs a row provider over the container's files, which
//!   `extraction-dbcon` supplies.

#[cfg(feature = "extraction-blueprint")]
pub mod blueprint;
#[cfg(feature = "ocel-bundle")]
pub mod export;
#[cfg(feature = "ocel-bundle")]
pub mod import;
pub mod meta;

#[cfg(feature = "extraction-blueprint")]
pub use blueprint::{blueprint_for, SOURCE_ID};
#[cfg(feature = "ocel-bundle")]
pub use export::{
    export_ocel_bundle, write_ocel_bundle_archive, BundleExportError, BundleExportOptions,
    ContainerLayout,
};
#[cfg(feature = "ocel-bundle")]
pub use import::{import_ocel_bundle, import_ocel_bundle_from_bytes, BundleImportError, Container};
pub use meta::{
    columns, encode_type_name, epoch, event_table, object_changes_table, object_table,
    type_attributes, AttributeDecl, AttributeType, BundleMeta, EventTypeDecl, ObjectTypeDecl,
    RelationFiles, StorageFormat, Value, BUNDLE_FORMAT_VERSION, E2O_TABLE, META_FILE_NAME,
    O2O_TABLE, OCEL_VERSION,
};

#[cfg(all(test, feature = "extraction-blueprint"))]
mod tests;
