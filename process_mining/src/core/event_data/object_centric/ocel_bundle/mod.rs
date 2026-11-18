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
mod export;
#[cfg(feature = "ocel-bundle")]
pub mod import;
pub mod meta;

#[cfg(feature = "extraction-blueprint")]
pub use blueprint::{blueprint_for, SOURCE_ID};
#[cfg(feature = "ocel-bundle")]
pub(crate) use export::{
    export_ocel_bundle, write_ocel_bundle_archive, BundleExportOptions, ContainerLayout,
};
#[cfg(feature = "ocel-bundle")]
pub use import::Container;
pub use meta::{
    columns, encode_type_name, epoch, event_table, object_changes_table, object_table,
    type_attributes, AttributeDecl, AttributeType, BundleMeta, EventTypeDecl, ObjectTypeDecl,
    RelationFiles, StorageFormat, Value, BUNDLE_FORMAT_VERSION, E2O_TABLE, META_FILE_NAME,
    O2O_TABLE, OCEL_VERSION,
};

#[cfg(all(test, feature = "extraction-blueprint"))]
mod tests;

#[cfg(feature = "ocel-bundle")]
use crate::core::event_data::object_centric::io::OCELIOError;

/// Why reading or writing a container failed. I/O failures are [`OCELIOError::Io`] instead.
#[cfg(feature = "ocel-bundle")]
#[derive(Debug)]
pub enum BundleError {
    /// The container has no [`META_FILE_NAME`].
    MissingManifest,
    /// [`META_FILE_NAME`] is not a manifest this build understands.
    Manifest(serde_json::Error),
    /// The manifest declares a major version this build does not implement.
    Version {
        /// Which manifest field.
        field: String,
        /// What the container declares.
        found: String,
        /// What this build implements.
        implemented: String,
    },
    /// An archive entry expands past what a container may hold.
    EntryTooLarge {
        /// The entry's name.
        entry: String,
    },
    /// A table is missing, lacks a column the format fixes, or holds a value the format forbids.
    Table {
        /// The path inside the container.
        file: String,
        /// What is wrong.
        detail: String,
    },
    /// A declared attribute name collides with a column the format fixes.
    ReservedAttribute {
        /// The type declaring it.
        type_name: String,
        /// The attribute name, which is also a `ocel_`-prefixed fixed column name.
        attribute: String,
    },
    /// Parquet storage in a build without the `ocel-bundle-parquet` feature.
    ParquetUnavailable,
    /// Reading or writing the ZIP archive failed.
    Archive(zip::result::ZipError),
    /// Reading or writing a CSV table failed.
    Csv {
        /// The path inside the container.
        file: String,
        /// The CSV error.
        source: csv::Error,
    },
    /// Reading or writing a Parquet table failed.
    #[cfg(feature = "ocel-bundle-parquet")]
    Parquet {
        /// The path inside the container.
        file: String,
        /// The Parquet error.
        source: parquet::errors::ParquetError,
    },
}

#[cfg(feature = "ocel-bundle")]
impl std::fmt::Display for BundleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BundleError::MissingManifest => write!(f, "the container has no {META_FILE_NAME}"),
            BundleError::Manifest(e) => write!(f, "invalid {META_FILE_NAME}: {e}"),
            BundleError::Version {
                field,
                found,
                implemented,
            } => write!(
                f,
                "the container declares {field} '{found}', which this build does not implement: it reads '{implemented}'"
            ),
            BundleError::EntryTooLarge { entry } => write!(
                f,
                "'{entry}' expands past what a container may hold ({} bytes per entry, {} in total)",
                import::MAX_ENTRY_BYTES,
                import::MAX_TOTAL_BYTES
            ),
            BundleError::Table { file, detail } => write!(f, "'{file}': {detail}"),
            BundleError::ReservedAttribute {
                type_name,
                attribute,
            } => write!(
                f,
                "'{type_name}' declares an attribute named '{attribute}', which is a column name the format reserves"
            ),
            BundleError::ParquetUnavailable => write!(
                f,
                "Parquet storage needs the 'ocel-bundle-parquet' feature, which this build lacks"
            ),
            BundleError::Archive(e) => write!(f, "ZIP archive: {e}"),
            BundleError::Csv { file, source } => write!(f, "'{file}': {source}"),
            #[cfg(feature = "ocel-bundle-parquet")]
            BundleError::Parquet { file, source } => write!(f, "'{file}': {source}"),
        }
    }
}

#[cfg(feature = "ocel-bundle")]
impl std::error::Error for BundleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BundleError::Manifest(e) => Some(e),
            BundleError::Archive(e) => Some(e),
            BundleError::Csv { source, .. } => Some(source),
            #[cfg(feature = "ocel-bundle-parquet")]
            BundleError::Parquet { source, .. } => Some(source),
            BundleError::MissingManifest
            | BundleError::Version { .. }
            | BundleError::EntryTooLarge { .. }
            | BundleError::Table { .. }
            | BundleError::ReservedAttribute { .. }
            | BundleError::ParquetUnavailable => None,
        }
    }
}

#[cfg(feature = "ocel-bundle")]
fn archive_err(e: zip::result::ZipError) -> OCELIOError {
    match e {
        zip::result::ZipError::Io(e) => OCELIOError::Io(e),
        e => BundleError::Archive(e).into(),
    }
}
