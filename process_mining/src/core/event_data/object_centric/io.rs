//! IO implementations for OCEL

use std::io::{Read, Write};
use std::path::Path;

use crate::core::event_data::object_centric::ocel_csv::OCELCSVImportError;
#[cfg(feature = "ocel-sqlite")]
use crate::core::event_data::object_centric::ocel_sql::export_ocel_sqlite_to_vec;
#[cfg(any(feature = "ocel-duckdb", feature = "ocel-sqlite"))]
use crate::core::event_data::object_centric::ocel_sql::DatabaseError;
use crate::core::event_data::object_centric::ocel_xml::xml_ocel_import::OCELImportOptions;
use crate::core::event_data::object_centric::OCEL;
use crate::core::io::{infer_format_from_path, Exportable, ExtensionWithMime, Importable};

/// Error type for OCEL IO operations
#[derive(Debug)]
pub enum OCELIOError {
    /// IO Error
    Io(std::io::Error),
    /// JSON Parsing Error
    Json(serde_json::Error),
    /// XML Parsing Error
    Xml(quick_xml::Error),
    /// CSV Parsing Error
    Csv(OCELCSVImportError),
    /// `SQLite` Error
    #[cfg(feature = "ocel-sqlite")]
    Sqlite(rusqlite::Error),
    /// `DuckDB` Error
    #[cfg(feature = "ocel-duckdb")]
    DuckDB(duckdb::Error),
    /// Reading a bundled CSV/Parquet container failed
    #[cfg(feature = "ocel-bundle")]
    BundleImport(crate::core::event_data::object_centric::ocel_bundle::BundleImportError),
    /// Writing a bundled CSV/Parquet container failed
    #[cfg(feature = "ocel-bundle")]
    BundleExport(crate::core::event_data::object_centric::ocel_bundle::BundleExportError),
    /// Unsupported Format
    UnsupportedFormat(String),
    /// Other Error
    Other(String),
}

impl std::fmt::Display for OCELIOError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OCELIOError::Io(e) => write!(f, "IO Error: {}", e),
            OCELIOError::Json(e) => write!(f, "JSON Error: {}", e),
            OCELIOError::Xml(e) => write!(f, "XML Error: {}", e),
            OCELIOError::Csv(e) => write!(f, "CSV Error: {}", e),
            #[cfg(feature = "ocel-sqlite")]
            OCELIOError::Sqlite(e) => write!(f, "SQLite Error: {}", e),
            #[cfg(feature = "ocel-duckdb")]
            OCELIOError::DuckDB(e) => write!(f, "DuckDB Error: {}", e),
            #[cfg(feature = "ocel-bundle")]
            OCELIOError::BundleImport(e) => write!(f, "Bundle Import Error: {}", e),
            #[cfg(feature = "ocel-bundle")]
            OCELIOError::BundleExport(e) => write!(f, "Bundle Export Error: {}", e),
            OCELIOError::UnsupportedFormat(s) => write!(f, "Unsupported Format: {}", s),
            OCELIOError::Other(s) => write!(f, "Error: {}", s),
        }
    }
}

impl std::error::Error for OCELIOError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            OCELIOError::Io(e) => Some(e),
            OCELIOError::Json(e) => Some(e),
            OCELIOError::Xml(e) => Some(e),
            OCELIOError::Csv(e) => Some(e),
            #[cfg(feature = "ocel-sqlite")]
            OCELIOError::Sqlite(e) => Some(e),
            #[cfg(feature = "ocel-duckdb")]
            OCELIOError::DuckDB(e) => Some(e),
            #[cfg(feature = "ocel-bundle")]
            OCELIOError::BundleImport(e) => Some(e),
            #[cfg(feature = "ocel-bundle")]
            OCELIOError::BundleExport(e) => Some(e),
            OCELIOError::UnsupportedFormat(_) => None,
            OCELIOError::Other(_) => None,
        }
    }
}

impl From<std::io::Error> for OCELIOError {
    fn from(e: std::io::Error) -> Self {
        OCELIOError::Io(e)
    }
}

impl From<serde_json::Error> for OCELIOError {
    fn from(e: serde_json::Error) -> Self {
        OCELIOError::Json(e)
    }
}

impl From<quick_xml::Error> for OCELIOError {
    fn from(e: quick_xml::Error) -> Self {
        OCELIOError::Xml(e)
    }
}

impl From<OCELCSVImportError> for OCELIOError {
    fn from(e: OCELCSVImportError) -> Self {
        OCELIOError::Csv(e)
    }
}

#[cfg(feature = "ocel-sqlite")]
impl From<rusqlite::Error> for OCELIOError {
    fn from(e: rusqlite::Error) -> Self {
        OCELIOError::Sqlite(e)
    }
}

#[cfg(feature = "ocel-duckdb")]
impl From<duckdb::Error> for OCELIOError {
    fn from(e: duckdb::Error) -> Self {
        OCELIOError::DuckDB(e)
    }
}

#[cfg(feature = "ocel-bundle")]
impl From<crate::core::event_data::object_centric::ocel_bundle::BundleImportError> for OCELIOError {
    fn from(e: crate::core::event_data::object_centric::ocel_bundle::BundleImportError) -> Self {
        OCELIOError::BundleImport(e)
    }
}

#[cfg(feature = "ocel-bundle")]
impl From<crate::core::event_data::object_centric::ocel_bundle::BundleExportError> for OCELIOError {
    fn from(e: crate::core::event_data::object_centric::ocel_bundle::BundleExportError) -> Self {
        OCELIOError::BundleExport(e)
    }
}

#[cfg(any(feature = "ocel-duckdb", feature = "ocel-sqlite"))]
impl From<DatabaseError> for OCELIOError {
    fn from(e: DatabaseError) -> Self {
        match e {
            #[cfg(feature = "ocel-sqlite")]
            DatabaseError::SQLITE(e) => OCELIOError::Sqlite(e),
            #[cfg(feature = "ocel-duckdb")]
            DatabaseError::DUCKDB(e) => OCELIOError::DuckDB(e),
        }
    }
}

impl Importable for OCEL {
    type Error = OCELIOError;
    type ImportOptions = ();

    fn infer_format(path: &Path) -> Option<String> {
        let p = path.to_string_lossy().to_lowercase();
        // Checked before `.json`, which the manifest name would otherwise match: pointing at a
        // container's manifest means "read the directory it is in", not "parse this file".
        #[cfg(feature = "ocel-bundle")]
        if path.file_name().is_some_and(|n| {
            n.eq_ignore_ascii_case(
                crate::core::event_data::object_centric::ocel_bundle::META_FILE_NAME,
            )
        }) {
            return Some("ocel.zip".to_string());
        }
        if p.ends_with(".csv.gz") {
            Some("ocel.csv.gz".to_string())
        } else if p.ends_with(".csv") {
            Some("ocel.csv".to_string())
        } else if p.ends_with(".json") || p.ends_with(".jsonocel") {
            Some("json".to_string())
        } else if p.ends_with(".xml") || p.ends_with(".xmlocel") {
            Some("xml".to_string())
        } else if p.ends_with(".sqlite") || p.ends_with(".sqlite3") || p.ends_with(".db") {
            Some("sqlite".to_string())
        } else if p.ends_with(".duckdb") {
            Some("duckdb".to_string())
        } else if p.ends_with(".zip") || path.is_dir() {
            // A directory is the bundled format's uncompressed form, with no extension to match.
            Some("ocel.zip".to_string())
        } else {
            infer_format_from_path(path)
        }
    }

    fn import_from_reader_with_options<R: Read>(
        // A SQLite file and a ZIP are both read from their end, so they consume the whole reader.
        #[cfg(any(feature = "ocel-sqlite", feature = "ocel-bundle"))] mut reader: R,
        #[cfg(not(any(feature = "ocel-sqlite", feature = "ocel-bundle")))] reader: R,
        format: &str,
        options: Self::ImportOptions,
    ) -> Result<Self, Self::Error> {
        if let Some(inner) = format.strip_suffix(".gz") {
            // Buffer the compressed bytes; `GzDecoder` reads from its inner in chunks.
            let gz: Box<dyn Read> = Box::new(flate2::read::GzDecoder::new(
                std::io::BufReader::new(reader),
            ));
            return Self::import_from_reader_with_options(gz, inner, options);
        }
        if format.ends_with("json") || format.ends_with("jsonocel") {
            let reader = std::io::BufReader::new(reader);
            let ocel: OCEL = serde_json::from_reader(reader)?;
            Ok(ocel)
        } else if format.ends_with("xml") || format.ends_with("xmlocel") {
            let reader = std::io::BufReader::new(reader);
            let mut xml_reader = quick_xml::Reader::from_reader(reader);
            let ocel =
                crate::core::event_data::object_centric::ocel_xml::xml_ocel_import::import_ocel_xml(
                    &mut xml_reader,
                    OCELImportOptions::default(),
                )?;
            Ok(ocel)
        } else if format.ends_with("ocel.csv") {
            let ocel = crate::core::event_data::object_centric::ocel_csv::import_ocel_csv(reader)
                .map_err(OCELIOError::Csv)?;
            Ok(ocel)
        } else if format.ends_with("sqlite")
            || (format.ends_with("db") && !format.ends_with("duckdb"))
        {
            #[cfg(feature = "ocel-sqlite")]
            {
                let mut b = Vec::new();
                reader.read_to_end(&mut b)?;
                crate::core::event_data::object_centric::ocel_sql::import_ocel_sqlite_from_slice(&b)
                    .map_err(OCELIOError::Sqlite)
            }
            #[cfg(not(feature = "ocel-sqlite"))]
            Err(OCELIOError::UnsupportedFormat(
                "SQLite support not enabled".to_string(),
            ))
        } else if format.ends_with("duckdb") {
            Err(OCELIOError::UnsupportedFormat(
                "DuckDB import from reader not supported".to_string(),
            ))
        } else if format.ends_with("zip") {
            #[cfg(feature = "ocel-bundle")]
            {
                let mut b = Vec::new();
                reader.read_to_end(&mut b)?;
                crate::core::event_data::object_centric::ocel_bundle::import_ocel_bundle_from_bytes(
                    &b,
                )
                .map_err(OCELIOError::from)
            }
            #[cfg(not(feature = "ocel-bundle"))]
            Err(OCELIOError::UnsupportedFormat(
                "bundled CSV/Parquet support not enabled".to_string(),
            ))
        } else {
            Err(OCELIOError::UnsupportedFormat(format.to_string()))
        }
    }

    fn import_from_path_with_options<P: AsRef<Path>>(
        path: P,
        _: Self::ImportOptions,
    ) -> Result<Self, Self::Error> {
        let path = path.as_ref();
        let format = <Self as Importable>::infer_format(path).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Could not infer format from path",
            )
        })?;

        if format.ends_with("sqlite") || (format.ends_with("db") && !format.ends_with("duckdb")) {
            #[cfg(feature = "ocel-sqlite")]
            return crate::core::event_data::object_centric::ocel_sql::import_ocel_sqlite_from_path(path)
                .map_err(OCELIOError::Sqlite);
            #[cfg(not(feature = "ocel-sqlite"))]
            return Err(OCELIOError::UnsupportedFormat(
                "SQLite support not enabled".to_string(),
            ));
        } else if format.ends_with("duckdb") {
            #[cfg(feature = "ocel-duckdb")]
            return crate::core::event_data::object_centric::ocel_sql::import_ocel_duckdb_from_path(path)
                .map_err(OCELIOError::DuckDB);
            #[cfg(not(feature = "ocel-duckdb"))]
            return Err(OCELIOError::UnsupportedFormat(
                "DuckDB support not enabled".to_string(),
            ));
        } else if format.ends_with("zip") {
            // A path can be a directory (the uncompressed form), and lets an archive be expanded
            // a table at a time rather than held whole.
            #[cfg(feature = "ocel-bundle")]
            return crate::core::event_data::object_centric::ocel_bundle::import_ocel_bundle(path)
                .map_err(OCELIOError::from);
            #[cfg(not(feature = "ocel-bundle"))]
            return Err(OCELIOError::UnsupportedFormat(
                "bundled CSV/Parquet support not enabled".to_string(),
            ));
        } else {
            let file = std::fs::File::open(path)?;
            let reader = std::io::BufReader::new(file);
            Self::import_from_reader(reader, &format)
        }
    }

    fn known_import_formats() -> Vec<crate::core::io::ExtensionWithMime> {
        vec![
            ExtensionWithMime::new("json", "application/json"),
            ExtensionWithMime::new("json.gz", "application/gzip"),
            ExtensionWithMime::new("xml", "application/xml"),
            ExtensionWithMime::new("xml.gz", "application/gzip"),
            ExtensionWithMime::new("ocel.csv", "text/csv"),
            ExtensionWithMime::new("ocel.csv.gz", "application/gzip"),
            #[cfg(feature = "ocel-bundle")]
            // Both names, even though the reader ignores the difference (storage is declared in
            // `ocel-meta.json`): export uses the `-parquet` name to pick which storage to write.
            ExtensionWithMime::new("ocel.zip", "application/zip"),
            #[cfg(feature = "ocel-bundle-parquet")]
            ExtensionWithMime::new("ocel-parquet.zip", "application/zip"),
            #[cfg(feature = "ocel-sqlite")]
            ExtensionWithMime::new("sqlite", "application/x-sqlite3"),
            #[cfg(feature = "ocel-duckdb")]
            ExtensionWithMime::new("duckdb", "application/octet-stream"),
        ]
    }
}

impl Exportable for OCEL {
    type Error = OCELIOError;
    type ExportOptions = ();

    fn infer_format(path: &Path) -> Option<String> {
        let p = path.to_string_lossy().to_lowercase();
        if p.ends_with(".csv.gz") {
            Some("ocel.csv.gz".to_string())
        } else if p.ends_with(".ocel.csv") || p.ends_with(".csv") {
            Some("ocel.csv".to_string())
        } else if p.ends_with(".json") || p.ends_with(".jsonocel") {
            Some("json".to_string())
        } else if p.ends_with(".xml") || p.ends_with(".xmlocel") {
            Some("xml".to_string())
        } else if p.ends_with(".sqlite") || p.ends_with(".db") {
            Some("sqlite".to_string())
        } else if p.ends_with(".duckdb") {
            Some("duckdb".to_string())
        } else if p.ends_with("-parquet.zip") || p.ends_with("-parquet") {
            // Storage is not part of the format's own naming, so a distinct format string is how
            // a caller asks for Parquet.
            Some("ocel-parquet.zip".to_string())
        } else if p.ends_with(".zip") {
            Some("ocel.zip".to_string())
        } else if path.is_dir() {
            // Only an existing directory: a path that does not yet exist cannot be told apart
            // from a misspelled filename, so writing a new directory container has to go through
            // `export_ocel_bundle` explicitly.
            Some("ocel.zip".to_string())
        } else {
            infer_format_from_path(path)
        }
    }

    fn export_to_path_with_options<P: AsRef<Path>>(
        &self,
        path: P,
        options: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        let path = path.as_ref();
        let format = <Self as Exportable>::infer_format(path).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Could not infer format from path",
            )
        })?;
        <Self as Exportable>::export_to_path_as(self, path, &format, options)
    }

    /// Handles the formats that need a real path: a database driver opens its target by name, and
    /// the bundled format's uncompressed form is a directory.
    fn export_to_path_as<P: AsRef<Path>>(
        &self,
        path: P,
        format: &str,
        _: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        let path = path.as_ref();
        if format.ends_with("sqlite") || (format.ends_with("db") && !format.ends_with("duckdb")) {
            #[cfg(feature = "ocel-sqlite")]
            return crate::core::event_data::object_centric::ocel_sql::export_ocel_sqlite_to_path(
                self, path,
            )
            .map_err(OCELIOError::from);
            #[cfg(not(feature = "ocel-sqlite"))]
            return Err(OCELIOError::UnsupportedFormat(
                "SQLite support not enabled".to_string(),
            ));
        } else if format.ends_with("duckdb") {
            #[cfg(feature = "ocel-duckdb")]
            {
                crate::core::event_data::object_centric::ocel_sql::export_ocel_duckdb_to_path(
                    self, path,
                )
                .map_err(OCELIOError::from)
            }
            #[cfg(not(feature = "ocel-duckdb"))]
            return Err(OCELIOError::UnsupportedFormat(
                "DuckDB support not enabled".to_string(),
            ));
        } else if format.ends_with("zip") {
            #[cfg(feature = "ocel-bundle")]
            {
                use crate::core::event_data::object_centric::ocel_bundle::{
                    export_ocel_bundle, BundleExportOptions, ContainerLayout, StorageFormat,
                };
                // A directory, or a name with no extension, means the uncompressed form.
                let layout = if path.is_dir() || path.extension().is_none() {
                    ContainerLayout::Directory
                } else {
                    ContainerLayout::Archive
                };
                if layout == ContainerLayout::Directory {
                    std::fs::create_dir_all(path)?;
                }
                export_ocel_bundle(
                    self,
                    path,
                    BundleExportOptions {
                        layout,
                        storage: if format.starts_with("ocel-parquet") {
                            StorageFormat::Parquet
                        } else {
                            StorageFormat::Csv
                        },
                    },
                )
                .map_err(OCELIOError::from)
            }
            #[cfg(not(feature = "ocel-bundle"))]
            return Err(OCELIOError::UnsupportedFormat(
                "bundled CSV/Parquet support not enabled".to_string(),
            ));
        } else {
            let file = std::fs::File::create(path)?;
            let writer = std::io::BufWriter::new(file);
            Self::export_to_writer(self, writer, format)
        }
    }

    fn export_to_writer_with_options<W: Write>(
        &self,
        #[cfg(feature = "ocel-sqlite")] mut writer: W,
        #[cfg(not(feature = "ocel-sqlite"))] writer: W,
        format: &str,
        options: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        if let Some(inner) = format.strip_suffix(".gz") {
            let mut encoder = flate2::write::GzEncoder::new(
                Box::new(writer) as Box<dyn Write>,
                flate2::Compression::default(),
            );
            self.export_to_writer_with_options(&mut encoder, inner, options)?;
            encoder.finish()?;
            return Ok(());
        }
        if format.ends_with("json") || format.ends_with("jsonocel") {
            serde_json::to_writer(writer, self)?;
            Ok(())
        } else if format.ends_with("xml") || format.ends_with("xmlocel") {
            crate::core::event_data::object_centric::ocel_xml::xml_ocel_export::export_ocel_xml(
                writer, self,
            )
            .map_err(OCELIOError::Xml)
        } else if format.ends_with("ocel.csv") {
            crate::core::event_data::object_centric::ocel_csv::export_ocel_csv(writer, self)
                .map_err(|e| OCELIOError::Other(e.to_string()))
        } else if format.ends_with("sqlite")
            || (format.ends_with("db") && !format.ends_with("duckdb"))
        {
            #[cfg(feature = "ocel-sqlite")]
            {
                let b = export_ocel_sqlite_to_vec(self).map_err(OCELIOError::from)?;
                writer.write_all(&b)?;
                Ok(())
            }
            #[cfg(not(feature = "ocel-sqlite"))]
            return Err(OCELIOError::UnsupportedFormat(
                "SQLite support not enabled".to_string(),
            ));
        } else if format.ends_with("zip") {
            #[cfg(feature = "ocel-bundle")]
            {
                use crate::core::event_data::object_centric::ocel_bundle::{
                    write_ocel_bundle_archive, StorageFormat,
                };
                // A ZIP is assembled by seeking back to fix up each entry's header, which a bare
                // `Write` cannot do, so it is built in memory and handed over whole.
                let mut buffer = std::io::Cursor::new(Vec::new());
                write_ocel_bundle_archive(
                    self,
                    &mut buffer,
                    if format.starts_with("ocel-parquet") {
                        StorageFormat::Parquet
                    } else {
                        StorageFormat::Csv
                    },
                )
                .map_err(OCELIOError::from)?;
                let mut writer = writer;
                writer.write_all(&buffer.into_inner())?;
                Ok(())
            }
            #[cfg(not(feature = "ocel-bundle"))]
            return Err(OCELIOError::UnsupportedFormat(
                "bundled CSV/Parquet support not enabled".to_string(),
            ));
        } else if format.ends_with("duckdb") {
            Err(OCELIOError::UnsupportedFormat(
                "DuckDB export to writer not supported".to_string(),
            ))
        } else {
            Err(OCELIOError::UnsupportedFormat(format.to_string()))
        }
    }

    fn known_export_formats() -> Vec<crate::core::io::ExtensionWithMime> {
        vec![
            ExtensionWithMime::new("json", "application/json"),
            ExtensionWithMime::new("json.gz", "application/gzip"),
            ExtensionWithMime::new("xml", "application/xml"),
            ExtensionWithMime::new("xml.gz", "application/gzip"),
            ExtensionWithMime::new("ocel.csv", "text/csv"),
            ExtensionWithMime::new("ocel.csv.gz", "application/gzip"),
            #[cfg(feature = "ocel-bundle")]
            ExtensionWithMime::new("ocel.zip", "application/zip"),
            #[cfg(feature = "ocel-bundle-parquet")]
            ExtensionWithMime::new("ocel-parquet.zip", "application/zip"),
            #[cfg(feature = "ocel-sqlite")]
            ExtensionWithMime::new("sqlite", "application/x-sqlite3"),
            #[cfg(feature = "ocel-duckdb")]
            ExtensionWithMime::new("duckdb", "application/octet-stream"),
        ]
    }
}
