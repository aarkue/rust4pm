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

impl Importable for OCEL {
    type Error = OCELIOError;
    type ImportOptions = ();

    fn infer_format(path: &Path) -> Option<String> {
        let p = path.to_string_lossy().to_lowercase();
        if p.ends_with(".csv") {
            Some("ocel.csv".to_string())
        } else if p.ends_with(".json") || p.ends_with(".jsonocel") {
            Some("json".to_string())
        } else if p.ends_with(".xml") || p.ends_with(".xmlocel") {
            Some("xml".to_string())
        } else if p.ends_with(".sqlite") || p.ends_with(".db") {
            Some("sqlite".to_string())
        } else if p.ends_with(".duckdb") {
            Some("duckdb".to_string())
        } else {
            infer_format_from_path(path)
        }
    }

    fn import_from_reader_with_options<R: Read>(
        #[cfg(feature = "ocel-sqlite")] mut reader: R,
        #[cfg(not(feature = "ocel-sqlite"))] reader: R,
        format: &str,
        _: Self::ImportOptions,
    ) -> Result<Self, Self::Error> {
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
                )
                .map_err(OCELIOError::Xml)?;
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
        } else {
            let file = std::fs::File::open(path)?;
            let reader = std::io::BufReader::new(file);
            Self::import_from_reader(reader, &format)
        }
    }

    fn known_import_formats() -> Vec<crate::core::io::ExtensionWithMime> {
        vec![
            ExtensionWithMime::new("json", "application/json"),
            ExtensionWithMime::new("xml", "application/xml"),
            ExtensionWithMime::new("ocel.csv", "text/csv"),
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
        if p.ends_with(".ocel.csv") {
            Some("ocel.csv".to_string())
        } else if p.ends_with(".json") || p.ends_with(".jsonocel") {
            Some("json".to_string())
        } else if p.ends_with(".xml") || p.ends_with(".xmlocel") {
            Some("xml".to_string())
        } else if p.ends_with(".sqlite") || p.ends_with(".db") {
            Some("sqlite".to_string())
        } else if p.ends_with(".duckdb") {
            Some("duckdb".to_string())
        } else {
            infer_format_from_path(path)
        }
    }

    fn export_to_path_with_options<P: AsRef<Path>>(
        &self,
        path: P,
        _: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        let path = path.as_ref();
        let format = <Self as Exportable>::infer_format(path).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Could not infer format from path",
            )
        })?;

        if format.ends_with("sqlite") || (format.ends_with("db") && !format.ends_with("duckdb")) {
            #[cfg(feature = "ocel-sqlite")]
            return crate::core::event_data::object_centric::ocel_sql::export_ocel_sqlite_to_path(
                self, path,
            )
            .map_err(|e| match e {
                #[cfg(feature = "ocel-sqlite")]
                DatabaseError::SQLITE(e) => OCELIOError::Sqlite(e),
                #[cfg(feature = "ocel-duckdb")]
                DatabaseError::DUCKDB(e) => OCELIOError::DuckDB(e),
            });
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
                .map_err(|e| match e {
                    #[cfg(feature = "ocel-sqlite")]
                    DatabaseError::SQLITE(e) => OCELIOError::Sqlite(e),
                    #[cfg(feature = "ocel-duckdb")]
                    DatabaseError::DUCKDB(e) => OCELIOError::DuckDB(e),
                })
            }
            #[cfg(not(feature = "ocel-duckdb"))]
            return Err(OCELIOError::UnsupportedFormat(
                "DuckDB support not enabled".to_string(),
            ));
        } else {
            let file = std::fs::File::create(path)?;
            let writer = std::io::BufWriter::new(file);
            Self::export_to_writer(self, writer, &format)
        }
    }

    fn export_to_writer_with_options<W: Write>(
        &self,
        #[cfg(feature = "ocel-sqlite")] mut writer: W,
        #[cfg(not(feature = "ocel-sqlite"))] writer: W,
        format: &str,
        _: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
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
                let b = export_ocel_sqlite_to_vec(self).map_err(|e| match e {
                    #[cfg(feature = "ocel-sqlite")]
                    DatabaseError::SQLITE(e) => OCELIOError::Sqlite(e),
                    #[cfg(feature = "ocel-duckdb")]
                    DatabaseError::DUCKDB(e) => OCELIOError::DuckDB(e),
                })?;
                writer.write_all(&b)?;
                Ok(())
            }
            #[cfg(not(feature = "ocel-sqlite"))]
            return Err(OCELIOError::UnsupportedFormat(
                "SQLite support not enabled".to_string(),
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
            ExtensionWithMime::new("xml", "application/xml"),
            ExtensionWithMime::new("ocel.csv", "text/csv"),
            #[cfg(feature = "ocel-sqlite")]
            ExtensionWithMime::new("sqlite", "application/x-sqlite3"),
            #[cfg(feature = "ocel-duckdb")]
            ExtensionWithMime::new("duckdb", "application/octet-stream"),
        ]
    }
}
