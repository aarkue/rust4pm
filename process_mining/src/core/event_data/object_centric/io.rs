//! IO implementations for OCEL

use std::io::{Read, Write};
use std::path::Path;

#[cfg(not(all(not(feature = "ocel-duckdb"), not(feature = "ocel-sqlite"))))]
use crate::core::event_data::object_centric::ocel_sql::DatabaseError;
use crate::core::event_data::object_centric::ocel_xml::xml_ocel_import::OCELImportOptions;
use crate::core::event_data::object_centric::OCEL;
use crate::core::io::{Exportable, Importable};

/// Error type for OCEL IO operations
#[derive(Debug)]
pub enum OCELIOError {
    /// IO Error
    Io(std::io::Error),
    /// JSON Parsing Error
    Json(serde_json::Error),
    /// XML Parsing Error
    Xml(quick_xml::Error),
    #[cfg(feature = "ocel-sqlite")]
    /// `SQLite` Error
    Sqlite(rusqlite::Error),
    #[cfg(feature = "ocel-duckdb")]
    /// `DuckDB` Error
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

    fn infer_format(path: &Path) -> Option<String> {
        let p = path.to_string_lossy().to_lowercase();
        if p.ends_with(".json") || p.ends_with(".jsonocel") {
            Some("json".to_string())
        } else if p.ends_with(".xml") || p.ends_with(".xmlocel") {
            Some("xml".to_string())
        } else if p.ends_with(".sqlite") || p.ends_with(".db") {
            Some("sqlite".to_string())
        } else if p.ends_with(".duckdb") {
            Some("duckdb".to_string())
        } else {
            path.extension()
                .and_then(|e| e.to_str())
                .map(|s| s.to_lowercase())
        }
    }

    fn import_from_path<P: AsRef<Path>>(path: P) -> Result<Self, Self::Error> {
        let path = path.as_ref();
        let format = <Self as Importable>::infer_format(path).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Could not infer format from path",
            )
        })?;

        if format == "sqlite" || format.ends_with(".sqlite") || format.ends_with(".db") {
            #[cfg(feature = "ocel-sqlite")]
            return crate::core::event_data::object_centric::ocel_sql::import_ocel_sqlite_from_path(path)
                .map_err(OCELIOError::Sqlite);
            #[cfg(not(feature = "ocel-sqlite"))]
            return Err(OCELIOError::UnsupportedFormat(
                "SQLite support not enabled".to_string(),
            ));
        } else if format == "duckdb" || format.ends_with(".duckdb") {
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

    fn import_from_reader<R: Read>(reader: R, format: &str) -> Result<Self, Self::Error> {
        if format == "json" || format.ends_with(".json") || format.ends_with(".jsonocel") {
            let reader = std::io::BufReader::new(reader);
            let ocel: OCEL = serde_json::from_reader(reader)?;
            Ok(ocel)
        } else if format == "xml" || format.ends_with(".xml") || format.ends_with(".xmlocel") {
            let reader = std::io::BufReader::new(reader);
            let mut xml_reader = quick_xml::Reader::from_reader(reader);
            let ocel =
                crate::core::event_data::object_centric::ocel_xml::xml_ocel_import::import_ocel_xml(
                    &mut xml_reader,
                    OCELImportOptions::default(),
                )
                .map_err(OCELIOError::Xml)?;
            Ok(ocel)
        } else if format.ends_with("sqlite") || format.ends_with("db") {
            Err(OCELIOError::UnsupportedFormat(
                "SQLite import from reader not supported".to_string(),
            ))
        } else if format.ends_with("duckdb") {
            Err(OCELIOError::UnsupportedFormat(
                "DuckDB import from reader not supported".to_string(),
            ))
        } else {
            Err(OCELIOError::UnsupportedFormat(format.to_string()))
        }
    }
}

impl Exportable for OCEL {
    type Error = OCELIOError;

    fn infer_format(path: &Path) -> Option<String> {
        let p = path.to_string_lossy().to_lowercase();
        if p.ends_with(".json") || p.ends_with(".jsonocel") {
            Some("json".to_string())
        } else if p.ends_with(".xml") || p.ends_with(".xmlocel") {
            Some("xml".to_string())
        } else if p.ends_with(".sqlite") || p.ends_with(".db") {
            Some("sqlite".to_string())
        } else if p.ends_with(".duckdb") {
            Some("duckdb".to_string())
        } else {
            path.extension()
                .and_then(|e| e.to_str())
                .map(|s| s.to_lowercase())
        }
    }

    fn export_to_path<P: AsRef<Path>>(&self, path: P) -> Result<(), Self::Error> {
        let path = path.as_ref();
        let format = <Self as Exportable>::infer_format(path).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Could not infer format from path",
            )
        })?;

        if format == "sqlite" || format.ends_with(".sqlite") || format.ends_with(".db") {
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
        } else if format == "duckdb" || format.ends_with(".duckdb") {
            #[cfg(feature = "ocel-duckdb")]
            return crate::core::event_data::object_centric::ocel_sql::export_ocel_duckdb_to_path(
                self, path,
            )
            .map_err(|e| match e {
                #[cfg(feature = "ocel-sqlite")]
                DatabaseError::SQLITE(e) => OCELIOError::Sqlite(e),
                #[cfg(feature = "ocel-duckdb")]
                DatabaseError::DUCKDB(e) => OCELIOError::DuckDB(e),
            });
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

    fn export_to_writer<W: Write>(&self, writer: W, format: &str) -> Result<(), Self::Error> {
        if format == "json" || format.ends_with(".json") || format.ends_with(".jsonocel") {
            serde_json::to_writer(writer, self)?;
            Ok(())
        } else if format == "xml" || format.ends_with(".xml") || format.ends_with(".xmlocel") {
            crate::core::event_data::object_centric::ocel_xml::xml_ocel_export::export_ocel_xml(
                writer, self,
            )
            .map_err(OCELIOError::Xml)
        } else if format.ends_with("sqlite") || format.ends_with("db") {
            Err(OCELIOError::UnsupportedFormat(
                "SQLite export to writer not supported".to_string(),
            ))
        } else if format.ends_with("duckdb") {
            Err(OCELIOError::UnsupportedFormat(
                "DuckDB export to writer not supported".to_string(),
            ))
        } else {
            Err(OCELIOError::UnsupportedFormat(format.to_string()))
        }
    }
}
