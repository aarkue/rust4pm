//! IO implementations for OCEL

#[cfg(any(feature = "ocel-sqlite", feature = "ocel-duckdb"))]
use std::borrow::Cow;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;

use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;

use crate::core::event_data::object_centric::appendable::{append_ocel, AppendableOCEL};
use crate::core::event_data::object_centric::linked_ocel::slim_linked_ocel::SlimAppendError;
#[cfg(feature = "ocel-bundle")]
use crate::core::event_data::object_centric::ocel_bundle;
use crate::core::event_data::object_centric::ocel_csv::{
    export_ocel_csv, read_ocel_csv, OCELCSVExportOptions,
};
use crate::core::event_data::object_centric::ocel_json::{
    export_ocel_json_to_writer, import_ocel_json_into,
};
#[cfg(any(feature = "ocel-sqlite", feature = "ocel-duckdb"))]
use crate::core::event_data::object_centric::ocel_sql;
use crate::core::event_data::object_centric::ocel_xml::xml_ocel_export::export_ocel_xml;
use crate::core::event_data::object_centric::ocel_xml::xml_ocel_import::{
    import_ocel_xml, import_ocel_xml_into,
};
use crate::core::event_data::object_centric::readable::ReadableOCEL;
use crate::core::event_data::object_centric::OCEL;
use crate::core::io::{
    controlled, infer_format_or_err, write_to_path, Exportable, ExtensionWithMime, Importable,
    ProgressReader, IMPORT_STAGE,
};
use crate::utils::control::{Cancelled, Control};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Options for importing an OCEL, in any format.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct OCELImportOptions {
    /// Date format tried first when parsing XML and CSV times, before the formats
    /// [`parse_timestamp`](crate::core::event_data::timestamp_utils::parse_timestamp) knows.
    ///
    /// See <https://docs.rs/chrono/latest/chrono/format/strftime/index.html> for the specifiers.
    pub date_format: Option<String>,
    /// Reject what the OCEL 2.0 specification forbids instead of skipping or tolerating it: CSV
    /// rows without a timestamp or with an unknown O2O source, and `SQLite`/`DuckDB` object-type
    /// tables without an `ocel_changed_field` column. Without `strict`, such a table is read as
    /// initial values only, which loses nothing, as it records no change.
    pub strict: bool,
}

/// Options for exporting an OCEL. A format's own options are in its field.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct OCELExportOptions {
    /// Options of the OCEL CSV format
    pub csv: OCELCSVExportOptions,
}

/// Error type for OCEL IO operations
#[derive(Debug)]
pub enum OCELIOError {
    /// IO Error
    Io(std::io::Error),
    /// JSON Parsing Error
    Json(serde_json::Error),
    /// XML Parsing Error
    Xml(quick_xml::Error),
    /// OCEL CSV Error
    Csv(OCELCSVError),
    /// `SQLite` Error
    #[cfg(feature = "ocel-sqlite")]
    Sqlite(rusqlite::Error),
    /// `DuckDB` Error
    #[cfg(feature = "ocel-duckdb")]
    DuckDB(duckdb::Error),
    /// Reading or writing a bundled CSV/Parquet container failed
    #[cfg(feature = "ocel-bundle")]
    Bundle(ocel_bundle::BundleError),
    /// The input breaks its format, e.g., a missing attribute, column or table, or a time that
    /// does not parse
    Invalid(String),
    /// A sink rejected an event or object
    Append(SlimAppendError),
    /// Unsupported Format
    UnsupportedFormat(String),
    /// The import was cancelled through its [`Control`]
    Cancelled,
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
            OCELIOError::Bundle(e) => write!(f, "OCEL Bundle Error: {}", e),
            OCELIOError::Invalid(s) => write!(f, "Invalid OCEL: {}", s),
            OCELIOError::Append(e) => write!(f, "{}", e),
            OCELIOError::UnsupportedFormat(s) => write!(f, "Unsupported Format: {}", s),
            OCELIOError::Cancelled => write!(f, "{}", Cancelled),
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
            OCELIOError::Bundle(e) => Some(e),
            OCELIOError::Append(e) => Some(e),
            OCELIOError::Invalid(_)
            | OCELIOError::UnsupportedFormat(_)
            | OCELIOError::Cancelled => None,
        }
    }
}

/// Error of the OCEL CSV format
#[derive(Debug)]
pub enum OCELCSVError {
    /// Reading or writing the CSV failed
    Csv(csv::Error),
    /// Missing required column
    MissingColumn(String),
    /// Invalid timestamp format
    InvalidTimestamp {
        /// Row number where the error occurred
        row: usize,
        /// The invalid timestamp value
        value: String,
    },
    /// Invalid object reference format
    InvalidObjectReference {
        /// Row number where the error occurred
        row: usize,
        /// The invalid object reference value
        value: String,
    },
    /// General parsing error with context
    ParseError {
        /// Row number where the error occurred
        row: usize,
        /// Error message
        message: String,
    },
}

impl std::fmt::Display for OCELCSVError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Csv(e) => write!(f, "{e}"),
            Self::MissingColumn(col) => write!(f, "Missing required column: {col}"),
            Self::InvalidTimestamp { row, value } => {
                write!(f, "Invalid timestamp at row {row}: '{value}'")
            }
            Self::InvalidObjectReference { row, value } => {
                write!(f, "Invalid object reference at row {row}: '{value}'")
            }
            Self::ParseError { row, message } => write!(f, "Parse error at row {row}: {message}"),
        }
    }
}

impl std::error::Error for OCELCSVError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Csv(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for OCELIOError {
    fn from(e: std::io::Error) -> Self {
        OCELIOError::Io(e)
    }
}

impl From<Cancelled> for OCELIOError {
    fn from(_: Cancelled) -> Self {
        OCELIOError::Cancelled
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

impl From<OCELCSVError> for OCELIOError {
    fn from(e: OCELCSVError) -> Self {
        OCELIOError::Csv(e)
    }
}

impl From<csv::Error> for OCELIOError {
    fn from(e: csv::Error) -> Self {
        OCELIOError::Csv(OCELCSVError::Csv(e))
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
impl From<ocel_bundle::BundleError> for OCELIOError {
    fn from(e: ocel_bundle::BundleError) -> Self {
        OCELIOError::Bundle(e)
    }
}

/// An OCEL format, named by [`OcelFormat::parse`] or read off a path by [`OcelFormat::infer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OcelFormat {
    pub(crate) kind: OcelKind,
    /// Gzip-compressed. Only the text formats can be.
    pub(crate) gz: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OcelKind {
    Json,
    Xml,
    Csv,
    #[cfg(feature = "ocel-sqlite")]
    Sqlite,
    #[cfg(feature = "ocel-duckdb")]
    DuckDb,
    /// The bundled CSV/Parquet format. `parquet` picks the storage an export writes, an import
    /// reads it from the manifest.
    #[cfg(feature = "ocel-bundle")]
    Bundle {
        parquet: bool,
    },
}

impl OcelKind {
    fn compressible(self) -> bool {
        matches!(self, OcelKind::Json | OcelKind::Xml | OcelKind::Csv)
    }
}

/// Format names, matched case-insensitively and without a `.gz`.
const FORMAT_NAMES: &[(&str, OcelKind)] = &[
    ("json", OcelKind::Json),
    ("jsonocel", OcelKind::Json),
    ("xml", OcelKind::Xml),
    ("xmlocel", OcelKind::Xml),
    ("ocel.csv", OcelKind::Csv),
    ("csv", OcelKind::Csv),
    #[cfg(feature = "ocel-sqlite")]
    ("sqlite", OcelKind::Sqlite),
    #[cfg(feature = "ocel-sqlite")]
    ("sqlite3", OcelKind::Sqlite),
    #[cfg(feature = "ocel-sqlite")]
    ("db", OcelKind::Sqlite),
    #[cfg(feature = "ocel-duckdb")]
    ("duckdb", OcelKind::DuckDb),
    #[cfg(feature = "ocel-bundle")]
    ("ocel.zip", OcelKind::Bundle { parquet: false }),
    #[cfg(feature = "ocel-bundle")]
    ("zip", OcelKind::Bundle { parquet: false }),
    #[cfg(feature = "ocel-bundle")]
    ("ocel-parquet.zip", OcelKind::Bundle { parquet: true }),
];

/// Path suffixes, lowercase and without a `.gz`, checked in order.
const PATH_SUFFIXES: &[(&str, OcelKind)] = &[
    (".csv", OcelKind::Csv),
    (".json", OcelKind::Json),
    (".jsonocel", OcelKind::Json),
    (".xml", OcelKind::Xml),
    (".xmlocel", OcelKind::Xml),
    #[cfg(feature = "ocel-sqlite")]
    (".sqlite", OcelKind::Sqlite),
    #[cfg(feature = "ocel-sqlite")]
    (".sqlite3", OcelKind::Sqlite),
    #[cfg(feature = "ocel-sqlite")]
    (".db", OcelKind::Sqlite),
    #[cfg(feature = "ocel-duckdb")]
    (".duckdb", OcelKind::DuckDb),
    // Storage is not part of the bundled format's naming, so this suffix is how a path asks for
    // Parquet. Without an extension it names a directory.
    #[cfg(feature = "ocel-bundle")]
    ("-parquet.zip", OcelKind::Bundle { parquet: true }),
    #[cfg(feature = "ocel-bundle")]
    ("-parquet", OcelKind::Bundle { parquet: true }),
    #[cfg(feature = "ocel-bundle")]
    (".zip", OcelKind::Bundle { parquet: false }),
];

impl OcelFormat {
    fn with_gz(kind: OcelKind, gz: bool) -> Option<Self> {
        (!gz || kind.compressible()).then_some(Self { kind, gz })
    }

    /// The format named `format`, e.g., `json`, `xml.gz`, `ocel.csv` or `sqlite`, ignoring case
    /// and a leading `.`. `db` is `SQLite`.
    pub(crate) fn parse(format: &str) -> Result<Self, OCELIOError> {
        let lower = format.to_ascii_lowercase();
        let name = lower.strip_prefix('.').unwrap_or(&lower);
        let (base, gz) = match name.strip_suffix(".gz") {
            Some(base) => (base, true),
            None => (name, false),
        };
        FORMAT_NAMES
            .iter()
            .find(|(n, _)| *n == base)
            .and_then(|&(_, kind)| Self::with_gz(kind, gz))
            .ok_or_else(|| OCELIOError::UnsupportedFormat(format.to_string()))
    }

    /// The format `path` names by its extension. An existing directory, or a bundle's manifest
    /// (which names the directory it is in), is the bundled format.
    pub(crate) fn infer(path: &Path) -> Option<Self> {
        #[cfg(feature = "ocel-bundle")]
        if path
            .file_name()
            .is_some_and(|n| n.eq_ignore_ascii_case(ocel_bundle::META_FILE_NAME))
        {
            return Some(Self {
                kind: OcelKind::Bundle { parquet: false },
                gz: false,
            });
        }
        let p = path.to_string_lossy().to_lowercase();
        let (base, gz) = match p.strip_suffix(".gz") {
            Some(base) => (base, true),
            None => (p.as_str(), false),
        };
        let kind = PATH_SUFFIXES
            .iter()
            .find(|(suffix, _)| base.ends_with(suffix))
            .map(|&(_, kind)| kind);
        #[cfg(feature = "ocel-bundle")]
        let kind = kind.or_else(|| path.is_dir().then_some(OcelKind::Bundle { parquet: false }));
        Self::with_gz(kind?, gz)
    }

    /// The canonical name, which [`Self::parse`] reads back.
    pub(crate) fn name(self) -> String {
        let base = match self.kind {
            OcelKind::Json => "json",
            OcelKind::Xml => "xml",
            OcelKind::Csv => "ocel.csv",
            #[cfg(feature = "ocel-sqlite")]
            OcelKind::Sqlite => "sqlite",
            #[cfg(feature = "ocel-duckdb")]
            OcelKind::DuckDb => "duckdb",
            #[cfg(feature = "ocel-bundle")]
            OcelKind::Bundle { parquet: false } => "ocel.zip",
            #[cfg(feature = "ocel-bundle")]
            OcelKind::Bundle { parquet: true } => "ocel-parquet.zip",
        };
        if self.gz {
            format!("{base}.gz")
        } else {
            base.to_string()
        }
    }

    /// The formats of this build, for both import and export.
    pub(crate) fn known() -> Vec<ExtensionWithMime> {
        let gzip = "application/gzip";
        let formats = [
            (OcelKind::Json, false, "application/json"),
            (OcelKind::Json, true, gzip),
            (OcelKind::Xml, false, "application/xml"),
            (OcelKind::Xml, true, gzip),
            (OcelKind::Csv, false, "text/csv"),
            (OcelKind::Csv, true, gzip),
            #[cfg(feature = "ocel-bundle")]
            (
                OcelKind::Bundle { parquet: false },
                false,
                "application/zip",
            ),
            #[cfg(feature = "ocel-bundle-parquet")]
            (OcelKind::Bundle { parquet: true }, false, "application/zip"),
            #[cfg(feature = "ocel-sqlite")]
            (OcelKind::Sqlite, false, "application/x-sqlite3"),
            #[cfg(feature = "ocel-duckdb")]
            (OcelKind::DuckDb, false, "application/octet-stream"),
        ];
        formats
            .into_iter()
            .map(|(kind, gz, mime)| ExtensionWithMime::new(Self { kind, gz }.name(), mime))
            .collect()
    }
}

/// Where an import reads from. `SQLite`, `DuckDB` and bundles read a path directly, everything
/// else reads its bytes.
pub(crate) enum OcelSource<'a> {
    Path(&'a Path),
    Reader(Box<dyn Read + 'a>),
}

impl<'a> OcelSource<'a> {
    /// `reader`, counting the bytes read as progress of stage `"Importing"` out of `total`, and
    /// failing to read once `ctl` is cancelled.
    pub(crate) fn reader<R: Read + 'a>(
        reader: R,
        ctl: &'a Control<'_>,
        total: Option<u64>,
    ) -> Self {
        Self::Reader(Box::new(ProgressReader::new(
            reader,
            ctl,
            IMPORT_STAGE,
            total,
        )))
    }

    /// The bytes of this source, gzip-decoded if `gz`. A path counts its bytes as
    /// [`Self::reader`] does, out of the file size.
    fn open(self, gz: bool, ctl: &'a Control<'_>) -> Result<Box<dyn Read + 'a>, OCELIOError> {
        let reader = match self {
            Self::Path(path) => {
                let file = File::open(path)?;
                let total = file.metadata()?.len();
                Box::new(ProgressReader::new(file, ctl, IMPORT_STAGE, Some(total)))
            }
            Self::Reader(reader) => reader,
        };
        Ok(if gz {
            // `MultiGzDecoder` reads from its inner in small chunks
            Box::new(MultiGzDecoder::new(BufReader::new(reader)))
        } else {
            reader
        })
    }
}

/// Where an export writes to. `SQLite`, `DuckDB` and bundles write a path directly, everything
/// else writes bytes.
pub(crate) enum OcelTarget<'a> {
    Path(&'a Path),
    Writer(&'a mut dyn Write),
}

#[cfg(any(feature = "ocel-sqlite", feature = "ocel-bundle"))]
fn read_all(mut reader: impl Read) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    Ok(bytes)
}

/// Import an [`OCEL`] in format `f` from `src`, reporting to `ctl`.
pub(crate) fn import_ocel(
    src: OcelSource<'_>,
    f: OcelFormat,
    options: &OCELImportOptions,
    ctl: &Control<'_>,
) -> Result<OCEL, OCELIOError> {
    match (src, f.kind) {
        #[cfg(feature = "ocel-sqlite")]
        (OcelSource::Path(path), OcelKind::Sqlite) if !f.gz => {
            ocel_sql::sqlite::sqlite_ocel_import::import_sqlite_path(path, options, ctl)
        }
        #[cfg(feature = "ocel-duckdb")]
        (OcelSource::Path(path), OcelKind::DuckDb) if !f.gz => {
            ocel_sql::duckdb::duckdb_ocel_import::import_duckdb_path(path, options, ctl)
        }
        // A path can be a directory, and lets an archive be expanded a table at a time
        #[cfg(feature = "ocel-bundle")]
        (OcelSource::Path(path), OcelKind::Bundle { .. }) if !f.gz => {
            ocel_bundle::Container::open(path)?.read()
        }
        (src, kind) => {
            let reader = src.open(f.gz, ctl)?;
            match kind {
                OcelKind::Json => Ok(serde_json::from_reader(BufReader::new(reader))?),
                OcelKind::Xml => import_ocel_xml(
                    &mut quick_xml::Reader::from_reader(BufReader::new(reader)),
                    options,
                    ctl,
                ),
                OcelKind::Csv => read_ocel_csv(reader, options, ctl),
                // A SQLite file and a ZIP are both read from their end
                #[cfg(feature = "ocel-sqlite")]
                OcelKind::Sqlite => ocel_sql::sqlite::sqlite_ocel_import::import_sqlite_slice(
                    &read_all(reader)?,
                    options,
                    ctl,
                ),
                #[cfg(feature = "ocel-duckdb")]
                OcelKind::DuckDb => Err(OCELIOError::UnsupportedFormat(
                    "DuckDB import from a reader is not supported".to_string(),
                )),
                #[cfg(feature = "ocel-bundle")]
                OcelKind::Bundle { .. } => {
                    ocel_bundle::Container::open_bytes(&read_all(reader)?)?.read()
                }
            }
        }
    }
}

/// Stream an OCEL in format `f` from `src` into `sink`, then [finalize](AppendableOCEL::finalize)
/// it. JSON, XML, bundles and the consolidated `DuckDB` schema stream, any other format is
/// imported as an [`OCEL`] first and appended with its events sorted by time.
pub(crate) fn stream_ocel<A>(
    sink: &mut A,
    src: OcelSource<'_>,
    f: OcelFormat,
    options: &OCELImportOptions,
    ctl: &Control<'_>,
) -> Result<(), OCELIOError>
where
    A: AppendableOCEL,
    A::Error: Into<OCELIOError>,
{
    match (src, f.kind) {
        (src, OcelKind::Json) => import_ocel_json_into(BufReader::new(src.open(f.gz, ctl)?), sink)?,
        (src, OcelKind::Xml) => import_ocel_xml_into(
            &mut quick_xml::Reader::from_reader(BufReader::new(src.open(f.gz, ctl)?)),
            sink,
            options,
            ctl,
        )?,
        #[cfg(feature = "ocel-bundle")]
        (src, OcelKind::Bundle { .. }) => {
            use ocel_bundle::Container;
            match src {
                OcelSource::Path(path) if !f.gz => Container::open(path)?,
                src => Container::open_bytes(&read_all(src.open(f.gz, ctl)?)?)?,
            }
            .read_into(sink)?
        }
        // The per-type layout also spells itself `.duckdb`, so the tables decide
        #[cfg(feature = "ocel-duckdb")]
        (OcelSource::Path(path), OcelKind::DuckDb) if !f.gz => {
            use ocel_sql::duckdb::{
                duckdb_ocel_import::open_read_only,
                schema::reader::{is_consolidated_schema, DuckDbReadInto},
            };
            let con = open_read_only(path)?;
            if is_consolidated_schema(&con)? {
                sink.read_from_duckdb(&con)?;
            } else {
                let ocel = ocel_sql::import::import_ocel(&con, options, ctl)?;
                append_ocel(sink, ocel, |_, e| Err(e)).map_err(Into::into)?;
            }
        }
        (src, _) => {
            append_ocel(sink, import_ocel(src, f, options, ctl)?, |_, e| Err(e))
                .map_err(Into::into)?;
        }
    }
    sink.finalize().map_err(Into::into)
}

/// Export `ocel` in format `f` to `dst`. A writer is flushed once at the end.
pub(crate) fn export_ocel<O: ReadableOCEL + ?Sized>(
    ocel: &O,
    dst: OcelTarget<'_>,
    f: OcelFormat,
    options: &OCELExportOptions,
) -> Result<(), OCELIOError> {
    match (dst, f.kind) {
        // A database driver opens its target by name
        #[cfg(feature = "ocel-sqlite")]
        (OcelTarget::Path(path), OcelKind::Sqlite) if !f.gz => {
            ocel_sql::export_ocel_sqlite_to_path(ocel, path)
        }
        #[cfg(feature = "ocel-duckdb")]
        (OcelTarget::Path(path), OcelKind::DuckDb) if !f.gz => {
            ocel_sql::export_ocel_duckdb_to_path(ocel, path)
        }
        #[cfg(feature = "ocel-bundle")]
        (OcelTarget::Path(path), OcelKind::Bundle { parquet }) if !f.gz => {
            export_bundle(ocel, path, parquet)
        }
        (OcelTarget::Path(path), _) => write_to_path(path, |file| {
            export_ocel(ocel, OcelTarget::Writer(file), f, options)
        }),
        (OcelTarget::Writer(writer), _) => {
            write_ocel(ocel, writer, f, options)?;
            // Once, here: a flush on a gzip encoder writes a needless sync block, and a writer
            // given by value would otherwise drop a failing final write with it
            writer.flush()?;
            Ok(())
        }
    }
}

/// [`export_ocel`] to `writer` without flushing it.
fn write_ocel<O: ReadableOCEL + ?Sized>(
    ocel: &O,
    writer: &mut dyn Write,
    f: OcelFormat,
    options: &OCELExportOptions,
) -> Result<(), OCELIOError> {
    if f.gz {
        let mut encoder = GzEncoder::new(writer, flate2::Compression::default());
        write_ocel(ocel, &mut encoder, OcelFormat { gz: false, ..f }, options)?;
        encoder.finish()?;
        return Ok(());
    }
    match f.kind {
        // A failed write is `Io`, as for the other formats
        OcelKind::Json => {
            Ok(export_ocel_json_to_writer(ocel, writer).map_err(std::io::Error::from)?)
        }
        OcelKind::Xml => Ok(export_ocel_xml(writer, ocel)?),
        OcelKind::Csv => export_ocel_csv(NoFlush(writer), ocel, &options.csv),
        #[cfg(feature = "ocel-sqlite")]
        OcelKind::Sqlite => {
            writer.write_all(&ocel_sql::export_ocel_sqlite_to_vec(ocel)?)?;
            Ok(())
        }
        #[cfg(feature = "ocel-duckdb")]
        OcelKind::DuckDb => Err(OCELIOError::UnsupportedFormat(
            "DuckDB export to a writer is not supported".to_string(),
        )),
        #[cfg(feature = "ocel-bundle")]
        OcelKind::Bundle { parquet } => {
            // A ZIP is assembled by seeking back to fix up each entry's header, which a bare
            // `Write` cannot do, so it is built in memory and handed over whole.
            let mut buffer = std::io::Cursor::new(Vec::new());
            ocel_bundle::write_ocel_bundle_archive(ocel, &mut buffer, bundle_storage(parquet))?;
            writer.write_all(&buffer.into_inner())?;
            Ok(())
        }
    }
}

#[cfg(feature = "ocel-bundle")]
fn bundle_storage(parquet: bool) -> ocel_bundle::StorageFormat {
    use ocel_bundle::StorageFormat;
    if parquet {
        StorageFormat::Parquet
    } else {
        StorageFormat::Csv
    }
}

#[cfg(feature = "ocel-bundle")]
fn export_bundle<O: ReadableOCEL + ?Sized>(
    ocel: &O,
    path: &Path,
    parquet: bool,
) -> Result<(), OCELIOError> {
    use ocel_bundle::{export_ocel_bundle, BundleExportOptions, ContainerLayout};
    // A directory, or a name with no extension, means the uncompressed form.
    let layout = if path.is_dir() || path.extension().is_none() {
        ContainerLayout::Directory
    } else {
        ContainerLayout::Archive
    };
    export_ocel_bundle(
        ocel,
        path,
        BundleExportOptions {
            layout,
            storage: bundle_storage(parquet),
        },
    )
}

/// A writer that leaves flushing to whoever owns the inner writer, for an exporter that flushes
/// on its own.
struct NoFlush<'a>(&'a mut dyn Write);

impl Write for NoFlush<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.0.write_all(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Importable for OCEL {
    type Error = OCELIOError;
    type ImportOptions = OCELImportOptions;

    fn infer_format(path: &Path) -> Option<String> {
        OcelFormat::infer(path).map(OcelFormat::name)
    }

    /// See [`Importable::import_from_reader_with`]. Also reports stage `"Importing"` with the bytes
    /// read, without a total, and stops reading once `ctl` is cancelled.
    fn import_from_reader_with<R: Read>(
        reader: R,
        format: &str,
        options: Self::ImportOptions,
        ctl: &Control<'_>,
    ) -> Result<Self, Self::Error> {
        let f = OcelFormat::parse(format)?;
        controlled(ctl, || {
            import_ocel(OcelSource::reader(reader, ctl, None), f, &options, ctl)
        })
    }

    /// See [`Importable::import_from_path_with`]. `SQLite` and `DuckDB` report one stage per
    /// table kind, counting rows: `"Importing objects"`, `"Importing object changes"`,
    /// `"Importing events"`, `"Importing event-object relations"` and
    /// `"Importing object-object relations"`, with a total where the database knows it. A bundle
    /// reports nothing and is only checked for a cancel before and after.
    fn import_from_path_with<P: AsRef<Path>>(
        path: P,
        options: Self::ImportOptions,
        ctl: &Control<'_>,
    ) -> Result<Self, Self::Error> {
        let path = path.as_ref();
        let f = infer_format_or_err(path, OcelFormat::infer)?;
        controlled(ctl, || {
            import_ocel(OcelSource::Path(path), f, &options, ctl)
        })
    }

    fn import_from_bytes_with(
        bytes: &[u8],
        format: &str,
        options: Self::ImportOptions,
        ctl: &Control<'_>,
    ) -> Result<Self, Self::Error> {
        let f = OcelFormat::parse(format)?;
        let total = Some(bytes.len() as u64);
        controlled(ctl, || {
            import_ocel(OcelSource::reader(bytes, ctl, total), f, &options, ctl)
        })
    }

    fn known_import_formats() -> Vec<ExtensionWithMime> {
        OcelFormat::known()
    }
}

impl Exportable for OCEL {
    type Error = OCELIOError;
    type ExportOptions = OCELExportOptions;

    fn infer_format(path: &Path) -> Option<String> {
        OcelFormat::infer(path).map(OcelFormat::name)
    }

    fn export_to_path_as<P: AsRef<Path>>(
        &self,
        path: P,
        format: &str,
        options: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        export_ocel(
            self,
            OcelTarget::Path(path.as_ref()),
            OcelFormat::parse(format)?,
            &options,
        )
    }

    fn export_to_writer_with_options<W: Write>(
        &self,
        mut writer: W,
        format: &str,
        options: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        export_ocel(
            self,
            OcelTarget::Writer(&mut writer),
            OcelFormat::parse(format)?,
            &options,
        )
    }

    fn known_export_formats() -> Vec<ExtensionWithMime> {
        OcelFormat::known()
    }
}

#[cfg(any(feature = "ocel-sqlite", feature = "ocel-duckdb"))]
/// Rows between two progress reports (and cancel checks) of a [`RowProgress`].
const ROWS_PER_REPORT: u64 = 4096;

#[cfg(any(feature = "ocel-sqlite", feature = "ocel-duckdb"))]
/// Counts the rows a table-based importer read as progress of stage `"Importing <what>"`,
/// reporting and checking for a cancel once per [`ROWS_PER_REPORT`] rows.
pub(crate) struct RowProgress<'c, 'a> {
    ctl: &'c Control<'a>,
    stage: Cow<'static, str>,
    done: u64,
    total: Option<u64>,
}

#[cfg(any(feature = "ocel-sqlite", feature = "ocel-duckdb"))]
impl<'c, 'a> RowProgress<'c, 'a> {
    /// `total` runs only when someone listens for progress.
    pub(crate) fn new(
        ctl: &'c Control<'a>,
        what: &str,
        total: impl FnOnce() -> Option<u64>,
    ) -> Self {
        let stage: Cow<'static, str> = format!("{IMPORT_STAGE} {what}").into();
        let total = if ctl.wants_progress() { total() } else { None };
        ctl.progress(stage.clone(), 0, total);
        Self {
            ctl,
            stage,
            done: 0,
            total,
        }
    }

    /// Count one row.
    pub(crate) fn row(&mut self) -> Result<(), Cancelled> {
        self.done += 1;
        if self.done.is_multiple_of(ROWS_PER_REPORT) {
            self.ctl.check()?;
            self.ctl.progress(self.stage.clone(), self.done, self.total);
        }
        Ok(())
    }

    /// Report the final count.
    pub(crate) fn finish(&self) {
        self.ctl.progress(self.stage.clone(), self.done, self.total);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    use super::*;
    use crate::test_utils::get_test_data_path;
    use crate::utils::control::Progress;

    fn reports(
        import: impl FnOnce(&Control<'_>) -> Result<OCEL, OCELIOError>,
    ) -> (OCEL, Vec<Progress>) {
        let seen = Mutex::new(Vec::new());
        let ctl = Control::new().on_progress(|p| seen.lock().unwrap().push(p));
        let ocel = import(&ctl).unwrap();
        drop(ctl);
        (ocel, seen.into_inner().unwrap())
    }

    fn assert_counts_bytes_up_to(progress: &[Progress], len: u64, total: Option<u64>) {
        assert!(progress.len() > 2, "{progress:?}");
        assert!(progress
            .iter()
            .all(|p| p.stage == "Importing" && p.total == total));
        assert!(progress.windows(2).all(|w| w[0].done < w[1].done));
        assert_eq!(progress.first().unwrap().done, 0);
        assert_eq!(progress.last().unwrap().done, len);
    }

    #[test]
    fn path_and_slice_imports_report_bytes_up_to_the_size() {
        for name in ["order-management.json", "order-management.xml"] {
            let path = get_test_data_path().join("ocel").join(name);
            let len = std::fs::metadata(&path).unwrap().len();
            let plain = OCEL::import_from_path(&path).unwrap();

            let (ocel, progress) =
                reports(|ctl| OCEL::import_from_path_with(&path, Default::default(), ctl));
            assert_eq!(ocel, plain);
            assert_counts_bytes_up_to(&progress, len, Some(len));

            let bytes = std::fs::read(&path).unwrap();
            let format = <OCEL as Importable>::infer_format(&path).unwrap();
            let (ocel, progress) = reports(|ctl| {
                OCEL::import_from_bytes_with(&bytes, &format, Default::default(), ctl)
            });
            assert_eq!(ocel, plain);
            assert_counts_bytes_up_to(&progress, len, Some(len));
        }
    }

    /// A flush on the gzip encoder would write a sync block and flush the sink a second time.
    #[test]
    fn gz_export_flushes_the_sink_once() {
        #[derive(Default)]
        struct Sink {
            bytes: Vec<u8>,
            flushes: usize,
        }
        impl Write for Sink {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.bytes.extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                self.flushes += 1;
                Ok(())
            }
        }
        let ocel = OCEL {
            event_types: Vec::new(),
            object_types: Vec::new(),
            events: Vec::new(),
            objects: Vec::new(),
        };
        let slim = crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL::from_ocel(
            ocel.clone(),
        );
        for format in ["json.gz", "xml.gz", "ocel.csv.gz"] {
            let mut sink = Sink::default();
            ocel.export_to_writer(&mut sink, format).unwrap();
            assert_eq!(sink.flushes, 1, "{format}");
            assert_eq!(OCEL::import_from_bytes(&sink.bytes, format).unwrap(), ocel);

            let mut sink = Sink::default();
            slim.export_to_writer(&mut sink, format).unwrap();
            assert_eq!(sink.flushes, 1, "slim {format}");
        }
    }

    #[test]
    fn importers_warn_through_the_control() {
        fn warnings(import: impl FnOnce(&Control<'_>)) -> Vec<String> {
            let seen = Mutex::new(Vec::new());
            let ctl = Control::new().on_warning(|w| seen.lock().unwrap().push(w.to_string()));
            import(&ctl);
            drop(ctl);
            seen.into_inner().unwrap()
        }
        let csv = b"id,activity,timestamp,ot:order\ne1,place,2020-01-01T00:00:00Z,o1\ne2,pay,,o1\n";
        let seen = warnings(|ctl| {
            OCEL::import_from_bytes_with(csv, "ocel.csv", Default::default(), ctl).unwrap();
        });
        assert!(
            seen.iter().any(|w| w.contains("missing timestamp")),
            "{seen:?}"
        );

        let json =
            br#"{"eventTypes": [{"name": "t", "attributes": [{"name": "a", "type": "string"}]}],
            "objectTypes": [], "objects": [],
            "events": [{"id": "e1", "type": "t", "time": "2020-01-01T00:00:00Z",
                "attributes": [{"name": "b", "value": "x"}]}]}"#;
        let seen = warnings(|ctl| {
            crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL::import_from_bytes_with(
                json, "json", Default::default(), ctl,
            )
            .unwrap();
        });
        assert!(seen.iter().any(|w| w.contains("auto-growing")), "{seen:?}");
    }

    #[test]
    fn cancel_from_progress_stops_the_import() {
        let path = get_test_data_path()
            .join("ocel")
            .join("order-management.xml");
        let flag = AtomicBool::new(false);
        let last = Mutex::new(0);
        let ctl = Control::new()
            .on_progress(|p| {
                *last.lock().unwrap() = p.done;
                if p.done > 0 {
                    flag.store(true, Ordering::Relaxed);
                }
            })
            .cancel_flag(&flag);
        assert!(matches!(
            OCEL::import_from_path_with(&path, Default::default(), &ctl),
            Err(OCELIOError::Cancelled)
        ));
        assert!(*last.lock().unwrap() < std::fs::metadata(&path).unwrap().len());
    }

    #[cfg(feature = "ocel-sqlite")]
    #[test]
    fn sqlite_import_reports_rows_per_table_kind() {
        let path = get_test_data_path()
            .join("ocel")
            .join("order-management.sqlite");
        let (ocel, progress) =
            reports(|ctl| OCEL::import_from_path_with(&path, Default::default(), ctl));
        let last = |stage: &str| {
            progress
                .iter()
                .rfind(|p| p.stage == format!("Importing {stage}"))
                .unwrap_or_else(|| panic!("no {stage} in {progress:?}"))
                .clone()
        };
        let events = last("events");
        assert_eq!(events.done, ocel.events.len() as u64);
        assert_eq!(events.total, Some(events.done));
        let objects = last("objects");
        assert_eq!(objects.total, Some(ocel.objects.len() as u64));
        let e2o = last("event-object relations");
        let relations: usize = ocel.events.iter().map(|e| e.relationships.len()).sum();
        assert_eq!(e2o.done, relations as u64);
        assert_eq!(e2o.total, Some(e2o.done));
        last("object-object relations");
        last("object changes");

        let flag = AtomicBool::new(false);
        let ctl = Control::new()
            .on_progress(|p| {
                if p.stage == "Importing events" && p.done > 0 {
                    flag.store(true, Ordering::Relaxed);
                }
            })
            .cancel_flag(&flag);
        assert!(matches!(
            OCEL::import_from_path_with(&path, Default::default(), &ctl),
            Err(OCELIOError::Cancelled)
        ));
    }

    #[cfg(feature = "ocel-duckdb")]
    #[test]
    fn duckdb_import_reports_rows_and_cancels() {
        let ocel = OCEL::import_from_path(
            get_test_data_path()
                .join("ocel")
                .join("ContainerLogistics.json"),
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.duckdb");
        ocel_sql::export_ocel_duckdb_to_path(&ocel, &path).unwrap();
        let (imported, progress) =
            reports(|ctl| OCEL::import_from_path_with(&path, Default::default(), ctl));
        let events = progress
            .iter()
            .rfind(|p| p.stage == "Importing events")
            .unwrap();
        assert_eq!(events.done, imported.events.len() as u64);
        assert_eq!(events.total, Some(events.done));

        let flag = AtomicBool::new(false);
        let ctl = Control::new()
            .on_progress(|p| {
                if p.stage == "Importing objects" {
                    flag.store(true, Ordering::Relaxed);
                }
            })
            .cancel_flag(&flag);
        assert!(matches!(
            OCEL::import_from_path_with(&path, Default::default(), &ctl),
            Err(OCELIOError::Cancelled)
        ));
    }
}
