use std::io::{Read, Write};
use std::path::Path;

/// Trait for importing types from a file path or reader
pub trait Importable: Sized {
    /// The error type returned by import operations
    type Error: std::error::Error + Send + Sync + 'static + From<std::io::Error>;

    /// Import from a reader, specifying the format.
    fn import_from_reader<R: Read>(reader: R, format: &str) -> Result<Self, Self::Error>;

    /// Import from a file path, optionally specifying the format.
    /// If format is None, it should be inferred from the file extension.
    fn import_from_path<P: AsRef<Path>>(path: P) -> Result<Self, Self::Error> {
        let path = path.as_ref();
        let format = Self::infer_format(path).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Could not infer format from path",
            )
        })?;

        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        Self::import_from_reader(reader, &format)
    }

    /// Import from a byte slice, specifying the format.
    fn import_from_bytes(bytes: &[u8], format: &str) -> Result<Self, Self::Error> {
        Self::import_from_reader(std::io::Cursor::new(bytes), format)
    }

    /// Infer format from path. Can be overridden for complex extensions (e.g., .xes.gz).
    fn infer_format(path: &Path) -> Option<String> {
        let path_str = path.to_string_lossy().to_lowercase();
        if path_str.ends_with(".xes.gz") {
            return Some("xes.gz".to_string());
        }
        path.extension()
            .and_then(|e| e.to_str())
            .map(|s| s.to_lowercase())
    }
}

/// Trait for exporting types to a file path or writer
pub trait Exportable {
    /// The error type returned by export operations
    type Error: std::error::Error + Send + Sync + 'static + From<std::io::Error>;

    /// Export to a writer, specifying the format.
    fn export_to_writer<W: Write>(&self, writer: W, format: &str) -> Result<(), Self::Error>;

    /// Export to a file path, optionally specifying the format.
    /// If format is None, it should be inferred from the file extension.
    fn export_to_path<P: AsRef<Path>>(&self, path: P) -> Result<(), Self::Error> {
        let path = path.as_ref();
        let format = Self::infer_format(path).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Could not infer format from path",
            )
        })?;

        let file = std::fs::File::create(path)?;
        let writer = std::io::BufWriter::new(file);
        Self::export_to_writer(self, writer, &format)
    }

    /// Infer format from path. Can be overridden for complex extensions.
    fn infer_format(path: &Path) -> Option<String> {
        let path_str = path.to_string_lossy().to_lowercase();
        if path_str.ends_with(".xes.gz") {
            return Some("xes.gz".to_string());
        }
        path.extension()
            .and_then(|e| e.to_str())
            .map(|s| s.to_lowercase())
    }
}

#[test]
fn test_stuff() {
    let log = super::EventLog::default();
    let mut bytes = Vec::new();
    log.export_to_writer(&mut bytes, ".xes.gz").unwrap();
}
