use std::io::{Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Try to infer file format from a path
///
/// Handles cases like test.file-abc.xes.gz -> xes.gz
pub fn infer_format_from_path(path: &Path) -> Option<String> {
    let path_str = path.to_string_lossy().to_lowercase();
    if path_str.ends_with(".gz") {
        return match path
            .file_stem()
            .and_then(|e| e.to_str())
            .and_then(|e| e.rsplit_once('.'))
            .map(|(_name, ext)| ext)
        {
            Some(ext) => Some(format!("{ext}.gz")),
            None => Some("gz".to_string()),
        };
    }
    path.extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_lowercase())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// File Extension and MIME Type
///
/// Example: `xes.gz` (Extension), `application/gzip` (MIME Type)
pub struct ExtensionWithMime {
    /// Extension part (e.g., `xes.gz`)
    pub extension: String,
    /// MIME part (e.g., `application/gzip`)
    pub mime: String,
}
impl ExtensionWithMime {
    /// Construct MIME Type
    pub fn new(extension: impl Into<String>, mime: impl Into<String>) -> Self {
        Self {
            extension: extension.into(),
            mime: mime.into(),
        }
    }
}

/// Trait for importing types from a file path or reader
pub trait Importable: Sized {
    /// The error type returned by import operations
    type Error: std::error::Error + Send + Sync + 'static + From<std::io::Error>;

    /// Import options
    type ImportOptions: Default;

    /// Import from a reader, specifying the format.
    fn import_from_reader<R: Read>(reader: R, data_format: &str) -> Result<Self, Self::Error> {
        Self::import_from_reader_with_options(reader, data_format, Self::ImportOptions::default())
    }

    /// Import from a reader, specifying the format and import options.
    fn import_from_reader_with_options<R: Read>(
        reader: R,
        data_format: &str,
        options: Self::ImportOptions,
    ) -> Result<Self, Self::Error>;

    /// Import from a file path, parsing the format from the file extension.
    fn import_from_path<P: AsRef<Path>>(path: P) -> Result<Self, Self::Error> {
        Self::import_from_path_with_options(path, Self::ImportOptions::default())
    }

    /// Import from a file path with the specified import options, parsing the format from the file extension.
    fn import_from_path_with_options<P: AsRef<Path>>(
        path: P,
        o: Self::ImportOptions,
    ) -> Result<Self, Self::Error> {
        let path = path.as_ref();
        let format = Self::infer_format(path).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Could not infer format from path",
            )
        })?;

        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        Self::import_from_reader_with_options(reader, &format, o)
    }

    /// Import from a byte slice, specifying the format.
    fn import_from_bytes(bytes: &[u8], data_format: &str) -> Result<Self, Self::Error> {
        Self::import_from_bytes_with_options(bytes, data_format, Self::ImportOptions::default())
    }
    /// Import from a byte slice in the given format with specified import options.
    fn import_from_bytes_with_options(
        bytes: &[u8],
        data_format: &str,
        options: Self::ImportOptions,
    ) -> Result<Self, Self::Error> {
        Self::import_from_reader_with_options(std::io::Cursor::new(bytes), data_format, options)
    }

    /// Infer format from path. Can be overridden for complex extensions.
    fn infer_format(path: &Path) -> Option<String> {
        infer_format_from_path(path)
    }

    /// Get known import formats
    ///
    /// This function can be used to suggest import formats or contruct file chooser filters
    fn known_import_formats() -> Vec<ExtensionWithMime>;
}

/// Trait for exporting types to a file path or writer
pub trait Exportable {
    /// The error type returned by export operations
    type Error: std::error::Error + Send + Sync + 'static + From<std::io::Error>;

    /// Export options
    type ExportOptions: Default;

    /// Export to a writer, specifying the format and export options.
    fn export_to_writer_with_options<W: Write>(
        &self,
        writer: W,
        format: &str,
        options: Self::ExportOptions,
    ) -> Result<(), Self::Error>;

    /// Export to a writer, specifying the format.
    fn export_to_writer<W: Write>(&self, writer: W, format: &str) -> Result<(), Self::Error> {
        self.export_to_writer_with_options(writer, format, Self::ExportOptions::default())
    }

    /// Export to a file path, optionally specifying the format and import options.
    fn export_to_path_with_options<P: AsRef<Path>>(
        &self,
        path: P,
        options: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        let path = path.as_ref();
        let format = Self::infer_format(path).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Could not infer format from path",
            )
        })?;

        let file = std::fs::File::create(path)?;
        let writer = std::io::BufWriter::new(file);
        self.export_to_writer_with_options(writer, &format, options)
    }

    /// Export to a file path, optionally specifying the format.
    fn export_to_path<P: AsRef<Path>>(&self, path: P) -> Result<(), Self::Error> {
        self.export_to_path_with_options(path, Self::ExportOptions::default())
    }

    /// Export as a byte array with the specified options
    fn export_to_bytes_with_options(
        &self,
        format: &str,
        options: Self::ExportOptions,
    ) -> Result<Vec<u8>, Self::Error> {
        let mut ret = Vec::new();
        self.export_to_writer_with_options(&mut ret, format, options)?;
        Ok(ret)
    }

    /// Export as a byte array
    fn export_to_bytes(&self, format: &str) -> Result<Vec<u8>, Self::Error> {
        self.export_to_bytes_with_options(format, Self::ExportOptions::default())
    }

    /// Infer format from path.
    fn infer_format(path: &Path) -> Option<String> {
        infer_format_from_path(path)
    }
    /// Get known export formats
    ///
    /// This function can be used to suggest exports formats or contruct file chooser filters
    fn known_export_formats() -> Vec<ExtensionWithMime>;
}

#[cfg(test)]
mod test {
    use super::infer_format_from_path;
    use crate::{core::io::ExtensionWithMime, Exportable, Importable};

    #[test]
    fn test_extension_extraction() {
        let x = infer_format_from_path("test.xes.gz".as_ref());
        assert_eq!(x, Some("xes.gz".to_string()));
        let x = infer_format_from_path("this.is.a.test.json.gz".as_ref());
        assert_eq!(x, Some("json.gz".to_string()));
        let x = infer_format_from_path("this.is.a.test.json".as_ref());
        assert_eq!(x, Some("json".to_string()));
        let x = infer_format_from_path("simple_archive.gz".as_ref());
        assert_eq!(x, Some("gz".to_string()));
    }

    #[test]
    fn test_stuff() {
        let log = crate::EventLog::default();
        let mut bytes = Vec::new();
        log.export_to_writer(&mut bytes, ".xes.gz").unwrap();
    }

    #[test]
    fn test_no_import_options() {
        struct X;
        impl Importable for X {
            type Error = std::io::Error;
            type ImportOptions = ();
            fn import_from_reader_with_options<R: std::io::Read>(
                _: R,
                _: &str,
                _: Self::ImportOptions,
            ) -> Result<Self, Self::Error> {
                Ok(X)
            }

            fn known_import_formats() -> Vec<ExtensionWithMime> {
                vec![]
            }
        }

        X::import_from_bytes(&[], ".test.gz").expect("Input should not matter.");
    }
}
