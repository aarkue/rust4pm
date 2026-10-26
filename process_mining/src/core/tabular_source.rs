//! Bytes of a tabular data file, held in the registry so an extraction can read them.

use std::any::Any;
use std::sync::{Mutex, MutexGuard};

use crate::core::io::{ExtensionWithMime, Importable};

/// A tabular data file kept in memory: a `SQLite` database, a CSV, a Parquet file, a workbook.
///
/// Bytes cannot travel through a binding's JSON arguments, so a dropped file is stored here and
/// named by registry id. On `wasm32` this is the only way to read a source at all.
///
/// The opened reader is cached and held as `Box<dyn Any + Send>` so this type stays free of the
/// `extraction-blueprint`/`ocel-sqlite` features. It sits behind a `Mutex` because a `SQLite`
/// connection is `Send` but not `Sync`, while the registry must be `Sync`.
pub struct TabularSource {
    bytes: Vec<u8>,
    format: String,
    opened: Mutex<Option<Box<dyn Any + Send>>>,
}

/// Reports the byte count rather than the bytes: [`RegistryItem`](crate::bindings::RegistryItem)
/// derives `Debug`, so one `{:?}` of the registry would otherwise dump every file it holds.
impl std::fmt::Debug for TabularSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TabularSource")
            .field("format", &self.format)
            .field("bytes", &self.bytes.len())
            .finish()
    }
}

impl TabularSource {
    /// Wrap `bytes` of a file in `format` (an extension: `sqlite`, `csv`, `parquet`, `xlsx`).
    ///
    /// `format` is lowercased, so [`TabularSource::format`] is comparable against a lowercase
    /// literal however the caller spelled the extension.
    #[must_use]
    pub fn new(bytes: Vec<u8>, format: impl Into<String>) -> Self {
        Self {
            bytes,
            format: format.into().to_lowercase(),
            opened: Mutex::new(None),
        }
    }

    /// The cached reader, opening it with `open` the first time.
    ///
    /// The guard is returned rather than a reference: the reader is not `Sync`, so it can only be
    /// touched while the lock is held.
    ///
    /// # Errors
    /// Returns `open`'s error, or a message if a cached reader is not of type `T`. Errors rather
    /// than blocking while another reader of the same source is alive, since a nested call would
    /// otherwise deadlock the thread against itself.
    pub fn reader<T, E, F>(&self, open: F) -> Result<TabularReader<'_, T>, String>
    where
        T: Any + Send,
        E: std::fmt::Display,
        F: FnOnce(&[u8]) -> Result<T, E>,
    {
        let mut guard = self.opened.try_lock().map_err(|e| match e {
            std::sync::TryLockError::WouldBlock => {
                "data source is already open for reading elsewhere".to_string()
            }
            std::sync::TryLockError::Poisoned(_) => "data source lock poisoned".to_string(),
        })?;
        if guard.is_none() {
            *guard = Some(Box::new(open(&self.bytes).map_err(|e| e.to_string())?));
        }
        if guard.as_ref().is_none_or(|b| !b.is::<T>()) {
            return Err("data source was already opened as a different reader".to_string());
        }
        Ok(TabularReader {
            guard,
            _marker: std::marker::PhantomData,
        })
    }

    /// The file's bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The file's format, lowercased.
    #[must_use]
    pub fn format(&self) -> &str {
        &self.format
    }
}

/// A borrowed, opened reader over a [`TabularSource`], valid while the lock is held.
#[allow(missing_debug_implementations)]
pub struct TabularReader<'a, T> {
    guard: MutexGuard<'a, Option<Box<dyn Any + Send>>>,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T: Any + Send> TabularReader<'_, T> {
    /// The opened reader.
    #[must_use]
    pub fn get(&self) -> &T {
        self.guard
            .as_ref()
            .and_then(|b| b.downcast_ref::<T>())
            .expect("reader was checked when the guard was taken")
    }
}

/// What can go wrong while importing a [`TabularSource`].
#[derive(Debug)]
pub enum TabularSourceError {
    /// An I/O error while reading the bytes.
    Io(std::io::Error),
}

impl std::fmt::Display for TabularSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for TabularSourceError {}

impl From<std::io::Error> for TabularSourceError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl Importable for TabularSource {
    type Error = TabularSourceError;
    type ImportOptions = ();

    fn import_from_reader_with_options<R: std::io::Read>(
        mut reader: R,
        data_format: &str,
        _options: Self::ImportOptions,
    ) -> Result<Self, Self::Error> {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Ok(Self::new(bytes, data_format))
    }

    fn known_import_formats() -> Vec<ExtensionWithMime> {
        // Only the formats this build can open, so a file dialog cannot offer an unreadable one.
        vec![
            ExtensionWithMime::new("sqlite", "application/vnd.sqlite3"),
            ExtensionWithMime::new("sqlite3", "application/vnd.sqlite3"),
            ExtensionWithMime::new("db", "application/vnd.sqlite3"),
            #[cfg(feature = "extraction-dbcon")]
            ExtensionWithMime::new("csv", "text/csv"),
            #[cfg(feature = "extraction-dbcon")]
            ExtensionWithMime::new("tsv", "text/tab-separated-values"),
            #[cfg(feature = "extraction-dbcon")]
            ExtensionWithMime::new("parquet", "application/vnd.apache.parquet"),
            #[cfg(feature = "extraction-dbcon")]
            ExtensionWithMime::new(
                "xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_format_is_lowercased_however_it_was_spelled() {
        for spelling in ["SQLite", "SQLITE", "sqlite"] {
            assert_eq!(TabularSource::new(Vec::new(), spelling).format(), "sqlite");
        }
        assert_eq!(
            TabularSource::import_from_bytes(b"a,b\n1,2\n", "CSV")
                .expect("reading from a slice cannot fail")
                .format(),
            "csv"
        );
    }

    #[test]
    fn debug_reports_the_size_rather_than_the_bytes() {
        let rendered = format!("{:?}", TabularSource::new(vec![0xAB; 4096], "sqlite"));
        assert!(
            rendered.contains("4096") && !rendered.contains("171, 171"),
            "a source should render as a summary: {rendered}"
        );
    }
}
