use std::borrow::Cow;
use std::io::{Read, Write};
use std::path::Path;

use quick_xml::{escape::resolve_predefined_entity, events::BytesRef};
use serde::{Deserialize, Serialize};

use crate::utils::control::{Cancelled, Control};

/// Try to infer file format from a path
///
/// Handles cases like test.file-abc.xes.gz -> xes.gz
pub fn infer_format_from_path(path: &Path) -> Option<String> {
    let path_str = path.to_string_lossy().to_lowercase();
    if path_str.ends_with(".gz") {
        return match Path::new(&path_str)
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

/// Unescape an XML attribute value, keeping it as is if it has an unknown or malformed reference.
pub(crate) fn read_xml_text_unescaped(s: &str) -> String {
    match quick_xml::escape::unescape(s) {
        Ok(unescaped) => unescaped.into_owned(),
        Err(_) => s.to_string(),
    }
}

/// Append what a reference in XML text stands for to `text`, or the reference itself if it is
/// unknown or malformed.
pub(crate) fn push_xml_general_ref(text: &mut String, r: &BytesRef<'_>) {
    match (r.resolve_char_ref(), resolve_predefined_entity(r)) {
        (Ok(Some(c)), _) => text.push(c),
        (Ok(None), Some(s)) => text.push_str(s),
        _ => {
            text.push('&');
            text.push_str(r);
            text.push(';');
        }
    }
}

/// `s` without the characters XML 1.0 does not allow (most C0 controls, `U+FFFE` and `U+FFFF`),
/// which no XML parser reads back, not even escaped as character references.
pub(crate) fn xml_chars(s: &str) -> Cow<'_, str> {
    let invalid = |c: char| {
        matches!(c, '\0'..='\u{8}' | '\u{B}' | '\u{C}' | '\u{E}'..='\u{1F}')
            || matches!(c, '\u{FFFE}' | '\u{FFFF}')
    };
    if s.contains(invalid) {
        Cow::Owned(s.chars().filter(|c| !invalid(*c)).collect())
    } else {
        Cow::Borrowed(s)
    }
}

/// Assert that `xml` is well-formed and has no character [`xml_chars`] drops
#[cfg(test)]
pub(crate) fn assert_xml_well_formed(xml: &[u8]) {
    let text = std::str::from_utf8(xml).unwrap();
    assert_eq!(xml_chars(text), text);
    let mut reader = quick_xml::Reader::from_reader(xml);
    let mut buf = Vec::new();
    while !matches!(
        reader.read_event_into(&mut buf).unwrap(),
        quick_xml::events::Event::Eof
    ) {
        buf.clear();
    }
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
    /// Construct Extension with MIME Type
    pub fn new(extension: impl Into<String>, mime: impl Into<String>) -> Self {
        Self {
            extension: extension.into(),
            mime: mime.into(),
        }
    }
}

/// Trait for importing types from a file path or reader
///
/// Implemented by the event data and model types, e.g., [`EventLog`](crate::EventLog),
/// [`OCEL`](crate::OCEL) and [`AcceptingPetriNet`](crate::AcceptingPetriNet). A format is named
/// by its extension (`xes`, `xes.gz`, `json`, `pnml`, ...), see
/// [`Importable::known_import_formats`].
///
/// ```
/// use process_mining::{event_log, EventLog, Exportable, Importable};
///
/// let log = event_log!(["a", "b"], ["a", "c"]);
/// let bytes = log.export_to_bytes("xes")?;
/// let imported = EventLog::import_from_bytes(&bytes, "xes")?;
/// assert_eq!(imported.traces.len(), 2);
/// # Ok::<(), process_mining::core::event_data::case_centric::io::EventLogIOError>(())
/// ```
pub trait Importable: Sized {
    /// The error type returned by import operations
    type Error: std::error::Error + Send + Sync + 'static + From<std::io::Error> + From<Cancelled>;

    /// Import options
    type ImportOptions: Default;

    /// Import from a reader in `format` with `options`, reporting progress and warnings to
    /// `ctl` and stopping once it is cancelled.
    ///
    /// Reports only what the importer knows (e.g., the rows of each `SQLite` table). The bytes
    /// read are reported by [`Self::import_from_path_with`] and [`Self::import_from_bytes_with`],
    /// which know the total.
    fn import_from_reader_with<R: Read>(
        reader: R,
        format: &str,
        options: Self::ImportOptions,
        ctl: &Control<'_>,
    ) -> Result<Self, Self::Error>;

    /// Import from a reader, specifying the format.
    fn import_from_reader<R: Read>(reader: R, format: &str) -> Result<Self, Self::Error> {
        Self::import_from_reader_with(
            reader,
            format,
            Self::ImportOptions::default(),
            &Control::new(),
        )
    }

    /// Import from a file path, inferring the format from it, with `options`.
    ///
    /// Reports stage `"Importing"` to `ctl`, where `done` counts the bytes read (compressed bytes
    /// for a `.gz`) out of the file size, plus whatever the importer reports. Fails with
    /// `Cancelled` once `ctl` is cancelled.
    fn import_from_path_with<P: AsRef<Path>>(
        path: P,
        options: Self::ImportOptions,
        ctl: &Control<'_>,
    ) -> Result<Self, Self::Error> {
        let path = path.as_ref();
        let format = infer_format_or_err(path, Self::infer_format)?;
        controlled(ctl, || import_file_with(path, &format, options, ctl))
    }

    /// Import from a file path, inferring the format from it.
    fn import_from_path<P: AsRef<Path>>(path: P) -> Result<Self, Self::Error> {
        Self::import_from_path_with(path, Self::ImportOptions::default(), &Control::new())
    }

    /// Import from a byte slice in `format` with `options`, reporting to `ctl` as
    /// [`Self::import_from_path_with`] does, out of `bytes.len()` bytes.
    fn import_from_bytes_with(
        bytes: &[u8],
        format: &str,
        options: Self::ImportOptions,
        ctl: &Control<'_>,
    ) -> Result<Self, Self::Error> {
        controlled(ctl, || {
            let reader = ProgressReader::new(bytes, ctl, IMPORT_STAGE, Some(bytes.len() as u64));
            Self::import_from_reader_with(std::io::BufReader::new(reader), format, options, ctl)
        })
    }

    /// Import from a byte slice, specifying the format.
    fn import_from_bytes(bytes: &[u8], format: &str) -> Result<Self, Self::Error> {
        Self::import_from_bytes_with(
            bytes,
            format,
            Self::ImportOptions::default(),
            &Control::new(),
        )
    }

    /// Infer format from path. Can be overridden for complex extensions.
    fn infer_format(path: &Path) -> Option<String> {
        infer_format_from_path(path)
    }

    /// Get known import formats
    ///
    /// This function can be used to suggest import formats or construct file chooser filters
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

    /// Export to a file path with export options, in the format inferred from the path. See
    /// [`Self::export_to_path_as`] for an explicit format.
    fn export_to_path_with_options<P: AsRef<Path>>(
        &self,
        path: P,
        options: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        let path = path.as_ref();
        let format = infer_format_or_err(path, Self::infer_format)?;
        self.export_to_path_as(path, &format, options)
    }

    /// Export to a file path, in the format inferred from the path. See
    /// [`Self::export_to_path_as`] for an explicit format.
    fn export_to_path<P: AsRef<Path>>(&self, path: P) -> Result<(), Self::Error> {
        self.export_to_path_with_options(path, Self::ExportOptions::default())
    }

    /// Export to `path` in an explicitly named format, rather than one read back off the path.
    ///
    /// Exists because a path cannot always carry the format: a directory has no extension, and
    /// the OCEL 2.0 bundled format's uncompressed form is a directory. A caller that already
    /// knows which format the user chose, for instance one picked from
    /// [`Self::known_export_formats`], should use this rather than encoding the choice into a
    /// filename and having it inferred straight back out.
    ///
    /// # Errors
    /// As [`Self::export_to_writer_with_options`], plus any I/O error creating `path`. The file
    /// is only created on the first write, so an export that fails before, e.g., on an
    /// unsupported format, leaves an existing file as it was. A failed export removes the file if
    /// it did not exist before.
    fn export_to_path_as<P: AsRef<Path>>(
        &self,
        path: P,
        format: &str,
        options: Self::ExportOptions,
    ) -> Result<(), Self::Error> {
        write_to_path(path.as_ref(), |file| {
            self.export_to_writer_with_options(file, format, options)
        })
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
    /// This function can be used to suggest exports formats or construct file chooser filters
    fn known_export_formats() -> Vec<ExtensionWithMime>;
}

/// The file at `path`, created (or truncated) only on the first write, flush or seek.
pub(crate) struct LazyFile<'p> {
    path: &'p Path,
    file: Option<std::io::BufWriter<std::fs::File>>,
}

impl LazyFile<'_> {
    fn file(&mut self) -> std::io::Result<&mut std::io::BufWriter<std::fs::File>> {
        if self.file.is_none() {
            let file = std::fs::File::create(self.path)?;
            self.file = Some(std::io::BufWriter::new(file));
        }
        Ok(self.file.as_mut().expect("just created"))
    }
}

impl Write for LazyFile<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file()?.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file()?.flush()
    }
}

impl std::io::Seek for LazyFile<'_> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.file()?.seek(pos)
    }
}

/// Run `write` on the [`LazyFile`] at `path` and flush it.
///
/// If that fails, the file is removed, but only if it did not exist before: `path` may name an
/// existing file or a device such as `/dev/full`.
pub(crate) fn write_to_path<E: From<std::io::Error>>(
    path: &Path,
    write: impl FnOnce(&mut LazyFile<'_>) -> Result<(), E>,
) -> Result<(), E> {
    let existed = path.exists();
    let mut file = LazyFile { path, file: None };
    // A write error held back by the buffer is otherwise dropped with it
    let written = write(&mut file).and_then(|()| file.flush().map_err(E::from));
    if written.is_err() && !existed && file.file.is_some() {
        drop(file);
        let _ = std::fs::remove_file(path);
    }
    written
}

/// Bytes between two progress reports of a [`ProgressReader`].
const BYTES_PER_REPORT: u64 = 1 << 20;

/// Progress stage of the bytes an import reads.
pub(crate) const IMPORT_STAGE: &str = "Importing";

/// Reports the bytes read from `inner` as progress, at most once per [`BYTES_PER_REPORT`] and
/// at the end or when dropped, and fails reading once the [`Control`] is cancelled.
pub(crate) struct ProgressReader<'c, 'a, R> {
    inner: R,
    ctl: &'c Control<'a>,
    stage: Cow<'static, str>,
    done: u64,
    reported: u64,
    total: Option<u64>,
}

impl<'c, 'a, R> ProgressReader<'c, 'a, R> {
    pub(crate) fn new(
        inner: R,
        ctl: &'c Control<'a>,
        stage: impl Into<Cow<'static, str>>,
        total: Option<u64>,
    ) -> Self {
        let stage = stage.into();
        ctl.progress(stage.clone(), 0, total);
        Self {
            inner,
            ctl,
            stage,
            done: 0,
            reported: 0,
            total,
        }
    }
}

impl<R> ProgressReader<'_, '_, R> {
    fn report(&mut self) {
        self.reported = self.done;
        self.ctl.progress(self.stage.clone(), self.done, self.total);
    }
}

// A parser may stop at the end of the document without reading to EOF.
impl<R> Drop for ProgressReader<'_, '_, R> {
    fn drop(&mut self) {
        if self.done > self.reported {
            self.report();
        }
    }
}

impl<R: Read> Read for ProgressReader<'_, '_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.ctl.is_cancelled() {
            return Err(std::io::Error::other(Cancelled));
        }
        let n = self.inner.read(buf)?;
        self.done += n as u64;
        if self.done - self.reported >= BYTES_PER_REPORT || (n == 0 && self.done > self.reported) {
            self.report();
        }
        Ok(n)
    }
}

/// The format `infer` reads off `path`, or an [`std::io::ErrorKind::InvalidInput`] error.
pub(crate) fn infer_format_or_err<T>(
    path: &Path,
    infer: impl FnOnce(&Path) -> Option<T>,
) -> std::io::Result<T> {
    infer(path).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Could not infer format from path {path:?}"),
        )
    })
}

/// Run `import` under `ctl`, failing with [`Cancelled`] when it was cancelled before or by the
/// end. A cancelled [`ProgressReader`] fails with an I/O error, which this turns into the cancel.
pub(crate) fn controlled<T, E: From<Cancelled>>(
    ctl: &Control<'_>,
    import: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    ctl.check()?;
    let result = import();
    ctl.check()?;
    result
}

/// [`Importable::import_from_reader_with`] on the file at `path`, counting its bytes as
/// progress. For an importer that overrides [`Importable::import_from_path_with`] but reads
/// some formats as a byte stream.
pub(crate) fn import_file_with<T: Importable>(
    path: &Path,
    format: &str,
    options: T::ImportOptions,
    ctl: &Control<'_>,
) -> Result<T, T::Error> {
    let file = std::fs::File::open(path)?;
    let total = file.metadata()?.len();
    let reader = ProgressReader::new(file, ctl, IMPORT_STAGE, Some(total));
    T::import_from_reader_with(std::io::BufReader::new(reader), format, options, ctl)
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
        let x = infer_format_from_path("Log.XES.GZ".as_ref());
        assert_eq!(x, Some("xes.gz".to_string()));
    }

    #[test]
    fn test_stuff() {
        let log = crate::EventLog::default();
        let mut bytes = Vec::new();
        log.export_to_writer(&mut bytes, ".xes.gz").unwrap();
    }

    /// A `BufWriter` handed over by value holds the whole small export, so the write to the
    /// failing sink only happens in the final flush.
    #[test]
    fn writer_by_value_reports_failing_final_flush() {
        struct Failing;
        impl std::io::Write for Failing {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("disk full"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let sink = || std::io::BufWriter::new(Failing);

        let log = crate::EventLog::default();
        for format in ["xes", "xes.gz", "json"] {
            assert!(log.export_to_writer(sink(), format).is_err(), "{format}");
        }
        let ocel = crate::OCEL {
            event_types: Vec::new(),
            object_types: Vec::new(),
            events: Vec::new(),
            objects: Vec::new(),
        };
        for format in ["json", "json.gz", "xml", "ocel.csv"] {
            assert!(ocel.export_to_writer(sink(), format).is_err(), "{format}");
        }
        let slim = crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL::new();
        assert!(slim.export_to_writer(sink(), "json").is_err());
        let net = crate::PetriNet::new();
        assert!(net.export_to_writer(sink(), "pnml").is_err());
    }

    #[test]
    fn failed_export_to_path_leaves_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let log = crate::EventLog::default();
        let path = dir.path().join("log.txt");
        assert!(log.export_to_path(&path).is_err());
        assert!(!path.exists());

        let projection: crate::core::event_data::case_centric::utils::activity_projection::EventLogActivityProjection = (&log).into();
        let path = dir.path().join("projection.xes");
        assert!(projection.export_to_path(&path).is_err());
        assert!(!path.exists());
    }

    #[test]
    fn an_unsupported_format_leaves_an_existing_file_as_it_was() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keep.txt");
        let ocel = crate::OCEL::default();
        let slim = crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL::new();
        let formats = ["nope", "nope.gz", "duckdb.gz"];
        let results = |format| {
            [
                crate::EventLog::default()
                    .export_to_path_as(&path, format, ())
                    .is_err(),
                ocel.export_to_path_as(&path, format, Default::default())
                    .is_err(),
                slim.export_to_path_as(&path, format, Default::default())
                    .is_err(),
                crate::PetriNet::new()
                    .export_to_path_as(&path, format, ())
                    .is_err(),
            ]
        };
        for format in formats {
            std::fs::write(&path, "keep").unwrap();
            assert_eq!(results(format), [true; 4], "{format}");
            assert_eq!(std::fs::read_to_string(&path).unwrap(), "keep", "{format}");
        }
    }

    #[test]
    fn a_failed_write_removes_only_a_file_it_created() {
        struct FailsHalfWay;
        impl Exportable for FailsHalfWay {
            type Error = std::io::Error;
            type ExportOptions = ();
            fn export_to_writer_with_options<W: std::io::Write>(
                &self,
                mut writer: W,
                _: &str,
                _: Self::ExportOptions,
            ) -> Result<(), Self::Error> {
                writer.write_all(b"partial")?;
                writer.flush()?;
                Err(std::io::Error::other("failed half way"))
            }
            fn known_export_formats() -> Vec<ExtensionWithMime> {
                vec![]
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new.out");
        assert!(FailsHalfWay.export_to_path_as(&path, "out", ()).is_err());
        assert!(!path.exists());

        std::fs::write(&path, "old").unwrap();
        assert!(FailsHalfWay.export_to_path_as(&path, "out", ()).is_err());
        assert!(path.exists());
    }

    #[test]
    fn test_no_import_options() {
        #[derive(Debug)]
        struct E;
        impl std::fmt::Display for E {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("e")
            }
        }
        impl std::error::Error for E {}
        impl From<std::io::Error> for E {
            fn from(_: std::io::Error) -> Self {
                E
            }
        }
        impl From<crate::Cancelled> for E {
            fn from(_: crate::Cancelled) -> Self {
                E
            }
        }
        struct X;
        impl Importable for X {
            type Error = E;
            type ImportOptions = ();
            fn import_from_reader_with<R: std::io::Read>(
                _: R,
                _: &str,
                _: Self::ImportOptions,
                _: &crate::Control<'_>,
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
