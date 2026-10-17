//! Reading a bundled container into an [`OCEL`].
//!
//! This reads the layout directly instead of going through the extraction engine, keeping
//! `.ocel.zip` an ordinary [`Importable`](crate::core::io::Importable) format alongside
//! `.ocel.csv` and `.jsonocel`, readable with no connector and on `wasm32`. The
//! [`Blueprint`](super::blueprint::blueprint_for) route reads the same layout through the
//! extraction engine, for treating a container as a data source in the blueprint editor. Both
//! share this module's [`meta`](super::meta).
//!
//! A directory is read a table at a time straight from disk. An archive is first expanded into a
//! temporary directory, one entry at a time, since a ZIP entry is only seekable when stored
//! uncompressed. Peak memory stays flat either way; an archive additionally costs disk for its
//! expanded size, freed when the [`Container`] drops.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, FixedOffset};

use super::meta::{
    columns, epoch, type_attributes, BundleMeta, StorageFormat, Value, BUNDLE_FORMAT_VERSION,
    META_FILE_NAME, OCEL_VERSION,
};
use crate::core::event_data::object_centric::appendable::AppendableOCEL;
use crate::core::event_data::object_centric::io::OCELIOError;
use crate::core::event_data::object_centric::{
    OCELAttributeType, OCELAttributeValue, OCELEventAttribute, OCELObjectAttribute,
    OCELRelationship, OCELType, OCEL,
};
use crate::core::event_data::timestamp_utils::parse_timestamp;

/// Why reading a container failed.
#[derive(Debug)]
pub enum BundleImportError {
    /// The container could not be read: a missing path, an unreadable archive, or one with no
    /// [`META_FILE_NAME`].
    Container(String),
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
    /// A table the manifest declares is missing, unreadable, or lacks a column the format fixes.
    Table {
        /// The declared path inside the container.
        file: String,
        /// What went wrong.
        detail: String,
    },
    /// Parquet storage in a build without the `ocel-bundle-parquet` feature.
    ParquetUnavailable,
}

impl std::fmt::Display for BundleImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BundleImportError::Container(m) => write!(f, "reading the container failed: {m}"),
            BundleImportError::Manifest(e) => write!(f, "reading {META_FILE_NAME} failed: {e}"),
            BundleImportError::Version {
                field,
                found,
                implemented,
            } => write!(
                f,
                "the container declares {field} '{found}', which this build does not implement: it reads '{implemented}'"
            ),
            BundleImportError::Table { file, detail } => {
                write!(f, "reading '{file}' failed: {detail}")
            }
            BundleImportError::ParquetUnavailable => write!(
                f,
                "this container uses Parquet storage, which needs the 'ocel-bundle-parquet' feature"
            ),
        }
    }
}

impl std::error::Error for BundleImportError {}

impl From<std::io::Error> for BundleImportError {
    fn from(e: std::io::Error) -> Self {
        BundleImportError::Container(e.to_string())
    }
}

fn container_err(e: impl std::fmt::Display) -> BundleImportError {
    BundleImportError::Container(e.to_string())
}

/// What one entry of an archive may expand to, and what a whole archive may.
///
/// A compressed entry can expand by orders of magnitude, so neither its declared size nor the
/// archive's own bounds what expanding it costs. Both limits are far above any real container.
const MAX_ENTRY_BYTES: u64 = 4 << 30;
const MAX_TOTAL_BYTES: u64 = 16 << 30;

/// Copy one archive entry, refusing to write past the limits above. `total` carries the running
/// count across the entries of one archive.
fn copy_entry<R: io::Read, W: io::Write>(
    entry: &mut R,
    sink: &mut W,
    name: &str,
    total: &mut u64,
) -> Result<(), BundleImportError> {
    let allowed = MAX_ENTRY_BYTES.min(MAX_TOTAL_BYTES - *total);
    // One byte past the limit, so a copy that fills it exactly is told apart from an overrun.
    let written = io::copy(&mut io::Read::take(entry, allowed + 1), sink)?;
    if written > allowed {
        return Err(BundleImportError::Container(format!(
            "'{name}' expands past what a container may hold ({MAX_ENTRY_BYTES} bytes per entry, {MAX_TOTAL_BYTES} in total)"
        )));
    }
    *total += written;
    Ok(())
}

/// An opened container: its manifest, and where each declared file's bytes are.
#[derive(Debug)]
pub struct Container {
    meta: BundleMeta,
    files: Files,
}

#[derive(Debug)]
enum Files {
    /// Files on disk under this root. `_temp` is present only for an expanded archive, and
    /// removes it when the container drops.
    Dir {
        root: PathBuf,
        _temp: Option<tempfile::TempDir>,
    },
    /// Entry contents, for an archive with no path to expand beside.
    Memory(HashMap<String, Vec<u8>>),
}

impl Container {
    /// Open a directory or a `.ocel.zip` archive.
    ///
    /// # Errors
    /// See [`BundleImportError`].
    pub fn open(path: impl AsRef<Path>) -> Result<Self, BundleImportError> {
        let path = path.as_ref();
        // The manifest is the only file in an uncompressed container a person can point at, so
        // selecting it means the directory it sits in. A file dialog cannot pick a directory on
        // every platform, which makes this the practical way to open one.
        let path = if path.file_name().is_some_and(|n| n == META_FILE_NAME) {
            // A bare `ocel-meta.json` has an empty parent rather than none.
            match path.parent() {
                Some(p) if !p.as_os_str().is_empty() => p,
                _ => Path::new("."),
            }
        } else {
            path
        };
        if path.is_dir() {
            let manifest = path.join(META_FILE_NAME);
            let meta = read_manifest(
                &std::fs::read(&manifest)
                    .map_err(|e| container_err(format!("{}: {e}", manifest.display())))?,
            )?;
            return Ok(Self {
                meta,
                files: Files::Dir {
                    root: path.to_path_buf(),
                    _temp: None,
                },
            });
        }

        let file = std::fs::File::open(path)
            .map_err(|e| container_err(format!("{}: {e}", path.display())))?;
        let mut archive = zip::ZipArchive::new(io::BufReader::new(file)).map_err(container_err)?;
        let temp = tempfile::tempdir().map_err(container_err)?;
        let root = temp.path().to_path_buf();

        let mut total = 0;
        for i in 0..archive.len() {
            let mut entry = archive.by_index(i).map_err(container_err)?;
            // `enclosed_name` rejects an entry that would escape the directory. An archive is
            // untrusted input, and `../` in an entry name is the standard way to abuse one.
            let Some(rel) = entry.enclosed_name() else {
                continue;
            };
            if entry.is_dir() {
                continue;
            }
            let out = root.join(&rel);
            if let Some(parent) = out.parent() {
                std::fs::create_dir_all(parent)?;
            }
            // Copied a block at a time, so the peak is a buffer rather than a whole entry.
            let mut sink = io::BufWriter::new(std::fs::File::create(&out)?);
            copy_entry(&mut entry, &mut sink, &rel.to_string_lossy(), &mut total)?;
        }

        let meta =
            read_manifest(&std::fs::read(root.join(META_FILE_NAME)).map_err(|e| {
                container_err(format!("the archive has no {META_FILE_NAME}: {e}"))
            })?)?;
        Ok(Self {
            meta,
            files: Files::Dir {
                root,
                _temp: Some(temp),
            },
        })
    }

    /// Open an archive already in memory, for contents with no path such as a browser upload.
    /// Holds every table in memory. Prefer [`Container::open`] where there is a path.
    ///
    /// # Errors
    /// See [`BundleImportError`].
    pub fn open_bytes(bytes: &[u8]) -> Result<Self, BundleImportError> {
        let mut archive = zip::ZipArchive::new(io::Cursor::new(bytes)).map_err(container_err)?;
        let mut files: HashMap<String, Vec<u8>> = HashMap::new();
        let mut total = 0;
        for i in 0..archive.len() {
            let mut entry = archive.by_index(i).map_err(container_err)?;
            let Some(rel) = entry.enclosed_name() else {
                continue;
            };
            if entry.is_dir() {
                continue;
            }
            let name = rel.to_string_lossy().replace('\\', "/");
            // No capacity hint: `size()` is a header field the archive supplies, so reserving it
            // up front allocates whatever an attacker wrote there before a byte is read.
            let mut buf = Vec::new();
            copy_entry(&mut entry, &mut buf, &name, &mut total)?;
            files.insert(name, buf);
        }
        let meta = read_manifest(
            files
                .get(META_FILE_NAME)
                .ok_or_else(|| container_err(format!("the archive has no {META_FILE_NAME}")))?,
        )?;
        Ok(Self {
            meta,
            files: Files::Memory(files),
        })
    }

    /// What the container declares about itself.
    #[must_use]
    pub fn meta(&self) -> &BundleMeta {
        &self.meta
    }

    /// Read the whole container into an [`OCEL`].
    ///
    /// Prefer [`read_into`](Self::read_into) when the target is not an [`OCEL`]. This holds the
    /// whole log in memory.
    ///
    /// # Errors
    /// See [`BundleImportError`].
    pub fn read(&self) -> Result<OCEL, BundleImportError> {
        let mut ocel = OCEL {
            event_types: Vec::new(),
            object_types: Vec::new(),
            events: Vec::new(),
            objects: Vec::new(),
        };
        self.read_into(&mut ocel).map_err(|e| match e {
            OCELIOError::BundleImport(e) => e,
            // `OCEL`'s sink is `Infallible`, so nothing else reaches here.
            other => BundleImportError::Container(other.to_string()),
        })?;
        Ok(ocel)
    }

    /// Read the whole container into `sink`, holding no intermediate log.
    ///
    /// Relations and attribute changes live in their own tables but are needed when an entity is
    /// appended, so this indexes them first and then streams the entity tables through.
    ///
    /// # Errors
    /// See [`BundleImportError`], plus the sink's own error.
    pub fn read_into<A>(&self, sink: &mut A) -> Result<(), OCELIOError>
    where
        A: AppendableOCEL,
        A::Error: Into<OCELIOError>,
    {
        // Checked before any table is read, so an escaping path is reported as such even when an
        // earlier table is missing.
        for (_, file) in self.meta.tables() {
            if !is_contained(file) {
                return Err(BundleImportError::Table {
                    file: file.to_string(),
                    detail: "a declared path must stay inside the container".to_string(),
                }
                .into());
            }
        }

        // Only the endpoint a relation is stored on is checked. The other is kept as the id it
        // is, which is what an OCEL relationship holds anyway.
        let e2o_file = &self.meta.relations.e2o;
        let mut e2o: HashMap<String, Vec<OCELRelationship>> = HashMap::new();
        for row in self.table(
            e2o_file,
            &[columns::EVENT_ID, columns::OBJECT_ID, columns::QUALIFIER],
        )? {
            e2o.entry(row.text(columns::EVENT_ID, e2o_file)?)
                .or_default()
                .push(OCELRelationship {
                    object_id: row.text(columns::OBJECT_ID, e2o_file)?,
                    qualifier: row.text_or_empty(columns::QUALIFIER),
                });
        }

        let o2o_file = &self.meta.relations.o2o;
        let mut o2o: HashMap<String, Vec<OCELRelationship>> = HashMap::new();
        for row in self.table(
            o2o_file,
            &[columns::SOURCE_ID, columns::TARGET_ID, columns::QUALIFIER],
        )? {
            o2o.entry(row.text(columns::SOURCE_ID, o2o_file)?)
                .or_default()
                .push(OCELRelationship {
                    object_id: row.text(columns::TARGET_ID, o2o_file)?,
                    qualifier: row.text_or_empty(columns::QUALIFIER),
                });
        }

        // `changed_at` keeps the file each id was named in, so a dangling change row can still
        // name where it came from.
        let mut changes: HashMap<String, Vec<OCELObjectAttribute>> = HashMap::new();
        let mut changed_at: HashMap<String, &str> = HashMap::new();
        let mut unknown_fields: Vec<(&str, Dangling)> = Vec::new();
        for decl in self.meta.object_types.values() {
            let Some(file) = &decl.changes_file else {
                continue;
            };
            let mut unknown_field = Dangling::default();
            for row in self.table(file, &[columns::ID, columns::TIME, columns::CHANGED_FIELD])? {
                let id = row.text(columns::ID, file)?;
                let time = row.time(columns::TIME, file)?;
                let name = row.text(columns::CHANGED_FIELD, file)?;
                changed_at.entry(id.clone()).or_insert(file);
                // Only the column `ocel_changed_field` names. The rest of a change row is empty
                // by construction, and reading them would record changes the container never
                // declared.
                let Some(a) = decl.attributes.iter().find(|a| a.name == name) else {
                    unknown_field.note(&name);
                    continue;
                };
                if let Some(value) = row.value(&name, a.value_type.into()) {
                    changes
                        .entry(id)
                        .or_default()
                        .push(OCELObjectAttribute { name, value, time });
                }
            }
            unknown_fields.push((file, unknown_field));
        }

        for (ty, decl) in &self.meta.event_types {
            sink.declare_event_type(OCELType {
                name: ty.clone(),
                attributes: type_attributes(&decl.attributes),
            })
            .map_err(Into::into)?;
        }
        for (ty, decl) in &self.meta.object_types {
            sink.declare_object_type(OCELType {
                name: ty.clone(),
                attributes: type_attributes(&decl.attributes),
            })
            .map_err(Into::into)?;
        }

        for (ty, decl) in &self.meta.event_types {
            for row in self.table(&decl.file, &[columns::ID, columns::TIME])? {
                let id = row.text(columns::ID, &decl.file)?;
                let time = row.time(columns::TIME, &decl.file)?;
                let attributes = decl
                    .attributes
                    .iter()
                    .filter_map(|a| {
                        row.value(&a.name, a.value_type.into())
                            .map(|value| OCELEventAttribute {
                                name: a.name.clone(),
                                value,
                            })
                    })
                    .collect();
                let relationships = e2o.remove(&id).unwrap_or_default();
                sink.append_event(id, ty, time, attributes, relationships)
                    .map_err(Into::into)?;
            }
        }

        for (ty, decl) in &self.meta.object_types {
            for row in self.table(&decl.file, &[columns::ID])? {
                let id = row.text(columns::ID, &decl.file)?;
                // An object table's values are the ones in force from the epoch; a change row
                // adds the later ones.
                let mut attributes: Vec<_> = decl
                    .attributes
                    .iter()
                    .filter_map(|a| {
                        row.value(&a.name, a.value_type.into())
                            .map(|value| OCELObjectAttribute {
                                name: a.name.clone(),
                                value,
                                time: epoch(),
                            })
                    })
                    .collect();
                attributes.extend(changes.remove(&id).unwrap_or_default());
                changed_at.remove(&id);
                let relationships = o2o.remove(&id).unwrap_or_default();
                sink.append_object(id, ty, attributes, relationships)
                    .map_err(Into::into)?;
            }
        }

        // Whatever the indexes still hold named an entity no table declared.
        for (file, unknown_field) in unknown_fields {
            let left: HashSet<&str> = changed_at
                .iter()
                .filter(|(_, from)| **from == file)
                .map(|(id, _)| id.as_str())
                .collect();
            self.report_dangling(
                file,
                columns::ID,
                &left,
                "no object table declares the object",
            )?;
            unknown_field.into_error(file, "the manifest declares no attribute")?;
        }
        self.report_dangling(
            e2o_file,
            columns::EVENT_ID,
            &e2o.keys().map(String::as_str).collect(),
            "no event table declares the event",
        )?;
        self.report_dangling(
            o2o_file,
            columns::SOURCE_ID,
            &o2o.keys().map(String::as_str).collect(),
            "no object table declares the object",
        )?;
        sink.finalize().map_err(Into::into)?;
        Ok(())
    }

    /// Fail on the rows of `file` whose `key_column` is in `left_over`.
    ///
    /// Re-reads the table, on the error path only, so the message still names the first offending
    /// row and counts the rest.
    fn report_dangling(
        &self,
        file: &str,
        key_column: &str,
        left_over: &HashSet<&str>,
        what: &str,
    ) -> Result<(), BundleImportError> {
        if left_over.is_empty() {
            return Ok(());
        }
        let mut dangling = Dangling::default();
        for row in self.table(file, &[key_column])? {
            let id = row.text(key_column, file)?;
            if left_over.contains(id.as_str()) {
                dangling.note(&id);
            }
        }
        dangling.into_error(file, what)
    }

    /// The rows of one declared table, once it is known to carry every column in `fixed`.
    fn table(&self, file: &str, fixed: &[&str]) -> Result<Vec<Row>, BundleImportError> {
        let missing = || BundleImportError::Table {
            file: file.to_string(),
            detail: "no such file in the container".to_string(),
        };
        if !is_contained(file) {
            // A manifest is untrusted input just as an archive's entry names are, and joining a
            // declared path onto the container root is exactly as exploitable.
            return Err(BundleImportError::Table {
                file: file.to_string(),
                detail: "a declared path must stay inside the container".to_string(),
            });
        }
        match (&self.files, self.meta.storage_format) {
            (Files::Dir { root, .. }, StorageFormat::Csv) => {
                let path = root.join(file);
                if !path.is_file() {
                    return Err(missing());
                }
                read_csv(std::fs::File::open(path)?, file, fixed)
            }
            (Files::Memory(files), StorageFormat::Csv) => read_csv(
                io::Cursor::new(files.get(file).ok_or_else(missing)?),
                file,
                fixed,
            ),
            #[cfg(feature = "ocel-bundle-parquet")]
            (Files::Dir { root, .. }, StorageFormat::Parquet) => {
                let path = root.join(file);
                if !path.is_file() {
                    return Err(missing());
                }
                read_parquet(std::fs::read(path)?, file, fixed)
            }
            #[cfg(feature = "ocel-bundle-parquet")]
            (Files::Memory(files), StorageFormat::Parquet) => {
                read_parquet(files.get(file).ok_or_else(missing)?.clone(), file, fixed)
            }
            #[cfg(not(feature = "ocel-bundle-parquet"))]
            (_, StorageFormat::Parquet) => Err(BundleImportError::ParquetUnavailable),
        }
    }
}

/// Read a container at `path`.
///
/// # Errors
/// See [`BundleImportError`].
pub fn import_ocel_bundle(path: impl AsRef<Path>) -> Result<OCEL, BundleImportError> {
    Container::open(path)?.read()
}

/// [`import_ocel_bundle`] for an archive already in memory.
///
/// # Errors
/// See [`BundleImportError`].
pub fn import_ocel_bundle_from_bytes(bytes: &[u8]) -> Result<OCEL, BundleImportError> {
    Container::open_bytes(bytes)?.read()
}

fn read_manifest(bytes: &[u8]) -> Result<BundleMeta, BundleImportError> {
    let meta: BundleMeta = serde_json::from_slice(bytes).map_err(BundleImportError::Manifest)?;
    check_version("ocelVersion", &meta.ocel_version, OCEL_VERSION)?;
    check_version(
        "bundleFormatVersion",
        &meta.bundle_format_version,
        BUNDLE_FORMAT_VERSION,
    )?;
    Ok(meta)
}

/// Whether `found` is a version this build implements. Only the major must match: a later minor
/// revision may only add what a reader can ignore, which is also why the manifest is not
/// `deny_unknown_fields`.
fn check_version(field: &str, found: &str, implemented: &str) -> Result<(), BundleImportError> {
    let major = |v: &str| v.split('.').next().unwrap_or_default().to_string();
    if major(found) == major(implemented) {
        return Ok(());
    }
    Err(BundleImportError::Version {
        field: field.to_string(),
        found: found.to_string(),
        implemented: implemented.to_string(),
    })
}

/// Whether a manifest's declared path stays inside the container. Checked per component rather
/// than by canonicalising, so it holds for an in-memory container with no filesystem to resolve
/// against.
fn is_contained(file: &str) -> bool {
    let path = Path::new(file);
    !path.is_absolute()
        && path.components().all(|c| {
            matches!(
                c,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        })
}

/// Ids a table names that nothing declares, kept as the first offender and a count so one broken
/// container yields one error rather than a flood.
#[derive(Debug, Default)]
struct Dangling {
    first: Option<String>,
    count: usize,
}

impl Dangling {
    fn note(&mut self, name: &str) {
        self.count += 1;
        if self.first.is_none() {
            self.first = Some(name.to_string());
        }
    }

    fn into_error(self, file: &str, what: &str) -> Result<(), BundleImportError> {
        let Some(first) = self.first else {
            return Ok(());
        };
        let more = match self.count {
            1 => String::new(),
            n => format!(", and {} further rows like it", n - 1),
        };
        Err(BundleImportError::Table {
            file: file.to_string(),
            detail: format!("{what} '{first}'{more}"),
        })
    }
}

/// One row, as a map from column name to whatever the storage held. CSV yields only
/// [`Value::Text`], Parquet the file's own types.
#[derive(Debug, Default)]
struct Row {
    cells: HashMap<String, Value>,
}

impl Row {
    /// A fixed column's text. Absent or empty is an error: the format makes these `required`.
    fn text(&self, column: &str, file: &str) -> Result<String, BundleImportError> {
        let text = match self.cells.get(column) {
            Some(Value::Text(s)) => s.clone(),
            Some(other) => other.to_string(),
            None => String::new(),
        };
        if text.is_empty() {
            return Err(BundleImportError::Table {
                file: file.to_string(),
                detail: format!("a row has no '{column}'"),
            });
        }
        Ok(text)
    }

    fn time(&self, column: &str, file: &str) -> Result<DateTime<FixedOffset>, BundleImportError> {
        let err = |detail: String| BundleImportError::Table {
            file: file.to_string(),
            detail,
        };
        match self.cells.get(column) {
            Some(Value::Time(t)) => Ok(*t),
            Some(Value::Text(s)) if !s.is_empty() => parse_timestamp(s, None, false)
                .map_err(|_| err(format!("'{s}' in '{column}' is not a timestamp"))),
            _ => Err(err(format!("a row has no '{column}'"))),
        }
    }

    /// A fixed column's text where the format allows it to be empty. The column's presence is
    /// checked when the table is read, so an absent cell here is an empty one.
    fn text_or_empty(&self, column: &str) -> String {
        self.cells
            .get(column)
            .map(Value::to_string)
            .unwrap_or_default()
    }

    /// An attribute value, or `None` when the cell is absent.
    ///
    /// Absence is decided when the table is read, not here: a CSV reader drops empty cells
    /// because the format says an empty cell is a missing value, while Parquet has a real null
    /// and so keeps an empty string as the value it is.
    fn value(&self, column: &str, declared: OCELAttributeType) -> Option<OCELAttributeValue> {
        Some(coerce(self.cells.get(column)?, declared))
    }
}

/// Read a cell as its declared type. A value that does not parse keeps its text, rather than
/// becoming null: losing it silently would be worse than carrying it in the wrong type, and the
/// manifest is the only thing claiming the type in CSV storage.
fn coerce(raw: &Value, declared: OCELAttributeType) -> OCELAttributeValue {
    match raw {
        Value::Integer(i) => match declared {
            OCELAttributeType::Float => OCELAttributeValue::Float(*i as f64),
            OCELAttributeType::String => OCELAttributeValue::String(i.to_string()),
            _ => OCELAttributeValue::Integer(*i),
        },
        Value::Float(f) => match declared {
            OCELAttributeType::String => OCELAttributeValue::String(f.to_string()),
            _ => OCELAttributeValue::Float(*f),
        },
        Value::Boolean(b) => OCELAttributeValue::Boolean(*b),
        Value::Time(t) => OCELAttributeValue::Time(*t),
        Value::Text(s) => match declared {
            OCELAttributeType::Integer => s.parse::<i64>().map_or_else(
                |_| OCELAttributeValue::String(s.clone()),
                OCELAttributeValue::Integer,
            ),
            OCELAttributeType::Float => s.parse::<f64>().map_or_else(
                |_| OCELAttributeValue::String(s.clone()),
                OCELAttributeValue::Float,
            ),
            OCELAttributeType::Boolean => match s.as_str() {
                "true" => OCELAttributeValue::Boolean(true),
                "false" => OCELAttributeValue::Boolean(false),
                _ => OCELAttributeValue::String(s.clone()),
            },
            OCELAttributeType::Time => parse_timestamp(s, None, false).map_or_else(
                |_| OCELAttributeValue::String(s.clone()),
                OCELAttributeValue::Time,
            ),
            _ => OCELAttributeValue::String(s.clone()),
        },
    }
}

/// Reject a table missing a column the format fixes, so a reader never has to decide between a
/// column that is absent and a cell that is empty.
fn require_columns(
    present: impl Fn(&str) -> bool,
    fixed: &[&str],
    file: &str,
) -> Result<(), BundleImportError> {
    for column in fixed {
        if !present(column) {
            return Err(BundleImportError::Table {
                file: file.to_string(),
                detail: format!("the table has no '{column}' column"),
            });
        }
    }
    Ok(())
}

fn read_csv<R: io::Read>(
    reader: R,
    file: &str,
    fixed: &[&str],
) -> Result<Vec<Row>, BundleImportError> {
    let table_err = |e: csv::Error| BundleImportError::Table {
        file: file.to_string(),
        detail: e.to_string(),
    };
    let mut rdr = csv::Reader::from_reader(reader);
    let header: Vec<String> = rdr
        .headers()
        .map_err(table_err)?
        .iter()
        .map(str::to_string)
        .collect();
    require_columns(|c| header.iter().any(|h| h == c), fixed, file)?;
    let mut rows = Vec::new();
    for record in rdr.records() {
        let record = record.map_err(table_err)?;
        let mut cells = HashMap::with_capacity(header.len());
        for (name, cell) in header.iter().zip(record.iter()) {
            // "missing values are represented by empty cells". CSV has no null, so this is the
            // only reading available, and why CSV storage cannot carry an attribute whose value
            // is the empty string.
            if cell.is_empty() {
                continue;
            }
            cells.insert(name.clone(), Value::Text(cell.to_string()));
        }
        rows.push(Row { cells });
    }
    Ok(rows)
}

#[cfg(feature = "ocel-bundle-parquet")]
fn read_parquet(bytes: Vec<u8>, file: &str, fixed: &[&str]) -> Result<Vec<Row>, BundleImportError> {
    use parquet::basic::{LogicalType, TimeUnit};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::Field;

    let table_err = |e: parquet::errors::ParquetError| BundleImportError::Table {
        file: file.to_string(),
        detail: e.to_string(),
    };
    let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).map_err(table_err)?;
    // Timestamps are physically INT64. The logical type says which unit, so it has to be read
    // from the schema rather than guessed from the value.
    let units: HashMap<String, Option<TimeUnit>> = reader
        .metadata()
        .file_metadata()
        .schema()
        .get_fields()
        .iter()
        .map(|f| {
            let unit = match f.get_basic_info().logical_type_ref() {
                Some(LogicalType::Timestamp { unit, .. }) => Some(*unit),
                _ => None,
            };
            (f.name().to_string(), unit)
        })
        .collect();
    require_columns(|c| units.contains_key(c), fixed, file)?;

    let mut rows = Vec::new();
    for row in reader.get_row_iter(None).map_err(table_err)? {
        let row = row.map_err(table_err)?;
        let mut cells = HashMap::new();
        for (name, field) in row.get_column_iter() {
            let value = match *field {
                Field::Null => continue,
                Field::Bool(b) => Value::Boolean(b),
                Field::Byte(v) => Value::Integer(i64::from(v)),
                Field::Short(v) => Value::Integer(i64::from(v)),
                Field::Int(v) => Value::Integer(i64::from(v)),
                // Physically an INT64; only the schema's logical type says whether it is an
                // instant, and in which unit.
                Field::Long(v) => match units.get(name).copied().flatten() {
                    Some(unit) => micros_of(v, unit).map_or(Value::Integer(v), Value::Time),
                    None => Value::Integer(v),
                },
                Field::UByte(v) => Value::Integer(i64::from(v)),
                Field::UShort(v) => Value::Integer(i64::from(v)),
                Field::UInt(v) => Value::Integer(i64::from(v)),
                Field::ULong(v) => Value::Integer(i64::try_from(v).unwrap_or(i64::MAX)),
                Field::Float(v) => Value::Float(f64::from(v)),
                Field::Double(v) => Value::Float(v),
                Field::TimestampMillis(v) => {
                    micros_of(v, TimeUnit::MILLIS).map_or(Value::Integer(v), Value::Time)
                }
                Field::TimestampMicros(v) => {
                    micros_of(v, TimeUnit::MICROS).map_or(Value::Integer(v), Value::Time)
                }
                Field::Str(ref s) => Value::Text(s.clone()),
                ref other => Value::Text(other.to_string()),
            };
            cells.insert(name.clone(), value);
        }
        rows.push(Row { cells });
    }
    Ok(rows)
}

#[cfg(feature = "ocel-bundle-parquet")]
fn micros_of(value: i64, unit: parquet::basic::TimeUnit) -> Option<DateTime<FixedOffset>> {
    use parquet::basic::TimeUnit;
    let utc = match unit {
        TimeUnit::MILLIS => DateTime::from_timestamp_millis(value)?,
        TimeUnit::MICROS => DateTime::from_timestamp_micros(value)?,
        TimeUnit::NANOS => DateTime::from_timestamp_nanos(value),
    };
    Some(utc.into())
}
