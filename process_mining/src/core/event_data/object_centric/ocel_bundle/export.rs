//! Writing an OCEL as a bundled container.
//!
//! An object's attributes are time-versioned, and the format splits them across two tables: the
//! object table holds the values in force from the Unix epoch, and the object-change table holds
//! every later observation, one row per changed attribute. The split is therefore by timestamp,
//! and lossless in both directions.
//!
//! An object whose earliest observation is after the epoch has no initial value, and its cell is
//! left empty rather than back-dating the first observation.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::Path;

use super::meta::{
    columns, encode_type_name, epoch, AttributeDecl, AttributeType, BundleMeta, EventTypeDecl,
    ObjectTypeDecl, RelationFiles, StorageFormat, Value, BUNDLE_FORMAT_VERSION, META_FILE_NAME,
    OCEL_VERSION,
};
use crate::core::event_data::object_centric::readable::ReadableOCEL;
use crate::core::event_data::object_centric::{
    OCELAttributeType, OCELAttributeValue, OCELTypeAttribute,
};

/// Whether a container is one file or a tree of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ContainerLayout {
    /// A `.ocel.zip` archive.
    #[default]
    Archive,
    /// A directory with the archive's internal layout.
    Directory,
}

/// How to write a container.
#[derive(Debug, Clone, Copy, Default)]
pub struct BundleExportOptions {
    /// One file or a directory.
    pub layout: ContainerLayout,
    /// Which physical storage the tables use.
    pub storage: StorageFormat,
}

/// Why writing a container failed.
#[derive(Debug)]
pub enum BundleExportError {
    /// Writing to the target failed.
    Io(std::io::Error),
    /// Assembling the archive failed.
    Archive(String),
    /// Encoding a table failed.
    Encode(String),
    /// A declared attribute name collides with a column the format fixes.
    ReservedAttribute {
        /// The type declaring it.
        type_name: String,
        /// The attribute name, which is also a `ocel_`-prefixed fixed column name.
        attribute: String,
    },
    /// Parquet storage was asked for in a build without the `ocel-bundle-parquet` feature.
    ParquetUnavailable,
}

impl std::fmt::Display for BundleExportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BundleExportError::Io(e) => write!(f, "writing the container failed: {e}"),
            BundleExportError::Archive(m) => write!(f, "assembling the archive failed: {m}"),
            BundleExportError::Encode(m) => write!(f, "encoding a table failed: {m}"),
            BundleExportError::ReservedAttribute {
                type_name,
                attribute,
            } => write!(
                f,
                "'{type_name}' declares an attribute named '{attribute}', which is a column name the format reserves"
            ),
            BundleExportError::ParquetUnavailable => write!(
                f,
                "Parquet storage needs the 'ocel-bundle-parquet' feature; this build has only CSV"
            ),
        }
    }
}

impl std::error::Error for BundleExportError {}

impl From<std::io::Error> for BundleExportError {
    fn from(e: std::io::Error) -> Self {
        BundleExportError::Io(e)
    }
}

/// Write `ocel` to `path` as a bundled container.
///
/// # Errors
/// See [`BundleExportError`].
pub fn export_ocel_bundle<O, P>(
    ocel: &O,
    path: P,
    options: BundleExportOptions,
) -> Result<(), BundleExportError>
where
    O: ReadableOCEL + ?Sized,
    P: AsRef<Path>,
{
    #[cfg(not(feature = "ocel-bundle-parquet"))]
    if options.storage == StorageFormat::Parquet {
        return Err(BundleExportError::ParquetUnavailable);
    }

    let files = build(ocel, options.storage)?;
    match options.layout {
        ContainerLayout::Directory => {
            for (rel, bytes) in files {
                let out = path.as_ref().join(&rel);
                if let Some(parent) = out.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(out, bytes)?;
            }
            Ok(())
        }
        ContainerLayout::Archive => {
            let file = std::fs::File::create(path)?;
            write_archive(std::io::BufWriter::new(file), files, options.storage)
        }
    }
}

/// Write the archive form to a writer, for a caller with no path (an HTTP response, say).
///
/// # Errors
/// See [`BundleExportError`].
pub fn write_ocel_bundle_archive<O, W>(
    ocel: &O,
    writer: W,
    storage: StorageFormat,
) -> Result<(), BundleExportError>
where
    O: ReadableOCEL + ?Sized,
    W: Write + std::io::Seek,
{
    #[cfg(not(feature = "ocel-bundle-parquet"))]
    if storage == StorageFormat::Parquet {
        return Err(BundleExportError::ParquetUnavailable);
    }
    write_archive(writer, build(ocel, storage)?, storage)
}

fn write_archive<W: Write + std::io::Seek>(
    writer: W,
    files: Vec<(String, Vec<u8>)>,
    storage: StorageFormat,
) -> Result<(), BundleExportError> {
    let mut zw = zip::ZipWriter::new(writer);
    // Parquet entries are stored rather than deflated so a reader can seek within them without
    // expanding the archive: a Parquet file is read footer-first, and a deflated entry has to be
    // fully inflated before that footer can be found. That costs nothing in size only because
    // `writer_properties` compresses each column chunk with ZSTD, so deflating the entry on top
    // would buy almost nothing and give up the seeking. CSV is read start to finish either way,
    // so it is deflated.
    let table_method = match storage {
        StorageFormat::Csv => zip::CompressionMethod::Deflated,
        StorageFormat::Parquet => zip::CompressionMethod::Stored,
    };
    for (rel, bytes) in files {
        let method = if rel == META_FILE_NAME {
            zip::CompressionMethod::Deflated
        } else {
            table_method
        };
        let opts: zip::write::FileOptions<'_, ()> =
            zip::write::FileOptions::default().compression_method(method);
        zw.start_file(&rel, opts)
            .map_err(|e| BundleExportError::Archive(e.to_string()))?;
        zw.write_all(&bytes)?;
    }
    zw.finish()
        .map_err(|e| BundleExportError::Archive(e.to_string()))?;
    Ok(())
}

/// Every file the container holds, as `(path inside the container, contents)`.
fn build<O: ReadableOCEL + ?Sized>(
    ocel: &O,
    storage: StorageFormat,
) -> Result<Vec<(String, Vec<u8>)>, BundleExportError> {
    let ext = storage.extension();
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    let mut meta = BundleMeta {
        ocel_version: OCEL_VERSION.to_string(),
        bundle_format_version: BUNDLE_FORMAT_VERSION.to_string(),
        storage_format: storage,
        event_types: BTreeMap::new(),
        object_types: BTreeMap::new(),
        relations: RelationFiles {
            e2o: format!("relations/e2o.{ext}"),
            o2o: format!("relations/o2o.{ext}"),
        },
    };

    let mut e2o = Table::new(
        &[columns::EVENT_ID, columns::OBJECT_ID, columns::QUALIFIER],
        &[],
    );
    for ev in ocel.iter_events() {
        for rel in &ev.relationships {
            e2o.push(vec![
                text(&ev.id),
                text(&rel.object_id),
                text(&rel.qualifier),
            ]);
        }
    }
    files.push((meta.relations.e2o.clone(), e2o.encode(storage)?));

    let mut o2o = Table::new(
        &[columns::SOURCE_ID, columns::TARGET_ID, columns::QUALIFIER],
        &[],
    );
    for ob in ocel.iter_objects() {
        for rel in &ob.relationships {
            o2o.push(vec![
                text(&ob.id),
                text(&rel.object_id),
                text(&rel.qualifier),
            ]);
        }
    }
    files.push((meta.relations.o2o.clone(), o2o.encode(storage)?));

    // One pass per entity kind, not one per declared type.
    //
    // `ReadableOCEL::iter_events_of_type`/`iter_objects_of_type` default to filtering a full
    // scan, and `OCEL` does not override them, so reading them once per type made the export
    // O(types x entities), i.e. one complete pass per type. The tables are built up front and
    // every entity is routed to its own in a single pass instead.
    let mut event_tables: BTreeMap<String, (Table, Vec<AttributeDecl>)> = BTreeMap::new();
    for ty in ocel.event_types() {
        let attrs = declared(&ty.name, &ty.attributes)?;
        event_tables.insert(ty.name.clone(), (event_table(&attrs), attrs));
    }
    for ev in ocel.iter_events() {
        // An entity of a type the log never declared still has to be written: the relation tables
        // cover every entity, so skipping it would leave relation rows pointing at nothing.
        if !event_tables.contains_key(ev.event_type.as_str()) {
            event_tables.insert(ev.event_type.clone(), (event_table(&[]), Vec::new()));
        }
        let (table, attrs) = event_tables
            .get_mut(ev.event_type.as_str())
            .expect("just inserted");
        let mut row = vec![text(&ev.id), Some(Value::Time(ev.time))];
        for a in attrs.iter() {
            let value = ev
                .attributes
                .iter()
                .find(|x| x.name == a.name)
                .map(|x| &x.value);
            row.push(cell(value, a.value_type));
        }
        table.push(row);
    }
    for (ty, (table, attributes)) in event_tables {
        let file = format!("events/event_{}.{ext}", encode_type_name(&ty));
        files.push((file.clone(), table.encode(storage)?));
        meta.event_types
            .insert(ty, EventTypeDecl { file, attributes });
    }

    let mut object_tables: BTreeMap<String, (Table, Table, Vec<AttributeDecl>)> = BTreeMap::new();
    for ty in ocel.object_types() {
        let attrs = declared(&ty.name, &ty.attributes)?;
        let (objects, changes) = object_and_change_tables(&attrs);
        object_tables.insert(ty.name.clone(), (objects, changes, attrs));
    }
    for ob in ocel.iter_objects() {
        if !object_tables.contains_key(ob.object_type.as_str()) {
            let (objects, changes) = object_and_change_tables(&[]);
            object_tables.insert(ob.object_type.clone(), (objects, changes, Vec::new()));
        }
        let (objects, changes, attrs) = object_tables
            .get_mut(ob.object_type.as_str())
            .expect("just inserted");

        // The object table holds one value per attribute, so only the first observation at the
        // epoch goes there. A second one at the same instant is routed to the change table.
        let mut in_object_row: BTreeSet<&str> = BTreeSet::new();
        let mut later = Vec::new();
        for obs in &ob.attributes {
            if obs.time == epoch() && in_object_row.insert(obs.name.as_str()) {
                continue;
            }
            later.push(obs);
        }

        let mut row = vec![text(&ob.id)];
        for a in attrs.iter() {
            let initial = ob
                .attributes
                .iter()
                .find(|x| x.name == a.name && x.time == epoch())
                .map(|x| &x.value);
            row.push(cell(initial, a.value_type));
        }
        objects.push(row);

        // One row per remaining observation, naming the attribute it changed and leaving the
        // other attribute columns empty. Reading a change row's other columns as values would
        // record changes the log never had.
        later.sort_by(|a, b| a.time.cmp(&b.time).then_with(|| a.name.cmp(&b.name)));
        for obs in later {
            let Some(pos) = attrs.iter().position(|a| a.name == obs.name) else {
                continue;
            };
            let mut row = vec![text(&ob.id), Some(Value::Time(obs.time)), text(&obs.name)];
            for (i, a) in attrs.iter().enumerate() {
                row.push(if i == pos {
                    cell(Some(&obs.value), a.value_type)
                } else {
                    None
                });
            }
            changes.push(row);
        }
    }
    for (ty, (objects, changes, attributes)) in object_tables {
        let enc = encode_type_name(&ty);
        let file = format!("objects/object_{enc}.{ext}");
        let changes_file = format!("object_changes/object_changes_{enc}.{ext}");
        files.push((file.clone(), objects.encode(storage)?));
        files.push((changes_file.clone(), changes.encode(storage)?));
        meta.object_types.insert(
            ty,
            ObjectTypeDecl {
                file,
                changes_file: Some(changes_file),
                attributes,
            },
        );
    }

    let manifest =
        serde_json::to_vec_pretty(&meta).map_err(|e| BundleExportError::Encode(e.to_string()))?;
    files.push((META_FILE_NAME.to_string(), manifest));
    Ok(files)
}

/// The manifest's attribute list for one type, deduplicated and ordered so two exports of the
/// same log are byte-identical.
///
/// # Errors
/// An attribute named after a column the format fixes would give the table two columns of that
/// name, so it is refused rather than written.
fn declared(
    type_name: &str,
    attrs: &[OCELTypeAttribute],
) -> Result<Vec<AttributeDecl>, BundleExportError> {
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    let mut out = Vec::new();
    for a in attrs {
        if is_fixed_column(&a.name) {
            return Err(BundleExportError::ReservedAttribute {
                type_name: type_name.to_string(),
                attribute: a.name.clone(),
            });
        }
        if !seen.insert(a.name.as_str()) {
            continue;
        }
        out.push(AttributeDecl {
            name: a.name.clone(),
            // `from_type_str` maps anything it does not know onto `Null`, which is not an OCEL
            // type; the format has no such variant, so it is written as `string`.
            value_type: match OCELAttributeType::from_type_str(&a.value_type) {
                OCELAttributeType::String | OCELAttributeType::Null => AttributeType::String,
                OCELAttributeType::Time => AttributeType::Time,
                OCELAttributeType::Integer => AttributeType::Integer,
                OCELAttributeType::Float => AttributeType::Float,
                OCELAttributeType::Boolean => AttributeType::Boolean,
            },
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// Whether `name` is a column name the format fixes.
fn is_fixed_column(name: &str) -> bool {
    matches!(
        name,
        columns::ID
            | columns::TIME
            | columns::CHANGED_FIELD
            | columns::EVENT_ID
            | columns::OBJECT_ID
            | columns::SOURCE_ID
            | columns::TARGET_ID
            | columns::QUALIFIER
    )
}

fn event_table(attrs: &[AttributeDecl]) -> Table {
    let mut table = Table::new(&[columns::ID, columns::TIME], attrs);
    table.set_time_column(1);
    table
}

fn object_and_change_tables(attrs: &[AttributeDecl]) -> (Table, Table) {
    let objects = Table::new(&[columns::ID], attrs);
    let mut changes = Table::new(&[columns::ID, columns::TIME, columns::CHANGED_FIELD], attrs);
    changes.set_time_column(1);
    (objects, changes)
}

fn text(value: &str) -> Option<Value> {
    Some(Value::Text(value.to_string()))
}

/// `value` rendered as `declared`, or `None` for a cell the log has no value for.
///
/// A value that does not fit its declared type falls back to its own text, which CSV storage
/// carries fine. Parquet storage writes null rather than a value of the wrong physical type.
fn cell(value: Option<&OCELAttributeValue>, declared: AttributeType) -> Option<Value> {
    let value = value?;
    Some(match (value, declared) {
        (OCELAttributeValue::Null, _) => return None,
        (OCELAttributeValue::Integer(i), AttributeType::Integer) => Value::Integer(*i),
        (OCELAttributeValue::Integer(i), AttributeType::Float) => Value::Float(*i as f64),
        (OCELAttributeValue::Float(f), AttributeType::Float) => Value::Float(*f),
        (OCELAttributeValue::Boolean(b), AttributeType::Boolean) => Value::Boolean(*b),
        (OCELAttributeValue::Time(t), AttributeType::Time) => Value::Time(*t),
        (v, _) => Value::Text(v.to_string()),
    })
}

/// A table being assembled, held column-wise at encode time because Parquet is written that way.
struct Table {
    header: Vec<String>,
    rows: Vec<Vec<Option<Value>>>,
    /// How many leading columns the format fixes, and so `required` in Parquet storage. Counted
    /// when the header is built, so an attribute named like a fixed column stays optional.
    #[cfg_attr(not(feature = "ocel-bundle-parquet"), allow(dead_code))]
    fixed: usize,
    /// Index of the `ocel_time` column, when the table has one.
    time_column: Option<usize>,
}

impl Table {
    fn new(fixed: &[&str], attrs: &[AttributeDecl]) -> Self {
        Self {
            header: fixed
                .iter()
                .map(|s| (*s).to_string())
                .chain(attrs.iter().map(|a| a.name.clone()))
                .collect(),
            rows: Vec::new(),
            fixed: fixed.len(),
            time_column: None,
        }
    }

    fn set_time_column(&mut self, index: usize) {
        self.time_column = Some(index);
    }

    fn push(&mut self, row: Vec<Option<Value>>) {
        debug_assert_eq!(row.len(), self.header.len());
        self.rows.push(row);
    }

    fn encode(&self, storage: StorageFormat) -> Result<Vec<u8>, BundleExportError> {
        match storage {
            StorageFormat::Csv => self.to_csv(),
            #[cfg(feature = "ocel-bundle-parquet")]
            StorageFormat::Parquet => self.to_parquet(),
            #[cfg(not(feature = "ocel-bundle-parquet"))]
            StorageFormat::Parquet => Err(BundleExportError::ParquetUnavailable),
        }
    }

    fn to_csv(&self) -> Result<Vec<u8>, BundleExportError> {
        let mut w = csv::Writer::from_writer(Vec::new());
        w.write_record(&self.header)
            .map_err(|e| BundleExportError::Encode(e.to_string()))?;
        for row in &self.rows {
            w.write_record(
                row.iter()
                    .map(|c| c.as_ref().map(Value::to_string).unwrap_or_default()),
            )
            .map_err(|e| BundleExportError::Encode(e.to_string()))?;
        }
        w.into_inner()
            .map_err(|e| BundleExportError::Encode(e.to_string()))
    }

    #[cfg(feature = "ocel-bundle-parquet")]
    fn to_parquet(&self) -> Result<Vec<u8>, BundleExportError> {
        use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType};
        use parquet::data_type::{BoolType, ByteArray, ByteArrayType, DoubleType, Int64Type};
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type;
        use std::sync::Arc;

        let fixed = self.fixed;
        // Built through the schema API rather than by formatting a `message { ... }` string:
        // an attribute name is arbitrary log data, and one containing a space (or a brace, or a
        // semicolon) does not survive the text parser.
        let mut fields = Vec::with_capacity(self.header.len());
        for (i, name) in self.header.iter().enumerate() {
            let repetition = if i < fixed {
                Repetition::REQUIRED
            } else {
                Repetition::OPTIONAL
            };
            let (physical, logical) = match self.column_kind(i) {
                ColumnKind::Time => (
                    PhysicalType::INT64,
                    Some(LogicalType::Timestamp {
                        is_adjusted_to_u_t_c: true,
                        unit: self.time_unit(i),
                    }),
                ),
                ColumnKind::Integer => (PhysicalType::INT64, None),
                ColumnKind::Float => (PhysicalType::DOUBLE, None),
                ColumnKind::Boolean => (PhysicalType::BOOLEAN, None),
                ColumnKind::Text => (PhysicalType::BYTE_ARRAY, Some(LogicalType::String)),
            };
            let field = Type::primitive_type_builder(name, physical)
                .with_repetition(repetition)
                .with_logical_type(logical)
                .build()
                .map_err(|e| BundleExportError::Encode(e.to_string()))?;
            fields.push(Arc::new(field));
        }
        let schema = Arc::new(
            Type::group_type_builder("row")
                .with_fields(fields)
                .build()
                .map_err(|e| BundleExportError::Encode(e.to_string()))?,
        );

        let mut out = Vec::new();
        let mut writer = SerializedFileWriter::new(&mut out, schema, Arc::new(writer_properties()))
            .map_err(|e| BundleExportError::Encode(e.to_string()))?;
        // A table with no rows is a row group with no rows; the header alone is the schema, so
        // an empty change table still declares its columns.
        {
            let mut group = writer
                .next_row_group()
                .map_err(|e| BundleExportError::Encode(e.to_string()))?;
            for i in 0..self.header.len() {
                let mut col = group
                    .next_column()
                    .map_err(|e| BundleExportError::Encode(e.to_string()))?
                    .ok_or_else(|| {
                        BundleExportError::Encode("fewer columns than the schema".to_string())
                    })?;
                let optional = i >= fixed;
                let name = self.header[i].as_str();
                match self.column_kind(i) {
                    ColumnKind::Time => {
                        let unit = self.time_unit(i);
                        write_column::<Int64Type, _>(
                            &mut col,
                            &self.rows,
                            i,
                            optional,
                            name,
                            |c| match c {
                                Some(Value::Time(t)) => match unit {
                                    TimeUnit::NANOS => t.timestamp_nanos_opt(),
                                    _ => Some(t.timestamp_micros()),
                                },
                                _ => None,
                            },
                        )?;
                    }
                    ColumnKind::Integer => {
                        write_column::<Int64Type, _>(
                            &mut col,
                            &self.rows,
                            i,
                            optional,
                            name,
                            |c| match c {
                                Some(Value::Integer(v)) => Some(*v),
                                _ => None,
                            },
                        )?;
                    }
                    ColumnKind::Float => {
                        write_column::<DoubleType, _>(
                            &mut col,
                            &self.rows,
                            i,
                            optional,
                            name,
                            |c| match c {
                                Some(Value::Float(v)) => Some(*v),
                                Some(Value::Integer(v)) => Some(*v as f64),
                                _ => None,
                            },
                        )?;
                    }
                    ColumnKind::Boolean => {
                        write_column::<BoolType, _>(
                            &mut col,
                            &self.rows,
                            i,
                            optional,
                            name,
                            |c| match c {
                                Some(Value::Boolean(v)) => Some(*v),
                                _ => None,
                            },
                        )?;
                    }
                    // Text is the one kind that holds every value: a cell of another type is
                    // written as the text it renders to.
                    ColumnKind::Text => {
                        write_column::<ByteArrayType, _>(
                            &mut col,
                            &self.rows,
                            i,
                            optional,
                            name,
                            |c| c.as_ref().map(|v| ByteArray::from(v.to_string().as_str())),
                        )?;
                    }
                }
                col.close()
                    .map_err(|e| BundleExportError::Encode(e.to_string()))?;
            }
            group
                .close()
                .map_err(|e| BundleExportError::Encode(e.to_string()))?;
        }
        writer
            .close()
            .map_err(|e| BundleExportError::Encode(e.to_string()))?;
        Ok(out)
    }

    /// The physical type column `i` is written as. Taken from the cells rather than from the
    /// manifest so a fixed column (always text, except `ocel_time`) needs no special case, and
    /// an all-null attribute column still gets a type.
    #[cfg(feature = "ocel-bundle-parquet")]
    fn column_kind(&self, i: usize) -> ColumnKind {
        if self.time_column == Some(i) {
            return ColumnKind::Time;
        }
        for row in &self.rows {
            match row[i] {
                None => continue,
                Some(Value::Integer(_)) => return ColumnKind::Integer,
                Some(Value::Float(_)) => return ColumnKind::Float,
                Some(Value::Boolean(_)) => return ColumnKind::Boolean,
                Some(Value::Time(_)) => return ColumnKind::Time,
                Some(Value::Text(_)) => return ColumnKind::Text,
            }
        }
        ColumnKind::Text
    }

    /// The unit a timestamp column is written in.
    ///
    /// Nanoseconds, so a sub-microsecond instant survives. `timestamp_nanos_opt` covers only
    /// 1677..=2262, and the unit belongs to the column rather than the cell, so one instant
    /// outside that range puts the whole column back on microseconds.
    #[cfg(feature = "ocel-bundle-parquet")]
    fn time_unit(&self, i: usize) -> parquet::basic::TimeUnit {
        use parquet::basic::TimeUnit;
        let representable = self.rows.iter().all(|r| match &r[i] {
            Some(Value::Time(t)) => t.timestamp_nanos_opt().is_some(),
            _ => true,
        });
        if representable {
            TimeUnit::NANOS
        } else {
            TimeUnit::MICROS
        }
    }
}

/// How every table in a Parquet container is written.
///
/// `parquet`'s writers default to no block compression, on the reasoning that a reader
/// streaming from object storage would rather have decode throughput. A container is a file
/// someone keeps and sends, so the tradeoff runs the other way: without this an exported
/// container came out several times larger than the same log as CSV, which is the opposite of
/// what choosing Parquet is for. ZSTD is what `parquet`'s own docs recommend for ratio,
/// speed and ecosystem support together.
#[cfg(feature = "ocel-bundle-parquet")]
fn writer_properties() -> parquet::file::properties::WriterProperties {
    use parquet::basic::{Compression, ZstdLevel};
    parquet::file::properties::WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build()
}

#[cfg(feature = "ocel-bundle-parquet")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnKind {
    Text,
    Integer,
    Float,
    Boolean,
    Time,
}

/// Write column `i` of `rows`, taking its values and its definition levels from the same `of`.
///
/// A cell whose type is not the column's has no representation in it. `of` returning `None`
/// writes it as a null, rather than as a zero of the column's type standing in for it.
#[cfg(feature = "ocel-bundle-parquet")]
fn write_column<T, F>(
    col: &mut parquet::file::writer::SerializedColumnWriter<'_>,
    rows: &[Vec<Option<Value>>],
    i: usize,
    optional: bool,
    name: &str,
    of: F,
) -> Result<(), BundleExportError>
where
    T: parquet::data_type::DataType,
    F: Fn(&Option<Value>) -> Option<T::T>,
{
    let mut values = Vec::with_capacity(rows.len());
    let mut defs = Vec::with_capacity(rows.len());
    for row in rows {
        match of(&row[i]) {
            Some(v) => {
                values.push(v);
                defs.push(1);
            }
            None => defs.push(0),
        }
    }
    if !optional && values.len() != rows.len() {
        return Err(BundleExportError::Encode(format!(
            "'{name}' is a column the format fixes and cannot hold a null"
        )));
    }
    col.typed::<T>()
        .write_batch(&values, optional.then_some(&defs[..]), None)
        .map_err(|e| BundleExportError::Encode(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decl(name: &str, value_type: AttributeType) -> AttributeDecl {
        AttributeDecl {
            name: name.to_string(),
            value_type,
        }
    }

    #[test]
    fn a_declared_attribute_list_is_deduplicated_and_ordered() {
        let attrs = vec![
            OCELTypeAttribute::new("b", &OCELAttributeType::Integer),
            OCELTypeAttribute::new("a", &OCELAttributeType::String),
            OCELTypeAttribute::new("b", &OCELAttributeType::Float),
        ];
        let out = declared("order", &attrs).expect("no reserved name");
        assert_eq!(out.len(), 2, "the repeat is dropped");
        assert_eq!(out[0].name, "a");
        assert_eq!(out[1].name, "b");
        assert_eq!(out[1].value_type, AttributeType::Integer, "first wins");
    }

    #[test]
    fn an_attribute_named_after_a_fixed_column_is_refused() {
        let attrs = vec![OCELTypeAttribute::new(
            columns::ID,
            &OCELAttributeType::String,
        )];
        let err = declared("order", &attrs).expect_err("reserved");
        assert!(err.to_string().contains(columns::ID), "{err}");
    }

    #[test]
    fn a_value_that_does_not_fit_its_declared_type_falls_back_to_text() {
        let c = cell(
            Some(&OCELAttributeValue::String("not a number".to_string())),
            AttributeType::Integer,
        );
        assert!(matches!(c, Some(Value::Text(_))), "{c:?}");
        assert_eq!(c.expect("some").to_string(), "not a number");
    }

    #[test]
    fn an_absent_value_is_an_empty_cell() {
        assert!(cell(None, AttributeType::String).is_none());
        assert!(cell(Some(&OCELAttributeValue::Null), AttributeType::Integer).is_none());
    }

    /// Fixed-ness is a position, not a name.
    #[test]
    fn fixed_columns_are_counted_when_the_header_is_built() {
        let t = Table::new(
            &[columns::ID, columns::TIME],
            &[
                decl("resource", AttributeType::String),
                decl("ocel_like_name", AttributeType::String),
            ],
        );
        assert_eq!(t.fixed, 2);
        assert_eq!(t.header.len(), 4);
    }
}
