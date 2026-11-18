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
use super::{archive_err, BundleError};
use crate::core::event_data::object_centric::io::OCELIOError;
use crate::core::event_data::object_centric::readable::ReadableOCEL;
use crate::core::event_data::object_centric::{
    OCELAttributeType, OCELAttributeValue, OCELTypeAttribute,
};

/// Whether a container is one file or a tree of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ContainerLayout {
    /// A `.ocel.zip` archive.
    #[default]
    Archive,
    /// A directory with the archive's internal layout.
    Directory,
}

/// How to write a container.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BundleExportOptions {
    /// One file or a directory.
    pub(crate) layout: ContainerLayout,
    /// Which physical storage the tables use.
    pub(crate) storage: StorageFormat,
}

/// Write `ocel` to `path` as a bundled container. Nothing is written when the log cannot be
/// encoded.
pub(crate) fn export_ocel_bundle<O, P>(
    ocel: &O,
    path: P,
    options: BundleExportOptions,
) -> Result<(), OCELIOError>
where
    O: ReadableOCEL + ?Sized,
    P: AsRef<Path>,
{
    #[cfg(not(feature = "ocel-bundle-parquet"))]
    if options.storage == StorageFormat::Parquet {
        return Err(BundleError::ParquetUnavailable.into());
    }

    let files = build(ocel, options.storage)?;
    match options.layout {
        ContainerLayout::Directory => {
            std::fs::create_dir_all(path.as_ref())?;
            for (rel, bytes) in files {
                let out = path.as_ref().join(&rel);
                if let Some(parent) = out.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(out, bytes)?;
            }
            Ok(())
        }
        ContainerLayout::Archive => crate::core::io::write_to_path(path.as_ref(), |file| {
            write_archive(file, files, options.storage)
        }),
    }
}

/// Write the archive form to a writer, for a caller with no path (an HTTP response, say).
pub(crate) fn write_ocel_bundle_archive<O, W>(
    ocel: &O,
    writer: W,
    storage: StorageFormat,
) -> Result<(), OCELIOError>
where
    O: ReadableOCEL + ?Sized,
    W: Write + std::io::Seek,
{
    #[cfg(not(feature = "ocel-bundle-parquet"))]
    if storage == StorageFormat::Parquet {
        return Err(BundleError::ParquetUnavailable.into());
    }
    write_archive(writer, build(ocel, storage)?, storage)
}

fn write_archive<W: Write + std::io::Seek>(
    writer: W,
    files: Vec<(String, Vec<u8>)>,
    storage: StorageFormat,
) -> Result<(), OCELIOError> {
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
        let opts = zip::write::SimpleFileOptions::default().compression_method(method);
        zw.start_file(&rel, opts).map_err(archive_err)?;
        zw.write_all(&bytes)?;
    }
    zw.finish().map_err(archive_err)?.flush()?;
    Ok(())
}

/// Every file the container holds, as `(path inside the container, contents)`.
fn build<O: ReadableOCEL + ?Sized>(
    ocel: &O,
    storage: StorageFormat,
) -> Result<Vec<(String, Vec<u8>)>, OCELIOError> {
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
    files.push((
        meta.relations.e2o.clone(),
        e2o.encode(storage, &meta.relations.e2o)?,
    ));

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
    files.push((
        meta.relations.o2o.clone(),
        o2o.encode(storage, &meta.relations.o2o)?,
    ));

    // One pass per entity kind: `ReadableOCEL::iter_events_of_type`/`iter_objects_of_type`
    // default to filtering a full scan, so reading them per type would scan once per type.
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
        files.push((file.clone(), table.encode(storage, &file)?));
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
        files.push((file.clone(), objects.encode(storage, &file)?));
        files.push((
            changes_file.clone(),
            changes.encode(storage, &changes_file)?,
        ));
        meta.object_types.insert(
            ty,
            ObjectTypeDecl {
                file,
                changes_file: Some(changes_file),
                attributes,
            },
        );
    }

    let manifest = serde_json::to_vec_pretty(&meta).map_err(BundleError::Manifest)?;
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
) -> Result<Vec<AttributeDecl>, BundleError> {
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    let mut out = Vec::new();
    for a in attrs {
        if is_fixed_column(&a.name) {
            return Err(BundleError::ReservedAttribute {
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
/// A value that does not fit its declared type falls back to its own text. Parquet storage then
/// writes the whole column as text (see [`Table::column_kind`]).
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
    /// The declared type of each column after the fixed ones.
    #[cfg_attr(not(feature = "ocel-bundle-parquet"), allow(dead_code))]
    declared: Vec<AttributeType>,
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
            declared: attrs.iter().map(|a| a.value_type).collect(),
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

    /// The table as the contents of `file`.
    fn encode(&self, storage: StorageFormat, file: &str) -> Result<Vec<u8>, OCELIOError> {
        match storage {
            StorageFormat::Csv => self.to_csv(file),
            #[cfg(feature = "ocel-bundle-parquet")]
            StorageFormat::Parquet => Ok(self.to_parquet(file)?),
            #[cfg(not(feature = "ocel-bundle-parquet"))]
            StorageFormat::Parquet => Err(BundleError::ParquetUnavailable.into()),
        }
    }

    fn to_csv(&self, file: &str) -> Result<Vec<u8>, OCELIOError> {
        let err = |source| BundleError::Csv {
            file: file.to_string(),
            source,
        };
        let mut w = csv::Writer::from_writer(Vec::new());
        w.write_record(&self.header).map_err(err)?;
        for row in &self.rows {
            w.write_record(
                row.iter()
                    .map(|c| c.as_ref().map(Value::to_string).unwrap_or_default()),
            )
            .map_err(err)?;
        }
        Ok(w.into_inner().map_err(|e| e.into_error())?)
    }

    #[cfg(feature = "ocel-bundle-parquet")]
    fn to_parquet(&self, file: &str) -> Result<Vec<u8>, BundleError> {
        use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType};
        use parquet::data_type::{BoolType, ByteArray, ByteArrayType, DoubleType, Int64Type};
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type;
        use std::sync::Arc;

        let err = |source| BundleError::Parquet {
            file: file.to_string(),
            source,
        };
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
                    // Microseconds, as OCEL 2.0 prescribes for timestamps (sec. 10). Nanoseconds
                    // would keep sub-microsecond parts, but cover only 1677..=2262, and other
                    // readers expect microseconds.
                    Some(LogicalType::timestamp(true, TimeUnit::MICROS)),
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
                .map_err(err)?;
            fields.push(Arc::new(field));
        }
        let schema = Arc::new(
            Type::group_type_builder("row")
                .with_fields(fields)
                .build()
                .map_err(err)?,
        );

        let mut out = Vec::new();
        let mut writer = SerializedFileWriter::new(&mut out, schema, Arc::new(writer_properties()))
            .map_err(err)?;
        // A table with no rows is a row group with no rows; the header alone is the schema, so
        // an empty change table still declares its columns.
        {
            let mut group = writer.next_row_group().map_err(err)?;
            for i in 0..self.header.len() {
                let mut col =
                    group
                        .next_column()
                        .map_err(err)?
                        .ok_or_else(|| BundleError::Table {
                            file: file.to_string(),
                            detail: "fewer columns than the schema".to_string(),
                        })?;
                let optional = i >= fixed;
                let name = self.header[i].as_str();
                match self.column_kind(i) {
                    ColumnKind::Time => {
                        write_column::<Int64Type, _>(
                            &mut col,
                            &self.rows,
                            i,
                            optional,
                            file,
                            name,
                            |c| match c {
                                Some(Value::Time(t)) => Some(t.timestamp_micros()),
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
                            file,
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
                            file,
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
                            file,
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
                            file,
                            name,
                            |c| c.as_ref().map(|v| ByteArray::from(v.to_string().as_str())),
                        )?;
                    }
                }
                col.close().map_err(err)?;
            }
            group.close().map_err(err)?;
        }
        writer.close().map_err(err)?;
        Ok(out)
    }

    /// The physical type column `i` is written as: that of its declared type, or text if a
    /// value in it does not fit that type, so that no value is written as null.
    #[cfg(feature = "ocel-bundle-parquet")]
    fn column_kind(&self, i: usize) -> ColumnKind {
        if self.time_column == Some(i) {
            return ColumnKind::Time;
        }
        let Some(&declared) = i.checked_sub(self.fixed).and_then(|a| self.declared.get(a)) else {
            return ColumnKind::Text;
        };
        let kind = match declared {
            AttributeType::String => return ColumnKind::Text,
            AttributeType::Time => ColumnKind::Time,
            AttributeType::Integer => ColumnKind::Integer,
            AttributeType::Float => ColumnKind::Float,
            AttributeType::Boolean => ColumnKind::Boolean,
        };
        let fits = |v: &Value| {
            matches!(
                (kind, v),
                (ColumnKind::Time, Value::Time(_))
                    | (ColumnKind::Integer, Value::Integer(_))
                    | (ColumnKind::Float, Value::Float(_) | Value::Integer(_))
                    | (ColumnKind::Boolean, Value::Boolean(_))
            )
        };
        if self.rows.iter().filter_map(|r| r[i].as_ref()).all(fits) {
            kind
        } else {
            ColumnKind::Text
        }
    }
}

/// How every table in a Parquet container is written.
///
/// `parquet`'s writers default to no block compression, which favors decode throughput for a
/// reader streaming from object storage. A container is a file someone keeps and sends, so its
/// tables are compressed with ZSTD, which `parquet`'s docs recommend for ratio, speed and
/// ecosystem support together. Uncompressed, a container is several times larger than the same
/// log as CSV.
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
    file: &str,
    name: &str,
    of: F,
) -> Result<(), BundleError>
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
        return Err(BundleError::Table {
            file: file.to_string(),
            detail: format!("'{name}' is a column the format fixes and cannot hold a null"),
        });
    }
    col.typed::<T>()
        .write_batch(&values, optional.then_some(&defs[..]), None)
        .map_err(|source| BundleError::Parquet {
            file: file.to_string(),
            source,
        })?;
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
