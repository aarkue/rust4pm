//! CSV Export for OCEL 2.0

use crate::core::event_data::object_centric::{
    ocel_struct::OCELAttributeValue,
    readable::{OCELLookup, ReadableOCEL},
};
use chrono::{DateTime, FixedOffset};
use std::{
    collections::{HashMap, HashSet},
    io::Write,
};

/// Options for CSV OCEL Export
#[derive(Debug, Clone)]
pub struct OCELCSVExportOptions {
    /// Whether to include O2O relationships in the export
    pub include_o2o: bool,
    /// Whether to include object attribute changes as separate rows
    pub include_object_attribute_changes: bool,
    /// Date format for timestamps (default: RFC3339)
    pub date_format: Option<String>,
}

impl Default for OCELCSVExportOptions {
    fn default() -> Self {
        Self {
            include_o2o: true,
            include_object_attribute_changes: true,
            date_format: None,
        }
    }
}

/// Error type for CSV export
#[derive(Debug)]
pub enum OCELCSVExportError {
    /// IO error during writing
    Io(std::io::Error),
    /// CSV writing error
    Csv(csv::Error),
}

impl std::fmt::Display for OCELCSVExportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Csv(e) => write!(f, "CSV error: {e}"),
        }
    }
}

impl std::error::Error for OCELCSVExportError {}
impl From<std::io::Error> for OCELCSVExportError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<csv::Error> for OCELCSVExportError {
    fn from(e: csv::Error) -> Self {
        Self::Csv(e)
    }
}

struct ObjectRef<'a> {
    object_id: &'a str,
    qualifier: &'a str,
    attributes: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Export OCEL to CSV format
pub fn export_ocel_csv<W, O>(writer: W, ocel: &O) -> Result<(), OCELCSVExportError>
where
    W: Write,
    O: ReadableOCEL + ?Sized,
{
    export_ocel_csv_with_options(writer, ocel, &OCELCSVExportOptions::default())
}

/// Export OCEL to CSV format with custom options
pub fn export_ocel_csv_with_options<W, O>(
    writer: W,
    ocel: &O,
    options: &OCELCSVExportOptions,
) -> Result<(), OCELCSVExportError>
where
    W: Write,
    O: ReadableOCEL + ?Sized,
{
    let mut csv_writer = csv::Writer::from_writer(writer);
    let lookup = ocel.lookup();

    let mut object_type_names: Vec<&str> = ocel
        .object_types()
        .iter()
        .map(|ot| ot.name.as_str())
        .collect();
    object_type_names.sort();

    let mut event_attr_names: Vec<&str> = ocel
        .event_types()
        .iter()
        .flat_map(|et| et.attributes.iter().map(|a| a.name.as_str()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    event_attr_names.sort();

    let mut headers: Vec<String> = vec!["id".into(), "activity".into(), "timestamp".into()];
    headers.extend(object_type_names.iter().map(|n| format!("ot:{n}")));
    headers.extend(event_attr_names.iter().map(|n| format!("ea:{n}")));
    csv_writer.write_record(&headers)?;

    // (time, object_id) pairs covered by event rows, so the later object-attribute pass
    // can skip rows that would duplicate an event row's value at the same timestamp.
    let mut event_obj_times: HashSet<(DateTime<FixedOffset>, &str)> = HashSet::new();

    for event_cow in ocel.iter_events_sorted_by_time() {
        let event = event_cow.as_ref();
        let mut object_refs: HashMap<&str, Vec<ObjectRef<'_>>> = HashMap::new();
        for rel in &event.relationships {
            let Some(obj_id) = lookup.get_id_borrow(&rel.object_id) else {
                continue;
            };
            event_obj_times.insert((event.time, obj_id));
            if let Some(obj_type) = lookup.object_type_of(obj_id) {
                let obj_attrs: serde_json::Map<_, _> = lookup
                    .object_attributes(obj_id)
                    .filter(|(_, _, t)| *t == event.time)
                    .map(|(name, value, _)| (name.to_string(), ocel_value_to_json(value)))
                    .collect();
                object_refs.entry(obj_type).or_default().push(ObjectRef {
                    object_id: obj_id,
                    qualifier: &rel.qualifier,
                    attributes: (!obj_attrs.is_empty()).then_some(obj_attrs),
                });
            }
        }
        let event_attrs: HashMap<&str, String> = event
            .attributes
            .iter()
            .map(|a| (a.name.as_str(), a.value.to_string()))
            .collect();
        write_record(
            &mut csv_writer,
            &event.id,
            &event.event_type,
            Some(event.time),
            &object_refs,
            &event_attrs,
            &object_type_names,
            &event_attr_names,
            options,
        )?;
    }

    if options.include_o2o {
        for obj_id in lookup.iter_object_ids() {
            let mut object_refs: HashMap<&str, Vec<ObjectRef<'_>>> = HashMap::new();
            let mut had_relationship = false;
            for (target_id, qualifier) in lookup.object_relationships(obj_id) {
                had_relationship = true;
                if let Some(target_type) = lookup.object_type_of(target_id) {
                    object_refs.entry(target_type).or_default().push(ObjectRef {
                        object_id: target_id,
                        qualifier,
                        attributes: None,
                    });
                }
            }
            if !had_relationship {
                continue;
            }
            write_record(
                &mut csv_writer,
                obj_id,
                "o2o",
                None,
                &object_refs,
                &HashMap::new(),
                &object_type_names,
                &event_attr_names,
                options,
            )?;
        }
    }

    if options.include_object_attribute_changes {
        for obj_id in lookup.iter_object_ids() {
            let Some(obj_type) = lookup.object_type_of(obj_id) else {
                continue;
            };
            let mut attrs_by_time: HashMap<
                DateTime<FixedOffset>,
                Vec<(String, serde_json::Value)>,
            > = HashMap::new();
            for (name, value, time) in lookup.object_attributes(obj_id) {
                attrs_by_time
                    .entry(time)
                    .or_default()
                    .push((name.to_string(), ocel_value_to_json(value)));
            }
            for (time, attrs) in attrs_by_time {
                if event_obj_times.contains(&(time, obj_id)) {
                    continue;
                }
                let attr_map: serde_json::Map<_, _> = attrs.into_iter().collect();
                let mut object_refs: HashMap<&str, Vec<ObjectRef<'_>>> = HashMap::new();
                object_refs.entry(obj_type).or_default().push(ObjectRef {
                    object_id: obj_id,
                    qualifier: "",
                    attributes: Some(attr_map),
                });
                write_record(
                    &mut csv_writer,
                    "",
                    "",
                    Some(time),
                    &object_refs,
                    &HashMap::new(),
                    &object_type_names,
                    &event_attr_names,
                    options,
                )?;
            }
        }
    }
    csv_writer.flush()?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_record<W: Write>(
    csv_writer: &mut csv::Writer<W>,
    id: &str,
    activity: &str,
    timestamp: Option<DateTime<FixedOffset>>,
    object_refs: &HashMap<&str, Vec<ObjectRef<'_>>>,
    event_attrs: &HashMap<&str, String>,
    object_type_names: &[&str],
    event_attr_names: &[&str],
    options: &OCELCSVExportOptions,
) -> Result<(), OCELCSVExportError> {
    let mut record: Vec<String> = vec![
        id.to_string(),
        activity.to_string(),
        timestamp
            .map(|ts| format_timestamp(&ts, options))
            .unwrap_or_default(),
    ];
    record.extend(object_type_names.iter().map(|ot| {
        object_refs
            .get(*ot)
            .map(|r| format_object_refs(r))
            .unwrap_or_default()
    }));
    record.extend(
        event_attr_names
            .iter()
            .map(|ea| event_attrs.get(*ea).cloned().unwrap_or_default()),
    );
    csv_writer.write_record(&record)?;
    Ok(())
}

fn format_timestamp(dt: &DateTime<FixedOffset>, options: &OCELCSVExportOptions) -> String {
    options
        .date_format
        .as_ref()
        .map(|f| dt.format(f).to_string())
        .unwrap_or_else(|| dt.format("%Y-%m-%dT%H:%M:%S%z").to_string())
}

fn format_object_refs(refs: &[ObjectRef<'_>]) -> String {
    refs.iter()
        .map(|r| {
            let mut s = r.object_id.to_string();
            if !r.qualifier.is_empty() {
                s.push('#');
                s.push_str(r.qualifier);
            }
            if let Some(attrs) = &r.attributes {
                if !attrs.is_empty() {
                    s.push_str(
                        &serde_json::to_string(attrs)
                            .expect("serde_json::Map<String, Value> always serializes to a string"),
                    );
                }
            }
            s
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn ocel_value_to_json(value: &OCELAttributeValue) -> serde_json::Value {
    match value {
        OCELAttributeValue::Time(dt) => serde_json::Value::String(dt.to_rfc3339()),
        OCELAttributeValue::Integer(i) => serde_json::Value::Number((*i).into()),
        OCELAttributeValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        OCELAttributeValue::Boolean(b) => serde_json::Value::Bool(*b),
        OCELAttributeValue::String(s) => serde_json::Value::String(s.clone()),
        OCELAttributeValue::Null => serde_json::Value::Null,
    }
}

/// Export OCEL to a CSV file at the specified path
pub fn export_ocel_csv_to_path<P, O>(ocel: &O, path: P) -> Result<(), OCELCSVExportError>
where
    P: AsRef<std::path::Path>,
    O: ReadableOCEL + ?Sized,
{
    export_ocel_csv(std::io::BufWriter::new(std::fs::File::create(path)?), ocel)
}

/// Export OCEL to a CSV file at the specified path with options
pub fn export_ocel_csv_to_path_with_options<P, O>(
    ocel: &O,
    path: P,
    options: &OCELCSVExportOptions,
) -> Result<(), OCELCSVExportError>
where
    P: AsRef<std::path::Path>,
    O: ReadableOCEL + ?Sized,
{
    export_ocel_csv_with_options(
        std::io::BufWriter::new(std::fs::File::create(path)?),
        ocel,
        options,
    )
}

/// Export OCEL to a CSV string
pub fn export_ocel_csv_to_string<O>(ocel: &O) -> Result<String, OCELCSVExportError>
where
    O: ReadableOCEL + ?Sized,
{
    let mut buf = Vec::new();
    export_ocel_csv(&mut buf, ocel)?;
    String::from_utf8(buf).map_err(|e| {
        OCELCSVExportError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    })
}

/// Export OCEL to a CSV string with options
pub fn export_ocel_csv_to_string_with_options<O>(
    ocel: &O,
    options: &OCELCSVExportOptions,
) -> Result<String, OCELCSVExportError>
where
    O: ReadableOCEL + ?Sized,
{
    let mut buf = Vec::new();
    export_ocel_csv_with_options(&mut buf, ocel, options)?;
    String::from_utf8(buf).map_err(|e| {
        OCELCSVExportError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::{
        ocel_csv::import_ocel_csv,
        ocel_struct::{
            OCELEvent, OCELObject, OCELObjectAttribute, OCELRelationship, OCELType,
            OCELTypeAttribute, OCEL,
        },
    };

    #[test]
    fn test_roundtrip() {
        let csv_input = r#"id,activity,timestamp,ot:order,ot:item,ea:billable,ea:area
e1,place order,2026-01-22T09:57:28+0000,o1,i1#part-of{"price": "5€"}/i2#part-of{"price": "15€"},no,
e2,pick item,2026-01-23T09:57:28+0000,,i1,no,outdoor
e3,produce item,2026-01-24T09:57:28+0000,,i2#target,no,indoor
e4,send order,2026-01-26T09:57:28+0000,o1,i1/i2,yes,"#;
        let ocel = import_ocel_csv(csv_input.as_bytes()).unwrap();
        let exported = export_ocel_csv_to_string(&ocel).unwrap();
        let reimported = import_ocel_csv(exported.as_bytes()).unwrap();
        assert_eq!(ocel.events.len(), reimported.events.len());
        assert_eq!(ocel.objects.len(), reimported.objects.len());
    }

    /// Regression: object attributes with an "initial value" timestamp (not matching any event
    /// time) must survive a default CSV roundtrip.
    #[test]
    fn test_initial_object_attribute_survives_roundtrip() {
        let initial_time: DateTime<FixedOffset> = DateTime::UNIX_EPOCH.into();
        let event_time: DateTime<FixedOffset> =
            DateTime::parse_from_rfc3339("2024-05-01T10:00:00+00:00").unwrap();
        let ocel = OCEL {
            event_types: vec![OCELType {
                name: "place order".into(),
                attributes: vec![],
            }],
            object_types: vec![OCELType {
                name: "item".into(),
                attributes: vec![OCELTypeAttribute {
                    name: "price".into(),
                    value_type: "float".into(),
                }],
            }],
            events: vec![OCELEvent {
                id: "e1".into(),
                event_type: "place order".into(),
                time: event_time,
                attributes: vec![],
                relationships: vec![OCELRelationship {
                    object_id: "i1".into(),
                    qualifier: "is in".into(),
                }],
            }],
            objects: vec![OCELObject {
                id: "i1".into(),
                object_type: "item".into(),
                attributes: vec![OCELObjectAttribute {
                    name: "price".into(),
                    value: OCELAttributeValue::Float(4.3),
                    time: initial_time,
                }],
                relationships: vec![],
            }],
        };

        let exported = export_ocel_csv_to_string(&ocel).unwrap();
        let reimported = import_ocel_csv(exported.as_bytes()).unwrap();

        let item = reimported
            .objects
            .iter()
            .find(|o| o.id == "i1")
            .expect("item i1 must survive roundtrip");
        let price = item
            .attributes
            .iter()
            .find(|a| a.name == "price")
            .expect("price attribute must survive roundtrip");
        assert_eq!(price.value, OCELAttributeValue::Float(4.3));
        assert_eq!(price.time, initial_time);
    }
}
