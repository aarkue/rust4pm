//! CSV Export for OCEL 2.0

use crate::core::event_data::object_centric::ocel_struct::{OCELAttributeValue, OCELObject, OCEL};
use chrono::{DateTime, FixedOffset};
use std::{
    collections::{HashMap, HashSet},
    io::Write,
};

/// Options for CSV OCEL Export
#[derive(Debug, Clone, Default)]
pub struct OCELCSVExportOptions {
    /// Whether to include O2O relationships in the export
    pub include_o2o: bool,
    /// Whether to include object attribute changes as separate rows
    pub include_object_attribute_changes: bool,
    /// Date format for timestamps (default: RFC3339)
    pub date_format: Option<String>,
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

struct ExportRow<'a> {
    timestamp: Option<&'a DateTime<FixedOffset>>,
    id: &'a str,
    activity: &'a str,
    object_refs: HashMap<&'a str, Vec<ObjectRef<'a>>>,
    event_attrs: HashMap<&'a str, String>,
}

struct ObjectRef<'a> {
    object_id: &'a str,
    qualifier: &'a str,
    attributes: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Export OCEL to CSV format
pub fn export_ocel_csv<W: Write>(writer: W, ocel: &OCEL) -> Result<(), OCELCSVExportError> {
    export_ocel_csv_with_options(writer, ocel, &OCELCSVExportOptions::default())
}

/// Export OCEL to CSV format with custom options
pub fn export_ocel_csv_with_options<W: Write>(
    writer: W,
    ocel: &OCEL,
    options: &OCELCSVExportOptions,
) -> Result<(), OCELCSVExportError> {
    let mut csv_writer = csv::Writer::from_writer(writer);
    let objects_by_id: HashMap<&str, &OCELObject> =
        ocel.objects.iter().map(|o| (o.id.as_str(), o)).collect();

    let mut object_type_names: Vec<&str> = ocel
        .object_types
        .iter()
        .map(|ot| ot.name.as_str())
        .collect();
    object_type_names.sort();

    let mut event_attr_names: Vec<&str> = ocel
        .events
        .iter()
        .flat_map(|e| e.attributes.iter().map(|a| a.name.as_str()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    event_attr_names.sort();

    // Build header
    let mut headers: Vec<String> = vec!["id".into(), "activity".into(), "timestamp".into()];
    headers.extend(object_type_names.iter().map(|n| format!("ot:{n}")));
    headers.extend(event_attr_names.iter().map(|n| format!("ea:{n}")));
    csv_writer.write_record(&headers)?;

    let mut rows: Vec<ExportRow<'_>> = Vec::new();

    // Event rows
    for event in &ocel.events {
        let mut object_refs: HashMap<&str, Vec<ObjectRef<'_>>> = HashMap::new();
        for rel in &event.relationships {
            if let Some(obj) = objects_by_id.get(rel.object_id.as_str()) {
                let matching_attrs: Vec<_> = obj
                    .attributes
                    .iter()
                    .filter(|a| a.time == event.time)
                    .collect();
                let obj_attrs = if matching_attrs.is_empty() {
                    None
                } else {
                    Some(
                        matching_attrs
                            .iter()
                            .map(|a| (a.name.clone(), ocel_value_to_json(&a.value)))
                            .collect::<serde_json::Map<_, _>>(),
                    )
                };
                object_refs
                    .entry(&obj.object_type)
                    .or_default()
                    .push(ObjectRef {
                        object_id: &rel.object_id,
                        qualifier: &rel.qualifier,
                        attributes: obj_attrs,
                    });
            }
        }
        rows.push(ExportRow {
            timestamp: Some(&event.time),
            id: &event.id,
            activity: &event.event_type,
            object_refs,
            event_attrs: event
                .attributes
                .iter()
                .map(|a| (a.name.as_str(), a.value.to_string()))
                .collect(),
        });
    }

    // O2O relationship rows
    if options.include_o2o {
        for obj in &ocel.objects {
            if obj.relationships.is_empty() {
                continue;
            }
            let mut object_refs: HashMap<&str, Vec<ObjectRef<'_>>> = HashMap::new();
            for rel in &obj.relationships {
                if let Some(target) = objects_by_id.get(rel.object_id.as_str()) {
                    object_refs
                        .entry(target.object_type.as_str())
                        .or_default()
                        .push(ObjectRef {
                            object_id: &rel.object_id,
                            qualifier: &rel.qualifier,
                            attributes: None,
                        });
                }
            }
            rows.push(ExportRow {
                timestamp: None,
                id: &obj.id,
                activity: "o2o",
                object_refs,
                event_attrs: HashMap::new(),
            });
        }
    }

    // Object attribute change rows
    if options.include_object_attribute_changes {
        for obj in &ocel.objects {
            let mut attrs_by_time: HashMap<&DateTime<FixedOffset>, Vec<_>> = HashMap::new();
            for attr in &obj.attributes {
                attrs_by_time.entry(&attr.time).or_default().push(attr);
            }
            for (time, attrs) in attrs_by_time {
                let in_event = ocel.events.iter().any(|e| {
                    &e.time == time && e.relationships.iter().any(|r| r.object_id == obj.id)
                });
                if in_event {
                    continue;
                }
                let attr_map: serde_json::Map<_, _> = attrs
                    .iter()
                    .map(|a| (a.name.clone(), ocel_value_to_json(&a.value)))
                    .collect();
                let mut object_refs: HashMap<&str, Vec<ObjectRef<'_>>> = HashMap::new();
                object_refs
                    .entry(&obj.object_type)
                    .or_default()
                    .push(ObjectRef {
                        object_id: &obj.id,
                        qualifier: "",
                        attributes: Some(attr_map),
                    });
                rows.push(ExportRow {
                    timestamp: Some(time),
                    id: "",
                    activity: "",
                    object_refs,
                    event_attrs: HashMap::new(),
                });
            }
        }
    }

    // Sort: timestamped rows first (by time), then O2O rows (by id)
    rows.sort_by(|a, b| match (a.timestamp, b.timestamp) {
        (None, None) => a.id.cmp(b.id),
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (Some(at), Some(bt)) => at.cmp(bt),
    });

    // Write rows
    for row in rows {
        let mut record = vec![
            row.id.to_string(),
            row.activity.to_string(),
            row.timestamp
                .map(|ts| format_timestamp(ts, options))
                .unwrap_or_default(),
        ];
        record.extend(object_type_names.iter().map(|ot| {
            row.object_refs
                .get(*ot)
                .map(|r| format_object_refs(r))
                .unwrap_or_default()
        }));
        record.extend(
            event_attr_names
                .iter()
                .map(|ea| row.event_attrs.get(*ea).cloned().unwrap_or_default()),
        );
        csv_writer.write_record(&record)?;
    }
    csv_writer.flush()?;
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
                    match serde_json::to_string(attrs) {
                        Ok(json) => s.push_str(&json),
                        Err(e) => {
                            eprintln!("Failed to serialize object attributes to JSON: {}", e);
                        }
                    }
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
pub fn export_ocel_csv_to_path<P: AsRef<std::path::Path>>(
    ocel: &OCEL,
    path: P,
) -> Result<(), OCELCSVExportError> {
    export_ocel_csv(std::io::BufWriter::new(std::fs::File::create(path)?), ocel)
}

/// Export OCEL to a CSV file at the specified path with options
pub fn export_ocel_csv_to_path_with_options<P: AsRef<std::path::Path>>(
    ocel: &OCEL,
    path: P,
    options: &OCELCSVExportOptions,
) -> Result<(), OCELCSVExportError> {
    export_ocel_csv_with_options(
        std::io::BufWriter::new(std::fs::File::create(path)?),
        ocel,
        options,
    )
}

/// Export OCEL to a CSV string
pub fn export_ocel_csv_to_string(ocel: &OCEL) -> Result<String, OCELCSVExportError> {
    let mut buf = Vec::new();
    export_ocel_csv(&mut buf, ocel)?;
    String::from_utf8(buf).map_err(|e| {
        OCELCSVExportError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    })
}

/// Export OCEL to a CSV string with options
pub fn export_ocel_csv_to_string_with_options(
    ocel: &OCEL,
    options: &OCELCSVExportOptions,
) -> Result<String, OCELCSVExportError> {
    let mut buf = Vec::new();
    export_ocel_csv_with_options(&mut buf, ocel, options)?;
    String::from_utf8(buf).map_err(|e| {
        OCELCSVExportError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::ocel_csv::import_ocel_csv;

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
}
