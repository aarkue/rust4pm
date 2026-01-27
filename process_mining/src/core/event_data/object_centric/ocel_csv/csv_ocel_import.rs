//! CSV Import for OCEL

use std::{collections::HashMap, fmt::Display, io::Read};

use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::core::event_data::{
    object_centric::ocel_struct::{
        OCELAttributeType, OCELAttributeValue, OCELEvent, OCELEventAttribute, OCELObject,
        OCELObjectAttribute, OCELRelationship, OCELType, OCELTypeAttribute, OCEL,
    },
    timestamp_utils::parse_timestamp,
};

/// Error type for CSV OCEL parsing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OCELCSVImportError {
    /// CSV parsing error
    CsvError(String),
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

impl Display for OCELCSVImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CsvError(e) => write!(f, "CSV error: {e}"),
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

impl std::error::Error for OCELCSVImportError {}

impl From<csv::Error> for OCELCSVImportError {
    fn from(e: csv::Error) -> Self {
        Self::CsvError(e.to_string())
    }
}

/// Options for CSV OCEL Import
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OCELCSVImportOptions {
    /// Verbosely log warnings (e.g., for missing referenced objects, skipped rows)
    pub verbose: bool,
    /// Optional date format to use when parsing timestamps
    pub date_format: Option<String>,
    /// Strict mode: fail on warnings instead of skipping problematic rows
    pub strict: bool,
}

/// Parsed object reference: `object_id#qualifier{json_attrs}`
#[derive(Debug, Clone)]
struct ObjectRef {
    id: String,
    qualifier: String,
    attributes: Option<Map<String, Value>>,
}

/// Parse object reference
/// Can of the following shapes: `id`, `id#qualifier`, `id{json}`, or `id#qualifier{json}`
fn parse_object_ref(input: &str) -> Result<ObjectRef, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("Empty object reference".into());
    }

    let (before_brace, json_map) = match input.find('{') {
        Some(pos) => {
            let json_str = &input[pos..];
            let parsed: serde_json::Value = serde_json::from_str(json_str)
                .map_err(|e| format!("Invalid JSON '{json_str}': {e}"))?;
            (&input[..pos], parsed.as_object().cloned())
        }
        None => (input, None),
    };

    let (id, qualifier) = match before_brace.find('#') {
        Some(pos) => (before_brace[..pos].trim(), before_brace[pos + 1..].trim()),
        None => (before_brace.trim(), ""),
    };

    Ok(ObjectRef {
        id: id.to_string(),
        qualifier: qualifier.to_string(),
        attributes: json_map,
    })
}

/// Parse cell with multiple object references separated by `/` (respecting JSON braces)
fn parse_object_cell(cell: &str) -> Result<Vec<ObjectRef>, String> {
    let cell = cell.trim();
    if cell.is_empty() {
        return Ok(vec![]);
    }

    let mut refs = Vec::new();
    let mut current = String::new();
    let mut brace_depth = 0;

    for c in cell.chars() {
        match c {
            '{' => {
                brace_depth += 1;
                current.push(c);
            }
            '}' => {
                brace_depth -= 1;
                current.push(c);
            }
            '/' if brace_depth == 0 => {
                if !current.is_empty() {
                    refs.push(parse_object_ref(&current)?);
                    current.clear();
                }
            }
            _ => current.push(c),
        }
    }
    if !current.is_empty() {
        refs.push(parse_object_ref(&current)?);
    }
    Ok(refs)
}

/// Parse string into typed attribute value
///
/// Tries to interpret the string in the following order:
/// bool > int > float > time > string
fn parse_value(s: &str, date_fmt: Option<&str>) -> OCELAttributeValue {
    let s = s.trim();
    if s.eq_ignore_ascii_case("true") {
        return OCELAttributeValue::Boolean(true);
    }
    if s.eq_ignore_ascii_case("false") {
        return OCELAttributeValue::Boolean(false);
    }
    if let Ok(i) = s.parse::<i64>() {
        return OCELAttributeValue::Integer(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return OCELAttributeValue::Float(f);
    }
    if let Ok(ts) = parse_timestamp(s, date_fmt, false) {
        return OCELAttributeValue::Time(ts);
    }
    OCELAttributeValue::String(s.to_string())
}

/// Convert JSON value to OCEL attribute value
fn json_to_value(v: &serde_json::Value) -> OCELAttributeValue {
    match v {
        serde_json::Value::Null => OCELAttributeValue::Null,
        serde_json::Value::Bool(b) => OCELAttributeValue::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                OCELAttributeValue::Integer(i)
            } else {
                OCELAttributeValue::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => OCELAttributeValue::String(s.clone()),
        _ => OCELAttributeValue::String(v.to_string()),
    }
}

/// Coalesce two types: same->same, null + x ->x, int + float -> float, other combination -> string
fn coalesce(t1: OCELAttributeType, t2: OCELAttributeType) -> OCELAttributeType {
    use OCELAttributeType::*;
    if t1 == t2 {
        return t1;
    }
    match (t1, t2) {
        (Null, other) | (other, Null) => other,
        (Integer, Float) | (Float, Integer) => Float,
        _ => String,
    }
}

/// Convert value to target type (mainly int -> float or any -> string)
fn convert_to_type(value: OCELAttributeValue, target: OCELAttributeType) -> OCELAttributeValue {
    let current: OCELAttributeType = value.get_type();
    if current == target {
        return value;
    }
    match (value, target) {
        (OCELAttributeValue::Null, _) => OCELAttributeValue::Null,
        (OCELAttributeValue::Integer(i), OCELAttributeType::Float) => {
            OCELAttributeValue::Float(i as f64)
        }
        (v, _) => OCELAttributeValue::String(v.to_string()),
    }
}

/// Type Registry: tracks inferred types per `type_name` and `attr_name`
/// Two-level map: `type_name` → `attr_name` → `inferred_type`
type TypeRegistry = HashMap<String, HashMap<String, OCELAttributeType>>;

/// Update type registry with a new value's type, coalescing if needed
fn register_type(
    registry: &mut TypeRegistry,
    type_name: &str,
    attr_name: &str,
    value: &OCELAttributeValue,
) {
    let value_type: OCELAttributeType = value.get_type();
    if let Some(attrs) = registry.get_mut(type_name) {
        if let Some(current) = attrs.get_mut(attr_name) {
            *current = coalesce(*current, value_type);
        } else {
            attrs.insert(attr_name.to_string(), value_type);
        }
    } else {
        let mut attrs = HashMap::new();
        attrs.insert(attr_name.to_string(), value_type);
        registry.insert(type_name.to_string(), attrs);
    }
}

/// Get inferred type from registry
fn get_type(registry: &TypeRegistry, type_name: &str, attr_name: &str) -> OCELAttributeType {
    registry
        .get(type_name)
        .and_then(|m| m.get(attr_name))
        .copied()
        .unwrap_or(OCELAttributeType::String)
}

/// Columns of CSV Format
enum Column {
    Id,
    Activity,
    Timestamp,
    ObjectType(String),
    EventAttr(String),
}

/// Classify all columns of the CSV
/// Returns a list with all columns, as well as the indices of the id column (1st), activity column (2nd), and timestamp column (3rd)
fn classify_columns(
    headers: &csv::StringRecord,
) -> (Vec<Column>, Option<usize>, Option<usize>, Option<usize>) {
    let mut columns = Vec::with_capacity(headers.len());
    let (mut id_col, mut act_col, mut ts_col) = (None, None, None);

    for (i, h) in headers.iter().enumerate() {
        let h = h.trim();
        let h_lower = h.to_lowercase();
        if h_lower == "id" {
            id_col = Some(i);
            columns.push(Column::Id);
        } else if h_lower == "activity" {
            act_col = Some(i);
            columns.push(Column::Activity);
        } else if h_lower == "timestamp" {
            ts_col = Some(i);
            columns.push(Column::Timestamp);
        } else if let Some(name) = h_lower.strip_prefix("ot:") {
            // Use original casing for the name part
            let orig_name = h.get(3..).unwrap_or(name).trim();
            columns.push(Column::ObjectType(orig_name.to_string()));
        } else if let Some(name) = h_lower.strip_prefix("ea:") {
            let orig_name = h.get(3..).unwrap_or(name).trim();
            columns.push(Column::EventAttr(orig_name.to_string()));
        } else {
            columns.push(Column::EventAttr(h.to_string()));
        }
    }
    (columns, id_col, act_col, ts_col)
}

/// Import OCEL from CSV reader
pub fn import_ocel_csv(reader: impl Read) -> Result<OCEL, OCELCSVImportError> {
    import_ocel_csv_with_options(reader, &OCELCSVImportOptions::default())
}

/// Import OCEL from CSV reader with custom options
pub fn import_ocel_csv_with_options(
    reader: impl Read,
    options: &OCELCSVImportOptions,
) -> Result<OCEL, OCELCSVImportError> {
    let mut rdr = csv::Reader::from_reader(reader);
    let headers = rdr.headers()?.clone();
    let (columns, id_col, act_col, ts_col) = classify_columns(&headers);

    let id_col = id_col.ok_or_else(|| OCELCSVImportError::MissingColumn("id".into()))?;
    let act_col = act_col.ok_or_else(|| OCELCSVImportError::MissingColumn("activity".into()))?;
    let ts_col = ts_col.ok_or_else(|| OCELCSVImportError::MissingColumn("timestamp".into()))?;

    let mut events: Vec<OCELEvent> = Vec::new();
    let mut objects: HashMap<String, OCELObject> = HashMap::new();
    let mut event_type_attrs: TypeRegistry = HashMap::new();
    let mut object_type_attrs: TypeRegistry = HashMap::new();

    let date_fmt = options.date_format.as_deref();

    for (row_idx, result) in rdr.records().enumerate() {
        let record = result?;
        let row_num = row_idx + 2;

        let id = record.get(id_col).unwrap_or("").trim();
        let activity = record.get(act_col).unwrap_or("").trim();
        let ts_str = record.get(ts_col).unwrap_or("").trim();

        let is_o2o = activity.eq_ignore_ascii_case("o2o");
        let is_attr_only = id.is_empty() && activity.is_empty();

        // Parse timestamp (required for events, optional for O2O)
        let mut timestamp: Option<DateTime<FixedOffset>> = if ts_str.is_empty() {
            if !is_o2o && !is_attr_only {
                if options.strict {
                    return Err(OCELCSVImportError::ParseError {
                        row: row_num,
                        message: "Missing timestamp".into(),
                    });
                }
                if options.verbose {
                    eprintln!("Warning: Skipping row {row_num} (missing timestamp)");
                }
                continue;
            }
            None
        } else {
            Some(
                parse_timestamp(ts_str, date_fmt, options.verbose).map_err(|_| {
                    OCELCSVImportError::InvalidTimestamp {
                        row: row_num,
                        value: ts_str.into(),
                    }
                })?,
            )
        };

        if is_attr_only && timestamp.is_none() {
            if options.verbose {
                eprintln!("Warning: Row {row_num} (attribute-only without timestamp). Will assume UNIX EPOCH as time.");
            }
            timestamp = Some(DateTime::UNIX_EPOCH.into());
        }

        // Process object columns
        let mut obj_refs: Vec<(&str, ObjectRef)> = Vec::new(); // (object_type, ref)

        for (col_idx, col) in columns.iter().enumerate() {
            if let Column::ObjectType(ot_name) = col {
                let cell = record.get(col_idx).unwrap_or("").trim();
                if cell.is_empty() {
                    continue;
                }

                let refs = parse_object_cell(cell).map_err(|e| {
                    OCELCSVImportError::InvalidObjectReference {
                        row: row_num,
                        value: format!("{cell}: {e}"),
                    }
                })?;

                for obj_ref in refs {
                    // Ensure object exists and type is registered
                    if !objects.contains_key(&obj_ref.id) {
                        // If object did not exist before, create it
                        objects.insert(
                            obj_ref.id.clone(),
                            OCELObject {
                                id: obj_ref.id.clone(),
                                object_type: ot_name.clone(),
                                attributes: Vec::new(),
                                relationships: Vec::new(),
                            },
                        );
                        // Ensure object type is in registry (even with no attributes)
                        object_type_attrs.entry(ot_name.clone()).or_default();
                    }

                    // Handle inline JSON attributes
                    if let (Some(attrs), Some(ts)) = (&obj_ref.attributes, timestamp) {
                        if let Some(obj) = objects.get_mut(&obj_ref.id) {
                            for (attr_name, attr_val) in attrs {
                                let value = json_to_value(attr_val);
                                register_type(&mut object_type_attrs, ot_name, attr_name, &value);
                                obj.attributes.push(OCELObjectAttribute {
                                    name: attr_name.clone(),
                                    value,
                                    time: ts,
                                });
                            }
                        }
                    }

                    obj_refs.push((ot_name.as_str(), obj_ref));
                }
            }
        }

        if is_attr_only {
            // Everything required for attribute changes is done already!
            continue;
        }

        if is_o2o {
            // O2O row: id column is source object, object columns are targets
            if let Some(source) = objects.get_mut(id) {
                for (_, obj_ref) in &obj_refs {
                    source.relationships.push(OCELRelationship {
                        object_id: obj_ref.id.clone(),
                        qualifier: obj_ref.qualifier.clone(),
                    });
                }
            } else if options.verbose {
                // If object never appeared before, we can't know its type.
                if options.strict {
                    return Err(OCELCSVImportError::InvalidObjectReference {
                        row: row_num,
                        value: format!(
                            "O2O source {id} is not known. Type of the object is not specified."
                        ),
                    });
                }
                eprintln!("Warning: O2O source '{id}' not found at row {row_num}");
            }
            continue;
        }

        // Regular event row
        let event_type = activity.to_string();
        let relationships: Vec<_> = obj_refs
            .iter()
            .map(|(_, r)| OCELRelationship {
                object_id: r.id.clone(),
                qualifier: r.qualifier.clone(),
            })
            .collect();

        // Collect event attributes
        let mut attrs: Vec<OCELEventAttribute> = Vec::new();
        for (col_idx, col) in columns.iter().enumerate() {
            if let Column::EventAttr(attr_name) = col {
                let cell = record.get(col_idx).unwrap_or("").trim();
                if !cell.is_empty() {
                    let value = parse_value(cell, date_fmt);
                    register_type(&mut event_type_attrs, &event_type, attr_name, &value);
                    attrs.push(OCELEventAttribute {
                        name: attr_name.clone(),
                        value,
                    });
                }
            }
        }

        events.push(OCELEvent {
            id: id.to_string(),
            event_type,
            time: timestamp.expect("Events must have timestamp"),
            attributes: attrs,
            relationships,
        });
    }

    // Convert values to their final coalesced types
    for event in &mut events {
        for attr in &mut event.attributes {
            let target = get_type(&event_type_attrs, &event.event_type, &attr.name);
            let value = std::mem::take(&mut attr.value);
            attr.value = convert_to_type(value, target);
        }
    }
    for obj in objects.values_mut() {
        for attr in &mut obj.attributes {
            let target = get_type(&object_type_attrs, &obj.object_type, &attr.name);
            let value = std::mem::take(&mut attr.value);
            attr.value = convert_to_type(value, target);
        }
    }

    // Build type definitions
    let event_types = build_types(&event_type_attrs);
    let object_types = build_types(&object_type_attrs);

    Ok(OCEL {
        event_types,
        object_types,
        events,
        objects: objects.into_values().collect(),
    })
}

/// Build [`OCELType`] list from type registry
fn build_types(registry: &TypeRegistry) -> Vec<OCELType> {
    registry
        .iter()
        .map(|(type_name, attrs)| OCELType {
            name: type_name.clone(),
            attributes: attrs
                .iter()
                .map(|(attr_name, attr_type)| OCELTypeAttribute {
                    name: attr_name.clone(),
                    value_type: attr_type.to_type_string(),
                })
                .collect(),
        })
        .collect()
}

/// Import OCEL from CSV file path
pub fn import_ocel_csv_from_path<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<OCEL, OCELCSVImportError> {
    let file =
        std::fs::File::open(path).map_err(|e| OCELCSVImportError::CsvError(e.to_string()))?;
    import_ocel_csv(std::io::BufReader::new(file))
}

/// Import OCEL from CSV file path with options
pub fn import_ocel_csv_from_path_with_options<P: AsRef<std::path::Path>>(
    path: P,
    options: &OCELCSVImportOptions,
) -> Result<OCEL, OCELCSVImportError> {
    let file =
        std::fs::File::open(path).map_err(|e| OCELCSVImportError::CsvError(e.to_string()))?;
    import_ocel_csv_with_options(std::io::BufReader::new(file), options)
}

#[cfg(test)]
/// Tests
mod tests {
    use super::*;
    use std::collections::HashSet;

    const TEST_CSV: &str = r#"id,activity,timestamp,ot:order,ot:item,ea:billable,ea:area
e1,place order,2026-01-22T09:57:28+0000,o1,i1#part-of{"price": "5€"}/i2#part-of{"price": "15€"},no,
e2,pick item,2026-01-23T09:57:28+0000,,i1,no,outdoor
e3,produce item,2026-01-24T09:57:28+0000,,i2#target,no,indoor
,,2026-01-25T09:57:28+0000,,i1{"price": "50€"},,
e4,send order,2026-01-26T09:57:28+0000,o1,i1/i2,yes,
o1,o2o,,,i1#has/i2#has,,
i2,o2o,,,i1#add-on,,"#;

    const TEST_CSV_VARIATIONS: &str = r#"ID,Activity,Timestamp,OT:Order, ot:Item ,EA:Billable
e1,place order,2026-01-22T09:57:28+0000, o1 , i1 ,yes"#;

    #[test]
    fn test_parse_object_ref_simple() {
        let r = parse_object_ref("i1").unwrap();
        assert_eq!(r.id, "i1");
        assert_eq!(r.qualifier, "");
        assert!(r.attributes.is_none());
    }

    #[test]
    fn test_parse_object_ref_with_qualifier() {
        let r = parse_object_ref("i2#target").unwrap();
        assert_eq!(r.id, "i2");
        assert_eq!(r.qualifier, "target");
    }

    #[test]
    fn test_parse_object_ref_with_json() {
        let r = parse_object_ref(r#"i1{"price": "5€"}"#).unwrap();
        assert_eq!(r.id, "i1");
        assert!(r.attributes.is_some());
    }

    #[test]
    fn test_parse_object_ref_full() {
        let r = parse_object_ref(r#"i1#part-of{"price": "5€"}"#).unwrap();
        assert_eq!(r.id, "i1");
        assert_eq!(r.qualifier, "part-of");
        assert!(r.attributes.is_some());
    }

    #[test]
    fn test_parse_object_cell_multiple() {
        let refs =
            parse_object_cell(r#"i1#part-of{"price": "5€"}/i2#part-of{"price": "15€"}"#).unwrap();
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].id, "i1");
        assert_eq!(refs[1].id, "i2");
    }

    #[test]
    fn test_import_ocel_csv() {
        let ocel = import_ocel_csv(TEST_CSV.as_bytes()).unwrap();
        assert_eq!(ocel.events.len(), 4);
        assert!(ocel.objects.len() >= 3);

        let et_names: HashSet<_> = ocel.event_types.iter().map(|t| t.name.as_str()).collect();
        assert!(et_names.contains("place order"));
        assert!(et_names.contains("pick item"));

        let ot_names: HashSet<_> = ocel.object_types.iter().map(|t| t.name.as_str()).collect();
        assert!(ot_names.contains("order"));
        assert!(ot_names.contains("item"));
    }

    #[test]
    fn test_import_case_insensitive_headers() {
        let ocel = import_ocel_csv(TEST_CSV_VARIATIONS.as_bytes()).unwrap();
        assert_eq!(ocel.events.len(), 1);

        let ot_names: HashSet<_> = ocel.object_types.iter().map(|t| t.name.as_str()).collect();
        assert!(ot_names.contains("Order"));
        assert!(ot_names.contains("Item"));
    }

    #[test]
    fn test_strict_mode() {
        let csv = "id,activity,timestamp,ot:item
        e1,test,,i1";
        let opts = OCELCSVImportOptions {
            strict: true,
            verbose: false,
            ..Default::default()
        };
        assert!(import_ocel_csv_with_options(csv.as_bytes(), &opts).is_err());
    }

    #[test]
    fn test_type_inference_integers() {
        let csv = "id,activity,timestamp,ea:count
        e1,test,2026-01-22T09:57:28+0000,1
        e2,test,2026-01-23T09:57:28+0000,100";
        let ocel = import_ocel_csv(csv.as_bytes()).unwrap();
        let et = ocel.event_types.iter().find(|t| t.name == "test").unwrap();
        let attr = et.attributes.iter().find(|a| a.name == "count").unwrap();
        assert_eq!(attr.value_type, "integer");
    }

    #[test]
    fn test_type_inference_floats() {
        let csv = "id,activity,timestamp,ea:price
        e1,test,2026-01-22T09:57:28+0000,1.5
        e2,test,2026-01-23T09:57:28+0000,2.75";
        let ocel = import_ocel_csv(csv.as_bytes()).unwrap();
        let et = ocel.event_types.iter().find(|t| t.name == "test").unwrap();
        let attr = et.attributes.iter().find(|a| a.name == "price").unwrap();
        assert_eq!(attr.value_type, "float");
    }

    #[test]
    fn test_type_coalesce_int_float() {
        let csv = "id,activity,timestamp,ea:val
        e1,test,2026-01-22T09:57:28+0000,1
        e2,test,2026-01-23T09:57:28+0000,2.5";
        let ocel = import_ocel_csv(csv.as_bytes()).unwrap();
        let et = ocel.event_types.iter().find(|t| t.name == "test").unwrap();
        let attr = et.attributes.iter().find(|a| a.name == "val").unwrap();
        assert_eq!(attr.value_type, "float");

        let e1 = ocel.events.iter().find(|e| e.id == "e1").unwrap();
        let v = e1.attributes.iter().find(|a| a.name == "val").unwrap();
        assert!(matches!(v.value, OCELAttributeValue::Float(f) if f == 1.0));
    }

    #[test]
    fn test_type_coalesce_to_string() {
        let csv = "id,activity,timestamp,ea:mixed
        e1,test,2026-01-22T09:57:28+0000,42
        e2,test,2026-01-23T09:57:28+0000,hello";
        let ocel = import_ocel_csv(csv.as_bytes()).unwrap();
        let et = ocel.event_types.iter().find(|t| t.name == "test").unwrap();
        let attr = et.attributes.iter().find(|a| a.name == "mixed").unwrap();
        assert_eq!(attr.value_type, "string");
    }

    #[test]
    fn test_type_inference_booleans() {
        let csv = "id,activity,timestamp,ea:flag
        e1,test,2026-01-22T09:57:28+0000,true
        e2,test,2026-01-23T09:57:28+0000,FALSE";
        let ocel = import_ocel_csv(csv.as_bytes()).unwrap();
        let et = ocel.event_types.iter().find(|t| t.name == "test").unwrap();
        let attr = et.attributes.iter().find(|a| a.name == "flag").unwrap();
        assert_eq!(attr.value_type, "boolean");
    }

    #[test]
    fn test_type_per_event_type() {
        let csv = "id,activity,timestamp,ea:amount
        e1,order,2026-01-22T09:57:28+0000,100
        e2,payment,2026-01-23T09:57:28+0000,50.5";
        let ocel = import_ocel_csv(csv.as_bytes()).unwrap();

        let order_t = ocel.event_types.iter().find(|t| t.name == "order").unwrap();
        assert_eq!(
            order_t
                .attributes
                .iter()
                .find(|a| a.name == "amount")
                .unwrap()
                .value_type,
            "integer"
        );

        let payment_t = ocel
            .event_types
            .iter()
            .find(|t| t.name == "payment")
            .unwrap();
        assert_eq!(
            payment_t
                .attributes
                .iter()
                .find(|a| a.name == "amount")
                .unwrap()
                .value_type,
            "float"
        );
    }

    #[test]
    fn test_object_attr_type_inference() {
        let csv = r#"id,activity,timestamp,ot:item
e1,test,2026-01-22T09:57:28+0000,i1{"quantity": 5}
e2,test,2026-01-23T09:57:28+0000,i2{"quantity": 10}"#;
        let ocel = import_ocel_csv(csv.as_bytes()).unwrap();
        let item_t = ocel.object_types.iter().find(|t| t.name == "item").unwrap();
        let attr = item_t
            .attributes
            .iter()
            .find(|a| a.name == "quantity")
            .unwrap();
        assert_eq!(attr.value_type, "integer");
    }
}
