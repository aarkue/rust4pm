use crate::ocel::ocel_struct::OCELAttributeType;

pub(crate) const OCEL_ID_COLUMN: &str = "ocel_id";
pub(crate) const OCEL_TIME_COLUMN: &str = "ocel_time";
pub(crate) const OCEL_CHANGED_FIELD: &str = "ocel_changed_field";
pub(crate) const IGNORED_PRAGMA_COLUMNS: [&str; 3] =
    [OCEL_ID_COLUMN, OCEL_TIME_COLUMN, OCEL_CHANGED_FIELD];
pub(crate) const OCEL_TYPE_MAP_COLUMN: &str = "ocel_type_map";
pub(crate) const OCEL_TYPE_COLUMN: &str = "ocel_type";
pub(crate) const OCEL_O2O_SOURCE_ID_COLUMN: &str = "ocel_source_id";
pub(crate) const OCEL_O2O_TARGET_ID_COLUMN: &str = "ocel_target_id";
pub(crate) const OCEL_E2O_EVENT_ID_COLUMN: &str = "ocel_event_id";
pub(crate) const OCEL_E2O_OBJECT_ID_COLUMN: &str = "ocel_object_id";
pub(crate) const OCEL_REL_QUALIFIER_COLUMN: &str = "ocel_qualifier";

pub(crate) fn sql_type_to_ocel(s: &str) -> OCELAttributeType {
    match s {
        "TEXT" => OCELAttributeType::String,
        "REAL" => OCELAttributeType::Float,
        "INTEGER" => OCELAttributeType::Integer,
        "BOOLEAN" => OCELAttributeType::Boolean,
        "TIMESTAMP" => OCELAttributeType::Time,
        _ => OCELAttributeType::String,
    }
}

pub(crate) fn ocel_type_to_sql(attr: &OCELAttributeType) -> &'static str {
    match attr {
        OCELAttributeType::String => "TEXT",
        OCELAttributeType::Float => "REAL",
        OCELAttributeType::Integer => "INTEGER",
        OCELAttributeType::Boolean => "BOOLEAN",
        OCELAttributeType::Time => "TIMESTAMP",
        _ => "TEXT",
    }
}

pub(crate) mod sqlite_ocel_export;
pub(crate) mod sqlite_ocel_import;
