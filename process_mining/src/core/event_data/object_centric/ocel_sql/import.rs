//! Reading an OCEL 2.0 SQL file, the same way for every database behind [`SqlSource`].
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, FixedOffset};

use super::*;
use crate::core::event_data::object_centric::io::{OCELIOError, OCELImportOptions, RowProgress};
use crate::core::event_data::object_centric::ocel_struct::{
    OCELEvent, OCELEventAttribute, OCELRelationship, OCEL,
};
use crate::core::event_data::object_centric::sql_columns::OCEL_SQL_BASE_COLS;
use crate::utils::control::Control;

/// A misspelling of [`OCEL_CHANGED_FIELD`] seen in the wild, only named in diagnostics.
const OCEL_CHANGE_FIELD_MISSPELLED: &str = "ocel_change_field";
const MISSPELLING_HINT: &str =
    "'ocel_change_field' looks like a misspelling of 'ocel_changed_field'. Fix the source file \
     to use 'ocel_changed_field'.";

/// A database connection an OCEL 2.0 SQL file is read from.
pub(crate) trait SqlSource {
    type Row<'r>: SqlRow;

    /// The database file, for diagnostics.
    fn db_path(&self) -> Option<&str>;

    fn has_table(&self, name: &str) -> Result<bool, OCELIOError>;

    /// The `(name, declared type)` of each column of `table`, in order.
    fn table_columns(&self, table: &str) -> Result<Vec<(String, String)>, OCELIOError>;

    /// The select list for all of a table's `(name, type)` `columns`, each named like its column.
    fn select_list(columns: &[(String, String)]) -> String;

    /// The SQL expression reading column `column` as text.
    fn as_text(column: &str) -> String;

    /// Run `sql` and call `each` with every row and the index of each of the `wanted` columns in
    /// it, resolved once for the whole result.
    fn for_each_row<'w>(
        &self,
        sql: &str,
        wanted: &[&'w str],
        each: impl FnMut(&Self::Row<'_>, &[Col<'w>]) -> Result<(), OCELIOError>,
    ) -> Result<(), OCELIOError>;

    /// The single integer `sql` selects.
    fn query_count(&self, sql: &str) -> Result<i64, OCELIOError>;
}

/// One row of a query result, read by column index.
pub(crate) trait SqlRow {
    fn text(&self, col: Col<'_>) -> Result<String, OCELIOError>;

    fn opt_text(&self, col: Col<'_>) -> Result<Option<String>, OCELIOError>;

    fn time(&self, col: Col<'_>) -> Result<DateTime<FixedOffset>, OCELIOError>;

    /// The cell of attribute `name`, declared as `declared`, or `None` if it is SQL `NULL`.
    ///
    /// `NULL` means the row has no value for the attribute (an object's initial row has one
    /// column per attribute of its type, whether or not the object has it), so the caller decides
    /// what it stands for.
    fn attribute(
        &self,
        col: Col<'_>,
        declared: OCELAttributeType,
        name: &str,
    ) -> Result<Option<OCELAttributeValue>, OCELIOError>;
}

/// A column a query reads: its index in the result, if the result has it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Col<'w> {
    pub index: Option<usize>,
    pub name: &'w str,
}

impl Col<'_> {
    pub(crate) fn index(self) -> Result<usize, OCELIOError> {
        self.index
            .ok_or_else(|| OCELIOError::Invalid(format!("no column '{}'", self.name)))
    }

    pub(crate) fn invalid_time(self, value: impl std::fmt::Debug) -> OCELIOError {
        OCELIOError::Invalid(format!("invalid time {value:?} in column '{}'", self.name))
    }
}

/// The [`Col`] of each of `wanted` in a result with columns `names`. Like a lookup by name in
/// either database, the first column whose name matches case-insensitively.
pub(crate) fn resolve_columns<'w>(names: &[impl AsRef<str>], wanted: &[&'w str]) -> Vec<Col<'w>> {
    wanted
        .iter()
        .map(|&name| Col {
            index: names
                .iter()
                .position(|n| n.as_ref().eq_ignore_ascii_case(name)),
            name,
        })
        .collect()
}

/// Import an OCEL from `con`, reporting the rows read and warnings to `ctl` and failing with
/// [`OCELIOError::Cancelled`] once it is cancelled.
pub(crate) fn import_ocel<C: SqlSource>(
    con: &C,
    options: &OCELImportOptions,
    ctl: &Control<'_>,
) -> Result<OCEL, OCELIOError> {
    let mut ocel = OCEL::default();
    let ev_type_map = type_map(con, "event_map_type")?;
    let ob_type_map = type_map(con, "object_map_type")?;
    let renamed = attr_column_renames(con)?;
    let mut object_map: HashMap<String, OCELObject> = HashMap::new();
    let mut event_map: HashMap<String, OCELEvent> = HashMap::new();

    // Counts objects as they are first seen, so a file listing each object once in `object`
    // reaches its total.
    let mut object_rows = RowProgress::new(ctl, "objects", || count_rows(con, "object"));
    let mut change_rows = RowProgress::new(ctl, "object changes", || None);
    // The time of every initial attribute value. Files do not agree on the `ocel_time` of
    // initial rows (not all use the epoch), and some tables have no such column at all.
    let initial_time: DateTime<FixedOffset> = DateTime::UNIX_EPOCH.into();

    for (ob_type, ob_type_ocel) in &ob_type_map {
        let table_name = format!("object_{ob_type}");
        let raw_ob_columns = con.table_columns(&table_name)?;
        let plan = plan_object_table(
            con,
            ob_type,
            &table_name,
            raw_ob_columns,
            &renamed,
            options,
            ctl,
        )
        .map_err(OCELIOError::Invalid)?;
        let attrs = &plan.attributes;

        let wanted: Vec<&str> = [OCEL_ID_COLUMN]
            .into_iter()
            .chain(plan.columns.iter().map(String::as_str))
            .collect();
        con.for_each_row(&plan.initial_query, &wanted, |x, cols| {
            let ob_id = x.text(cols[0])?;
            let mut attributes = Vec::new();
            for ((attr, ty), &col) in attrs.iter().zip(&plan.types).zip(&cols[1..]) {
                if let Some(value) = x.attribute(col, *ty, &attr.name)? {
                    attributes.push(OCELObjectAttribute {
                        name: attr.name.clone(),
                        value,
                        time: initial_time,
                    });
                }
            }
            if add_initial_object_row(&mut object_map, ob_id, ob_type_ocel, attributes, ctl) {
                object_rows.row()?;
            }
            Ok(())
        })?;

        if let Some(changed_query) = &plan.changed_query {
            let mut attr_index: HashMap<&str, usize> = HashMap::new();
            for (i, a) in attrs.iter().enumerate() {
                attr_index.entry(a.name.as_str()).or_insert(i);
            }
            let wanted: Vec<&str> = [OCEL_ID_COLUMN, OCEL_CHANGED_FIELD, OCEL_TIME_COLUMN]
                .into_iter()
                .chain(plan.columns.iter().map(String::as_str))
                .collect();
            con.for_each_row(changed_query, &wanted, |x, cols| {
                change_rows.row()?;
                let ob_id = x.text(cols[0])?;
                let changed_field = x.text(cols[1])?;
                let Some(&i) = attr_index.get(changed_field.as_str()) else {
                    ctl.warn(format_args!(
                        "attribute change of object {ob_id} not added as object type \
                         '{ob_type_ocel}' has no attribute '{changed_field}'."
                    ));
                    return Ok(());
                };
                let time = x.time(cols[2])?;
                // `ocel_changed_field` names the attribute, so a `NULL` here is a change to `Null`.
                let value = x
                    .attribute(cols[3 + i], plan.types[i], &attrs[i].name)?
                    .unwrap_or(OCELAttributeValue::Null);
                let object = match object_map.entry(ob_id) {
                    Entry::Occupied(o) => o.into_mut(),
                    Entry::Vacant(v) => {
                        object_rows.row()?;
                        let id = v.key().clone();
                        v.insert(OCELObject {
                            id,
                            object_type: ob_type_ocel.clone(),
                            attributes: Vec::default(),
                            relationships: Vec::default(),
                        })
                    }
                };
                object.attributes.push(OCELObjectAttribute {
                    name: changed_field,
                    value,
                    time,
                });
                Ok(())
            })?;
        }

        ocel.object_types.push(OCELType {
            name: ob_type_ocel.clone(),
            attributes: plan.attributes,
        });
    }

    if con.has_table("object")? {
        let sql = format!(
            "SELECT {} AS {OCEL_ID_COLUMN}, {} AS {OCEL_TYPE_COLUMN} FROM object",
            C::as_text(OCEL_ID_COLUMN),
            C::as_text(OCEL_TYPE_COLUMN)
        );
        let mut listed = ListedObjects::new(&ocel.object_types);
        con.for_each_row(&sql, &[OCEL_ID_COLUMN, OCEL_TYPE_COLUMN], |x, cols| {
            let id = x.text(cols[0])?;
            let object_type = x.opt_text(cols[1])?;
            if listed.add(
                id,
                object_type,
                &mut object_map,
                &mut ocel.object_types,
                ctl,
            ) {
                object_rows.row()?;
            }
            Ok(())
        })?;
    }
    object_rows.finish();
    change_rows.finish();

    let mut event_rows = RowProgress::new(ctl, "events", || count_rows(con, "event"));
    for (ev_type, ev_type_ocel) in &ev_type_map {
        let table_name = format!("event_{ev_type}");
        let raw_ev_columns = con.table_columns(&table_name)?;
        let select = C::select_list(&raw_ev_columns);
        let (ev_type_attrs, ev_columns) = attribute_columns(&table_name, raw_ev_columns, &renamed);
        let types = attribute_types(&ev_type_attrs);
        let wanted: Vec<&str> = [OCEL_ID_COLUMN, OCEL_TIME_COLUMN]
            .into_iter()
            .chain(ev_columns.iter().map(String::as_str))
            .collect();
        let sql = format!("SELECT {select} FROM {}", quoted_ident(&table_name));
        con.for_each_row(&sql, &wanted, |x, cols| {
            event_rows.row()?;
            let ev_id = x.text(cols[0])?;
            let time = x.time(cols[1])?;
            let mut attributes = Vec::new();
            for ((attr, ty), &col) in ev_type_attrs.iter().zip(&types).zip(&cols[2..]) {
                // `NULL`: the event has no value for this attribute.
                if let Some(value) = x.attribute(col, *ty, &attr.name)? {
                    attributes.push(OCELEventAttribute {
                        name: attr.name.clone(),
                        value,
                    });
                }
            }
            event_map.insert(
                ev_id.clone(),
                OCELEvent {
                    id: ev_id,
                    event_type: ev_type_ocel.clone(),
                    time,
                    attributes,
                    relationships: Vec::default(),
                },
            );
            Ok(())
        })?;
        ocel.event_types.push(OCELType {
            name: ev_type_ocel.clone(),
            attributes: ev_type_attrs,
        });
    }
    event_rows.finish();

    // Without an `event_<type>` table such an event has no time, so it cannot be kept.
    if con.has_table("event")? {
        let undeclared = con.query_count(&format!(
            "SELECT count(*) FROM event WHERE {OCEL_TYPE_COLUMN} NOT IN \
             (SELECT {OCEL_TYPE_COLUMN} FROM event_map_type)"
        ))?;
        if undeclared > 0 {
            ctl.warn(format_args!(
                "{undeclared} events not added as their type is not in event_map_type."
            ));
        }
    }

    let mut e2o_rows = RowProgress::new(ctl, "event-object relations", || {
        count_rows(con, "event_object")
    });
    let wanted = [
        OCEL_E2O_EVENT_ID_COLUMN,
        OCEL_E2O_OBJECT_ID_COLUMN,
        OCEL_REL_QUALIFIER_COLUMN,
    ];
    let mut skipped = 0usize;
    con.for_each_row("SELECT * FROM event_object", &wanted, |x, cols| {
        e2o_rows.row()?;
        let Some((ev_id, object_id, qualifier)) = relation_row(x, cols) else {
            skipped += 1;
            return Ok(());
        };
        if let Some(ev) = event_map.get_mut(&ev_id) {
            ev.relationships.push(OCELRelationship {
                object_id,
                qualifier,
            });
        } else {
            ctl.warn(format_args!(
                "E2O relationship not added as event with ID {ev_id} was not found."
            ));
        }
        Ok(())
    })?;
    e2o_rows.finish();
    warn_skipped_relations(ctl, "event_object", skipped);

    let mut o2o_rows = RowProgress::new(ctl, "object-object relations", || {
        count_rows(con, "object_object")
    });
    let wanted = [
        OCEL_O2O_SOURCE_ID_COLUMN,
        OCEL_O2O_TARGET_ID_COLUMN,
        OCEL_REL_QUALIFIER_COLUMN,
    ];
    let mut skipped = 0usize;
    con.for_each_row("SELECT * FROM object_object", &wanted, |x, cols| {
        o2o_rows.row()?;
        let Some((source_id, object_id, qualifier)) = relation_row(x, cols) else {
            skipped += 1;
            return Ok(());
        };
        if let Some(ob) = object_map.get_mut(&source_id) {
            ob.relationships.push(OCELRelationship {
                object_id,
                qualifier,
            });
        } else {
            ctl.warn(format_args!(
                "O2O relationship not added as object with ID {source_id} was not found."
            ));
        }
        Ok(())
    })?;
    o2o_rows.finish();
    warn_skipped_relations(ctl, "object_object", skipped);

    ocel.objects = object_map.into_values().collect();
    ocel.events = event_map.into_values().collect();
    Ok(ocel)
}

fn texts<R: SqlRow, const N: usize>(x: &R, cols: &[Col<'_>]) -> Result<[String; N], OCELIOError> {
    let mut out: [String; N] = std::array::from_fn(|_| String::new());
    for (o, &col) in out.iter_mut().zip(cols) {
        *o = x.text(col)?;
    }
    Ok(out)
}

/// The two ids and the qualifier of a relation row, or `None` if an id is `NULL` or a cell is not
/// text.
///
/// A `NULL` qualifier is read as `""`, as in the XML and bundle formats.
fn relation_row<R: SqlRow>(x: &R, cols: &[Col<'_>]) -> Option<(String, String, String)> {
    let [from, to] = texts(x, &cols[..2]).ok()?;
    let qualifier = x.opt_text(cols[2]).ok()?.unwrap_or_default();
    Some((from, to, qualifier))
}

fn warn_skipped_relations(ctl: &Control<'_>, table: &str, skipped: usize) {
    if skipped > 0 {
        ctl.warn(format_args!(
            "{skipped} rows of {table} not added as an id is NULL or a cell is not text."
        ));
    }
}

/// `ocel_type_map -> ocel_type` from `table`. A row that does not read as two strings is skipped.
fn type_map<C: SqlSource>(con: &C, table: &str) -> Result<HashMap<String, String>, OCELIOError> {
    let mut map = HashMap::new();
    con.for_each_row(
        &format!("SELECT * FROM {table}"),
        &[OCEL_TYPE_MAP_COLUMN, OCEL_TYPE_COLUMN],
        |x, cols| {
            if let Ok([map_name, name]) = texts(x, cols) {
                map.insert(map_name, name);
            }
            Ok(())
        },
    )?;
    Ok(map)
}

/// The number of rows of `table`, if it exists.
fn count_rows<C: SqlSource>(con: &C, table: &str) -> Option<u64> {
    if !con.has_table(table).ok()? {
        return None;
    }
    let n = con
        .query_count(&format!("SELECT count(*) FROM {}", quoted_ident(table)))
        .ok()?;
    u64::try_from(n).ok()
}

/// `(table, column) -> attribute` for each column [`T_OCEL_ATTR_COLUMNS`] records.
type AttrColumnRenames = HashMap<(String, String), String>;

/// The [`T_OCEL_ATTR_COLUMNS`] rows, if `con` has that table.
fn attr_column_renames<C: SqlSource>(con: &C) -> Result<AttrColumnRenames, OCELIOError> {
    let mut renames = AttrColumnRenames::new();
    if !con.has_table(T_OCEL_ATTR_COLUMNS)? {
        return Ok(renames);
    }
    con.for_each_row(
        &format!("SELECT table_name, column_name, attr_name FROM {T_OCEL_ATTR_COLUMNS}"),
        &["table_name", "column_name", "attr_name"],
        |x, cols| {
            let [table, column, attr] = texts(x, cols)?;
            renames.insert((table, column), attr);
            Ok(())
        },
    )?;
    Ok(renames)
}

/// The attributes stored in the columns of type table `table` with `(name, type)`
/// `raw_columns`, i.e. in every column that is not OCEL-reserved, and the column of each.
fn attribute_columns(
    table: &str,
    raw_columns: Vec<(String, String)>,
    renamed: &AttrColumnRenames,
) -> (Vec<OCELTypeAttribute>, Vec<String>) {
    raw_columns
        .into_iter()
        .filter(|(name, _)| {
            !OCEL_SQL_BASE_COLS
                .iter()
                .any(|base| base.eq_ignore_ascii_case(name))
        })
        .map(|(column, atype)| {
            let name = renamed
                .get(&(table.to_string(), column.clone()))
                .cloned()
                .unwrap_or_else(|| column.clone());
            let attribute = OCELTypeAttribute {
                name,
                value_type: sql_type_to_ocel(&atype).to_type_string(),
            };
            (attribute, column)
        })
        .unzip()
}

fn attribute_types(attributes: &[OCELTypeAttribute]) -> Vec<OCELAttributeType> {
    attributes
        .iter()
        .map(|a| OCELAttributeType::from_type_str(&a.value_type))
        .collect()
}

/// What the columns of one object-type table mean for the import.
struct ObjectTablePlan {
    /// The attributes stored in the table, i.e. in every column that is not OCEL-reserved.
    attributes: Vec<OCELTypeAttribute>,
    /// The type of each of `attributes`.
    types: Vec<OCELAttributeType>,
    /// The column of each of `attributes`.
    columns: Vec<String>,
    /// Query for the rows carrying an object's initial state.
    initial_query: String,
    /// Query for the rows carrying an attribute change, `None` when the table has no
    /// `ocel_changed_field` column.
    changed_query: Option<String>,
}

/// Interpret the `(name, type)` columns of one object-type table of `con`.
///
/// # Errors
/// The table has no `ocel_changed_field` column and `options` are strict.
fn plan_object_table<C: SqlSource>(
    con: &C,
    ob_type: &str,
    table_name: &str,
    raw_ob_columns: Vec<(String, String)>,
    renamed: &AttrColumnRenames,
    options: &OCELImportOptions,
    ctl: &Control<'_>,
) -> Result<ObjectTablePlan, String> {
    let select = C::select_list(&raw_ob_columns);
    let has_column = |wanted: &str| raw_ob_columns.iter().any(|(name, _)| name == wanted);
    let has_changed_field = has_column(OCEL_CHANGED_FIELD);
    if !has_changed_field {
        if options.strict {
            let raw_ob_column_names: Vec<String> = raw_ob_columns
                .iter()
                .map(|(name, _)| name.clone())
                .collect();
            return Err(missing_changed_field_message(
                con.db_path(),
                ob_type,
                table_name,
                &raw_ob_column_names,
            ));
        }
        if has_column(OCEL_CHANGE_FIELD_MISSPELLED) {
            ctl.warn(format_args!(
                "object type '{ob_type}' (table '{table_name}') has no '{OCEL_CHANGED_FIELD}' \
                 column, so '{OCEL_CHANGE_FIELD_MISSPELLED}' is read as a regular attribute. \
                 {MISSPELLING_HINT}"
            ));
        }
    }
    let (attributes, columns) = attribute_columns(table_name, raw_ob_columns, renamed);
    let types = attribute_types(&attributes);
    // Without `ocel_changed_field` there is nothing to exclude: every row is an object's
    // initial (and only) state.
    let table = quoted_ident(table_name);
    let (initial_query, changed_query) = if has_changed_field {
        (
            format!("SELECT {select} FROM {table} WHERE {OCEL_CHANGED_FIELD} IS NULL"),
            Some(format!(
                "SELECT {select} FROM {table} WHERE {OCEL_CHANGED_FIELD} IS NOT NULL"
            )),
        )
    } else {
        (format!("SELECT {select} FROM {table}"), None)
    };
    Ok(ObjectTablePlan {
        attributes,
        types,
        columns,
        initial_query,
        changed_query,
    })
}

/// The diagnostic for an object-type table missing its required `ocel_changed_field` column,
/// pointing out a same-shaped misspelling if one is present.
fn missing_changed_field_message(
    db_path: Option<&str>,
    ob_type: &str,
    table_name: &str,
    raw_column_names: &[String],
) -> String {
    let where_ = db_path.map(|p| format!(" in '{p}'")).unwrap_or_default();
    let mut msg = format!(
        "object type '{ob_type}' (table '{table_name}'{where_}) has no '{OCEL_CHANGED_FIELD}' \
         column. This file does not conform to the OCEL 2.0 SQLite/DuckDB specification, which \
         requires that column on every object-type table."
    );
    if raw_column_names
        .iter()
        .any(|n| n == OCEL_CHANGE_FIELD_MISSPELLED)
    {
        msg.push(' ');
        msg.push_str(MISSPELLING_HINT);
    }
    msg
}

/// Add an object from an initial-state row of its type table. `true` if the object is new.
///
/// A second initial row for the same object only adds the attributes the earlier ones left
/// `NULL`, so the first value of each attribute wins regardless of how many rows there are.
fn add_initial_object_row(
    object_map: &mut HashMap<String, OCELObject>,
    id: String,
    object_type: &str,
    attributes: Vec<OCELObjectAttribute>,
    ctl: &Control<'_>,
) -> bool {
    match object_map.entry(id) {
        Entry::Vacant(v) => {
            let id = v.key().clone();
            v.insert(OCELObject {
                id,
                object_type: object_type.to_string(),
                attributes,
                relationships: Vec::default(),
            });
            true
        }
        Entry::Occupied(mut o) => {
            let ob = o.get_mut();
            if ob.object_type != object_type {
                ctl.warn(format_args!(
                    "object {} of type '{}' also has a row in the table of type \
                     '{object_type}', which is ignored.",
                    ob.id, ob.object_type
                ));
                return false;
            }
            for a in attributes {
                if !ob.attributes.iter().any(|b| b.name == a.name) {
                    ob.attributes.push(a);
                }
            }
            false
        }
    }
}

/// Adds the `(id, type)` rows of the `object` table that no type table has, as objects without
/// attributes, and their types if no type table declares them.
///
/// The specification lists every object there, while the table of a type without attributes
/// may be empty.
struct ListedObjects {
    known_types: HashSet<String>,
}

impl ListedObjects {
    fn new(object_types: &[OCELType]) -> Self {
        Self {
            known_types: object_types.iter().map(|t| t.name.clone()).collect(),
        }
    }

    /// `true` if no type table had object `id`.
    fn add(
        &mut self,
        id: String,
        object_type: Option<String>,
        object_map: &mut HashMap<String, OCELObject>,
        object_types: &mut Vec<OCELType>,
        ctl: &Control<'_>,
    ) -> bool {
        let Entry::Vacant(v) = object_map.entry(id) else {
            return false;
        };
        let Some(object_type) = object_type else {
            ctl.warn(format_args!(
                "object {} not added as it has no type.",
                v.key()
            ));
            return true;
        };
        if self.known_types.insert(object_type.clone()) {
            object_types.push(OCELType {
                name: object_type.clone(),
                attributes: Vec::new(),
            });
        }
        let id = v.key().clone();
        v.insert(OCELObject {
            id,
            object_type,
            attributes: Vec::new(),
            relationships: Vec::new(),
        });
        true
    }
}

#[cfg(test)]
mod progress_tests {
    use std::sync::Mutex;

    use crate::core::event_data::object_centric::ocel_struct::OCEL;
    use crate::test_utils::get_test_data_path;
    use crate::utils::control::{Control, Progress};

    use super::{import_ocel, SqlSource};

    fn import_reporting<C: SqlSource>(con: &C) -> (OCEL, Vec<Progress>) {
        let seen = Mutex::new(Vec::new());
        let ctl = Control::new().on_progress(|p| seen.lock().unwrap().push(p));
        let ocel = import_ocel(con, &Default::default(), &ctl).unwrap();
        drop(ctl);
        (ocel, seen.into_inner().unwrap())
    }

    /// The last report of each stage, in order of first report.
    fn last_per_stage(progress: &[Progress]) -> Vec<Progress> {
        let mut last: Vec<Progress> = Vec::new();
        for p in progress {
            match last.iter_mut().find(|l| l.stage == p.stage) {
                Some(l) => *l = p.clone(),
                None => last.push(p.clone()),
            }
        }
        last
    }

    fn assert_every_stage_completes(ocel: &OCEL, progress: &[Progress]) {
        let last = last_per_stage(progress);
        let stages: Vec<&str> = last.iter().map(|p| p.stage.as_ref()).collect();
        assert_eq!(
            stages,
            [
                "Importing objects",
                "Importing object changes",
                "Importing events",
                "Importing event-object relations",
                "Importing object-object relations",
            ]
        );
        for p in &last {
            if p.stage != "Importing object changes" {
                assert_eq!(Some(p.done), p.total, "{p:?}");
            }
        }
        assert_eq!(last[0].done, ocel.objects.len() as u64);
        assert_eq!(last[2].done, ocel.events.len() as u64);
    }

    #[cfg(feature = "ocel-sqlite")]
    #[test]
    fn sqlite_import_completes_every_stage() {
        let path = get_test_data_path()
            .join("ocel")
            .join("order-management.sqlite");
        let con = rusqlite::Connection::open(path).unwrap();
        let (ocel, progress) = import_reporting(&con);
        assert_every_stage_completes(&ocel, &progress);
    }

    /// Truck, Handling Unit and Forklift objects are only in the `object` table.
    #[cfg(feature = "ocel-sqlite")]
    #[test]
    fn objects_only_in_the_object_table_count_as_progress() {
        let path = get_test_data_path()
            .join("ocel")
            .join("ContainerLogistics.sqlite");
        if !path.exists() {
            eprintln!("Skipping: {} not found", path.display());
            return;
        }
        let con = rusqlite::Connection::open(path).unwrap();
        let (ocel, progress) = import_reporting(&con);
        assert_every_stage_completes(&ocel, &progress);
    }

    #[cfg(feature = "ocel-duckdb")]
    #[test]
    fn duckdb_import_completes_every_stage() {
        use crate::core::event_data::object_centric::ocel_sql::export_ocel_duckdb_to_path;
        let ocel = <OCEL as crate::Importable>::import_from_path(
            get_test_data_path()
                .join("ocel")
                .join("order-management.json"),
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.duckdb");
        export_ocel_duckdb_to_path(&ocel, &path).unwrap();
        let con = ::duckdb::Connection::open(&path).unwrap();
        let (back, progress) = import_reporting(&con);
        assert_every_stage_completes(&back, &progress);
    }

    #[cfg(feature = "ocel-sqlite")]
    #[test]
    fn null_qualifiers_read_as_empty_and_null_ids_are_skipped() {
        let con = rusqlite::Connection::open_in_memory().unwrap();
        con.execute_batch(
            "
            CREATE TABLE event_map_type (ocel_type_map TEXT, ocel_type TEXT);
            CREATE TABLE object_map_type (ocel_type_map TEXT, ocel_type TEXT);
            CREATE TABLE event_object (ocel_event_id TEXT, ocel_object_id TEXT, ocel_qualifier TEXT);
            CREATE TABLE object_object (ocel_source_id TEXT, ocel_target_id TEXT, ocel_qualifier TEXT);
            CREATE TABLE object_item (ocel_id TEXT, ocel_time TIMESTAMP, ocel_changed_field TEXT);
            CREATE TABLE event_go (ocel_id TEXT, ocel_time TIMESTAMP);
            INSERT INTO object_map_type VALUES ('item', 'item');
            INSERT INTO event_map_type VALUES ('go', 'go');
            INSERT INTO object_item VALUES ('i1', '1970-01-01T00:00:00+00:00', NULL);
            INSERT INTO object_item VALUES ('i2', '1970-01-01T00:00:00+00:00', NULL);
            INSERT INTO event_go VALUES ('e1', '2024-01-01T00:00:00+00:00');
            INSERT INTO event_object VALUES ('e1', 'i1', NULL);
            INSERT INTO event_object VALUES (NULL, 'i1', 'q');
            INSERT INTO event_object VALUES ('e1', NULL, 'q');
            INSERT INTO object_object VALUES ('i1', 'i2', NULL);
            INSERT INTO object_object VALUES (NULL, 'i2', 'q');
            ",
        )
        .unwrap();
        let warnings = Mutex::new(Vec::new());
        let progress = Mutex::new(Vec::new());
        let ctl = Control::new()
            .on_warning(|w| warnings.lock().unwrap().push(w.to_string()))
            .on_progress(|p| progress.lock().unwrap().push(p));
        let ocel = import_ocel(&con, &Default::default(), &ctl).unwrap();
        drop(ctl);

        let rels = |rels: &[crate::core::event_data::object_centric::OCELRelationship]| {
            rels.iter()
                .map(|r| (r.object_id.clone(), r.qualifier.clone()))
                .collect::<Vec<_>>()
        };
        let i1 = ocel.objects.iter().find(|o| o.id == "i1").unwrap();
        assert_eq!(
            rels(&ocel.events[0].relationships),
            [("i1".into(), String::new())]
        );
        assert_eq!(rels(&i1.relationships), [("i2".into(), String::new())]);

        let warnings = warnings.into_inner().unwrap();
        let about = |table: &str| warnings.iter().filter(|w| w.contains(table)).count();
        assert_eq!(
            (about("event_object"), about("object_object")),
            (1, 1),
            "{warnings:?}"
        );
        assert!(warnings
            .iter()
            .any(|w| w.starts_with("2 rows of event_object")));
        assert!(warnings
            .iter()
            .any(|w| w.starts_with("1 rows of object_object")));

        let last = last_per_stage(&progress.into_inner().unwrap());
        let done = |stage: &str| last.iter().find(|p| p.stage == stage).unwrap().done;
        assert_eq!(done("Importing event-object relations"), 3);
        assert_eq!(done("Importing object-object relations"), 2);
    }
}
