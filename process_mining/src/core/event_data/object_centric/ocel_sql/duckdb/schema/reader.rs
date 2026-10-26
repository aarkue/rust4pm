//! Read a `DuckDB` database back into an [`OCEL`] (consolidated schema).
use std::collections::HashMap;

use chrono::{DateTime, FixedOffset};
use duckdb::Connection;

use crate::core::event_data::object_centric::appendable::AppendableOCEL;
use crate::core::event_data::object_centric::io::OCELIOError;
use crate::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;
use crate::core::event_data::object_centric::ocel_struct::{
    OCELEventAttribute, OCELObjectAttribute, OCELRelationship, OCELType, OCELTypeAttribute, OCEL,
};
use crate::core::event_data::timestamp_utils::parse_timestamp;
use macros_process_mining::register_binding;

use super::value::{duck_timestamp_to_datetime, duck_value_to_ocel, from_sql_value};

/// Read an [`OCEL`] from a `DuckDB` database written by
/// [`stream_ocel_file_to_duckdb`](super::stream::stream_ocel_file_to_duckdb). Eager: the
/// whole log is materialized in memory.
///
/// Reads the consolidated schema, not the per-type table layout that
/// [`import_ocel_duckdb_from_con`](crate::core::event_data::object_centric::ocel_sql::import_ocel_duckdb_from_con)
/// expects.
pub fn read_ocel_from_duckdb(con: &Connection) -> Result<OCEL, OCELIOError> {
    let mut ocel = OCEL {
        event_types: Vec::new(),
        object_types: Vec::new(),
        events: Vec::new(),
        objects: Vec::new(),
    };
    ocel.read_from_duckdb(con)?;
    Ok(ocel)
}

/// Read an [`OCEL`] from a `DuckDB` database file in the consolidated schema, i.e. one written by
/// [`stream_ocel_file_to_duckdb`](super::stream::stream_ocel_file_to_duckdb).
///
/// Databases in the per-type table layout of the OCEL 2.0 standard are read by
/// [`import_ocel_duckdb_from_path`](crate::core::event_data::object_centric::ocel_sql::import_ocel_duckdb_from_path)
/// instead.
#[register_binding(name = "read_consolidated_ocel_from_duckdb", stringify_error)]
pub fn read_consolidated_ocel_from_duckdb_path(
    db_path: impl AsRef<std::path::Path>,
) -> Result<OCEL, OCELIOError> {
    let con = Connection::open(db_path)?;
    read_ocel_from_duckdb(&con)
}

/// Read a [`SlimLinkedOCEL`] from a `DuckDB` database file in the consolidated schema, i.e. one
/// written by [`stream_ocel_file_to_duckdb`](super::stream::stream_ocel_file_to_duckdb).
///
/// Rows are read into the linked structure directly, without building an [`OCEL`] first.
#[register_binding(name = "read_consolidated_slim_ocel_from_duckdb", stringify_error)]
pub fn read_consolidated_slim_ocel_from_duckdb_path(
    db_path: impl AsRef<std::path::Path>,
) -> Result<SlimLinkedOCEL, OCELIOError> {
    let con = Connection::open(db_path)?;
    SlimLinkedOCEL::from_duckdb(&con)
}

/// Whether `con` holds the consolidated schema instead of the OCEL 2.0 per-type layout.
///
/// Both are written to a `.duckdb`, so only the tables tell them apart. These five are fixed by
/// [`create_schema`](super::tables::create_schema), and the per-type layout has none of them.
pub(crate) fn is_consolidated_schema(con: &Connection) -> Result<bool, duckdb::Error> {
    let found: i64 = con.query_row(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name IN \
         ('events', 'objects', 'e2o', 'o2o', 'event_attr_meta')",
        [],
        |r| r.get(0),
    )?;
    Ok(found == 5)
}

/// Read a `DuckDB` schema database into any [`AppendableOCEL`] sink.
///
/// Currently buffers `e2o`/`o2o` relationships and object attributes into maps.
/// TODO: Implement real row-streaming with SQL joins etc.
pub(crate) trait DuckDbReadInto: AppendableOCEL {
    fn read_from_duckdb(&mut self, con: &Connection) -> Result<(), OCELIOError>
    where
        Self::Error: Into<OCELIOError>,
    {
        // Event-type attributes come from the persisted `event_attr_meta`.
        for t in collect_types(
            con,
            "SELECT event_type, attr_name, attr_type FROM event_attr_meta",
            "SELECT DISTINCT ocel_type FROM events",
        )? {
            self.declare_event_type(t).map_err(Into::into)?;
        }
        // Object-type attributes come from `object_attr_meta`, unioned with the types observed in
        // the change rows: a declaration no row ever wrote exists only in the meta table.
        for t in collect_types(
            con,
            r#"SELECT object_type, attr_name, attr_type FROM object_attr_meta
               UNION
               SELECT DISTINCT o.ocel_type, oa.name, oa.value_type
               FROM objects o JOIN object_attribute_changes oa ON oa.id = o.id"#,
            r#"SELECT DISTINCT ocel_type FROM objects
               UNION
               SELECT DISTINCT object_type FROM object_attr_meta"#,
        )? {
            self.declare_object_type(t).map_err(Into::into)?;
        }

        // Buffer relationships + object attributes (append_* wants them up front).
        let mut e2o: HashMap<String, Vec<OCELRelationship>> = HashMap::new();
        {
            let mut stmt = con.prepare("SELECT event_id, object_id, qualifier FROM e2o")?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?;
            for row in rows {
                let (event_id, object_id, qualifier) = row?;
                e2o.entry(event_id).or_default().push(OCELRelationship {
                    object_id,
                    qualifier,
                });
            }
        }
        let mut o2o: HashMap<String, Vec<OCELRelationship>> = HashMap::new();
        {
            let mut stmt = con.prepare("SELECT source_id, target_id, qualifier FROM o2o")?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?;
            for row in rows {
                let (source_id, target_id, qualifier) = row?;
                o2o.entry(source_id).or_default().push(OCELRelationship {
                    object_id: target_id,
                    qualifier,
                });
            }
        }
        let mut ob_attrs: HashMap<String, Vec<OCELObjectAttribute>> = HashMap::new();
        {
            let mut stmt = con.prepare(
                r#"SELECT id, name, "time", value, value_type FROM object_attribute_changes"#,
            )?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, duckdb::types::Value>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                ))
            })?;
            for row in rows {
                let (id, name, time_val, value, value_type) = row?;
                let time = value_to_datetime(&time_val)?;
                ob_attrs.entry(id).or_default().push(OCELObjectAttribute {
                    name,
                    value: from_sql_value(&value, &value_type),
                    time,
                });
            }
        }

        // Objects first so events' e2o object references already exist.
        {
            let mut stmt = con.prepare("SELECT id, ocel_type FROM objects")?;
            let rows =
                stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
            for row in rows {
                let (id, ocel_type) = row?;
                let attributes = ob_attrs.remove(&id).unwrap_or_default();
                let relationships = o2o.remove(&id).unwrap_or_default();
                self.append_object(id, &ocel_type, attributes, relationships)
                    .map_err(Into::into)?;
            }
        }

        {
            let col_names: Vec<String> = {
                let mut stmt = con.prepare(
                    "SELECT column_name FROM information_schema.columns \
                     WHERE table_name = 'events' ORDER BY ordinal_position",
                )?;
                stmt.query_map([], |r| r.get::<_, String>(0))?
                    .collect::<Result<_, _>>()?
            };
            let n = col_names.len();
            let mut stmt = con.prepare("SELECT * FROM events")?;
            let rows = stmt.query_map([], |r| {
                let mut vals: Vec<duckdb::types::Value> = Vec::with_capacity(n);
                for i in 0..n {
                    vals.push(r.get::<_, duckdb::types::Value>(i)?);
                }
                Ok(vals)
            })?;
            for row in rows {
                let vals = row?;
                let id = as_text(&vals[0], "id")?;
                let ocel_type = as_text(&vals[1], "ocel_type")?;
                let time = value_to_datetime(&vals[2])?;
                let attributes = (3..n)
                    .filter(|&i| !matches!(vals[i], duckdb::types::Value::Null))
                    .map(|i| OCELEventAttribute {
                        name: super::tables::event_attr_name(&col_names[i]).into_owned(),
                        value: duck_value_to_ocel(vals[i].clone()),
                    })
                    .collect();
                let relationships = e2o.remove(&id).unwrap_or_default();
                self.append_event(id, &ocel_type, time, attributes, relationships)
                    .map_err(Into::into)?;
            }
        }

        self.finalize().map_err(Into::into)?;
        Ok(())
    }
}

impl<A: AppendableOCEL> DuckDbReadInto for A {}

/// Read a `TEXT` cell, erroring (naming `column`) instead of silently collapsing distinct
/// non-text values to `""`, which would be fatal for an `id`/`ocel_type` cell.
fn as_text(v: &duckdb::types::Value, column: &str) -> Result<String, duckdb::Error> {
    match v {
        duckdb::types::Value::Text(s) => Ok(s.clone()),
        _ => Err(duckdb::Error::InvalidColumnType(
            0,
            format!("expected TEXT in column {column:?}, got {v:?}"),
            duckdb::types::Type::Text,
        )),
    }
}

/// Read a timestamp column. Text is accepted too, for databases written by other tooling.
fn value_to_datetime(v: &duckdb::types::Value) -> Result<DateTime<FixedOffset>, duckdb::Error> {
    match v {
        duckdb::types::Value::Timestamp(tu, t) => {
            duck_timestamp_to_datetime(*tu, *t).ok_or(duckdb::Error::InvalidQuery)
        }
        duckdb::types::Value::Text(s) => {
            parse_timestamp(s, None, false).map_err(|_| duckdb::Error::InvalidQuery)
        }
        _ => Err(duckdb::Error::InvalidQuery),
    }
}

/// Build `OCELType`s: every name from `all_types_sql`, `attributes` from `attr_sql`'s
/// `(type, name, value_type)` rows.
///
/// One entry per distinct `(type, name, value_type)`: a name reported under several types (e.g.
/// `object_attr_meta`'s declaration and the types actually observed in
/// `object_attribute_changes` disagree) yields one `OCELTypeAttribute` per type observed for it,
/// not a single widened one, since there is no declaration-level type to prefer over another.
fn collect_types(
    con: &Connection,
    attr_sql: &str,
    all_types_sql: &str,
) -> Result<Vec<OCELType>, duckdb::Error> {
    let mut attrs_by_type: HashMap<String, Vec<OCELTypeAttribute>> = HashMap::new();
    {
        let mut stmt = con.prepare(attr_sql)?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        for row in rows {
            let (ocel_type, name, value_type) = row?;
            let entry = attrs_by_type.entry(ocel_type).or_default();
            if !entry
                .iter()
                .any(|a| a.name == name && a.value_type == value_type)
            {
                entry.push(OCELTypeAttribute { name, value_type });
            }
        }
    }

    let mut types: Vec<OCELType> = Vec::new();
    let mut stmt = con.prepare(all_types_sql)?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    for row in rows {
        let name = row?;
        let attributes = attrs_by_type.remove(&name).unwrap_or_default();
        types.push(OCELType { name, attributes });
    }
    Ok(types)
}

#[cfg(all(test, feature = "ocel-duckdb"))]
mod tests {
    use std::collections::HashSet;

    use chrono::{DateTime, FixedOffset};

    use super::super::stream::stream_ocel_file_to_duckdb;
    use crate::core::event_data::object_centric::linked_ocel::{
        IndexLinkedOCEL, LinkedOCELAccess, SlimLinkedOCEL,
    };
    use crate::core::event_data::object_centric::ocel_json::import_ocel_json_path;
    use crate::test_utils::get_test_data_path;

    fn order_management_path() -> std::path::PathBuf {
        get_test_data_path()
            .join("ocel")
            .join("order-management.json")
    }

    // Object attributes live in the EAV change table, so an uninstantiated declaration survives
    // only in `object_attr_meta`.
    #[test]
    fn an_object_type_with_no_instances_and_an_unwritten_attribute_survive_a_roundtrip() {
        use crate::core::event_data::object_centric::appendable::AppendableOCEL;
        use crate::core::event_data::object_centric::ocel_struct::{OCELType, OCELTypeAttribute};

        let con = duckdb::Connection::open_in_memory().unwrap();
        super::super::tables::create_schema(&con).unwrap();
        {
            let mut sink = super::super::sink::DuckDbOcelSink::new(&con).unwrap();
            sink.declare_object_type(OCELType {
                name: "CreditNote".to_string(),
                attributes: vec![OCELTypeAttribute {
                    name: "reason".to_string(),
                    value_type: "string".to_string(),
                }],
            })
            .unwrap();
            sink.finalize().unwrap();
        }

        let loaded = super::read_ocel_from_duckdb(&con).unwrap();
        let ty = loaded
            .object_types
            .iter()
            .find(|t| t.name == "CreditNote")
            .expect("a declared object type with no instances must survive");
        assert_eq!(
            ty.attributes
                .iter()
                .map(|a| (a.name.as_str(), a.value_type.as_str()))
                .collect::<Vec<_>>(),
            vec![("reason", "string")]
        );
    }

    // ocel2-p2p is used because its event types declare attributes, unlike order-management.
    #[test]
    fn event_attr_declarations_and_values_roundtrip() {
        let src = get_test_data_path().join("ocel").join("ocel2-p2p.json");
        let reference = IndexLinkedOCEL::from_ocel(import_ocel_json_path(&src).unwrap());

        let out = get_test_data_path()
            .join("export")
            .join("p2p-attr-roundtrip.duckdb");
        let _ = std::fs::remove_file(&out);
        stream_ocel_file_to_duckdb(&src, &out).unwrap();
        let con = duckdb::Connection::open(&out).unwrap();
        let loaded = IndexLinkedOCEL::from_ocel(super::read_ocel_from_duckdb(&con).unwrap());

        // (1) Declared `attributes` (name + value_type) round-trip via event_attr_meta.
        let mut types_checked = 0;
        for tname in reference.get_ev_types() {
            let want: HashSet<(String, String)> = reference
                .get_ev_type(tname)
                .expect("reference type")
                .attributes
                .iter()
                .map(|a| (a.name.clone(), a.value_type.clone()))
                .collect();
            let got: HashSet<(String, String)> = loaded
                .get_ev_type(tname)
                .unwrap_or_else(|| panic!("loaded missing event type {tname}"))
                .attributes
                .iter()
                .map(|a| (a.name.clone(), a.value_type.clone()))
                .collect();
            assert_eq!(got, want, "declared attributes mismatch for type {tname}");
            if !want.is_empty() {
                types_checked += 1;
            }
        }
        assert!(types_checked > 1, "expected >1 type carrying attributes");

        // (2) Attribute values round-trip: first valued event per type; >1 type, >1 attr.
        let mut typed_events_checked = 0;
        let mut attr_values_checked = 0;
        for tname in reference.get_ev_types() {
            let Some(ref_ev) = reference
                .get_evs_of_type(tname)
                .find(|e| reference.get_ev_attrs(*e).next().is_some())
            else {
                continue;
            };
            let id = reference.get_ev_id(ref_ev).to_string();
            let loaded_ev = loaded.get_ev_by_id(&id).expect("loaded has event");
            let mut names: Vec<String> =
                reference.get_ev_attrs(ref_ev).map(str::to_string).collect();
            names.sort();
            for name in &names {
                let want = reference.get_ev_attr_val(ref_ev, name);
                let got = loaded.get_ev_attr_val(&loaded_ev, name);
                assert_eq!(got, want, "event {id} attr {name} value mismatch");
                attr_values_checked += 1;
            }
            typed_events_checked += 1;
        }
        assert!(
            typed_events_checked > 1,
            "expected >1 type with a valued event"
        );
        assert!(
            attr_values_checked > 1,
            "expected >1 attribute value checked"
        );
    }

    #[test]
    fn from_duckdb_matches_json() {
        let src = order_management_path();
        let reference = IndexLinkedOCEL::from_ocel(import_ocel_json_path(&src).unwrap());

        let out = get_test_data_path()
            .join("export")
            .join("from-duckdb-parity.duckdb");
        let _ = std::fs::remove_file(&out);
        stream_ocel_file_to_duckdb(&src, &out).unwrap();
        let con = duckdb::Connection::open(&out).unwrap();
        let loaded = IndexLinkedOCEL::from_ocel(super::read_ocel_from_duckdb(&con).unwrap());

        assert_eq!(loaded.get_num_evs(), reference.get_num_evs());
        assert_eq!(loaded.get_num_obs(), reference.get_num_obs());
        assert_eq!(
            loaded.get_ev_types().count(),
            reference.get_ev_types().count()
        );
        assert_eq!(
            loaded.get_ob_types().count(),
            reference.get_ob_types().count()
        );

        // Spot-check a sample of event ids: type, time, and e2o set parity.
        let sample_ev_ids: Vec<String> = reference
            .get_all_evs()
            .take(25)
            .map(|e| reference.get_ev_id(e).to_string())
            .collect();
        assert!(!sample_ev_ids.is_empty());
        for id in &sample_ev_ids {
            let ref_ev = reference.get_ev_by_id(id).expect("reference has event");
            let loaded_ev = loaded.get_ev_by_id(id).expect("loaded should have event");

            assert_eq!(
                loaded.get_ev_type_of(loaded_ev),
                reference.get_ev_type_of(ref_ev),
                "event type mismatch for {id}"
            );
            assert_eq!(
                loaded.get_ev_time(loaded_ev),
                reference.get_ev_time(ref_ev),
                "event time mismatch for {id}"
            );

            let ref_e2o: HashSet<(String, String)> = reference
                .get_e2o(ref_ev)
                .map(|(q, o)| (q.to_string(), reference.get_ob_id(*o).to_string()))
                .collect();
            let loaded_e2o: HashSet<(String, String)> = loaded
                .get_e2o(loaded_ev)
                .map(|(q, o)| (q.to_string(), loaded.get_ob_id(*o).to_string()))
                .collect();
            assert_eq!(loaded_e2o, ref_e2o, "e2o mismatch for {id}");
        }

        // Spot-check an object with attributes (order-management carries them on objects,
        // not on events).
        let ref_ob_with_attr = reference
            .get_all_obs()
            .find_map(|ob| {
                let name = reference
                    .get_ocel_ref()
                    .objects
                    .iter()
                    .find(|o| o.id == reference.get_ob_id(ob))?
                    .attributes
                    .first()?
                    .name
                    .clone();
                let vals: Vec<_> = reference.get_ob_attr_vals(ob, &name).collect();
                if vals.is_empty() {
                    None
                } else {
                    Some((reference.get_ob_id(ob).to_string(), name))
                }
            })
            .expect("order-management should have an object with an attribute");
        let (ob_id, attr_name) = ref_ob_with_attr;

        let ref_ob = reference.get_ob_by_id(&ob_id).unwrap();
        let loaded_ob = loaded
            .get_ob_by_id(&ob_id)
            .expect("loaded should have object");

        let mut ref_vals: Vec<(DateTime<FixedOffset>, String)> = reference
            .get_ob_attr_vals(ref_ob, &attr_name)
            .map(|(t, v)| (*t, v.to_string()))
            .collect();
        let mut loaded_vals: Vec<(DateTime<FixedOffset>, String)> = loaded
            .get_ob_attr_vals(loaded_ob, &attr_name)
            .map(|(t, v)| (*t, v.to_string()))
            .collect();
        ref_vals.sort();
        loaded_vals.sort();
        assert_eq!(
            loaded_vals, ref_vals,
            "object attr value/time set mismatch for {ob_id}/{attr_name}"
        );
    }

    // A non-text id/ocel_type cell would otherwise silently collapse to "", making distinct
    // events collide instead of surfacing the schema mismatch.
    #[test]
    fn as_text_errors_on_non_text_cell_naming_the_column() {
        let err = super::as_text(&duckdb::types::Value::Int(5), "id").unwrap_err();
        assert!(
            format!("{err}").contains("id"),
            "error should name the offending column: {err}"
        );
        assert!(super::as_text(&duckdb::types::Value::Null, "ocel_type").is_err());
        assert_eq!(
            super::as_text(&duckdb::types::Value::Text("e1".to_string()), "id").unwrap(),
            "e1"
        );
    }

    #[test]
    fn slim_from_duckdb_matches_from_ocel() {
        let src = order_management_path();
        let reference = SlimLinkedOCEL::from_ocel(import_ocel_json_path(&src).unwrap());

        let out = get_test_data_path()
            .join("export")
            .join("slim-from-duckdb.duckdb");
        let _ = std::fs::remove_file(&out);
        stream_ocel_file_to_duckdb(&src, &out).unwrap();
        let con = duckdb::Connection::open(&out).unwrap();
        let loaded = SlimLinkedOCEL::from_duckdb(&con).unwrap();

        assert_eq!(loaded.get_num_evs(), reference.get_num_evs());
        assert_eq!(loaded.get_num_obs(), reference.get_num_obs());
        assert_eq!(
            loaded.get_ev_types().count(),
            reference.get_ev_types().count()
        );
        assert_eq!(
            loaded.get_ob_types().count(),
            reference.get_ob_types().count()
        );
    }
}
