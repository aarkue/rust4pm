//! Streaming sink writing the schema.
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, FixedOffset};
use duckdb::{Appender, Connection, ToSql};

use crate::core::event_data::object_centric::appendable::AppendableOCEL;
use crate::core::event_data::object_centric::ocel_struct::{
    OCELAttributeType, OCELEventAttribute, OCELObjectAttribute, OCELRelationship, OCELType,
};

use super::tables::{
    add_events_column, build_events_table, T_E2O, T_EVENTS, T_EVENT_ATTR_META, T_O2O, T_OBJECTS,
    T_OBJECT_ATTR_CHANGES, T_OBJECT_ATTR_META,
};
use super::value::{datetime_to_duck_timestamp, ocel_value_to_duck, to_sql_value};

/// Streaming sink for the schema. Holds one appender per table, each borrowing `&'con Connection`.
/// The `events` appender is created once the wide schema is known.
pub(crate) struct DuckDbOcelSink<'con> {
    events: Option<Appender<'con>>,
    event_attr_meta: Option<Appender<'con>>,
    object_attr_meta: Option<Appender<'con>>,
    e2o: Option<Appender<'con>>,
    objects: Option<Appender<'con>>,
    object_attribute_changes: Option<Appender<'con>>,
    o2o: Option<Appender<'con>>,
    con: &'con Connection,
    /// Event-attr schema accumulated over all `declare_event_type` calls: name -> type, widened
    /// via [`OCELAttributeType::coalesce`] when event types disagree (integer + float -> float).
    ev_attr_types: HashMap<String, OCELAttributeType>,
    /// `ev_attr_types` frozen into ordered columns on the first `append_event`, then extended by
    /// any column added on the fly. The type is both the SQL column type and the conversion target.
    ev_columns: Vec<(String, OCELAttributeType)>,
    /// name -> index into `ev_columns` (for building a full-width row per event).
    ev_col_index: HashMap<String, usize>,
    events_created: bool,
    /// Undeclared attribute name -> event types already written to `event_attr_meta`,
    /// so a recurring name costs one meta row per event type, not one per event.
    undeclared_meta_written: HashMap<String, HashSet<String>>,
    /// Event type -> attribute names it declared, so an attribute another type declared still
    /// gets this type's own `event_attr_meta` row when it first shows up on one of its events.
    declared_ev_attrs: HashMap<String, HashSet<String>>,
}

impl<'con> DuckDbOcelSink<'con> {
    pub(crate) fn new(con: &'con Connection) -> Result<Self, duckdb::Error> {
        Ok(Self {
            events: None,
            event_attr_meta: Some(con.appender(T_EVENT_ATTR_META)?),
            object_attr_meta: Some(con.appender(T_OBJECT_ATTR_META)?),
            e2o: Some(con.appender(T_E2O)?),
            objects: Some(con.appender(T_OBJECTS)?),
            object_attribute_changes: Some(con.appender(T_OBJECT_ATTR_CHANGES)?),
            o2o: Some(con.appender(T_O2O)?),
            con,
            ev_attr_types: HashMap::new(),
            ev_columns: Vec::new(),
            ev_col_index: HashMap::new(),
            events_created: false,
            undeclared_meta_written: HashMap::new(),
            declared_ev_attrs: HashMap::new(),
        })
    }

    /// Add a column for an attribute no event type declared, so its values are not dropped.
    /// `DuckDB` refuses `ALTER TABLE` while an appender is open, hence the flush/reopen, which
    /// happens once per new name rather than per event.
    fn add_undeclared_event_column(
        &mut self,
        name: &str,
        event_type: &str,
    ) -> Result<(), duckdb::Error> {
        if let Some(mut a) = self.events.take() {
            a.flush()?;
        }
        // Nothing declared this attribute, so nothing says what it holds.
        let ty = OCELAttributeType::String;
        self.con.execute_batch(&add_events_column(name, ty))?;
        self.events = Some(self.con.appender(T_EVENTS)?);
        // `ADD COLUMN` appends at the end, matching the push onto `ev_columns`.
        self.ev_col_index
            .insert(name.to_string(), self.ev_columns.len());
        self.ev_columns.push((name.to_string(), ty));
        self.record_undeclared_meta(name, event_type)
    }

    /// Record an undeclared attribute in `event_attr_meta`, once per event type, so the
    /// reader and `generate_type_views` both pick it up.
    fn record_undeclared_meta(
        &mut self,
        name: &str,
        event_type: &str,
    ) -> Result<(), duckdb::Error> {
        if self
            .undeclared_meta_written
            .get(name)
            .is_some_and(|types| types.contains(event_type))
        {
            return Ok(());
        }
        self.event_attr_meta.as_mut().unwrap().append_row([
            event_type,
            name,
            OCELAttributeType::String.as_type_str(),
        ])?;
        self.undeclared_meta_written
            .entry(name.to_string())
            .or_default()
            .insert(event_type.to_string());
        Ok(())
    }

    /// Create the wide `events` table (attribute columns sorted for determinism) and its appender.
    /// Idempotent: called on the first `append_event` and, for an event-less import, in `finalize`.
    fn ensure_events_created(&mut self) -> Result<(), duckdb::Error> {
        if self.events_created {
            return Ok(());
        }
        let mut names: Vec<String> = self.ev_attr_types.keys().cloned().collect();
        names.sort();
        self.ev_columns = names
            .into_iter()
            .map(|n| {
                let ty = self.ev_attr_types[&n];
                (n, ty)
            })
            .collect();
        self.ev_col_index = self
            .ev_columns
            .iter()
            .enumerate()
            .map(|(i, (n, _))| (n.clone(), i))
            .collect();
        self.con
            .execute_batch(&build_events_table(&self.ev_columns))?;
        self.events = Some(self.con.appender(T_EVENTS)?);
        self.events_created = true;
        Ok(())
    }
}

impl<'con> AppendableOCEL for DuckDbOcelSink<'con> {
    type Error = duckdb::Error;

    // Accumulate the wide event-attr schema and record the declared attributes in
    // `event_attr_meta` for round-trip export. A name declared with different types across
    // event types widens to the type covering both (see `coalesce`).
    fn declare_event_type(&mut self, t: OCELType) -> Result<(), Self::Error> {
        let meta = self.event_attr_meta.as_mut().unwrap();
        let declared = self.declared_ev_attrs.entry(t.name.clone()).or_default();
        for a in &t.attributes {
            let ty = OCELAttributeType::from_type_str(&a.value_type);
            self.ev_attr_types
                .entry(a.name.clone())
                .and_modify(|existing| *existing = existing.coalesce(ty))
                .or_insert(ty);
            declared.insert(a.name.clone());
            meta.append_row([&t.name, &a.name, &a.value_type])?;
        }
        Ok(())
    }
    // Record the declared object attributes in `object_attr_meta`. Object attributes live in the
    // EAV change table, so a type with no instances, or an attribute no row ever wrote, has
    // nowhere else to be observed from.
    fn declare_object_type(&mut self, t: OCELType) -> Result<(), Self::Error> {
        let meta = self.object_attr_meta.as_mut().unwrap();
        for a in &t.attributes {
            meta.append_row([&t.name, &a.name, &a.value_type])?;
        }
        Ok(())
    }

    fn append_event(
        &mut self,
        id: String,
        event_type: &str,
        time: DateTime<FixedOffset>,
        attributes: Vec<OCELEventAttribute>,
        relationships: Vec<OCELRelationship>,
    ) -> Result<(), Self::Error> {
        self.ensure_events_created()?;

        // Give undeclared attributes a column before the row is sized, so none are dropped.
        // Declarations are tracked per event type: a name only another type declared has a
        // column already but still needs this type's own meta row.
        for a in &attributes {
            if !self.ev_col_index.contains_key(&a.name) {
                self.add_undeclared_event_column(&a.name, event_type)?;
            } else if !self
                .declared_ev_attrs
                .get(event_type)
                .is_some_and(|names| names.contains(&a.name))
            {
                self.record_undeclared_meta(&a.name, event_type)?;
            }
        }

        // Full-width row: [id, ocel_type, time, <attr cols...>], NULL where unfilled.
        let mut row: Vec<duckdb::types::Value> = Vec::with_capacity(3 + self.ev_columns.len());
        row.push(duckdb::types::Value::Text(id));
        row.push(duckdb::types::Value::Text(event_type.to_string()));
        row.push(datetime_to_duck_timestamp(time));
        row.extend(std::iter::repeat_n(
            duckdb::types::Value::Null,
            self.ev_columns.len(),
        ));
        for a in &attributes {
            let idx = self.ev_col_index[&a.name];
            row[3 + idx] = ocel_value_to_duck(&a.value, self.ev_columns[idx].1);
        }
        let params: Vec<&dyn ToSql> = row.iter().map(|v| v as &dyn ToSql).collect();
        self.events
            .as_mut()
            .unwrap()
            .append_row(params.as_slice())?;

        let id_ref = match &row[0] {
            duckdb::types::Value::Text(s) => s.as_str(),
            _ => unreachable!(),
        };
        let e2o = self.e2o.as_mut().unwrap();
        for r in &relationships {
            e2o.append_row([id_ref, &r.object_id, &r.qualifier])?;
        }
        Ok(())
    }

    fn append_object(
        &mut self,
        id: String,
        object_type: &str,
        attributes: Vec<OCELObjectAttribute>,
        relationships: Vec<OCELRelationship>,
    ) -> Result<(), Self::Error> {
        self.objects
            .as_mut()
            .unwrap()
            .append_row((&id, &object_type))?;
        let oa = self.object_attribute_changes.as_mut().unwrap();
        for a in &attributes {
            let (value, value_type) = to_sql_value(&a.value);
            let value: &str = value.as_ref();
            let t = datetime_to_duck_timestamp(a.time);
            oa.append_row((&id, &a.name, &t, value, value_type))?;
        }
        let o2o = self.o2o.as_mut().unwrap();
        for r in &relationships {
            o2o.append_row([&id, &r.object_id, &r.qualifier])?;
        }
        Ok(())
    }

    fn finalize(&mut self) -> Result<(), Self::Error> {
        // Ensure `events` exists even for an event-less import, so readers always find the table.
        self.ensure_events_created()?;
        // Flush explicitly (not just drop) so write errors such as PRIMARY KEY violations
        // surface here instead of being swallowed by `Drop`.
        if let Some(mut a) = self.events.take() {
            a.flush()?;
        }
        if let Some(mut a) = self.event_attr_meta.take() {
            a.flush()?;
        }
        if let Some(mut a) = self.object_attr_meta.take() {
            a.flush()?;
        }
        if let Some(mut a) = self.e2o.take() {
            a.flush()?;
        }
        if let Some(mut a) = self.objects.take() {
            a.flush()?;
        }
        if let Some(mut a) = self.object_attribute_changes.take() {
            a.flush()?;
        }
        if let Some(mut a) = self.o2o.take() {
            a.flush()?;
        }
        // Indexes are created by `run_import` as the final step, after the optional file-size
        // rewrite that would otherwise drop them.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::tables::create_schema;
    use super::*;
    use crate::core::event_data::object_centric::ocel_struct::{
        OCELAttributeValue, OCELTypeAttribute,
    };

    #[test]
    fn append_one_event_and_object() {
        let con = Connection::open_in_memory().unwrap();
        create_schema(&con).unwrap();
        {
            let mut sink = DuckDbOcelSink::new(&con).unwrap();
            sink.declare_event_type(OCELType {
                name: "pay".into(),
                attributes: vec![OCELTypeAttribute {
                    name: "amount".into(),
                    value_type: "float".into(),
                }],
            })
            .unwrap();
            let t = chrono::Utc::now().fixed_offset();
            sink.append_event(
                "e1".into(),
                "pay",
                t,
                vec![OCELEventAttribute {
                    name: "amount".into(),
                    value: OCELAttributeValue::Float(9.5),
                }],
                vec![OCELRelationship {
                    object_id: "o1".into(),
                    qualifier: "reg".into(),
                }],
            )
            .unwrap();
            sink.append_object(
                "o1".into(),
                "order",
                vec![OCELObjectAttribute {
                    name: "prio".into(),
                    value: OCELAttributeValue::Integer(1),
                    time: t,
                }],
                vec![],
            )
            .unwrap();
            sink.finalize().unwrap();
        }
        let n_ev: i64 = con
            .query_row("SELECT count(*) FROM events", [], |r| r.get(0))
            .unwrap();
        // The event attribute is stored as a typed wide column on `events`.
        let amount: f64 = con
            .query_row(r#"SELECT "amount" FROM events WHERE id = 'e1'"#, [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(amount, 9.5);
        let n_meta: i64 = con
            .query_row("SELECT count(*) FROM event_attr_meta", [], |r| r.get(0))
            .unwrap();
        let n_e2o: i64 = con
            .query_row("SELECT count(*) FROM e2o", [], |r| r.get(0))
            .unwrap();
        let n_ob: i64 = con
            .query_row("SELECT count(*) FROM objects", [], |r| r.get(0))
            .unwrap();
        let n_oa: i64 = con
            .query_row("SELECT count(*) FROM object_attribute_changes", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!((n_ev, n_meta, n_e2o, n_ob, n_oa), (1, 1, 1, 1, 1));
    }

    #[test]
    fn conflicting_declared_types_widen() {
        // integer + float must widen to DOUBLE, staying numeric rather than collapsing to VARCHAR.
        let con = Connection::open_in_memory().unwrap();
        create_schema(&con).unwrap();
        {
            let mut sink = DuckDbOcelSink::new(&con).unwrap();
            for (ty, vt) in [("a", "integer"), ("b", "float")] {
                sink.declare_event_type(OCELType {
                    name: ty.into(),
                    attributes: vec![OCELTypeAttribute {
                        name: "n".into(),
                        value_type: vt.into(),
                    }],
                })
                .unwrap();
            }
            let t = chrono::Utc::now().fixed_offset();
            sink.append_event(
                "e1".into(),
                "a",
                t,
                vec![OCELEventAttribute {
                    name: "n".into(),
                    value: OCELAttributeValue::Integer(7),
                }],
                vec![],
            )
            .unwrap();
            sink.finalize().unwrap();
        }
        let col_type: String = con
            .query_row(
                "SELECT data_type FROM information_schema.columns \
                 WHERE table_name = 'events' AND column_name = 'n'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(col_type, "DOUBLE");
        // The integer value survives as a number, not as text.
        let n: f64 = con
            .query_row(r#"SELECT "n" FROM events WHERE id = 'e1'"#, [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(n, 7.0);
    }

    #[test]
    fn undeclared_event_attrs_get_a_column_instead_of_being_dropped() {
        let con = Connection::open_in_memory().unwrap();
        create_schema(&con).unwrap();
        let t = chrono::Utc::now().fixed_offset();
        {
            let mut sink = DuckDbOcelSink::new(&con).unwrap();
            // "pay" declares only `amount`. The events below also carry `note` and `qty`,
            // which no event type declares.
            sink.declare_event_type(OCELType {
                name: "pay".into(),
                attributes: vec![OCELTypeAttribute {
                    name: "amount".into(),
                    value_type: "float".into(),
                }],
            })
            .unwrap();
            sink.append_event(
                "e1".into(),
                "pay",
                t,
                vec![
                    OCELEventAttribute {
                        name: "amount".into(),
                        value: OCELAttributeValue::Float(9.5),
                    },
                    OCELEventAttribute {
                        name: "note".into(),
                        value: OCELAttributeValue::String("hello".into()),
                    },
                ],
                vec![],
            )
            .unwrap();
            // A second event adds another new name and re-uses `note` with a non-string
            // value: VARCHAR holds both.
            sink.append_event(
                "e2".into(),
                "ship",
                t,
                vec![
                    OCELEventAttribute {
                        name: "note".into(),
                        value: OCELAttributeValue::Integer(7),
                    },
                    OCELEventAttribute {
                        name: "qty".into(),
                        value: OCELAttributeValue::Integer(3),
                    },
                ],
                vec![],
            )
            .unwrap();
            sink.finalize().unwrap();
        }

        let note1: String = con
            .query_row(r#"SELECT "note" FROM events WHERE id = 'e1'"#, [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(note1, "hello");
        let (note2, qty2): (String, String) = con
            .query_row(
                r#"SELECT "note", "qty" FROM events WHERE id = 'e2'"#,
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((note2.as_str(), qty2.as_str()), ("7", "3"));
        // The declared column keeps its declared type, and e2 left it unset.
        let amount1: f64 = con
            .query_row(r#"SELECT "amount" FROM events WHERE id = 'e1'"#, [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(amount1, 9.5);
        let qty1: Option<String> = con
            .query_row(r#"SELECT "qty" FROM events WHERE id = 'e1'"#, [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(qty1, None);

        // Each on-the-fly column is recorded in `event_attr_meta` once per event type, so
        // the reader re-declares it and `generate_type_views` projects it.
        let mut meta: Vec<(String, String)> = con
            .prepare("SELECT event_type, attr_name FROM event_attr_meta ORDER BY 1, 2")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        meta.sort();
        assert_eq!(
            meta,
            vec![
                ("pay".to_string(), "amount".to_string()),
                ("pay".to_string(), "note".to_string()),
                ("ship".to_string(), "note".to_string()),
                ("ship".to_string(), "qty".to_string()),
            ]
        );
    }

    #[test]
    fn duplicate_event_id_surfaces_as_error() {
        let con = Connection::open_in_memory().unwrap();
        create_schema(&con).unwrap();
        let mut sink = DuckDbOcelSink::new(&con).unwrap();
        let t = chrono::Utc::now().fixed_offset();
        sink.append_event("dup".into(), "pay", t, vec![], vec![])
            .unwrap();
        // Whether the PRIMARY KEY violation surfaces at append or at finalize depends on
        // duckdb's appender buffering, but it must not be silently dropped.
        let append_result = sink.append_event("dup".into(), "pay", t, vec![], vec![]);
        let result = append_result.and_then(|()| sink.finalize());
        assert!(
            result.is_err(),
            "duplicate id must surface as an error, not succeed silently"
        );
    }
}
