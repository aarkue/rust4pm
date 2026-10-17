//! Getting case-centric data into the object-centric world: a blueprint for the traditional
//! case/activity/timestamp table, and a writer for an already-parsed [`EventLog`].

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, FixedOffset};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::blueprint::{
    Blueprint, DuplicateObjectPolicy, IdRendering, InlineObjectRef, Mapping, MappingEntry,
    MissingEndpointPolicy, Node, NodeOp, ObjectEndpoint, Target,
};
use super::expr::{AttributeMapping, TimestampSource, ValueExpression};
use super::sink::{ExtractionSink, SinkError};
use super::slim_sink::SlimOcelSink;
use crate::core::event_data::case_centric::constants::{ACTIVITY_NAME, TRACE_ID_NAME};
use crate::core::event_data::case_centric::{
    Attribute, AttributeValue, Event, EventLog, Trace, XESEditableAttribute,
};
use crate::core::event_data::object_centric::linked_ocel::{LinkedOCELAccess, SlimLinkedOCEL};
use crate::core::event_data::object_centric::{
    OCELAttributeType, OCELAttributeValue, OCELTypeAttribute, OCEL,
};

/// A flat table with one row per event, the usual shape of a case-centric log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FlatEventTable {
    /// Source id, resolved to a connection at execution time.
    pub source_id: String,
    /// Table name.
    pub table: String,
    /// Column identifying the case.
    pub case_id: String,
    /// Column naming the activity, which becomes the event type.
    pub activity: String,
    /// Column holding the event's timestamp.
    pub timestamp: String,
    /// Object type to give the case, conventionally `Case`.
    pub case_object_type: String,
    /// Case-level columns. Recorded as static attributes, never change-tracked.
    pub case_attributes: Vec<AttributeMapping>,
    /// Event-level columns.
    pub event_attributes: Vec<AttributeMapping>,
}

impl Blueprint {
    /// Build a blueprint for a flat event table.
    ///
    /// One node and one event mapping, whose inline object reference creates the case objects --
    /// hence `on_missing_endpoint: Create`. A second mapping appears only when there are case
    /// attributes, and is static (`timestamp: None`), so a case-level column repeated across the
    /// case's events does not store one identical timed value per event.
    #[must_use]
    pub fn from_flat_event_table(spec: FlatEventTable) -> Blueprint {
        let node_id = "events".to_string();
        let case_endpoint = || ObjectEndpoint {
            id: ValueExpression::Column {
                column: spec.case_id.clone(),
            },
            object_type: Some(ValueExpression::Constant {
                value: spec.case_object_type.clone(),
            }),
            split: None,
        };

        let mut mappings = vec![MappingEntry::Single(Mapping {
            node: node_id.clone(),
            label: Some("events".into()),
            when: None,
            target: Target::Event {
                event_type: ValueExpression::Column {
                    column: spec.activity.clone(),
                },
                id: None,
                timestamp: TimestampSource::column(spec.timestamp.clone()),
                attributes: spec.event_attributes.clone(),
                objects: vec![InlineObjectRef {
                    object: case_endpoint(),
                    qualifier: Some(ValueExpression::Constant {
                        value: "case".into(),
                    }),
                }],
            },
        })];

        if !spec.case_attributes.is_empty() {
            mappings.push(MappingEntry::Single(Mapping {
                node: node_id.clone(),
                label: Some("cases".into()),
                when: None,
                target: Target::Object {
                    object_type: ValueExpression::Constant {
                        value: spec.case_object_type.clone(),
                    },
                    id: ValueExpression::Column {
                        column: spec.case_id.clone(),
                    },
                    timestamp: None,
                    attributes: spec.case_attributes.clone(),
                },
            }));
        }

        Blueprint {
            version: super::MODEL_VERSION,
            id_rendering: IdRendering::Raw,
            nodes: vec![Node {
                id: node_id,
                label: Some(spec.table.clone()),
                op: NodeOp::Source {
                    source_id: spec.source_id,
                    table: spec.table,
                },
            }],
            mappings,
            on_missing_endpoint: MissingEndpointPolicy::Create,
            on_duplicate_object: DuplicateObjectPolicy::FirstWins,
        }
    }
}

/// The object type given to the one object each trace becomes.
pub const CASE_OBJECT_TYPE: &str = "Case";

/// The qualifier on every event-to-case relation [`write_event_log_to_sink`] writes.
pub const CASE_QUALIFIER: &str = "case";

/// The event type given to an event whose [`ACTIVITY_NAME`] attribute is missing or not a string.
const UNKNOWN_ACTIVITY: &str = "UNKNOWN";

/// The XES key an event's timestamp is read from.
const TIMESTAMP_NAME: &str = "time:timestamp";

/// The one OCEL value a XES value maps to.
///
/// OCEL attribute values do not nest, so a list or container keeps its debug rendering rather
/// than being dropped. Lossy, but not silently empty.
fn xes_attribute_to_ocel(value: &AttributeValue) -> OCELAttributeValue {
    match value {
        AttributeValue::String(s) => OCELAttributeValue::String(s.clone()),
        AttributeValue::Date(t) => OCELAttributeValue::Time(*t),
        AttributeValue::Int(i) => OCELAttributeValue::Integer(*i),
        AttributeValue::Float(f) => OCELAttributeValue::Float(*f),
        AttributeValue::Boolean(b) => OCELAttributeValue::Boolean(*b),
        AttributeValue::ID(uuid) => OCELAttributeValue::String(uuid.to_string()),
        AttributeValue::List(attrs) => OCELAttributeValue::String(format!("{attrs:?}")),
        AttributeValue::Container(attrs) => OCELAttributeValue::String(format!("{attrs:?}")),
        AttributeValue::None() => OCELAttributeValue::Null,
    }
}

fn activity_of(event: &Event) -> &str {
    event
        .attributes
        .get_by_key(ACTIVITY_NAME)
        .and_then(|a| a.value.try_as_string())
        .map_or(UNKNOWN_ACTIVITY, String::as_str)
}

/// An event's instant, or `None` when [`TIMESTAMP_NAME`] is absent or holds something other
/// than a date.
fn timestamp_of(event: &Event) -> Option<DateTime<FixedOffset>> {
    event
        .attributes
        .get_by_key(TIMESTAMP_NAME)
        .and_then(|a| a.value.try_as_date())
        .copied()
}

/// The attribute names one type carries, in first-seen order, each typed to cover every value
/// seen under that name. This is the schema a sink needs before any entity of the type.
///
/// Widened rather than pinned to the first value seen, which would type an attribute from a row
/// that happens to hold nothing, or an integer, where later rows hold text.
#[derive(Debug, Default)]
struct TypeAttributes<'a> {
    order: Vec<&'a str>,
    types: HashMap<&'a str, OCELAttributeType>,
}

impl<'a> TypeAttributes<'a> {
    fn observe(&mut self, attribute: &'a Attribute) {
        let observed = xes_attribute_to_ocel(&attribute.value).get_type();
        match self.types.entry(attribute.key.as_str()) {
            Entry::Occupied(mut e) => {
                let widened = e.get().coalesce(observed);
                e.insert(widened);
            }
            Entry::Vacant(e) => {
                self.order.push(attribute.key.as_str());
                e.insert(observed);
            }
        }
    }

    /// The declaration, valid only once every value has been observed.
    fn declared(&self) -> Vec<OCELTypeAttribute> {
        self.order
            .iter()
            .map(|name| OCELTypeAttribute::new(name, &self.types[name]))
            .collect()
    }
}

/// A case id no earlier trace has taken.
///
/// Two traces sharing a `concept:name` are still two cases, so the repeat is disambiguated
/// rather than merged: an OCEL holding two objects under one id is not a well-formed OCEL, and
/// a sink rejects the second outright.
fn unique_case_id(trace: &Trace, trace_index: usize, used: &mut HashSet<String>) -> String {
    let base = trace
        .attributes
        .get_by_key(TRACE_ID_NAME)
        .and_then(|a| a.value.try_as_string())
        .cloned()
        .unwrap_or_else(|| format!("ob_{trace_index}"));
    if used.insert(base.clone()) {
        return base;
    }
    let mut n = 2;
    loop {
        let candidate = format!("{base}~{n}");
        if used.insert(candidate.clone()) {
            return candidate;
        }
        n += 1;
    }
}

/// What [`write_event_log_to_sink`] wrote, and what it could not represent faithfully.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EventLogWriteReport {
    /// Events handed to the sink.
    pub events_written: u64,
    /// How many of those had no usable `time:timestamp` (absent, or not a date) and were written
    /// at the Unix epoch.
    ///
    /// Counted here because an epoch timestamp is indistinguishable from a real one in the log
    /// itself, unlike the `UNKNOWN` type a nameless event takes.
    pub events_without_timestamp: u64,
}

/// Write a case-centric log into an object-centric sink: one object per case.
///
/// Each trace becomes one object of type [`CASE_OBJECT_TYPE`] carrying the trace's attributes,
/// timed at the epoch because a case-level attribute has no instant of its own. Each of its
/// events becomes an event of the type its [`ACTIVITY_NAME`] names, carrying the event's own
/// attributes, related back to its case under [`CASE_QUALIFIER`]. Events are numbered `ev_<n>`
/// across the whole log. A case takes its [`TRACE_ID_NAME`], or `ob_<trace index>` when it has
/// none.
///
/// The log is read twice: once for each type's attribute schema, which a sink must be told before
/// it is given anything of that type, and once to write. Only the schema is held between.
///
/// Does not call [`ExtractionSink::finalize`], so several logs can go into one sink. A
/// file-backed sink is not readable until finalized. [`event_log_to_slim_ocel`] and
/// [`event_log_to_ocel`] do both.
///
/// # Errors
///
/// Returns whatever the sink returns.
pub fn write_event_log_to_sink<S: ExtractionSink>(
    log: &EventLog,
    sink: &mut S,
) -> Result<EventLogWriteReport, SinkError> {
    let mut case_type = TypeAttributes::default();
    let mut event_type_names: Vec<&str> = Vec::new();
    let mut event_types: Vec<TypeAttributes<'_>> = Vec::new();
    let mut event_type_index: HashMap<&str, usize> = HashMap::new();

    for trace in &log.traces {
        for attribute in &trace.attributes {
            case_type.observe(attribute);
        }
        for event in &trace.events {
            let activity = activity_of(event);
            let index = *event_type_index.entry(activity).or_insert_with(|| {
                event_type_names.push(activity);
                event_types.push(TypeAttributes::default());
                event_types.len() - 1
            });
            for attribute in &event.attributes {
                event_types[index].observe(attribute);
            }
        }
    }

    sink.declare_object_type(CASE_OBJECT_TYPE, &case_type.declared())?;
    for (name, attributes) in event_type_names.iter().zip(&event_types) {
        sink.declare_event_type(name, &attributes.declared())?;
    }

    let epoch: DateTime<FixedOffset> = DateTime::UNIX_EPOCH.into();
    let mut used_case_ids: HashSet<String> = HashSet::with_capacity(log.traces.len());
    let mut report = EventLogWriteReport::default();

    for (trace_index, trace) in log.traces.iter().enumerate() {
        let case_id = unique_case_id(trace, trace_index, &mut used_case_ids);
        let attributes: Vec<_> = trace
            .attributes
            .iter()
            .map(|a| (a.key.clone(), epoch, xes_attribute_to_ocel(&a.value)))
            .collect();
        let case = sink.add_object(CASE_OBJECT_TYPE, &case_id, &attributes)?;

        for event in &trace.events {
            let attributes: Vec<_> = event
                .attributes
                .iter()
                .map(|a| (a.key.clone(), xes_attribute_to_ocel(&a.value)))
                .collect();
            let id = format!("ev_{}", report.events_written);
            report.events_written += 1;
            let time = match timestamp_of(event) {
                Some(t) => t,
                None => {
                    report.events_without_timestamp += 1;
                    epoch
                }
            };
            let written = sink.add_event(activity_of(event), time, &id, &attributes)?;
            // `add_e2o`'s contract: exactly one `resolve_object` for this row's own object,
            // immediately before it. A deferring sink has no id index and needs the adjacency to
            // link the ask to the relation that made it.
            let related = sink
                .resolve_object(&case_id, Some(CASE_OBJECT_TYPE))
                .into_ref()
                .unwrap_or_else(|| case.clone());
            sink.add_e2o(&written, &related, CASE_QUALIFIER)?;
        }
    }

    Ok(report)
}

/// [`write_event_log_to_sink`] into an in-memory [`SlimOcelSink`], finalized.
///
/// Discards the [`EventLogWriteReport`]. A caller that needs it drives the sink itself.
///
/// # Errors
///
/// Returns whatever the sink returns.
pub fn event_log_to_slim_ocel(log: &EventLog) -> Result<SlimLinkedOCEL, SinkError> {
    let mut sink = SlimOcelSink::new();
    write_event_log_to_sink(log, &mut sink)?;
    sink.finalize()?;
    Ok(sink.into_ocel())
}

/// [`event_log_to_slim_ocel`], materialised as a plain [`OCEL`].
///
/// # Errors
///
/// Returns whatever the sink returns.
pub fn event_log_to_ocel(log: &EventLog) -> Result<OCEL, SinkError> {
    Ok(event_log_to_slim_ocel(log)?.construct_ocel())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::extraction::blueprint::*;
    use crate::core::event_data::object_centric::extraction::catalog::{
        ExtractionCatalog, TableSchema,
    };
    use crate::core::event_data::object_centric::extraction::validate::validate;

    #[test]
    fn flat_event_table_crosses_a_bindings_boundary() {
        // FlatEventTable is a public input to Blueprint::from_flat_event_table, so a caller on
        // the other side of a bindings boundary needs to be able to build and send one.
        let spec = FlatEventTable {
            source_id: "db".into(),
            table: "events".into(),
            case_id: "case_id".into(),
            activity: "activity".into(),
            timestamp: "ts".into(),
            case_object_type: "Case".into(),
            case_attributes: vec![],
            event_attributes: vec![],
        };
        let json = serde_json::to_string(&spec).expect("serialize");
        let back: FlatEventTable = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, spec);
        let schema = schemars::schema_for!(FlatEventTable);
        assert!(serde_json::to_value(&schema).is_ok());
    }

    #[test]
    fn builds_a_valid_single_mapping_blueprint() {
        let bp = Blueprint::from_flat_event_table(FlatEventTable {
            source_id: "db".into(),
            table: "events".into(),
            case_id: "case_id".into(),
            activity: "activity".into(),
            timestamp: "ts".into(),
            case_object_type: "Case".into(),
            case_attributes: vec![],
            event_attributes: vec![],
        });

        assert_eq!(bp.nodes.len(), 1);
        assert_eq!(bp.mappings.len(), 1);
        assert_eq!(bp.on_missing_endpoint, MissingEndpointPolicy::Create);

        let catalog = ExtractionCatalog::new().with_table(
            "db",
            TableSchema::new(
                "events",
                [
                    ("case_id", "TEXT", false),
                    ("activity", "TEXT", false),
                    ("ts", "TEXT", false),
                ],
            ),
        );
        assert_eq!(validate(&bp, &catalog), vec![]);
    }

    #[test]
    fn case_attributes_land_on_a_static_object_mapping() {
        // Static, not change-tracked: a case-level column repeated across every one of the
        // case's events would otherwise store one identical timed value per event.
        let bp = Blueprint::from_flat_event_table(FlatEventTable {
            source_id: "db".into(),
            table: "events".into(),
            case_id: "case_id".into(),
            activity: "activity".into(),
            timestamp: "ts".into(),
            case_object_type: "Case".into(),
            case_attributes: vec![AttributeMapping {
                source_column: "region".into(),
                name: "region".into(),
                value_type: None,
            }],
            event_attributes: vec![],
        });

        assert_eq!(bp.mappings.len(), 2);
        let object_mapping = bp.mappings.iter().find_map(|m| match m {
            MappingEntry::Single(m) => match &m.target {
                Target::Object {
                    timestamp,
                    attributes,
                    ..
                } => Some((timestamp, attributes)),
                _ => None,
            },
            MappingEntry::Ordered { .. } => None,
        });
        let (timestamp, attributes) = object_mapping.expect("an object mapping for the case");
        assert!(timestamp.is_none(), "case attributes must be static");
        assert_eq!(attributes.len(), 1);
    }

    fn at(rfc3339: &str) -> DateTime<FixedOffset> {
        DateTime::parse_from_rfc3339(rfc3339).expect("a timestamp")
    }

    fn event(activity: &str, time: &str, extra: Vec<(&str, AttributeValue)>) -> Event {
        let mut event = Event::new(activity.to_string());
        event
            .attributes
            .add_to_attributes(TIMESTAMP_NAME.to_string(), AttributeValue::Date(at(time)));
        for (key, value) in extra {
            event.attributes.add_to_attributes(key.to_string(), value);
        }
        event
    }

    fn trace(case_id: &str, extra: Vec<(&str, AttributeValue)>, events: Vec<Event>) -> Trace {
        let mut trace = Trace::new();
        trace.attributes.add_to_attributes(
            TRACE_ID_NAME.to_string(),
            AttributeValue::String(case_id.to_string()),
        );
        for (key, value) in extra {
            trace.attributes.add_to_attributes(key.to_string(), value);
        }
        trace.events = events;
        trace
    }

    /// One case with two events, a case-level attribute and an event-level one.
    fn two_event_log() -> EventLog {
        EventLog {
            traces: vec![trace(
                "case-1",
                vec![("region", AttributeValue::String("EU".into()))],
                vec![
                    event(
                        "create",
                        "2020-01-01T00:00:00Z",
                        vec![("amount", AttributeValue::Int(7))],
                    ),
                    event("close", "2020-01-02T00:00:00Z", vec![]),
                ],
            )],
            ..EventLog::default()
        }
    }

    #[test]
    fn every_trace_becomes_one_case_object_its_events_hang_off() {
        let locel = event_log_to_slim_ocel(&two_event_log()).expect("convert");

        assert_eq!(locel.get_ob_types().collect::<Vec<_>>(), [CASE_OBJECT_TYPE]);
        assert_eq!(locel.get_num_obs(), 1);
        assert_eq!(locel.get_num_evs(), 2);

        let case = locel.get_ob_by_id("case-1").expect("the case object");
        let mut attached: Vec<_> = locel
            .get_e2o_rev(case)
            .map(|(qualifier, ev)| (qualifier, locel.get_ev_type_of(ev)))
            .collect();
        attached.sort_unstable();
        assert_eq!(attached, [("case", "close"), ("case", "create")]);
    }

    #[test]
    fn case_attributes_land_on_the_object_and_event_attributes_on_the_event() {
        let locel = event_log_to_slim_ocel(&two_event_log()).expect("convert");

        let case = locel.get_ob_by_id("case-1").expect("the case object");
        assert_eq!(
            locel.get_ob_attr_vals(case, "region").collect::<Vec<_>>(),
            [(
                &DateTime::UNIX_EPOCH.into(),
                &OCELAttributeValue::String("EU".into())
            )]
        );
        // The event-level attribute is not on the case, and the case-level one is not on the
        // event: neither type was ever declared as carrying the other's.
        assert!(locel.get_ob_attr_vals(case, "amount").next().is_none());

        let created = locel.get_ev_by_id("ev_0").expect("the first event");
        assert_eq!(locel.get_ev_type_of(created), "create");
        assert_eq!(
            locel.get_ev_time(created),
            &at("2020-01-01T00:00:00Z"),
            "the event keeps its own timestamp"
        );
        assert_eq!(
            locel.get_ev_attr_val(created, "amount"),
            Some(&OCELAttributeValue::Integer(7))
        );
        assert_eq!(locel.get_ev_attr_val(created, "region"), None);

        // An event of a type that never carried the attribute still declares it, since the type
        // is declared from every event of that type at once. An attribute no event carried is
        // not declared.
        let closed = locel.get_ev_by_id("ev_1").expect("the second event");
        assert_eq!(
            locel.get_ev_attr_val(closed, "amount"),
            None,
            "'amount' belongs to 'create', which 'close' is not"
        );
    }

    #[test]
    fn traces_sharing_a_name_stay_two_cases() {
        let log = EventLog {
            traces: vec![
                trace(
                    "dup",
                    vec![],
                    vec![event("a", "2020-01-01T00:00:00Z", vec![])],
                ),
                trace(
                    "dup",
                    vec![],
                    vec![event("b", "2020-01-02T00:00:00Z", vec![])],
                ),
            ],
            ..EventLog::default()
        };

        let locel = event_log_to_slim_ocel(&log).expect("convert");
        assert_eq!(locel.get_num_obs(), 2);
        let ids: HashSet<&str> = locel.get_all_obs().map(|o| locel.get_ob_id(o)).collect();
        assert_eq!(ids.len(), 2, "two objects must not share an id");
        assert!(ids.contains("dup"));
    }

    #[test]
    fn a_xes_file_becomes_a_slim_linked_ocel() {
        use crate::Importable;

        let path = crate::test_utils::get_test_data_path()
            .join("xes")
            .join("small-example.xes");
        let log = EventLog::import_from_path(&path).expect("import the fixture");
        let locel = event_log_to_slim_ocel(&log).expect("convert");

        assert_eq!(locel.get_num_obs(), log.traces.len());
        assert_eq!(
            locel.get_num_evs(),
            log.traces.iter().map(|t| t.events.len()).sum::<usize>()
        );
        assert!(locel.get_ob_by_id("Trace number one").is_some());
        for ev in locel.get_all_evs() {
            assert_eq!(
                locel.get_e2o(ev).count(),
                1,
                "every event belongs to exactly one case"
            );
        }
        // The second trace's events carry no `concept:name`, so they fall back rather than being
        // dropped.
        assert!(locel.get_ev_types().any(|t| t == UNKNOWN_ACTIVITY));
    }

    /// The point of writing this against [`ExtractionSink`]: the deferring, file-backed sink
    /// takes the same log with no OCEL in between, and settles the case relations at finalize.
    #[cfg(feature = "ocel-duckdb")]
    #[test]
    fn the_same_log_streams_into_a_file_backed_sink() {
        use super::super::duckdb_sink::DuckDbSink;
        use crate::core::event_data::object_centric::ocel_sql::read_ocel_from_duckdb;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("sink.duckdb");
        let mut sink = DuckDbSink::new(&path).expect("open sink");
        write_event_log_to_sink(&two_event_log(), &mut sink).expect("write");
        ExtractionSink::finalize(&mut sink).expect("finalize");

        let con = duckdb::Connection::open(&path).expect("reopen");
        let ocel = read_ocel_from_duckdb(&con).expect("read back");
        assert_eq!(ocel.objects.len(), 1);
        assert_eq!(ocel.objects[0].id, "case-1");
        assert_eq!(ocel.objects[0].object_type, CASE_OBJECT_TYPE);
        assert_eq!(ocel.events.len(), 2);
        for event in &ocel.events {
            assert_eq!(
                event
                    .relationships
                    .iter()
                    .map(|r| (r.object_id.as_str(), r.qualifier.as_str()))
                    .collect::<Vec<_>>(),
                [("case-1", CASE_QUALIFIER)]
            );
        }
    }

    #[cfg(feature = "bindings")]
    #[test]
    fn the_registry_reads_xes_bytes_as_object_centric_data() {
        use crate::bindings::{RegistryItem, RegistryItemKind};

        let path = crate::test_utils::get_test_data_path()
            .join("xes")
            .join("small-example.xes");
        let bytes = std::fs::read(&path).expect("read the fixture");

        assert!(
            RegistryItemKind::SlimLinkedOCEL
                .known_import_formats()
                .iter()
                .any(|f| f.extension == "xes"),
            "a host asking what the kind accepts must be told about xes"
        );

        let from_path = RegistryItem::load_from_path(
            &RegistryItemKind::SlimLinkedOCEL,
            &path.to_string_lossy(),
        )
        .expect("load a xes path as a slim linked OCEL");
        assert!(matches!(from_path, RegistryItem::SlimLinkedOCEL(_)));

        let slim = RegistryItem::load_from_bytes(&RegistryItemKind::SlimLinkedOCEL, &bytes, "xes")
            .expect("load a xes as a slim linked OCEL");
        match slim {
            RegistryItem::SlimLinkedOCEL(locel) => {
                assert!(locel.get_ob_by_id("Trace number one").is_some());
            }
            other => panic!("expected a SlimLinkedOCEL, got {other:?}"),
        }

        let ocel = RegistryItem::load_from_bytes(&RegistryItemKind::OCEL, &bytes, "xes")
            .expect("load a xes as an OCEL");
        match ocel {
            RegistryItem::OCEL(ocel) => {
                assert_eq!(
                    ocel.object_types
                        .iter()
                        .map(|t| &t.name)
                        .collect::<Vec<_>>(),
                    [CASE_OBJECT_TYPE]
                );
            }
            other => panic!("expected an OCEL, got {other:?}"),
        }
    }
}
