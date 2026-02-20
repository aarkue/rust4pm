//!  Functionality to Flatten OCEL on an Object Type
use macros_process_mining::register_binding;

use crate::core::{
    event_data::{
        case_centric::event_log_struct::{
            Attribute, AttributeValue, Event, Trace, XESEditableAttribute,
        },
        object_centric::{linked_ocel::LinkedOCELAccess, ocel_struct::OCELAttributeValue},
    },
    EventLog,
};

#[register_binding]
/// Flatten an OCEL on a specific object type, resulting in a case-centric Event Log
/// For each object of the specified type, a trace is created containing all events related to that object,
/// ordered by their timestamp.
///
pub fn flatten_ocel_on<'a>(
    ocel: &'a impl LinkedOCELAccess<'a>,
    object_type: impl AsRef<str>,
) -> EventLog {
    let mut traces: Vec<_> = ocel
        .get_obs_of_type(object_type.as_ref())
        .map(|ob| {
            let ob_val = ocel.get_full_ob(ob);
            let mut events: Vec<_> = ocel
                .get_e2o_rev(ob)
                .map(|(_q, ev)| {
                    let ev_val = ocel.get_full_ev(ev);
                    let mut xes_ev = Event {
                        attributes: vec![
                            Attribute::new(
                                "concept:name".to_string(),
                                AttributeValue::String(ev_val.event_type.clone()),
                            ),
                            Attribute::new(
                                "time:timestamp".to_string(),
                                AttributeValue::Date(ev_val.time),
                            ),
                        ],
                    };

                    xes_ev.attributes.extend(ev_val.attributes.iter().map(|at| {
                        let xes_attr_val: AttributeValue = at.value.clone().into();
                        Attribute {
                            key: at.name.clone(),
                            value: xes_attr_val,
                            own_attributes: None,
                        }
                    }));

                    xes_ev
                })
                .collect();
            events.sort_by_cached_key(|ev| {
                ev.attributes
                    .get_by_key("time:timestamp")
                    .and_then(|a| a.value.try_as_date().cloned())
            });
            let mut xes_t = Trace {
                attributes: vec![Attribute::new(
                    "concept:name".to_string(),
                    AttributeValue::String(ob_val.id.clone()),
                )],
                events,
            };
            xes_t
                .attributes
                .extend(ob_val.attributes.iter().flat_map(|at| {
                    let xes_attr_val: Option<AttributeValue> = match &at.value {
                        OCELAttributeValue::Integer(i) => Some(AttributeValue::Int(*i)),
                        OCELAttributeValue::Float(f) => Some(AttributeValue::Float(*f)),
                        OCELAttributeValue::String(s) => Some(AttributeValue::String(s.clone())),
                        // OCELAttributeValue::Time(date_time) => None,
                        // OCELAttributeValue::Boolean(_) => todo!(),
                        // OCELAttributeValue::Null => todo!(),
                        _ => None,
                    };
                    xes_attr_val.map(|v| Attribute {
                        key: at.name.clone(),
                        value: v,
                        own_attributes: None,
                    })
                }));
            xes_t
        })
        .collect();
    let mut ret = EventLog::new();
    traces.sort_by_cached_key(|t| {
        t.events.first().map(|e| {
            e.attributes
                .get_by_key("time:timestamp")
                .and_then(|a| a.value.try_as_date())
                .cloned()
        })
    });
    ret.traces = traces;
    ret
}
