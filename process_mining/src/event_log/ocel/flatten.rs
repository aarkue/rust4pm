use crate::{
    event_log::{Attribute, AttributeValue, Event, Trace, XESEditableAttribute},
    EventLog,
};

use super::{
    linked_ocel::{IndexLinkedOCEL, LinkedOCELAccess},
    ocel_struct::OCELAttributeValue,
};

pub(crate) fn flatten_ocel_on(locel: &IndexLinkedOCEL, object_type: &str) -> EventLog {
    let mut traces: Vec<_> = locel
        .get_obs_of_type(object_type)
        .map(|ob| {
            let ob_val = locel.get_ob(ob);
            let mut events: Vec<_> = locel
                .get_e2o_rev(ob)
                .map(|(_q, ev)| {
                    let ev_val = locel.get_ev(ev);
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

                    xes_ev
                        .attributes
                        .extend(ev_val.attributes.iter().flat_map(|at| {
                            let xes_attr_val: Option<AttributeValue> = match &at.value {
                                OCELAttributeValue::Integer(i) => Some(AttributeValue::Int(*i)),
                                OCELAttributeValue::Float(f) => Some(AttributeValue::Float(*f)),
                                OCELAttributeValue::String(s) => {
                                    Some(AttributeValue::String(s.clone()))
                                }
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
