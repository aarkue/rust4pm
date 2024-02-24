use std::collections::{HashMap, HashSet};

use rayon::prelude::*;

use super::event_log_struct::{Attribute, AttributeValue, EventLog};

use super::constants::ACTIVITY_NAME;
use super::AttributeAddable;

pub const START_ACTIVITY: &str = "__START";
pub const END_ACTIVITY: &str = "__END";

#[derive(Debug, Clone)]
/// Projection of an event log on just activity labels
///
/// Currently assumes a default activity name ([ACTIVITY_NAME])
pub struct EventLogActivityProjection {
    pub activities: Vec<String>,
    pub act_to_index: HashMap<String, usize>,
    pub traces: Vec<(Vec<usize>, u64)>,
}

#[derive(Debug, Default, Clone)]
pub struct ActivityProjectionDFG {
    pub nodes: Vec<usize>,
    pub edges: HashMap<(usize, usize), u64>,
}

impl ActivityProjectionDFG {
    pub fn df_between(&self, a: usize, b: usize) -> u64 {
        *self.edges.get(&(a, b)).unwrap_or(&0)
    }

    pub fn df_preset_of<T: FromIterator<usize>>(&self, act: usize, df_threshold: u64) -> T {
        self.edges
            .iter()
            .filter_map(|((a, b), w)| {
                if *b == act && *w >= df_threshold {
                    Some(*a)
                } else {
                    None
                }
            })
            .collect()
    }
    pub fn df_postset_of(&self, act: usize, df_threshold: u64) -> impl Iterator<Item = usize> + '_ {
        self.edges.iter().filter_map(move |((a, b), w)| {
            if *a == act && *w >= df_threshold {
                Some(*b)
            } else {
                None
            }
        })
    }

    pub fn from_event_log_projection(log: &EventLogActivityProjection) -> Self {
        let dfg = ActivityProjectionDFG {
            nodes: (0..log.activities.len()).collect(),
            edges: log
                .traces
                .par_iter()
                .map(|(t, w)| {
                    let mut trace_dfs: Vec<((usize, usize), u64)> = Vec::new();
                    let mut prev_event: Option<usize> = None;
                    for e in t {
                        if let Some(prev_e) = prev_event {
                            trace_dfs.push(((prev_e, *e), *w));
                        }
                        prev_event = Some(*e);
                    }
                    trace_dfs
                })
                .flatten()
                .fold(
                    HashMap::<(usize, usize), u64>::new,
                    |mut map, (df_pair, w)| {
                        *map.entry(df_pair).or_insert(0) += w;
                        map
                    },
                )
                .reduce_with(|mut m1, mut m2| {
                    if m1.len() < m2.len() {
                        for (k, v) in m2 {
                            *m1.entry(k).or_default() += v;
                        }
                        m1
                    } else {
                        for (k, v) in m1 {
                            *m2.entry(k).or_default() += v;
                        }
                        m2
                    }
                })
                .unwrap(),
        };
        dfg
    }
}

impl<'a> From<super::stream_xes::XESParsingTraceStream<'a>> for EventLogActivityProjection {
    fn from(mut value: super::stream_xes::XESParsingTraceStream<'a>) -> Self {
        (&mut value).into()
    }
}
impl<'a, 'b> From<&'b mut super::stream_xes::XESParsingTraceStream<'a>>
    for EventLogActivityProjection
{
    fn from(value: &mut super::stream_xes::XESParsingTraceStream) -> Self {
        let mut act_to_index: HashMap<String, usize> = HashMap::new();
        let mut activities: Vec<String> = Vec::new();
        let mut traces: HashMap<Vec<usize>, u64> = HashMap::new();
        for t in value.into_iter() {
            let mut trace_acts: Vec<usize> = Vec::with_capacity(t.events.len());
            for e in t.events {
                let act = match e.attributes.get_by_key(ACTIVITY_NAME) {
                    Some(act_attr) => match &act_attr.value {
                        AttributeValue::String(s) => s.as_str(),
                        _ => "No Activity",
                    },
                    None => "No Activity",
                };
                if let Some(index) = act_to_index.get(act) {
                    trace_acts.push(*index);
                } else {
                    let new_act_index = activities.len();
                    activities.push(act.to_string());
                    act_to_index.insert(act.to_string(), new_act_index);
                    trace_acts.push(new_act_index)
                }
            }

            *traces.entry(trace_acts).or_insert(0) += 1;
        }
        Self {
            activities,
            act_to_index,
            traces: traces.into_iter().collect(),
        }
    }
}
impl From<&EventLog> for EventLogActivityProjection {
    fn from(val: &EventLog) -> Self {
        let acts_per_trace: Vec<Vec<String>> = val
            .traces
            .par_iter()
            .map(|t| -> Vec<String> {
                t.events
                    .iter()
                    .map(|e| {
                        match e
                            .attributes
                            .get_by_key(ACTIVITY_NAME)
                            .cloned()
                            .unwrap_or(Attribute {
                                key: ACTIVITY_NAME.into(),
                                value: AttributeValue::String("No Activity".into()),
                                own_attributes: None,
                            })
                            .value
                        {
                            AttributeValue::String(s) => s,
                            _ => "No Activity".into(),
                        }
                    })
                    .collect::<Vec<String>>()
            })
            .collect();
        let activity_set: HashSet<&String> = acts_per_trace.iter().flatten().collect();
        let activities: Vec<String> = activity_set.into_iter().cloned().collect();
        let act_to_index: HashMap<String, usize> = activities
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, act)| (act, i))
            .collect();
        let mut traces_set: HashMap<Vec<usize>, u64> = HashMap::new();
        acts_per_trace.iter().for_each(|t| {
            let trace: Vec<usize> = t
                .iter()
                .map(|act| *act_to_index.get(act).unwrap())
                .collect();
            *traces_set.entry(trace).or_insert(0) += 1;
        });

        EventLogActivityProjection {
            activities,
            act_to_index,
            traces: traces_set.into_iter().collect(),
        }
    }
}

impl EventLogActivityProjection {
    pub fn acts_to_names(&self, acts: &[usize]) -> Vec<String> {
        let mut ret: Vec<String> = acts
            .iter()
            .map(|act| self.activities[*act].clone())
            .collect();
        ret.sort();
        ret
    }
}
