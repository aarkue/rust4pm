use std::collections::{HashMap, HashSet};

use rayon::prelude::*;

use crate::{Attribute, AttributeValue, EventLog};

use super::constants::ACTIVITY_NAME;

#[derive(Debug, Clone)]
pub struct EventLogActivityProjection {
    pub activities: Vec<String>,
    pub act_to_index: HashMap<String, usize>,
    pub traces: Vec<(Vec<usize>,u64)>,
}

#[derive(Debug, Default, Clone)]
pub struct ActivityProjectionDFG {
    pub nodes: Vec<usize>,
    pub edges: HashMap<(usize, usize), u64>,
}

impl ActivityProjectionDFG {
    pub fn df_between(self: &Self, a: usize, b: usize) -> u64 {
        *self.edges.get(&(a, b)).unwrap_or(&0)
    }

    pub fn df_preset_of<T: FromIterator<usize>>(self: &Self, act: usize, df_threshold: u64) -> T {
        self.edges
            .iter()
            .filter_map(|((a, b), w)| {
                if *b == act && *w >= df_threshold {
                    return Some(*a);
                } else {
                    return None;
                }
            })
            .collect()
    }
    pub fn df_postset_of<'a>(self: &Self, act: usize, df_threshold: u64) ->  impl Iterator<Item = usize> + '_ {
        self.edges
            .iter()
            .filter_map(move |((a, b), w)| {
                if *a == act && *w >= df_threshold {
                    return Some(*b);
                } else {
                    return None;
                }
            })
    }

    pub fn from_event_log_projection(log: &EventLogActivityProjection) -> Self {
        let mut dfg = ActivityProjectionDFG::default();
        dfg.nodes = (0..log.activities.len()).collect();
        dfg.edges = log
            .traces
            .par_iter()
            .map(|(t,w)| {
                let mut trace_dfs: Vec<((usize, usize),u64)> = Vec::new();
                let mut prev_event: Option<usize> = None;
                for e in t {
                    match prev_event {
                        Some(prev_e) => {
                            trace_dfs.push(((prev_e, *e),*w));
                        }
                        None => {}
                    }
                    prev_event = Some(*e);
                }
                trace_dfs
            })
            .flatten()
            .fold(HashMap::<(usize, usize), u64>::new, |mut map, (df_pair,w)| {
                *map.entry(df_pair).or_insert(0) += w;
                map
            })
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
            .unwrap();
        return dfg;
    }
}

impl Into<EventLogActivityProjection> for &EventLog {
    fn into(self) -> EventLogActivityProjection {
        let acts_per_trace: Vec<Vec<String>> = self
            .traces
            .par_iter()
            .map(|t| -> Vec<String> {
                t.events
                    .iter()
                    .map(|e| {
                        match e
                            .attributes
                            .get(ACTIVITY_NAME)
                            .cloned()
                            .unwrap_or(Attribute {
                                key: ACTIVITY_NAME.into(),
                                value: AttributeValue::String("No Activity".into()),
                                own_attributes: None
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
        let activities: Vec<String> = activity_set.into_iter().map(|s| s.clone()).collect();
        let act_to_index: HashMap<String, usize> = activities
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, act)| (act, i))
            .collect();
        let mut traces_set: HashMap<Vec<usize>,u64>  = HashMap::new();
        acts_per_trace
            .iter()
            .for_each(|t| {
                let trace: Vec<usize> = t.iter()
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
    pub fn acts_to_names(self: &Self,acts: &Vec<usize>) -> Vec<String> {
        let mut ret: Vec<String> = acts.iter().map(|act| self.activities[*act].clone()).collect();
        ret.sort();
        return ret;
    }
}