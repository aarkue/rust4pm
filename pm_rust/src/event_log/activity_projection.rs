use std::{collections::{HashMap, HashSet}, rc::Rc};

use rayon::prelude::*;

use crate::{EventLog, Attribute, AttributeValue};

use super::constants::ACTIVITY_NAME;


pub struct EventLogActivityProjection<T> {
  pub activities: Vec<String>,
  pub act_to_index: HashMap<String, T>,
  pub traces: Vec<Vec<T>>,
  pub event_log: Rc<EventLog>,
}

impl Into<EventLog> for EventLogActivityProjection<usize> {
  fn into(self) -> EventLog {
      Rc::into_inner(self.event_log).unwrap()
  }
}

impl Into<EventLogActivityProjection<usize>> for EventLog {
  fn into(self) -> EventLogActivityProjection<usize> {
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
      let traces: Vec<Vec<usize>> = acts_per_trace
          .par_iter()
          .map(|t| -> Vec<usize> {
              t.iter()
                  .map(|act| *act_to_index.get(act).unwrap())
                  .collect()
          })
          .collect();
      EventLogActivityProjection {
          activities,
          act_to_index,
          traces,
          event_log: Rc::from(self),
      }
  }
}