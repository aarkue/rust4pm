use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
    rc::Rc,
};

pub const START_EVENT: &str = "__START__";
pub const END_EVENT: &str = "__END__";

pub const ACTIVITY_NAME: &str = "concept:name";
#[allow(dead_code)]
pub const TRACE_ID_NAME: &str = "case:concept:name";

#[derive(Debug, Deserialize, Serialize)]
pub struct Event {
    pub attributes: HashMap<String, String>,
}
impl Event {
    pub fn new(activity: String) -> Self {
        Event {
            attributes: vec![(ACTIVITY_NAME.to_string(), activity)]
                .into_iter()
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Trace {
    pub attributes: HashMap<String, String>,
    pub events: Vec<Event>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EventLog {
    pub attributes: HashMap<String, String>,
    pub traces: Vec<Trace>,
}

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
            .map(|t| -> Option<Vec<String>> {
                t.events
                    .iter()
                    .map(|e| e.attributes.get(ACTIVITY_NAME).cloned())
                    .collect::<Option<Vec<String>>>()
            })
            .collect::<Option<Vec<Vec<String>>>>()
            .unwrap();
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

pub fn loop_sum_sqrt(from: usize, to: usize) -> f32 {
    (from..to).map(|x| (x as f32).sqrt()).sum()
}

pub fn add_start_end_acts(log: &mut EventLog) {
    log.traces.par_iter_mut().for_each(|t| {
        let start_event = Event::new(START_EVENT.to_string());
        let end_event = Event::new(END_EVENT.to_string());
        t.events.insert(0, start_event);
        t.events.push(end_event);
    });
}

pub fn export_log<P: AsRef<Path>>(path: P, log: &EventLog) {
    let file = File::create(path).unwrap();
    let writer = BufWriter::new(file);
    serde_json::to_writer(writer, log).unwrap();
}

pub fn export_log_to_string(log: &EventLog) -> String {
    serde_json::to_string(log).unwrap()
}

pub fn export_log_to_byte_vec(log: &EventLog) -> Vec<u8> {
    serde_json::to_vec(log).unwrap()
}

pub fn import_log<P: AsRef<Path>>(path: P) -> EventLog {
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    let log: EventLog = serde_json::from_reader(reader).unwrap();
    return log;
}

pub fn import_log_from_byte_array(bytes: &[u8]) -> EventLog {
    let log: EventLog = serde_json::from_slice(&bytes).unwrap();
    return log;
}

// const raw_schema: &str = r#"
// {"type":"record","name":"EventLogJava","fields":[{"name":"attributes","type":{"type":"map","values":"string"}},{"name":"traces","type":{"type":"array","items":{"type":"record","name":"TraceJava","fields":[{"name":"attributes","type":{"type":"map","values":"string"}},{"name":"events","type":{"type":"array","items":{"type":"record","name":"EventJava","fields":[{"name":"attributes","type":{"type":"map","values":"string"}}]},"java-class":"java.util.ArrayList"}}]},"java-class":"java.util.ArrayList"}}]}
// "#;
// pub fn import_log_from_byte_vec_avro(bytes: &Vec<u8>) -> Option<EventLog> {
//     let reader_schema = Schema::parse_str(&raw_schema).unwrap();
//     let reader = Reader::with_schema(&reader_schema, &bytes[..]).unwrap();
//     for value in reader {
//         // println!("{:?}", val);
//         let log: EventLog = apache_avro::from_value(&value.unwrap()).unwrap();
//         return Some(log);
//     }
//     return None;
// }

pub fn import_log_from_str(json: String) -> EventLog {
    serde_json::from_str(&json).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = loop_sum_sqrt(4, 5);
        assert_eq!(result, 2.0);
    }
}
