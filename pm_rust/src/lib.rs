use std::{collections::{HashMap, HashSet}, path::Path, fs::File, io::{BufWriter, BufRead, BufReader}};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Map;

const START_EVENT: &str = "__START__";
const END_EVENT: &str = "__END__";

const ACTIVITY_NAME: &str = "concept:name";
const TRACE_ID_NAME: &str = "case:concept:name";

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
    pub logName: String,
    pub traces: Vec<Trace>,
}

pub fn loop_sum_sqrt(from: usize, to: usize) -> f32 {
    (from..to).map(|x| (x as f32).sqrt()).sum()
}

pub fn add_start_end_acts(log: &mut EventLog) {
    log.traces.iter_mut().for_each(|t| {
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


pub fn import_log<P: AsRef<Path>>(path: P) -> EventLog {
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    let log : EventLog = serde_json::from_reader(reader).unwrap();
    return log;
}

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
