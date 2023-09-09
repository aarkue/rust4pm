pub use chrono::NaiveDateTime;
pub use chrono::{serde::ts_milliseconds, DateTime, Utc, TimeZone};
pub use event_log::event_log_struct::{EventLog, AttributeValue, Attribute, Attributes, Event, Trace, AttributeAddable};
use rayon::prelude::*;
use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
};
pub use uuid::Uuid;

pub mod event_log {
    pub mod event_log_struct;
    pub mod constants;
    pub mod activity_projection;
    pub mod import_xes;
}

pub mod petri_net {
    pub mod petri_net_struct;
}

pub const START_EVENT: &str = "__START__";
pub const END_EVENT: &str = "__END__";


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
