pub use chrono::NaiveDateTime;
pub use chrono::{serde::ts_milliseconds, DateTime, TimeZone, Utc};
use event_log::activity_projection::EventLogActivityProjection;
pub use event_log::event_log_struct::{
    Attribute, AttributeAddable, AttributeValue, Attributes, Event, EventLog, Trace,
};
use petri_net::petri_net_struct::{ArcType, PetriNet, PlaceID};
use rayon::prelude::*;
use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
};
pub use uuid::Uuid;

pub mod event_log {
    pub mod activity_projection;
    pub mod constants;
    pub mod event_log_struct;
    pub mod import_xes;
}

pub mod petri_net {
    pub mod petri_net_struct;
    pub mod pnml;
}

pub mod alphappp {
    pub mod candidate_building;
    pub mod candidate_pruning;
    pub mod full;
    pub mod log_repair;
}

pub const START_EVENT: &str = "__START";
pub const END_EVENT: &str = "__END";

pub fn loop_sum_sqrt(from: usize, to: usize) -> f32 {
    (from..to).map(|x| (x as f32).sqrt()).sum()
}

pub fn add_start_end_acts_proj(log: &mut EventLogActivityProjection) {
    let start_act = match log.act_to_index.get(&START_EVENT.to_string()) {
        Some(a) => {
            eprintln!("Start activity ({}) already present in activity set! Still adding start/end to every trace, which might not be the desired outcome.", START_EVENT);
            *a
        }
        None => {
            let a = log.activities.len();
            log.activities.push(START_EVENT.to_string());
            log.act_to_index.insert(START_EVENT.to_string(), a);
            a
        }
    };
    let end_act = match log.act_to_index.get(&END_EVENT.to_string()) {
        Some(a) => {
            eprintln!("End activity ({}) already present in activity set! Still adding start/end to every trace, which might not be the desired outcome.", END_EVENT);
            *a
        }
        None => {
            let a = log.activities.len();
            log.activities.push(END_EVENT.to_string());
            log.act_to_index.insert(END_EVENT.to_string(), a);
            a
        }
    };

    log.traces.par_iter_mut().for_each(|t| {
        t.insert(0, start_act);
        t.push(end_act);
    });
}

pub fn add_start_end_acts(log: &mut EventLog) {
    log.traces.par_iter_mut().for_each(|t| {
        let start_event = Event::new(START_EVENT.to_string());
        let end_event = Event::new(END_EVENT.to_string());
        t.events.insert(0, start_event);
        t.events.push(end_event);
    });
}

pub fn add_sample_transition(net: &mut PetriNet) {
    let t1 = net.add_transition(Some("Use rust".into()), None);
    let start_places: Vec<PlaceID> = net
        .places
        .iter()
        .filter_map(|(_, p)| {
            if net.preset_of_place(p.into()).is_empty() {
                Some(p.into())
            } else {
                None
            }
        })
        .collect();
    let end_places: Vec<PlaceID> = net
        .places
        .iter()
        .filter_map(|(_, p)| {
            if net.postset_of_place(p.into()).is_empty() {
                Some(p.into())
            } else {
                None
            }
        })
        .collect();
    start_places
        .into_iter()
        .for_each(|p| net.add_arc(ArcType::place_to_transition(p, t1), None));
    end_places
        .into_iter()
        .for_each(|p| net.add_arc(ArcType::transition_to_place(t1, p), None));
}

pub fn petrinet_to_json(net: &PetriNet) -> String {
    serde_json::to_string(net).unwrap()
}
pub fn json_to_petrinet(net_json: &str) -> PetriNet {
    serde_json::from_str(&net_json).unwrap()
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
