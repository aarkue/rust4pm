//!  # Process Mining
//!
//! `process_mining` is a collection of functions, structs and utilitities related to Process Mining
//!

use event_log::activity_projection::{EventLogActivityProjection, END_ACTIVITY, START_ACTIVITY};
use event_log::event_log_struct::Event;
use petri_net::petri_net_struct::PetriNet;
use rayon::prelude::*;

///
/// Module for Event Logs (traditional and OCEL)
///
pub mod event_log {
    pub mod activity_projection;
    pub mod constants;
    pub mod event_log_struct;
    pub mod import_xes;
    pub mod stream_xes;
    pub mod ocel {
        pub mod ocel_struct;
        #[allow(clippy::single_match)]
        pub mod xml_ocel_import;
    }
    pub use event_log_struct::{
        Attribute, AttributeAddable, AttributeValue, Attributes, Event, EventLog, Trace,
    };
    #[cfg(test)]
    mod tests;
}

///
/// Module for Petri nets
///
pub mod petri_net {
    pub mod petri_net_struct;
    pub mod pnml;
}

#[doc(inline)]
pub use alphappp::full::alphappp_discover_petri_net;

#[doc(inline)]
pub use event_log::import_xes::import_xes;

#[doc(inline)]
pub use event_log::import_xes::import_xes_file;

#[doc(inline)]
pub use event_log::import_xes::import_xes_slice;

#[doc(inline)]
pub use event_log::import_xes::XESImportOptions;

#[doc(inline)]
pub use event_log::event_log_struct::EventLog;

#[doc(inline)]
pub use event_log::ocel::ocel_struct::OCEL;

#[doc(inline)]
pub use event_log::ocel::xml_ocel_import::import_ocel_xml;

#[doc(inline)]
pub use event_log::ocel::xml_ocel_import::import_ocel_xml_file;

#[doc(inline)]
pub use event_log::ocel::xml_ocel_import::import_ocel_xml_slice;

///
/// Module for the Alpha+++ Process Discovery algorithm
///
pub mod alphappp {
    pub mod auto_parameters;
    pub mod candidate_building;
    pub mod candidate_pruning;
    pub mod full;
    pub mod log_repair;
}

///
/// Add artificial start and end activities to a given [EventLogActivityProjection]
///
/// Mutating the [EventLogActivityProjection] in place
/// Additionally also checks if artificial [START_ACTIVITY] or [END_ACTIVITY] are already present in log
///
pub fn add_start_end_acts_proj(log: &mut EventLogActivityProjection) {
    let mut should_add_start = true;
    let start_act = match log.act_to_index.get(&START_ACTIVITY.to_string()) {
        Some(a) => {
            eprintln!("Start activity ({}) already present in activity set! Will skip adding a start activity to every trace, which might not be the desired outcome.", START_ACTIVITY);
            should_add_start = false;
            *a
        }
        None => {
            let a = log.activities.len();
            log.activities.push(START_ACTIVITY.to_string());
            log.act_to_index.insert(START_ACTIVITY.to_string(), a);
            a
        }
    };

    let mut should_add_end = true;
    let end_act = match log.act_to_index.get(&END_ACTIVITY.to_string()) {
        Some(a) => {
            eprintln!("End activity ({}) already present in activity set! Still adding an end activity to every trace, which might not be the desired outcome.", END_ACTIVITY);
            should_add_end = false;
            *a
        }
        None => {
            let a = log.activities.len();
            log.activities.push(END_ACTIVITY.to_string());
            log.act_to_index.insert(END_ACTIVITY.to_string(), a);
            a
        }
    };

    if should_add_start || should_add_end {
        log.traces.iter_mut().for_each(|(t, _)| {
            if should_add_start {
                t.insert(0, start_act);
            }
            if should_add_end {
                t.push(end_act);
            }
        });
    }
}

///
/// Add artificial start and end activities to a given [EventLog]
///
/// Mutating the [EventLog] in place
/// Caution: Does not check if [START_ACTIVITY] or [END_ACTIVITY] are already present in the log
///
pub fn add_start_end_acts(log: &mut EventLog) {
    log.traces.par_iter_mut().for_each(|t| {
        let start_event = Event::new(START_ACTIVITY.to_string());
        let end_event = Event::new(END_ACTIVITY.to_string());
        t.events.insert(0, start_event);
        t.events.push(end_event);
    });
}

///
/// Serialize a [PetriNet] as a JSON-encoded [String]
///
pub fn petrinet_to_json(net: &PetriNet) -> String {
    serde_json::to_string(net).unwrap()
}
///
/// Deserialize a [PetriNet] from a JSON-encoded [String]
///
pub fn json_to_petrinet(net_json: &str) -> PetriNet {
    serde_json::from_str(net_json).unwrap()
}

// pub fn export_log<P: AsRef<Path>>(path: P, log: &EventLog) {
//     let file = File::create(path).unwrap();
//     let writer = BufWriter::new(file);
//     serde_json::to_writer(writer, log).unwrap();
// }

// pub fn export_log_to_string(log: &EventLog) -> String {
//     serde_json::to_string(log).unwrap()
// }

// pub fn export_log_to_byte_vec(log: &EventLog) -> Vec<u8> {
//     serde_json::to_vec(log).unwrap()
// }

// pub fn import_log<P: AsRef<Path>>(path: P) -> EventLog {
//     let file = File::open(path).unwrap();
//     let reader = BufReader::new(file);
//     let log: EventLog = serde_json::from_reader(reader).unwrap();
//     return log;
// }

// pub fn import_log_from_byte_array(bytes: &[u8]) -> EventLog {
//     let log: EventLog = serde_json::from_slice(&bytes).unwrap();
//     return log;
// }

// pub fn import_log_from_str(json: String) -> EventLog {
//     serde_json::from_str(&json).unwrap()
// }
