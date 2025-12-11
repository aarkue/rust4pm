//! Core modules for process mining

pub mod event_data;

pub mod process_models;

pub use event_data::{case_centric::event_log_struct::EventLog, object_centric::ocel_struct::OCEL};
pub use process_models::case_centric::petri_net::petri_net_struct::PetriNet;
