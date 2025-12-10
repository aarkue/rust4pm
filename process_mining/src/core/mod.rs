//! Core modules for process mining

pub mod event_data;

pub mod process_models;

pub use event_data::case_centric::event_log_struct::EventLog;
pub use event_data::object_centric::ocel_struct::OCEL;
