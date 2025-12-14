//! Core modules for process mining

pub use chrono;
pub mod event_data;

pub mod process_models;

pub use event_data::object_centric::ocel_struct::OCEL;
pub use event_data::EventLog;
pub use process_models::case_centric::petri_net::petri_net_struct::PetriNet;
// #[doc(hidden)]
// #[doc(inline)]
// pub use event_data::*;
// #[doc(inline)]
// pub use process_models::*;
// pub use event_data::{case_centric::event_log_struct::EventLog, object_centric::ocel_struct::OCEL};
// pub use process_models::case_centric::petri_net::petri_net_struct::PetriNet;

// use process_mining::core::event_data::EventLog;
// use process_mining::core::process_models::PetriNet;
