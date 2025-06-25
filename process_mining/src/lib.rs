#![warn(
    clippy::doc_markdown,
    missing_debug_implementations,
    rust_2018_idioms,
    missing_docs
)]

#![allow(clippy::needless_doctest_main)]

#![doc = include_str!("../README.md")]

///
/// Event Logs (traditional [`EventLog`] and Object-Centric [`OCEL`])
///
pub mod event_log {
    /// Activity projection of event logs
    pub mod activity_projection;
    /// Constants
    pub mod constants;
    /// Conversion of XES event data from/to polars `DataFrame`
    #[cfg(feature = "dataframes")]
    pub mod dataframe;
    /// Splitting an event log into several sub event logs by given activities or randomly
    #[cfg(feature = "log-splitting")]
    pub mod event_log_splitter;
    /// [`EventLog`] struct and sub-structs
    pub mod event_log_struct;
    /// XES Export
    pub mod export_xes;
    /// XES Import
    pub mod import_xes;
    /// Streaming XES Import
    pub mod stream_xes;
    ///
    /// OCEL2.0 (Object-Centric Event Logs)
    ///
    pub mod ocel {
        /// Linked OCEL 2.0, allowing convenient usage of object-centric data
        pub mod linked_ocel;
        /// OCEL 2.0 struct and sub-structs
        pub mod ocel_struct;
        /// Functionality to flatten OCEL on an object type
        pub mod flatten;
        /// `SQLite` OCEL 2.0
        #[cfg(feature = "ocel-sqlite")]
        pub mod sqlite;
        /// XML Export for OCEL 2.0
        pub mod xml_ocel_export;
        #[allow(clippy::single_match)]
        /// Parser for the OCEL 2.0 XML format
        pub mod xml_ocel_import;
        /// Macros for the creation of [`OCEL`]
        pub mod macros;
    }
    pub use event_log_struct::{
        Attribute, AttributeValue, Attributes, Event, EventLog, Trace, XESEditableAttribute,
    };
    #[cfg(test)]
    mod tests;
}

/// Object-centric discovery and conformance checking
pub mod object_centric {
    /// Object-centric conformance checking
    pub mod conformance;
    /// Object-centric process trees [`OCPT`]
    pub mod ocpt;
    /// Object-centric directly-follows graphs
    pub mod object_centric_dfg_struct;
}

/// Util module with smaller helper functions, structs or enums
pub mod utils;

///
/// Petri nets
///
pub mod petri_net {
    /// Export [`PetriNet`] to `.pnml`
    pub mod export_pnml;
    #[cfg(feature = "graphviz-export")]
    /// Export [`PetriNet`] to images (SVG, PNG, ...)
    ///
    /// __Requires the `graphviz-export` feature to be enabled__
    ///
    /// Also requires an active graphviz installation in the PATH.
    /// See also <https://github.com/besok/graphviz-rust?tab=readme-ov-file#caveats> and <https://graphviz.org/download/>
    pub mod image_export;
    /// Import [`PetriNet`] from `.pnml`
    pub mod import_pnml;
    /// [`PetriNet`] struct
    pub mod petri_net_struct;

    #[doc(inline)]
    pub use petri_net_struct::PetriNet;
}

///
/// Conformance Checking
///
pub mod conformance {
    /// Token-based replay
    #[cfg(feature = "token_based_replay")]
    pub mod token_based_replay;
}

///
/// Directly-follows graph
///
pub mod dfg {
    /// [`DirectlyFollowsGraph`] struct
    pub mod dfg_struct;
    #[cfg(feature = "graphviz-export")]
    /// Export [`DirectlyFollowsGraph`] to images (SVG, PNG, ...)
    ///
    /// __Requires the `graphviz-export` feature to be enabled__
    ///
    /// Also requires an active graphviz installation in the PATH.
    /// See also <https://github.com/besok/graphviz-rust?tab=readme-ov-file#caveats> and <https://graphviz.org/download/>
    pub mod image_export;

    #[doc(inline)]
    pub use crate::dfg::dfg_struct::DirectlyFollowsGraph;
}

///
/// Partial Orders
///
pub mod partial_orders {
    #[cfg(feature = "graphviz-export")]
    /// Export [`PartialOrderTrace`] to images (SVG, PNG, ...)
    ///
    /// __Requires the `graphviz-export` feature to be enabled__
    ///
    /// Also requires an active graphviz installation in the PATH.
    /// See also <https://github.com/besok/graphviz-rust?tab=readme-ov-file#caveats> and <https://graphviz.org/download/>
    pub mod image_export;
    /// [`PartialOrderTrace`] and [`PartialOrderEventLog`] struct
    pub mod partial_event_log_struct;

    #[doc(inline)]
    pub use crate::partial_orders::partial_event_log_struct::PartialOrderTrace;

    #[doc(inline)]
    pub use crate::partial_orders::partial_event_log_struct::PartialOrderEventLog;
}

use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::path::Path;

#[doc(inline)]
pub use event_log::ocel;

#[doc(inline)]
pub use alphappp::full::alphappp_discover_petri_net;

#[doc(inline)]
pub use event_log::import_xes::import_xes_file;

#[doc(inline)]
pub use event_log::import_xes::import_xes_slice;

#[doc(inline)]
pub use event_log::stream_xes::stream_xes_from_path;

#[doc(inline)]
pub use event_log::stream_xes::stream_xes_slice;

#[doc(inline)]
pub use event_log::stream_xes::stream_xes_slice_gz;

#[doc(inline)]
pub use event_log::stream_xes::stream_xes_file;

#[doc(inline)]
pub use event_log::stream_xes::stream_xes_file_gz;

#[doc(inline)]
pub use event_log::export_xes::export_xes_trace_stream_to_file;

#[doc(inline)]
pub use event_log::export_xes::export_xes_event_log_to_file_path;

#[doc(inline)]
pub use event_log::stream_xes::StreamingXESParser;

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

#[doc(inline)]
pub use event_log::ocel::xml_ocel_export::export_ocel_xml_path;

#[doc(inline)]
pub use event_log::ocel::xml_ocel_export::export_ocel_xml;

#[cfg(feature = "ocel-sqlite")]
#[doc(inline)]
pub use event_log::ocel::sqlite::sqlite_ocel_import::import_ocel_sqlite_from_path;

#[cfg(feature = "ocel-sqlite")]
#[doc(inline)]
pub use event_log::ocel::sqlite::sqlite_ocel_import::import_ocel_sqlite_from_con;

#[cfg(feature = "ocel-sqlite")]
#[doc(inline)]
pub use event_log::ocel::sqlite::sqlite_ocel_import::import_ocel_sqlite_from_slice;

#[cfg(feature = "ocel-sqlite")]
#[doc(inline)]
pub use event_log::ocel::sqlite::sqlite_ocel_export::export_ocel_sqlite_to_con;
#[cfg(feature = "ocel-sqlite")]
#[doc(inline)]
pub use event_log::ocel::sqlite::sqlite_ocel_export::export_ocel_sqlite_to_path;

#[cfg(feature = "ocel-sqlite")]
#[doc(inline)]
pub use event_log::ocel::sqlite::sqlite_ocel_export::export_ocel_sqlite_to_vec;

#[cfg(feature = "dataframes")]
#[doc(inline)]
pub use event_log::dataframe::convert_log_to_dataframe;

#[cfg(feature = "dataframes")]
#[doc(inline)]
pub use event_log::dataframe::convert_dataframe_to_log;

#[doc(inline)]
pub use petri_net::petri_net_struct::PetriNet;

#[cfg(feature = "graphviz-export")]
#[doc(inline)]
pub use petri_net::image_export::export_petri_net_image_png;

#[cfg(feature = "graphviz-export")]
#[doc(inline)]
pub use petri_net::image_export::export_petri_net_image_svg;

#[doc(inline)]
pub use petri_net::export_pnml::export_petri_net_to_pnml;

#[doc(inline)]
pub use petri_net::import_pnml::import_pnml;

#[doc(inline)]
pub use event_log::activity_projection::EventLogActivityProjection;

///
/// Module for the Alpha+++ Process Discovery algorithm
///
pub mod alphappp {
    /// Automatic determining algorithm parameters for Alpha+++
    pub mod auto_parameters;
    /// Alpha+++ Place Candidate Building
    pub mod candidate_building;
    /// Alpha+++ Place Candidate Pruning
    pub mod candidate_pruning;
    /// Full Alpha+++ Discovery algorithm
    pub mod full;
    /// Event Log Repair (Adding artificial activities)
    pub mod log_repair;
}

///
/// Serialize a [`PetriNet`] as a JSON [`String`]
///
pub fn petrinet_to_json(net: &PetriNet) -> String {
    serde_json::to_string(net).unwrap()
}
///
/// Deserialize a [`PetriNet`] from a JSON [`String`]
///
pub fn json_to_petrinet(net_json: &str) -> PetriNet {
    serde_json::from_str(net_json).unwrap()
}

///
/// Serialize [`OCEL`] as a JSON [`String`]
///
/// [`serde_json`] can also be used to convert [`OCEL`] to other targets (e.g., `serde_json::to_writer`)
///
pub fn ocel_to_json(ocel: &OCEL) -> String {
    serde_json::to_string(ocel).unwrap()
}

///
/// Import [`OCEL`] from a JSON [`String`]
///
/// [`serde_json`] can also be used to import [`OCEL`] from other targets (e.g., `serde_json::from_reader`)
///
pub fn json_to_ocel(ocel_json: &str) -> OCEL {
    serde_json::from_str(ocel_json).unwrap()
}

///
/// Import [`OCEL`] from a JSON file given by a filepath
///
/// See also [`import_ocel_json_from_slice`].
///
pub fn import_ocel_json_from_path<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<OCEL, std::io::Error> {
    let reader: BufReader<File> = BufReader::new(File::open(path)?);
    Ok(serde_json::from_reader(reader)?)
}

///
/// Import [`OCEL`] from a JSON byte slice
///
/// See also [`import_ocel_json_from_path`].
///
pub fn import_ocel_json_from_slice(slice: &[u8]) -> Result<OCEL, std::io::Error> {
    Ok(serde_json::from_slice(slice)?)
}

///
/// Export [`OCEL`] to a JSON file at the specified path
///
/// To import an OCEL .json file see [`import_ocel_json_from_path`] instead.
///
pub fn export_ocel_json_path<P: AsRef<Path>>(ocel: &OCEL, path: P) -> Result<(), std::io::Error> {
    let writer: BufWriter<File> = BufWriter::new(File::create(path)?);
    Ok(serde_json::to_writer(writer, ocel)?)
}

///
/// Export [`OCEL`] to JSON in a byte array ([`Vec<u8>`])
///
/// To import an OCEL .json file see [`import_ocel_json_from_path`] instead.
///
pub fn export_ocel_json_to_vec(ocel: &OCEL) -> Result<Vec<u8>, std::io::Error> {
    Ok(serde_json::to_vec(ocel)?)
}
