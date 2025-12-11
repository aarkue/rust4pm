#![warn(
    clippy::doc_markdown,
    missing_debug_implementations,
    rust_2018_idioms,
    missing_docs
)]
// #![allow(clippy::needless_doctest_main)]
// #![doc = include_str!("../README.md")]

pub use chrono;

pub mod core;

pub mod conformance;
pub mod discovery;

/// Used for internal testing
// #[doc(hidden)]
pub mod test_utils {
    use std::path::PathBuf;

    /// Get the based path for test data.
    ///
    ///  Used for internal testing
    pub fn get_test_data_path() -> PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_data")
    }
}
/// A wrapper for either an owned or mutable reference to a [`quick_xml::Writer`]
#[allow(missing_debug_implementations)]
pub enum XMLWriterWrapper<'a, W> {
    /// Owned [`quick_xml::Writer`]
    Owned(quick_xml::Writer<W>),
    /// Mutable Reference to [`quick_xml::Writer`]
    Ref(&'a mut quick_xml::Writer<W>),
}

impl<'a, W> XMLWriterWrapper<'a, W> {
    /// Return a mutable reference to a [`quick_xml::Writer`]
    pub fn to_xml_writer(&'a mut self) -> &'a mut quick_xml::Writer<W> {
        match self {
            XMLWriterWrapper::Owned(w) => w,
            XMLWriterWrapper::Ref(w) => w,
        }
    }
}

impl<W: std::io::Write> From<W> for XMLWriterWrapper<'_, W> {
    fn from(w: W) -> Self {
        Self::Owned(quick_xml::Writer::new(w))
    }
}

impl<'a, W> From<&'a mut quick_xml::Writer<W>> for XMLWriterWrapper<'a, W> {
    fn from(w: &'a mut quick_xml::Writer<W>) -> Self {
        Self::Ref(w)
    }
}

// Not used yet, but maybe useful in the future:

// /// A wrapper for either an owned or mutable reference to a [`quick_xml::Reader`]
// #[allow(missing_debug_implementations)]
// pub enum XMLReaderWrapper<'a, R> {
//     /// Owned [`quick_xml::Reader`]
//     Owned(quick_xml::Reader<R>),
//     /// Mutable Reference to [`quick_xml::Reader`]
//     Ref(&'a mut quick_xml::Reader<R>),
// }

// impl<'a, R> XMLReaderWrapper<'a, R> {
//     /// Return a mutable reference to a [`quick_xml::Reader`]
//     pub fn to_xml_reader(&'a mut self) -> &mut quick_xml::Reader<R> {
//         match self {
//             XMLReaderWrapper::Owned(r) => r,
//             XMLReaderWrapper::Ref(r) => r,
//         }
//     }
// }

// impl<'a, R: std::io::Read> From<R> for XMLReaderWrapper<'a, R> {
//     fn from(r: R) -> Self {
//         Self::Owned(quick_xml::Reader::from_reader(r))
//     }
// }

// impl<'a, R> From<&'a mut quick_xml::Reader<R>> for XMLReaderWrapper<'a, R> {
//     fn from(w: &'a mut quick_xml::Reader<R>) -> Self {
//         Self::Ref(w)
//     }
// }

// // ///
// // /// Event Logs (traditional [`EventLog`] and Object-Centric [`OCEL`])
// // ///
// // pub mod event_log {
// //     /// Activity projection of event logs
// //     pub mod activity_projection;
// //     /// Constants
// //     // pub mod constants;
// //     /// Conversion of XES event data from/to polars `DataFrame`
// //     #[cfg(feature = "dataframes")]
// //     pub mod dataframe;
// //     /// Splitting an event log into several sub event logs by given activities or randomly
// //     #[cfg(feature = "log-splitting")]
// //     pub mod event_log_splitter;
// //     // // /// [`EventLog`] struct and sub-structs
// //     // // pub mod event_log_struct;
// //     // /// XES Export
// //     // pub mod export_xes;
// //     // /// XES Import
// //     // pub mod import_xes;
// //     /// Event log creation macros
// //     pub mod macros;
// //     ///
// //     /// OCEL2.0 (Object-Centric Event Logs)
// //     ///
// //     pub mod ocel;
// //     // /// Streaming XES Import
// //     // pub mod stream_xes;
// //     // pub use event_log_struct::{
// //     //     Attribute, AttributeValue, Attributes, Event, EventLog, Trace, XESEditableAttribute,
// //     // };
// //     #[cfg(test)]
// //     mod tests;
// // }

// /// Object-centric discovery and conformance checking
// pub mod object_centric;

// /// Util module with smaller helper functions, structs or enums
// pub mod utils;

// ///
// /// Petri nets
// ///
// pub mod petri_net {
//     /// Export [`PetriNet`] to `.pnml`
//     pub mod export_pnml;
//     #[cfg(feature = "graphviz-export")]
//     /// Export [`PetriNet`] to images (SVG, PNG, ...)
//     ///
//     /// __Requires the `graphviz-export` feature to be enabled__
//     ///
//     /// Also requires an active graphviz installation in the PATH.
//     /// See also <https://github.com/besok/graphviz-rust?tab=readme-ov-file#caveats> and <https://graphviz.org/download/>
//     pub mod image_export;
//     /// Import [`PetriNet`] from `.pnml`
//     pub mod import_pnml;
//     /// [`PetriNet`] struct
//     pub mod petri_net_struct;

//     #[doc(inline)]
//     pub use petri_net_struct::PetriNet;
// }

// ///
// /// Process trees
// ///
// pub mod process_tree {
//     /// [`ProcessTree`] struct
//     pub mod process_tree_struct;

//     #[doc(inline)]
//     pub use crate::process_tree::process_tree_struct::ProcessTree;
// }

// ///
// /// Conformance Checking
// ///
// pub mod conformance {
//     /// Token-based replay
//     #[cfg(feature = "token_based_replay")]
//     pub mod token_based_replay;
// }

// ///
// /// Directly-follows graph
// ///
// pub mod dfg {
//     /// [`DirectlyFollowsGraph`] struct
//     pub mod dfg_struct;
//     #[cfg(feature = "graphviz-export")]
//     /// Export [`DirectlyFollowsGraph`] to images (SVG, PNG, ...)
//     ///
//     /// __Requires the `graphviz-export` feature to be enabled__
//     ///
//     /// Also requires an active graphviz installation in the PATH.
//     /// See also <https://github.com/besok/graphviz-rust?tab=readme-ov-file#caveats> and <https://graphviz.org/download/>
//     pub mod image_export;

//     #[doc(inline)]
//     pub use crate::dfg::dfg_struct::DirectlyFollowsGraph;
// }

// ///
// /// Partial Orders
// ///
// pub mod partial_orders {
//     #[cfg(feature = "graphviz-export")]
//     /// Export [`PartialOrderTrace`] to images (SVG, PNG, ...)
//     ///
//     /// __Requires the `graphviz-export` feature to be enabled__
//     ///
//     /// Also requires an active graphviz installation in the PATH.
//     /// See also <https://github.com/besok/graphviz-rust?tab=readme-ov-file#caveats> and <https://graphviz.org/download/>
//     pub mod image_export;
//     /// [`PartialOrderTrace`] and [`PartialOrderEventLog`] struct
//     pub mod partial_event_log_struct;

//     #[doc(inline)]
//     pub use crate::partial_orders::partial_event_log_struct::PartialOrderTrace;

//     #[doc(inline)]
//     pub use crate::partial_orders::partial_event_log_struct::PartialOrderEventLog;
// }

// use std::fs::File;
// use std::io::BufReader;
// use std::io::BufWriter;
// use std::path::Path;

// #[doc(inline)]
// pub use alphappp::full::alphappp_discover_petri_net;

// // #[doc(inline)]
// // pub use event_log::import_xes::import_xes_file;

// // #[doc(inline)]
// // pub use event_log::import_xes::import_xes_slice;

// // #[doc(inline)]
// // pub use event_log::stream_xes::stream_xes_from_path;

// // #[doc(inline)]
// // pub use event_log::stream_xes::stream_xes_slice;

// // #[doc(inline)]
// // pub use event_log::stream_xes::stream_xes_slice_gz;

// // #[doc(inline)]
// // pub use event_log::stream_xes::stream_xes_file;

// // #[doc(inline)]
// // pub use event_log::stream_xes::stream_xes_file_gz;

// // #[doc(inline)]
// // pub use event_log::export_xes::export_xes_trace_stream_to_file;

// // #[doc(inline)]
// // pub use event_log::export_xes::export_xes_event_log_to_file_path;

// // #[doc(inline)]
// // pub use event_log::stream_xes::StreamingXESParser;

// // #[doc(inline)]
// // pub use event_log::import_xes::XESImportOptions;

// // #[doc(inline)]

// #[cfg(feature = "ocel-sqlite")]
// #[doc(inline)]
// pub use event_log::ocel::sql::sqlite::sqlite_ocel_import::import_ocel_sqlite_from_path;

// #[cfg(feature = "ocel-sqlite")]
// #[doc(inline)]
// pub use event_log::ocel::sql::sqlite::sqlite_ocel_import::import_ocel_sqlite_from_con;

// #[cfg(feature = "ocel-sqlite")]
// #[doc(inline)]
// pub use event_log::ocel::sql::sqlite::sqlite_ocel_import::import_ocel_sqlite_from_slice;

// #[doc(inline)]
// #[cfg(not(all(not(feature = "ocel-duckdb"), not(feature = "ocel-sqlite"))))]
// pub use event_log::ocel::sql::export::export_ocel_to_sql_con;

// #[cfg(feature = "ocel-sqlite")]
// #[doc(inline)]
// pub use event_log::ocel::sql::sqlite::sqlite_ocel_export::export_ocel_sqlite_to_path;

// #[cfg(feature = "ocel-sqlite")]
// #[doc(inline)]
// pub use event_log::ocel::sql::sqlite::sqlite_ocel_export::export_ocel_sqlite_to_vec;

// #[cfg(feature = "dataframes")]
// #[doc(inline)]
// pub use event_log::dataframe::convert_log_to_dataframe;

// #[cfg(feature = "dataframes")]
// #[doc(inline)]
// pub use event_log::dataframe::convert_dataframe_to_log;

// #[doc(inline)]
// pub use petri_net::petri_net_struct::PetriNet;

// #[cfg(feature = "graphviz-export")]
// #[doc(inline)]
// pub use petri_net::image_export::export_petri_net_image_png;

// #[cfg(feature = "graphviz-export")]
// #[doc(inline)]
// pub use petri_net::image_export::export_petri_net_image_svg;

// #[doc(inline)]
// pub use petri_net::export_pnml::export_petri_net_to_pnml;

// #[doc(inline)]
// pub use petri_net::import_pnml::import_pnml;

// ///
// /// Module for the Alpha+++ Process Discovery algorithm
// ///
// pub mod alphappp {
//     /// Automatic determining algorithm parameters for Alpha+++
//     pub mod auto_parameters;
//     /// Alpha+++ Place Candidate Building
//     pub mod candidate_building;
//     /// Alpha+++ Place Candidate Pruning
//     pub mod candidate_pruning;
//     /// Full Alpha+++ Discovery algorithm
//     pub mod full;
//     /// Event Log Repair (Adding artificial activities)
//     pub mod log_repair;
// }

// ///
// /// Serialize a [`PetriNet`] as a JSON [`String`]
// ///
// pub fn petrinet_to_json(net: &PetriNet) -> String {
//     serde_json::to_string(net).unwrap()
// }
// ///
// /// Deserialize a [`PetriNet`] from a JSON [`String`]
// ///
// pub fn json_to_petrinet(net_json: &str) -> PetriNet {
//     serde_json::from_str(net_json).unwrap()
// }
