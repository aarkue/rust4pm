#![warn(
    clippy::doc_markdown,
    missing_debug_implementations,
    rust_2018_idioms,
    missing_docs,
    clippy::redundant_clone,
    clippy::clone_on_copy
)]
// #![allow(clippy::needless_doctest_main)]
#![doc = include_str!("../../README.md")]

pub mod core;

/// Conformance Checking
///
/// Conformance checking techniques typically compare the behavior of a process model with
/// event data.
pub mod conformance;
pub mod discovery;

pub use core::io::{Exportable, Importable};

// Re-export main structs for convenience
pub use core::{EventLog, PetriNet, OCEL};

/// Bindings (WIP)
pub mod bindings;

/// Used for internal testing
#[doc(hidden)]
pub mod test_utils {
    use std::path::PathBuf;

    /// Get the based path for test data.
    ///
    ///  Used for internal testing
    #[allow(unused)]
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
