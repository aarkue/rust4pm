#![warn(
    clippy::doc_markdown,
    missing_debug_implementations,
    rust_2018_idioms,
    missing_docs,
    clippy::redundant_clone,
    clippy::clone_on_copy
)]
#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]

// Allow the `#[register_binding]` macro's generated code to reference this crate by
// its absolute name (`::process_mining::...`) even from within the crate itself.
#[allow(unused_extern_crates)]
extern crate self as process_mining;

/// Crate re-exports used by the `#[register_binding]` macro's generated code, so downstream
/// crates registering bindings only need a dependency on `process_mining`.
#[cfg(feature = "bindings")]
#[doc(hidden)]
pub mod __private {
    pub use {inventory, schemars, serde_json, uuid};
}

pub mod analysis;
pub mod conformance;
pub mod core;
pub mod discovery;
pub mod utils;
#[cfg(feature = "visualization")]
pub mod visualization;

pub use core::io::{Exportable, Importable};

// Re-export main structs for convenience
pub use core::{AcceptingPetriNet, EventLog, PetriNet, OCEL};
pub use utils::control::{Cancelled, Control, Progress};

// Re-export OCEL backend traits, the streaming entry point and the IO options.
pub use core::event_data::object_centric::{
    appendable::{AppendableOCEL, StreamImportOCEL},
    io::{OCELExportOptions, OCELImportOptions},
    readable::{OCELLookup, ReadableOCEL},
};

/// Bindings
pub mod bindings;

/// Used for internal testing
#[doc(hidden)]
pub mod test_utils {
    use std::path::PathBuf;

    use crate::OCEL;

    /// Get the base path for test data.
    ///
    ///  Used for internal testing
    #[allow(unused)]
    pub fn get_test_data_path() -> PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_data")
    }

    /// Sort all collections inside an [`OCEL`] (events, objects, types,
    /// relationships, attributes) by stable keys so two OCELs that are
    /// structurally equivalent but inserted in different orders compare
    /// equal via `assert_eq!`.
    #[allow(unused)]
    pub fn sort_ocel_for_equality_compare(ocel: &mut OCEL) {
        ocel.events
            .sort_by(|x, y| (x.time, &x.id).cmp(&(y.time, &y.id)));
        ocel.objects.sort_by(|x, y| x.id.cmp(&y.id));
        ocel.event_types.sort_by(|x, y| x.name.cmp(&y.name));
        ocel.object_types.sort_by(|x, y| x.name.cmp(&y.name));
        for e in &mut ocel.events {
            e.relationships
                .sort_by(|x, y| (&x.object_id, &x.qualifier).cmp(&(&y.object_id, &y.qualifier)));
        }
        for o in &mut ocel.objects {
            o.attributes
                .sort_by(|x, y| (x.time, &x.name).cmp(&(y.time, &y.name)));
            o.relationships
                .sort_by(|x, y| (&x.object_id, &x.qualifier).cmp(&(&y.object_id, &y.qualifier)));
        }
    }

    /// Panics unless `xml` is well-formed, with matching end tags and at least one element.
    #[allow(unused)]
    pub fn assert_xml_well_formed(xml: &str) {
        use quick_xml::events::Event;
        let mut reader = quick_xml::Reader::from_str(xml);
        reader.config_mut().check_end_names = true;
        let mut depth = 0i32;
        let mut elements = 0usize;
        loop {
            match reader.read_event() {
                Ok(Event::Start(_)) => {
                    depth += 1;
                    elements += 1;
                }
                Ok(Event::Empty(_)) => elements += 1,
                Ok(Event::End(_)) => depth -= 1,
                Ok(Event::Eof) => break,
                Ok(_) => {}
                Err(e) => panic!(
                    "not well-formed XML at byte {}: {e}",
                    reader.buffer_position()
                ),
            }
            assert!(depth >= 0, "an end tag with no matching start tag");
        }
        assert_eq!(depth, 0, "unclosed elements");
        assert!(elements > 0, "no elements at all");
    }

    /// Checks `svg` and writes it to `test_data/export/<name>`, to look at by hand.
    #[allow(unused)]
    pub fn write_export(name: &str, svg: &str) {
        assert_xml_well_formed(svg);
        std::fs::write(get_test_data_path().join("export").join(name), svg).unwrap();
    }
}
