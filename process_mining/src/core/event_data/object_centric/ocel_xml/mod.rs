//! OCEL 2.0 XML Format Import/Export
/// XML Export for OCEL 2.0
pub mod xml_ocel_export;
#[allow(clippy::single_match)]
/// Parser for the OCEL 2.0 XML format
pub mod xml_ocel_import;
#[doc(inline)]
pub use xml_ocel_export::*;
#[doc(inline)]
pub use xml_ocel_import::*;
