//! Fixtures shared by the OCEL benchmarks.
//!
//! A directory module rather than `benches/common.rs`, which cargo would take for a bench target.

// Each bench pulls in the whole module but uses only the part it needs.
#![allow(dead_code)]

use std::path::PathBuf;

use process_mining::test_utils::get_test_data_path;

/// The order-management log in the given serialization (`json`, `xml`, ...).
pub fn order_management(ext: &str) -> PathBuf {
    get_test_data_path()
        .join("ocel")
        .join(format!("order-management.{ext}"))
}
