#![cfg(feature = "bindings")]
//! `#[register_binding]` and `#[derive(CustomRegistryEntity)]` expand in the caller's scope, so
//! they must not depend on what the caller's `Result`, `Option` or `String` mean.

#[allow(unused_imports)]
use std::io::Result;
#[allow(unused_imports)]
use std::option::Option as String;

use process_mining::bindings::{
    call, list_functions, list_functions_meta, register_binding, AppState, CustomRegistryEntity,
    CustomRegistryValue, RegistryItem,
};
use process_mining::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;

#[register_binding]
/// Adds one
pub fn add_one_for_hygiene_test(x: i64) -> i64 {
    x + 1
}

#[derive(Debug, CustomRegistryEntity)]
pub struct HygieneHandle(u32);

impl CustomRegistryValue for HygieneHandle {
    fn kind_name() -> &'static str {
        "HygieneHandle"
    }
    fn to_value(&self) -> std::result::Result<serde_json::Value, std::string::String> {
        std::result::Result::Ok(self.0.into())
    }
}

#[register_binding]
/// The number in a stored handle
pub fn hygiene_handle_number(#[bind(handle)] h: &HygieneHandle) -> u32 {
    h.0
}

#[register_binding(stringify_error)]
/// An empty log, stored under `output_id`, next to a number
pub fn hygiene_log_and_number(n: u32) -> Result<(SlimLinkedOCEL, u32)> {
    Ok((SlimLinkedOCEL::new(), n))
}

#[register_binding(returns_handle)]
/// A stored handle next to a number, built under the write lock
pub fn hygiene_handle_pair(#[bind(handle)] h: &mut HygieneHandle) -> (HygieneHandle, u32) {
    h.0 += 1;
    (HygieneHandle(h.0), h.0)
}

#[test]
fn binding_registered_next_to_shadowed_prelude_names() {
    assert!(list_functions_meta()
        .iter()
        .any(|b| b.name == "add_one_for_hygiene_test"));

    let state = AppState::default();
    state.add("h", RegistryItem::custom(HygieneHandle(7)));
    let binding = list_functions()
        .into_iter()
        .find(|b| b.name == "hygiene_handle_number")
        .unwrap();
    let out = call(binding, &serde_json::json!({ "h": "h" }), &state).unwrap();
    assert_eq!(out, b"7");

    let binding = list_functions()
        .into_iter()
        .find(|b| b.name == "hygiene_log_and_number")
        .unwrap();
    let out = call(
        binding,
        &serde_json::json!({ "n": 3, "output_id": "log" }),
        &state,
    )
    .unwrap();
    assert_eq!(out, b"3");
    assert!(matches!(
        state.read()["log"],
        RegistryItem::SlimLinkedOCEL(_)
    ));

    let binding = list_functions()
        .into_iter()
        .find(|b| b.name == "hygiene_handle_pair")
        .unwrap();
    let out = call(
        binding,
        &serde_json::json!({ "h": "h", "output_id": "h2" }),
        &state,
    )
    .unwrap();
    assert_eq!(out, b"8");
    assert!(state.read()["h2"].as_custom::<HygieneHandle>().is_some());
}
