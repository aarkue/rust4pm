//! Registry kinds for process models, so a host can keep a model as a handle
//!
//! Each model type is a [`CustomRegistryValue`] registered under its type name, rather than a
//! built-in [`RegistryItem`] variant: a built-in variant would make every existing binding that
//! returns the model store it and hand back an id instead of its JSON.
//!
//! A model reaches the registry through [`RegistryItem::custom`], [`RegistryItem::load_from_path`]
//! / [`RegistryItem::load_from_bytes`] (`json`, and `pnml` for the Petri nets), or a
//! `#[register_binding(returns_handle)]` binding. A `#[bind(handle)] &T` argument borrows it.

use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

use super::{CustomRegistryValue, FromContext, InnerAppState};
use crate::analysis::object_centric::reflow::{AnnotatedOcpn, ReflowResult};
use crate::core::{
    io::{Exportable, ExtensionWithMime, Importable},
    process_models::{
        case_centric::process_tree::ProcessTree,
        object_centric::{
            oc_declare::OCDeclareArc,
            ocpn::{json::ObjectCentricPetriNetJson, ObjectCentricPetriNet},
        },
    },
    AcceptingPetriNet, PetriNet,
};

fn json_format() -> ExtensionWithMime {
    ExtensionWithMime::new("json", "application/json")
}

fn unsupported(kind: &str, format: &str) -> String {
    format!("{} has no '{}' format", kind, format)
}

/// What `#[derive(CustomRegistryEntity)]` generates, for types defined outside this module.
macro_rules! handle_from_context {
    ($t:ty) => {
        impl<'a> FromContext<'a> for &'a $t {
            fn from_context(value: &Value, state: &'a InnerAppState) -> Result<Self, String> {
                let id = value.as_str().ok_or("Expected String ID")?;
                let item = state
                    .get(id)
                    .ok_or_else(|| format!("Item '{}' not found", id))?;
                item.as_custom::<$t>().ok_or_else(|| {
                    format!(
                        "ID '{}' is not a {}",
                        id,
                        <$t as CustomRegistryValue>::kind_name()
                    )
                })
            }
        }
    };
}

/// How a model kind reads its JSON form, and the file formats it has besides JSON.
trait ModelFormats: CustomRegistryValue + Serialize + DeserializeOwned {
    /// Read the JSON form, where that takes more than plain `Deserialize`.
    fn from_json_value(value: &Value) -> Result<Self, String> {
        Self::deserialize(value).map_err(|e| e.to_string())
    }

    /// Read the JSON form from bytes. A kind overriding [`ModelFormats::from_json_value`] reads
    /// through it with [`via_value`].
    fn from_json_bytes(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|e| e.to_string())
    }

    fn other_formats() -> Vec<ExtensionWithMime> {
        Vec::new()
    }

    fn other_export_formats() -> Vec<ExtensionWithMime> {
        Self::other_formats()
    }

    fn import_other(_bytes: &[u8], format: &str) -> Result<Self, String> {
        Err(unsupported(Self::kind_name(), format))
    }

    fn export_other(&self, format: &str) -> Result<Vec<u8>, String> {
        Err(unsupported(Self::kind_name(), format))
    }
}

fn via_value<T: ModelFormats>(bytes: &[u8]) -> Result<T, String> {
    let value: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    T::from_json_value(&value)
}

/// A registry kind whose formats are its JSON form and the [`ModelFormats`] of `$t`.
macro_rules! model_kind {
    ($t:ty, $name:literal) => {
        impl CustomRegistryValue for $t {
            fn kind_name() -> &'static str {
                $name
            }
            fn to_value(&self) -> Result<Value, String> {
                serde_json::to_value(self).map_err(|e| e.to_string())
            }
            fn from_value(value: &Value) -> Result<Self, String> {
                <$t as ModelFormats>::from_json_value(value)
            }
            fn from_bytes(bytes: &[u8], format: &str) -> Result<Self, String> {
                if format.ends_with("json") {
                    <$t as ModelFormats>::from_json_bytes(bytes)
                } else {
                    <$t as ModelFormats>::import_other(bytes, format)
                }
            }
            fn export_to_bytes(&self, format: &str) -> Result<Vec<u8>, String> {
                if format.ends_with("json") {
                    serde_json::to_vec(self).map_err(|e| e.to_string())
                } else {
                    self.export_other(format)
                }
            }
            fn known_import_formats() -> Vec<ExtensionWithMime> {
                let mut formats = vec![json_format()];
                formats.extend(<$t as ModelFormats>::other_formats());
                formats
            }
            fn known_export_formats() -> Vec<ExtensionWithMime> {
                let mut formats = vec![json_format()];
                formats.extend(<$t as ModelFormats>::other_export_formats());
                formats
            }
        }
        handle_from_context!($t);
        crate::register_custom_registry_kind!($t, $name);
    };
}

/// The Petri nets: PNML besides JSON.
macro_rules! pnml_formats {
    ($($t:ty),*) => {$(
        impl ModelFormats for $t {
            fn other_formats() -> Vec<ExtensionWithMime> {
                <$t as Importable>::known_import_formats()
            }
            fn other_export_formats() -> Vec<ExtensionWithMime> {
                <$t as Exportable>::known_export_formats()
            }
            fn import_other(bytes: &[u8], format: &str) -> Result<Self, String> {
                <$t>::import_from_bytes(bytes, format).map_err(|e| e.to_string())
            }
            fn export_other(&self, format: &str) -> Result<Vec<u8>, String> {
                let mut bytes = Vec::new();
                self.export_to_writer(&mut bytes, format)
                    .map_err(|e| e.to_string())?;
                Ok(bytes)
            }
        }
    )*};
}

pnml_formats!(PetriNet, AcceptingPetriNet);
impl ModelFormats for ProcessTree {}
impl ModelFormats for ReflowResult {}
impl ModelFormats for AnnotatedOcpn {}

/// Also reads the flat form of [`ObjectCentricPetriNet::to_json`].
impl ModelFormats for ObjectCentricPetriNet {
    fn from_json_value(value: &Value) -> Result<Self, String> {
        ObjectCentricPetriNet::deserialize(value).or_else(|e| {
            ObjectCentricPetriNetJson::deserialize(value)
                .map(|flat| ObjectCentricPetriNet::from_json_form(&flat))
                .map_err(|_| e.to_string())
        })
    }
    fn from_json_bytes(bytes: &[u8]) -> Result<Self, String> {
        via_value(bytes)
    }
}

/// Also reads an object with an `arcs` list, the shape of a saved model with layout.
impl ModelFormats for OCDeclareModel {
    fn from_json_value(value: &Value) -> Result<Self, String> {
        let arcs = match value.get("arcs") {
            Some(arcs) if value.is_object() => arcs,
            _ => value,
        };
        Vec::<OCDeclareArc>::deserialize(arcs)
            .map(OCDeclareModel::from)
            .map_err(|e| e.to_string())
    }
    fn from_json_bytes(bytes: &[u8]) -> Result<Self, String> {
        via_value(bytes)
    }
}

/// An OC-DECLARE model: the list of its constraint arcs
///
/// The JSON form is the bare arc list, as the OC-DECLARE discovery bindings return it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct OCDeclareModel {
    /// The constraint arcs
    pub arcs: Vec<OCDeclareArc>,
}

impl From<Vec<OCDeclareArc>> for OCDeclareModel {
    fn from(arcs: Vec<OCDeclareArc>) -> Self {
        Self { arcs }
    }
}

model_kind!(PetriNet, "PetriNet");
model_kind!(AcceptingPetriNet, "AcceptingPetriNet");
model_kind!(ProcessTree, "ProcessTree");
model_kind!(ObjectCentricPetriNet, "ObjectCentricPetriNet");
model_kind!(OCDeclareModel, "OCDeclareModel");
model_kind!(ReflowResult, "ReflowResult");
model_kind!(AnnotatedOcpn, "AnnotatedOcpn");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bindings::{
        call, call_resolved, list_functions, AppState, RegistryItem, RegistryItemKind,
    };
    use crate::core::process_models::{
        case_centric::{
            petri_net::{ArcType, Marking},
            process_tree::{Node, OperatorType},
        },
        object_centric::oc_declare::{OCDeclareArcLabel, OCDeclareArcType, OCDeclareNode},
    };
    use crate::test_utils::get_test_data_path;
    use macros_process_mining::register_binding;

    fn kind(name: &str) -> RegistryItemKind {
        name.parse().unwrap()
    }

    fn sample_net() -> AcceptingPetriNet {
        AcceptingPetriNet::import_from_path(get_test_data_path().join("petri-net").join("pn.pnml"))
            .unwrap()
    }

    /// Count the places of a stored net, taken as a handle.
    #[register_binding]
    fn test_count_places(#[bind(handle)] net: &AcceptingPetriNet) -> usize {
        net.net.places().count()
    }

    #[test]
    fn model_kinds_are_registered_by_name() {
        for name in [
            "PetriNet",
            "AcceptingPetriNet",
            "ProcessTree",
            "ObjectCentricPetriNet",
            "OCDeclareModel",
        ] {
            let k = kind(name);
            assert_eq!(k, RegistryItemKind::Custom(name));
            assert!(RegistryItemKind::all_registered_kinds().contains(&k));
            assert!(!k.known_import_formats().is_empty(), "{}", name);
            assert!(!k.known_export_formats().is_empty(), "{}", name);
        }
        let exts: Vec<_> = kind("AcceptingPetriNet")
            .known_export_formats()
            .into_iter()
            .map(|f| f.extension)
            .collect();
        assert_eq!(exts, vec!["json", "pnml"]);
    }

    #[test]
    fn petri_net_round_trips_through_pnml_and_json() {
        let net = sample_net();
        let item = RegistryItem::custom(net.clone());
        assert_eq!(item.kind(), kind("AcceptingPetriNet"));

        // PNML renames the nodes, so only the shape survives it.
        let pnml = item.export_to_bytes("pnml").unwrap();
        let back =
            RegistryItem::load_from_bytes(&kind("AcceptingPetriNet"), &pnml, "pnml").unwrap();
        let back = back.as_custom::<AcceptingPetriNet>().unwrap();
        assert_eq!(back.net.places().len(), net.net.places().len());
        assert_eq!(back.final_marking, net.final_marking);

        let json = item.export_to_bytes("json").unwrap();
        let back =
            RegistryItem::load_from_bytes(&kind("AcceptingPetriNet"), &json, "json").unwrap();
        assert_eq!(back.as_custom::<AcceptingPetriNet>().unwrap(), &net);

        let value = item.to_value().unwrap();
        assert_eq!(value, serde_json::to_value(&net).unwrap());
        let back = RegistryItem::from_json_value(&kind("AcceptingPetriNet"), &value).unwrap();
        assert_eq!(back.as_custom::<AcceptingPetriNet>().unwrap(), &net);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("net.pnml");
        item.export_to_path(&path).unwrap();
        let back = RegistryItem::load_from_path(&kind("PetriNet"), path.to_str().unwrap()).unwrap();
        assert_eq!(
            back.as_custom::<PetriNet>().unwrap().places().len(),
            net.net.places().len()
        );

        assert!(item.export_to_bytes("xes").is_err());
    }

    #[test]
    fn json_only_kinds_round_trip() {
        let tree = ProcessTree::new(Node::new_operator(OperatorType::Sequence));
        let item = RegistryItem::custom(tree);
        let json = item.export_to_bytes("json").unwrap();
        let back = RegistryItem::load_from_bytes(&kind("ProcessTree"), &json, "json").unwrap();
        assert_eq!(back.to_value().unwrap(), item.to_value().unwrap());
        assert!(item.export_to_bytes("ptml").is_err());

        let mut ocpn = ObjectCentricPetriNet::new();
        ocpn.nets.insert("order".to_string(), sample_net());
        let item = RegistryItem::custom(ocpn.clone());
        let back = RegistryItem::from_json_value(
            &kind("ObjectCentricPetriNet"),
            &item.to_value().unwrap(),
        )
        .unwrap();
        assert_eq!(
            back.as_custom::<ObjectCentricPetriNet>().unwrap().nets,
            ocpn.nets
        );
        let flat = ocpn.to_json();
        let back =
            RegistryItem::load_from_bytes(&kind("ObjectCentricPetriNet"), flat.as_bytes(), "json")
                .unwrap();
        assert!(back
            .as_custom::<ObjectCentricPetriNet>()
            .unwrap()
            .nets
            .contains_key("order"));

        let arc = OCDeclareArc {
            from: OCDeclareNode::new("a"),
            to: OCDeclareNode::new("b"),
            arc_type: OCDeclareArcType::EF,
            label: OCDeclareArcLabel::default(),
            counts: (Some(1), None),
        };
        let model = OCDeclareModel::from(vec![arc]);
        let value = RegistryItem::custom(model.clone()).to_value().unwrap();
        assert!(value.is_array());
        for value in [
            value.clone(),
            serde_json::json!({ "arcs": value, "nodes": [] }),
        ] {
            let back = RegistryItem::from_json_value(&kind("OCDeclareModel"), &value).unwrap();
            assert_eq!(back.as_custom::<OCDeclareModel>().unwrap(), &model);
            let bytes = serde_json::to_vec(&value).unwrap();
            let back =
                RegistryItem::load_from_bytes(&kind("OCDeclareModel"), &bytes, "json").unwrap();
            assert_eq!(back.as_custom::<OCDeclareModel>().unwrap(), &model);
        }
        let err = RegistryItem::load_from_bytes(&kind("OCDeclareModel"), b"[]", "xml").unwrap_err();
        assert!(
            err.contains("OCDeclareModel has no 'xml' format"),
            "{}",
            err
        );
    }

    #[test]
    fn model_handle_crosses_the_binding_boundary() {
        let state = AppState::default();
        let net = sample_net();
        let places = net.net.places().count();
        state.add("n1", RegistryItem::custom(net));
        state.add("t1", RegistryItem::custom(OCDeclareModel::default()));
        let binding = list_functions()
            .into_iter()
            .find(|b| b.name == "test_count_places")
            .unwrap();
        assert_eq!(
            (binding.args)()[0].1["x-registry-ref"],
            serde_json::json!("AcceptingPetriNet")
        );
        let out = call(binding, &serde_json::json!({ "net": "n1" }), &state).unwrap();
        assert_eq!(out, places.to_string().as_bytes());
        let err = call(binding, &serde_json::json!({ "net": "t1" }), &state).unwrap_err();
        assert!(err.contains("is not a AcceptingPetriNet"), "{}", err);
    }

    fn binding_named(name: &str) -> &'static crate::bindings::Binding {
        list_functions()
            .into_iter()
            .find(|b| b.name == name)
            .unwrap_or_else(|| panic!("no binding {}", name))
    }

    fn assert_net_is_a_handle(name: &str, arg: &str) {
        let args = (binding_named(name).args)();
        let schema = &args.iter().find(|(n, _)| n == arg).unwrap().1;
        assert_eq!(schema["x-registry-ref"], "AcceptingPetriNet", "{}", name);
    }

    #[test]
    fn conformance_bindings_take_the_net_as_a_handle() {
        for (name, arg) in [
            ("align_variants", "net"),
            ("align_trace", "net"),
            ("compute_fitness", "net"),
            ("escaping_edges_precision", "net"),
            ("escaping_edges_precision_from_alignments", "net"),
        ] {
            assert_net_is_a_handle(name, arg);
        }

        let mut net = PetriNet::new();
        let p1 = net.add_place(None);
        let p2 = net.add_place(None);
        let a = net.add_transition(Some("a".into()), None);
        net.add_arc(ArcType::place_to_transition(p1, a), None);
        net.add_arc(ArcType::transition_to_place(a, p2), None);
        let net = AcceptingPetriNet::new(net, Marking::from([(p1, 1)]), Marking::from([(p2, 1)]));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("net.pnml");
        net.export_to_path(&path).unwrap();
        let state = AppState::default();
        state.add("n1", RegistryItem::custom(net.clone()));
        let binding = binding_named("align_trace");
        let by_id = call_resolved(
            binding,
            &serde_json::json!({ "net": "n1", "trace": [] }),
            &state,
        )
        .unwrap();
        for net in [serde_json::to_value(&net).unwrap(), path.to_str().into()] {
            let out = call_resolved(
                binding,
                &serde_json::json!({ "net": net, "trace": [] }),
                &state,
            )
            .unwrap();
            assert_eq!(out, by_id);
        }
        assert_eq!(state.read().len(), 1);
    }

    #[cfg(feature = "token-based-replay")]
    #[test]
    fn token_based_replay_binding_stringifies_its_error() {
        assert_net_is_a_handle("apply_token_based_replay", "net");
        let mut net = PetriNet::new();
        let p1 = net.add_place(None);
        let p2 = net.add_place(None);
        let tau = net.add_transition(None, None);
        net.add_arc(ArcType::place_to_transition(p1, tau), None);
        net.add_arc(ArcType::transition_to_place(tau, p2), None);
        let net = AcceptingPetriNet::new(net, Marking::from([(p1, 1)]), Marking::from([(p2, 1)]));
        let log = serde_json::json!({ "activities": [], "act_to_index": {}, "traces": [] });
        let binding = binding_named("apply_token_based_replay");
        let state = AppState::default();
        let args = serde_json::json!({ "net": net, "event_log": log });
        let err = call_resolved(binding, &args, &state).unwrap_err();
        assert!(err.contains("silent"), "{}", err);

        let mut net = PetriNet::new();
        let p = net.add_place(None);
        let net = AcceptingPetriNet::new(net, Marking::new(), Marking::from([(p, 1)]));
        let args = serde_json::json!({ "net": net, "event_log": log });
        let out: Value =
            serde_json::from_slice(&call_resolved(binding, &args, &state).unwrap()).unwrap();
        assert!(out.get("produced").is_some(), "{}", out);
    }

    #[test]
    fn escaping_edges_options_binding_returns_the_presets() {
        use crate::conformance::case_centric::precision::EscapingEdgesOptions;
        let binding = binding_named("escaping_edges_options");
        let state = AppState::default();
        for (preset, expected) in [
            (None, EscapingEdgesOptions::default()),
            (Some("default"), EscapingEdgesOptions::default()),
            (Some("prom"), EscapingEdgesOptions::prom()),
            (Some("pm4py"), EscapingEdgesOptions::pm4py()),
        ] {
            let args = match preset {
                Some(p) => serde_json::json!({ "preset": p }),
                None => serde_json::json!({}),
            };
            let out: Value =
                serde_json::from_slice(&call(binding, &args, &state).unwrap()).unwrap();
            assert_eq!(out, serde_json::to_value(expected).unwrap(), "{:?}", preset);
        }
        assert!(call(binding, &serde_json::json!({ "preset": "x" }), &state).is_err());
    }
}
