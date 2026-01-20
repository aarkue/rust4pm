use std::{env::args, path::PathBuf};

use process_mining::{
    core::event_data::object_centric::linked_ocel::SlimLinkedOCEL,
    discovery::object_centric::oc_declare::{
        discover_behavior_constraints, OCDeclareDiscoveryOptions,
    },
    Importable, OCEL,
};

pub fn main() {
    let path_opt = args().nth(1);
    if let Some(path) = path_opt.map(PathBuf::from) {
        let ocel = OCEL::import_from_path(&path).expect("Failed to import OCEL.");
        let locel = SlimLinkedOCEL::from_ocel(ocel);
        let discovered_constraints = discover_behavior_constraints(
            &locel,
            OCDeclareDiscoveryOptions {
                ..Default::default()
            },
        );
        println!(
            "Discovered {} OC-DECLARE constraints",
            discovered_constraints.len()
        );
    }
}
