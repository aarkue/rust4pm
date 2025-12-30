use std::{env::args, path::PathBuf};

use process_mining::{
    core::process_models::object_centric::oc_declare,
    discovery::object_centric::oc_declare::{
        discover_behavior_constraints, OCDeclareDiscoveryOptions,
    },
    Importable, OCEL,
};

pub fn main() {
    let path_opt = args().nth(1);
    if let Some(path) = path_opt.map(PathBuf::from) {
        let ocel = OCEL::import_from_path(&path).expect("Failed to import OCEL.");
        let processed_locel = oc_declare::preprocess_ocel(ocel);
        let discovered_constraints = discover_behavior_constraints(
            &processed_locel,
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
