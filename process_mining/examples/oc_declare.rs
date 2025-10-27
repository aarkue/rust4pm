use std::{env::args, path::PathBuf};

use process_mining::{
    import_ocel_xml_file,
    object_centric::oc_declare::{self, OCDeclareDiscoveryOptions},
};

pub fn main() {
    let path_opt = args().nth(1);
    if let Some(path) = path_opt.map(PathBuf::from) {
        let ocel = import_ocel_xml_file(&path);
        let processed_locel = oc_declare::preprocess_ocel(ocel);
        let discovered_constraints = oc_declare::discover_behavior_constraints(
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
