use std::{env::args, path::PathBuf};

use process_mining::{
    core::event_data::object_centric::linked_ocel::SlimLinkedOCEL,
    discovery::object_centric::oc_declare::{discover_oc_declare, OCDeclareDiscoveryOptions},
    Importable, OCEL,
};

pub fn main() {
    let args: Vec<String> = args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <path_to_ocel>", args[0]);
        std::process::exit(1);
    }
    let path = PathBuf::from(&args[1]);
    let ocel = OCEL::import_from_path(&path).expect("Failed to import OCEL.");
    let locel = SlimLinkedOCEL::from_ocel(ocel);
    let discovered_constraints = discover_oc_declare(&locel, OCDeclareDiscoveryOptions::default());
    println!(
        "Discovered {} OC-DECLARE constraints",
        discovered_constraints.len()
    );
}
