use process_mining::{Exportable, Importable, PetriNet};
use std::env;
use std::error::Error;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <input_pnml_path> <output_pnml_path>", args[0]);
        std::process::exit(1);
    }

    let input_path = PathBuf::from(&args[1]);
    let output_path = PathBuf::from(&args[2]);

    println!("Importing Petri net from {:?}", input_path);
    let petri_net = PetriNet::import_from_path(&input_path)?;

    println!("Petri net stats:");
    println!("  Places: {}", petri_net.places.len());
    println!("  Transitions: {}", petri_net.transitions.len());
    println!("  Arcs: {}", petri_net.arcs.len());

    println!("Exporting Petri net to {:?}", output_path);
    petri_net.export_to_path(&output_path)?;

    println!("Done!");
    Ok(())
}
