use process_mining::core::process_models::case_centric::petri_net::pnml::{
    export_pnml, import_pnml,
};
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufWriter};
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
    let (petri_net, initial_marking, final_marking) =
        import_pnml(BufReader::new(File::open(&input_path)?))?;

    println!("Petri net stats:");
    println!("  Places: {}", petri_net.num_places());
    println!("  Transitions: {}", petri_net.num_transitions());
    println!("  Arcs: {}", petri_net.arcs().count());
    println!("  Initial marking: {initial_marking:?}");
    println!("  Final marking: {final_marking:?}");

    println!("Exporting Petri net to {:?}", output_path);
    export_pnml(
        BufWriter::new(File::create(&output_path)?),
        &petri_net,
        initial_marking.as_ref(),
        final_marking.as_ref(),
    )?;

    println!("Done!");
    Ok(())
}
