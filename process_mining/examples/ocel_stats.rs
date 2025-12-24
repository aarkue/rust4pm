use process_mining::{Importable, OCEL};
use std::env;
use std::error::Error;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <path_to_ocel_file>", args[0]);
        std::process::exit(1);
    }

    let path = PathBuf::from(&args[1]);
    println!("Importing OCEL from {:?}", path);

    let ocel = OCEL::import_from_path(&path)?;
    println!("Successfully imported OCEL.");
    println!("Number of events: {}", ocel.events.len());
    println!("Number of objects: {}", ocel.objects.len());

    println!(
        "Event Types: {:?}",
        ocel.event_types
            .iter()
            .map(|et| &et.name)
            .collect::<Vec<_>>()
    );
    println!(
        "Object Types: {:?}",
        ocel.object_types
            .iter()
            .map(|ot| &ot.name)
            .collect::<Vec<_>>()
    );
    Ok(())
}
