use process_mining::core::event_data::case_centric::utils::activity_projection::EventLogActivityProjection;
use process_mining::discovery::case_centric::alphappp::full::{
    alphappp_discover_petri_net, AlphaPPPConfig,
};
use process_mining::{EventLog, Exportable, Importable};
use std::env;
use std::error::Error;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <path_to_event_log> <output_pnml_path>", args[0]);
        std::process::exit(1);
    }

    let input_path = PathBuf::from(&args[1]);
    let output_path = PathBuf::from(&args[2]);

    println!("Importing event log from {:?}", input_path);
    let log = EventLog::import_from_path(&input_path)?;

    println!("Converting to activity projection...");
    let projection = EventLogActivityProjection::from(&log);

    println!("Discovering Petri net using Alpha+++...");
    let config = AlphaPPPConfig::default();
    let petri_net = alphappp_discover_petri_net(&projection, config);

    println!(
        "Discovered Petri net with {} places and {} transitions.",
        petri_net.places.len(),
        petri_net.transitions.len()
    );

    println!("Exporting Petri net to {:?}", output_path);
    petri_net.export_to_path(&output_path)?;

    println!("Done!");
    Ok(())
}
