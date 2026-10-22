use process_mining::conformance::alignments::cost::CostFunction;
use process_mining::conformance::alignments::{
    align_empty_trace, align_log, compute_fitness, AlignmentOptions,
};
use process_mining::core::event_data::case_centric::utils::activity_projection::EventLogActivityProjection;
use process_mining::{AcceptingPetriNet, EventLog, Importable};
use std::env;
use std::error::Error;
use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <path_to_event_log> <path_to_petri_net>", args[0]);
        std::process::exit(1);
    }

    let path = PathBuf::from(&args[1]);
    println!("Importing event log from {:?}", path);
    let log = EventLog::import_from_path(&path)?;
    println!("Successfully imported event log.");

    let pn_path = PathBuf::from(&args[2]);
    println!("Importing Petri net from {:?}", pn_path);
    let pn = AcceptingPetriNet::import_from_path(&pn_path)?;

    let options = AlignmentOptions {
        cost_fn: CostFunction::standard(),
        ..AlignmentOptions::default()
    };
    let now = Instant::now();
    let empty_trace = align_empty_trace(&pn, &options);
    println!(
        "Empty-trace alignment took {:?}. Result: {:?}",
        now.elapsed(),
        empty_trace
    );
    let now = Instant::now();
    let alignments_res = align_log(&pn, &EventLogActivityProjection::from(&log), &options);
    println!("All trace alignments took {:?}.", now.elapsed());

    let f = compute_fitness(&alignments_res, &pn, &options);

    println!("{:?}", f);

    Ok(())
}
