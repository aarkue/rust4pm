use process_mining::{EventLog, Importable};
use std::env;
use std::error::Error;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <path_to_event_log>", args[0]);
        std::process::exit(1);
    }

    let path = PathBuf::from(&args[1]);
    println!("Importing event log from {:?}", path);

    let log = EventLog::import_from_path(&path)?;
    println!("Successfully imported event log.");
    println!("Number of traces: {}", log.traces.len());

    let total_events: usize = log.traces.iter().map(|t| t.events.len()).sum();
    println!("Total number of events: {}", total_events);

    if !log.traces.is_empty() {
        let avg_events = total_events as f64 / log.traces.len() as f64;
        println!("Average events per trace: {:.2}", avg_events);
    }

    // Example: Print first trace ID if available
    if let Some(first_trace) = log.traces.first() {
        if let Some(attr) = first_trace
            .attributes
            .iter()
            .find(|a| a.key == "concept:name")
        {
            println!("First trace ID: {:?}", attr.value);
        }
    }
    Ok(())
}
