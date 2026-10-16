//! Discovers an object-centric Petri net with the Inductive Miner.
//!
//! ```text
//! cargo run --release --example object_centric_discovery -- <ocel file> [noise threshold]
//!     [variable arc tolerance] [--types a,b,c | --exclude a,b,c] [--json]
//! ```
//!
//! A noise threshold of `0` (the default) mines the per-type nets with plain IM, anything above it
//! with IMf. The tolerance is the fraction of an activity's executions that may involve other than
//! exactly one object while its arc still counts as normal. `--types` discovers only the given
//! object types, `--exclude` all but them. `--json` prints only the net as JSON.
use process_mining::core::event_data::object_centric::linked_ocel::IndexLinkedOCEL;
use process_mining::discovery::case_centric::inductive_miner::InductiveMinerOptions;
use process_mining::discovery::object_centric::ocpn::{
    discover_ocpn, ObjectCentricDiscoveryOptions, ObjectTypeFilter,
};
use process_mining::{Importable, OCEL};
use std::env;
use std::error::Error;
use std::time::Instant;

fn main() -> Result<(), Box<dyn Error>> {
    let all: Vec<String> = env::args().collect();
    let as_json = all.iter().any(|arg| arg == "--json");
    let list = |flag: &str| {
        let at = all.iter().position(|arg| arg == flag)?;
        Some(
            all.get(at + 1)?
                .split(',')
                .map(str::trim)
                .collect::<Vec<_>>(),
        )
    };
    let object_types = match (list("--types"), list("--exclude")) {
        (Some(only), _) => ObjectTypeFilter::only(only),
        (None, Some(except)) => ObjectTypeFilter::except(except),
        (None, None) => ObjectTypeFilter::All,
    };

    let mut args: Vec<&String> = Vec::new();
    let mut rest = all.iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--json" => {}
            "--types" | "--exclude" => {
                rest.next();
            }
            _ => args.push(arg),
        }
    }
    if args.len() < 2 || args.len() > 4 {
        eprintln!(
            "Usage: {} <path_to_ocel> [noise_threshold] [variable_arc_tolerance] \
             [--types a,b,c | --exclude a,b,c] [--json]",
            args[0]
        );
        std::process::exit(1);
    }
    let parse_or_zero = |at: usize| args.get(at).map_or(Ok(0.0), |value| value.parse::<f64>());
    let noise_threshold = parse_or_zero(2)?;
    let tolerance = parse_or_zero(3)?;

    let ocel = OCEL::import_from_path(args[1])?;
    let (events, objects, types) = (
        ocel.events.len(),
        ocel.objects.len(),
        ocel.object_types.len(),
    );
    let locel = IndexLinkedOCEL::from(ocel);

    let options = ObjectCentricDiscoveryOptions::new(InductiveMinerOptions::imf(noise_threshold))
        .with_variable_arc_tolerance(tolerance)
        .with_object_types(object_types);
    let started = Instant::now();
    let ocpn = discover_ocpn(&locel, options);
    let elapsed = started.elapsed();

    if as_json {
        println!("{}", ocpn.to_json());
        return Ok(());
    }
    let activities = ocpn.activities();
    println!("Imported {events} events and {objects} objects of {types} object types.");
    println!(
        "Discovered an object-centric Petri net in {elapsed:?}: {} places, {} transitions, \
         {} arcs, {} activities.\n",
        ocpn.num_places(),
        ocpn.num_transitions(),
        ocpn.num_arcs(),
        activities.len()
    );

    for object_type in ocpn.object_types() {
        let net = &ocpn.nets[object_type];
        let variable: Vec<&str> = activities
            .iter()
            .copied()
            .filter(|activity| ocpn.is_variable_arc(object_type, activity))
            .collect();
        println!(
            "  {object_type:<28} {:>4} places {:>4} transitions {:>4} arcs   variable arcs: {}",
            net.places.len(),
            net.transitions.len(),
            net.arcs.len(),
            if variable.is_empty() {
                "-".to_string()
            } else {
                variable.join(", ")
            }
        );
    }

    println!("\nActivities shared between object types:");
    for activity in &activities {
        let types = ocpn.object_types_of(activity);
        if types.len() > 1 {
            println!("  {activity:<40} {}", types.join(", "));
        }
    }

    Ok(())
}
