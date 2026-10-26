//! Discovers a process tree with the Inductive Miner.
//!
//! ```text
//! cargo run --release --example inductive_miner -- <event log> [noise threshold] [output.pnml]
//!     [--json] [--dfg] [--thesis] [--like-prom] [--like-pm4py] [option flags]
//! ```
//!
//! A noise threshold of `0` (the default) runs plain IM, anything above it IMf. Given an output
//! path, the tree is also exported as a PNML Petri net. `--json` prints only the tree as JSON.
//! `--dfg` runs the directly-follows variant IMd/IMfd.
//!
//! `--like-prom` and `--like-pm4py` select a preset of [`InductiveMinerOptions`] or
//! [`InductiveMinerDfgOptions`], `--thesis` one of [`InductiveMinerOptions`]. The remaining flags
//! toggle single [`InductiveMinerOptions`] on top of it, so `--dfg` ignores them.
use process_mining::core::event_data::case_centric::EventLogClassifier;
use process_mining::discovery::case_centric::inductive_miner::{
    discover_inductive_tree_from_log, InductiveMinerDfgOptions, InductiveMinerOptions,
};
use process_mining::{EventLog, Exportable, Importable};
use std::env;
use std::error::Error;
use std::time::Instant;

const FLAGS: &[&str] = &[
    "--json",
    "--dfg",
    "--thesis",
    "--like-prom",
    "--like-pm4py",
    "--interleaved",
    "--inclusive-choice",
    "--no-msd",
    "--no-strict-sequence",
    "--guard-empty-cut-parts",
    "--degenerate-tau-loops",
    "--no-fallthroughs",
    "--no-filter-single-activity",
    "--filter-fallthrough-probe",
];

fn main() -> Result<(), Box<dyn Error>> {
    let all: Vec<String> = env::args().collect();
    let set = |flag: &str| all.iter().any(|arg| arg == flag);

    if let Some(unknown) = all
        .iter()
        .find(|arg| arg.starts_with("--") && !FLAGS.contains(&arg.as_str()))
    {
        eprintln!("Unknown flag {unknown}. Known flags: {}", FLAGS.join(" "));
        std::process::exit(2);
    }
    let args: Vec<&String> = all.iter().filter(|arg| !arg.starts_with("--")).collect();
    if args.len() < 2 || args.len() > 4 {
        eprintln!(
            "Usage: {} <path_to_event_log> [noise_threshold] [output_pnml_path] {}",
            args[0],
            FLAGS.join(" ")
        );
        std::process::exit(1);
    }
    let noise_threshold: f64 = match args.get(2) {
        Some(value) => value.parse()?,
        None => 0.0,
    };

    let log = EventLog::import_from_path(args[1])?;
    let classifier = EventLogClassifier::default();
    let started = Instant::now();
    let tree = if set("--dfg") {
        let base = if set("--like-pm4py") {
            InductiveMinerDfgOptions::pm4py(noise_threshold)
        } else if set("--like-prom") {
            InductiveMinerDfgOptions::prom(noise_threshold)
        } else {
            InductiveMinerDfgOptions::imfd(noise_threshold)
        };
        discover_inductive_tree_from_log(&log, &classifier, base)
    } else {
        let base = if set("--thesis") {
            InductiveMinerOptions::imf_thesis(noise_threshold)
        } else if set("--like-pm4py") {
            InductiveMinerOptions::pm4py(noise_threshold)
        } else if set("--like-prom") {
            InductiveMinerOptions::prom(noise_threshold)
        } else {
            InductiveMinerOptions::imf(noise_threshold)
        };
        let options = InductiveMinerOptions {
            use_interleaved: base.use_interleaved || set("--interleaved"),
            use_inclusive_choice: base.use_inclusive_choice || set("--inclusive-choice"),
            use_minimum_self_distance: base.use_minimum_self_distance && !set("--no-msd"),
            strict_sequence: base.strict_sequence && !set("--no-strict-sequence"),
            guard_empty_cut_parts: base.guard_empty_cut_parts || set("--guard-empty-cut-parts"),
            use_degenerate_tau_loops: base.use_degenerate_tau_loops
                || set("--degenerate-tau-loops"),
            use_fallthroughs: base.use_fallthroughs && !set("--no-fallthroughs"),
            filter_single_activity: base.filter_single_activity
                && !set("--no-filter-single-activity"),
            filter_activity_concurrent_probe: base.filter_activity_concurrent_probe
                || set("--filter-fallthrough-probe"),
            ..base
        };
        discover_inductive_tree_from_log(&log, &classifier, options)
    };
    let elapsed = started.elapsed();

    if set("--json") {
        println!("{}", serde_json::to_string(&tree)?);
        return Ok(());
    }
    println!("Discovered in {elapsed:?}:\n\n{tree}\n");
    println!("The tree has {} leaves.", tree.find_all_leaves().len());
    if let Some(output_path) = args.get(3) {
        tree.to_petri_net().export_to_path(output_path)?;
        println!("Exported the Petri net to {output_path}.");
    }
    Ok(())
}
