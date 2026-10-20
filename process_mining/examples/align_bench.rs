//! Benchmark harness for alignment memory/runtime, over a selectable subset of trace variants.
//!
//! Variants are selected by a content hash of their activity sequence, because
//! [`log_to_activity_projection`] does not order variants deterministically across runs.
//!
//! ```text
//! align_bench discover <log> <net> <mib>              # align all variants, list the expensive ones
//! align_bench run <log> <net> <mib> <all|k1,k2,k3>    # align only the variants with these keys
//!
//! `<mib>` is the memory limit of all searches together, see `AlignmentOptions::max_memory_mib`.
//! ```
use process_mining::conformance::alignments::{align_variants, AlignmentOptions};
use process_mining::core::event_data::case_centric::utils::activity_projection::{
    log_to_activity_projection, EventLogActivityProjection,
};
use process_mining::{EventLog, Importable, PetriNet};
use std::error::Error;
use std::time::Instant;

fn peak_rss_mb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))?
                .split_whitespace()
                .nth(1)?
                .parse::<u64>()
                .ok()
        })
        .map(|kb| kb / 1024)
        .unwrap_or(0)
}

/// A projection holding only the variants at `keep`, preserving their order
fn subset(proj: &EventLogActivityProjection, keep: &[usize]) -> EventLogActivityProjection {
    EventLogActivityProjection {
        activities: proj.activities.clone(),
        act_to_index: proj.act_to_index.clone(),
        traces: keep.iter().map(|&i| proj.traces[i].clone()).collect(),
    }
}

/// Stable identifier for a variant: FNV-1a over its activity *names*.
/// Neither the variant order nor the activity indices are stable across runs.
fn variant_key(proj: &EventLogActivityProjection, variant: usize) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &act in &proj.traces[variant].0 {
        for b in proj.activities[act].as_bytes().iter().chain(b"\x1f") {
            h = (h ^ *b as u64).wrapping_mul(0x1000_0000_01b3);
        }
    }
    h
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!(
            "usage:\n  {0} discover <log> <net> <mib>\n  {0} run <log> <net> <mib> <all|i,j,k>",
            args[0]
        );
        std::process::exit(1);
    }
    let (mode, log_path, net_path, cap) = (&args[1], &args[2], &args[3], args[4].parse::<usize>()?);

    let log = EventLog::import_from_path(log_path)?;
    let net = PetriNet::import_from_path(net_path)?;
    let proj = log_to_activity_projection(&log);

    let options = AlignmentOptions {
        max_memory_mib: Some(cap),
        ..AlignmentOptions::default()
    };

    let selected: Vec<usize> = match args.get(5).map(String::as_str) {
        None | Some("all") => (0..proj.traces.len()).collect(),
        Some(list) => {
            let wanted: Vec<u64> = list
                .split(',')
                .map(|s| u64::from_str_radix(s.trim(), 16))
                .collect::<Result<_, _>>()?;
            // Keep the requested order, so the report is comparable across runs
            let by_key: std::collections::HashMap<u64, usize> = (0..proj.traces.len())
                .map(|i| (variant_key(&proj, i), i))
                .collect();
            wanted
                .iter()
                .map(|k| {
                    by_key
                        .get(k)
                        .copied()
                        .ok_or_else(|| format!("no variant with key {k:016x}"))
                })
                .collect::<Result<_, _>>()?
        }
    };
    let sub = subset(&proj, &selected);

    eprintln!(
        "cap={cap} variants={}/{} threads={}",
        selected.len(),
        proj.traces.len(),
        rayon::current_num_threads()
    );

    // Repeat so short workloads are long enough to measure against process start-up and import
    let repeats: usize = std::env::var("REPEAT")
        .ok()
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or(1);
    let now = Instant::now();
    let mut results = align_variants(&net, &sub, &options);
    for _ in 1..repeats {
        results = align_variants(&net, &sub, &options);
    }
    let wall = now.elapsed();

    let mut states_total: u64 = 0;
    let mut cost_total: u64 = 0;
    let mut failed: Vec<u64> = Vec::new();
    let mut rows: Vec<(u64, usize, usize)> = Vec::new(); // (key, len, states)
    for (slot, res) in results.iter().enumerate() {
        let key = variant_key(&proj, selected[slot]);
        match &res.result {
            Ok(r) => {
                states_total += r.states_visited as u64;
                cost_total += r.cost as u64;
                rows.push((key, res.activities.len(), r.states_visited));
            }
            Err(_) => failed.push(key),
        }
    }

    if mode == "discover" {
        rows.sort_by_key(|r| std::cmp::Reverse(r.2));
        eprintln!("failed (hit cap), {} keys:", failed.len());
        for k in failed.iter().take(12) {
            eprintln!("  {k:016x}");
        }
        eprintln!("top 10 finished (key, len, states):");
        for (k, len, st) in rows.iter().take(10) {
            eprintln!("  {k:016x} len={len} states={st}");
        }
    }

    // Single machine-readable line, so before/after runs diff cleanly
    println!(
        "RESULT wall_ms={} peak_rss_mb={} states_total={} cost_total={} failed={}",
        wall.as_millis(),
        peak_rss_mb(),
        states_total,
        cost_total,
        failed.len()
    );
    Ok(())
}
