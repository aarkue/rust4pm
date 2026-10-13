use std::{
    collections::{HashMap, HashSet},
    env,
    fs::File,
    hint::black_box,
    path::PathBuf,
    time::Instant,
};

use process_mining::{
    core::{
        event_data::object_centric::linked_ocel::{LinkedOCELAccess, SlimLinkedOCEL},
        process_models::oc_declare::{OCDeclareArc, OCDeclareArcType},
    },
    discovery::object_centric::oc_declare::{
        discover_behavior_constraints, project_oc_arcs, O2OMode, OCDeclareDiscoveryOptions,
        OCDeclareReductionMode,
    },
    Importable, OCEL,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProjectionResult {
    k: usize,
    selection_mode: String,
    activities: Vec<String>,
    original_count: usize,
    naive_count: usize,
    naive_duration_seconds: f64,
    smart_count: usize,
    smart_duration_seconds: f64,
    smart_recovered: usize,
    naive_connected_pairs: usize,
    smart_connected_pairs: usize,
    naive_lost_pairs: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EvaluationSummary {
    log_name: String,
    seed: u64,
    num_events: usize,
    num_objects: usize,
    num_event_types: usize,
    num_object_types: usize,
    discovery_duration_seconds: f64,
    total_constraints: usize,
    projections: Vec<ProjectionResult>,
}

/// SplitMix64, so that random activity samples are reproducible without extra dependencies
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = (self.next() % (i as u64 + 1)) as usize;
            items.swap(i, j);
        }
    }
}

fn count_connected_pairs(arcs: &[OCDeclareArc]) -> usize {
    arcs.iter()
        .map(|a| (a.from.as_str(), a.to.as_str()))
        .collect::<HashSet<_>>()
        .len()
}

fn run_projection(
    all_arcs: &[OCDeclareArc],
    selected_acts: &HashSet<String>,
    selected_names: Vec<String>,
    k: usize,
    selection_mode: &str,
) -> ProjectionResult {
    let now = Instant::now();
    let naive_result: Vec<OCDeclareArc> = black_box(
        all_arcs
            .iter()
            .filter(|a| {
                selected_acts.contains(a.from.as_str()) && selected_acts.contains(a.to.as_str())
            })
            .cloned()
            .collect(),
    );
    let naive_duration = now.elapsed().as_secs_f64();

    let now = Instant::now();
    let smart_result = black_box(project_oc_arcs(
        all_arcs.to_vec(),
        selected_acts,
        OCDeclareReductionMode::Lossless,
    ));
    let smart_duration = now.elapsed().as_secs_f64();

    let naive_pairs = count_connected_pairs(&naive_result);
    let smart_pairs = count_connected_pairs(&smart_result);
    ProjectionResult {
        k,
        selection_mode: selection_mode.to_string(),
        activities: selected_names,
        original_count: all_arcs.len(),
        naive_count: naive_result.len(),
        naive_duration_seconds: naive_duration,
        smart_count: smart_result.len(),
        smart_duration_seconds: smart_duration,
        smart_recovered: smart_result.len().saturating_sub(naive_result.len()),
        naive_connected_pairs: naive_pairs,
        smart_connected_pairs: smart_pairs,
        naive_lost_pairs: smart_pairs.saturating_sub(naive_pairs),
    }
}

fn main() {
    let base_path: PathBuf = env::args()
        .nth(1)
        .expect("Please provide a base path for the OCEL 2.0 files as the first argument!")
        .into();
    let seed: u64 = env::args()
        .nth(2)
        .map(|s| s.parse().expect("Second argument must be the random seed"))
        .unwrap_or(42);
    let num_random_samples: usize = env::args()
        .nth(3)
        .map(|s| {
            s.parse()
                .expect("Third argument must be the number of random samples per k")
        })
        .unwrap_or(10);
    println!(
        "Using base path {:?}, seed {}, {} random samples per k",
        base_path, seed, num_random_samples
    );
    let event_logs = vec![
        ("Logistics", base_path.join("ContainerLogistics.json")),
        ("P2P", base_path.join("ocel2-p2p.json")),
        ("O2C", base_path.join("order-management.json")),
        (
            "BPIC2017",
            base_path.join("bpic2017-o2o-workflow-qualifier-index-no-ev-attrs.json"),
        ),
        ("BPIC2014", base_path.join("BPIC14.jsonocel-OCEL2.xml")),
        ("BPIC2019", base_path.join("BPIC19.jsonocel-OCEL2.xml")),
    ];
    let ks = [2, 3, 4, 5, 6, 7, 8];
    let mut rng = SplitMix64(seed);

    for (name, log_path) in event_logs {
        println!("\n=== Evaluating on {} ===", name);
        let ocel = OCEL::import_from_path(&log_path).expect("Failed to import OCEL.");
        let num_evs = ocel.events.len();
        let num_obs = ocel.objects.len();
        let num_et = ocel.event_types.len();
        let num_ot = ocel.object_types.len();
        let locel = SlimLinkedOCEL::from_ocel(ocel);

        let options = OCDeclareDiscoveryOptions {
            noise_threshold: 0.2,
            o2o_mode: O2OMode::None,
            acts_to_use: None,
            counts_for_generation: (Some(1), None),
            counts_for_filter: (Some(1), None),
            reduction: OCDeclareReductionMode::Lossless,
            refinement: false,
            considered_arrow_types: [
                OCDeclareArcType::AS,
                OCDeclareArcType::EF,
                OCDeclareArcType::EP,
            ]
            .into_iter()
            .collect(),
        };
        let now = Instant::now();
        let all_arcs = black_box(discover_behavior_constraints(&locel, options));
        let discovery_duration = now.elapsed().as_secs_f64();
        println!(
            "  Discovered {} constraints in {:.3}s",
            all_arcs.len(),
            discovery_duration
        );

        let act_counts: HashMap<String, usize> = locel
            .get_ev_types()
            .map(|et| (et.to_string(), locel.get_evs_of_type(et).count()))
            .collect();
        let mut all_act_names: Vec<String> = act_counts.keys().cloned().collect();
        // Ties are broken by name, so that the top-k selection does not depend on hash order
        all_act_names.sort_by(|a, b| act_counts[b].cmp(&act_counts[a]).then_with(|| a.cmp(b)));

        let mut summary = EvaluationSummary {
            log_name: name.to_string(),
            seed,
            num_events: num_evs,
            num_objects: num_obs,
            num_event_types: num_et,
            num_object_types: num_ot,
            discovery_duration_seconds: discovery_duration,
            total_constraints: all_arcs.len(),
            projections: Vec::new(),
        };

        for &k in &ks {
            if k > all_act_names.len() {
                println!(
                    "  k={}: skipping (only {} event types)",
                    k,
                    all_act_names.len()
                );
                continue;
            }
            let top_k_names: Vec<String> = all_act_names.iter().take(k).cloned().collect();
            let top_k_acts: HashSet<String> = top_k_names.iter().cloned().collect();
            let res = run_projection(&all_arcs, &top_k_acts, top_k_names, k, "top-k");
            println!(
                "  k={} top-k: naive={} | smart={} | recovered={}",
                k, res.naive_count, res.smart_count, res.smart_recovered
            );
            summary.projections.push(res);

            for sample_i in 0..num_random_samples {
                let mut shuffled = all_act_names.clone();
                rng.shuffle(&mut shuffled);
                let random_names: Vec<String> = shuffled.into_iter().take(k).collect();
                let random_acts: HashSet<String> = random_names.iter().cloned().collect();
                let res = run_projection(
                    &all_arcs,
                    &random_acts,
                    random_names,
                    k,
                    &format!("random-{}", sample_i),
                );
                summary.projections.push(res);
            }
        }

        let out_file = File::create(format!("{}-projection-evaluation.json", name)).unwrap();
        serde_json::to_writer_pretty(out_file, &summary).unwrap();
        println!("  Results written to {}-projection-evaluation.json", name);
    }
}
