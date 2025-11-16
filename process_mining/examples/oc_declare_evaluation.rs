use std::{env, fs::File, hint::black_box, path::PathBuf, time::Instant};

use process_mining::{
    import_ocel_json_from_path, import_ocel_xml_file,
    object_centric::oc_declare::{
        discover_behavior_constraints, preprocess_ocel, reduce_oc_arcs, O2OMode, OCDeclareArcType,
        OCDeclareDiscoveryOptions, OCDeclareReductionMode,
    },
};
use serde::{Deserialize, Serialize};

fn main() {
    let base_path: Option<String> = env::args().nth(1);
    match base_path {
        None => panic!("Please provide a base path for the OCEL 2.0 files as the first argument!"),
        Some(base_path) => {
            let path: PathBuf = PathBuf::from(base_path);
            println!("Using base path {:?}", path);
            let num_runs = 5;
            let noise_thresh = 0.2;
            let event_logs = vec![
                ("Logistics", path.join("ContainerLogistics.json")),
                ("P2P", path.join("ocel2-p2p.json")),
                ("O2C", path.join("order-management.json")),
                (
                    "BPIC2017",
                    path.join("bpic2017-o2o-workflow-qualifier-index-no-ev-attrs.json"),
                ),
                ("BPIC2014", path.join("BPIC14.jsonocel-OCEL2.xml")),
                ("BPIC2019", path.join("BPIC19.jsonocel-OCEL2.xml")),
            ];
            for (name, path) in event_logs {
                println!("Evaluating on {name}.");
                let ocel = if path
                    .extension()
                    .unwrap()
                    .to_string_lossy()
                    .ends_with("json")
                {
                    import_ocel_json_from_path(path).unwrap()
                } else {
                    import_ocel_xml_file(path)
                };
                let num_evs = ocel.events.len();
                let num_obs = ocel.objects.len();
                let num_et = ocel.event_types.len();
                let num_ot = ocel.object_types.len();

                let locel = preprocess_ocel(ocel);
                for o2o_mode in [O2OMode::None] {
                    //, O2OMode::Direct] {
                    println!("{:?}", o2o_mode);
                    let mut eval_res = EvaluationResult {
                        num_events: num_evs,
                        num_objects: num_obs,
                        num_event_types: num_et,
                        num_object_types: num_ot,
                        ..Default::default()
                    };
                    let mut res = Vec::new();
                    let mut reduced = Vec::new();
                    let mut reduced_lossy = Vec::new();
                    for i in 0..num_runs {
                        let options = OCDeclareDiscoveryOptions {
                            noise_threshold: noise_thresh,
                            o2o_mode,
                            reduction: OCDeclareReductionMode::None,
                            considered_arrow_types: vec![
                                OCDeclareArcType::AS,
                                OCDeclareArcType::EF,
                                OCDeclareArcType::EP,
                            ]
                            .into_iter()
                            .collect(),
                            ..Default::default()
                        };
                        let now = Instant::now();
                        res = black_box(discover_behavior_constraints(&locel, options));
                        let discovery_duration = now.elapsed();
                        eval_res
                            .discovery_durations_seconds
                            .push(discovery_duration.as_secs_f64());
                        if i == 0 {
                            eval_res.number_of_discovered_results = res.len();
                        } else {
                            assert_eq!(eval_res.number_of_discovered_results, res.len());
                        }
                        let now = Instant::now();
                        reduced = black_box(reduce_oc_arcs(&res, true));
                        let duration = now.elapsed();
                        eval_res.number_of_results_after_reduction = reduced.len();
                        eval_res
                            .reduction_duration_seconds
                            .push(duration.as_secs_f64());
                        if i == 0 {
                            eval_res.number_of_results_after_reduction = reduced.len();
                        } else {
                            assert_eq!(eval_res.number_of_results_after_reduction, reduced.len());
                        }

                        let now = Instant::now();
                        reduced_lossy = black_box(reduce_oc_arcs(&res, false));
                        let duration = now.elapsed();
                        eval_res.number_of_results_after_lossy_reduction = reduced_lossy.len();
                        eval_res
                            .lossy_reduction_duration_seconds
                            .push(duration.as_secs_f64());
                        if i == 0 {
                            eval_res.number_of_results_after_lossy_reduction = reduced_lossy.len();
                        } else {
                            assert_eq!(
                                eval_res.number_of_results_after_lossy_reduction,
                                reduced_lossy.len()
                            );
                        }
                        println!(
                            "Got {} (reduced to {}, lossy to {}) results in {:?}",
                            res.len(),
                            reduced.len(),
                            reduced_lossy.len(),
                            discovery_duration
                        );
                    }
                    eval_res.mean_discovery_duration =
                        eval_res.discovery_durations_seconds.iter().sum::<f64>()
                            / eval_res.discovery_durations_seconds.len() as f64;

                    eval_res.mean_reduction_duration =
                        eval_res.reduction_duration_seconds.iter().sum::<f64>()
                            / eval_res.reduction_duration_seconds.len() as f64;

                    eval_res.mean_lossy_reduction_duration = eval_res
                        .lossy_reduction_duration_seconds
                        .iter()
                        .sum::<f64>()
                        / eval_res.lossy_reduction_duration_seconds.len() as f64;

                    let summary_file =
                        File::create(format!("{}-{:?}-summary.json", name, o2o_mode)).unwrap();
                    serde_json::to_writer_pretty(summary_file, &eval_res).unwrap();

                    let results_file =
                        File::create(format!("{}-{:?}-discovered.json", name, o2o_mode)).unwrap();
                    serde_json::to_writer_pretty(results_file, &res).unwrap();
                    let reduced_file =
                        File::create(format!("{}-{:?}-reduced-lossless.json", name, o2o_mode))
                            .unwrap();
                    serde_json::to_writer_pretty(reduced_file, &reduced).unwrap();
                    let reduced_file =
                        File::create(format!("{}-{:?}-reduced-lossy.json", name, o2o_mode))
                            .unwrap();
                    serde_json::to_writer_pretty(reduced_file, &reduced_lossy).unwrap();
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct EvaluationResult {
    discovery_durations_seconds: Vec<f64>,
    mean_discovery_duration: f64,
    number_of_discovered_results: usize,
    reduction_duration_seconds: Vec<f64>,
    mean_reduction_duration: f64,
    number_of_results_after_reduction: usize,
    lossy_reduction_duration_seconds: Vec<f64>,
    mean_lossy_reduction_duration: f64,
    number_of_results_after_lossy_reduction: usize,
    num_events: usize,
    num_objects: usize,
    num_event_types: usize,
    num_object_types: usize,
}
