use std::{collections::HashMap, env, fs::File, hint::black_box, path::PathBuf, time::Instant};

use process_mining::{
    core::{
        event_data::object_centric::{
            linked_ocel::SlimLinkedOCEL, ocel_json::import_ocel_json_from_path,
            ocel_xml::import_ocel_xml_file,
        },
        process_models::oc_declare::{
            get_activity_object_involvements, get_object_to_object_involvements,
            get_rev_object_to_object_involvements, OCDeclareArcType, ObjectInvolvementCounts,
        },
    },
    discovery::object_centric::oc_declare::{
        discover_behavior_constraints, reduce_oc_arcs, refine_oc_arcs, O2OMode,
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
            let num_runs = 1;
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

                let locel = SlimLinkedOCEL::from_ocel(ocel);
                for o2o_mode in [O2OMode::None] {
                    //, O2OMode::Direct] {
                    println!("{:?}", o2o_mode);
                    for (max_gen_count, max_filter_count) in [
                        ((Some(1), None), (Some(1), None)),
                        ((Some(1), None), (Some(1), Some(20))),
                        ((Some(1), Some(20)), (Some(1), Some(20))),
                    ] {
                        println!(
                            "Gen max: {:?} Filter max: {:?}",
                            max_gen_count.1, max_filter_count.1
                        );
                        let mut eval_res = EvaluationResult {
                            num_events: num_evs,
                            num_objects: num_obs,
                            num_event_types: num_et,
                            num_object_types: num_ot,
                            ..Default::default()
                        };
                        let mut res = Vec::new();
                        let mut reduced = Vec::new();
                        let mut refined = Vec::new();
                        for i in 0..num_runs {
                            let options = OCDeclareDiscoveryOptions {
                                noise_threshold: noise_thresh,
                                o2o_mode,
                                reduction: OCDeclareReductionMode::None,
                                counts_for_generation: max_gen_count,
                                counts_for_filter: max_filter_count,
                                refinement: false,
                                considered_arrow_types: vec![
                                    OCDeclareArcType::AS,
                                    OCDeclareArcType::EF,
                                    OCDeclareArcType::EP,
                                ]
                                .into_iter()
                                .collect(),
                                ..Default::default()
                            };
                            let total_start = Instant::now();
                            let now = Instant::now();
                            res = black_box(discover_behavior_constraints(&locel, options.clone()));
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
                            reduced = black_box(reduce_oc_arcs(res.clone(), true));
                            let duration = now.elapsed();
                            eval_res.number_of_results_after_reduction = reduced.len();
                            eval_res
                                .reduction_duration_seconds
                                .push(duration.as_secs_f64());
                            if i == 0 {
                                eval_res.number_of_results_after_reduction = reduced.len();
                            } else {
                                assert_eq!(
                                    eval_res.number_of_results_after_reduction,
                                    reduced.len()
                                );
                            }
                            // Refinement
                            let act_ob_inv: HashMap<
                                String,
                                HashMap<String, ObjectInvolvementCounts>,
                            > = get_activity_object_involvements(&locel);
                            let ob_ob_inv: HashMap<
                                String,
                                HashMap<String, ObjectInvolvementCounts>,
                            > = get_object_to_object_involvements(&locel);

                            let ob_ob_rev_inv = get_rev_object_to_object_involvements(&locel);
                            let now = Instant::now();
                            refined = black_box(refine_oc_arcs(
                                &reduced,
                                &act_ob_inv,
                                &ob_ob_inv,
                                &ob_ob_rev_inv,
                                &options,
                                &locel,
                            ));
                            let duration = now.elapsed();
                            eval_res.number_of_results_after_refinement = refined.len();
                            eval_res
                                .refinement_duration_seconds
                                .push(duration.as_secs_f64());
                            if i == 0 {
                                eval_res.number_of_results_after_refinement = refined.len();
                            } else {
                                assert_eq!(
                                    eval_res.number_of_results_after_refinement,
                                    refined.len()
                                );
                            }
                            println!(
                                "Got {} (reduced to {}, refined to {}) results in {:?} / total {:?}",
                                res.len(),
                                reduced.len(),
                                refined.len(),
                                discovery_duration,
                                total_start.elapsed()
                            );
                        }
                        eval_res.mean_discovery_duration =
                            eval_res.discovery_durations_seconds.iter().sum::<f64>()
                                / eval_res.discovery_durations_seconds.len() as f64;

                        eval_res.mean_reduction_duration =
                            eval_res.reduction_duration_seconds.iter().sum::<f64>()
                                / eval_res.reduction_duration_seconds.len() as f64;

                        eval_res.mean_refinement_duration =
                            eval_res.refinement_duration_seconds.iter().sum::<f64>()
                                / eval_res.refinement_duration_seconds.len() as f64;

                        eval_res.mean_lossy_reduction_duration = eval_res
                            .lossy_reduction_duration_seconds
                            .iter()
                            .sum::<f64>()
                            / eval_res.lossy_reduction_duration_seconds.len() as f64;

                        let summary_file = File::create(format!(
                            "{}-{:?}-GEN{:?}-FILTER{:?}-summary.json",
                            name, o2o_mode, max_gen_count.1, max_filter_count.1
                        ))
                        .unwrap();
                        serde_json::to_writer_pretty(summary_file, &eval_res).unwrap();

                        let results_file = File::create(format!(
                            "{}-{:?}-GEN{:?}-FILTER{:?}-discovered.json",
                            name, o2o_mode, max_gen_count.1, max_filter_count.1
                        ))
                        .unwrap();
                        serde_json::to_writer_pretty(results_file, &res).unwrap();
                        let reduced_file = File::create(format!(
                            "{}-{:?}-GEN{:?}-FILTER{:?}-reduced-lossless.json",
                            name, o2o_mode, max_gen_count.1, max_filter_count.1
                        ))
                        .unwrap();
                        serde_json::to_writer_pretty(reduced_file, &reduced).unwrap();
                        let refined_file = File::create(format!(
                            "{}-{:?}-GEN{:?}-FILTER{:?}-refined.json",
                            name, o2o_mode, max_gen_count.1, max_filter_count.1
                        ))
                        .unwrap();
                        serde_json::to_writer_pretty(refined_file, &refined).unwrap();
                    }
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
    refinement_duration_seconds: Vec<f64>,
    mean_refinement_duration: f64,
    number_of_results_after_refinement: usize,
    num_events: usize,
    num_objects: usize,
    num_event_types: usize,
    num_object_types: usize,
}
