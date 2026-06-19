//! Implements optimal alignment search
use std::cell::RefCell;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    conformance::alignments::{dijkstra::AlignmentError, sync_prod_net::SyncProductNet},
    core::{
        event_data::case_centric::utils::activity_projection::EventLogActivityProjection,
        process_models::petri_net::TransitionID,
    },
    PetriNet,
};

pub mod cost;
pub mod dijkstra;
pub mod sync_prod_net;

/// A single alignment step
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlignmentMove {
    /// Synchronous move (model and log agree)
    SyncMove {
        /// The transition that was fired
        transition: TransitionID,
        /// Index of the event in the trace
        trace_event_index: usize,
    },
    /// Model move (only the model moves,)
    ModelMove {
        /// The transition that was fired
        transition: TransitionID,
    },
    /// Log move (only the log moves)
    LogMove {
        /// Index of the event in the trace
        trace_event_index: usize,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
/// Alignment Result
pub struct AlignmentResult {
    /// The sequence of alignment moves
    pub moves: Vec<AlignmentMove>,
    /// Total cost of the alignment
    pub cost: u32,
    /// Number of states visited during search
    pub states_visited: usize,
}
/// Alignment result of a complete log (i.e., for all variants)
#[derive(Debug, Clone)]
pub struct LogAlignmentResult {
    /// Alignment results per trace variant (`variant_activities`, frequency, alignment result)
    pub variant_results: Vec<(Vec<String>, u64, Result<AlignmentResult, AlignmentError>)>,
}

/// Options for computing alignment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlignmentOptions {
    /// Cost function for alignment moves
    pub cost_fn: cost::CostFunction,
    /// Maximum number of states to visit before aborting (per trace).
    /// `None` means no limit.
    pub max_states: Option<usize>,
}
/// Compute alignments for all variants of an event log.
pub fn align_log<'a>(
    net: &PetriNet,
    log: impl Into<&'a EventLogActivityProjection>,
    options: &AlignmentOptions,
) -> LogAlignmentResult {
    let projection: &EventLogActivityProjection = log.into();
    align_projection(net, projection, options)
}

/// Compute alignments for all variants from a pre-computed activity projection.
pub fn align_projection(
    net: &PetriNet,
    projection: &EventLogActivityProjection,
    options: &AlignmentOptions,
) -> LogAlignmentResult {
    let variant_results: Vec<_> = projection
        .traces
        .par_iter()
        .map(|(trace_indices, count)| {
            let x: Vec<_> = trace_indices
                .iter()
                .map(|i| projection.activities[*i].as_str())
                .collect();
            let sp = SyncProductNet::construct(net, &x, &options.cost_fn);

            thread_local! {
                static CTX: RefCell<dijkstra::DijkstraContext> =
                    RefCell::new(dijkstra::DijkstraContext::default());
            }
            let activities: Vec<String> = trace_indices
                .iter()
                .map(|&idx| projection.activities[idx].clone())
                .collect();
            let result = CTX.with(|ctx| {
                sp.map_err(AlignmentError::SyncProdNetConstructionFailed)
                    .and_then(|sp| dijkstra::search(&sp, &mut ctx.borrow_mut(), options.max_states))
            });
            (activities, *count, result)
        })
        .collect();
    LogAlignmentResult { variant_results }
}

/// Compute alignment for a single trace (given as activity sequence).
pub fn align_trace(
    net: &PetriNet,
    trace: &[&str],
    options: &AlignmentOptions,
) -> Result<AlignmentResult, AlignmentError> {
    let sp = SyncProductNet::construct(net, trace, &options.cost_fn)?;
    dijkstra::search(
        &sp,
        &mut dijkstra::DijkstraContext::default(),
        options.max_states,
    )
}

/// Align the empty trace to the given model
/// with the specified options
pub fn align_empty_trace(
    net: &PetriNet,
    options: &AlignmentOptions,
) -> Result<AlignmentResult, AlignmentError> {
    let sp = SyncProductNet::construct(net, &[], &options.cost_fn)?;
    dijkstra::search(
        &sp,
        &mut dijkstra::DijkstraContext::default(),
        options.max_states,
    )
}

#[derive(Debug, Clone)]
/// Alignment Fitness Result
pub struct FitnessResult {
    /// Log fitness, as the total computed fitness (summing up the costs for all traces)
    pub log_fitness: f64,
    /// Average trace fitness (across all traces)
    pub average_fitness: f64,
    /// Fraction of traces that perfectly fit (i.e., have an alignment cost of `0`)
    pub perfectly_fitting_frac: f64,
    /// The total cost, summed up from all traces
    pub total_costs: u64,
}

/// Compute fitness stats from alignment results
///
/// Also constructs the empty-trace alignment (shortest path through model)
pub fn compute_fitness(
    align_res: &LogAlignmentResult,
    net: &PetriNet,
    options: &AlignmentOptions,
) -> Result<FitnessResult, AlignmentError> {
    let empty = align_empty_trace(net, options)?;
    let model_path_min = empty.cost;
    let mut num_perfectly_fitting = 0;
    let mut total_costs = 0;
    let mut fitness_sum_for_avg = 0f64;
    let mut num_traces = 0;
    let mut num_events = 0;
    for (variant, freq, res) in &align_res.variant_results {
        let res = res.as_ref().map_err(|e| e.clone())?;
        let costs = res.cost;
        if costs == 0 {
            num_perfectly_fitting += freq;
        }
        total_costs += freq * costs as u64;
        num_traces += freq;
        num_events += freq * variant.len() as u64;
        let fitness = 1f64
            - (costs as f64
                / (variant.len() as f64 * options.cost_fn.log_move_cost as f64
                    + model_path_min as f64));
        fitness_sum_for_avg += *freq as f64 * fitness;
    }
    let log_fitness = 1f64
        - (total_costs as f64
            / (num_events as f64 * options.cost_fn.log_move_cost as f64
                + num_traces as f64 * model_path_min as f64));
    Ok(FitnessResult {
        log_fitness,
        average_fitness: fitness_sum_for_avg / num_traces as f64,
        perfectly_fitting_frac: num_perfectly_fitting as f64 / num_traces as f64,
        total_costs,
    })
}

#[cfg(test)]
mod test {
    use std::time::Instant;

    use crate::{
        conformance::alignments::{
            align_log, compute_fitness, cost::CostFunction, dijkstra::AlignmentError,
            AlignmentOptions,
        },
        core::event_data::case_centric::utils::activity_projection::log_to_activity_projection,
        test_utils::get_test_data_path,
        EventLog, Importable, PetriNet,
    };

    fn align_helper(
        log_name: &str,
        net_name: &str,
    ) -> (
        super::LogAlignmentResult,
        Result<super::FitnessResult, AlignmentError>,
    ) {
        let test_path = get_test_data_path();
        let log = EventLog::import_from_path(test_path.join("xes").join(log_name)).unwrap();
        let net = PetriNet::import_pnml(test_path.join("petri-net").join(net_name)).unwrap();
        let act_proj = log_to_activity_projection(&log);
        let options = AlignmentOptions {
            cost_fn: CostFunction {
                model_move_cost: 1,
                log_move_cost: 1,
                sync_move_cost: 0,
                silent_move_cost: 0,
            },
            max_states: None,
        };
        let now = Instant::now();
        let result = align_log(&net, &act_proj, &options);
        println!("Aligning traces took {:?}", now.elapsed());
        let fitness = compute_fitness(&result, &net, &options);
        println!("{fitness:?}");
        (result, fitness)
    }

    #[test]
    fn sepsis_total_cost() {
        let (_alignment, fitness) =
            align_helper("Sepsis Cases - Event Log.xes.gz", "sepsis-DISCovered.apnml");
        let fitness = fitness.unwrap();
        // Ground truth total alignment cost was computed and additionally verified with external source (PM4Py)
        assert_eq!(fitness.total_costs, 4118);
    }

    #[test]
    fn rtfm_total_cost() {
        let (_alignment, fitness) = align_helper(
            "Road_Traffic_Fine_Management_Process.xes.gz",
            "rtfm-imf-02.apnml",
        );
        let fitness = fitness.unwrap();
        // Ground truth total alignment cost was computed and additionally verified with external source (PM4Py)
        assert_eq!(fitness.total_costs, 17650);
    }
}
