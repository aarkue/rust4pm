//! Implements optimal alignment search
use std::cell::RefCell;

use macros_process_mining::register_binding;
use rayon::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    conformance::alignments::{
        cost::CostFunction,
        petri_net::{AlignmentError, PetriNetAlignmentSpace, PetriNetStep},
        sync_prod_net::SyncProductNet,
    },
    core::{
        event_data::case_centric::utils::activity_projection::EventLogActivityProjection,
        process_models::petri_net::TransitionID,
    },
    utils::dijkstra_search::SearchState,
    PetriNet,
};

pub mod cost;
pub mod petri_net;
pub mod sync_prod_net;

/// A single alignment step
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
/// Alignment Result
pub struct AlignmentResult {
    /// The sequence of alignment moves
    pub moves: Vec<AlignmentMove>,
    /// Total cost of the alignment
    pub cost: u32,
    /// Number of states visited during search
    pub states_visited: usize,
}
/// Alignment result for a single trace variant
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VariantAlignmentResult {
    /// The variant's activity sequence
    pub activities: Vec<String>,
    /// How many traces follow this variant
    pub frequency: u64,
    /// The alignment result or error for this variant
    pub result: Result<AlignmentResult, AlignmentError>,
}

/// Options for computing alignment
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AlignmentOptions {
    /// Cost function for alignment moves
    pub cost_fn: cost::CostFunction,
    /// Maximum number of states to visit before aborting (per trace).
    /// `None` means no limit.
    pub max_states: Option<usize>,
}
impl Default for AlignmentOptions {
    fn default() -> Self {
        Self {
            cost_fn: CostFunction::standard(),
            max_states: Some(100_000),
        }
    }
}
/// Compute alignments for all variants of an event log.
pub fn align_log<'a>(
    net: &PetriNet,
    log: impl Into<&'a EventLogActivityProjection>,
    options: &AlignmentOptions,
) -> Vec<VariantAlignmentResult> {
    let projection: &EventLogActivityProjection = log.into();
    align_variants(net, projection, options)
}

/// Compute alignments for all variants from a pre-computed activity projection.
#[register_binding]
pub fn align_variants(
    net: &PetriNet,
    projection: &EventLogActivityProjection,
    #[bind(default)] options: &AlignmentOptions,
) -> Vec<VariantAlignmentResult> {
    projection
        .traces
        .par_iter()
        .map(|(trace_indices, count)| {
            let x: Vec<_> = trace_indices
                .iter()
                .map(|i| projection.activities[*i].as_str())
                .collect();
            let sp = SyncProductNet::construct(net, &x, &options.cost_fn);

            thread_local! {
                static CTX: RefCell<(PetriNetAlignmentSpace, SearchState<PetriNetStep>)> =
                    RefCell::new((PetriNetAlignmentSpace::default(), SearchState::default()));
            }
            let activities: Vec<String> = trace_indices
                .iter()
                .map(|&idx| projection.activities[idx].clone())
                .collect();
            let result = CTX.with(|ctx| {
                let (space, state) = &mut *ctx.borrow_mut();
                sp.map_err(AlignmentError::SyncProdNetConstructionFailed)
                    .and_then(|sp| petri_net::align(&sp, space, state, options.max_states))
            });
            VariantAlignmentResult {
                activities,
                frequency: *count,
                result,
            }
        })
        .collect()
}

/// Compute alignment for a single trace (given as activity sequence).
pub fn align_trace(
    net: &PetriNet,
    trace: &[&str],
    options: &AlignmentOptions,
) -> Result<AlignmentResult, AlignmentError> {
    let sp = SyncProductNet::construct(net, trace, &options.cost_fn)?;
    petri_net::align(
        &sp,
        &mut PetriNetAlignmentSpace::default(),
        &mut SearchState::default(),
        options.max_states,
    )
}

#[register_binding(stringify_error, name = "align_trace")]
fn align_trace_binding(
    net: &PetriNet,
    trace: &[String],
    #[bind(default)] options: &AlignmentOptions,
) -> Result<AlignmentResult, AlignmentError> {
    let trace_as_str: Vec<_> = trace.iter().map(|s| s.as_str()).collect();
    align_trace(net, &trace_as_str, options)
}

/// Align the empty trace to the given model
/// with the specified options
#[register_binding(stringify_error)]
pub fn align_empty_trace(
    net: &PetriNet,
    #[bind(default)] options: &AlignmentOptions,
) -> Result<AlignmentResult, AlignmentError> {
    let sp = SyncProductNet::construct(net, &[], &options.cost_fn)?;
    petri_net::align(
        &sp,
        &mut PetriNetAlignmentSpace::default(),
        &mut SearchState::default(),
        options.max_states,
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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
#[register_binding(stringify_error)]
pub fn compute_fitness(
    align_res: &[VariantAlignmentResult],
    net: &PetriNet,
    #[bind(default)] options: &AlignmentOptions,
) -> Result<FitnessResult, AlignmentError> {
    let empty = align_empty_trace(net, options)?;
    let model_path_min = empty.cost;
    let mut num_perfectly_fitting = 0;
    let mut total_costs = 0;
    let mut fitness_sum_for_avg = 0f64;
    let mut num_traces = 0;
    let mut num_events = 0;
    for variant in align_res {
        let res = variant.result.as_ref().map_err(|e| e.clone())?;
        let costs = res.cost;
        if costs == 0 {
            num_perfectly_fitting += variant.frequency;
        }
        total_costs += variant.frequency * costs as u64;
        num_traces += variant.frequency;
        num_events += variant.frequency * variant.activities.len() as u64;
        let denom = variant.activities.len() as f64 * options.cost_fn.log_move_cost as f64
            + model_path_min as f64;
        // denom == 0 means an empty trace against a net with initial == final marking: Perfectly fitting
        let fitness = if denom == 0.0 {
            1f64
        } else {
            1f64 - (costs as f64 / denom)
        };
        fitness_sum_for_avg += variant.frequency as f64 * fitness;
    }
    let log_denom = num_events as f64 * options.cost_fn.log_move_cost as f64
        + num_traces as f64 * model_path_min as f64;
    let log_fitness = if log_denom == 0.0 {
        1f64
    } else {
        1f64 - (total_costs as f64 / log_denom)
    };
    Ok(FitnessResult {
        log_fitness,
        average_fitness: if num_traces == 0 {
            // Could be either way..
            0f64
        } else {
            fitness_sum_for_avg / num_traces as f64
        },
        perfectly_fitting_frac: if num_traces == 0 {
            0f64
        } else {
            num_perfectly_fitting as f64 / num_traces as f64
        },
        total_costs,
    })
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, time::Instant};

    use crate::{
        conformance::alignments::{
            align_empty_trace, align_log, compute_fitness,
            cost::CostFunction,
            petri_net::AlignmentError,
            sync_prod_net::{SyncProdNetConstructionError, SyncProductNet},
            AlignmentOptions,
        },
        core::{
            event_data::case_centric::utils::activity_projection::log_to_activity_projection,
            process_models::petri_net::{ArcType, PlaceID},
        },
        test_utils::get_test_data_path,
        utils::dijkstra_search::SearchError,
        EventLog, Importable, PetriNet,
    };

    fn align_helper(
        log_name: &str,
        net_name: &str,
    ) -> (
        Vec<super::VariantAlignmentResult>,
        Result<super::FitnessResult, AlignmentError>,
    ) {
        let test_path = get_test_data_path();
        let log = EventLog::import_from_path(test_path.join("xes").join(log_name)).unwrap();
        let net = PetriNet::import_pnml(test_path.join("petri-net").join(net_name)).unwrap();
        let act_proj = log_to_activity_projection(&log);
        let options = AlignmentOptions::default();
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

    #[test]
    fn no_initial_marking_err() {
        let test_path = get_test_data_path();
        let mut net =
            PetriNet::import_pnml(test_path.join("petri-net").join("sepsis-DISCovered.apnml"))
                .unwrap();
        net.initial_marking = None;
        let sn = SyncProductNet::construct(&net, &[], &CostFunction::standard());
        assert_eq!(sn, Err(SyncProdNetConstructionError::NoInitialMarking));
    }
    #[test]
    fn no_final_markings_err() {
        let test_path = get_test_data_path();
        let mut net =
            PetriNet::import_pnml(test_path.join("petri-net").join("sepsis-DISCovered.apnml"))
                .unwrap();
        net.final_markings = None;
        let sn = SyncProductNet::construct(&net, &[], &CostFunction::standard());
        assert_eq!(sn, Err(SyncProdNetConstructionError::NoFinalMarking));
    }
    #[test]
    fn unknown_place_in_initial_marking_err() {
        let test_path = get_test_data_path();
        let mut net =
            PetriNet::import_pnml(test_path.join("petri-net").join("sepsis-DISCovered.apnml"))
                .unwrap();
        let new_id = PlaceID(uuid::Uuid::new_v4());
        net.initial_marking
            .as_mut()
            .expect("exists in apnml")
            .insert(new_id, 1);
        let sn = SyncProductNet::construct(&net, &[], &CostFunction::standard());
        assert_eq!(
            sn,
            Err(SyncProdNetConstructionError::InvalidPlaceInMarking(new_id))
        );
    }
    #[test]
    fn unknown_place_in_final_marking_err() {
        let test_path = get_test_data_path();
        let mut net =
            PetriNet::import_pnml(test_path.join("petri-net").join("sepsis-DISCovered.apnml"))
                .unwrap();
        let new_id = PlaceID(uuid::Uuid::new_v4());
        net.final_markings
            .as_mut()
            .expect("exists in apnml")
            .first_mut()
            .expect("one final marking exists")
            .insert(new_id, 1);
        let sn = SyncProductNet::construct(&net, &[], &CostFunction::standard());
        assert_eq!(
            sn,
            Err(SyncProdNetConstructionError::InvalidPlaceInMarking(new_id))
        );
    }

    #[test]
    fn final_marking_unreachable_err() {
        let test_path = get_test_data_path();
        let log = EventLog::import_from_path(
            test_path
                .join("xes")
                .join("Sepsis Cases - Event Log.xes.gz"),
        )
        .unwrap();
        let mut net =
            PetriNet::import_pnml(test_path.join("petri-net").join("sepsis-DISCovered.apnml"))
                .unwrap();
        let places_in_final_marking: HashSet<_> = net
            .final_markings
            .as_mut()
            .expect("exists in file")
            .first_mut()
            .expect("not empty")
            .keys()
            .map(|id| id.0)
            .collect();
        net.arcs.retain(|arc| match arc.from_to {
            ArcType::PlaceTransition(_, _) => true,
            ArcType::TransitionPlace(_, place) => !places_in_final_marking.contains(&place),
        });
        let act_proj = log_to_activity_projection(&log);
        let options = AlignmentOptions {
            cost_fn: CostFunction::standard(),
            max_states: None,
        };
        let empty_trace_align = align_empty_trace(&net, &options);
        assert_eq!(
            empty_trace_align,
            Err(AlignmentError::SearchError(SearchError::Unreachable))
        );
        let result = align_log(&net, &act_proj, &options);
        for variant in result {
            assert_eq!(
                variant.result,
                Err(AlignmentError::SearchError(SearchError::Unreachable))
            );
        }
    }
    #[test]
    fn max_states_reached_err() {
        let test_path = get_test_data_path();
        let log = EventLog::import_from_path(
            test_path
                .join("xes")
                .join("Sepsis Cases - Event Log.xes.gz"),
        )
        .unwrap();
        let net =
            PetriNet::import_pnml(test_path.join("petri-net").join("sepsis-DISCovered.apnml"))
                .unwrap();
        let act_proj = log_to_activity_projection(&log);
        let options = AlignmentOptions {
            cost_fn: CostFunction::standard(),
            max_states: Some(10),
        };
        let empty_trace_align = align_empty_trace(&net, &options);
        assert_eq!(
            empty_trace_align,
            Err(AlignmentError::SearchError(SearchError::LimitReached))
        );
        let result = align_log(&net, &act_proj, &options);
        for variant in result {
            assert_eq!(
                variant.result,
                Err(AlignmentError::SearchError(SearchError::LimitReached))
            );
        }
    }
    #[test]
    fn max_states_reached_not_easy_sound_err() {
        let test_path = get_test_data_path();
        let log = EventLog::import_from_path(
            test_path
                .join("xes")
                .join("Sepsis Cases - Event Log.xes.gz"),
        )
        .unwrap();
        let net =
            PetriNet::import_pnml(test_path.join("petri-net").join("sepsis-fodina.apnml")).unwrap();
        let act_proj = log_to_activity_projection(&log);
        let options = AlignmentOptions::default();
        let empty_trace_align = align_empty_trace(&net, &options);
        assert_eq!(
            empty_trace_align,
            Err(AlignmentError::SearchError(SearchError::LimitReached))
        );
        let result = align_log(&net, &act_proj, &options);
        for variant in result {
            assert_eq!(
                variant.result,
                Err(AlignmentError::SearchError(SearchError::LimitReached))
            );
        }
    }
}
