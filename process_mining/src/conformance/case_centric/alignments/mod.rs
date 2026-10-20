//! Implements optimal alignment search
use std::cell::RefCell;

use macros_process_mining::register_binding;
use rayon::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    conformance::alignments::{
        cost::CostFunction,
        petri_net::{AlignmentContext, AlignmentError},
        sync_prod_net::{ModelNet, SyncProductNet},
    },
    core::{
        event_data::case_centric::utils::activity_projection::EventLogActivityProjection,
        process_models::petri_net::TransitionID,
    },
    utils::dijkstra_search::{SearchError, SearchLimits},
    PetriNet,
};

pub mod cost;
pub mod petri_net;
pub(crate) mod reachability;
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
    /// Number of states visited during search, summed over both search directions
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
    /// Maximum number of states to visit before aborting (per trace, both search directions
    /// together). `None` means no limit.
    ///
    /// This bounds runtime. See [`AlignmentOptions::max_memory_mib`] for memory.
    pub max_states: Option<usize>,
    /// Memory in MiB that all alignment searches of one call may hold together (default 6 GiB).
    /// `None` means no limit.
    ///
    /// Traces are aligned in parallel, so each search gets an even share: the budget divided by
    /// `rayon::current_num_threads()`, or by the number of variants if there are fewer. A single
    /// search (e.g., [`align_trace`]) gets all of it. A trace that needs more than its share fails with
    /// [`SearchError::MemoryLimitReached`], while the other traces are aligned as usual.
    ///
    /// This bounds the buffers of the searches. The event log, the model and the alignments come
    /// on top, as does memory the allocator keeps, so leave some headroom.
    ///
    /// [`align_variants_with_retry`] aligns the traces that ran out again, one at a time with
    /// the whole budget. By hand:
    ///
    /// ```no_run
    /// # use process_mining::{conformance::alignments::{*, petri_net::AlignmentError}, utils::dijkstra_search::SearchError, PetriNet};
    /// # use process_mining::core::event_data::case_centric::utils::activity_projection::EventLogActivityProjection;
    /// # fn retry(net: &PetriNet, log: &EventLogActivityProjection, options: &AlignmentOptions) {
    /// let mut results = align_variants(net, log, options);
    /// for variant in &mut results {
    ///     if variant.result == Err(AlignmentError::SearchError(SearchError::MemoryLimitReached)) {
    ///         let trace: Vec<&str> = variant.activities.iter().map(String::as_str).collect();
    ///         variant.result = align_trace(net, &trace, options);
    ///     }
    /// }
    /// # }
    /// ```
    pub max_memory_mib: Option<usize>,
}

impl Default for AlignmentOptions {
    fn default() -> Self {
        Self {
            cost_fn: CostFunction::standard(),
            max_states: None,
            max_memory_mib: Some(6 * 1024),
        }
    }
}

impl AlignmentOptions {
    /// The limits of each of `searches` searches running at once
    fn limits_for(&self, searches: usize) -> SearchLimits {
        SearchLimits {
            max_states: self.max_states,
            max_memory: self
                .max_memory_mib
                .map(|mib| mib.saturating_mul(1 << 20) / searches.max(1)),
        }
    }
}

thread_local! {
    /// Search buffers a thread reuses across the traces of one [`align_variants`] call
    static CONTEXT: RefCell<AlignmentContext> = RefCell::new(AlignmentContext::default());
}

/// Compute alignments for all variants of an event log.
///
/// Permits at most 255 ([`petri_net::TokenCount::MAX`]) tokens in each place.
pub fn align_log<'a>(
    net: &PetriNet,
    log: impl Into<&'a EventLogActivityProjection>,
    options: &AlignmentOptions,
) -> Vec<VariantAlignmentResult> {
    let projection: &EventLogActivityProjection = log.into();
    align_variants(net, projection, options)
}

/// Compute alignments for all variants from a pre-computed activity projection.
///
/// Permits at most 255 ([`petri_net::TokenCount::MAX`]) tokens in each place.
#[register_binding]
pub fn align_variants(
    net: &PetriNet,
    projection: &EventLogActivityProjection,
    #[bind(default)] options: &AlignmentOptions,
) -> Vec<VariantAlignmentResult> {
    // Both the model half and its reachability are properties of the net, shared by every trace
    align_variants_on(&build_model(net, &options.cost_fn), projection, options)
}

/// Like [`align_variants`], then aligns every variant that ran out of memory again, one at a
/// time on the calling thread, with the whole [`AlignmentOptions::max_memory_mib`].
///
/// A retried search may take much longer than its first attempt, and may still run out.
#[register_binding]
pub fn align_variants_with_retry(
    net: &PetriNet,
    projection: &EventLogActivityProjection,
    #[bind(default)] options: &AlignmentOptions,
) -> Vec<VariantAlignmentResult> {
    let model = build_model(net, &options.cost_fn);
    let mut results = align_variants_on(&model, projection, options);
    // Without a model every variant already carries its error
    let Ok(model) = model else {
        return results;
    };
    let limits = options.limits_for(1);
    let mut ctx = AlignmentContext::default();
    for variant in &mut results {
        if matches!(
            variant.result,
            Err(AlignmentError::SearchError(SearchError::MemoryLimitReached))
        ) {
            let trace: Vec<&str> = variant.activities.iter().map(String::as_str).collect();
            variant.result = align_on(&model, &trace, &options.cost_fn, &mut ctx, limits);
        }
    }
    results
}

fn align_variants_on(
    model: &Result<ModelNet, AlignmentError>,
    projection: &EventLogActivityProjection,
    options: &AlignmentOptions,
) -> Vec<VariantAlignmentResult> {
    let searches = rayon::current_num_threads().min(projection.traces.len());
    let limits = options.limits_for(searches);
    let results = projection
        .traces
        .par_iter()
        .map(|(trace_indices, count)| {
            let activities: Vec<String> = trace_indices
                .iter()
                .map(|&idx| projection.activities[idx].clone())
                .collect();
            let trace: Vec<&str> = activities.iter().map(String::as_str).collect();
            let result = match model {
                Err(e) => Err(e.clone()),
                Ok(model) => CONTEXT.with(|ctx| {
                    align_on(
                        model,
                        &trace,
                        &options.cost_fn,
                        &mut ctx.borrow_mut(),
                        limits,
                    )
                }),
            };
            VariantAlignmentResult {
                activities,
                frequency: *count,
                result,
            }
        })
        .collect();
    // Kept buffers would otherwise stay allocated after the call, next to a later search that
    // may use the whole budget
    rayon::broadcast(|_| CONTEXT.take());
    results
}

fn align_on(
    model: &ModelNet,
    trace: &[&str],
    cost_fn: &CostFunction,
    ctx: &mut AlignmentContext,
    limits: SearchLimits,
) -> Result<AlignmentResult, AlignmentError> {
    let sp = SyncProductNet::construct(model, trace, cost_fn)?;
    petri_net::align(&sp, ctx, limits)
}

/// Build the trace-independent half, rejecting nets that cannot reach their final marking
fn build_model(net: &PetriNet, cost_fn: &CostFunction) -> Result<ModelNet, AlignmentError> {
    let model = ModelNet::build(net, cost_fn)?;
    if !reachability::final_marking_reachable(&model) {
        return Err(SearchError::Unreachable.into());
    }
    Ok(model)
}

/// Compute alignment for a single trace (given as activity sequence).
///
/// Permits at most 255 ([`petri_net::TokenCount::MAX`]) tokens in each place.
pub fn align_trace(
    net: &PetriNet,
    trace: &[&str],
    options: &AlignmentOptions,
) -> Result<AlignmentResult, AlignmentError> {
    let model = build_model(net, &options.cost_fn)?;
    let mut ctx = AlignmentContext::default();
    align_on(
        &model,
        trace,
        &options.cost_fn,
        &mut ctx,
        options.limits_for(1),
    )
}

/// Compute alignment for a single trace (given as activity sequence).
///
/// Permits at most 255 ([`petri_net::TokenCount::MAX`]) tokens in each place.
#[allow(dead_code)]
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
///
/// Permits at most 255 ([`petri_net::TokenCount::MAX`]) tokens in each place.
#[register_binding(stringify_error)]
pub fn align_empty_trace(
    net: &PetriNet,
    #[bind(default)] options: &AlignmentOptions,
) -> Result<AlignmentResult, AlignmentError> {
    align_trace(net, &[], options)
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
    use std::{
        collections::{HashMap, HashSet},
        time::Instant,
    };

    use uuid::Uuid;

    use crate::{
        conformance::alignments::{
            align_empty_trace, align_log, align_trace, align_variants, align_variants_with_retry,
            compute_fitness,
            cost::CostFunction,
            petri_net::{self, AlignmentContext, AlignmentError, TracePos},
            sync_prod_net::{ModelNet, SyncProdNetConstructionError, SyncProductNet},
            AlignmentMove, AlignmentOptions,
        },
        core::{
            event_data::case_centric::utils::activity_projection::{
                log_to_activity_projection, EventLogActivityProjection,
            },
            process_models::petri_net::{Arc, ArcType, PlaceID, TransitionID},
        },
        test_utils::get_test_data_path,
        utils::dijkstra_search::{SearchError, SearchLimits},
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

    fn sepsis_net() -> PetriNet {
        let path = get_test_data_path()
            .join("petri-net")
            .join("sepsis-DISCovered.apnml");
        PetriNet::import_pnml(path).unwrap()
    }

    fn sepsis() -> (PetriNet, EventLogActivityProjection) {
        let path = get_test_data_path()
            .join("xes")
            .join("Sepsis Cases - Event Log.xes.gz");
        let log = EventLog::import_from_path(path).unwrap();
        (sepsis_net(), log_to_activity_projection(&log))
    }

    fn variants(projection: &EventLogActivityProjection) -> Vec<Vec<&str>> {
        projection
            .traces
            .iter()
            .map(|(trace, _)| {
                trace
                    .iter()
                    .map(|&a| projection.activities[a].as_str())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn fewer_variants_than_threads_share_the_whole_budget() {
        let (net, mut projection) = sepsis();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(32)
            .build()
            .unwrap();
        let options = AlignmentOptions {
            max_memory_mib: Some(1),
            ..AlignmentOptions::default()
        };
        let out_of_memory = Err(AlignmentError::SearchError(SearchError::MemoryLimitReached));
        let all = pool.install(|| align_variants(&net, &projection, &options));
        let ran_out = all.iter().position(|v| v.result == out_of_memory).unwrap();
        projection.traces = vec![projection.traces[ran_out].clone()];
        let alone = pool.install(|| align_variants(&net, &projection, &options));
        assert!(alone[0].result.is_ok());
    }

    #[test]
    fn retry_aligns_what_the_share_could_not() {
        let (net, projection) = sepsis();
        // 1 MiB over 32 threads leaves each search 32 KiB, too little for the largest variants
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(32)
            .build()
            .unwrap();
        let options = AlignmentOptions {
            max_memory_mib: Some(1),
            ..AlignmentOptions::default()
        };
        let out_of_memory = Err(AlignmentError::SearchError(SearchError::MemoryLimitReached));
        let parallel = pool.install(|| align_variants(&net, &projection, &options));
        assert!(parallel.iter().any(|v| v.result == out_of_memory));

        let retried = pool.install(|| align_variants_with_retry(&net, &projection, &options));
        let unlimited = align_variants(
            &net,
            &projection,
            &AlignmentOptions {
                max_memory_mib: None,
                ..AlignmentOptions::default()
            },
        );
        let costs = |results: &[super::VariantAlignmentResult]| {
            let mut costs: Vec<_> = results
                .iter()
                .map(|v| (v.activities.clone(), v.result.as_ref().unwrap().cost))
                .collect();
            costs.sort();
            costs
        };
        assert_eq!(costs(&retried), costs(&unlimited));
    }

    #[test]
    fn kept_buffers_never_cause_a_failure() {
        let (net, projection) = sepsis();
        let cost_fn = CostFunction::standard();
        let model = ModelNet::build(&net, &cost_fn).unwrap();
        for max_memory in [32 << 10, 64 << 10, 128 << 10] {
            let limits = SearchLimits {
                max_states: None,
                max_memory: Some(max_memory),
            };
            let mut kept = AlignmentContext::default();
            for trace in variants(&projection) {
                let sp = SyncProductNet::construct(&model, &trace, &cost_fn).unwrap();
                let fresh = petri_net::align(&sp, &mut AlignmentContext::default(), limits);
                let with_kept = petri_net::align(&sp, &mut kept, limits);
                // The other way round may differ: kept buffers that are large enough need no
                // growth, which briefly holds the old and the new buffer
                if fresh.is_ok() {
                    assert_eq!(with_kept, fresh, "{max_memory} bytes, {trace:?}");
                }
            }
        }
    }

    #[test]
    fn tiny_budget_fails_cleanly() {
        let model = ModelNet::build(&sepsis_net(), &CostFunction::standard()).unwrap();
        let sp = SyncProductNet::construct(&model, &[], &CostFunction::standard()).unwrap();
        let limits = SearchLimits {
            max_states: None,
            max_memory: Some(64),
        };
        assert_eq!(
            petri_net::align(&sp, &mut AlignmentContext::default(), limits),
            Err(AlignmentError::SearchError(SearchError::MemoryLimitReached))
        );
    }

    #[test]
    fn memory_limit_stops_large_searches_only() {
        let (net, projection) = sepsis();
        let cost_fn = CostFunction::standard();
        let model = ModelNet::build(&net, &cost_fn).unwrap();
        let align_with = |trace: &[&str], max_memory| {
            let sp = SyncProductNet::construct(&model, trace, &cost_fn).unwrap();
            let limits = SearchLimits {
                max_states: None,
                max_memory,
            };
            petri_net::align(&sp, &mut AlignmentContext::default(), limits)
        };
        let mut stopped = 0;
        for trace in variants(&projection) {
            let unlimited = align_with(&trace, None).unwrap();
            assert_eq!(
                align_with(&trace, Some(1 << 30)).unwrap().cost,
                unlimited.cost
            );
            match align_with(&trace, Some(64 * 1024)) {
                Ok(alignment) => assert_eq!(alignment.cost, unlimited.cost),
                Err(AlignmentError::SearchError(SearchError::MemoryLimitReached)) => stopped += 1,
                Err(e) => panic!("{e:?}"),
            }
        }
        assert!(stopped > 0, "64 KiB should stop the largest searches");
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
        let sn = ModelNet::build(&net, &CostFunction::standard());
        assert_eq!(sn, Err(SyncProdNetConstructionError::NoInitialMarking));
    }
    #[test]
    fn no_final_markings_err() {
        let test_path = get_test_data_path();
        let mut net =
            PetriNet::import_pnml(test_path.join("petri-net").join("sepsis-DISCovered.apnml"))
                .unwrap();
        net.final_markings = None;
        let sn = ModelNet::build(&net, &CostFunction::standard());
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
        let sn = ModelNet::build(&net, &CostFunction::standard());
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
        let sn = ModelNet::build(&net, &CostFunction::standard());
        assert_eq!(
            sn,
            Err(SyncProdNetConstructionError::InvalidPlaceInMarking(new_id))
        );
    }

    #[test]
    fn parallel_arcs_weights_add_up() {
        // Two arcs p0 -> t, so firing `t` needs two tokens, not one
        let mut net = PetriNet::new();
        let p0 = net.add_place(None);
        let p1 = net.add_place(None);
        let t = net.add_transition(Some("a".to_string()), None);
        net.add_arc(ArcType::PlaceTransition(p0.0, t.0), None);
        net.add_arc(ArcType::PlaceTransition(p0.0, t.0), None);
        net.add_arc(ArcType::TransitionPlace(t.0, p1.0), None);
        net.final_markings = Some(vec![[(p1, 1)].into_iter().collect()]);
        let options = AlignmentOptions::default();

        net.initial_marking = Some([(p0, 1)].into_iter().collect());
        assert_eq!(
            align_trace(&net, &["a"], &options),
            Err(AlignmentError::SearchError(SearchError::Unreachable))
        );
        net.initial_marking = Some([(p0, 2)].into_iter().collect());
        assert_eq!(align_trace(&net, &["a"], &options).unwrap().cost, 0);

        // The summed weight must still fit a TokenCount
        net.arcs.push(Arc {
            from_to: ArcType::PlaceTransition(p0.0, t.0),
            weight: 300,
        });
        assert_eq!(
            ModelNet::build(&net, &CostFunction::standard()),
            Err(SyncProdNetConstructionError::ArcWeightTooLarge(302))
        );
    }

    #[test]
    fn oversized_marking_err() {
        let test_path = get_test_data_path();
        let mut net =
            PetriNet::import_pnml(test_path.join("petri-net").join("sepsis-DISCovered.apnml"))
                .unwrap();
        let place = *net.initial_marking.as_ref().unwrap().keys().next().unwrap();
        net.initial_marking.as_mut().unwrap().insert(place, 400);
        assert_eq!(
            ModelNet::build(&net, &CostFunction::standard()),
            Err(SyncProdNetConstructionError::MarkingTooLarge(place, 400))
        );
    }

    /// The state equation is solvable, but nothing can ever fire, so the search must decide
    #[test]
    fn spurious_state_equation_solution() {
        let mut net = PetriNet::new();
        let p: Vec<_> = (0..4).map(|_| net.add_place(None)).collect();
        // t1: p1 + p3 -> p2   (needs a token in p3, which only t2 produces)
        let t1 = net.add_transition(Some("a".to_string()), None);
        net.add_arc(ArcType::PlaceTransition(p[0].0, t1.0), None);
        net.add_arc(ArcType::PlaceTransition(p[2].0, t1.0), None);
        net.add_arc(ArcType::TransitionPlace(t1.0, p[1].0), None);
        // t2: p2 -> p3 + p4
        let t2 = net.add_transition(Some("b".to_string()), None);
        net.add_arc(ArcType::PlaceTransition(p[1].0, t2.0), None);
        net.add_arc(ArcType::TransitionPlace(t2.0, p[2].0), None);
        net.add_arc(ArcType::TransitionPlace(t2.0, p[3].0), None);
        net.initial_marking = Some([(p[0], 1)].into_iter().collect());
        net.final_markings = Some(vec![[(p[3], 1)].into_iter().collect()]);

        let model = ModelNet::build(&net, &CostFunction::standard()).unwrap();
        // x = (1,1) solves initial + C x = final, so the cheap check cannot rule it out
        assert!(
            super::reachability::final_marking_reachable(&model),
            "expected the state equation to admit a spurious solution"
        );
        // ... but nothing is ever enabled, so the search must still report Unreachable
        let res = align_trace(&net, &["a"], &AlignmentOptions::default());
        assert_eq!(
            res,
            Err(AlignmentError::SearchError(SearchError::Unreachable))
        );
    }

    /// A transition consuming a place past the 64th is not covered by the place word, so it must
    /// still be checked against the marking rather than taken as enabled.
    #[test]
    fn place_past_the_mask_word_still_blocks() {
        let mut net = PetriNet::new();
        // Places are indexed in id order, so `places[100]` lies past the 64-place word
        let places: Vec<_> = (0..200)
            .map(|i| net.add_place(Some(Uuid::from_u128(i))))
            .collect();
        // "a" carries the token from start to end
        let a = net.add_transition(Some("a".to_string()), None);
        net.add_arc(ArcType::PlaceTransition(places[0].0, a.0), None);
        net.add_arc(ArcType::TransitionPlace(a.0, places[1].0), None);
        // "b" needs a token in a place nothing ever marks, so it can never fire
        let b = net.add_transition(Some("b".to_string()), None);
        net.add_arc(ArcType::PlaceTransition(places[100].0, b.0), None);
        net.add_arc(ArcType::TransitionPlace(b.0, places[1].0), None);
        net.initial_marking = Some([(places[0], 1)].into_iter().collect());
        net.final_markings = Some(vec![[(places[1], 1)].into_iter().collect()]);

        let options = AlignmentOptions::default();
        assert_eq!(align_trace(&net, &["a"], &options).unwrap().cost, 0);
        assert_eq!(
            align_trace(&net, &["b"], &options).unwrap().cost,
            2,
            "\"b\" cannot fire, so only a log move plus a model move aligns"
        );
    }

    /// A zero-weight arc moves no token, so it must neither block its transition nor fire
    #[test]
    fn zero_weight_arcs_are_ignored() {
        let mut net = PetriNet::new();
        let places: Vec<_> = (0..3).map(|_| net.add_place(None)).collect();
        let a = net.add_transition(Some("a".to_string()), None);
        net.add_arc(ArcType::PlaceTransition(places[0].0, a.0), None);
        // Takes nothing from a place that never holds a token, so it must not stop "a" firing
        net.add_arc(ArcType::PlaceTransition(places[2].0, a.0), Some(0));
        net.add_arc(ArcType::TransitionPlace(a.0, places[1].0), None);
        net.initial_marking = Some([(places[0], 1)].into_iter().collect());
        net.final_markings = Some(vec![[(places[1], 1)].into_iter().collect()]);
        assert_eq!(
            align_trace(&net, &["a"], &AlignmentOptions::default())
                .unwrap()
                .cost,
            0
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
            ..AlignmentOptions::default()
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
            ..AlignmentOptions::default()
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
    fn not_easy_sound_unreachable_err() {
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
        // The state equation has no solution for this net, so the final marking is unreachable
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
    fn alignment_ties_are_deterministic() {
        // Each fresh `HashMap` gets new hash keys, so iteration order differs between builds
        let build = || {
            let mut net = PetriNet::new();
            let start = net.add_place(Some(Uuid::from_u128(1)));
            let end = net.add_place(Some(Uuid::from_u128(2)));
            for (id, label) in [(10, "A"), (11, "B")] {
                let t = net.add_transition(Some(label.to_string()), Some(Uuid::from_u128(id)));
                net.add_arc(ArcType::place_to_transition(start, t), None);
                net.add_arc(ArcType::transition_to_place(t, end), None);
            }
            net.initial_marking = Some([(start, 1)].into());
            net.final_markings = Some(vec![[(end, 1)].into()]);
            net
        };
        let alignments: Vec<_> = (0..20)
            .map(|_| {
                align_trace(&build(), &["X"], &AlignmentOptions::default())
                    .unwrap()
                    .moves
            })
            .collect();
        assert!(alignments.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[test]
    fn overlong_trace_err() {
        let mut net = PetriNet::new();
        let start = net.add_place(None);
        let end = net.add_place(None);
        let a = net.add_transition(Some("a".to_string()), None);
        net.add_arc(ArcType::place_to_transition(start, a), None);
        net.add_arc(ArcType::transition_to_place(a, end), None);
        net.initial_marking = Some([(start, 1)].into());
        net.final_markings = Some(vec![[(end, 1)].into()]);
        let trace = vec!["a"; usize::from(TracePos::MAX) + 1];
        assert_eq!(
            align_trace(&net, &trace, &AlignmentOptions::default()),
            Err(AlignmentError::SyncProdNetConstructionFailed(
                SyncProdNetConstructionError::TraceTooLong(trace.len())
            ))
        );
    }

    /// Replays `moves` for `trace` on `net`, independently of the search, and returns their cost
    fn replay(
        net: &PetriNet,
        trace: &[&str],
        moves: &[AlignmentMove],
        cost_fn: &CostFunction,
    ) -> Result<u32, String> {
        let mut marking: HashMap<Uuid, i64> = net
            .initial_marking
            .iter()
            .flatten()
            .map(|(place, tokens)| (place.0, *tokens as i64))
            .collect();
        let mut fire = |transition: TransitionID| {
            for arc in &net.arcs {
                if let ArcType::PlaceTransition(place, t) = arc.from_to {
                    if t == transition.0 {
                        *marking.entry(place).or_default() -= i64::from(arc.weight);
                    }
                }
            }
            if marking.values().any(|tokens| *tokens < 0) {
                return Err(format!("{transition:?} is not enabled"));
            }
            for arc in &net.arcs {
                if let ArcType::TransitionPlace(t, place) = arc.from_to {
                    if t == transition.0 {
                        *marking.entry(place).or_default() += i64::from(arc.weight);
                    }
                }
            }
            Ok(())
        };
        let label = |transition: TransitionID| net.transitions[&transition.0].label.as_deref();
        let (mut position, mut cost) = (0, 0);
        for step in moves {
            match step {
                AlignmentMove::SyncMove {
                    transition,
                    trace_event_index,
                } => {
                    if *trace_event_index != position || label(*transition) != Some(trace[position])
                    {
                        return Err(format!("{step:?} does not match event {position}"));
                    }
                    fire(*transition)?;
                    position += 1;
                    cost += cost_fn.sync_move_cost;
                }
                AlignmentMove::ModelMove { transition } => {
                    fire(*transition)?;
                    cost += match label(*transition) {
                        Some(_) => cost_fn.model_move_cost,
                        None => cost_fn.silent_move_cost,
                    };
                }
                AlignmentMove::LogMove { trace_event_index } => {
                    if *trace_event_index != position {
                        return Err(format!("{step:?} does not match event {position}"));
                    }
                    position += 1;
                    cost += cost_fn.log_move_cost;
                }
            }
        }
        if position != trace.len() {
            return Err(format!(
                "only {position} of {} events consumed",
                trace.len()
            ));
        }
        marking.retain(|_, tokens| *tokens != 0);
        let expected: HashMap<Uuid, i64> = net.final_markings.as_ref().unwrap()[0]
            .iter()
            .filter(|(_, tokens)| **tokens != 0)
            .map(|(place, tokens)| (place.0, *tokens as i64))
            .collect();
        if marking != expected {
            return Err("final marking not reached".to_string());
        }
        Ok(cost)
    }

    /// A small xorshift generator, so random nets are the same in every run
    struct Rng(u64);

    impl Rng {
        fn below(&mut self, bound: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            (self.0 % bound as u64) as usize
        }
    }

    /// A net whose transitions put back as many tokens as they take, so its state space is finite
    fn random_net(rng: &mut Rng) -> PetriNet {
        let mut net = PetriNet::new();
        let places: Vec<_> = (0..2 + rng.below(4))
            .map(|i| net.add_place(Some(Uuid::from_u128(i as u128))))
            .collect();
        for index in 0..2 + rng.below(6) {
            let label = ["a", "b", "c"].get(rng.below(4)).map(|l| l.to_string());
            let t = net.add_transition(label, Some(Uuid::from_u128(1000 + index as u128)));
            for _ in 0..1 + rng.below(2) {
                let (input, output) = (
                    places[rng.below(places.len())],
                    places[rng.below(places.len())],
                );
                net.add_arc(ArcType::place_to_transition(input, t), None);
                net.add_arc(ArcType::transition_to_place(t, output), None);
            }
        }
        let tokens = 1 + rng.below(2) as u64;
        net.initial_marking = Some([(places[0], tokens)].into());
        net.final_markings = Some(vec![[(places[places.len() - 1], tokens)].into()]);
        net
    }

    #[test]
    fn bidirectional_search_matches_unidirectional() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let options = AlignmentOptions::default();
        for _ in 0..3000 {
            let net = random_net(&mut rng);
            let trace: Vec<&str> = (0..rng.below(7))
                .map(|_| ["a", "b", "c", "d"][rng.below(4)])
                .collect();
            let model = ModelNet::build(&net, &options.cost_fn).unwrap();
            let sp = SyncProductNet::construct(&model, &trace, &options.cost_fn).unwrap();
            let forward = petri_net::align_forward(&sp, options.limits_for(1)).map(|a| a.cost);
            let both =
                petri_net::align(&sp, &mut AlignmentContext::default(), options.limits_for(1));
            assert_eq!(
                both.as_ref().map(|a| a.cost),
                forward.as_ref().copied(),
                "{net:?} {trace:?}"
            );
            if let Ok(alignment) = both {
                assert_eq!(
                    replay(&net, &trace, &alignment.moves, &options.cost_fn),
                    Ok(alignment.cost),
                    "{net:?} {trace:?}"
                );
            }
        }
    }

    #[test]
    fn alignments_replay_on_real_logs() {
        let test_path = get_test_data_path();
        for (log, net) in [
            ("Sepsis Cases - Event Log.xes.gz", "sepsis-DISCovered.apnml"),
            (
                "Road_Traffic_Fine_Management_Process.xes.gz",
                "rtfm-imf-02.apnml",
            ),
        ] {
            let log = EventLog::import_from_path(test_path.join("xes").join(log)).unwrap();
            let net = PetriNet::import_pnml(test_path.join("petri-net").join(net)).unwrap();
            let options = AlignmentOptions::default();
            let model = ModelNet::build(&net, &options.cost_fn).unwrap();
            for variant in align_log(&net, &log_to_activity_projection(&log), &options) {
                let trace: Vec<&str> = variant.activities.iter().map(String::as_str).collect();
                let alignment = variant.result.unwrap();
                assert_eq!(
                    replay(&net, &trace, &alignment.moves, &options.cost_fn),
                    Ok(alignment.cost)
                );
                let sp = SyncProductNet::construct(&model, &trace, &options.cost_fn).unwrap();
                let forward = petri_net::align_forward(&sp, options.limits_for(1)).unwrap();
                assert_eq!(forward.cost, alignment.cost, "{trace:?}");
            }
        }
    }
}
