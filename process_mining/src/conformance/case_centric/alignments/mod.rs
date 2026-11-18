//! Implements optimal alignment search
//!
//! A place holds at most 255 ([`petri_net::TokenCount::MAX`]) tokens. The search does not enter
//! markings above that.
use std::{cell::RefCell, sync::atomic::AtomicBool};

use macros_process_mining::{controlled, register_binding};
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
    utils::{
        control::{Cancelled, Control},
        dijkstra_search::{SearchError, SearchLimits},
    },
    AcceptingPetriNet,
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
        /// The label of the transition (and activity of the event)
        label: String,
        /// Index of the event in the trace
        trace_event_index: usize,
    },
    /// Model move (only the model moves,)
    ModelMove {
        /// The transition that was fired
        transition: TransitionID,
        /// The label of the transition (`None` if it is silent)
        label: Option<String>,
    },
    /// Log move (only the log moves)
    LogMove {
        /// Index of the event in the trace
        trace_event_index: usize,
    },
}
impl AlignmentMove {
    /// Whether this move is a deviation between log and model, i.e., a log move or a model move
    /// of a visible transition
    pub fn is_deviation(&self) -> bool {
        match self {
            AlignmentMove::SyncMove { .. } => false,
            AlignmentMove::ModelMove { label, .. } => label.is_some(),
            AlignmentMove::LogMove { .. } => true,
        }
    }
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
    /// See [`AlignmentOptions::retry_out_of_memory`] for aligning the traces that ran out again.
    pub max_memory_mib: Option<usize>,
    /// Whether [`align_variants`] aligns every variant that ran out of its memory share again, one
    /// at a time with the whole [`AlignmentOptions::max_memory_mib`] (default `false`).
    #[serde(default)]
    pub retry_out_of_memory: bool,
}

impl Default for AlignmentOptions {
    fn default() -> Self {
        Self {
            cost_fn: CostFunction::standard(),
            max_states: None,
            max_memory_mib: Some(6 * 1024),
            retry_out_of_memory: false,
        }
    }
}

impl AlignmentOptions {
    /// The limits of each of `searches` searches running at once
    fn limits_for<'a>(&self, searches: usize, cancel: Option<&'a AtomicBool>) -> SearchLimits<'a> {
        SearchLimits {
            max_states: self.max_states,
            max_memory: self
                .max_memory_mib
                .map(|mib| mib.saturating_mul(1 << 20) / searches.max(1)),
            cancel,
        }
    }
}

thread_local! {
    /// Search buffers a thread reuses across the traces of one [`align_variants`] call
    static CONTEXT: RefCell<AlignmentContext> = RefCell::new(AlignmentContext::default());
}

/// Progress stage of [`align_variants`]: one unit per aligned variant
const ALIGN_STAGE: &str = "Aligning variants";

/// One partial result of [`align_variants`]
#[derive(Serialize)]
struct VariantPartial<'a> {
    /// Index of the variant in [`EventLogActivityProjection::traces`]
    variant: usize,
    result: &'a VariantAlignmentResult,
}

/// Compute alignments for all variants from a pre-computed activity projection.
///
/// With [`AlignmentOptions::retry_out_of_memory`], every variant that ran out of its memory share
/// is aligned again afterwards, one at a time on the calling thread, with the whole
/// [`AlignmentOptions::max_memory_mib`]. Such a search may take much longer than its first
/// attempt, and may still run out.
///
/// Reports progress with stage `"Aligning variants"`, one unit per variant. One partial result is
/// a finished variant, `{"variant": i, "result": r}`, where `i` is its index in
/// `projection.traces` and `r` its [`VariantAlignmentResult`]. A retry reports stage
/// `"Retrying variants with more memory"`, one unit per retried variant, and emits each retried
/// variant again: a later partial result with the same index replaces the earlier one.
#[controlled]
#[register_binding]
pub fn align_variants(
    #[bind(handle)] net: &AcceptingPetriNet,
    projection: &EventLogActivityProjection,
    #[bind(default)] options: &AlignmentOptions,
    ctl: &Control<'_>,
) -> Result<Vec<VariantAlignmentResult>, Cancelled> {
    // Both the model half and its reachability are properties of the net, shared by every
    // trace
    let model = build_model(net, &options.cost_fn);
    let mut results = align_variants_on(&model, projection, options, ctl)?;
    // Without a model every variant already carries its error
    let (true, Ok(model)) = (options.retry_out_of_memory, model) else {
        return Ok(results);
    };
    let out_of_memory: Vec<usize> = results
        .iter()
        .enumerate()
        .filter(|(_, variant)| {
            matches!(
                variant.result,
                Err(AlignmentError::SearchError(SearchError::MemoryLimitReached))
            )
        })
        .map(|(index, _)| index)
        .collect();
    if out_of_memory.is_empty() {
        return Ok(results);
    }
    let counter = ctl.counter(
        "Retrying variants with more memory",
        Some(out_of_memory.len() as u64),
    );
    let limits = options.limits_for(1, ctl.cancel_flag_ref());
    let mut ctx = AlignmentContext::default();
    for index in out_of_memory {
        ctl.check()?;
        let variant = &mut results[index];
        let trace: Vec<&str> = variant.activities.iter().map(String::as_str).collect();
        variant.result =
            not_cancelled(align_on(&model, &trace, &options.cost_fn, &mut ctx, limits))?;
        ctl.partial(&VariantPartial {
            variant: index,
            result: variant,
        });
        counter.tick();
    }
    Ok(results)
}

fn align_variants_on(
    model: &Result<ModelNet, AlignmentError>,
    projection: &EventLogActivityProjection,
    options: &AlignmentOptions,
    ctl: &Control<'_>,
) -> Result<Vec<VariantAlignmentResult>, Cancelled> {
    let searches = rayon::current_num_threads().min(projection.traces.len());
    let limits = options.limits_for(searches, ctl.cancel_flag_ref());
    let counter = ctl.counter(ALIGN_STAGE, Some(projection.traces.len() as u64));
    let results = projection
        .traces
        .par_iter()
        .enumerate()
        .map(|(index, (trace_indices, count))| {
            ctl.check()?;
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
            let variant = VariantAlignmentResult {
                activities,
                frequency: *count,
                result: not_cancelled(result)?,
            };
            ctl.partial(&VariantPartial {
                variant: index,
                result: &variant,
            });
            counter.tick();
            Ok(variant)
        })
        .collect();
    // Kept buffers would otherwise stay allocated after the call, next to a later search that
    // may use the whole budget
    rayon::broadcast(|_| CONTEXT.take());
    results
}

/// A cancelled search ends the whole call rather than being a result of its variant
fn not_cancelled(
    result: Result<AlignmentResult, AlignmentError>,
) -> Result<Result<AlignmentResult, AlignmentError>, Cancelled> {
    match result {
        Err(AlignmentError::SearchError(SearchError::Cancelled)) => Err(Cancelled),
        result => Ok(result),
    }
}

fn align_on(
    model: &ModelNet,
    trace: &[&str],
    cost_fn: &CostFunction,
    ctx: &mut AlignmentContext,
    limits: SearchLimits<'_>,
) -> Result<AlignmentResult, AlignmentError> {
    let sp = SyncProductNet::construct(model, trace, cost_fn)?;
    petri_net::align(&sp, ctx, limits)
}

/// Build the trace-independent half, rejecting nets that cannot reach their final marking
fn build_model(
    net: &AcceptingPetriNet,
    cost_fn: &CostFunction,
) -> Result<ModelNet, AlignmentError> {
    let model = ModelNet::build(net, cost_fn)?;
    if !reachability::final_marking_reachable(&model) {
        return Err(SearchError::Unreachable.into());
    }
    Ok(model)
}

/// Compute alignment for a single trace (given as activity sequence).
///
/// Stops once a [`Control`] is cancelled, and reports no progress or partial results.
#[controlled]
#[register_binding(stringify_error)]
pub fn align_trace(
    #[bind(handle)] net: &AcceptingPetriNet,
    trace: &[&str],
    #[bind(default)] options: &AlignmentOptions,
    ctl: &Control<'_>,
) -> Result<AlignmentResult, AlignmentError> {
    let model = build_model(net, &options.cost_fn)?;
    let mut ctx = AlignmentContext::default();
    align_on(
        &model,
        trace,
        &options.cost_fn,
        &mut ctx,
        options.limits_for(1, ctl.cancel_flag_ref()),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
/// Alignment Fitness Result
pub struct FitnessResult {
    /// Log fitness, as the total computed fitness (summing up the costs for all traces)
    pub log_fitness: f64,
    /// Average trace fitness (across all traces)
    pub average_fitness: f64,
    /// Fraction of traces that perfectly fit (i.e., whose alignment has no log moves and no
    /// model moves of visible transitions)
    pub perfectly_fitting_frac: f64,
    /// The total cost, summed up from all traces
    pub total_costs: u64,
}

/// Compute fitness stats from alignment results
///
/// Also constructs the empty-trace alignment (shortest path through model).
/// Without any traces, all fitness values are `1.0`.
#[register_binding(stringify_error)]
pub fn compute_fitness(
    #[bind(handle)] net: &AcceptingPetriNet,
    alignments: &[VariantAlignmentResult],
    #[bind(default)] options: &AlignmentOptions,
) -> Result<FitnessResult, AlignmentError> {
    let empty = align_trace(net, &[], options)?;
    let model_path_min = empty.cost;
    let mut num_perfectly_fitting = 0;
    let mut total_costs = 0;
    let mut fitness_sum_for_avg = 0f64;
    let mut num_traces = 0;
    let mut num_events = 0;
    for variant in alignments {
        let res = variant.result.as_ref().map_err(|e| e.clone())?;
        let costs = res.cost;
        // Sync and silent moves may have a cost, so a fitting trace can have a nonzero cost
        if !res.moves.iter().any(AlignmentMove::is_deviation) {
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
            1f64
        } else {
            fitness_sum_for_avg / num_traces as f64
        },
        perfectly_fitting_frac: if num_traces == 0 {
            1f64
        } else {
            num_perfectly_fitting as f64 / num_traces as f64
        },
        total_costs,
    })
}

#[cfg(test)]
mod test {
    use std::time::Instant;

    use crate::{
        conformance::alignments::{
            align_trace, align_variants, compute_fitness,
            cost::CostFunction,
            petri_net::{self, AlignmentContext, AlignmentError, TracePos},
            sync_prod_net::{ModelNet, SyncProdNetConstructionError, SyncProductNet},
            AlignmentMove, AlignmentOptions,
        },
        core::{
            event_data::case_centric::{
                utils::activity_projection::{
                    log_to_activity_projection, EventLogActivityProjection,
                },
                Event, Trace,
            },
            process_models::petri_net::{ArcType, Marking, PlaceID, TransitionID},
        },
        test_utils::get_test_data_path,
        utils::{
            control::{Cancelled, Control, Progress},
            dijkstra_search::{SearchError, SearchLimits},
        },
        AcceptingPetriNet, EventLog, Importable, PetriNet,
    };
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    };

    fn sepsis_net() -> AcceptingPetriNet {
        AcceptingPetriNet::import_from_path(
            get_test_data_path()
                .join("petri-net")
                .join("sepsis-DISCovered.apnml"),
        )
        .unwrap()
    }

    fn align_helper(
        log_name: &str,
        net_name: &str,
    ) -> (
        Vec<super::VariantAlignmentResult>,
        Result<super::FitnessResult, AlignmentError>,
    ) {
        let test_path = get_test_data_path();
        let log = EventLog::import_from_path(test_path.join("xes").join(log_name)).unwrap();
        let net = AcceptingPetriNet::import_from_path(test_path.join("petri-net").join(net_name))
            .unwrap();
        let act_proj = log_to_activity_projection(&log);
        let options = AlignmentOptions::default();
        let now = Instant::now();
        let result = align_variants(&net, &act_proj, &options);
        println!("Aligning traces took {:?}", now.elapsed());
        let fitness = compute_fitness(&net, &result, &options);
        println!("{fitness:?}");
        (result, fitness)
    }

    fn sepsis() -> (AcceptingPetriNet, EventLogActivityProjection) {
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

        let retry = AlignmentOptions {
            retry_out_of_memory: true,
            ..options
        };
        let retried = pool.install(|| align_variants(&net, &projection, &retry));
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
    fn control_reports_every_variant() {
        let (net, projection) = sepsis();
        let progress = Mutex::new(Vec::new());
        let partials = Mutex::new(Vec::new());
        let ctl = Control::new()
            .on_progress(|p| progress.lock().unwrap().push(p))
            .on_partial(|v| partials.lock().unwrap().push(v));
        let results = ctl
            .align_variants(&net, &projection, &AlignmentOptions::default())
            .unwrap();
        drop(ctl);

        let total = projection.traces.len() as u64;
        let progress: Vec<Progress> = progress.into_inner().unwrap();
        assert!(progress
            .iter()
            .all(|p| p.stage == "Aligning variants" && p.total == Some(total)));
        // Parallel workers may skip an intermediate count, never the first or the last
        let done: Vec<u64> = progress.iter().map(|p| p.done).collect();
        assert_eq!(done.first(), Some(&0));
        assert_eq!(done.last(), Some(&total));
        assert!(done.is_sorted());

        let mut partials = partials.into_inner().unwrap();
        partials.sort_by_key(|p| p["variant"].as_u64().unwrap());
        assert_eq!(partials.len(), results.len());
        for (index, (partial, result)) in partials.iter().zip(&results).enumerate() {
            assert_eq!(
                partial,
                &serde_json::json!({ "variant": index, "result": result })
            );
        }
    }

    #[test]
    fn control_cancels_after_the_first_variant() {
        let (net, projection) = sepsis();
        let options = AlignmentOptions::default();
        let flag = AtomicBool::new(false);
        let finished = Mutex::new(0);
        let ctl = Control::new()
            .on_progress(|p| {
                if p.done >= 1 {
                    flag.store(true, Ordering::Relaxed);
                }
            })
            .on_partial(|_| *finished.lock().unwrap() += 1)
            .cancel_flag(&flag);
        assert_eq!(
            ctl.align_variants(&net, &projection, &options).unwrap_err(),
            Cancelled
        );
        // Each thread finishes at most the variant it was aligning when the flag was set
        assert!(*finished.lock().unwrap() <= rayon::current_num_threads());

        flag.store(false, Ordering::Relaxed);
        assert_eq!(
            ctl.align_variants(
                &net,
                &projection,
                &AlignmentOptions {
                    retry_out_of_memory: true,
                    ..options.clone()
                }
            )
            .unwrap_err(),
            Cancelled
        );
        flag.store(false, Ordering::Relaxed);
        assert_eq!(
            ctl.align_variants(&net, &projection, &options).unwrap_err(),
            Cancelled
        );
    }

    #[test]
    fn cancelled_search_is_cancelled_not_a_limit() {
        let flag = AtomicBool::new(true);
        let ctl = Control::new().cancel_flag(&flag);
        let options = AlignmentOptions {
            max_states: Some(0),
            ..AlignmentOptions::default()
        };
        assert_eq!(
            ctl.align_trace(&sepsis_net(), &["A", "B"], &options),
            Err(AlignmentError::SearchError(SearchError::Cancelled))
        );
        flag.store(false, Ordering::Relaxed);
        assert_eq!(
            ctl.align_trace(&sepsis_net(), &["A", "B"], &options),
            Err(AlignmentError::SearchError(SearchError::LimitReached))
        );
    }

    #[test]
    fn retry_emits_replaced_variants_again() {
        let (net, projection) = sepsis();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(32)
            .build()
            .unwrap();
        let options = AlignmentOptions {
            max_memory_mib: Some(1),
            ..AlignmentOptions::default()
        };
        let partials = Mutex::new(Vec::new());
        let retries = Mutex::new(Vec::new());
        let ctl = Control::new()
            .on_partial(|v| partials.lock().unwrap().push(v))
            .on_progress(|p| {
                if p.stage == "Retrying variants with more memory" {
                    retries.lock().unwrap().push((p.done, p.total));
                }
            });
        let results = pool
            .install(|| {
                ctl.align_variants(
                    &net,
                    &projection,
                    &AlignmentOptions {
                        retry_out_of_memory: true,
                        ..options.clone()
                    },
                )
            })
            .unwrap();
        drop(ctl);

        let partials = partials.into_inner().unwrap();
        let retried = (partials.len() - results.len()) as u64;
        assert!(retried > 0);
        assert_eq!(
            retries.into_inner().unwrap().last(),
            Some(&(retried, Some(retried)))
        );
        let mut last = vec![None; results.len()];
        for partial in partials {
            last[partial["variant"].as_u64().unwrap() as usize] = Some(partial["result"].clone());
        }
        for (last, result) in last.into_iter().zip(&results) {
            assert!(result.result.is_ok());
            assert_eq!(last, Some(serde_json::to_value(result).unwrap()));
        }
    }

    #[cfg(feature = "bindings")]
    #[test]
    fn align_variants_binding_streams_and_cancels() {
        use crate::bindings::{
            call_with_outcome, list_functions, AppState, CallError, RegistryItem,
        };
        let mut net = PetriNet::new();
        let start = net.add_place(None);
        let mid = net.add_place(None);
        let end = net.add_place(None);
        let a = net.add_transition(Some("a".to_string()), None);
        let b = net.add_transition(Some("b".to_string()), None);
        net.add_arc(ArcType::place_to_transition(start, a), None);
        net.add_arc(ArcType::transition_to_place(a, mid), None);
        net.add_arc(ArcType::place_to_transition(mid, b), None);
        net.add_arc(ArcType::transition_to_place(b, end), None);
        let net =
            AcceptingPetriNet::new(net, Marking::from([(start, 1)]), Marking::from([(end, 1)]));
        let log = crate::event_log!(["a", "b"], ["a"], ["b", "a", "b"]);
        let state = AppState::default();
        state.add("net", RegistryItem::custom(net));
        state.add(
            "log",
            RegistryItem::EventLogActivityProjection(log_to_activity_projection(&log)),
        );
        let binding = list_functions()
            .into_iter()
            .find(|b| b.name == "align_variants")
            .unwrap();
        let args = serde_json::json!({ "net": "net", "projection": "log" });

        let partials = Mutex::new(Vec::new());
        let ctl = Control::new().on_partial(|v| partials.lock().unwrap().push(v));
        let out = call_with_outcome(binding, &args, &state, &ctl)
            .unwrap()
            .result;
        drop(ctl);
        let results: serde_json::Value = serde_json::from_slice(&out).unwrap();
        let mut partials = partials.into_inner().unwrap();
        partials.sort_by_key(|p| p["variant"].as_u64().unwrap());
        assert_eq!(partials.len(), 3);
        for (index, partial) in partials.iter().enumerate() {
            assert_eq!(partial["variant"], index);
            assert_eq!(partial["result"], results[index]);
        }

        let flag = AtomicBool::new(false);
        let ctl = Control::new()
            .on_progress(|_| flag.store(true, Ordering::Relaxed))
            .cancel_flag(&flag);
        assert_eq!(
            call_with_outcome(binding, &args, &state, &ctl),
            Err(CallError::Cancelled)
        );
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
                cancel: None,
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
            cancel: None,
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
                cancel: None,
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
    fn unknown_place_in_initial_marking_err() {
        let mut net = sepsis_net();
        net.initial_marking.add(PlaceID(999), 1);
        let sn = ModelNet::build(&net, &CostFunction::standard());
        assert_eq!(
            sn,
            Err(SyncProdNetConstructionError::InvalidPlaceInMarking(
                PlaceID(999)
            ))
        );
    }
    #[test]
    fn unknown_place_in_final_marking_err() {
        let mut net = sepsis_net();
        net.final_marking.add(PlaceID(999), 1);
        let sn = ModelNet::build(&net, &CostFunction::standard());
        assert_eq!(
            sn,
            Err(SyncProdNetConstructionError::InvalidPlaceInMarking(
                PlaceID(999)
            ))
        );
    }

    #[test]
    fn parallel_arcs_weights_add_up() {
        // Two arcs p0 -> t, so firing `t` needs two tokens, not one
        let mut net = PetriNet::new();
        let p0 = net.add_place(None);
        let p1 = net.add_place(None);
        let t = net.add_transition(Some("a".to_string()), None);
        net.add_arc(ArcType::place_to_transition(p0, t), None);
        net.add_arc(ArcType::place_to_transition(p0, t), None);
        net.add_arc(ArcType::transition_to_place(t, p1), None);
        let mut net =
            AcceptingPetriNet::new(net, Marking::from([(p0, 1)]), Marking::from([(p1, 1)]));
        let options = AlignmentOptions::default();

        assert_eq!(
            align_trace(&net, &["a"], &options),
            Err(AlignmentError::SearchError(SearchError::Unreachable))
        );
        net.initial_marking = Marking::from([(p0, 2)]);
        assert_eq!(align_trace(&net, &["a"], &options).unwrap().cost, 0);

        // The summed weight must still fit a TokenCount
        net.net
            .add_arc(ArcType::place_to_transition(p0, t), Some(300));
        assert_eq!(
            ModelNet::build(&net, &CostFunction::standard()),
            Err(SyncProdNetConstructionError::ArcWeightTooLarge(302))
        );
    }

    #[test]
    fn oversized_marking_err() {
        let mut net = sepsis_net();
        let (place, _) = net.initial_marking.iter().next().unwrap();
        net.initial_marking.set(place, 400);
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
        net.add_arc(ArcType::place_to_transition(p[0], t1), None);
        net.add_arc(ArcType::place_to_transition(p[2], t1), None);
        net.add_arc(ArcType::transition_to_place(t1, p[1]), None);
        // t2: p2 -> p3 + p4
        let t2 = net.add_transition(Some("b".to_string()), None);
        net.add_arc(ArcType::place_to_transition(p[1], t2), None);
        net.add_arc(ArcType::transition_to_place(t2, p[2]), None);
        net.add_arc(ArcType::transition_to_place(t2, p[3]), None);
        let net =
            AcceptingPetriNet::new(net, Marking::from([(p[0], 1)]), Marking::from([(p[3], 1)]));

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
        let places: Vec<_> = (0..200).map(|_| net.add_place(None)).collect();
        // "a" carries the token from start to end
        let a = net.add_transition(Some("a".to_string()), None);
        net.add_arc(ArcType::place_to_transition(places[0], a), None);
        net.add_arc(ArcType::transition_to_place(a, places[1]), None);
        // "b" needs a token in a place nothing ever marks, so it can never fire
        let b = net.add_transition(Some("b".to_string()), None);
        net.add_arc(ArcType::place_to_transition(places[100], b), None);
        net.add_arc(ArcType::transition_to_place(b, places[1]), None);
        let net = AcceptingPetriNet::new(
            net,
            Marking::from([(places[0], 1)]),
            Marking::from([(places[1], 1)]),
        );

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
        net.add_arc(ArcType::place_to_transition(places[0], a), None);
        // Takes nothing from a place that never holds a token, so it must not stop "a" firing
        net.add_arc(ArcType::place_to_transition(places[2], a), Some(0));
        net.add_arc(ArcType::transition_to_place(a, places[1]), None);
        let net = AcceptingPetriNet::new(
            net,
            Marking::from([(places[0], 1)]),
            Marking::from([(places[1], 1)]),
        );
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
        // The Sepsis net without the arcs that mark the final places
        let source = sepsis_net();
        let mut net = PetriNet::new();
        for (_, place) in source.net.places() {
            net.add_place(place.name.clone());
        }
        for (_, t) in source.net.transitions() {
            net.add_transition(t.label.clone(), t.name.clone());
        }
        for arc in source.net.arcs() {
            if let ArcType::TransitionPlace(_, place) = arc.from_to {
                if source.final_marking.tokens(place) > 0 {
                    continue;
                }
            }
            net.add_arc(arc.from_to, Some(arc.weight));
        }
        let net = AcceptingPetriNet::new(net, source.initial_marking, source.final_marking);
        let act_proj = log_to_activity_projection(&log);
        let options = AlignmentOptions {
            cost_fn: CostFunction::standard(),
            max_states: None,
            ..AlignmentOptions::default()
        };
        let empty_trace_align = align_trace(&net, &[], &options);
        assert_eq!(
            empty_trace_align,
            Err(AlignmentError::SearchError(SearchError::Unreachable))
        );
        let result = align_variants(&net, &act_proj, &options);
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
        let net = sepsis_net();
        let act_proj = log_to_activity_projection(&log);
        let options = AlignmentOptions {
            cost_fn: CostFunction::standard(),
            max_states: Some(10),
            ..AlignmentOptions::default()
        };
        let empty_trace_align = align_trace(&net, &[], &options);
        assert_eq!(
            empty_trace_align,
            Err(AlignmentError::SearchError(SearchError::LimitReached))
        );
        let result = align_variants(&net, &act_proj, &options);
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
        let net = AcceptingPetriNet::import_from_path(
            test_path.join("petri-net").join("sepsis-fodina.apnml"),
        )
        .unwrap();
        let act_proj = log_to_activity_projection(&log);
        let options = AlignmentOptions::default();
        // The state equation has no solution for this net, so the final marking is unreachable
        let empty_trace_align = align_trace(&net, &[], &options);
        assert_eq!(
            empty_trace_align,
            Err(AlignmentError::SearchError(SearchError::Unreachable))
        );
        let result = align_variants(&net, &act_proj, &options);
        for variant in result {
            assert_eq!(
                variant.result,
                Err(AlignmentError::SearchError(SearchError::Unreachable))
            );
        }
    }

    #[test]
    fn ties_break_in_transition_order() {
        let chosen = |labels: [&str; 2]| {
            let mut net = PetriNet::new();
            let start = net.add_place(None);
            let end = net.add_place(None);
            for label in labels {
                let t = net.add_transition(Some(label.to_string()), None);
                net.add_arc(ArcType::place_to_transition(start, t), None);
                net.add_arc(ArcType::transition_to_place(t, end), None);
            }
            let net =
                AcceptingPetriNet::new(net, Marking::from([(start, 1)]), Marking::from([(end, 1)]));
            let moves = align_trace(&net, &["X"], &AlignmentOptions::default())
                .unwrap()
                .moves;
            moves
                .into_iter()
                .find_map(|m| match m {
                    AlignmentMove::ModelMove { transition, .. } => Some(transition),
                    _ => None,
                })
                .unwrap()
        };
        assert_eq!(chosen(["A", "B"]), chosen(["B", "A"]));
    }

    #[test]
    fn perfectly_fitting_ignores_sync_and_silent_costs() {
        let mut net = PetriNet::new();
        let start = net.add_place(None);
        let mid = net.add_place(None);
        let end = net.add_place(None);
        let a = net.add_transition(Some("a".to_string()), None);
        let tau = net.add_transition(None, None);
        net.add_arc(ArcType::place_to_transition(start, a), None);
        net.add_arc(ArcType::transition_to_place(a, mid), None);
        net.add_arc(ArcType::place_to_transition(mid, tau), None);
        net.add_arc(ArcType::transition_to_place(tau, end), None);
        let net =
            AcceptingPetriNet::new(net, Marking::from([(start, 1)]), Marking::from([(end, 1)]));
        let options = AlignmentOptions {
            cost_fn: CostFunction {
                sync_move_cost: 1,
                silent_move_cost: 1,
                ..CostFunction::standard()
            },
            ..AlignmentOptions::default()
        };
        let mut log = EventLog::new();
        for trace in [&["a"][..], &["a", "b"]] {
            let mut t = Trace::new();
            t.events
                .extend(trace.iter().map(|a| Event::new(a.to_string())));
            log.traces.push(t);
        }
        let results = align_variants(&net, &log_to_activity_projection(&log), &options);
        let fitting = results.iter().find(|v| v.activities == ["a"]).unwrap();
        assert_eq!(fitting.result.as_ref().unwrap().cost, 2);
        let fitness = compute_fitness(&net, &results, &options).unwrap();
        assert_eq!(fitness.perfectly_fitting_frac, 0.5);
    }

    #[test]
    fn fitness_of_empty_log_is_one() {
        let net = sepsis_net();
        let options = AlignmentOptions::default();
        let fitness = compute_fitness(&net, &[], &options).unwrap();
        assert_eq!(fitness.log_fitness, 1.0);
        assert_eq!(fitness.average_fitness, 1.0);
        assert_eq!(fitness.perfectly_fitting_frac, 1.0);
        assert_eq!(fitness.total_costs, 0);
    }

    #[test]
    fn overlong_trace_err() {
        let mut net = PetriNet::new();
        let start = net.add_place(None);
        let end = net.add_place(None);
        let a = net.add_transition(Some("a".to_string()), None);
        net.add_arc(ArcType::place_to_transition(start, a), None);
        net.add_arc(ArcType::transition_to_place(a, end), None);
        let net =
            AcceptingPetriNet::new(net, Marking::from([(start, 1)]), Marking::from([(end, 1)]));
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
        net: &AcceptingPetriNet,
        trace: &[&str],
        moves: &[AlignmentMove],
        cost_fn: &CostFunction,
    ) -> Result<u32, String> {
        let mut marking = net.initial_marking.clone();
        let mut fire = |transition: TransitionID| {
            marking = net
                .net
                .fire(&marking, transition)
                .ok_or(format!("{transition:?} is not enabled"))?;
            Ok::<_, String>(())
        };
        let label = |transition: TransitionID| net.net.transition(transition).label.as_deref();
        let (mut position, mut cost) = (0, 0);
        for step in moves {
            match step {
                AlignmentMove::SyncMove {
                    transition,
                    label: move_label,
                    trace_event_index,
                } => {
                    if *trace_event_index != position
                        || label(*transition) != Some(trace[position])
                        || move_label != trace[position]
                    {
                        return Err(format!("{step:?} does not match event {position}"));
                    }
                    fire(*transition)?;
                    position += 1;
                    cost += cost_fn.sync_move_cost;
                }
                AlignmentMove::ModelMove {
                    transition,
                    label: move_label,
                } => {
                    if move_label.as_deref() != label(*transition) {
                        return Err(format!("{step:?} has the wrong label"));
                    }
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
        if marking != net.final_marking {
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
    fn random_net(rng: &mut Rng) -> AcceptingPetriNet {
        let mut net = PetriNet::new();
        let places: Vec<_> = (0..2 + rng.below(4)).map(|_| net.add_place(None)).collect();
        for _ in 0..2 + rng.below(6) {
            let label = ["a", "b", "c"].get(rng.below(4)).map(|l| l.to_string());
            let t = net.add_transition(label, None);
            for _ in 0..1 + rng.below(2) {
                let (input, output) = (
                    places[rng.below(places.len())],
                    places[rng.below(places.len())],
                );
                net.add_arc(ArcType::place_to_transition(input, t), None);
                net.add_arc(ArcType::transition_to_place(t, output), None);
            }
        }
        let tokens = 1 + rng.below(2) as u32;
        AcceptingPetriNet::new(
            net,
            Marking::from([(places[0], tokens)]),
            Marking::from([(places[places.len() - 1], tokens)]),
        )
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
            let forward =
                petri_net::align_forward(&sp, options.limits_for(1, None)).map(|a| a.cost);
            let both = petri_net::align(
                &sp,
                &mut AlignmentContext::default(),
                options.limits_for(1, None),
            );
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
            let net =
                AcceptingPetriNet::import_from_path(test_path.join("petri-net").join(net)).unwrap();
            let options = AlignmentOptions::default();
            let model = ModelNet::build(&net, &options.cost_fn).unwrap();
            for variant in align_variants(&net, &log_to_activity_projection(&log), &options) {
                let trace: Vec<&str> = variant.activities.iter().map(String::as_str).collect();
                let alignment = variant.result.unwrap();
                assert_eq!(
                    replay(&net, &trace, &alignment.moves, &options.cost_fn),
                    Ok(alignment.cost)
                );
                let sp = SyncProductNet::construct(&model, &trace, &options.cost_fn).unwrap();
                let forward = petri_net::align_forward(&sp, options.limits_for(1, None)).unwrap();
                assert_eq!(forward.cost, alignment.cost, "{trace:?}");
            }
        }
    }
}
