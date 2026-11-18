//! Escaping-edges precision of a Petri net with respect to an event log
//!
//! Precision measures how much of the model's behavior the log backs up. The log's paths are
//! inserted into a prefix tree. In every state `s` (a prefix), the activities the model allows
//! next (`av(s)`) are compared with those the log continues with (`ex(s)`). What the model allows
//! but no path takes is an escaping edge, and
//!
//! ```text
//! precision = sum_s w(s) * |ex(s)| / sum_s w(s) * |av(s)|
//! ```
//!
//! where `w(s)` is the number of traces passing through `s`, and `1.0` if nothing is available.
//!
//! [`EscapingEdgesOptions`] selects how traces become paths and how silent transitions count,
//! and has presets reproducing `ProM` and pm4py.
//!
//! References:
//! - J. Munoz-Gama, J. Carmona: A Fresh Look at Precision in Process Conformance. BPM 2010.
//! - A. Adriansyah, J. Munoz-Gama, J. Carmona, B.F. van Dongen, W.M.P. van der Aalst:
//!   Measuring Precision of Modeled Behavior. Information Systems and
//!   e-Business Management 13(1), 2015.
use macros_process_mining::{controlled, register_binding};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    conformance::alignments::{
        petri_net::AlignmentError, sync_prod_net::SyncProdNetConstructionError, AlignmentMove,
        AlignmentOptions, VariantAlignmentResult,
    },
    core::{
        event_data::case_centric::utils::activity_projection::EventLogActivityProjection,
        process_models::petri_net::TransitionID,
    },
    utils::control::{Cancelled, Control},
    AcceptingPetriNet,
};

use self::{
    prefix_tree::{Inserted, PrefixTree},
    replay::{Replayer, Symbol},
};

mod prefix_tree;
mod replay;
#[cfg(test)]
mod tests;

/// Where the paths through the model come from
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub enum LogPaths {
    /// One optimal alignment per variant, without its log moves. A trace that does not fit is
    /// measured along its closest model run. A variant whose alignment fails is left out, see
    /// [`EscapingEdgesPrecision::unaligned_traces`]. If all of them fail, the result is
    /// [`PrecisionError::NoTraceAligned`].
    Aligned {
        /// How to align the variants
        options: AlignmentOptions,
    },
    /// The traces as recorded, each up to its first event the model cannot replay, as in the
    /// original escaping-edges precision
    ReplayUntilDeviation,
}

impl Default for LogPaths {
    fn default() -> Self {
        Self::Aligned {
            options: AlignmentOptions::default(),
        }
    }
}

/// Options for [`escaping_edges_precision`]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct EscapingEdgesOptions {
    /// Where the paths through the model come from
    pub log_paths: LogPaths,
    /// How silent transitions take part
    pub silent_transitions: SilentTransitions,
    /// How traces that end in a state count there
    pub trace_ends: TraceEnds,
    /// Whether to count escaping edges in the initial state. Without it, only the activities the
    /// traces start with count as available there.
    pub count_escaping_edges_at_start: bool,
    /// Whether an activity only counts as available if the final marking can still be reached
    /// after it, so that the dead ends of an unsound net are not counted as allowed behavior
    pub exclude_dead_ends: bool,
    /// Limit on the markings visited per search (following silent transitions, or checking that
    /// the final marking can be reached). Reaching it can make the precision wrong in either
    /// direction, see [`EscapingEdgesPrecision::search_limit_reached`].
    pub search_limit: Option<usize>,
    /// Whether to return every escaping edge
    pub report_escaping_edges: bool,
}

impl Default for EscapingEdgesOptions {
    fn default() -> Self {
        Self {
            log_paths: LogPaths::default(),
            silent_transitions: SilentTransitions::Skip,
            trace_ends: TraceEnds::Weigh,
            count_escaping_edges_at_start: true,
            exclude_dead_ends: false,
            search_limit: Some(10_000),
            report_escaping_edges: false,
        }
    }
}

impl EscapingEdgesOptions {
    /// The options of `ProM`'s "Check Precision using Escaping Edges" (Precision package).
    ///
    /// The result is close to `ProM`'s, but not equal: `ProM`'s aligner picks other optimal
    /// alignments, and where it fires silent transitions before they are needed, its precision
    /// depends on that order (e.g., 0.575 instead of 0.548 on the Sepsis log).
    pub fn prom() -> Self {
        Self {
            silent_transitions: SilentTransitions::AsActivities,
            count_escaping_edges_at_start: false,
            ..Self::default()
        }
    }

    /// The options of `pm4py.precision_alignments`.
    ///
    /// pm4py's search over silent transitions keeps one marking per silent transition, and only
    /// starts from the markings reached with the fewest silent moves. It can miss available
    /// activities (e.g., one that needs silent transitions in two parallel branches), so results
    /// can differ.
    pub fn pm4py() -> Self {
        Self {
            log_paths: LogPaths::ReplayUntilDeviation,
            trace_ends: TraceEnds::Ignore,
            ..Self::default()
        }
    }

    /// The options of a preset
    pub fn preset(preset: EscapingEdgesPreset) -> Self {
        match preset {
            EscapingEdgesPreset::Default => Self::default(),
            EscapingEdgesPreset::Prom => Self::prom(),
            EscapingEdgesPreset::Pm4py => Self::pm4py(),
        }
    }
}

/// Whose escaping-edges precision a run follows
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EscapingEdgesPreset {
    /// [`EscapingEdgesOptions::default`]
    #[default]
    Default,
    /// [`EscapingEdgesOptions::prom`]
    Prom,
    /// [`EscapingEdgesOptions::pm4py`]
    Pm4py,
}

/// The [`EscapingEdgesOptions`] of a preset, e.g., for a host to adjust before calling
/// [`escaping_edges_precision`]
#[register_binding]
pub fn escaping_edges_options(
    #[bind(default)] preset: EscapingEdgesPreset,
) -> EscapingEdgesOptions {
    EscapingEdgesOptions::preset(preset)
}

/// An activity (or transition) the model allows in a state, but no trace takes there
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
pub struct EscapingEdge {
    /// The state, as the path leading to it
    pub prefix: Vec<PathStep>,
    /// What escapes
    pub escaping: PathStep,
    /// Weight of the state
    pub weight: u64,
}

/// Result of [`escaping_edges_precision`]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EscapingEdgesPrecision {
    /// Weighted share of available behavior that the log takes, `1.0` if nothing is available
    pub precision: f64,
    /// Sum over all states of their weight times the number of available activities
    pub weighted_available: u64,
    /// Sum over all states of their weight times the number of escaping edges
    pub weighted_escaping: u64,
    /// Number of states with a positive weight
    pub num_states: usize,
    /// Traces left out because their alignment failed (e.g., a search limit was reached, or the
    /// trace is too long)
    pub unaligned_traces: u64,
    /// Traces only measured up to the first step the model could not follow (a deviation, or a
    /// search stopped at `search_limit`)
    pub truncated_traces: u64,
    /// Whether a search stopped at `search_limit` somewhere, which can make `precision` too high
    /// (fewer activities available after silent transitions) or too low (a dead end counted as
    /// completing)
    pub search_limit_reached: bool,
    /// Every escaping edge, if `report_escaping_edges` was set
    pub escaping_edges: Option<Vec<EscapingEdge>>,
}

/// Escaping-edges precision of `net` with respect to `log`
///
/// See the [module documentation](self) for the measure and [`EscapingEdgesOptions`] for the
/// variants.
///
/// With [`LogPaths::Aligned`], first reports the progress of
/// [`align_variants`](crate::conformance::case_centric::alignments::align_variants) (stage
/// `"Aligning variants"`, then `"Retrying variants with more memory"` if retrying) to a
/// [`Control`]. Then reports progress with stage `"Measuring escaping edges"`, one unit per
/// variant. Emits no partial results.
#[controlled]
#[register_binding(stringify_error)]
pub fn escaping_edges_precision(
    #[bind(handle)] net: &AcceptingPetriNet,
    log: &EventLogActivityProjection,
    #[bind(default)] options: &EscapingEdgesOptions,
    ctl: &Control<'_>,
) -> Result<EscapingEdgesPrecision, PrecisionError> {
    match &options.log_paths {
        LogPaths::Aligned {
            options: alignment_options,
        } => {
            // Forward the alignment's progress only: its partial results are alignments, not
            // precision results.
            let progress_only =
                Control::new().on_progress(|p| ctl.progress(p.stage, p.done, p.total));
            let align_ctl = match ctl.cancel_flag_ref() {
                Some(flag) => progress_only.cancel_flag(flag),
                None => progress_only,
            };
            let alignments = align_ctl.align_variants(net, log, alignment_options)?;
            from_alignments(net, &alignments, options, ctl)
        }
        LogPaths::ReplayUntilDeviation => replay_precision(net, log, options, ctl),
    }
}

/// Progress stage of the measure: one unit per variant added to the prefix tree
const MEASURE_STAGE: &str = "Measuring escaping edges";

fn replay_precision(
    net: &AcceptingPetriNet,
    log: &EventLogActivityProjection,
    options: &EscapingEdgesOptions,
    ctl: &Control<'_>,
) -> Result<EscapingEdgesPrecision, PrecisionError> {
    if options.silent_transitions == SilentTransitions::AsActivities {
        return Err(PrecisionError::IncompatibleOptions(
            "replaying the log requires `SilentTransitions::Skip`".to_string(),
        ));
    }
    let mut measure = Measure::new(net, options)?;
    let symbols: Vec<Option<Symbol>> = log
        .activities
        .iter()
        .map(|activity| measure.replayer.activity_symbol(activity))
        .collect();
    let counter = ctl.counter(MEASURE_STAGE, Some(log.traces.len() as u64));
    for (trace, frequency) in &log.traces {
        ctl.check()?;
        measure.add(trace.iter().map(|&activity| symbols[activity]), *frequency);
        counter.tick();
    }
    ctl.check()?;
    Ok(measure.finish())
}

/// Escaping-edges precision of `net` with respect to the log behind `alignments`
///
/// Reuses alignments computed for `net` (e.g., for fitness). Requires [`LogPaths::Aligned`],
/// whose fields are then not used.
///
/// Fails with [`PrecisionError::NoTraceAligned`] if there are traces, but none of them was
/// aligned (e.g., because the final marking is unreachable).
///
/// Reports progress to a [`Control`] with stage `"Measuring escaping edges"`, one unit per
/// variant. Emits no partial results.
#[controlled]
#[register_binding(stringify_error)]
pub fn escaping_edges_precision_from_alignments(
    #[bind(handle)] net: &AcceptingPetriNet,
    alignments: &[VariantAlignmentResult],
    #[bind(default)] options: &EscapingEdgesOptions,
    ctl: &Control<'_>,
) -> Result<EscapingEdgesPrecision, PrecisionError> {
    from_alignments(net, alignments, options, ctl)
}

fn from_alignments(
    net: &AcceptingPetriNet,
    alignments: &[VariantAlignmentResult],
    options: &EscapingEdgesOptions,
    ctl: &Control<'_>,
) -> Result<EscapingEdgesPrecision, PrecisionError> {
    if matches!(options.log_paths, LogPaths::ReplayUntilDeviation) {
        return Err(PrecisionError::IncompatibleOptions(
            "precision from alignments requires `LogPaths::Aligned`".to_string(),
        ));
    }
    let mut measure = Measure::new(net, options)?;
    let mut aligned_traces = 0;
    let mut first_error = None;
    let counter = ctl.counter(MEASURE_STAGE, Some(alignments.len() as u64));
    for variant in alignments {
        ctl.check()?;
        match &variant.result {
            Ok(alignment) => {
                let mut path = alignment
                    .moves
                    .iter()
                    .filter_map(|step| match step {
                        AlignmentMove::SyncMove { transition, .. }
                        | AlignmentMove::ModelMove { transition, .. } => {
                            measure.replayer.transition_symbol(*transition).transpose()
                        }
                        AlignmentMove::LogMove { .. } => None,
                    })
                    .collect::<Result<Vec<Symbol>, _>>()?;
                if options.silent_transitions == SilentTransitions::AsActivities {
                    measure.replayer.delay_silent_transitions(&mut path);
                }
                measure.add(path.into_iter().map(Some), variant.frequency);
                aligned_traces += variant.frequency;
            }
            Err(
                error @ (AlignmentError::SearchError(_)
                | AlignmentError::SyncProdNetConstructionFailed(
                    SyncProdNetConstructionError::TraceTooLong(_),
                )),
            ) => {
                measure.unaligned_traces += variant.frequency;
                first_error.get_or_insert(error);
            }
            Err(AlignmentError::SyncProdNetConstructionFailed(error)) => {
                return Err(error.clone().into())
            }
        }
        counter.tick();
    }
    ctl.check()?;
    if aligned_traces == 0 && measure.unaligned_traces > 0 {
        if let Some(error) = first_error {
            return Err(PrecisionError::NoTraceAligned(error.clone()));
        }
    }
    Ok(measure.finish())
}

/// The prefix tree under construction, and what was left out of it
#[derive(Debug)]
struct Measure<'a> {
    options: &'a EscapingEdgesOptions,
    replayer: Replayer,
    tree: PrefixTree,
    unaligned_traces: u64,
    truncated_traces: u64,
}

impl<'a> Measure<'a> {
    fn new(
        net: &AcceptingPetriNet,
        options: &'a EscapingEdgesOptions,
    ) -> Result<Self, PrecisionError> {
        let replayer = Replayer::new(net, options)?;
        Ok(Self {
            options,
            tree: PrefixTree::new(&replayer),
            replayer,
            unaligned_traces: 0,
            truncated_traces: 0,
        })
    }

    fn add(&mut self, path: impl IntoIterator<Item = Option<Symbol>>, weight: u64) {
        if self.tree.insert(path, weight, &mut self.replayer) == Inserted::Truncated {
            self.truncated_traces += weight;
        }
    }

    fn finish(mut self) -> EscapingEdgesPrecision {
        let totals = self.tree.totals(&mut self.replayer, self.options);
        let escaping_edges = self
            .options
            .report_escaping_edges
            .then(|| self.tree.escaping_edges(&mut self.replayer, self.options));
        let precision = if totals.available == 0 {
            1.0
        } else {
            (totals.available - totals.escaping) as f64 / totals.available as f64
        };
        EscapingEdgesPrecision {
            precision,
            weighted_available: totals.available,
            weighted_escaping: totals.escaping,
            num_states: totals.states,
            unaligned_traces: self.unaligned_traces,
            truncated_traces: self.truncated_traces,
            search_limit_reached: self.replayer.limit_reached,
            escaping_edges,
        }
    }
}

/// How silent transitions take part in the measure
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum SilentTransitions {
    /// Activities are the symbols. An activity is available in a state if some sequence of
    /// silent transitions enables it.
    #[default]
    Skip,
    /// Every transition, silent ones included, is a symbol of its own, and only directly
    /// enabled transitions are available. Silent transitions are moved behind
    /// every following transition that does not consume what they produce, so the result does
    /// not depend on where the alignment placed them.
    AsActivities,
}

/// How traces that end in a state count there
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum TraceEnds {
    /// They do not count towards the weight of the state
    Ignore,
    /// They count towards the weight of the state, so a model that could continue where the log
    /// stops is penalized there
    #[default]
    Weigh,
    /// Like `Weigh`, and stopping is an activity of its own: the model allows it where the final
    /// marking can be reached through silent transitions, and the log takes it where traces end
    AsActivity,
}

/// One step of a path through the model: an activity, or a transition when
/// [`SilentTransitions::AsActivities`] is used
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
pub enum PathStep {
    /// An activity (transition label)
    Activity(String),
    /// A transition, silent or not
    Transition(TransitionID),
    /// The end of a trace, with [`TraceEnds::AsActivity`]
    End,
}

/// Error when computing precision
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum PrecisionError {
    /// The net cannot be replayed or aligned
    InvalidNet(SyncProdNetConstructionError),
    /// The options contradict each other or the chosen entry point
    IncompatibleOptions(String),
    /// An alignment fires a transition that is not in the net, e.g., because it was computed for
    /// another net
    UnknownTransition(TransitionID),
    /// No trace could be aligned, so there is nothing to measure. Holds the error of one of them.
    NoTraceAligned(AlignmentError),
    /// The computation was cancelled through its [`Control`]
    Cancelled,
}

impl std::fmt::Display for PrecisionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidNet(error) => write!(f, "invalid Petri net: {error}"),
            Self::IncompatibleOptions(reason) => write!(f, "incompatible options: {reason}"),
            Self::UnknownTransition(id) => {
                write!(f, "transition {} is not in the net", id.index())
            }
            Self::NoTraceAligned(error) => write!(f, "no trace could be aligned: {error}"),
            Self::Cancelled => write!(f, "{Cancelled}"),
        }
    }
}

impl std::error::Error for PrecisionError {}

impl From<SyncProdNetConstructionError> for PrecisionError {
    fn from(error: SyncProdNetConstructionError) -> Self {
        Self::InvalidNet(error)
    }
}

impl From<Cancelled> for PrecisionError {
    fn from(_: Cancelled) -> Self {
        Self::Cancelled
    }
}
