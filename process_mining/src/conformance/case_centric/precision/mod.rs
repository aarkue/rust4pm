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
use macros_process_mining::register_binding;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    conformance::alignments::{
        align_log, align_variants_with_retry, petri_net::AlignmentError,
        sync_prod_net::SyncProdNetConstructionError, AlignmentMove, AlignmentOptions,
        VariantAlignmentResult,
    },
    core::{
        event_data::case_centric::utils::activity_projection::EventLogActivityProjection,
        process_models::petri_net::TransitionID,
    },
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
    /// [`EscapingEdgesPrecision::unaligned_traces`].
    Aligned {
        /// How to align the variants
        options: AlignmentOptions,
        /// Whether to align the variants that ran out of their memory share again, one at a time
        /// with the whole budget (as [`align_variants_with_retry`] does). This can take long, and
        /// they may still run out.
        #[serde(default)]
        retry_out_of_memory: bool,
    },
    /// The traces as recorded, each up to its first event the model cannot replay (as in the
    /// original escaping-edges precision and in pm4py)
    ReplayUntilDeviation,
}

impl Default for LogPaths {
    fn default() -> Self {
        Self::Aligned {
            options: AlignmentOptions::default(),
            retry_out_of_memory: false,
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
    /// Whether to count escaping edges in the initial state. `ProM` does not, and only counts the
    /// activities the traces start with as available there.
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
#[register_binding(stringify_error)]
pub fn escaping_edges_precision(
    net: &AcceptingPetriNet,
    log: &EventLogActivityProjection,
    #[bind(default)] options: &EscapingEdgesOptions,
) -> Result<EscapingEdgesPrecision, PrecisionError> {
    match &options.log_paths {
        LogPaths::Aligned {
            options: alignment_options,
            retry_out_of_memory,
        } => {
            let alignments = if *retry_out_of_memory {
                align_variants_with_retry(net, log, alignment_options)
            } else {
                align_log(net, log, alignment_options)
            };
            escaping_edges_precision_from_alignments(net, &alignments, options)
        }
        LogPaths::ReplayUntilDeviation => replay_precision(net, log, options),
    }
}

fn replay_precision(
    net: &AcceptingPetriNet,
    log: &EventLogActivityProjection,
    options: &EscapingEdgesOptions,
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
    for (trace, frequency) in &log.traces {
        measure.add(trace.iter().map(|&activity| symbols[activity]), *frequency);
    }
    Ok(measure.finish())
}

/// Escaping-edges precision of `net` with respect to the log behind `alignments`
///
/// Reuses alignments computed for `net` (e.g., for fitness). Requires [`LogPaths::Aligned`],
/// whose fields are then not used.
#[register_binding(stringify_error)]
pub fn escaping_edges_precision_from_alignments(
    net: &AcceptingPetriNet,
    alignments: &[VariantAlignmentResult],
    #[bind(default)] options: &EscapingEdgesOptions,
) -> Result<EscapingEdgesPrecision, PrecisionError> {
    if matches!(options.log_paths, LogPaths::ReplayUntilDeviation) {
        return Err(PrecisionError::IncompatibleOptions(
            "precision from alignments requires `LogPaths::Aligned`".to_string(),
        ));
    }
    let mut measure = Measure::new(net, options)?;
    for variant in alignments {
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
            }
            Err(
                AlignmentError::SearchError(_)
                | AlignmentError::SyncProdNetConstructionFailed(
                    SyncProdNetConstructionError::TraceTooLong(_),
                ),
            ) => measure.unaligned_traces += variant.frequency,
            Err(AlignmentError::SyncProdNetConstructionFailed(error)) => {
                return Err(error.clone().into())
            }
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
    /// enabled transitions are available (as in `ProM`). Silent transitions are moved behind
    /// every following transition that does not consume what they produce, so the result does
    /// not depend on where the alignment placed them.
    AsActivities,
}

/// How traces that end in a state count there
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum TraceEnds {
    /// They do not count towards the weight of the state (as in pm4py)
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
}

impl std::fmt::Display for PrecisionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidNet(error) => write!(f, "invalid Petri net: {error}"),
            Self::IncompatibleOptions(reason) => write!(f, "incompatible options: {reason}"),
            Self::UnknownTransition(id) => {
                write!(f, "transition {} is not in the net", id.index())
            }
        }
    }
}

impl std::error::Error for PrecisionError {}

impl From<SyncProdNetConstructionError> for PrecisionError {
    fn from(error: SyncProdNetConstructionError) -> Self {
        Self::InvalidNet(error)
    }
}
