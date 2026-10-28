//! The Inductive Miner (IM) and its infrequent variant (`IMf`), following §6.1 and §6.2.2 of
//! Sander J. J. Leemans, *Robust Process Mining with Guarantees*, LNBIP 440, Springer, 2022,
//! <https://doi.org/10.1007/978-3-030-96655-3>. Section numbers in this module refer to that book.
//!
//! For a log it tries the base cases, then cut detection with a log split and
//! recursion, and finally the [fall throughs](fallthrough), ending in a flower model. `IMf` adds
//! a deviation threshold `f` to all four steps and gives up the fitness guarantee of plain IM.
//!
//! For the variant that recurses on a directly-follows graph instead of the log, see
//! [`dfg_miner`].
//!
//! ```
//! use process_mining::discovery::case_centric::inductive_miner::{
//!     discover_inductive_tree_from_log, InductiveMinerOptions,
//! };
//! use process_mining::core::event_data::case_centric::EventLogClassifier;
//! use process_mining::event_log;
//!
//! let log = event_log!(["a", "b", "d"], ["a", "c", "d"]);
//! let tree = discover_inductive_tree_from_log(
//!     &log,
//!     &EventLogClassifier::default(),
//!     InductiveMinerOptions::imf(0.2),
//! );
//!
//! assert_eq!(tree.to_string(), "→(a, ×(b, c), d)");
//! ```

use macros_process_mining::register_binding;
use rayon::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::event_data::case_centric::utils::activity_projection::EventLogActivityProjection;
use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::case_centric::petri_net::AcceptingPetriNet;
use crate::core::process_models::process_tree::{Node, OperatorType, ProcessTree};
use crate::EventLog;

use base_cases::{find_base_case, BaseCase};
use cut_finder::find_cut_filtering;
use dfg::{ActivityDfg, NoiseFilter};
use fallthrough::{empty_traces, find_fallthrough, Fallthrough};
use log::{ActivityID, ActivityLog};
use splits::split_log;

pub(crate) mod base_cases;
pub(crate) mod cut_finder;
pub mod dfg;
pub mod dfg_miner;
pub mod fallthrough;
pub mod log;
pub(crate) mod splits;
pub(crate) mod structures;

pub use dfg_miner::{discover_inductive_tree_from_dfg, InductiveMinerDfgOptions};

/// Settings of the Inductive Miner. Each constructor sets up one variant:
/// - [`default`](Self::default): IM
/// - [`imf`](Self::imf): `IMf` with the given noise threshold
/// - [`ima`](Self::ima): IM with the cuts of `IMa`
/// - [`im_thesis`](Self::im_thesis) and [`imf_thesis`](Self::imf_thesis): IM and `IMf` as
///   published, without the refinements of this implementation
/// - [`prom`](Self::prom) and [`pm4py`](Self::pm4py): the settings closest to those tools
///
/// Fields missing in JSON take their default value.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct InductiveMinerOptions {
    /// Deviation threshold `f ∈ [0, 1]` of `IMf`, clamped to that range. `0.0` turns filtering
    /// off.
    pub noise_threshold: f64,
    /// Whether to try the [fall throughs](fallthrough) before resorting to a flower model.
    pub use_fallthroughs: bool,
    /// Whether to merge the parts of a sequence cut that only occur together with a skippable
    /// neighbor.
    pub strict_sequence: bool,
    /// Whether to look for interleaved (`↔`) cuts, tried before concurrency. Under `IMf` they still
    /// read the unfiltered traces, so noise can prevent them.
    pub use_interleaved: bool,
    /// Whether to regroup concurrent cuts into nested concurrency, optionality and inclusive
    /// choice (`∨`).
    pub use_inclusive_choice: bool,
    /// Whether to refuse a cut whose split leaves a part without any events, which only the
    /// filtering splits of `IMf` can do, and fall through instead. Otherwise the cut is taken and
    /// the part becomes `τ`, as in the thesis.
    pub guard_empty_cut_parts: bool,
    /// Whether concurrency detection consults the minimum-self-distance relation (∧↔.1), which
    /// tells `a` and `b` running concurrently from `a` looping around `b`. A concurrent cut that
    /// keeps such activities together is preferred. If there is none, the plain one is taken.
    pub use_minimum_self_distance: bool,
    /// Whether to take a tau loop that cuts every trace into single events, as the thesis does.
    /// Mining those single events gives the flower model, so this only decides whether
    /// [`two_activities_concurrent`](fallthrough::two_activities_concurrent) gets a chance first.
    pub use_degenerate_tau_loops: bool,
    /// Whether `IMf` keeps the single-activity base case despite repetitions when single
    /// executions dominate (§6.2.2.3). Without it any repetition ends the recursion in a loop.
    pub filter_single_activity: bool,
    /// Whether the [activity-concurrent fall through](fallthrough::activity_concurrent) also
    /// accepts a candidate whose remaining log only has a cut after filtering. The thesis
    /// (§6.2.2.4) probes the unfiltered graph.
    pub filter_activity_concurrent_probe: bool,
    /// Whether `IMf` keeps an edge, a start or end activity, or the empty traces, when their
    /// frequency is exactly at the threshold.
    pub keep_at_threshold: bool,
    /// Whether `IMf` also removes infrequent start and end activities.
    pub filter_start_end: bool,
    /// Whether the [activity-concurrent fall through](fallthrough::activity_concurrent) tries the
    /// activities by name instead of the most frequent first. It takes the first whose removal
    /// leaves a cut.
    pub activity_concurrent_by_name: bool,
    /// Whether to [fold](crate::core::process_models::process_tree::Node::fold) the tree, e.g.,
    /// `→(→(a, b), c)` into `→(a, b, c)` and `→(τ, a)` into `a`.
    pub fold: bool,
}

impl Default for InductiveMinerOptions {
    /// The Inductive Miner without filtering, with the steps that measured as improvements over
    /// the thesis switched on. See [`im_thesis`](Self::im_thesis) for the definition as published.
    /// None of those steps gives up behavior, so the model still fits the log.
    fn default() -> Self {
        Self {
            noise_threshold: 0.0,
            use_fallthroughs: true,
            strict_sequence: true,
            use_interleaved: false,
            use_inclusive_choice: false,
            guard_empty_cut_parts: false,
            use_minimum_self_distance: true,
            use_degenerate_tau_loops: false,
            filter_single_activity: true,
            filter_activity_concurrent_probe: false,
            keep_at_threshold: true,
            filter_start_end: true,
            activity_concurrent_by_name: false,
            fold: true,
        }
    }
}

impl InductiveMinerOptions {
    /// Inductive Miner - infrequent with the given deviation threshold, clamped to `[0, 1]`, and
    /// the improvements of [`default`](Self::default). `0.2` is a reasonable starting point for
    /// real-life logs.
    pub fn imf(noise_threshold: f64) -> Self {
        Self {
            noise_threshold: noise_threshold.clamp(0.0, 1.0),
            ..Self::default()
        }
    }

    /// Plain IM as published (§6.1), with none of the later refinements.
    ///
    /// Differs from [`default`](Self::default) in two places: sequence cuts are the maximal ones
    /// of →.1, and a tau loop is taken even when it degenerates into the flower model.
    pub fn im_thesis() -> Self {
        Self {
            strict_sequence: false,
            use_degenerate_tau_loops: true,
            ..Self::default()
        }
    }

    /// `IMf` as published (§6.2.2), the [thesis version](Self::im_thesis) of
    /// [`imf`](Self::imf).
    pub fn imf_thesis(noise_threshold: f64) -> Self {
        Self {
            noise_threshold: noise_threshold.clamp(0.0, 1.0),
            ..Self::im_thesis()
        }
    }

    /// [`default`](Self::default) with the interleaved and inclusive choice cuts of `IMa`
    /// (§6.4.2). Set a [`noise_threshold`](Self::noise_threshold) for `IMfa`.
    pub fn ima() -> Self {
        Self {
            use_interleaved: true,
            use_inclusive_choice: true,
            ..Self::default()
        }
    }

    /// The settings under which this implementation behaves most like `ProM`'s IM.
    /// The same as [`imf`](Self::imf): `ProM` also applies the strict sequence cut and the
    /// minimum-self-distance relation.
    pub fn prom(noise_threshold: f64) -> Self {
        Self::imf(noise_threshold)
    }

    /// The settings under which this implementation behaves most like `PM4Py`'s IM.
    /// Like [`prom`](Self::prom), except that `PM4Py`
    /// - has no minimum-self-distance relation,
    /// - omits the single-activity filtering of `IMf`, so a repeated activity always becomes a
    ///   loop,
    /// - also removes what is exactly at the threshold, but never start and end activities,
    /// - tries the activities of the activity-concurrent fall through by name.
    pub fn pm4py(noise_threshold: f64) -> Self {
        Self {
            use_minimum_self_distance: false,
            filter_single_activity: false,
            keep_at_threshold: false,
            filter_start_end: false,
            activity_concurrent_by_name: true,
            ..Self::prom(noise_threshold)
        }
    }

    /// The filter `IMf` applies to a graph before retrying cut detection.
    pub(crate) fn noise_filter(&self) -> NoiseFilter {
        NoiseFilter {
            threshold: self.noise_threshold,
            keep_at_threshold: self.keep_at_threshold,
            filter_start_end: self.filter_start_end,
        }
    }
}

/// An Inductive Miner with its settings: one that recurses on the log, or the
/// [directly-follows variant](dfg_miner).
///
/// In JSON, `kind` tells them apart, e.g., `{"kind": "log", "noise_threshold": 0.2}` or
/// `{"kind": "dfg"}`, and fields left out take their default value.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InductiveMiner {
    /// IM, `IMf` or `IMa`, which recurse on the log.
    Log(InductiveMinerOptions),
    /// `IMd` or `IMfd`, which recurse on the directly-follows graph.
    Dfg(InductiveMinerDfgOptions),
}

impl Default for InductiveMiner {
    /// Plain IM with the [default](InductiveMinerOptions::default) settings.
    fn default() -> Self {
        Self::Log(InductiveMinerOptions::default())
    }
}

impl From<InductiveMinerOptions> for InductiveMiner {
    fn from(options: InductiveMinerOptions) -> Self {
        Self::Log(options)
    }
}

impl From<InductiveMinerDfgOptions> for InductiveMiner {
    fn from(options: InductiveMinerDfgOptions) -> Self {
        Self::Dfg(options)
    }
}

/// The published variants of the Inductive Miner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum InductiveMinerVariant {
    /// IM: the model fits the log.
    #[serde(rename = "IM")]
    Im,
    /// `IMf`: filters infrequent behavior.
    #[serde(rename = "IMf")]
    Imf,
    /// `IMa`: adds the interleaved and inclusive choice cuts. With a noise threshold, it is `IMfa`.
    #[serde(rename = "IMa")]
    Ima,
    /// `IMd`: recurses on the directly-follows graph, for large logs.
    #[serde(rename = "IMd")]
    Imd,
    /// `IMfd`: `IMd` with filtering.
    #[serde(rename = "IMfd")]
    Imfd,
}

/// Whose Inductive Miner a run follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InductiveMinerPreset {
    /// This implementation's, with the refinements over the thesis that measured as improvements.
    #[default]
    Default,
    /// The algorithms as published.
    Thesis,
    /// `ProM`'s.
    Prom,
    /// `PM4Py`'s.
    Pm4py,
}

impl InductiveMinerVariant {
    /// The variant's settings under a preset, e.g., for a host to offer in a menu. The threshold is
    /// clamped to `[0, 1]`, and IM and `IMd` ignore it.
    pub fn options(self, preset: InductiveMinerPreset, noise_threshold: f64) -> InductiveMiner {
        use InductiveMinerPreset as P;
        let threshold = match self {
            Self::Im | Self::Imd => 0.0,
            Self::Imf | Self::Ima | Self::Imfd => noise_threshold,
        };
        match self {
            Self::Im | Self::Imf | Self::Ima => {
                let options = match preset {
                    P::Default => InductiveMinerOptions::imf(threshold),
                    P::Thesis => InductiveMinerOptions::imf_thesis(threshold),
                    P::Prom => InductiveMinerOptions::prom(threshold),
                    P::Pm4py => InductiveMinerOptions::pm4py(threshold),
                };
                InductiveMiner::Log(if self == Self::Ima {
                    InductiveMinerOptions {
                        use_interleaved: true,
                        use_inclusive_choice: true,
                        ..options
                    }
                } else {
                    options
                })
            }
            Self::Imd | Self::Imfd => InductiveMiner::Dfg(match preset {
                P::Default | P::Thesis => InductiveMinerDfgOptions::imfd(threshold),
                P::Prom => InductiveMinerDfgOptions::prom(threshold),
                P::Pm4py => InductiveMinerDfgOptions::pm4py(threshold),
            }),
        }
    }
}

/// The settings of an Inductive Miner variant under a preset. See
/// [`InductiveMinerVariant::options`].
#[register_binding]
pub fn inductive_miner_options(
    variant: InductiveMinerVariant,
    #[bind(default)] preset: InductiveMinerPreset,
    #[bind(default)] noise_threshold: f64,
) -> InductiveMiner {
    variant.options(preset, noise_threshold)
}

/// Counts how often the exhaustive check in [`Miner::verify_cut_detection`] ran, so a test can
/// tell that it did rather than passing vacuously.
#[cfg(test)]
pub(crate) static EXHAUSTIVE_CHECKS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// What [`Miner::step`] found for a log.
enum Step {
    Done(Node),
    /// An activity-concurrent fall through, which leaves the rest for [`Miner::mine`] to go on
    /// with.
    TakeOut {
        activity_log: ActivityLog,
        rest: ActivityLog,
    },
}

/// What the recursion needs besides the current log.
struct Miner<'a> {
    /// Activity labels, indexed by [`ActivityID`].
    labels: &'a [String],
    options: &'a InductiveMinerOptions,
}

/// Discovers an [`AcceptingPetriNet`] with a variant of the Inductive Miner, `IMf` at `0.2` by
/// default.
///
/// The threshold applies to `IMf`, `IMa` and `IMfd`, and the preset says whose behavior to follow
/// (see [`InductiveMinerVariant::options`]). For other settings, use
/// [`discover_inductive_net_with`].
#[register_binding]
pub fn discover_inductive_net(
    log: &EventLogActivityProjection,
    #[bind(default = InductiveMinerVariant::Imf)] variant: InductiveMinerVariant,
    #[bind(default = 0.2)] noise_threshold: f64,
    #[bind(default)] preset: InductiveMinerPreset,
) -> AcceptingPetriNet {
    discover_inductive_net_with(log, variant.options(preset, noise_threshold))
}

/// Discovers a [`ProcessTree`] with a variant of the Inductive Miner. See
/// [`discover_inductive_net`].
#[register_binding]
pub fn discover_inductive_tree(
    log: &EventLogActivityProjection,
    #[bind(default = InductiveMinerVariant::Imf)] variant: InductiveMinerVariant,
    #[bind(default = 0.2)] noise_threshold: f64,
    #[bind(default)] preset: InductiveMinerPreset,
) -> ProcessTree {
    discover_inductive_tree_with(log, variant.options(preset, noise_threshold))
}

/// Discovers a [`ProcessTree`] with the Inductive Miner.
///
/// See [`InductiveMiner`] for the variants and their settings, and the
/// [module documentation](self) for an example. To read activities from an [`EventLog`] with a
/// classifier, use [`discover_inductive_tree_from_log`].
#[register_binding]
pub fn discover_inductive_tree_with(
    log: &EventLogActivityProjection,
    #[bind(default)] miner: InductiveMiner,
) -> ProcessTree {
    let (labels, activity_log) = log::from_activity_projection(log);
    mine(&labels, &activity_log, miner)
}

/// Discovers an [`AcceptingPetriNet`] with the Inductive Miner, by converting the discovered
/// [`ProcessTree`] with [`ProcessTree::to_petri_net`].
#[register_binding]
pub fn discover_inductive_net_with(
    log: &EventLogActivityProjection,
    #[bind(default)] miner: InductiveMiner,
) -> AcceptingPetriNet {
    discover_inductive_tree_with(log, miner).to_petri_net()
}

/// Discovers a [`ProcessTree`] from an event log with the Inductive Miner, where the classifier
/// decides what counts as an activity, e.g., `&EventLogClassifier::default()` for
/// `concept:name`.
pub fn discover_inductive_tree_from_log(
    event_log: &EventLog,
    classifier: &EventLogClassifier,
    miner: impl Into<InductiveMiner>,
) -> ProcessTree {
    let (labels, activity_log) = log::project_event_log(event_log, classifier);
    mine(&labels, &activity_log, miner.into())
}

fn mine(labels: &[String], activity_log: &ActivityLog, miner: InductiveMiner) -> ProcessTree {
    match miner {
        InductiveMiner::Log(options) => discover(labels, activity_log, options),
        InductiveMiner::Dfg(options) => {
            dfg_miner::discover_dfg(labels, &ActivityDfg::discover(activity_log), options)
        }
    }
}

/// Runs the recursion and wraps the result up as a [`ProcessTree`].
fn discover(
    labels: &[String],
    activity_log: &ActivityLog,
    options: InductiveMinerOptions,
) -> ProcessTree {
    let options = InductiveMinerOptions {
        noise_threshold: options.noise_threshold.clamp(0.0, 1.0),
        ..options
    };
    let miner = Miner {
        labels,
        options: &options,
    };
    let tree = ProcessTree::new(miner.mine(activity_log));

    if options.fold {
        tree.fold()
    } else {
        tree
    }
}

impl Miner<'_> {
    /// Discovers the sub-tree for one (sub-)log.
    ///
    /// Mines the rest of an activity-concurrent fall through in a loop to keep the stack flat.
    fn mine(&self, log: &ActivityLog) -> Node {
        let mut taken_out = Vec::new();
        let mut rest = None;
        let last = loop {
            match self.step(rest.as_ref().unwrap_or(log)) {
                Step::Done(node) => break node,
                Step::TakeOut {
                    activity_log,
                    rest: next,
                } => {
                    taken_out.push(self.mine(&activity_log));
                    rest = Some(next);
                }
            }
        };
        if taken_out.is_empty() {
            last
        } else if self.options.fold {
            // What folding the nested concurrency gives, without building it first.
            taken_out.push(last);
            operator(OperatorType::Concurrency, taken_out)
        } else {
            taken_out.into_iter().rev().fold(last, |inner, node| {
                operator(OperatorType::Concurrency, [node, inner])
            })
        }
    }

    /// Base cases, cut detection or a fall through for one log.
    fn step(&self, log: &ActivityLog) -> Step {
        // Base cases and cut detection are not defined for a log with empty traces.
        if let Some(fallthrough) = empty_traces(log, self.options) {
            return Step::Done(self.build(fallthrough));
        }

        let base_case_threshold = if self.options.filter_single_activity {
            self.options.noise_threshold
        } else {
            0.0
        };
        match find_base_case(log, base_case_threshold) {
            Some(BaseCase::EmptyLog) => return Step::Done(tau()),
            Some(BaseCase::SingleActivity(activity)) => {
                return Step::Done(leaf(self.labels, activity))
            }
            None => {}
        }

        let dfg = ActivityDfg::discover(log);
        let cut = find_cut_filtering(log, &dfg, self.options);

        #[cfg(test)]
        self.verify_cut_detection(log, &dfg, cut.as_ref());

        if let Some(cut) = cut {
            let sub_logs = split_log(log, &cut);
            // A part without events would become τ and lose its activities. Falling through keeps
            // them.
            let usable = !self.options.guard_empty_cut_parts
                || sub_logs.iter().all(|sub_log| sub_log.num_events() > 0);

            if usable {
                return Step::Done(operator(cut.operator(), self.mine_all(&sub_logs)));
            }
        }

        match find_fallthrough(log, &dfg, self.options) {
            Fallthrough::ActivityConcurrent { activity_log, rest } => {
                Step::TakeOut { activity_log, rest }
            }
            fallthrough => Step::Done(self.build(fallthrough)),
        }
    }

    /// Discovers the sub-trees of a list of sub-logs, in order, in parallel if they are large
    /// enough.
    fn mine_all(&self, sub_logs: &[ActivityLog]) -> Vec<Node> {
        /// Total events below which the sub-logs are mined on the current thread.
        const PARALLEL_THRESHOLD: u64 = 2_048;

        let worth_parallelizing = sub_logs.len() > 1
            && sub_logs.iter().map(ActivityLog::num_events).sum::<u64>() > PARALLEL_THRESHOLD;

        if worth_parallelizing {
            sub_logs
                .par_iter()
                .map(|sub_log| self.mine(sub_log))
                .collect()
        } else {
            sub_logs.iter().map(|sub_log| self.mine(sub_log)).collect()
        }
    }

    /// Checks what cut detection decided against an exhaustive search of every possible cut.
    ///
    /// Asserts that the cut taken adheres to its footprint, that no earlier cut type was also
    /// available, and that where the miner fell through no cut existed at all.
    ///
    /// Only meaningful without filtering and without
    /// [`strict_sequence`](InductiveMinerOptions::strict_sequence). Skipped on graphs too large to
    /// enumerate.
    #[cfg(test)]
    fn verify_cut_detection(
        &self,
        log: &ActivityLog,
        dfg: &ActivityDfg,
        cut: Option<&cut_finder::cut::Cut>,
    ) {
        use cut_finder::cut::{precedence, Cut};
        use cut_finder::strict_sequence_cut;
        use cut_finder::test_utils::{all_valid_cuts, footprint_violation};
        use structures::minimum_self_distance::MinimumSelfDistance;

        if self.options.noise_threshold > 0.0 {
            return;
        }

        let describe = |cut: &Cut| {
            let parts: Vec<Vec<&str>> = cut
                .partitions()
                .iter()
                .map(|part| part.iter().map(|&a| self.labels[a].as_str()).collect())
                .collect();
            format!("{:?}{parts:?}", cut.operator())
        };

        if let Some(cut) = cut {
            if let Some(problem) = footprint_violation(log, dfg, cut) {
                panic!(
                    "{} does not adhere to its footprint: {problem}",
                    describe(cut)
                );
            }
        }

        // The oracle needs the same minimum-self-distance restriction the miner applies,
        // otherwise it reports concurrent cuts the miner is right to pass over.
        let minimum_self_distance = self
            .options
            .use_minimum_self_distance
            .then(|| MinimumSelfDistance::discover(log));
        let Some(mut possible) = all_valid_cuts(log, dfg, minimum_self_distance.as_ref()) else {
            return;
        };
        EXHAUSTIVE_CHECKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // The strict sequence cut coarsens the maximal sequence cut, or rejects it if that leaves
        // a single part.
        let maximal_sequence = possible
            .iter()
            .filter(|other| other.operator() == OperatorType::Sequence)
            .max_by_key(|other| other.len())
            .cloned();
        if let (true, Some(maximal)) = (self.options.strict_sequence, &maximal_sequence) {
            if strict_sequence_cut(dfg, maximal.clone()).is_none() {
                possible.retain(|other| other.operator() != OperatorType::Sequence);
            }
        }

        match cut {
            None => assert!(
                possible.is_empty(),
                "fell through although {} cut(s) exist, e.g. {}",
                possible.len(),
                describe(&possible[0])
            ),
            Some(cut) => {
                if let Some(earlier) = possible
                    .iter()
                    .find(|other| precedence(other.operator()) < precedence(cut.operator()))
                {
                    panic!(
                        "took {} although the earlier {} was also available",
                        describe(cut),
                        describe(earlier)
                    );
                }

                if let (true, OperatorType::Sequence, Some(maximal)) = (
                    self.options.strict_sequence,
                    cut.operator(),
                    &maximal_sequence,
                ) {
                    let mut finer = maximal.partitions().iter();
                    for part in cut.partitions() {
                        let mut covered: Vec<ActivityID> = Vec::new();
                        while covered.len() < part.len() {
                            covered.extend(finer.next().expect("more parts than the maximal cut"));
                        }
                        covered.sort_unstable();
                        assert_eq!(
                            &covered,
                            part,
                            "{} does not merge neighbors of {}",
                            describe(cut),
                            describe(maximal)
                        );
                    }
                    return;
                }

                // The oracle does not enumerate interleaved cuts, and `coo_cut` merges the parts of
                // a concurrent cut on purpose.
                let regrouped = self.options.use_inclusive_choice
                    && matches!(
                        cut.operator(),
                        OperatorType::Concurrency | OperatorType::InclusiveChoice
                    );
                if cut.operator() == OperatorType::Interleaving || regrouped {
                    return;
                }

                let operator = cut.operator();
                let most_parts = possible
                    .iter()
                    .filter(|other| other.operator() == operator)
                    .map(|other| other.len())
                    .max()
                    .unwrap_or(0);
                assert_eq!(
                    cut.len(),
                    most_parts,
                    "took {}, but a {operator:?} cut with {most_parts} parts exists (not maximal)",
                    describe(cut)
                );
            }
        }
    }

    /// Builds the sub-tree a fall through decided on, recursing where it asked for it.
    fn build(&self, fallthrough: Fallthrough) -> Node {
        match fallthrough {
            // Avoids `×(τ, τ)`.
            Fallthrough::OptionalEmptyTraces(rest) if rest.is_empty() => tau(),
            Fallthrough::OptionalEmptyTraces(rest) => {
                operator(OperatorType::ExclusiveChoice, [tau(), self.mine(&rest)])
            }
            Fallthrough::DroppedEmptyTraces(rest) => self.mine(&rest),
            Fallthrough::ActivityConcurrent { activity_log, rest } => operator(
                OperatorType::Concurrency,
                [self.mine(&activity_log), self.mine(&rest)],
            ),
            Fallthrough::TauLoop(body) => operator(OperatorType::Loop, [self.mine(&body), tau()]),
            Fallthrough::FlowerModel(activities) => {
                let flower = choice(self.labels, &activities);
                operator(OperatorType::Loop, [flower, tau()])
            }
        }
    }
}

fn tau() -> Node {
    Node::new_leaf(None)
}

fn leaf(labels: &[String], activity: ActivityID) -> Node {
    Node::new_leaf(Some(labels[activity].clone()))
}

fn operator(operator: OperatorType, children: impl IntoIterator<Item = Node>) -> Node {
    let mut node = Node::new_operator(operator);
    for child in children {
        node.add_child(child);
    }
    node
}

/// A choice between the given activities, or the activity itself if there is only one.
fn choice(labels: &[String], activities: &[ActivityID]) -> Node {
    match activities {
        [] => tau(),
        [activity] => leaf(labels, *activity),
        _ => operator(
            OperatorType::ExclusiveChoice,
            activities.iter().map(|&a| leaf(labels, a)),
        ),
    }
}

#[cfg(test)]
mod test_inductive_miner {
    use super::log::test_utils::log_of;
    use super::*;
    use crate::event_log;

    /// Mines a log given as activity names and renders the tree, e.g. `"→(a, b)"`.
    fn mine(traces: &[&[&str]]) -> String {
        mine_with(traces, InductiveMinerOptions::default())
    }

    fn mine_with(traces: &[&[&str]], options: InductiveMinerOptions) -> String {
        let (labels, log) = log_of(traces);
        let tree = discover(&labels, &log, options);
        assert!(tree.is_valid(), "discovered an invalid tree: {tree}");
        tree.to_string()
    }

    #[test]
    fn test_base_cases_and_operators() {
        for (traces, expected) in [
            (&[] as &[&[&str]], "tau"),
            (&[&[], &[]], "tau"),
            (&[&["a"], &["a"]], "a"),
            (&[&["a", "b", "c"]], "→(a, b, c)"),
            (&[&["a"], &["b"]], "×(a, b)"),
            (&[&["a", "b"], &["b", "a"]], "∧(a, b)"),
            (&[&["a", "c"], &["a", "c", "b", "a", "c"]], "↺(→(a, c), b)"),
            (&[&["a", "a"]], "↺(a, tau)"),
            (&[&["a"], &[]], "×(tau, a)"),
            (&[&[], &["a", "a"]], "×(tau, ↺(a, tau))"),
            (&[&["a", "b", "d"], &["a", "c", "d"]], "→(a, ×(b, c), d)"),
            (
                &[&["a", "b", "c", "d"], &["a", "c", "b", "d"]],
                "→(a, ∧(b, c), d)",
            ),
        ] {
            assert_eq!(mine(traces), expected, "for {traces:?}");
        }
    }

    /// The running example L81, for which the activity-once-per-trace fall through takes d
    /// out, and the same log with the fall throughs disabled.
    #[test]
    fn test_fall_throughs() {
        let l81: &[&[&str]] = &[
            &["a", "b", "c", "d"],
            &["d", "a", "b"],
            &["a", "d", "c"],
            &["b", "c", "d"],
        ];
        assert_eq!(mine(l81), "∧(d, →(×(tau, a), ×(tau, b), ×(tau, c)))");
        assert_eq!(
            mine_with(
                l81,
                InductiveMinerOptions {
                    use_fallthroughs: false,
                    ..InductiveMinerOptions::default()
                }
            ),
            "↺(×(a, b, c, d), tau)"
        );
    }

    #[test]
    fn test_folding_can_be_disabled() {
        // Cut detection reports maximal cuts, so nesting comes from the fall throughs: the empty
        // trace produces an XOR whose second child is the XOR between a and b.
        let traces: &[&[&str]] = &[&[], &["a"], &["b"]];
        assert_eq!(mine(traces), "×(tau, a, b)");
        assert_eq!(
            mine_with(
                traces,
                InductiveMinerOptions {
                    fold: false,
                    ..InductiveMinerOptions::default()
                }
            ),
            "×(tau, ×(a, b))"
        );
    }

    #[test]
    fn test_imf_ignores_infrequent_behavior() {
        // A clean sequence a, b, c plus one trace in which c came first. That trace closes the
        // cycle a to b to c to a, leaving IM without a cut, so it falls through to taking c out as
        // concurrent to the sequence that is left.
        let (labels, base) = log_of(&[&["a", "b", "c"]]);
        let log = base.derive([(vec![0, 1, 2], 100), (vec![2, 0, 1], 1)]);
        assert_eq!(
            discover(&labels, &log, InductiveMinerOptions::im_thesis()).to_string(),
            "∧(c, →(a, b))"
        );
        assert_eq!(
            discover(&labels, &log, InductiveMinerOptions::imf(0.2)).to_string(),
            "→(a, b, c)"
        );

        // A single empty trace among a thousand is noise rather than optionality.
        let (labels, base) = log_of(&[&["a"]]);
        let log = base.derive([(vec![0], 1000), (vec![], 1)]);
        assert_eq!(
            discover(&labels, &log, InductiveMinerOptions::im_thesis()).to_string(),
            "×(tau, a)"
        );
        assert_eq!(
            discover(&labels, &log, InductiveMinerOptions::imf(0.2)).to_string(),
            "a"
        );

        assert_eq!(InductiveMinerOptions::imf(5.0).noise_threshold, 1.0);
        assert_eq!(InductiveMinerOptions::imf(-1.0).noise_threshold, 0.0);
    }

    #[test]
    fn test_strict_sequence_cut() {
        let thesis = InductiveMinerOptions::im_thesis();
        let strict = InductiveMinerOptions {
            strict_sequence: true,
            ..thesis
        };
        // The maximal cut separates b from d, which lets a d happen without a b.
        let traces: &[&[&str]] = &[
            &["c", "c", "c", "a", "c"],
            &["b", "b", "d", "a"],
            &["b", "c"],
        ];
        assert_eq!(
            mine_with(traces, thesis),
            "→(×(tau, ↺(b, tau)), ×(tau, d), ∧(×(tau, a), ×(tau, ↺(c, tau))))"
        );
        // Not the case without the option.
        assert_eq!(
            mine_with(traces, strict),
            "→(×(tau, →(↺(b, tau), ×(tau, d))), ∧(×(tau, a), ×(tau, ↺(c, tau))))"
        );
        // Parts that the log shows apart are still separated.
        assert_eq!(
            mine_with(&[&["a", "b"], &["b", "c"]], strict),
            "→(×(tau, a), b, ×(tau, c))"
        );
    }

    #[test]
    fn test_interleaved_cut() {
        let interleaved = InductiveMinerOptions {
            use_interleaved: true,
            ..InductiveMinerOptions::default()
        };
        // a, b never overlaps c but their order varies, which no directly-follows cut sees: the
        // fall through puts c next to the sequence, the interleaved cut is exact.
        let traces: &[&[&str]] = &[&["a", "b", "c"], &["c", "a", "b"]];
        assert_eq!(mine(traces), "∧(c, →(a, b))");
        assert_eq!(mine_with(traces, interleaved), "↔(→(a, b), c)");

        // Where both apply, the interleaved cut comes first. For single activities the two
        // operators have the same language.
        assert_eq!(
            mine_with(&[&["a", "b"], &["b", "a"]], interleaved),
            "↔(a, b)"
        );

        // The concurrency would also accept ⟨a, c, b⟩.
        let traces: &[&[&str]] = &[
            &["a", "c"],
            &["c", "a"],
            &["b", "c"],
            &["c", "b"],
            &["a", "b", "c"],
            &["c", "a", "b"],
        ];
        assert_eq!(mine(traces), "∧(→(×(tau, a), ×(tau, b)), c)");
        assert_eq!(
            mine_with(traces, interleaved),
            "↔(→(×(tau, a), ×(tau, b)), c)"
        );
    }

    #[test]
    fn test_inclusive_choice_cut() {
        let thesis = InductiveMinerOptions::im_thesis();
        let inclusive = InductiveMinerOptions {
            use_inclusive_choice: true,
            ..thesis
        };
        // a and b are concurrent, but neither has to happen, so the concurrency also allows the
        // empty trace. The log does not, and the inclusive choice says so.
        let traces: &[&[&str]] = &[&["a", "b"], &["b", "a"], &["a"], &["b"]];
        assert_eq!(mine_with(traces, thesis), "∧(×(tau, a), ×(tau, b))");
        assert_eq!(mine_with(traces, inclusive), "∨(a, b)");

        // Every trace runs both parts, so there is nothing to skip and the cut stays a
        // concurrency.
        assert_eq!(mine_with(&[&["a", "b"], &["b", "a"]], inclusive), "∧(a, b)");

        // c is mandatory and only b is ever skipped. An inclusive choice would allow a b without a
        // c, which is more than the concurrency permits, so the cut stays as it is.
        let mandatory: &[&[&str]] = &[&["c"], &["b", "c"], &["c"], &["c", "b", "b"]];
        assert_eq!(
            mine_with(mandatory, inclusive),
            mine_with(mandatory, thesis)
        );
    }

    /// The presets against the trees the two tools actually produce, taken from black-box runs of
    /// their `IMf`.
    #[test]
    fn test_comparison_presets() {
        // ∧↔.1 forbids every concurrent cut, since f sits between the two closest a's. `ProM`
        // then takes the plain one, and `PM4Py` has no ∧↔.1.
        let looping: &[&[&str]] = &[&["a", "c", "f"], &["a", "f", "a"], &["c", "a", "f"]];
        for preset in [
            InductiveMinerOptions::default(),
            InductiveMinerOptions::prom(0.0),
            InductiveMinerOptions::pm4py(0.0),
        ] {
            assert_eq!(mine_with(looping, preset), "∧(↺(a, tau), →(×(tau, c), f))");
        }

        // `PM4Py` omits the single-activity filtering of IMf, so a repetition becomes a loop
        // where the thesis and ProM keep the leaf.
        let (labels, base) = log_of(&[&["a"]]);
        let log = base.derive([(vec![0], 100), (vec![0, 0], 1)]);
        for (preset, expected) in [
            (InductiveMinerOptions::imf(0.2), "a"),
            (InductiveMinerOptions::prom(0.2), "a"),
            (InductiveMinerOptions::pm4py(0.2), "↺(a, tau)"),
        ] {
            assert_eq!(discover(&labels, &log, preset).to_string(), expected);
        }
    }

    #[test]
    fn test_filtering_the_fallthrough_probe_changes_the_tree() {
        let log: &[&[&str]] = &[
            &["a", "b", "b", "b", "b"],
            &["a", "b", "c", "b", "b", "c"],
            &["a", "b", "c", "b", "a", "b"],
        ];

        let imf = InductiveMinerOptions::imf(0.2);
        assert_eq!(mine_with(log, imf), "↺(→(a, ↺(→(b, ×(tau, c)), tau)), tau)");

        // The filtered probe accepts a candidate, and the tree that follows allows `a` only once,
        // so it no longer replays ⟨a, b, c, b, a, b⟩.
        assert_eq!(
            mine_with(
                log,
                InductiveMinerOptions {
                    filter_activity_concurrent_probe: true,
                    ..imf
                }
            ),
            "∧(×(tau, c), →(a, ↺(b, tau)))"
        );
    }

    #[test]
    fn test_entry_points() {
        let log = event_log!(["a", "b"], ["a", "b"]);
        let options = InductiveMinerOptions::default();
        let classifier = EventLogClassifier::default();
        assert_eq!(
            discover_inductive_tree_from_log(&log, &classifier, options).to_string(),
            "→(a, b)"
        );

        let projection: EventLogActivityProjection = (&log).into();
        assert_eq!(
            discover_inductive_tree_with(&projection, options.into()).to_string(),
            "→(a, b)"
        );
    }

    /// Without filtering, the model replays every trace of the log.
    #[test]
    fn test_plain_im_replays_the_log() {
        use crate::conformance::case_centric::alignments::{
            align_log, compute_fitness, AlignmentOptions,
        };
        use crate::core::event_data::case_centric::xes::import_xes::{
            import_xes_path, XESImportOptions,
        };
        use crate::test_utils::get_test_data_path;

        for name in ["RepairExample.xes", "AN1-example.xes"] {
            let path = get_test_data_path().join("xes").join(name);
            let log = import_xes_path(path, XESImportOptions::default()).unwrap();
            // Mine the projection the alignment uses, so both agree on the activities.
            let projection = EventLogActivityProjection::from(&log);
            let tree = discover_inductive_tree_with(&projection, InductiveMiner::default());

            let net = tree.to_petri_net();
            let options = AlignmentOptions::default();
            let alignments = align_log(&net, &projection, &options);
            let fitness = compute_fitness(&alignments, &net, &options).unwrap();
            assert_eq!(fitness.log_fitness, 1.0, "{name} is not replayed by {tree}");
        }
    }

    /// Runs the miner over the real-life test logs, with the footprint and exhaustive checks on.
    #[test]
    fn test_real_life_logs() {
        use crate::core::event_data::case_centric::xes::import_xes::{
            import_xes_path, XESImportOptions,
        };
        use crate::test_utils::get_test_data_path;

        for name in ["RepairExample.xes", "Sepsis Cases - Event Log.xes.gz"] {
            let path = get_test_data_path().join("xes").join(name);
            let log = import_xes_path(path, XESImportOptions::default()).unwrap();

            for noise_threshold in [0.0, 0.2] {
                let tree = discover_inductive_tree_from_log(
                    &log,
                    &EventLogClassifier::default(),
                    InductiveMinerOptions::imf(noise_threshold),
                );
                assert!(tree.is_valid(), "{name} at {noise_threshold}: {tree}");
                assert!(!tree.find_all_leaves().is_empty(), "{name} is empty");
            }
        }
    }

    /// Mines a few thousand random logs, with every cut checked against an exhaustive search of
    /// all possible cuts.
    #[test]
    fn test_cut_detection_against_exhaustive_search() {
        use std::sync::atomic::Ordering;

        // A small deterministic generator, so a failure is always reproducible.
        let mut seed = 0x5eed_1337_u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };

        let before = EXHAUSTIVE_CHECKS.load(Ordering::Relaxed);
        for _ in 0..3000 {
            let alphabet = 2 + (next() % 4) as usize;
            let labels: Vec<String> = (0..alphabet)
                .map(|a| ((b'a' + a as u8) as char).to_string())
                .collect();
            let traces = 1 + (next() % 7) as usize;
            let variants: Vec<(Vec<ActivityID>, u64)> = (0..traces)
                .map(|_| {
                    let length = (next() % 7) as usize;
                    (
                        (0..length)
                            .map(|_| (next() % alphabet as u64) as usize)
                            .collect(),
                        1,
                    )
                })
                .collect();

            let log = ActivityLog::new(alphabet, variants);
            for options in [
                InductiveMinerOptions::im_thesis(),
                InductiveMinerOptions {
                    use_interleaved: true,
                    ..InductiveMinerOptions::im_thesis()
                },
                InductiveMinerOptions {
                    use_inclusive_choice: true,
                    ..InductiveMinerOptions::im_thesis()
                },
                InductiveMinerOptions::default(),
                InductiveMinerOptions::ima(),
            ] {
                let tree = discover(&labels, &log, options);
                assert!(tree.is_valid(), "invalid tree for {:?}", log.variants());
            }
        }

        let checks = EXHAUSTIVE_CHECKS.load(Ordering::Relaxed) - before;
        assert!(
            checks > 3000,
            "the exhaustive check only ran {checks} times"
        );
    }

    #[test]
    fn test_discovery_is_deterministic() {
        // Neither hash-map iteration order nor trace order may leak into the result.
        let traces: &[&[&str]] = &[
            &["a", "b", "c", "d", "e"],
            &["a", "d", "b", "e"],
            &["a", "e", "b"],
            &["a", "c", "b"],
        ];
        let reference = mine(traces);
        for _ in 0..10 {
            assert_eq!(mine(traces), reference);
        }

        let mut reversed: Vec<&[&str]> = traces.to_vec();
        reversed.reverse();
        assert_eq!(mine(&reversed), reference);
    }
    /// Directly-follows complete for ∧(↺(→(a, b), c), d), with d between the two closest a's. The
    /// minimum self-distance forbids every concurrent cut, so the plain one has to be tried before
    /// the loop.
    #[test]
    fn test_rediscovers_from_a_directly_follows_complete_log() {
        let traces: &[&[&str]] = &[
            &["d", "a", "b"],
            &["a", "b", "d"],
            &["a", "d", "b"],
            &["a", "b", "c", "d", "a", "b"],
            &["a", "b", "d", "c", "a", "b"],
        ];
        for options in [
            InductiveMinerOptions::default(),
            InductiveMinerOptions::im_thesis(),
        ] {
            assert_eq!(mine_with(traces, options), "∧(↺(→(a, b), c), d)");
        }
    }

    #[test]
    fn test_imf_survives_one_repetition_among_hundreds() {
        let (labels, base) = log_of(&[&["a", "b", "c"]]);
        let log = base.derive([
            (vec![0, 1, 2], 100),
            (vec![1, 0, 2], 100),
            (vec![1, 2, 0], 100),
            (vec![0, 1, 2, 0], 1),
        ]);
        assert_eq!(
            discover(&labels, &log, InductiveMinerOptions::imf(0.2)).to_string(),
            "∧(a, →(b, c))"
        );
    }

    /// Without a, the rest has the sequence b, c, d, although [a a] leaves an empty trace.
    #[test]
    fn test_activity_concurrent_may_leave_empty_traces() {
        let traces: &[&[&str]] = &[&["a", "b", "c", "d"], &["b", "c", "d", "a"], &["a", "a"]];
        let expected = "∧(↺(a, tau), ×(tau, →(b, c, d)))";
        assert_eq!(mine(traces), expected);
        assert_eq!(
            mine_with(traces, InductiveMinerOptions::pm4py(0.0)),
            expected
        );
    }

    #[test]
    fn test_thresholds_from_json_are_clamped() {
        for (json, expected) in [
            (r#"{"noise_threshold": -0.5}"#, "a"),
            (r#"{"noise_threshold": 1.5}"#, "a"),
        ] {
            let options: InductiveMinerOptions = serde_json::from_str(json).unwrap();
            assert_eq!(mine_with(&[&["a"]], options), expected, "for {json}");
        }
        let l81: &[&[&str]] = &[
            &["a", "b", "c", "d"],
            &["d", "a", "b"],
            &["a", "d", "c"],
            &["b", "c", "d"],
        ];
        let options: InductiveMinerOptions =
            serde_json::from_str(r#"{"noise_threshold": 1.5}"#).unwrap();
        assert_eq!(
            mine_with(l81, options),
            mine_with(l81, InductiveMinerOptions::imf(1.0))
        );
    }
    /// A few traces that each run every activity once, in random order. The graph is too sparse
    /// for a cut, so the once-per-trace fall through takes out one activity after the other.
    #[test]
    fn test_a_chain_of_fall_throughs_does_not_grow_the_stack() {
        let n = 200;
        let labels: Vec<String> = (0..n).map(|i| format!("x{i:03}")).collect();
        let mut seed: u64 = 1;
        let mut next = |m: usize| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((seed >> 33) % m as u64) as usize
        };
        let traces: Vec<(Vec<ActivityID>, u64)> = (0..6)
            .map(|_| {
                let mut trace: Vec<ActivityID> = (0..n).collect();
                for i in (1..n).rev() {
                    trace.swap(i, next(i + 1));
                }
                (trace, 1)
            })
            .collect();
        let log = ActivityLog::new(n, traces);
        let tree = std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(move || discover(&labels, &log, InductiveMinerOptions::default()))
            .unwrap()
            .join()
            .unwrap();
        assert!(tree.is_valid());
    }
}

#[cfg(test)]
mod test_presets {
    use super::*;

    #[test]
    fn variants_under_presets_are_the_constructors() {
        use InductiveMinerPreset::*;
        use InductiveMinerVariant::*;
        let log = |options| InductiveMiner::Log(options);
        assert_eq!(
            Im.options(Default, 0.2),
            log(InductiveMinerOptions::default())
        );
        assert_eq!(
            Imf.options(Default, 0.2),
            log(InductiveMinerOptions::imf(0.2))
        );
        assert_eq!(
            Im.options(Thesis, 0.2),
            log(InductiveMinerOptions::im_thesis())
        );
        assert_eq!(Ima.options(Default, 0.0), log(InductiveMinerOptions::ima()));
        assert_eq!(
            Imf.options(Prom, 0.2),
            log(InductiveMinerOptions::prom(0.2))
        );
        assert_eq!(
            Imf.options(Pm4py, 0.2),
            log(InductiveMinerOptions::pm4py(0.2))
        );
        assert_eq!(
            Imd.options(Default, 0.2),
            InductiveMiner::Dfg(InductiveMinerDfgOptions::default())
        );
        assert_eq!(
            Imfd.options(Prom, 0.2),
            InductiveMiner::Dfg(InductiveMinerDfgOptions::prom(0.2))
        );
    }

    #[test]
    fn the_simple_entry_points_default_to_imf() {
        let log = crate::event_log!(["a", "b"], ["a", "c"], ["a", "c"], ["a", "c"], ["a", "c"]);
        let projection: EventLogActivityProjection = (&log).into();
        let tree = discover_inductive_tree(
            &projection,
            InductiveMinerVariant::Imf,
            0.2,
            InductiveMinerPreset::Default,
        );
        assert_eq!(
            tree.to_string(),
            discover_inductive_tree_with(&projection, InductiveMinerOptions::imf(0.2).into())
                .to_string()
        );
        assert_eq!(
            discover_inductive_net(
                &projection,
                InductiveMinerVariant::Imd,
                0.2,
                InductiveMinerPreset::Default,
            ),
            discover_inductive_tree_with(&projection, InductiveMinerDfgOptions::default().into())
                .to_petri_net()
        );
    }

    /// The simple binding, called with only a log, runs `IMf` at 0.2.
    #[cfg(feature = "bindings")]
    #[test]
    fn the_simple_binding_defaults_to_imf() {
        use crate::bindings::{call_resolved, get_fn_binding, AppState, RegistryItem};
        let state = AppState::default();
        let log = crate::event_log!(["a", "b", "d"], ["a", "c", "d"], ["a", "b", "b", "d"]);
        state.add("log", RegistryItem::EventLog(log));
        let call = |name: &str, args: serde_json::Value| -> AcceptingPetriNet {
            let id = format!("process_mining::discovery::case_centric::inductive_miner::{name}");
            let out = call_resolved(get_fn_binding(&id).unwrap(), &args, &state).unwrap();
            serde_json::from_slice(&out).unwrap()
        };
        let simple = call(
            "discover_inductive_net",
            serde_json::json!({ "log": "log" }),
        );
        let imf =
            serde_json::json!({ "log": "log", "miner": { "kind": "log", "noise_threshold": 0.2 } });
        let im = serde_json::json!({ "log": "log", "miner": { "kind": "log" } });
        assert_eq!(simple, call("discover_inductive_net_with", imf));
        assert_ne!(simple, call("discover_inductive_net_with", im));
    }

    #[test]
    fn miners_in_json() {
        assert_eq!(
            serde_json::to_value(InductiveMinerVariant::Imfd).unwrap(),
            "IMfd"
        );
        assert_eq!(
            serde_json::to_value(InductiveMinerPreset::Pm4py).unwrap(),
            "pm4py"
        );
        let miner: InductiveMiner =
            serde_json::from_str(r#"{"kind": "log", "noise_threshold": 0.2}"#).unwrap();
        assert_eq!(miner, InductiveMiner::Log(InductiveMinerOptions::imf(0.2)));
        let miner: InductiveMiner = serde_json::from_str(r#"{"kind": "dfg"}"#).unwrap();
        assert_eq!(
            miner,
            InductiveMiner::Dfg(InductiveMinerDfgOptions::default())
        );
        let json = serde_json::to_value(miner).unwrap();
        assert_eq!(json["kind"], "dfg");
    }

    #[test]
    fn the_dfg_variant_mines_a_projection_like_the_log() {
        use crate::core::event_data::case_centric::utils::activity_projection::EventLogActivityProjection;
        use crate::test_utils::get_test_data_path;
        use crate::Importable;
        let path = get_test_data_path().join("xes").join("RepairExample.xes");
        let log = EventLog::import_from_path(path).unwrap();
        let options = InductiveMinerDfgOptions::imfd(0.2);
        let from_log =
            discover_inductive_tree_from_log(&log, &EventLogClassifier::default(), options);
        let projection = EventLogActivityProjection::from(&log);
        let from_projection = discover_inductive_tree_with(&projection, options.into());
        assert_eq!(from_projection.to_string(), from_log.to_string());
    }

    #[test]
    fn the_dfg_variant_mines_a_graph_like_the_log() {
        use crate::discovery::case_centric::dfg::discover_dfg;
        use crate::test_utils::get_test_data_path;
        use crate::Importable;
        let path = get_test_data_path().join("xes").join("RepairExample.xes");
        let log = EventLog::import_from_path(path).unwrap();
        for options in [
            InductiveMinerDfgOptions::default(),
            InductiveMinerDfgOptions::imfd(0.2),
            InductiveMinerDfgOptions::imfd(0.6),
        ] {
            let from_log =
                discover_inductive_tree_from_log(&log, &EventLogClassifier::default(), options);
            let from_graph = discover_inductive_tree_from_dfg(&discover_dfg(&log), options);
            assert_eq!(from_graph.to_string(), from_log.to_string());
        }
    }

    #[test]
    fn test_the_default_improves_on_the_thesis() {
        let options = InductiveMinerOptions::imf(0.4);
        assert_eq!(options.noise_threshold, 0.4);
        assert!(options.strict_sequence);
        assert!(!options.use_inclusive_choice);
        assert!(!options.use_degenerate_tau_loops);

        let ima = InductiveMinerOptions::ima();
        assert!(ima.use_interleaved && ima.use_inclusive_choice);

        let thesis = InductiveMinerOptions::imf_thesis(0.4);
        assert_eq!(thesis.noise_threshold, 0.4);
        assert!(!thesis.strict_sequence);
        assert!(!thesis.use_inclusive_choice);
        assert!(thesis.use_degenerate_tau_loops);
    }
}
