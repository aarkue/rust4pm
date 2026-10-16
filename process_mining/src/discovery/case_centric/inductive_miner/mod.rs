//! The Inductive Miner (IM) and its infrequent variant (`IMf`), following Leemans, "Robust Process
//! Mining with Guarantees" (§6.1 and §6.2.2).
//!
//! For a log it tries the base cases, then [cut detection](cut_finder) with a log split and
//! recursion, and finally the [fall throughs](fallthrough), ending in a flower model. `IMf` adds a deviation threshold `f` to all four steps and gives up the fitness
//! guarantee of plain IM.
//!
//! For the variant that recurses on a directly-follows graph instead of the log, see
//! [`dfg_miner`].
//!
//! ```
//! use process_mining::discovery::case_centric::inductive_miner::{
//!     inductive_miner, InductiveMinerOptions,
//! };
//! use process_mining::core::event_data::case_centric::EventLogClassifier;
//! use process_mining::event_log;
//!
//! let log = event_log!(["a", "b", "d"], ["a", "c", "d"]);
//! let tree = inductive_miner(
//!     &log,
//!     &EventLogClassifier::default(),
//!     InductiveMinerOptions::default(),
//! );
//!
//! assert_eq!(tree.to_string(), "→(a, X(b, c), d)");
//! ```

use rayon::prelude::*;

use crate::core::event_data::case_centric::utils::activity_projection::EventLogActivityProjection;
use crate::core::event_data::case_centric::EventLogClassifier;
use crate::core::process_models::process_tree::{Node, OperatorType, ProcessTree};
use crate::EventLog;

use base_cases::{find_base_case, BaseCase};
use cut_finder::find_cut_filtering;
use dfg::ActivityDfg;
use fallthrough::{empty_traces, find_fallthrough, Fallthrough};
use log::{ActivityID, ActivityLog};
use splits::split_log;

pub(crate) mod base_cases;
pub mod cut_finder;
pub mod dfg;
pub mod dfg_miner;
pub mod fallthrough;
pub mod log;
pub(crate) mod splits;
pub(crate) mod structures;

pub use dfg_miner::{
    inductive_miner_dfg, inductive_miner_dfg_from_graph, InductiveMinerDfgOptions,
};

/// Settings for a run of the Inductive Miner.
///
/// Start from a preset and change single fields with struct update syntax, as in
/// `InductiveMinerOptions { use_interleaved: true, ..InductiveMinerOptions::imf(0.2) }`.
/// [`default`](Self::default) is IM and [`imf`](Self::imf) is `IMf`, each with a few refinements
/// over the thesis. [`im_thesis`](Self::im_thesis) and [`imf_thesis`](Self::imf_thesis) are the
/// algorithms as published, and [`ima`](Self::ima) adds the cuts of `IMa`. For real-life logs,
/// prefer [`imf`](Self::imf).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InductiveMinerOptions {
    /// Deviation threshold `f ∈ [0, 1]` of `IMf`. `0.0` turns filtering off.
    pub noise_threshold: f64,
    /// Whether to try the [fall throughs](fallthrough) before resorting to a flower model.
    pub use_fallthroughs: bool,
    /// Whether to merge the parts of a sequence cut that only occur together with a skippable
    /// neighbour. See [`strict_sequence_cut`](cut_finder::strict_sequence_cut).
    pub strict_sequence: bool,
    /// Whether to look for interleaved (`↔`) cuts, tried before concurrency. Under `IMf` they still
    /// read the unfiltered traces, so noise can prevent them.
    /// See [`interleaved_cut`](cut_finder::interleaved_cut).
    pub use_interleaved: bool,
    /// Whether to regroup concurrent cuts into nested concurrency, optionality and inclusive
    /// choice (`∨`). See [`coo_cut`](cut_finder::coo_cut).
    pub use_inclusive_choice: bool,
    /// Whether to refuse a cut whose split leaves a part without any events, which only the
    /// filtering splits of `IMf` can do, and fall through instead. The thesis and `ProM` take the
    /// cut, and the part becomes `τ`.
    pub guard_empty_cut_parts: bool,
    /// Whether concurrency detection consults the minimum-self-distance relation (∧↔.1), which
    /// tells `a` and `b` running concurrently from `a` looping around `b`.
    pub use_minimum_self_distance: bool,
    /// Whether to take a tau loop that cuts every trace into single events, as the thesis does.
    /// Mining those single events gives the flower model, so this only decides whether
    /// [`two_activities_concurrent`](fallthrough::two_activities_concurrent) gets a chance first.
    pub use_degenerate_tau_loops: bool,
    /// Whether `IMf` keeps the single-activity base case despite repetitions when single
    /// executions dominate (§6.2.2.3). Without it any repetition ends the recursion in a loop.
    pub filter_single_activity: bool,
    /// Whether the [activity-concurrent fall through](fallthrough::activity_concurrent) accepts a
    /// candidate whose remaining log only admits a cut after filtering.
    ///
    /// `IMf` reuses IM's fall throughs unchanged (§6.2.2.4), so the thesis filters only in cut
    /// detection, and `ProM` probes the unfiltered graph as well. With this on, the fall through fires where cut detection already gave up. On logs
    /// with heavy per-trace repetition the relative edge filter drops most of the graph, the rest is
    /// acyclic, and every candidate yields a sequence cut the log does not support.
    pub filter_activity_concurrent_probe: bool,
    /// Whether to collapse nested occurrences of the same associative operator (e.g.,
    /// `→(→(a, b), c)` into `→(a, b, c)`).
    pub fold: bool,
}

impl Default for InductiveMinerOptions {
    /// The Inductive Miner without filtering, with the steps that measured as improvements over
    /// the thesis switched on. See [`im_thesis`](Self::im_thesis) for the definition as published.
    /// None of those steps gives up behaviour, so the model still fits the log.
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

    /// Plain IM as Leemans defines it (§6.1), with none of the later refinements.
    ///
    /// Differs from [`default`](Self::default) in two places: sequence cuts are the maximal ones
    /// of →.1, and a tau loop is taken even when it degenerates into the flower model. The thesis
    /// leaves open which activity the once-per-trace fall through takes out, so that is not a
    /// difference.
    pub fn im_thesis() -> Self {
        Self {
            strict_sequence: false,
            use_degenerate_tau_loops: true,
            ..Self::default()
        }
    }

    /// `IMf` as Leemans defines it (§6.2.2), the [thesis version](Self::im_thesis) of
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

    /// The settings under which this implementation behaves most like `ProM`'s IM, for comparing
    /// the two.
    ///
    /// `ProM` applies the strict sequence cut, as [`imf`](Self::imf) does, but not the
    /// minimum-self-distance restriction ∧↔.1, so it reports a concurrency where an activity
    /// loops around another.
    pub fn prom(noise_threshold: f64) -> Self {
        Self {
            use_minimum_self_distance: false,
            ..Self::imf(noise_threshold)
        }
    }

    /// The settings under which this implementation behaves most like `PM4Py`'s IM, for comparing
    /// the two. As [`prom`](Self::prom), except that `PM4Py` also omits the single-activity
    /// filtering of `IMf`, so a repeated activity always becomes a loop.
    pub fn pm4py(noise_threshold: f64) -> Self {
        Self {
            filter_single_activity: false,
            ..Self::prom(noise_threshold)
        }
    }
}

/// Counts how often the exhaustive check in [`Miner::verify_cut_detection`] ran, so a test can
/// tell that it did rather than passing vacuously.
#[cfg(test)]
pub(crate) static EXHAUSTIVE_CHECKS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// What the recursion needs besides the current log.
struct Miner<'a> {
    /// Activity labels, indexed by [`ActivityID`].
    labels: &'a [String],
    options: &'a InductiveMinerOptions,
}

/// Discovers a [`ProcessTree`] from an event log using the Inductive Miner.
///
/// The classifier decides what counts as an activity; pass `&EventLogClassifier::default()` to use
/// `concept:name`. See [`InductiveMinerOptions`] for the available settings, and the
/// [module documentation](self) for an example.
///
/// If you already have an [`EventLogActivityProjection`], use [`inductive_miner_projection`] to
/// avoid projecting the log twice.
pub fn inductive_miner(
    event_log: &EventLog,
    classifier: &EventLogClassifier,
    options: InductiveMinerOptions,
) -> ProcessTree {
    let (labels, activity_log) = log::project_event_log(event_log, classifier);
    discover(&labels, &activity_log, options)
}

/// Discovers a [`ProcessTree`] from an already projected event log using the Inductive Miner.
///
/// See [`inductive_miner`].
pub fn inductive_miner_projection(
    projection: &EventLogActivityProjection,
    options: InductiveMinerOptions,
) -> ProcessTree {
    let (labels, activity_log) = log::from_activity_projection(projection);
    discover(&labels, &activity_log, options)
}

/// Runs the recursion and wraps the result up as a [`ProcessTree`].
fn discover(
    labels: &[String],
    activity_log: &ActivityLog,
    options: InductiveMinerOptions,
) -> ProcessTree {
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
    fn mine(&self, log: &ActivityLog) -> Node {
        // Base cases and cut detection are not defined for a log with empty traces.
        if let Some(fallthrough) = empty_traces(log, self.options.noise_threshold) {
            return self.build(fallthrough);
        }

        let base_case_threshold = if self.options.filter_single_activity {
            self.options.noise_threshold
        } else {
            0.0
        };
        match find_base_case(log, base_case_threshold) {
            Some(BaseCase::EmptyLog) => return tau(),
            Some(BaseCase::SingleActivity(activity)) => return leaf(self.labels, activity),
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
                return operator(cut.operator(), self.mine_all(&sub_logs));
            }
        }

        self.build(find_fallthrough(log, &dfg, self.options))
    }

    /// Discovers the sub-trees of a list of sub-logs, in order.
    ///
    /// The sub-logs of a cut are independent, so they are mined in parallel once there is enough
    /// work to pay for the hand-off. Recursion produces many tiny sub-logs near the leaves, and
    /// spawning tasks for those costs more than mining them.
    fn mine_all(&self, sub_logs: &[ActivityLog]) -> Vec<Node> {
        /// Total events below which the sub-logs are mined on the current thread.
        const PARALLEL_THRESHOLD: u64 = 2_048;

        let worth_parallelising = sub_logs.len() > 1
            && sub_logs.iter().map(ActivityLog::num_events).sum::<u64>() > PARALLEL_THRESHOLD;

        if worth_parallelising {
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
    /// available, and that where the miner fell through no cut existed at all. The last is what
    /// the search is for: fitness cannot catch a missed cut, since falling through only adds
    /// behaviour, and there is no cut to check either.
    ///
    /// Only meaningful without filtering, since with a noise threshold a cut may come from a
    /// filtered graph, and without
    /// [`strict_sequence`](InductiveMinerOptions::strict_sequence), which deliberately reports
    /// cuts that are neither maximal nor the earliest available. Skipped on graphs too large to
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
        // otherwise it reports concurrent cuts the miner is right to reject.
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
                            "{} does not merge neighbours of {}",
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
            (&[&["a"], &["b"]], "X(a, b)"),
            (&[&["a", "b"], &["b", "a"]], "∧(a, b)"),
            (&[&["a", "c"], &["a", "c", "b", "a", "c"]], "↻(→(a, c), b)"),
            (&[&["a", "a"]], "↻(a, tau)"),
            (&[&["a"], &[]], "X(tau, a)"),
            (&[&[], &["a", "a"]], "X(tau, ↻(a, tau))"),
            (&[&["a", "b", "d"], &["a", "c", "d"]], "→(a, X(b, c), d)"),
            (
                &[&["a", "b", "c", "d"], &["a", "c", "b", "d"]],
                "→(a, ∧(b, c), d)",
            ),
        ] {
            assert_eq!(mine(traces), expected, "for {traces:?}");
        }
    }

    /// Leemans' running example L81, for which the activity-once-per-trace fall through takes d
    /// out, and the same log with the fall throughs disabled.
    #[test]
    fn test_fall_throughs() {
        let l81: &[&[&str]] = &[
            &["a", "b", "c", "d"],
            &["d", "a", "b"],
            &["a", "d", "c"],
            &["b", "c", "d"],
        ];
        assert_eq!(mine(l81), "∧(d, →(X(tau, a), X(tau, b), X(tau, c)))");
        assert_eq!(
            mine_with(
                l81,
                InductiveMinerOptions {
                    use_fallthroughs: false,
                    ..InductiveMinerOptions::default()
                }
            ),
            "↻(X(a, b, c, d), tau)"
        );
    }

    #[test]
    fn test_folding_can_be_disabled() {
        // Cut detection reports maximal cuts, so nesting comes from the fall throughs: the empty
        // trace produces an XOR whose second child is the XOR between a and b.
        let traces: &[&[&str]] = &[&[], &["a"], &["b"]];
        assert_eq!(mine(traces), "X(tau, a, b)");
        assert_eq!(
            mine_with(
                traces,
                InductiveMinerOptions {
                    fold: false,
                    ..InductiveMinerOptions::default()
                }
            ),
            "X(tau, X(a, b))"
        );
    }

    #[test]
    fn test_imf_ignores_infrequent_behaviour() {
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
            "X(tau, a)"
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
            "→(X(tau, ↻(b, tau)), X(tau, d), ∧(X(tau, a), X(tau, ↻(c, tau))))"
        );
        // The same tree ProM and PM4Py report, which is not the case without the option.
        assert_eq!(
            mine_with(traces, strict),
            "→(X(tau, →(↻(b, tau), X(tau, d))), ∧(X(tau, a), X(tau, ↻(c, tau))))"
        );
        // Parts that the log shows apart are still separated.
        assert_eq!(
            mine_with(&[&["a", "b"], &["b", "c"]], strict),
            "→(X(tau, a), b, X(tau, c))"
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
        assert_eq!(mine(traces), "∧(→(X(tau, a), X(tau, b)), c)");
        assert_eq!(
            mine_with(traces, interleaved),
            "↔(→(X(tau, a), X(tau, b)), c)"
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
        assert_eq!(mine_with(traces, thesis), "∧(X(tau, a), X(tau, b))");
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
        // ∧↔.1 keeps a and f apart, since f sits between the two closest a's. Neither tool
        // applies it, so both separate them and report a concurrency.
        let looping: &[&[&str]] = &[&["a", "c", "f"], &["a", "f", "a"], &["c", "a", "f"]];
        assert_eq!(mine(looping), "∧(f, ↻(a, tau), X(tau, c))");
        for preset in [
            InductiveMinerOptions::prom(0.0),
            InductiveMinerOptions::pm4py(0.0),
        ] {
            assert_eq!(mine_with(looping, preset), "∧(↻(a, tau), →(X(tau, c), f))");
        }

        // `PM4Py` omits the single-activity filtering of IMf, so a repetition becomes a loop
        // where the thesis and ProM keep the leaf.
        let (labels, base) = log_of(&[&["a"]]);
        let log = base.derive([(vec![0], 100), (vec![0, 0], 1)]);
        for (preset, expected) in [
            (InductiveMinerOptions::imf(0.2), "a"),
            (InductiveMinerOptions::prom(0.2), "a"),
            (InductiveMinerOptions::pm4py(0.2), "↻(a, tau)"),
        ] {
            assert_eq!(discover(&labels, &log, preset).to_string(), expected);
        }
    }

    #[test]
    fn test_filtering_the_fallthrough_probe_changes_the_tree() {
        let log: &[&[&str]] = &[
            &["b", "c", "b", "b", "b", "c"],
            &["b", "b"],
            &["a", "b", "b", "b", "a", "b"],
        ];

        let imf = InductiveMinerOptions::imf(0.2);
        assert_eq!(mine_with(log, imf), "↻(→(X(tau, a), b, X(tau, c)), tau)");

        // The filtered probe accepts a candidate, and the tree that follows allows `a` at most
        // once, so it no longer replays ⟨a, b, b, b, a, b⟩.
        assert_eq!(
            mine_with(
                log,
                InductiveMinerOptions {
                    filter_activity_concurrent_probe: true,
                    ..imf
                }
            ),
            "∧(X(tau, c), →(X(tau, a), ↻(b, tau)))"
        );
    }

    #[test]
    fn test_entry_points() {
        let log = event_log!(["a", "b"], ["a", "b"]);
        let options = InductiveMinerOptions::default();
        let classifier = EventLogClassifier::default();
        assert_eq!(
            inductive_miner(&log, &classifier, options).to_string(),
            "→(a, b)"
        );

        let projection: EventLogActivityProjection = (&log).into();
        assert_eq!(
            inductive_miner_projection(&projection, options).to_string(),
            "→(a, b)"
        );
    }

    /// The one guarantee IM has: without filtering the model replays every trace of the log. A
    /// fitness below 1 here is a bug in the miner, and nothing else in the suite would catch it,
    /// since a missed cut or a botched split still yields a valid tree.
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
            // Mining the projection the alignment sees rules out the two disagreeing on what an
            // activity is.
            let projection = EventLogActivityProjection::from(&log);
            let tree = inductive_miner_projection(&projection, InductiveMinerOptions::default());

            let net = tree.to_petri_net();
            let options = AlignmentOptions::default();
            let alignments = align_log(&net, &projection, &options);
            let fitness = compute_fitness(&alignments, &net, &options).unwrap();
            assert_eq!(fitness.log_fitness, 1.0, "{name} is not replayed by {tree}");
        }
    }

    /// Runs the miner over the real-life test logs. The point is not the tree but the footprint
    /// and exhaustive checks that run along the way, which these exercise far more than the
    /// hand-written logs above.
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
                let tree = inductive_miner(
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
    /// all possible cuts. A cut finder that silently misses cuts fails here even though the
    /// resulting model still replays the whole log.
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
}

#[cfg(test)]
mod test_presets {
    use super::*;

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
