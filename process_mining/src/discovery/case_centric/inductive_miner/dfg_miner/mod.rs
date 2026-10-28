//! The directly-follows variant of the Inductive Miner (`IMd` and `IMfd`), following §6.6 of the
//! book cited in the [parent module](super).
//!
//! It builds a directly-follows graph in one pass over the log and recurses on that graph alone,
//! so it suits very large logs and can start from a graph. Unlike IM, the model may not replay the
//! whole log (§6.6.6.1). Rediscoverability is kept.
//!
//! [`InductiveMinerDfgOptions::prom`] and [`InductiveMinerDfgOptions::pm4py`] imitate those two
//! tools for a comparison run.
//!
//! ```
//! use process_mining::discovery::case_centric::inductive_miner::{
//!     discover_inductive_tree_from_log, InductiveMinerDfgOptions,
//! };
//! use process_mining::core::event_data::case_centric::EventLogClassifier;
//! use process_mining::event_log;
//!
//! let log = event_log!(["a", "b", "d"], ["a", "c", "d"]);
//! let tree = discover_inductive_tree_from_log(
//!     &log,
//!     &EventLogClassifier::default(),
//!     InductiveMinerDfgOptions::default(),
//! );
//!
//! assert_eq!(tree.to_string(), "→(a, ×(b, c), d)");
//! ```

use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::process_models::case_centric::dfg::DirectlyFollowsGraph;
use crate::core::process_models::process_tree::{Node, OperatorType, ProcessTree};

use super::cut_finder::cut::Cut;
use super::cut_finder::{
    concurrent_cut, exclusive_choice_cut, loop_cut, sequence_cut, strict_sequence_cut,
};
use super::dfg::{ActivityDfg, NoiseFilter};
use super::{choice, leaf, operator, tau};
use splits::split_dfg;

mod splits;

/// Settings of the DFG variant of the Inductive Miner. Settings that need traces, such as the
/// interleaved cut, do not exist here.
///
/// Fields missing in JSON take their default value.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct InductiveMinerDfgOptions {
    /// Deviation threshold `f ∈ [0, 1]` of `IMfd`, clamped to that range. `0.0` is plain `IMd`.
    pub noise_threshold: f64,
    /// Whether to merge the parts of a sequence cut that only occur together with a skippable
    /// neighbor.
    pub strict_sequence: bool,
    /// Whether to collapse nested occurrences of the same associative operator, turning, e.g.,
    /// `→(→(a, b), c)` into `→(a, b, c)`.
    pub fold: bool,
    /// Whether to try the [tau loops](strict_dfg_tau_loop) before resorting to a flower model.
    pub use_tau_loops: bool,
    /// Whether the flower model allows the empty trace, i.e., is `↺(τ, ×(a₁, ..., aₙ))` instead of
    /// the thesis' `↺(×(a₁, ..., aₙ), τ)`. The empty-traces step runs before any fall through, so
    /// allowing one here only loses precision.
    pub flower_accepts_empty: bool,
    /// Whether a lone activity with a self edge becomes a flower instead of `↺(a, τ)`. The
    /// thesis hands the case to [`strict_dfg_tau_loop`] (§6.6.3.3). With
    /// [`flower_accepts_empty`](Self::flower_accepts_empty), this turns `a⁺` into `a*`.
    pub repeated_activity_to_flower: bool,
    /// Whether `IMfd` keeps the single-activity base case despite a self edge when single
    /// executions dominate (§6.6.4.3). Without it a self edge always ends the recursion in a loop.
    pub filter_single_activity: bool,
}

impl Default for InductiveMinerDfgOptions {
    /// Plain `IMd` as published (§6.6.3).
    fn default() -> Self {
        Self {
            noise_threshold: 0.0,
            strict_sequence: false,
            fold: true,
            use_tau_loops: true,
            flower_accepts_empty: false,
            repeated_activity_to_flower: false,
            filter_single_activity: true,
        }
    }
}

impl InductiveMinerDfgOptions {
    /// `IMfd` with the given deviation threshold, clamped to `[0, 1]` (§6.6.4).
    pub fn imfd(noise_threshold: f64) -> Self {
        Self {
            noise_threshold: noise_threshold.clamp(0.0, 1.0),
            ..Self::default()
        }
    }

    /// The settings under which this variant behaves most like `ProM`'s `IMd`.
    /// `ProM` applies the strict sequence cut, sends a lone activity with a self edge to the
    /// flower instead of to [`strict_dfg_tau_loop`], its flower allows the empty trace, and its
    /// `IMfd` reuses the unfiltered base cases.
    pub fn prom(noise_threshold: f64) -> Self {
        Self {
            strict_sequence: true,
            flower_accepts_empty: true,
            repeated_activity_to_flower: true,
            filter_single_activity: false,
            ..Self::imfd(noise_threshold)
        }
    }

    /// The settings under which this variant behaves most like `PM4Py`'s `IMd`.
    /// `PM4Py` offers empty traces and the flower as the only fall throughs, and its flower
    /// allows the empty trace. It also ignores the deviation threshold entirely, so pass `0.0` to
    /// compare like for like.
    pub fn pm4py(noise_threshold: f64) -> Self {
        Self {
            use_tau_loops: false,
            ..Self::prom(noise_threshold)
        }
    }
}

/// Discovers a [`ProcessTree`] from an existing [`DirectlyFollowsGraph`] with `IMd` or `IMfd`.
///
/// Start and end weights are the graph's start and end counts. Empty traces cannot be
/// represented, so to mine a log that has them, use
/// [`discover_inductive_tree_with`](super::discover_inductive_tree_with) with
/// [`InductiveMiner::Dfg`](super::InductiveMiner::Dfg).
pub fn discover_inductive_tree_from_dfg(
    graph: &DirectlyFollowsGraph<'_>,
    options: InductiveMinerDfgOptions,
) -> ProcessTree {
    let mut labels: Vec<String> = graph.activities.keys().cloned().collect();
    labels.extend(
        graph
            .directly_follows_relations
            .keys()
            .flat_map(|(from, to)| [from.to_string(), to.to_string()]),
    );
    labels.extend(graph.start_activities.keys().cloned());
    labels.extend(graph.end_activities.keys().cloned());
    labels.sort_unstable();
    labels.dedup();

    let id_of: HashMap<&str, usize> = labels
        .iter()
        .enumerate()
        .map(|(i, label)| (label.as_str(), i))
        .collect();
    let n = labels.len();

    let edges: Vec<(u32, u32, u64)> = graph
        .directly_follows_relations
        .iter()
        .map(|((from, to), &count)| {
            (
                id_of[from.as_ref()] as u32,
                id_of[to.as_ref()] as u32,
                count as u64,
            )
        })
        .collect();
    let count =
        |of: &HashMap<_, u32>, label: &String| u64::from(of.get(label).copied().unwrap_or(0));
    let start = labels
        .iter()
        .map(|l| count(&graph.start_activities, l))
        .collect();
    let end = labels
        .iter()
        .map(|l| count(&graph.end_activities, l))
        .collect();

    let dfg = ActivityDfg::from_parts(n, (0..n).collect(), start, end, edges, 0);
    discover_dfg(&labels, &dfg, options)
}

/// Runs the recursion and wraps the result up as a [`ProcessTree`].
pub(super) fn discover_dfg(
    labels: &[String],
    dfg: &ActivityDfg,
    options: InductiveMinerDfgOptions,
) -> ProcessTree {
    let options = InductiveMinerDfgOptions {
        noise_threshold: options.noise_threshold.clamp(0.0, 1.0),
        ..options
    };
    let miner = DfgMiner {
        labels,
        options: &options,
    };
    let tree = ProcessTree::new(miner.mine(dfg));

    if options.fold {
        tree.fold()
    } else {
        tree
    }
}

/// What the recursion needs besides the current graph.
struct DfgMiner<'a> {
    /// Activity labels, indexed by global activity id.
    labels: &'a [String],
    options: &'a InductiveMinerDfgOptions,
}

impl DfgMiner<'_> {
    /// Discovers the sub-tree for one (sub)graph.
    fn mine(&self, dfg: &ActivityDfg) -> Node {
        // Base cases and cut detection assume a graph without empty traces (§6.6.3.5).
        if dfg.empty_traces() > 0 {
            return self.empty_traces(dfg);
        }
        if let Some(base_case) = self.base_case(dfg) {
            return base_case;
        }
        if let Some(cut) = self.find_cut(dfg) {
            let subgraphs = split_dfg(dfg, &cut);
            return operator(cut.operator(), subgraphs.iter().map(|sub| self.mine(sub)));
        }
        self.fallthrough(dfg)
    }

    /// Handles the empty traces, `emptyTracesDfg`. `IMfd` drops them instead when fewer than a
    /// fraction `noise_threshold` of the traces are empty (§6.6.4.4).
    ///
    /// The thesis measures against `|Start(D)|`, which leaves out the empty traces themselves.
    /// Counting them, as `IMf` does, makes both variants decide alike on the same log.
    fn empty_traces(&self, dfg: &ActivityDfg) -> Node {
        let traces: u64 =
            (0..dfg.len()).map(|a| dfg.start_count(a)).sum::<u64>() + dfg.empty_traces();
        let frequent_enough =
            dfg.empty_traces() as f64 >= traces as f64 * self.options.noise_threshold;

        let rest = dfg.without_empty_traces();
        match (frequent_enough, rest.is_empty()) {
            (true, true) => tau(),
            (true, false) => operator(OperatorType::ExclusiveChoice, [tau(), self.mine(&rest)]),
            (false, _) => self.mine(&rest),
        }
    }

    /// The two base cases of §6.6.3.3: an empty graph is `τ`, and a single activity without a
    /// self edge is a leaf.
    ///
    /// `IMfd` keeps the leaf despite a self edge when single executions dominate: with a start
    /// weight `s` and a self edge of weight `w`, the trace lengths read as a geometric
    /// distribution with `p = s / (2s + w)`, and the leaf survives `|p - 0.5| ≤ f` (§6.6.4.3).
    /// Otherwise the self edge falls to [`strict_dfg_tau_loop`], which turns it into `↺(a, τ)`.
    fn base_case(&self, dfg: &ActivityDfg) -> Option<Node> {
        if dfg.is_empty() {
            return Some(tau());
        }
        if dfg.len() != 1 {
            return None;
        }

        let self_edge = dfg.edge_count(0, 0);
        let start = dfg.start_count(0) as f64;
        let p = start / (2.0 * start + self_edge as f64);
        let threshold = if self.options.filter_single_activity {
            self.options.noise_threshold
        } else {
            0.0
        };
        // Without a self edge, `p` is 0.5 or, for a start weight of 0, NaN.
        (self_edge == 0 || (p - 0.5).abs() <= threshold).then(|| leaf(self.labels, dfg.activity(0)))
    }

    /// Cut detection of `IMd`: the four footprint cuts of IM, with `concurrentCut(D, ∅)`, since
    /// there is no log to compute a minimum-self-distance relation from (§6.6.3.1). `IMfd`
    /// retries on the filtered graph, exactly like `IMf` (§6.6.4.1).
    fn find_cut(&self, dfg: &ActivityDfg) -> Option<Cut> {
        let footprint_cuts = |dfg: &ActivityDfg| {
            exclusive_choice_cut(dfg)
                .or_else(|| {
                    let cut = sequence_cut(dfg)?;
                    if self.options.strict_sequence {
                        strict_sequence_cut(dfg, cut)
                    } else {
                        Some(cut)
                    }
                })
                .or_else(|| concurrent_cut(dfg, None))
                .or_else(|| loop_cut(dfg))
        };

        footprint_cuts(dfg).or_else(|| {
            (self.options.noise_threshold > 0.0)
                .then(|| {
                    footprint_cuts(&dfg.filtered(NoiseFilter::new(self.options.noise_threshold)))
                })
                .flatten()
        })
    }

    /// The fall throughs of §6.6.3.4: the two tau loops, then the flower model.
    ///
    /// The activity fall throughs of IM have no counterpart here, since a graph does not show how
    /// often an activity occurs per trace.
    fn fallthrough(&self, dfg: &ActivityDfg) -> Node {
        let lone_repeated_activity = self.options.repeated_activity_to_flower && dfg.len() == 1;
        if self.options.use_tau_loops && !lone_repeated_activity {
            if let Some(inner) = strict_dfg_tau_loop(dfg).or_else(|| dfg_tau_loop(dfg)) {
                return operator(OperatorType::Loop, [self.mine(&inner), tau()]);
            }
        }

        let flower = choice(self.labels, dfg.activities());
        if self.options.flower_accepts_empty {
            operator(OperatorType::Loop, [tau(), flower])
        } else {
            operator(OperatorType::Loop, [flower, tau()])
        }
    }
}

/// `strictDfgTauLoop`: removes every edge from an end to a start activity, where one execution
/// seems to end and the next to begin. Returns `None` if there is no such edge.
pub fn strict_dfg_tau_loop(dfg: &ActivityDfg) -> Option<ActivityDfg> {
    without_edges(dfg, |from, to| dfg.is_end(from) && dfg.is_start(to))
}

/// `dfgTauLoop`: removes every edge into a start activity, where an execution could have begun
/// without the previous one having finished. Returns `None` if there is no such edge.
pub fn dfg_tau_loop(dfg: &ActivityDfg) -> Option<ActivityDfg> {
    without_edges(dfg, |_, to| dfg.is_start(to))
}

/// The graph without the edges `drop` selects, or `None` if it selects none.
fn without_edges(dfg: &ActivityDfg, drop: impl Fn(usize, usize) -> bool) -> Option<ActivityDfg> {
    let kept: Vec<(u32, u32, u64)> = dfg
        .edges()
        .filter(|&(from, to, _)| !drop(from, to))
        .map(|(from, to, count)| (from as u32, to as u32, count))
        .collect();

    (kept.len() < dfg.num_edges()).then(|| {
        ActivityDfg::from_parts(
            dfg.alphabet_size(),
            dfg.activities().to_vec(),
            (0..dfg.len()).map(|a| dfg.start_count(a)).collect(),
            (0..dfg.len()).map(|a| dfg.end_count(a)).collect(),
            kept,
            0,
        )
    })
}

#[cfg(test)]
mod test_dfg_miner {
    use super::super::log::test_utils::log_of;
    use super::*;
    use crate::core::event_data::case_centric::EventLogClassifier;
    use crate::event_log;

    fn mine(traces: &[&[&str]]) -> String {
        mine_with(traces, InductiveMinerDfgOptions::default())
    }

    fn mine_with(traces: &[&[&str]], options: InductiveMinerDfgOptions) -> String {
        let (labels, log) = log_of(traces);
        let tree = discover_dfg(&labels, &ActivityDfg::discover(&log), options);
        assert!(tree.is_valid(), "discovered an invalid tree: {tree}");
        tree.to_string()
    }

    /// The worked example of §6.6.1: L113 and the tree `IMd` discovers for it, including the
    /// strict-tau-loop fall through on the `{f, g, h}` subgraph.
    #[test]
    fn test_leemans_worked_example() {
        let l113: &[&[&str]] = &[
            &["a", "b", "c", "f", "g", "h", "i"],
            &["a", "b", "c", "g", "h", "f", "i"],
            &["a", "b", "c", "h", "f", "g", "i"],
            &["a", "c", "b", "f", "g", "h", "i"],
            &["a", "c", "b", "g", "h", "f", "i"],
            &["a", "c", "b", "h", "f", "g", "i"],
            &["a", "d", "f", "g", "h", "i"],
            &["a", "d", "e", "d", "g", "h", "f", "i"],
            &["a", "d", "e", "d", "e", "d", "h", "f", "g", "i"],
        ];
        assert_eq!(
            mine(l113),
            "→(a, ×(∧(b, c), ↺(d, e)), ↺(×(f, g, h), tau), i)"
        );
    }

    /// On clean structured logs the DFG variant agrees with IM. These are IM's own base-case and
    /// operator tests.
    #[test]
    fn test_agrees_with_im_on_structured_logs() {
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

    #[test]
    fn test_tau_loops() {
        // The end-to-start edge b → a is removed, and the rest is the repeated sequence.
        assert_eq!(mine(&[&["a", "b", "a", "b"]]), "↺(→(a, b), tau)");

        // No edge runs from an end to a start activity, but b → a re-enters the start activity a
        // mid-trace, which only the loose variant cuts.
        assert_eq!(
            mine(&[&["a", "b", "a", "c", "b", "c"]]),
            "↺(→(a, ↺(→(×(tau, b), c), tau)), tau)"
        );
    }

    #[test]
    fn test_strict_sequence() {
        let strict = InductiveMinerDfgOptions {
            strict_sequence: true,
            ..Default::default()
        };
        // d is only reached from b, which some traces skip, so b and d share a part.
        let traces: &[&[&str]] = &[
            &["c", "c", "c", "a", "c"],
            &["b", "b", "d", "a"],
            &["b", "c"],
        ];
        assert_eq!(
            mine(traces),
            "→(×(tau, ↺(b, tau)), ×(tau, d), ∧(a, ↺(c, tau)))"
        );
        assert_eq!(
            mine_with(traces, strict),
            "→(×(tau, →(↺(b, tau), ×(tau, d))), ∧(a, ↺(c, tau)))"
        );
    }

    #[test]
    fn test_single_activity_with_self_edge() {
        // A single trace with a repetition makes the leaf a loop.
        let (labels, base) = log_of(&[&["a"]]);
        let log = base.derive([(vec![0], 100), (vec![0, 0], 1)]);
        let dfg = ActivityDfg::discover(&log);

        assert_eq!(
            discover_dfg(&labels, &dfg, InductiveMinerDfgOptions::default()).to_string(),
            "↺(a, tau)"
        );
        // IMfd tolerates it: p = 101 / 203 is close enough to 0.5.
        assert_eq!(
            discover_dfg(&labels, &dfg, InductiveMinerDfgOptions::imfd(0.2)).to_string(),
            "a"
        );

        assert_eq!(InductiveMinerDfgOptions::imfd(7.0).noise_threshold, 1.0);
    }

    /// The presets against the trees the two tools actually produce, taken from black-box runs of
    /// their `IMd`.
    #[test]
    fn test_comparison_presets() {
        // A lone activity with a self edge: `a⁺` for the thesis, `a*` for both tools.
        let repeated: &[&[&str]] = &[&["a", "a"]];
        assert_eq!(mine(repeated), "↺(a, tau)");
        assert_eq!(
            mine_with(repeated, InductiveMinerDfgOptions::prom(0.0)),
            "↺(tau, a)"
        );
        assert_eq!(
            mine_with(repeated, InductiveMinerDfgOptions::pm4py(0.0)),
            "↺(tau, a)"
        );

        // The {f, g, h} subgraph of L113: ProM takes the tau loop, PM4Py has none and flowers.
        let unordered: &[&[&str]] = &[&["f", "g", "h"], &["g", "h", "f"], &["h", "f", "g"]];
        assert_eq!(
            mine_with(unordered, InductiveMinerDfgOptions::prom(0.0)),
            "↺(×(f, g, h), tau)"
        );
        assert_eq!(
            mine_with(unordered, InductiveMinerDfgOptions::pm4py(0.0)),
            "↺(tau, ×(f, g, h))"
        );

        // ProM's IMfd reuses the unfiltered base cases, so a self edge stays a loop.
        let (labels, base) = log_of(&[&["a"]]);
        let log = base.derive([(vec![0], 100), (vec![0, 0], 1)]);
        let dfg = ActivityDfg::discover(&log);
        assert_eq!(
            discover_dfg(&labels, &dfg, InductiveMinerDfgOptions::imfd(0.2)).to_string(),
            "a"
        );
        assert_eq!(
            discover_dfg(&labels, &dfg, InductiveMinerDfgOptions::prom(0.2)).to_string(),
            "↺(tau, a)"
        );

        // Where all three implement the same thing the presets change nothing.
        for traces in [
            &[&["a", "b", "a", "b"] as &[&str], &["b", "a", "b", "a"]] as &[&[&str]],
            &[&["a", "b", "c"], &["a", "c"]],
        ] {
            assert_eq!(
                mine_with(traces, InductiveMinerDfgOptions::prom(0.0)),
                mine(traces)
            );
        }
    }

    #[test]
    fn test_imfd_drops_infrequent_empty_traces() {
        let (labels, base) = log_of(&[&["a"]]);
        let log = base.derive([(vec![], 1), (vec![0], 1000)]);
        let dfg = ActivityDfg::discover(&log);

        assert_eq!(
            discover_dfg(&labels, &dfg, InductiveMinerDfgOptions::default()).to_string(),
            "×(tau, a)"
        );
        assert_eq!(
            discover_dfg(&labels, &dfg, InductiveMinerDfgOptions::imfd(0.2)).to_string(),
            "a"
        );
    }

    /// IM's filtering example: a single trace in which c came first closes the cycle
    /// a → b → c → a, and only the filtered graph shows the sequence.
    #[test]
    fn test_imfd_ignores_infrequent_behavior() {
        let (labels, base) = log_of(&[&["a", "b", "c"]]);
        let log = base.derive([(vec![0, 1, 2], 100), (vec![2, 0, 1], 1)]);
        let dfg = ActivityDfg::discover(&log);

        assert_eq!(
            discover_dfg(&labels, &dfg, InductiveMinerDfgOptions::imfd(0.2)).to_string(),
            "→(a, b, c)"
        );
    }

    #[test]
    fn test_from_graph() {
        let mut graph = DirectlyFollowsGraph::default();
        graph.add_df_relation("a".into(), "b".into(), 2);
        graph.add_start_activity("a".to_string(), 1);
        graph.add_end_activity("b".to_string(), 1);
        assert_eq!(
            discover_inductive_tree_from_dfg(&graph, InductiveMinerDfgOptions::default())
                .to_string(),
            "→(a, b)"
        );

        graph.add_df_relation("b".into(), "a".into(), 2);
        graph.add_start_activity("b".to_string(), 1);
        graph.add_end_activity("a".to_string(), 1);
        assert_eq!(
            discover_inductive_tree_from_dfg(&graph, InductiveMinerDfgOptions::default())
                .to_string(),
            "∧(a, b)"
        );
    }

    #[test]
    fn test_entry_point() {
        let log = event_log!(["a", "b"], ["a", "b"]);
        let tree = super::super::discover_inductive_tree_from_log(
            &log,
            &EventLogClassifier::default(),
            InductiveMinerDfgOptions::default(),
        );
        assert_eq!(tree.to_string(), "→(a, b)");
    }

    #[test]
    fn test_discovery_is_deterministic() {
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

    /// Runs the variant over real-life logs. Only validity is asserted: unlike IM, the DFG
    /// variant does not guarantee that the model replays the log.
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
                let tree = super::super::discover_inductive_tree_from_log(
                    &log,
                    &EventLogClassifier::default(),
                    InductiveMinerDfgOptions::imfd(noise_threshold),
                );
                assert!(tree.is_valid(), "{name} at {noise_threshold}: {tree}");
                assert!(!tree.find_all_leaves().is_empty(), "{name} is empty");
            }
        }
    }

    #[test]
    fn test_thresholds_from_json_are_clamped() {
        let l81: &[&[&str]] = &[
            &["a", "b", "c", "d"],
            &["d", "a", "b"],
            &["a", "d", "c"],
            &["b", "c", "d"],
        ];
        for (json, clamped) in [
            (r#"{"noise_threshold": -0.5}"#, 0.0),
            (r#"{"noise_threshold": 1.5}"#, 1.0),
        ] {
            let options: InductiveMinerDfgOptions = serde_json::from_str(json).unwrap();
            assert_eq!(
                mine_with(l81, options),
                mine_with(l81, InductiveMinerDfgOptions::imfd(clamped)),
                "for {json}"
            );
        }
    }
}
