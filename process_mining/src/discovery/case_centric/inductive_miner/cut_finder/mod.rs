//! Cut detection: finding the operator that structures a (sub-)log.
//!
//! Every recursion step tries to split the activities of the current log into parts that follow
//! one of the four directly-follows footprints of Definition 5.3: see
//! [`exclusive_choice_cut`], [`sequence_cut`], [`concurrent_cut`] and [`loop_cut()`]. The optional
//! refinements of `IMa` (§6.4.2.1) are [`strict_sequence_cut`], [`interleaved_cut`] and [`coo_cut`].
//! The last two also read the traces.

use super::dfg::ActivityDfg;
use super::log::{ActivityID, ActivityLog};
use super::structures::minimum_self_distance::MinimumSelfDistance;
use super::InductiveMinerOptions;
use cut::Cut;

pub mod concurrent;
pub mod coo;
pub mod cut;
pub mod exclusive_choice;
pub mod interleaved;
pub mod loop_cut;
pub mod sequence;

pub use concurrent::concurrent_cut;
pub use coo::coo_cut;
pub use exclusive_choice::exclusive_choice_cut;
pub use interleaved::interleaved_cut;
pub use loop_cut::loop_cut;
pub use sequence::{sequence_cut, strict_sequence_cut};

/// Translates groups of local [`ActivityDfg`] indices into parts of global [`ActivityID`]s.
fn to_partitions(dfg: &ActivityDfg, groups: &[Vec<usize>]) -> Vec<Vec<ActivityID>> {
    groups
        .iter()
        .map(|group| group.iter().map(|&local| dfg.activity(local)).collect())
        .collect()
}

/// Tries to find a cut in the directly-follows graph of a log.
///
/// The cut types are tried in the order exclusive choice, sequence, interleaving, concurrency,
/// loop, and the first non-trivial cut is returned. A log may adhere to several footprints at
/// once, and the earlier ones lead to more precise models. The
/// [interleaved cut](InductiveMinerOptions::use_interleaved) is optional. Its model allows a subset
/// of what the concurrency allows.
///
/// `dfg` is passed separately because `IMf` runs this on a filtered graph of the same log. The log
/// itself is read for the minimum-self-distance relation and by the cuts that look at traces.
pub fn find_cut(
    log: &ActivityLog,
    dfg: &ActivityDfg,
    options: &InductiveMinerOptions,
) -> Option<Cut> {
    exclusive_choice_cut(dfg)
        .or_else(|| {
            let cut = sequence_cut(dfg)?;
            if options.strict_sequence {
                strict_sequence_cut(dfg, cut)
            } else {
                Some(cut)
            }
        })
        .or_else(|| {
            options
                .use_interleaved
                .then(|| interleaved_cut(log, dfg))
                .flatten()
        })
        .or_else(|| {
            // Without a cut that respects the minimum self-distance, take the plain one before trying
            // the loop. Rediscovery needs this.
            let minimum_self_distance = options
                .use_minimum_self_distance
                .then(|| MinimumSelfDistance::discover(log));
            let cut = minimum_self_distance
                .and_then(|msd| concurrent_cut(dfg, Some(&msd)))
                .or_else(|| concurrent_cut(dfg, None))?;
            Some(if options.use_inclusive_choice {
                coo_cut(log, cut, options.noise_threshold)
            } else {
                cut
            })
        })
        .or_else(|| loop_cut(dfg))
}

/// Cut detection of the Inductive Miner - infrequent (`IMf`).
///
/// The unfiltered graph is tried first, as in plain IM. Only if that fails are infrequent edges
/// removed (see [`ActivityDfg::filtered`]) and the same cut types tried again. This keeps IM's
/// rediscoverability: on a complete, noise-free log `IMf` discovers what IM discovers.
///
/// With a [`noise_threshold`](InductiveMinerOptions::noise_threshold) of `0.0` this is exactly
/// [`find_cut`].
pub fn find_cut_filtering(
    log: &ActivityLog,
    dfg: &ActivityDfg,
    options: &InductiveMinerOptions,
) -> Option<Cut> {
    find_cut(log, dfg, options).or_else(|| {
        (options.noise_threshold > 0.0)
            .then(|| find_cut(log, &dfg.filtered(options.noise_filter()), options))
            .flatten()
    })
}

#[cfg(test)]
pub(crate) mod test_utils {
    use super::cut::Cut;
    use super::ActivityDfg;
    use super::ActivityLog;
    use super::MinimumSelfDistance;
    use crate::core::process_models::process_tree::OperatorType;

    /// Largest graph [`all_valid_cuts`] enumerates over.
    pub const EXHAUSTIVE_LIMIT: usize = 8;

    /// Runs a cut finder on a log given as activity names and returns the parts as names.
    pub fn cut_of(
        traces: &[&[&str]],
        find: impl Fn(&ActivityDfg) -> Option<Cut>,
    ) -> Option<Vec<Vec<String>>> {
        let (labels, log) = super::super::log::test_utils::log_of(traces);
        find(&ActivityDfg::discover(&log)).map(|cut| {
            cut.partitions()
                .iter()
                .map(|part| part.iter().map(|&a| labels[a].clone()).collect())
                .collect()
        })
    }

    /// Builds the expected value of [`cut_of`].
    pub fn parts(parts: &[&[&str]]) -> Option<Vec<Vec<String>>> {
        Some(
            parts
                .iter()
                .map(|p| p.iter().map(|a| a.to_string()).collect())
                .collect(),
        )
    }

    /// Finds every non-trivial cut of the graph by testing all partitions of its activities
    /// against all four footprints.
    ///
    /// `minimum_self_distance` must be passed whenever the miner uses it, since it forbids
    /// concurrent cuts the footprints alone allow (∧↔.1), unless it forbids all of them.
    /// Interleaved cuts are not enumerated. Returns `None` if the graph has more than
    /// [`EXHAUSTIVE_LIMIT`] activities.
    pub fn all_valid_cuts(
        log: &ActivityLog,
        dfg: &ActivityDfg,
        minimum_self_distance: Option<&MinimumSelfDistance>,
    ) -> Option<Vec<Cut>> {
        let n = dfg.len();
        if n > EXHAUSTIVE_LIMIT {
            return None;
        }

        let mut cuts = Vec::new();
        let mut separating_loops = Vec::new();
        for partition in set_partitions(n) {
            if partition.len() < 2 {
                continue;
            }
            for operator in [
                OperatorType::ExclusiveChoice,
                OperatorType::Sequence,
                OperatorType::Concurrency,
                OperatorType::Loop,
            ] {
                let Some(cut) = as_cut(log, dfg, &partition, operator) else {
                    continue;
                };
                // ∧↔.1: an activity that loops around another may not be separated from it.
                if operator == OperatorType::Concurrency {
                    if let Some(msd) = minimum_self_distance {
                        let separated_loop = partition.iter().enumerate().any(|(i, part)| {
                            partition.iter().skip(i + 1).any(|other| {
                                part.iter().any(|&a| {
                                    other
                                        .iter()
                                        .any(|&b| msd.are_related(dfg.activity(a), dfg.activity(b)))
                                })
                            })
                        });
                        if separated_loop {
                            separating_loops.push(cut);
                            continue;
                        }
                    }
                }
                cuts.push(cut);
            }
        }
        if !cuts
            .iter()
            .any(|cut| cut.operator() == OperatorType::Concurrency)
        {
            cuts.extend(separating_loops);
        }
        Some(cuts)
    }

    /// Builds the cut this partition would form under `operator`, if it adheres to the footprint.
    ///
    /// The order of the parts is forced: a sequence is ordered by reachability, and the body of a
    /// loop is the part holding every start and end activity.
    fn as_cut(
        log: &ActivityLog,
        dfg: &ActivityDfg,
        partition: &[Vec<usize>],
        operator: OperatorType,
    ) -> Option<Cut> {
        let mut parts = partition.to_vec();

        match operator {
            OperatorType::Sequence => {
                // The first part reaches every other, the second all but the first, and so on.
                // Sorting by that count stays well defined on partitions that are no sequence cut,
                // which the footprint check below rejects.
                let reaches = transitive_closure(dfg);
                let n = dfg.len();
                let reach_count = |part: &Vec<usize>| {
                    partition
                        .iter()
                        .filter(|other| other[0] != part[0] && reaches[part[0] * n + other[0]])
                        .count()
                };
                parts.sort_by_key(|part| std::cmp::Reverse(reach_count(part)));
            }
            OperatorType::Loop => {
                let body = parts.iter().position(|part| {
                    dfg.start_activities()
                        .chain(dfg.end_activities())
                        .all(|a| part.contains(&a))
                })?;
                parts.swap(0, body);
            }
            _ => {}
        }

        let cut = Cut::new(
            operator,
            parts
                .iter()
                .map(|part| part.iter().map(|&a| dfg.activity(a)).collect())
                .collect(),
        );
        footprint_violation(log, dfg, &cut).is_none().then_some(cut)
    }

    /// All partitions of `{0, ..., n-1}`, as restricted growth strings.
    fn set_partitions(n: usize) -> Vec<Vec<Vec<usize>>> {
        if n == 0 {
            return vec![];
        }
        let mut assignment = vec![0usize; n];
        let mut partitions = Vec::new();

        loop {
            let blocks = assignment.iter().max().copied().unwrap_or(0) + 1;
            let mut parts = vec![Vec::new(); blocks];
            for (element, &block) in assignment.iter().enumerate() {
                parts[block].push(element);
            }
            partitions.push(parts);

            // Next restricted growth string: an element may move to any existing block or open
            // one new block, so position `i` may grow while it is at most `max(a[..i])`.
            let mut position = n;
            loop {
                if position == 1 {
                    return partitions;
                }
                position -= 1;
                let highest_before = assignment[..position].iter().max().copied().unwrap_or(0);
                if assignment[position] <= highest_before {
                    assignment[position] += 1;
                    assignment[position + 1..].fill(0);
                    break;
                }
            }
        }
    }

    /// Checks a cut against the requirements of its operator, transcribed from Definition 5.3
    /// independently of the cut finders.
    ///
    /// Returns the first violated requirement, or `None` if the cut adheres to them.
    pub fn footprint_violation(log: &ActivityLog, dfg: &ActivityDfg, cut: &Cut) -> Option<String> {
        let parts: Vec<Vec<usize>> = cut
            .partitions()
            .iter()
            .map(|part| part.iter().filter_map(|&a| dfg.local_index(a)).collect())
            .collect();

        match cut.operator() {
            OperatorType::ExclusiveChoice => check_exclusive_choice(dfg, &parts),
            OperatorType::Sequence => check_sequence(dfg, &parts),
            OperatorType::Concurrency | OperatorType::InclusiveChoice => {
                check_concurrent(dfg, &parts)
            }
            OperatorType::Loop => check_loop(dfg, &parts),
            OperatorType::Interleaving => check_interleaved(log, dfg, &parts),
        }
    }

    /// Requirement ↔.1, all and only the edges from an end to a start activity between parts,
    /// and that every trace runs the activities of a part as one uninterrupted block.
    fn check_interleaved(
        log: &ActivityLog,
        dfg: &ActivityDfg,
        parts: &[Vec<usize>],
    ) -> Option<String> {
        let violation = for_each_cross_pair(parts, |i, a, j, b| {
            [(a, i, b, j), (b, j, a, i)]
                .into_iter()
                .find(|&(from, _, to, _)| {
                    dfg.follows(from, to) != (dfg.is_end(from) && dfg.is_start(to))
                })
                .map(|(from, k, to, l)| {
                    let follows = dfg.follows(from, to);
                    format!("↔.1 violated: {from} (Σ{k}) → {to} (Σ{l}) is {follows}")
                })
        });
        if violation.is_some() {
            return violation;
        }

        let mut part_of = vec![usize::MAX; log.alphabet_size()];
        for (index, part) in parts.iter().enumerate() {
            for &local in part {
                part_of[dfg.activity(local)] = index;
            }
        }
        for variant in log.variants() {
            let mut order: Vec<usize> = Vec::new();
            for &activity in &variant.activities {
                let part = part_of[activity];
                if order.last() != Some(&part) {
                    if order.contains(&part) {
                        return Some(format!("Σ{part} occurs in two separate blocks"));
                    }
                    order.push(part);
                }
            }
        }
        None
    }

    /// Requirement ×.1: no part is connected to any other part.
    fn check_exclusive_choice(dfg: &ActivityDfg, parts: &[Vec<usize>]) -> Option<String> {
        for_each_cross_pair(parts, |i, a, j, b| {
            (dfg.follows(a, b) || dfg.follows(b, a))
                .then(|| format!("×.1 violated: Σ{i} and Σ{j} are connected via {a} and {b}"))
        })
    }

    /// Requirement →.1: each part reaches all later parts and none of the earlier ones.
    fn check_sequence(dfg: &ActivityDfg, parts: &[Vec<usize>]) -> Option<String> {
        let n = dfg.len();
        let reaches = transitive_closure(dfg);
        for_each_cross_pair(parts, |i, a, j, b| {
            let (forwards, backwards) = (reaches[a * n + b], reaches[b * n + a]);
            (!forwards || backwards).then(|| {
                format!(
                    "→.1 violated: Σ{i} before Σ{j}, but {a} ⇝ {b} is {forwards} \
                     and {b} ⇝ {a} is {backwards}"
                )
            })
        })
    }

    /// Requirements ∧.1 and ∧.2: every part has a start and an end activity, and all parts are
    /// fully interconnected.
    fn check_concurrent(dfg: &ActivityDfg, parts: &[Vec<usize>]) -> Option<String> {
        for (i, part) in parts.iter().enumerate() {
            if !part.iter().any(|&a| dfg.is_start(a)) {
                return Some(format!("∧.1 violated: Σ{i} has no start activity"));
            }
            if !part.iter().any(|&a| dfg.is_end(a)) {
                return Some(format!("∧.1 violated: Σ{i} has no end activity"));
            }
        }

        for_each_cross_pair(parts, |i, a, j, b| {
            (!dfg.follows(a, b) || !dfg.follows(b, a)).then(|| {
                format!("∧.2 violated: {a} (Σ{i}) and {b} (Σ{j}) are not connected both ways")
            })
        })
    }

    /// Requirements ↺.1 to ↺.4.
    fn check_loop(dfg: &ActivityDfg, parts: &[Vec<usize>]) -> Option<String> {
        let body = &parts[0];
        let redo = &parts[1..];

        // ↺.1: all start and end activities are in the body.
        for activity in dfg.start_activities().chain(dfg.end_activities()) {
            if !body.contains(&activity) {
                return Some(format!(
                    "↺.1 violated: {activity} starts or ends a trace but is not in the body"
                ));
            }
        }

        for (i, part) in redo.iter().enumerate() {
            for &b in part {
                // ↺.2: edges between the body and a redo part only touch end resp. start
                // activities of the body.
                for &a in body {
                    if dfg.follows(a, b) && !dfg.is_end(a) {
                        return Some(format!(
                            "↺.2 violated: {a} → {b} leaves the body at a non-end activity"
                        ));
                    }
                    if dfg.follows(b, a) && !dfg.is_start(a) {
                        return Some(format!(
                            "↺.2 violated: {b} → {a} enters the body at a non-start activity"
                        ));
                    }
                }

                // ↺.4: a redo part connects either to all start / end activities or to none.
                let to_start = dfg
                    .start_activities()
                    .filter(|&s| dfg.follows(b, s))
                    .count();
                if to_start > 0 && to_start < dfg.start_activities().count() {
                    return Some(format!(
                        "↺.4 violated: {b} leads back to some but not all start activities"
                    ));
                }
                let from_end = dfg.end_activities().filter(|&e| dfg.follows(e, b)).count();
                if from_end > 0 && from_end < dfg.end_activities().count() {
                    return Some(format!(
                        "↺.4 violated: {b} is reached from some but not all end activities"
                    ));
                }
            }

            // ↺.3: redo parts are not connected to each other.
            for (j, other) in redo.iter().enumerate().skip(i + 1) {
                for &a in part {
                    for &b in other {
                        if dfg.follows(a, b) || dfg.follows(b, a) {
                            return Some(format!(
                                "↺.3 violated: redo parts {i} and {j} are connected via {a} and {b}"
                            ));
                        }
                    }
                }
            }
        }

        None
    }

    /// Calls `check` for every pair of activities from two different parts, with `i < j`.
    fn for_each_cross_pair(
        parts: &[Vec<usize>],
        check: impl Fn(usize, usize, usize, usize) -> Option<String>,
    ) -> Option<String> {
        for (i, left) in parts.iter().enumerate() {
            for (j, right) in parts.iter().enumerate().skip(i + 1) {
                for &a in left {
                    for &b in right {
                        if let Some(problem) = check(i, a, j, b) {
                            return Some(problem);
                        }
                    }
                }
            }
        }
        None
    }

    /// Transitive closure as a row-major `n * n` matrix, independent of the `Reachability` the
    /// sequence cut uses.
    fn transitive_closure(dfg: &ActivityDfg) -> Vec<bool> {
        let n = dfg.len();
        let mut reaches = vec![false; n * n];
        for from in 0..n {
            for to in 0..n {
                reaches[from * n + to] = dfg.follows(from, to);
            }
        }
        for via in 0..n {
            for from in 0..n {
                if reaches[from * n + via] {
                    for to in 0..n {
                        reaches[from * n + to] |= reaches[via * n + to];
                    }
                }
            }
        }
        reaches
    }
}

#[cfg(test)]
mod test_find_cut {
    use super::super::log::test_utils::log_of;
    use super::*;
    use crate::core::process_models::process_tree::OperatorType;

    fn cut_of(traces: &[&[&str]]) -> Option<Cut> {
        let (_, log) = log_of(traces);
        find_cut(
            &log,
            &ActivityDfg::discover(&log),
            &InductiveMinerOptions::im_thesis(),
        )
    }

    #[test]
    fn test_choice_wins_over_sequence() {
        let cut = cut_of(&[&["a", "b"], &["c"]]).unwrap();
        assert_eq!(cut.operator(), OperatorType::ExclusiveChoice);
    }

    #[test]
    fn test_sequence_wins_over_concurrency() {
        let cut = cut_of(&[&["a", "b"], &["a", "b"]]).unwrap();
        assert_eq!(cut.operator(), OperatorType::Sequence);
    }

    #[test]
    fn test_concurrency_wins_over_loop() {
        let cut = cut_of(&[&["a", "b"], &["b", "a"]]).unwrap();
        assert_eq!(cut.operator(), OperatorType::Concurrency);
    }

    #[test]
    fn test_loop_is_found_last() {
        let cut = cut_of(&[&["a", "c"], &["a", "c", "b", "a", "c"]]).unwrap();
        assert_eq!(cut.operator(), OperatorType::Loop);
    }

    #[test]
    fn test_log_without_any_cut() {
        assert!(cut_of(&[
            &["a", "b", "c", "d"],
            &["d", "a", "b"],
            &["a", "d", "c"],
            &["b", "c", "d"],
        ])
        .is_none());
    }

    #[test]
    fn test_filtering_is_only_tried_after_plain_cut_detection() {
        // A clean sequence a to b with a single deviating trace b, a. IM sees the two as
        // concurrent since both edges are present, and so does IMf, because IM's cut detection is
        // tried first and succeeds.
        let (_, log) = log_of(&[&["a", "b"], &["b", "a"]]);
        let log = log.derive([(vec![0, 1], 100), (vec![1, 0], 1)]);
        let dfg = ActivityDfg::discover(&log);

        assert_eq!(
            find_cut(&log, &dfg, &InductiveMinerOptions::im_thesis()).map(|c| c.operator()),
            Some(OperatorType::Concurrency)
        );
        assert_eq!(
            find_cut_filtering(&log, &dfg, &InductiveMinerOptions::imf(0.2)).map(|c| c.operator()),
            Some(OperatorType::Concurrency)
        );
    }

    #[test]
    fn test_filtering_finds_a_cut_that_im_misses() {
        // A clean sequence a → b → c, plus one trace in which c came first. That single trace
        // closes the cycle a → b → c → a, which leaves IM without any cut at all.
        let (_, log) = log_of(&[&["a", "b", "c"]]);
        let log = log.derive([(vec![0, 1, 2], 100), (vec![2, 0, 1], 1)]);
        let dfg = ActivityDfg::discover(&log);

        assert!(find_cut(&log, &dfg, &InductiveMinerOptions::im_thesis()).is_none());
        assert!(find_cut_filtering(&log, &dfg, &InductiveMinerOptions::im_thesis()).is_none());

        let cut = find_cut_filtering(&log, &dfg, &InductiveMinerOptions::imf(0.2)).unwrap();
        assert_eq!(cut.operator(), OperatorType::Sequence);
        assert_eq!(cut.partitions(), &[vec![0], vec![1], vec![2]]);
    }
}
