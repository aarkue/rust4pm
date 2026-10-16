//! The tau-loop fall throughs.

use super::super::dfg::ActivityDfg;
use super::super::log::{ActivityID, ActivityLog};
use super::Fallthrough;

/// Detects looping behaviour by cutting traces where one execution seems to end and the next to
/// begin.
///
/// If a trace can be cut into several pieces that each look like a full trace, the log plausibly
/// describes something being repeated. The pieces then form a new log `L'` and the model becomes
/// `↺(IM(L'), τ)`: do `L'`, then optionally do it again.
///
/// Both variants below work this way and only differ in where they cut. They discard the
/// information about which pieces belonged together, which is why they are tried only after the
/// activity fall throughs.
fn tau_loop_over(
    log: &ActivityLog,
    keep_degenerate: bool,
    split_before: impl Fn(&[ActivityID], usize) -> bool,
) -> Option<Fallthrough> {
    let mut pieces: Vec<(Vec<ActivityID>, u64)> = Vec::new();

    for variant in log.variants() {
        let trace = &variant.activities;
        let mut piece_start = 0;
        for position in 1..trace.len() {
            if split_before(trace, position) {
                pieces.push((trace[piece_start..position].to_vec(), variant.count));
                piece_start = position;
            }
        }
        pieces.push((trace[piece_start..].to_vec(), variant.count));
    }

    let split = log.derive(pieces);
    // If nothing was cut, recursing would loop forever. If every piece is a single event, the
    // recursion can only find the flower model, which would pre-empt the later fall throughs.
    let worthwhile = split.num_traces() > log.num_traces()
        && (keep_degenerate || split.num_events() > split.num_traces());
    worthwhile.then_some(Fallthrough::TauLoop(split))
}

/// Cuts traces where an end activity is directly followed by a start activity.
///
/// One execution ending and the next beginning is exactly what that looks like. For example, with
/// start activities `{a, b, d}` and end activities `{b, c, d}`, the trace `⟨a, b, c, d⟩` is cut
/// into `⟨a, b, c⟩` and `⟨d⟩`. Returns `None` if no trace could be cut.
pub fn strict_tau_loop(
    log: &ActivityLog,
    dfg: &ActivityDfg,
    keep_degenerate: bool,
) -> Option<Fallthrough> {
    let is_start = lookup(log, dfg, dfg.start_activities());
    let is_end = lookup(log, dfg, dfg.end_activities());
    tau_loop_over(log, keep_degenerate, |trace, position| {
        is_end[trace[position - 1]] && is_start[trace[position]]
    })
}

/// Cuts traces before every occurrence of a start activity.
///
/// Unlike [`strict_tau_loop`], this does not require the previous execution to have finished.
/// With start activities `{a, b, d}`, the
/// trace `⟨a, b, c, d⟩` is cut into `⟨a⟩`, `⟨b, c⟩` and `⟨d⟩`. The shorter pieces retain less of
/// the original log, so [`strict_tau_loop`] is tried first. Returns `None` if no trace could be
/// cut.
pub fn tau_loop(
    log: &ActivityLog,
    dfg: &ActivityDfg,
    keep_degenerate: bool,
) -> Option<Fallthrough> {
    let is_start = lookup(log, dfg, dfg.start_activities());
    tau_loop_over(log, keep_degenerate, |trace, position| {
        is_start[trace[position]]
    })
}

/// Marks the given local activities of `dfg` in a lookup indexed by activity id.
fn lookup(log: &ActivityLog, dfg: &ActivityDfg, locals: impl Iterator<Item = usize>) -> Vec<bool> {
    let mut marked = vec![false; log.alphabet_size()];
    for local in locals {
        marked[dfg.activity(local)] = true;
    }
    marked
}

#[cfg(test)]
mod test_tau_loop {
    use super::super::super::log::test_utils::{describe, expect, log_of};
    use super::*;

    /// Leemans' running example L81, with start activities {a, b, d} and end activities {b, c, d}.
    fn leemans_example() -> (Vec<String>, ActivityLog) {
        log_of(&[
            &["a", "b", "c", "d"],
            &["d", "a", "b"],
            &["a", "d", "c"],
            &["b", "c", "d"],
        ])
    }

    #[test]
    fn test_strict_variant_cuts_where_one_execution_ended() {
        let (labels, log) = leemans_example();
        let dfg = ActivityDfg::discover(&log);

        let Some(Fallthrough::TauLoop(split)) = strict_tau_loop(&log, &dfg, false) else {
            panic!("expected the fall through to apply");
        };
        assert_eq!(
            describe(&split, &labels),
            expect(&[
                (&["a", "b", "c"], 1),
                (&["a", "b"], 1),
                (&["a", "d", "c"], 1),
                (&["b", "c"], 1),
                (&["d"], 3),
            ])
        );
    }

    #[test]
    fn test_loose_variant_cuts_before_every_start_activity() {
        let (labels, log) = leemans_example();
        let dfg = ActivityDfg::discover(&log);

        let Some(Fallthrough::TauLoop(split)) = tau_loop(&log, &dfg, false) else {
            panic!("expected the fall through to apply");
        };
        assert_eq!(
            describe(&split, &labels),
            expect(&[
                (&["a"], 3),
                (&["b", "c"], 2),
                (&["b"], 1),
                (&["d", "c"], 1),
                (&["d"], 3),
            ])
        );
    }

    #[test]
    fn test_does_not_apply_when_nothing_can_be_cut() {
        let (_, log) = log_of(&[&["a", "b", "c"]]);
        let dfg = ActivityDfg::discover(&log);
        assert!(strict_tau_loop(&log, &dfg, false).is_none());
        assert!(tau_loop(&log, &dfg, false).is_none());

        // a starts traces and b ends them, so ⟨a, a, b⟩ has a start activity in the middle, but
        // the activity before it does not end a trace.
        let (_, log) = log_of(&[&["a", "b"], &["a", "a", "b"]]);
        let dfg = ActivityDfg::discover(&log);
        assert!(strict_tau_loop(&log, &dfg, false).is_none());
        assert!(tau_loop(&log, &dfg, false).is_some());
    }
}
