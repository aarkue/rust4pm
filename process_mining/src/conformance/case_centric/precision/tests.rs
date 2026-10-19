use std::{collections::HashMap, fs::File};

use flate2::read::GzDecoder;
use uuid::Uuid;

use super::{
    escaping_edges_precision, escaping_edges_precision_from_alignments,
    replay::{Replayer, Symbol},
    EscapingEdge, EscapingEdgesOptions, EscapingEdgesPrecision, LogPaths, PathStep, PrecisionError,
    SilentTransitions, TraceEnds,
};
use crate::{
    conformance::alignments::{
        align_log, sync_prod_net::SyncProdNetConstructionError, AlignmentMove, AlignmentOptions,
        AlignmentResult, VariantAlignmentResult,
    },
    core::{
        event_data::case_centric::utils::activity_projection::{
            log_to_activity_projection, EventLogActivityProjection,
        },
        process_models::petri_net::{ArcType, TransitionID},
    },
    event_log,
    test_utils::get_test_data_path,
    EventLog, Importable, PetriNet,
};

/// Transitions as `(label, input places, output places)`
type Spec<'a> = &'a [(Option<&'a str>, &'a [&'a str], &'a [&'a str])];

/// Builds a net with one token in `initial` and the final marking `final_`.
///
/// Ids are fixed (transition `i` is [`transition_id`]`(i)`), so alignment ties break the same way
/// in every run.
fn net(transitions: Spec<'_>, initial: &str, final_: &str) -> PetriNet {
    let mut net = PetriNet::new();
    let mut places = HashMap::new();
    let mut place = |net: &mut PetriNet, name: &str| {
        let id = Uuid::from_u128(places.len() as u128);
        *places
            .entry(name.to_string())
            .or_insert_with(|| net.add_place(Some(id)))
    };
    for (index, (label, inputs, outputs)) in transitions.iter().enumerate() {
        let t = net.add_transition(label.map(str::to_string), Some(transition_id(index).0));
        for input in *inputs {
            let p = place(&mut net, input);
            net.add_arc(ArcType::place_to_transition(p, t), None);
        }
        for output in *outputs {
            let p = place(&mut net, output);
            net.add_arc(ArcType::transition_to_place(t, p), None);
        }
    }
    net.initial_marking = Some([(place(&mut net, initial), 1)].into());
    net.final_markings = Some(vec![[(place(&mut net, final_), 1)].into()]);
    net
}

fn replayer(
    net: &PetriNet,
    silent_transitions: SilentTransitions,
    search_limit: Option<usize>,
) -> Result<Replayer, PrecisionError> {
    let options = EscapingEdgesOptions {
        silent_transitions,
        search_limit,
        ..Default::default()
    };
    Replayer::new(net, &options)
}

fn transition_id(index: usize) -> TransitionID {
    TransitionID(Uuid::from_u128(1000 + index as u128))
}

fn activity(label: &str) -> PathStep {
    PathStep::Activity(label.to_string())
}

/// What is available after replaying `path` from the initial marking, sorted
fn available_after(replayer: &mut Replayer, path: &[Symbol]) -> Vec<PathStep> {
    let mut markings: Box<[_]> = [replayer.initial()].into();
    for &symbol in path {
        markings = replayer.step(&markings, symbol);
    }
    assert!(!markings.is_empty(), "path is replayable");
    let available = replayer.available_in_any(&markings).into_owned();
    let mut steps: Vec<_> = available.iter().map(|s| replayer.path_step(s)).collect();
    steps.sort();
    steps
}

/// `a`, then a silent choice between `b` and `c`
const SILENT_CHOICE: Spec<'static> = &[
    (Some("a"), &["p0"], &["p1"]),
    (None, &["p1"], &["p2"]),
    (None, &["p1"], &["p3"]),
    (Some("b"), &["p2"], &["pf"]),
    (Some("c"), &["p3"], &["pf"]),
];

/// Two transitions labelled `a`, the second one behind a silent transition
const DUPLICATE_LABELS: Spec<'static> = &[
    (Some("a"), &["p0"], &["p1"]),
    (None, &["p0"], &["p2"]),
    (Some("a"), &["p2"], &["p3"]),
    (Some("b"), &["p1"], &["pf"]),
    (Some("c"), &["p3"], &["pf"]),
];

#[test]
fn skip_expands_silent_choice() {
    let net = net(SILENT_CHOICE, "p0", "pf");
    let mut replayer = replayer(&net, SilentTransitions::Skip, None).unwrap();
    let a = replayer.activity_symbol("a").unwrap();
    assert_eq!(
        available_after(&mut replayer, &[a]),
        [activity("b"), activity("c")]
    );
}

#[test]
fn as_activities_counts_silent_transitions() {
    let net = net(SILENT_CHOICE, "p0", "pf");
    let mut replayer = replayer(&net, SilentTransitions::AsActivities, None).unwrap();
    let a = replayer
        .transition_symbol(transition_id(0))
        .unwrap()
        .unwrap();
    assert_eq!(
        available_after(&mut replayer, &[a]),
        [
            PathStep::Transition(transition_id(1)),
            PathStep::Transition(transition_id(2))
        ]
    );
}

#[test]
fn duplicate_labels_reach_all_markings() {
    let net = net(DUPLICATE_LABELS, "p0", "pf");
    let mut replayer = replayer(&net, SilentTransitions::Skip, None).unwrap();
    let a = replayer.activity_symbol("a").unwrap();
    let initial = replayer.initial();
    assert_eq!(replayer.step(&[initial], a).len(), 2);
    assert_eq!(
        available_after(&mut replayer, &[a]),
        [activity("b"), activity("c")]
    );
}

#[test]
fn silent_self_loop_terminates() {
    let net = net(
        &[
            (Some("a"), &["p0"], &["p1"]),
            (None, &["p1"], &["p1"]),
            (Some("b"), &["p1"], &["pf"]),
        ],
        "p0",
        "pf",
    );
    let mut replayer = replayer(&net, SilentTransitions::Skip, None).unwrap();
    let a = replayer.activity_symbol("a").unwrap();
    assert_eq!(available_after(&mut replayer, &[a]), [activity("b")]);
    assert!(!replayer.limit_reached);
}

#[test]
fn token_generating_silent_transition_hits_limit() {
    let net = net(
        &[
            (Some("a"), &["p0"], &["p1"]),
            (None, &["p1"], &["p1", "acc"]),
            (Some("b"), &["p1"], &["pf"]),
        ],
        "p0",
        "pf",
    );
    let mut replayer = replayer(&net, SilentTransitions::Skip, Some(50)).unwrap();
    let a = replayer.activity_symbol("a").unwrap();
    assert_eq!(available_after(&mut replayer, &[a]), [activity("b")]);
    assert!(replayer.limit_reached);
}

#[test]
fn missing_initial_marking_is_an_error() {
    let mut net = net(SILENT_CHOICE, "p0", "pf");
    net.initial_marking = None;
    assert_eq!(
        replayer(&net, SilentTransitions::Skip, None).err(),
        Some(PrecisionError::InvalidNet(
            SyncProdNetConstructionError::NoInitialMarking
        ))
    );
}

fn measure(
    net: &PetriNet,
    log: &EventLog,
    options: &EscapingEdgesOptions,
) -> EscapingEdgesPrecision {
    escaping_edges_precision(net, &log_to_activity_projection(log), options).unwrap()
}

fn assert_precision(net: &PetriNet, log: &EventLog, options: &EscapingEdgesOptions, expected: f64) {
    let precision = measure(net, log, options).precision;
    assert!(
        (precision - expected).abs() < 1e-12,
        "precision {precision}, expected {expected}"
    );
}

fn as_activities() -> EscapingEdgesOptions {
    EscapingEdgesOptions {
        silent_transitions: SilentTransitions::AsActivities,
        ..Default::default()
    }
}

/// `A`, then `B`
const SEQUENCE: Spec<'static> = &[(Some("A"), &["p0"], &["p1"]), (Some("B"), &["p1"], &["pf"])];

/// `A`, then `B` or `C`
const UNUSED_BRANCH: Spec<'static> = &[
    (Some("A"), &["p0"], &["p1"]),
    (Some("B"), &["p1"], &["pf"]),
    (Some("C"), &["p1"], &["pf"]),
];

/// `A`, `B`, then `C` or `E`
const SKIPPABLE: Spec<'static> = &[
    (Some("A"), &["p0"], &["p1"]),
    (Some("B"), &["p1"], &["p2"]),
    (Some("C"), &["p2"], &["pf"]),
    (Some("E"), &["p2"], &["pf"]),
];

#[test]
fn matching_model_is_precise() {
    let log = event_log!(["A", "B"], ["A", "B"], ["A", "B"], ["A", "B"], ["A", "B"]);
    assert_precision(&net(SEQUENCE, "p0", "pf"), &log, &Default::default(), 1.0);
}

#[test]
fn unused_branch_escapes() {
    let log = event_log!(["A", "B"]);
    assert_precision(
        &net(UNUSED_BRANCH, "p0", "pf"),
        &log,
        &Default::default(),
        2.0 / 3.0,
    );
}

#[test]
fn adding_a_fitting_trace_can_lower_precision() {
    let xs: Vec<String> = (1..=10).map(|i| format!("x{i}")).collect();
    let mut spec: Vec<(Option<&str>, &[&str], &[&str])> = vec![
        (Some("a"), &["p0"], &["p1"]),
        (Some("b"), &["p1"], &["pf"]),
        (Some("c"), &["p1"], &["p2"]),
    ];
    spec.extend(
        xs.iter()
            .map(|x| (Some(x.as_str()), &["p2"][..], &["pf"][..])),
    );
    let net = net(&spec, "p0", "pf");
    let options = EscapingEdgesOptions::default();
    assert_precision(&net, &event_log!(["a", "b"]), &options, 2.0 / 3.0);
    assert_precision(
        &net,
        &event_log!(["a", "b"], ["a", "c", "x1"]),
        &options,
        7.0 / 16.0,
    );
}

#[test]
fn silent_transitions_skip_vs_as_activities() {
    let net = net(SILENT_CHOICE, "p0", "pf");
    let log = event_log!(["a", "b"]);
    assert_precision(&net, &log, &Default::default(), 2.0 / 3.0);
    assert_precision(&net, &log, &as_activities(), 0.75);
}

#[test]
fn duplicate_labels_union_markings() {
    // pm4py reports 1.0: it only expands the marking reached with the fewest silent moves
    let log = event_log!(["a", "b"]);
    assert_precision(
        &net(DUPLICATE_LABELS, "p0", "pf"),
        &log,
        &Default::default(),
        2.0 / 3.0,
    );
}

#[test]
fn trace_ends_count_only_when_included() {
    let net = net(
        &[
            (Some("A"), &["p0"], &["p1"]),
            (Some("B"), &["p1"], &["pf"]),
            (Some("D"), &["pf"], &["p1"]),
        ],
        "p0",
        "pf",
    );
    let log = event_log!(["A", "B"]);
    assert_precision(&net, &log, &Default::default(), 2.0 / 3.0);
    let without_ends = EscapingEdgesOptions {
        trace_ends: TraceEnds::Ignore,
        ..Default::default()
    };
    assert_precision(&net, &log, &without_ends, 1.0);
}

#[test]
fn aligned_repairs_skipped_activity() {
    let log = event_log!(["A", "C"]);
    assert_precision(&net(SKIPPABLE, "p0", "pf"), &log, &Default::default(), 0.75);
}

#[test]
fn unknown_activity_is_a_log_move_when_aligned() {
    let log = event_log!(["A", "X", "B"]);
    assert_precision(
        &net(UNUSED_BRANCH, "p0", "pf"),
        &log,
        &Default::default(),
        2.0 / 3.0,
    );
}

#[test]
fn prom_preset_ignores_initial_choices() {
    let net = net(
        &[(Some("A"), &["p0"], &["pf"]), (Some("B"), &["p0"], &["pf"])],
        "p0",
        "pf",
    );
    let log = event_log!(["A"]);
    assert_precision(&net, &log, &EscapingEdgesOptions::prom(), 1.0);
    assert_precision(&net, &log, &Default::default(), 0.5);
}

#[test]
fn escaping_edges_are_sorted() {
    // `D` comes first in the net, so it gets the smaller symbol
    let net = net(
        &[
            (Some("A"), &["p0"], &["p1"]),
            (Some("B"), &["p1"], &["pf"]),
            (Some("D"), &["p1"], &["pf"]),
            (Some("C"), &["p1"], &["pf"]),
        ],
        "p0",
        "pf",
    );
    let options = EscapingEdgesOptions {
        report_escaping_edges: true,
        ..Default::default()
    };
    let edge = |escaping| EscapingEdge {
        prefix: vec![activity("A")],
        escaping: activity(escaping),
        weight: 1,
    };
    assert_eq!(
        measure(&net, &event_log!(["A", "B"]), &options).escaping_edges,
        Some(vec![edge("C"), edge("D")])
    );
}

#[test]
fn empty_log_is_precise() {
    let net = net(SEQUENCE, "p0", "pf");
    let result = measure(&net, &event_log!(), &Default::default());
    assert_eq!((result.precision, result.weighted_available), (1.0, 0));
    assert_precision(&net, &event_log!([]), &Default::default(), 1.0);
}

#[test]
fn failed_alignments_are_counted() {
    let options = EscapingEdgesOptions {
        log_paths: LogPaths::Aligned(AlignmentOptions {
            max_states: Some(1),
            ..Default::default()
        }),
        ..Default::default()
    };
    let result = measure(
        &net(UNUSED_BRANCH, "p0", "pf"),
        &event_log!(["A", "B"]),
        &options,
    );
    assert_eq!(result.unaligned_traces, 1);
}

#[test]
fn from_alignments_matches_direct() {
    let net = net(SKIPPABLE, "p0", "pf");
    let log = log_to_activity_projection(&event_log!(["A", "C"], ["A", "B", "E"]));
    let options = EscapingEdgesOptions::default();
    let alignments = align_log(&net, &log, &AlignmentOptions::default());
    assert_eq!(
        escaping_edges_precision_from_alignments(&net, &alignments, &options),
        escaping_edges_precision(&net, &log, &options)
    );
}

#[test]
fn incompatible_options_are_rejected() {
    let net = net(SEQUENCE, "p0", "pf");
    let replay_transitions = EscapingEdgesOptions {
        log_paths: LogPaths::ReplayUntilDeviation,
        ..as_activities()
    };
    let log = log_to_activity_projection(&event_log!(["A", "B"]));
    assert!(matches!(
        escaping_edges_precision(&net, &log, &replay_transitions),
        Err(PrecisionError::IncompatibleOptions(_))
    ));
    assert!(matches!(
        escaping_edges_precision_from_alignments(&net, &[], &EscapingEdgesOptions::pm4py()),
        Err(PrecisionError::IncompatibleOptions(_))
    ));
}

#[test]
fn replay_truncates_at_first_deviation() {
    let result = measure(
        &net(SKIPPABLE, "p0", "pf"),
        &event_log!(["A", "C"]),
        &EscapingEdgesOptions::pm4py(),
    );
    assert_eq!((result.precision, result.truncated_traces), (0.5, 1));
}

#[test]
fn unknown_activity_truncates_replay() {
    let log = event_log!(["A", "X", "B"]);
    assert_precision(
        &net(UNUSED_BRANCH, "p0", "pf"),
        &log,
        &EscapingEdgesOptions::pm4py(),
        1.0 / 3.0,
    );
}

#[test]
fn pm4py_preset_ignores_trace_ends() {
    let net = net(
        &[
            (Some("A"), &["p0"], &["p1"]),
            (Some("B"), &["p1"], &["pf"]),
            (Some("D"), &["pf"], &["p1"]),
        ],
        "p0",
        "pf",
    );
    assert_precision(
        &net,
        &event_log!(["A", "B"]),
        &EscapingEdgesOptions::pm4py(),
        1.0,
    );
}

fn test_data(net: &str, log: &str) -> (PetriNet, EventLogActivityProjection) {
    let path = get_test_data_path();
    let net = PetriNet::import_pnml(path.join("petri-net").join(net)).unwrap();
    let log = EventLog::import_from_path(path.join("xes").join(log)).unwrap();
    (net, log_to_activity_projection(&log))
}

fn sepsis() -> (PetriNet, EventLogActivityProjection) {
    test_data("sepsis-DISCovered.apnml", "Sepsis Cases - Event Log.xes.gz")
}

fn rtfm() -> (PetriNet, EventLogActivityProjection) {
    test_data(
        "rtfm-imf-02.apnml",
        "Road_Traffic_Fine_Management_Process.xes.gz",
    )
}

/// `(weighted available, weighted escaping)`
fn sums(result: &EscapingEdgesPrecision) -> (u64, u64) {
    (result.weighted_available, result.weighted_escaping)
}

/// Reference: `pm4py.precision_alignments` (pm4py 2.7.23.11)
#[test]
fn pm4py_preset_matches_pm4py() {
    let (net, log) = rtfm();
    let result = escaping_edges_precision(&net, &log, &EscapingEdgesOptions::pm4py()).unwrap();
    assert!(
        (result.precision - 0.6978138117313145).abs() < 1e-12,
        "{result:?}"
    );
}

/// pm4py (2.7.23.11) finds the same 18 states and escaping edges, but an available sum of 8579:
/// its silent-transition expansion keeps one marking per silent transition, so after
/// `..., Leucocytes, CRP` it misses `LacticAcid`, which needs silent transitions in two branches
#[test]
fn pm4py_preset_on_sepsis_agrees_on_escaping_edges() {
    let (net, log) = sepsis();
    let result = escaping_edges_precision(&net, &log, &EscapingEdgesOptions::pm4py()).unwrap();
    assert_eq!((result.num_states, sums(&result)), (18, (8826, 2055)));
}

/// Reference: `ProM` Precision package 6.14.7 ("Check Precision using Escaping Edges") reported
/// 0.7783761622541056 for these alignments, which `ProM`'s `IterativeAStarPlugin` computed (run
/// through `prom-fitness-and-precision-computation`). The small difference comes from delaying
/// silent transitions, which `ProM`'s aligner already does for almost all traces of this net.
#[test]
fn prom_preset_is_close_to_prom_on_its_alignments() {
    let path = get_test_data_path();
    let fixture = path
        .join("precision")
        .join("rtfm-imf-02-prom-alignments.json.gz");
    let alignments: Vec<VariantAlignmentResult> =
        serde_json::from_reader(GzDecoder::new(File::open(fixture).unwrap())).unwrap();
    let net = PetriNet::import_pnml(path.join("petri-net").join("rtfm-imf-02.apnml")).unwrap();
    let result =
        escaping_edges_precision_from_alignments(&net, &alignments, &EscapingEdgesOptions::prom())
            .unwrap();
    assert!(
        (result.precision - 0.7783761622541056).abs() < 1e-5,
        "{result:?}"
    );
}

/// Regression values. `prom()` gives 0.7783 on RTFM (`ProM`: 0.7784) and 0.5154 on Sepsis
/// (`ProM`: 0.575): `ProM`'s aligner picks other optimal alignments, and on Sepsis it fires many
/// silent transitions before they are needed. Both nets are sound, so excluding dead ends changes
/// nothing.
#[test]
fn aligned_precision_regression_values() {
    for ((net, log), [default, prom]) in [
        (sepsis(), [(48271, 22642), (64786, 31395)]),
        (rtfm(), [(1961593, 307740), (2622593, 581333)]),
    ] {
        let alignments = align_log(&net, &log, &AlignmentOptions::default());
        let without_dead_ends = EscapingEdgesOptions {
            exclude_dead_ends: true,
            ..Default::default()
        };
        let measured = [
            EscapingEdgesOptions::default(),
            without_dead_ends,
            EscapingEdgesOptions::prom(),
        ]
        .map(|options| {
            sums(&escaping_edges_precision_from_alignments(&net, &alignments, &options).unwrap())
        });
        assert_eq!(measured, [default, default, prom]);
    }
}

#[test]
fn alignments_of_another_net_are_rejected() {
    let foreign = TransitionID(Uuid::from_u128(42));
    let alignments = [VariantAlignmentResult {
        activities: vec!["A".to_string()],
        frequency: 1,
        result: Ok(AlignmentResult {
            moves: vec![AlignmentMove::SyncMove {
                transition: foreign,
                trace_event_index: 0,
            }],
            cost: 0,
            states_visited: 1,
        }),
    }];
    assert_eq!(
        escaping_edges_precision_from_alignments(
            &net(SEQUENCE, "p0", "pf"),
            &alignments,
            &Default::default()
        ),
        Err(PrecisionError::UnknownTransition(foreign))
    );
}

#[test]
fn silent_transitions_count_where_they_are_needed() {
    // `A` splits into `B` or `D` and a silent transition, joined by `C`
    let net = net(
        &[
            (Some("A"), &["p0"], &["p1", "p2"]),
            (Some("B"), &["p1"], &["p3"]),
            (Some("D"), &["p1"], &["p3"]),
            (None, &["p2"], &["p4"]),
            (Some("C"), &["p3", "p4"], &["pf"]),
        ],
        "p0",
        "pf",
    );
    let sync = |t| AlignmentMove::SyncMove {
        transition: transition_id(t),
        trace_event_index: 0,
    };
    let silent = AlignmentMove::ModelMove {
        transition: transition_id(3),
    };
    let variant = |moves| VariantAlignmentResult {
        activities: Vec::new(),
        frequency: 1,
        result: Ok(AlignmentResult {
            moves,
            cost: 0,
            states_visited: 0,
        }),
    };
    let alignments = [
        variant(vec![sync(0), silent.clone(), sync(1), sync(4)]),
        variant(vec![sync(0), sync(2), silent, sync(4)]),
    ];
    let result =
        escaping_edges_precision_from_alignments(&net, &alignments, &EscapingEdgesOptions::prom())
            .unwrap();
    assert_eq!(sums(&result), (12, 2));
}

#[test]
fn dead_ends_are_not_available() {
    // `C` and, behind a silent transition, `D` lead into places the model cannot leave
    let net = net(
        &[
            (Some("A"), &["p0"], &["p1"]),
            (Some("B"), &["p1"], &["pf"]),
            (Some("C"), &["p1"], &["dead"]),
            (None, &["p1"], &["p2"]),
            (Some("D"), &["p2"], &["dead"]),
        ],
        "p0",
        "pf",
    );
    let log = event_log!(["A", "B"]);
    let without_dead_ends = EscapingEdgesOptions {
        exclude_dead_ends: true,
        ..Default::default()
    };
    assert_precision(&net, &log, &Default::default(), 0.5);
    assert_precision(&net, &log, &without_dead_ends, 1.0);
    let transitions = EscapingEdgesOptions {
        exclude_dead_ends: true,
        ..as_activities()
    };
    assert_precision(&net, &log, &transitions, 1.0);
}

#[test]
fn stopping_counts_as_an_activity() {
    // `A` reaches the final marking, from which `B, C` may loop back to it
    let net = net(
        &[
            (Some("A"), &["p0"], &["pf"]),
            (Some("B"), &["pf"], &["p2"]),
            (Some("C"), &["p2"], &["pf"]),
        ],
        "p0",
        "pf",
    );
    let stopping = EscapingEdgesOptions {
        trace_ends: TraceEnds::AsActivity,
        report_escaping_edges: true,
        ..Default::default()
    };
    assert_precision(&net, &event_log!(["A"]), &Default::default(), 0.5);
    assert_precision(&net, &event_log!(["A"]), &stopping, 2.0 / 3.0);
    assert_precision(
        &net,
        &event_log!(["A"], ["A", "B", "C"]),
        &Default::default(),
        5.0 / 6.0,
    );
    assert_precision(
        &net,
        &event_log!(["A"], ["A", "B", "C"]),
        &stopping,
        8.0 / 9.0,
    );
    let edge = |prefix: &[&str], escaping| EscapingEdge {
        prefix: prefix.iter().map(|a| activity(a)).collect(),
        escaping,
        weight: 1,
    };
    assert_eq!(
        measure(&net, &event_log!(["A", "B", "C"]), &stopping).escaping_edges,
        Some(vec![
            edge(&["A"], PathStep::End),
            edge(&["A", "B", "C"], activity("B"))
        ])
    );
}

#[test]
fn dead_end_filter_needs_a_final_marking() {
    let mut net = net(SEQUENCE, "p0", "pf");
    net.final_markings = None;
    let options = EscapingEdgesOptions {
        exclude_dead_ends: true,
        ..EscapingEdgesOptions::pm4py()
    };
    let log = log_to_activity_projection(&event_log!(["A", "B"]));
    assert_eq!(
        escaping_edges_precision(&net, &log, &options),
        Err(PrecisionError::InvalidNet(
            SyncProdNetConstructionError::NoFinalMarking
        ))
    );
    assert!(escaping_edges_precision(&net, &log, &EscapingEdgesOptions::pm4py()).is_ok());
}
