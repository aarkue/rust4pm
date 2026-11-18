use std::collections::{HashMap, HashSet};

use super::abstraction::{Fact, FactKind};
use super::grid::{ActivityGrid, Reconstruction, ReconstructionVia};
use super::log::{LogView, Traces, Variants};
use super::schema::Schema;
use super::tagged::tests::{e2o, toy};
use super::*;
use crate::core::chrono::DateTime;
use crate::core::event_data::object_centric::linked_ocel::{LinkedOCELAccess, SlimLinkedOCEL};

fn at(ms: i64) -> crate::core::chrono::DateTime<crate::core::chrono::FixedOffset> {
    DateTime::from_timestamp_millis(1_600_000_000_000 + ms)
        .unwrap()
        .fixed_offset()
}

fn states(result: &ReflowResult) -> Vec<(String, String, CellState)> {
    result
        .cells
        .iter()
        .map(|c| (c.activity.clone(), c.object_type.clone(), c.state))
        .collect()
}

fn derived_pairs(locel: &SlimLinkedOCEL) -> HashSet<(String, String)> {
    let view = LogView::new(locel);
    let schema = Schema::discover(&view, locel);
    schema
        .functions
        .keys()
        .map(|(s, t)| (view.types[*s].clone(), view.types[*t].clone()))
        .collect()
}

/// The order of an item is implied at both activities, through the recorded `of` relation.
#[test]
fn the_toy_log_keeps_the_items_and_implies_the_orders() {
    let ocel = toy();
    let result = reflow(&ocel, &ReflowOptions::default());
    for c in &result.cells {
        match c.object_type.as_str() {
            "items" => assert_eq!(c.state, CellState::Flow),
            _ => {
                assert_eq!(c.state, CellState::Implied);
                let d = c.determined_by.as_ref().unwrap();
                assert_eq!(d.from, "items");
                assert_eq!(d.reconstruction.share, 1.0);
            }
        }
    }
    assert_eq!((result.stats.flow, result.stats.implied), (2, 2));
}

#[test]
fn tagging_keeps_every_tuple_and_untag_gives_the_input_back() {
    let ocel = toy();
    let result = reflow(&ocel, &ReflowOptions::default());
    let tagged = result.tag(&ocel);
    assert_eq!(e2o(&tagged).len(), e2o(&ocel).len());
    assert_eq!(e2o(&untag(&tagged)), e2o(&ocel));
    let flow = flow_projection(&tagged);
    assert_eq!(e2o(&flow).len(), 4, "only the item cells flow");
    assert_eq!(e2o(&flow), e2o(&result.flow_log(&ocel)));
    let o2o = |l: &SlimLinkedOCEL| l.get_all_obs().map(|o| l.get_o2o(o).count()).sum::<usize>();
    assert_eq!(o2o(&tagged), o2o(&ocel));
}

/// The annotation needs nothing but the tagged log.
#[test]
fn the_annotation_reads_the_tagged_log_alone() {
    let ocel = toy();
    let result = reflow(&ocel, &ReflowOptions::default());
    let tagged = result.tag(&ocel);
    let read = reflow_from_tagged(&tagged, &ReflowOptions::default());
    assert_eq!(read.cells, result.cells);
    let untagged = reflow_from_tagged(&ocel, &ReflowOptions::default());
    assert!(untagged.cells.iter().all(|c| c.state == CellState::Flow));
}

/// Running Reflow on a tagged log reads it as the log it tags.
#[test]
fn reflow_ignores_existing_tags() {
    let ocel = toy();
    let result = reflow(&ocel, &ReflowOptions::default());
    let again = reflow(&result.tag(&ocel), &ReflowOptions::default());
    assert_eq!(again.cells, result.cells);
}

#[test]
fn an_o2o_edge_within_a_type_is_no_function() {
    let mut ocel = SlimLinkedOCEL::new();
    ocel.add_object_type("items", Vec::new());
    let mut previous = None;
    for n in 0..3 {
        let replaces = previous.map(|p| vec![("replaces".into(), p)]);
        previous = ocel.add_object(
            "items",
            Some(format!("i{n}")),
            Vec::new(),
            replaces.unwrap_or_default(),
        );
    }
    let view = LogView::new(&ocel);
    let schema = Schema::discover(&view, &ocel);
    assert!(schema.base.is_empty());
    assert!(schema.recorded.is_empty());
}

/// An added participation must not witness a relation, or the schema grows on what expansion
/// wrote.
#[test]
fn schema_discovery_ignores_added_participations() {
    let ocel = toy();
    let base = derived_pairs(&ocel);
    let evs: Vec<_> = ocel.get_evs_of_type("place").copied().collect();
    let orders: Vec<_> = ocel
        .get_all_obs()
        .filter(|o| ocel.get_ob_type_of(*o) == "orders")
        .collect();
    let mut added = ocel.clone();
    let mut naive = ocel.clone();
    for e in &evs {
        for o in &orders {
            added.add_e2o(*e, *o, ADDED_QUALIFIER.to_string());
            naive.add_e2o(*e, *o, "order".to_string());
        }
    }
    assert_eq!(derived_pairs(&added), base);
    let base_len = |l: &SlimLinkedOCEL| Schema::discover(&LogView::new(l), l).base.len();
    assert!(base_len(&naive) < base_len(&ocel));
}

/// One type, and per object one event per activity of `plan`, at the given offsets.
fn single_type_log(n: usize, plan: &[(&str, i64)]) -> SlimLinkedOCEL {
    let mut ocel = SlimLinkedOCEL::new();
    ocel.add_object_type("parts", Vec::new());
    let mut names: Vec<&str> = plan.iter().map(|(a, _)| *a).collect();
    names.sort_unstable();
    names.dedup();
    for a in names {
        ocel.add_event_type(a, Vec::new());
    }
    for k in 0..n {
        let o = ocel
            .add_object("parts", Some(format!("o{k}")), Vec::new(), Vec::new())
            .unwrap();
        for (a, ms) in plan {
            ocel.add_event(
                a,
                at(*ms),
                Some(format!("{a}-{k}")),
                Vec::new(),
                vec![("part".into(), o)],
            );
        }
    }
    ocel
}

fn orderings_and_arcs(ocel: &SlimLinkedOCEL) -> (Vec<String>, HashSet<(usize, usize)>, Variants) {
    let view = LogView::new(ocel);
    let traces = Traces::build(&view, ocel, &[], None);
    let cells: HashSet<(usize, usize)> = (0..view.activities.len()).map(|a| (a, 0)).collect();
    let input = AbstractionInput {
        view: &view,
        traces: &traces,
        candidates: &cells,
        only_type: None,
    };
    let facts: HashSet<(usize, usize)> = OrderingAbstraction
        .facts(&input)
        .into_iter()
        .map(|f| (f.activities[0], f.activities[1]))
        .collect();
    let variants = Variants::build(&view, &traces);
    (view.activities.clone(), facts, variants)
}

/// Two events of one object at the same instant order nothing, but the directly-follows graph
/// still draws one arc, by activity name.
#[test]
fn simultaneous_events_order_nothing_and_draw_one_arc() {
    let ocel = single_type_log(3, &[("start", 0), ("complete", 0)]);
    let (acts, facts, variants) = orderings_and_arcs(&ocel);
    assert!(facts.is_empty());
    let ix = |a: &str| acts.iter().position(|x| x == a).unwrap();
    assert_eq!(
        variants.variants,
        vec![(0, vec![ix("complete"), ix("start")])]
    );
    assert_eq!(variants.arcs(|_| true), 1);
}

/// A tie inside a trace is chained through in activity order, and orders nothing across itself.
#[test]
fn a_tie_inside_a_trace_is_chained_but_orders_nothing() {
    let ocel = single_type_log(2, &[("a", 0), ("b", 1000), ("c", 1000), ("d", 2000)]);
    let (acts, facts, variants) = orderings_and_arcs(&ocel);
    let ix = |a: &str| acts.iter().position(|x| x == a).unwrap();
    let (a, b, c, d) = (ix("a"), ix("b"), ix("c"), ix("d"));
    assert_eq!(variants.variants, vec![(0, vec![a, b, c, d])]);
    assert_eq!(variants.arcs(|_| true), 3);
    assert!(!facts.contains(&(b, c)) && !facts.contains(&(c, b)));
    assert!(facts.contains(&(a, d)) && facts.contains(&(a, b)));
}

/// Items `0` determine orders `1` at both activities; nothing determines employees `2`.
fn repair_grid() -> Vec<ActivityGrid> {
    let recon = Some(Reconstruction {
        via: ReconstructionVia::Preimage,
        share: 1.0,
    });
    let here = ActivityGrid {
        present: vec![0, 1, 2],
        counts: vec![1, 1, 1],
        objects: vec![(1, 1); 3],
        recon: vec![None, recon, None, None, None, None, None, None, None],
    };
    vec![here.clone(), here]
}

fn order(t: usize, a: usize, b: usize) -> Fact {
    Fact {
        kind: FactKind::ORDER,
        object_type: t,
        activities: [a, b],
    }
}

#[test]
fn repair_adds_back_exactly_the_undetermined_cells() {
    let grid = repair_grid();
    let facts = [order(0, 0, 1), order(1, 0, 1), order(2, 0, 1)];
    let mut flow: HashSet<(usize, usize)> = HashSet::from([(0, 0), (1, 0)]);
    let added = search::repair(&grid, &facts, &mut flow);
    assert_eq!(added, 2);
    assert!(
        !flow.contains(&(0, 1)) && !flow.contains(&(1, 1)),
        "orders are determined"
    );
    assert!(
        flow.contains(&(0, 2)) && flow.contains(&(1, 2)),
        "employees come back"
    );
    assert_eq!(search::repair(&grid, &facts, &mut flow), 0, "idempotent");
}

/// Orders `1` determine items `0`, not the other way. The first pass adds the items, then the
/// orders, which make the items redundant.
#[test]
fn repair_adds_no_cell_the_other_added_cells_determine() {
    let recon = Some(Reconstruction {
        via: ReconstructionVia::Image,
        share: 1.0,
    });
    let here = ActivityGrid {
        present: vec![0, 1],
        counts: vec![1, 1],
        objects: vec![(1, 1); 2],
        recon: vec![None, None, recon, None],
    };
    let grid = vec![here.clone(), here];
    let facts = [order(0, 0, 1), order(1, 0, 1)];
    let mut flow = HashSet::new();
    assert_eq!(search::repair(&grid, &facts, &mut flow), 2);
    assert_eq!(flow, HashSet::from([(0, 1), (1, 1)]));
}

/// The grid of [`repair_grid`] with every type ordering `0 < 1`, and `facts` of those. The items
/// are the cheapest and draw two arcs, orders and employees one each.
fn with_repair_input(facts: &[Fact], check: impl FnOnce(&SearchInput<'_>)) {
    let grid = repair_grid();
    let orderings = [order(0, 0, 1), order(1, 0, 1), order(2, 0, 1)];
    let cost: HashMap<(usize, usize), usize> = (0..2)
        .flat_map(|a| [((a, 0), 1), ((a, 1), 10), ((a, 2), 10)])
        .collect();
    let variants = Variants {
        variants: vec![(0, vec![0, 0, 1]), (1, vec![0, 1]), (2, vec![0, 1])],
    };
    let types = ["items", "orders", "employees"].map(String::from);
    check(&SearchInput {
        facts,
        abstraction: &OrderingAbstraction,
        cost: &cost,
        recorded: &cost,
        objects_per_type: &[1, 2, 3],
        rep: &[0, 1, 2],
        types: &types,
        n_acts: 2,
        variants: &variants,
        grid: &grid,
        orderings: &orderings,
    });
}

/// The orders alone draw the fewest arcs, but repair adds the items and employees to them. The
/// items determine the orders, so both searches keep items and employees.
#[test]
fn both_searches_compare_layers_after_repair() {
    let facts = [order(0, 0, 1), order(1, 0, 1), order(2, 0, 1)];
    with_repair_input(&facts, |input| {
        let mut orders = HashSet::from([(0, 1), (1, 1)]);
        assert_eq!(input.variants.arcs(|c| orders.contains(&c)), 1);
        search::repair(input.grid, input.orderings, &mut orders);
        assert_eq!(input.variants.arcs(|c| orders.contains(&c)), 4);

        let ctl = Control::new();
        let mut budget = Budget::new(u64::MAX, &ctl);
        let heuristic = search::heuristic(input, &mut budget);
        assert_eq!(heuristic.repaired, 2);
        let exact = search::exact(input, &heuristic, &mut budget).unwrap();
        let expected = HashSet::from([(0, 0), (1, 0), (0, 2), (1, 2)]);
        for layer in [&heuristic, &exact] {
            assert_eq!(layer.cells, expected);
            assert_eq!(layer.arcs, 3);
        }
    });
}

/// Under an abstraction where only the items show a fact, orders and employees still get rows,
/// as the employees must flow wherever they order a pair.
#[test]
fn the_exact_search_has_rows_for_types_that_only_order() {
    with_repair_input(&[order(0, 0, 1)], |input| {
        let unseeded = Layer {
            cells: HashSet::new(),
            seed: SearchSeed::Greedy,
            entailed: 0,
            arcs: 0,
            participations: 0,
            repaired: 0,
        };
        let ctl = Control::new();
        let mut budget = Budget::new(u64::MAX, &ctl);
        let exact = search::exact(input, &unseeded, &mut budget).unwrap();
        assert_eq!(exact.cells, HashSet::from([(0, 0), (1, 0), (0, 2), (1, 2)]));
    });
}

#[test]
fn repair_never_adds_an_unrecorded_cell() {
    let mut grid = repair_grid();
    grid[1].present = vec![0, 1];
    grid[1].counts = vec![1, 1];
    grid[1].recon = vec![None, None, None, None];
    let mut flow: HashSet<(usize, usize)> = HashSet::from([(0, 0), (1, 0)]);
    search::repair(&grid, &[order(2, 0, 1)], &mut flow);
    assert!(flow.contains(&(0, 2)) && !flow.contains(&(1, 2)));
}

/// Three items, each in its own package, picked, packed and shipped. Items order `pick <
/// create`, packages `create < ship`, and only the chain covers `pick < ship`.
fn chain_log() -> SlimLinkedOCEL {
    let mut ocel = SlimLinkedOCEL::new();
    for t in ["items", "packages"] {
        ocel.add_object_type(t, Vec::new());
    }
    for a in ["pick item", "create package", "ship package"] {
        ocel.add_event_type(a, Vec::new());
    }
    let mut clock = 0;
    for n in 0..3 {
        let item = ocel
            .add_object("items", Some(format!("i{n}")), Vec::new(), Vec::new())
            .unwrap();
        let package = ocel
            .add_object(
                "packages",
                Some(format!("p{n}")),
                Vec::new(),
                vec![("packed".into(), item)],
            )
            .unwrap();
        for (act, objs) in [
            ("pick item", vec![("item".into(), item)]),
            (
                "create package",
                vec![("item".into(), item), ("package".into(), package)],
            ),
            ("ship package", vec![("package".into(), package)]),
        ] {
            clock += 1000;
            ocel.add_event(act, at(clock), Some(format!("{act}-{n}")), Vec::new(), objs);
        }
    }
    ocel
}

#[test]
fn the_exact_search_is_never_worse_and_both_preserve() {
    let ocel = chain_log();
    let heuristic = reflow(&ocel, &ReflowOptions::default());
    let exact = reflow(
        &ocel,
        &ReflowOptions {
            search: SearchStrategy::Exact,
            ..ReflowOptions::default()
        },
    );
    assert!(!exact.stats.limit_reached);
    assert!(exact.stats.arcs_flow <= heuristic.stats.arcs_flow);
    for r in [&heuristic, &exact] {
        assert!(preserving(&ocel, r), "{:?}", states(r));
    }
    // Each type shows its ordering only while it flows at both of its activities.
    assert_eq!(heuristic.stats.flow, 4);
}

/// Every fact of the log, with the participations a tagged log marks as added, is entailed by
/// the facts visible in the result's flow layer.
fn preserving(ocel: &SlimLinkedOCEL, result: &ReflowResult) -> bool {
    let view = LogView::new(ocel);
    let added: Vec<(u32, u32)> = view.added.iter().map(|(e, o, _)| (*e, *o)).collect();
    let traces = Traces::build(&view, ocel, &added, None);
    let cells: HashSet<(usize, usize)> = result
        .cells
        .iter()
        .map(|c| {
            (
                view.activities
                    .iter()
                    .position(|a| *a == c.activity)
                    .unwrap(),
                view.types.iter().position(|t| *t == c.object_type).unwrap(),
            )
        })
        .collect();
    let input = AbstractionInput {
        view: &view,
        traces: &traces,
        candidates: &cells,
        only_type: None,
    };
    let facts = OrderingAbstraction.facts(&input);
    let flow: HashSet<(usize, usize)> = result
        .flow_layer()
        .map(|(a, t)| {
            (
                view.activities.iter().position(|x| x == a).unwrap(),
                view.types.iter().position(|x| x == t).unwrap(),
            )
        })
        .collect();
    let visible: Vec<Fact> = facts
        .iter()
        .filter(|f| f.cells().all(|c| flow.contains(&c)))
        .copied()
        .collect();
    let e = OrderingAbstraction.entailment(&visible, view.activities.len());
    facts.iter().all(|f| e.entails(f))
}

/// The same log with a note per event, each note named once: a type that shows no fact.
fn chain_log_with_notes() -> SlimLinkedOCEL {
    let mut ocel = chain_log();
    ocel.add_object_type("notes", Vec::new());
    let events: Vec<_> = ocel.get_all_evs().collect();
    for (n, e) in events.into_iter().enumerate() {
        let note = ocel
            .add_object("notes", Some(format!("n{n}")), Vec::new(), Vec::new())
            .unwrap();
        ocel.add_e2o(e, note, "note".to_string());
    }
    ocel
}

#[test]
fn the_exact_search_spends_nothing_on_a_type_without_facts() {
    let options = ReflowOptions {
        search: SearchStrategy::Exact,
        ..ReflowOptions::default()
    };
    let plain = reflow(&chain_log(), &options);
    let noted = reflow(&chain_log_with_notes(), &options);
    assert_eq!(noted.stats.facts, plain.stats.facts);
    assert_eq!(noted.stats.seed, Some(SearchSeed::Exact));
    assert_eq!(noted.stats.work, plain.stats.work);
    let flow = |r: &ReflowResult| -> Vec<(String, String)> {
        r.flow_layer()
            .map(|(a, t)| (a.to_string(), t.to_string()))
            .collect()
    };
    assert_eq!(flow(&noted), flow(&plain));
}

#[test]
fn a_spent_budget_still_gives_a_preserving_layer() {
    let ocel = chain_log();
    let result = reflow(
        &ocel,
        &ReflowOptions {
            max_work: 0,
            ..ReflowOptions::default()
        },
    );
    assert!(result.stats.limit_reached);
    assert!(preserving(&ocel, &result));
}

/// Every cell of the chain log is a cell of an ordering. Repair adds the items at `create
/// package`, which determine the packages there, and the packages at `ship package`. The layer
/// hides the ordering of the packages, which repair does not restore.
#[test]
fn an_edited_flow_layer_is_repaired() {
    let ocel = chain_log();
    let result = reflow(&ocel, &ReflowOptions::default());
    let edited = result.with_flow_layer([("pick item", "items")]).unwrap();
    assert_eq!(edited.stats.repaired, 2);
    let cell = |a: &str, t: &str| edited.cell(a, t).unwrap().state;
    assert_eq!(cell("create package", "items"), CellState::Flow);
    assert_eq!(cell("create package", "packages"), CellState::Implied);
    assert_eq!(cell("ship package", "packages"), CellState::Flow);
    assert!(!preserving(&ocel, &edited));
    assert_eq!(edited.stats.seed, None);
    assert!(matches!(
        result.with_flow_layer([("pick item", "packages")]),
        Err(ReflowError::UnknownCell { .. })
    ));
}

/// Orders and items placed together, items picked alone and orders shipped alone, so each
/// relation is known but `pick` names no order and `ship` no item.
fn expansion_log() -> SlimLinkedOCEL {
    let mut ocel = SlimLinkedOCEL::new();
    ocel.add_object_type("items", Vec::new());
    ocel.add_object_type("orders", Vec::new());
    for a in ["place", "pick", "ship"] {
        ocel.add_event_type(a, Vec::new());
    }
    let mut clock = 0;
    for n in 0..2 {
        let order = ocel
            .add_object("orders", Some(format!("o{n}")), Vec::new(), Vec::new())
            .unwrap();
        let item = ocel
            .add_object(
                "items",
                Some(format!("i{n}")),
                Vec::new(),
                vec![("of".into(), order)],
            )
            .unwrap();
        for (act, objs) in [
            (
                "place",
                vec![("item".into(), item), ("order".into(), order)],
            ),
            ("pick", vec![("item".into(), item)]),
            ("ship", vec![("order".into(), order)]),
        ] {
            clock += 1000;
            ocel.add_event(
                act,
                at(clock),
                Some(format!("{act}-{clock}")),
                Vec::new(),
                objs,
            );
        }
    }
    ocel
}

#[test]
fn expansion_writes_the_cells_a_relation_determines_and_the_log_omits() {
    let ocel = expansion_log();
    let options = ReflowOptions {
        expand: true,
        ..ReflowOptions::default()
    };
    let result = reflow(&ocel, &options);
    assert_eq!(result.stats.expansion_candidates, 2);
    let added: Vec<(&str, &str)> = result
        .cells
        .iter()
        .filter(|c| !c.recorded)
        .map(|c| (c.activity.as_str(), c.object_type.as_str()))
        .collect();
    assert!(!added.is_empty());
    for (a, t) in &added {
        assert!([("pick", "orders"), ("ship", "items")].contains(&(*a, *t)));
    }
    let tagged = result.tag(&ocel);
    assert!(e2o(&tagged).iter().any(|(_, _, q)| q == ADDED_QUALIFIER));
    assert_eq!(e2o(&untag(&tagged)), e2o(&ocel));
    let read = reflow_from_tagged(&tagged, &options);
    assert_eq!(read.cells, result.cells);
    assert!(preserving(&tagged, &result));
}

/// An expansion skipped for its work leaves the search the whole budget, so the layer is the one
/// without expansion.
#[test]
fn an_expansion_over_the_limit_costs_the_search_nothing() {
    let ocel = test_log("order-management.json");
    let plain = reflow(&ocel, &ReflowOptions::default());
    let options = |expand| ReflowOptions {
        expand,
        max_work: plain.stats.work,
        ..ReflowOptions::default()
    };
    let unexpanded = reflow(&ocel, &options(false));
    assert!(!unexpanded.stats.limit_reached);
    let skipped = reflow(&ocel, &options(true));
    assert!(skipped.stats.limit_reached);
    assert_eq!(skipped.stats.expansion_candidates, 0);
    assert_eq!(skipped.stats.work, plain.stats.work);
    assert_eq!(skipped.cells, plain.cells);
}

/// Writing packages at `place`, before any package exists, fails the lifetime check.
#[test]
fn a_cell_that_antedates_its_objects_is_not_admitted() {
    let mut ocel = expansion_log();
    ocel.add_object_type("packages", Vec::new());
    ocel.add_event_type("pack", Vec::new());
    let orders: Vec<_> = ocel
        .get_all_obs()
        .filter(|o| ocel.get_ob_type_of(*o) == "orders")
        .collect();
    for (n, o) in orders.iter().enumerate() {
        let p = ocel
            .add_object(
                "packages",
                Some(format!("p{n}")),
                Vec::new(),
                vec![("for".into(), *o)],
            )
            .unwrap();
        ocel.add_event(
            "pack",
            at(100_000 + n as i64 * 1000),
            Some(format!("pack-{n}")),
            Vec::new(),
            vec![("package".into(), p), ("order".into(), *o)],
        );
    }
    let result = reflow(
        &ocel,
        &ReflowOptions {
            expand: true,
            ..ReflowOptions::default()
        },
    );
    assert!(result.cell("place", "packages").is_none());
}

/// `items -> employees` two ways, through the order and through the package, naming different
/// people: the pair determines nothing, so no employee is written at `place`.
#[test]
fn contradicting_routes_determine_nothing() {
    let mut ocel = SlimLinkedOCEL::new();
    for t in ["items", "orders", "packages", "employees"] {
        ocel.add_object_type(t, Vec::new());
    }
    ocel.add_event_type("place", Vec::new());
    ocel.add_event_type("sell", Vec::new());
    let seller = ocel
        .add_object("employees", Some("sells".into()), Vec::new(), Vec::new())
        .unwrap();
    let packer = ocel
        .add_object("employees", Some("packs".into()), Vec::new(), Vec::new())
        .unwrap();
    for n in 0..3i64 {
        let o = ocel
            .add_object(
                "orders",
                Some(format!("o{n}")),
                Vec::new(),
                vec![("sold by".into(), seller)],
            )
            .unwrap();
        let p = ocel
            .add_object(
                "packages",
                Some(format!("p{n}")),
                Vec::new(),
                vec![("packed by".into(), packer)],
            )
            .unwrap();
        let i = ocel
            .add_object(
                "items",
                Some(format!("i{n}")),
                Vec::new(),
                vec![("of".into(), o), ("in".into(), p)],
            )
            .unwrap();
        ocel.add_event(
            "place",
            at(n * 1000),
            Some(format!("place-{n}")),
            Vec::new(),
            vec![("item".into(), i)],
        );
        ocel.add_event(
            "sell",
            at(n * 1000 + 500),
            Some(format!("sell-{n}")),
            Vec::new(),
            vec![("e".into(), seller), ("e".into(), packer)],
        );
    }
    let result = reflow(
        &ocel,
        &ReflowOptions {
            expand: true,
            ..ReflowOptions::default()
        },
    );
    assert!(result.cell("place", "employees").is_none());
}

fn test_log(name: &str) -> SlimLinkedOCEL {
    use crate::core::io::Importable;
    let path = crate::test_utils::get_test_data_path()
        .join("ocel")
        .join(name);
    SlimLinkedOCEL::import_from_path(path).expect("Importing")
}

/// Order management: 14 flow, 3 involved and 23 implied cells of 40.
#[test]
fn order_management_states() {
    let ocel = test_log("order-management.json");
    let result = reflow(&ocel, &ReflowOptions::default());
    assert_eq!(result.cells.len(), 40);
    assert_eq!(
        (
            result.stats.flow,
            result.stats.involved,
            result.stats.implied
        ),
        (14, 3, 23)
    );
    assert!(!result.stats.limit_reached);
    assert!(preserving(&ocel, &result));
    let tagged = result.tag(&ocel);
    assert_eq!(
        reflow_from_tagged(&tagged, &ReflowOptions::default()).cells,
        result.cells
    );
    assert_eq!(e2o(&untag(&tagged)), e2o(&ocel));
    let json = serde_json::to_value(&result).unwrap();
    let back: ReflowResult = serde_json::from_value(json).unwrap();
    assert_eq!(back, result);
}

#[test]
fn the_annotated_net_has_a_transition_per_flowing_activity_and_annotates_the_rest() {
    let ocel = test_log("order-management.json");
    let result = reflow(&ocel, &ReflowOptions::default());
    let model = result.annotated_ocpn(&ocel, &AnnotatedOcpnOptions::default());
    let mut types = model.ocpn.object_types();
    types.sort_unstable();
    assert_eq!(types, ["items", "orders", "packages"]);
    let annotated: usize = model.annotations.values().map(Vec::len).sum();
    assert_eq!(annotated, 26);
    let confirm = &model.annotations["confirm order"];
    assert!(confirm
        .iter()
        .any(|c| c.object_type == "items" && c.from.as_deref() == Some("orders")));
    assert!(model.places.is_empty());

    let tagged = result.tag(&ocel);
    let theta = ReflowOptions::default().theta;
    let from_tags = annotated_ocpn(&tagged, theta, &AnnotatedOcpnOptions::default());
    assert_eq!(from_tags.annotations, model.annotations);
    assert_eq!(from_tags.ocpn.num_arcs(), model.ocpn.num_arcs());

    let with_places = result.annotated_ocpn(
        &ocel,
        &AnnotatedOcpnOptions {
            places: true,
            ..AnnotatedOcpnOptions::default()
        },
    );
    let mut types = with_places.ocpn.object_types();
    types.sort_unstable();
    assert_eq!(
        types,
        [
            "customers",
            "employees",
            "items",
            "orders",
            "packages",
            "products"
        ]
    );
    let roles: HashMap<&str, usize> = with_places.places.iter().fold(HashMap::new(), |mut m, p| {
        *m.entry(p.object_type.as_str()).or_default() += 1;
        m
    });
    assert!(roles.contains_key("employees") && roles.contains_key("products"));
}

#[cfg(feature = "visualization")]
#[test]
fn the_annotated_net_draws_badges_and_dashed_places() {
    use crate::visualization::ocpn::OcpnStyle;
    let ocel = toy();
    let result = reflow(&ocel, &ReflowOptions::default());
    let model = result.annotated_ocpn(
        &ocel,
        &AnnotatedOcpnOptions {
            places: true,
            ..AnnotatedOcpnOptions::default()
        },
    );
    let drawing = model.drawing();
    assert_eq!(drawing.options.badges["place"].len(), 1);
    assert_eq!(
        drawing.options.badges["place"][0].from.as_deref(),
        Some("items")
    );
    use crate::visualization::badges::InvolvementCard;
    let badge = &drawing.options.badges["place"][0];
    assert_eq!(badge.card, Some(InvolvementCard::One));
    assert_eq!(badge.from_card, Some(InvolvementCard::One));
    assert!(!drawing.overrides.nodes.is_empty());
    let svg = model.svg(&OcpnStyle::default(), None).unwrap();
    assert!(svg.contains("gl-mapsto"));
}

/// An abstraction defined outside the crate's list: an object type "starts" at an activity when
/// some of its objects begin there. Starts do not entail each other.
struct Starts;

struct Visible(HashSet<Fact>);

impl Entailment for Visible {
    fn entails(&self, fact: &Fact) -> bool {
        self.0.contains(fact)
    }
}

impl Abstraction for Starts {
    fn facts(&self, input: &AbstractionInput<'_>) -> Vec<Fact> {
        let mut out = Vec::new();
        for t in input.types() {
            for trace in input.traces(t) {
                if let Some((_, a)) = trace.first() {
                    out.push(Fact {
                        kind: FactKind("starts"),
                        object_type: t,
                        activities: [*a, *a],
                    });
                }
            }
        }
        out
    }

    fn entailment(&self, visible: &[Fact], _: usize) -> Box<dyn Entailment> {
        Box::new(Visible(visible.iter().copied().collect()))
    }
}

#[test]
fn a_custom_abstraction_keeps_its_facts_visible() {
    let ocel = chain_log();
    let result = reflow_with(&ocel, &ReflowOptions::default(), &Starts);
    assert_eq!(result.stats.facts, 2);
    for (a, t) in [("pick item", "items"), ("create package", "packages")] {
        assert_eq!(result.cell(a, t).unwrap().state, CellState::Flow);
    }
    // The repair phase still states every ordering of a type at each of its activities, as a
    // flow cell or an implied one.
    assert!(result.cells.iter().all(|c| c.state != CellState::Involved));
}

#[test]
fn the_flow_log_is_the_flow_projection_of_the_tagged_log() {
    let expand = ReflowOptions {
        expand: true,
        ..ReflowOptions::default()
    };
    let cases = [
        (test_log("order-management.json"), ReflowOptions::default()),
        (expansion_log(), expand),
    ];
    for (ocel, options) in cases {
        let result = reflow(&ocel, &options);
        let tagged = result.tag(&ocel);
        let projected = flow_projection(&tagged);
        let flow = result.flow_log(&ocel);
        assert_eq!(e2o(&flow), e2o(&projected));
        assert!(flow.qualifiers().iter().all(|q| qualifier_flows(q)));
        let o2o = |l: &SlimLinkedOCEL| {
            let mut v: Vec<(String, String)> = l
                .get_all_obs()
                .flat_map(|o| {
                    l.get_o2o(o)
                        .map(|(q, t)| {
                            (
                                l.get_ob_id(o).to_string(),
                                format!("{q}:{}", l.get_ob_id(t)),
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect();
            v.sort();
            v
        };
        assert_eq!(o2o(&flow), o2o(&projected));
        assert_eq!(flow.get_all_evs().count(), projected.get_all_evs().count());
    }
}

/// The counts cover every event of the activity, so a type some events lack has a minimum of 0,
/// as in the OC-DECLARE involvements, but count distinct objects. Added cells count the added
/// participations.
#[test]
fn cells_count_the_objects_per_event() {
    use crate::core::process_models::object_centric::oc_declare::get_activity_object_involvements;
    let ocel = test_log("order-management.json");
    let result = reflow(&ocel, &ReflowOptions::default());
    let involvements = get_activity_object_involvements(&ocel);
    let mut distinct: HashMap<(String, String), (usize, usize)> = HashMap::new();
    for e in ocel.get_all_evs() {
        let activity = ocel.get_ev_type_of(e).to_string();
        let mut objects: Vec<_> = ocel.get_e2o(e).map(|(_, o)| o).collect();
        objects.sort();
        objects.dedup();
        for t in &result.object_types {
            let n = objects
                .iter()
                .filter(|o| ocel.get_ob_type_of(**o) == t)
                .count();
            let entry = distinct
                .entry((activity.clone(), t.clone()))
                .or_insert((usize::MAX, 0));
            *entry = (entry.0.min(n), entry.1.max(n));
        }
    }
    for c in &result.cells {
        let key = (c.activity.clone(), c.object_type.clone());
        assert_eq!((c.min_objects, c.max_objects), distinct[&key], "{key:?}");
        // OC-DECLARE counts an object named twice by an event twice.
        let counts = &involvements[&c.activity][&c.object_type];
        assert_eq!(c.min_objects == 0, counts.min == 0, "{key:?}");
    }
    assert!(result.cells.iter().any(|c| c.max_objects > 1));
    assert_eq!(result.involvements()["place order"]["orders"].max, 1);

    let mut ocel = expansion_log();
    let item = ocel.get_ob_by_id("i0").unwrap();
    let _ = ocel.add_event(
        "place",
        at(99_000),
        Some("place-alone".into()),
        Vec::new(),
        vec![("item".into(), item)],
    );
    let result = reflow(&ocel, &ReflowOptions::default());
    let c = result.cell("place", "orders").unwrap();
    assert_eq!((c.min_objects, c.max_objects), (0, 1));

    let ocel = expansion_log();
    let options = ReflowOptions {
        expand: true,
        ..ReflowOptions::default()
    };
    let result = reflow(&ocel, &options);
    let added: Vec<_> = result.cells.iter().filter(|c| !c.recorded).collect();
    assert!(!added.is_empty());
    for c in added {
        assert_eq!((c.min_objects, c.max_objects), (1, 1), "{c:?}");
    }
}

#[test]
fn a_control_reports_each_stage_and_stops_the_search_once_cancelled() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;
    let ocel = expansion_log();
    let options = ReflowOptions {
        expand: true,
        ..ReflowOptions::default()
    };
    let stages = Mutex::new(Vec::new());
    let ctl = Control::new().on_progress(|p| {
        let mut stages = stages.lock().unwrap();
        if stages.last() != Some(&p.stage) {
            stages.push(p.stage);
        }
    });
    let result = ctl.reflow(&ocel, &options).unwrap();
    assert_eq!(result, reflow(&ocel, &options));
    drop(ctl);
    assert_eq!(
        stages.into_inner().unwrap(),
        ["Building activity grid", "Expanding", "Searching flow"]
    );

    let with = ctl_reflow_with_cancel_at("Building activity grid");
    assert_eq!(with, Err(Cancelled));
    let with = ctl_reflow_with_cancel_at("Searching flow");
    assert_eq!(with, Err(Cancelled));

    fn ctl_reflow_with_cancel_at(stage: &'static str) -> Result<ReflowResult, Cancelled> {
        let ocel = test_log("order-management.json");
        let flag = AtomicBool::new(false);
        let ctl = Control::new()
            .on_progress(|p| {
                if p.stage == stage {
                    flag.store(true, Ordering::Relaxed);
                }
            })
            .cancel_flag(&flag);
        ctl.reflow_with(&ocel, &ReflowOptions::default(), &OrderingAbstraction)
    }
}

/// The exact search on the order-management log takes millions of work units and polls the
/// control in between, so a cancel stops it early.
#[test]
fn a_cancel_stops_the_search_itself() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    let ocel = test_log("order-management.json");
    let options = ReflowOptions {
        search: SearchStrategy::Exact,
        ..ReflowOptions::default()
    };
    let full = reflow(&ocel, &options).stats.work;
    let flag = AtomicBool::new(false);
    let reports = AtomicU64::new(0);
    let last = AtomicU64::new(0);
    let ctl = Control::new()
        .on_progress(|p| {
            if p.stage == "Searching flow" {
                assert_eq!(p.total, Some(options.max_work));
                last.store(p.done, Ordering::Relaxed);
                if reports.fetch_add(1, Ordering::Relaxed) == 2 {
                    flag.store(true, Ordering::Relaxed);
                }
            }
        })
        .cancel_flag(&flag);
    assert_eq!(ctl.reflow(&ocel, &options), Err(Cancelled));
    assert_eq!(reports.load(Ordering::Relaxed), 3);
    assert!(last.load(Ordering::Relaxed) < full / 4);
}

/// The searches already give what the repair phase would add, also under a custom abstraction.
#[test]
fn a_search_result_needs_no_repair() {
    for search in [SearchStrategy::Heuristic, SearchStrategy::Exact] {
        let options = ReflowOptions {
            search,
            ..ReflowOptions::default()
        };
        for result in [
            reflow(&test_log("order-management.json"), &options),
            reflow_with(&chain_log(), &options, &Starts),
        ] {
            let again = result.with_flow_layer(result.flow_layer()).unwrap();
            assert_eq!(again.stats.repaired, 0, "{search:?}");
            assert_eq!(again.cells, result.cells, "{search:?}");
        }
    }
}

#[test]
fn a_result_with_an_index_out_of_range_does_not_load() {
    use serde_json::json;
    let ocel = expansion_log();
    let result = reflow(
        &ocel,
        &ReflowOptions {
            expand: true,
            ..ReflowOptions::default()
        },
    );
    let saved = serde_json::to_value(&result).unwrap();
    assert_eq!(
        serde_json::from_value::<ReflowResult>(saved.clone()).unwrap(),
        result
    );
    for (path, value) in [
        ("/activities", json!(["pick"])),
        ("/grid/0/present/0", json!(9)),
        ("/grid/0/counts", json!([])),
        ("/orderings/0/1", json!(9)),
        ("/added/0/cell/1", json!(9)),
        ("/variants/0/1/0", json!(9)),
    ] {
        let mut broken = saved.clone();
        *broken.pointer_mut(path).expect(path) = value;
        assert!(
            serde_json::from_value::<ReflowResult>(broken).is_err(),
            "{path}"
        );
    }
}
