//! Reflow: preprocessing object-centric event data based on relationships and behavior.
//!
//! Object-centric discovery reads every event-to-object participation as control flow, so a
//! customer or an employee named by most events adds arcs to every model. Reflow decides per
//! cell, i.e., per pair of an activity and an object type, whether its participations flow:
//!
//! - **flow**: the participations contribute arcs;
//! - **implied**: a flow cell at the same activity determines the participations through the
//!   object schema, e.g., the products of the items of an order;
//! - **involved**: the type takes part without contributing arcs.
//!
//! A cell leaves the flow only if every fact the [`Abstraction`] extracts (by default, which
//! activity precedes which for an object type) stays visible or entailed, and among such flow
//! layers [`reflow`] searches one with few directly-follows arcs.
//!
//! The output is a tagged OCEL: the input log with the event-to-object qualifiers of the non-flow
//! participations prefixed by [`NON_FLOW_PREFIX`] (`!`). It is an ordinary OCEL 2.0 log.
//! Discovery reads its [`flow_projection`] (or [`ReflowResult::flow_log`], without the tagged
//! log), every other analysis its [`full_projection`], and
//! [`reflow_from_tagged`] recomputes which non-flow cells are implied from the tagged log
//! alone. [`ReflowResult::annotated_ocpn`] discovers an object-centric Petri net from the flow
//! projection and annotates its transitions with the non-flow cells.
//!
//! | qualifier | participation |
//! |---|---|
//! | `q` | recorded, flows |
//! | `!q` | recorded, does not flow |
//! | `+` | added by expansion, flows |
//!
//! ```no_run
//! use process_mining::analysis::object_centric::reflow::{
//!     flow_projection, reflow, AnnotatedOcpnOptions, CellState, ReflowOptions,
//! };
//! use process_mining::core::event_data::object_centric::linked_ocel::SlimLinkedOCEL;
//! use process_mining::Importable;
//!
//! let log = SlimLinkedOCEL::import_from_path("order-management.json")?;
//! let result = reflow(&log, &ReflowOptions::default());
//! for cell in result.cells.iter().filter(|c| c.state != CellState::Flow) {
//!     println!("{} at {}: {:?}", cell.object_type, cell.activity, cell.state);
//! }
//! let tagged = result.tag(&log);
//! let for_discovery = flow_projection(&tagged);
//! let model = result.annotated_ocpn(&log, &AnnotatedOcpnOptions::default());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod abstraction;
mod expansion;
mod grid;
mod log;
mod ocpn;
mod schema;
mod search;
mod tagged;

use std::collections::{HashMap, HashSet};

use macros_process_mining::{controlled, register_binding};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::event_data::object_centric::{
    linked_ocel::{
        slim_linked_ocel::{EventIndex, ObjectIndex},
        LinkedOCELAccess, SlimLinkedOCEL,
    },
    readable::ReadableOCEL,
};
use crate::utils::control::{Cancelled, Control};

pub use abstraction::{
    Abstraction, AbstractionInput, AbstractionKind, Entailment, Fact, FactKind, OrderingAbstraction,
};
pub use grid::{QualifiedRelation, Reconstruction, ReconstructionVia};
#[cfg(feature = "visualization")]
pub use ocpn::AnnotatedOcpnDrawing;
pub use ocpn::{
    annotated_ocpn, AddedPlace, AnnotatedOcpn, AnnotatedOcpnOptions, CellAnnotation, PlaceRole,
};
pub use search::SearchSeed;
pub use tagged::{
    flow_projection, full_projection, is_tagged, qualifier_flows, qualifier_is_added, untag,
    untagged_qualifier, ADDED_QUALIFIER, NON_FLOW_PREFIX,
};

use grid::{build_grid, ActivityGrid};
use log::{tuple_ids, Cell, LogView, Traces, Variants};
use schema::Schema;
use search::{Budget, Layer, SearchInput};

/// How the flow layer is searched.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum SearchStrategy {
    /// Cover and prune from two seeds, repair both, and keep the layer with fewer arcs.
    #[default]
    Heuristic,
    /// The preserving layer with the fewest arcs among those the repair phase adds nothing to,
    /// by branch and bound over the types that show a fact or order a pair, seeded with the
    /// heuristic layer. Falls back to that layer when the work limit is reached.
    Exact,
}

/// Settings of [`reflow`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct ReflowOptions {
    /// Share of an activity's events at which a reconstruction must give exactly the recorded
    /// objects for a cell to count as determined.
    pub theta: f64,
    /// Also consider unrecorded cells that a relation of the object schema admits (the
    /// expansion case). Off by default: the result then only tags the log.
    pub expand: bool,
    /// Share of an expanded cell's participations that must fall inside the recorded lifetime
    /// of their object.
    pub lifetime_share: f64,
    /// Which facts must stay visible or entailed.
    pub abstraction: AbstractionKind,
    /// How the flow layer is searched.
    pub search: SearchStrategy,
    /// Most work before expansion is skipped and the search stops early, in units of roughly a
    /// nanosecond, counted from the operations done so the outcome does not depend on the
    /// machine. The result is preserving either way and reports [`ReflowStats::limit_reached`].
    pub max_work: u64,
}

impl Default for ReflowOptions {
    fn default() -> Self {
        Self {
            theta: 0.95,
            expand: false,
            lifetime_share: 0.95,
            abstraction: AbstractionKind::Ordering,
            search: SearchStrategy::Heuristic,
            max_work: 250_000_000,
        }
    }
}

/// Whether a cell's participations flow.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub enum CellState {
    /// The participations contribute control-flow arcs.
    Flow,
    /// Non-flow, and a flow cell at the same activity determines the participations.
    Implied,
    /// Non-flow, and nothing at the activity determines the participations.
    Involved,
}

/// The reconstruction that recovers an implied cell: the last step of the shortest chain from
/// the flow cells at its activity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Determination {
    /// The object type whose cell at the same activity the participations are recomputed
    /// from. It flows or is implied itself.
    pub from: String,
    /// How they are recomputed.
    pub reconstruction: Reconstruction,
}

/// One cell of the result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReflowCell {
    /// The activity.
    pub activity: String,
    /// The object type.
    pub object_type: String,
    /// Whether the participations flow.
    pub state: CellState,
    /// `false` for a cell expansion added, which always flows.
    pub recorded: bool,
    /// The cell's participations (distinct event and object pairs).
    pub participations: usize,
    /// The fewest distinct objects of the type an event of the activity has, `0` if some event
    /// has none. A recorded cell counts the recorded participations, so a tagged log gives the
    /// counts of its source log, and an added cell those expansion adds.
    pub min_objects: usize,
    /// The most distinct objects of the type an event of the activity has, counted as for
    /// [`Self::min_objects`].
    pub max_objects: usize,
    /// For an implied cell, what determines it.
    pub determined_by: Option<Determination>,
}

/// Sizes and how the search went.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReflowStats {
    /// Facts the abstraction extracts over the candidate cells.
    pub facts: usize,
    /// Recorded cells that flow.
    pub flow: usize,
    /// Recorded cells that are implied.
    pub implied: usize,
    /// Recorded cells that are involved.
    pub involved: usize,
    /// Unrecorded cells in the flow layer.
    pub added: usize,
    /// Directly-follows arcs (per object type) when every recorded cell flows.
    pub arcs_recorded: usize,
    /// Directly-follows arcs of the flow layer.
    pub arcs_flow: usize,
    /// The seed of the layer the search kept, `None` if the layer was given.
    pub seed: Option<SearchSeed>,
    /// Cells the repair phase added back.
    pub repaired: usize,
    /// Work done, in the unit of [`ReflowOptions::max_work`].
    pub work: u64,
    /// Whether the work limit stopped the search or expansion early.
    pub limit_reached: bool,
    /// Unrecorded cells a relation writes, admitted or not.
    pub expansion_candidates: usize,
}

/// An admitted unrecorded cell and the participations it writes, by event and object id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
struct AddedCell {
    cell: Cell,
    tuples: Vec<(String, String)>,
    /// The fewest and most added objects per event of the activity.
    objects: (usize, usize),
}

impl AddedCell {
    fn new(view: &LogView, locel: &SlimLinkedOCEL, cell: Cell, tuples: &[(u32, u32)]) -> Self {
        debug_assert!(
            tuples.windows(2).all(|w| w[0] < w[1]),
            "sorted and distinct"
        );
        let mut per_event: HashMap<u32, usize> = HashMap::new();
        for (e, _) in tuples {
            *per_event.entry(*e).or_default() += 1;
        }
        let max = per_event.values().copied().max().unwrap_or(0);
        let min = if per_event.len() < view.evs_of_act[cell.0].len() {
            0
        } else {
            per_event.values().copied().min().unwrap_or(0)
        };
        AddedCell {
            cell,
            tuples: tuples
                .iter()
                .map(|(e, o)| tuple_ids(locel, *e, *o))
                .collect(),
            objects: (min, max),
        }
    }
}

/// The result of [`reflow`]: every cell with its state, and what is needed to tag a log or to
/// change the flow layer without reading the log again.
///
/// Deserializing fails on an activity or object type index out of range.
#[derive(Debug, Clone, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReflowResult {
    /// Activities, sorted.
    pub activities: Vec<String>,
    /// Object types, sorted.
    pub object_types: Vec<String>,
    /// Every recorded cell and every added one, sorted by activity and object type.
    pub cells: Vec<ReflowCell>,
    /// Sizes and how the search went.
    pub stats: ReflowStats,
    grid: Vec<ActivityGrid>,
    /// The orderings `(object type, a, b)` the repair phase reads.
    orderings: Vec<(usize, usize, usize)>,
    added: Vec<AddedCell>,
    /// Activity sequences per object type, for the arcs of a flow layer.
    variants: Vec<(usize, Vec<usize>)>,
}

impl<'de> Deserialize<'de> for ReflowResult {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Unchecked {
            activities: Vec<String>,
            object_types: Vec<String>,
            cells: Vec<ReflowCell>,
            stats: ReflowStats,
            grid: Vec<ActivityGrid>,
            orderings: Vec<(usize, usize, usize)>,
            added: Vec<AddedCell>,
            variants: Vec<(usize, Vec<usize>)>,
        }
        let u = Unchecked::deserialize(deserializer)?;
        let result = ReflowResult {
            activities: u.activities,
            object_types: u.object_types,
            cells: u.cells,
            stats: u.stats,
            grid: u.grid,
            orderings: u.orderings,
            added: u.added,
            variants: u.variants,
        };
        result.check_indices().map_err(serde::de::Error::custom)?;
        Ok(result)
    }
}

/// Why a flow layer was not accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReflowError {
    /// The log neither records the cell nor does expansion admit it.
    UnknownCell {
        /// The activity.
        activity: String,
        /// The object type.
        object_type: String,
    },
}

impl std::fmt::Display for ReflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReflowError::UnknownCell {
                activity,
                object_type,
            } => write!(
                f,
                "({activity}, {object_type}) is neither recorded nor admitted by expansion"
            ),
        }
    }
}

impl std::error::Error for ReflowError {}

/// Reflow under the abstraction [`ReflowOptions::abstraction`] names.
///
/// Reports progress in the stages `"Building activity grid"` (activities checked),
/// `"Expanding"` (candidate cells checked, with [`ReflowOptions::expand`]) and `"Searching flow"`
/// (work done, out of [`ReflowOptions::max_work`]).
#[controlled]
#[register_binding(returns_handle)]
pub fn reflow(
    ocel: &SlimLinkedOCEL,
    #[bind(default)] options: &ReflowOptions,
    ctl: &Control<'_>,
) -> Result<ReflowResult, Cancelled> {
    let abstraction = options.abstraction.abstraction();
    let facts_are_orderings = options.abstraction.facts_are_orderings();
    run(
        ocel,
        options,
        abstraction.as_ref(),
        facts_are_orderings,
        ctl,
    )
}

/// Read the flow layer off a tagged log and annotate it.
///
/// The object schema is discovered from the full projection, so nothing but the tagged log
/// is needed. A recorded cell flows if one of its participations flows. Only
/// [`ReflowOptions::theta`] and [`ReflowOptions::abstraction`] are read, and the repair
/// phase adds no cell.
///
/// Reports progress in the stage `"Building activity grid"`, as [`reflow`].
#[controlled]
#[register_binding(returns_handle)]
pub fn reflow_from_tagged(
    ocel: &SlimLinkedOCEL,
    #[bind(default)] options: &ReflowOptions,
    ctl: &Control<'_>,
) -> Result<ReflowResult, Cancelled> {
    read_tagged(ocel, options, ctl)
}

/// Reflow under any abstraction, e.g., one defined outside this crate, instead of the one
/// [`ReflowOptions::abstraction`] names. Reports progress as [`reflow`].
#[controlled]
pub fn reflow_with(
    locel: &SlimLinkedOCEL,
    options: &ReflowOptions,
    abstraction: &dyn Abstraction,
    ctl: &Control<'_>,
) -> Result<ReflowResult, Cancelled> {
    run(locel, options, abstraction, false, ctl)
}

/// What every entry point derives from the log before deciding the flow layer.
struct Prepared {
    view: LogView,
    schema: Schema,
    grid: Vec<ActivityGrid>,
    recorded: HashMap<Cell, usize>,
}

fn prepare(locel: &SlimLinkedOCEL, theta: f64, ctl: &Control<'_>) -> Result<Prepared, Cancelled> {
    let view = LogView::new(locel);
    ctl.check()?;
    let schema = Schema::discover(&view, locel);
    ctl.check()?;
    let grid = build_grid(&view, &schema, theta, ctl)?;
    let mut recorded = HashMap::new();
    for (a, here) in grid.iter().enumerate() {
        for (t, n) in here.present.iter().zip(&here.counts) {
            recorded.insert((a, *t), *n);
        }
    }
    Ok(Prepared {
        view,
        schema,
        grid,
        recorded,
    })
}

fn facts_over(
    abstraction: &dyn Abstraction,
    view: &LogView,
    traces: &Traces,
    candidates: &HashSet<Cell>,
) -> Vec<Fact> {
    let input = AbstractionInput {
        view,
        traces,
        candidates,
        only_type: None,
    };
    let mut facts: Vec<Fact> = abstraction
        .facts(&input)
        .into_iter()
        .filter(|f| f.cells().all(|c| candidates.contains(&c)))
        .collect();
    facts.sort_unstable();
    facts.dedup();
    facts
}

fn orderings_of(facts: &[Fact]) -> Vec<(usize, usize, usize)> {
    facts
        .iter()
        .filter(|f| f.kind == FactKind::ORDER)
        .map(|f| (f.object_type, f.activities[0], f.activities[1]))
        .collect()
}

fn ordering_facts(orderings: &[(usize, usize, usize)]) -> Vec<Fact> {
    orderings
        .iter()
        .map(|(t, a, b)| Fact {
            kind: FactKind::ORDER,
            object_type: *t,
            activities: [*a, *b],
        })
        .collect()
}

fn recorded_cells_of(grid: &[ActivityGrid]) -> HashSet<Cell> {
    grid.iter()
        .enumerate()
        .flat_map(|(a, here)| here.present.iter().map(move |t| (a, *t)))
        .collect()
}

/// What the traces show over a set of candidate cells.
struct Observed {
    facts: Vec<Fact>,
    /// The orderings the repair phase reads.
    orderings: Vec<(usize, usize, usize)>,
    variants: Variants,
}

/// The facts over `candidates` of the traces with the participations `written` added.
fn observe(
    view: &LogView,
    locel: &SlimLinkedOCEL,
    abstraction: &dyn Abstraction,
    facts_are_orderings: bool,
    candidates: &HashSet<Cell>,
    written: &[(u32, u32)],
) -> Observed {
    let traces = Traces::build(view, locel, written, None);
    let facts = facts_over(abstraction, view, &traces, candidates);
    let orderings = if facts_are_orderings {
        orderings_of(&facts)
    } else {
        orderings_of(&facts_over(&OrderingAbstraction, view, &traces, candidates))
    };
    Observed {
        facts,
        orderings,
        variants: Variants::build(view, &traces),
    }
}

fn run(
    locel: &SlimLinkedOCEL,
    options: &ReflowOptions,
    abstraction: &dyn Abstraction,
    facts_are_orderings: bool,
    ctl: &Control<'_>,
) -> Result<ReflowResult, Cancelled> {
    let p = prepare(locel, options.theta, ctl)?;
    let recorded_cells: HashSet<Cell> = p.recorded.keys().copied().collect();
    let observe_with = |candidates: &HashSet<Cell>, written: &[(u32, u32)]| {
        observe(
            &p.view,
            locel,
            abstraction,
            facts_are_orderings,
            candidates,
            written,
        )
    };
    let mut budget = Budget::new(options.max_work, ctl);

    let mut stats = ReflowStats::default();
    let mut admitted = Vec::new();
    let mut recorded = None;
    if options.expand {
        let seen = observe_with(&recorded_cells, &[]);
        ctl.check()?;
        let entailment = abstraction.entailment(&seen.facts, p.view.activities.len());
        let expansion = expansion::expand(
            &p.view,
            locel,
            &p.schema,
            &p.recorded,
            abstraction,
            entailment.as_ref(),
            options.lifetime_share,
            options.max_work,
            ctl,
        )?;
        stats.expansion_candidates = expansion.candidates;
        if expansion.skipped {
            stats.limit_reached = true;
        } else {
            budget.used = expansion.work;
        }
        admitted = expansion.admitted;
        recorded = Some(seen);
    }

    let observed = match recorded {
        Some(seen) if admitted.is_empty() => seen,
        _ => {
            let mut candidates = recorded_cells;
            candidates.extend(admitted.iter().map(|a| a.cell));
            let written: Vec<(u32, u32)> = admitted
                .iter()
                .flat_map(|a| a.tuples.iter().copied())
                .collect();
            observe_with(&candidates, &written)
        }
    };
    ctl.check()?;
    let mut cost = p.recorded.clone();
    for a in &admitted {
        cost.insert(a.cell, a.tuples.len());
    }

    let objects_per_type = p.view.objects_per_type();
    let orderings = ordering_facts(&observed.orderings);
    let input = SearchInput {
        facts: &observed.facts,
        abstraction,
        cost: &cost,
        recorded: &p.recorded,
        objects_per_type: &objects_per_type,
        rep: &p.schema.rep,
        types: &p.view.types,
        n_acts: p.view.activities.len(),
        variants: &observed.variants,
        grid: &p.grid,
        orderings: &orderings,
    };
    budget.poll();
    let mut layer: Layer = search::heuristic(&input, &mut budget);
    if options.search == SearchStrategy::Exact && !budget.hit {
        if let Some(exact) = search::exact(&input, &layer, &mut budget) {
            layer = exact;
        }
    }
    ctl.check()?;
    stats.seed = Some(layer.seed);
    stats.repaired = layer.repaired;
    stats.work = budget.used;
    stats.limit_reached |= budget.hit;

    let added = admitted
        .iter()
        .map(|a| AddedCell::new(&p.view, locel, a.cell, &a.tuples))
        .collect();
    Ok(ReflowResult::assemble(
        p,
        observed,
        added,
        stats,
        layer.cells,
    ))
}

fn read_tagged(
    tagged: &SlimLinkedOCEL,
    options: &ReflowOptions,
    ctl: &Control<'_>,
) -> Result<ReflowResult, Cancelled> {
    let p = prepare(tagged, options.theta, ctl)?;
    let mut added_tuples: HashMap<Cell, (Vec<(u32, u32)>, bool)> = HashMap::new();
    for (e, o, flows) in &p.view.added {
        let cell = (p.view.ev_act[*e as usize], p.view.ob_type[*o as usize]);
        let entry = added_tuples.entry(cell).or_default();
        entry.0.push((*e, *o));
        entry.1 |= *flows;
    }
    let mut candidates: HashSet<Cell> = p.recorded.keys().copied().collect();
    candidates.extend(added_tuples.keys().copied());
    let written: Vec<(u32, u32)> = p.view.added.iter().map(|(e, o, _)| (*e, *o)).collect();
    let abstraction = options.abstraction.abstraction();
    let observed = observe(
        &p.view,
        tagged,
        abstraction.as_ref(),
        options.abstraction.facts_are_orderings(),
        &candidates,
        &written,
    );
    ctl.check()?;

    let mut flow: HashSet<Cell> = p
        .recorded
        .keys()
        .filter(|c| p.view.flowing.contains(c))
        .copied()
        .collect();
    flow.extend(
        added_tuples
            .iter()
            .filter(|(_, (_, f))| *f)
            .map(|(c, _)| *c),
    );
    let mut added: Vec<AddedCell> = added_tuples
        .into_iter()
        .map(|(cell, (tuples, _))| AddedCell::new(&p.view, tagged, cell, &tuples))
        .collect();
    added.sort_by_key(|a| a.cell);
    Ok(ReflowResult::assemble(
        p,
        observed,
        added,
        ReflowStats::default(),
        flow,
    ))
}

impl ReflowResult {
    /// Whether every index into the activities and object types is in range, and every grid
    /// row has one entry per present type (`recon` one per pair).
    fn check_indices(&self) -> Result<(), String> {
        let (n_acts, n_types) = (self.activities.len(), self.object_types.len());
        if self.grid.len() != n_acts {
            return Err(format!(
                "{} grid rows for {n_acts} activities",
                self.grid.len()
            ));
        }
        let out_of_range = |what: &str| Err(format!("{what} out of range"));
        for (a, here) in self.grid.iter().enumerate() {
            let k = here.present.len();
            if here.counts.len() != k || here.objects.len() != k || here.recon.len() != k * k {
                return Err(format!("grid row {a} does not match its {k} object types"));
            }
            if here.present.iter().any(|t| *t >= n_types) {
                return out_of_range(&format!("object type of grid row {a}"));
            }
        }
        let cell_ok = |(a, t): Cell| a < n_acts && t < n_types;
        if let Some(o) = self
            .orderings
            .iter()
            .find(|(t, a, b)| !cell_ok((*a, *t)) || !cell_ok((*b, *t)))
        {
            return out_of_range(&format!("ordering {o:?}"));
        }
        if let Some(c) = self.added.iter().find(|c| !cell_ok(c.cell)) {
            return out_of_range(&format!("added cell {:?}", c.cell));
        }
        if let Some((t, _)) = self
            .variants
            .iter()
            .find(|(t, seq)| seq.iter().any(|a| !cell_ok((*a, *t))) || *t >= n_types)
        {
            return out_of_range(&format!("variant of object type {t}"));
        }
        Ok(())
    }

    /// The result of a prepared log under `flow`.
    fn assemble(
        p: Prepared,
        observed: Observed,
        added: Vec<AddedCell>,
        mut stats: ReflowStats,
        flow: HashSet<Cell>,
    ) -> ReflowResult {
        stats.facts = observed.facts.len();
        let mut result = ReflowResult {
            activities: p.view.activities,
            object_types: p.view.types,
            cells: Vec::new(),
            stats,
            grid: p.grid,
            orderings: observed.orderings,
            added,
            variants: observed.variants.variants,
        };
        result.stats.arcs_recorded = result.arcs(&recorded_cells_of(&result.grid));
        result.settle(flow, false);
        result
    }

    /// The same log under another flow layer, e.g., one an analyst edited.
    ///
    /// The repair phase adds the cells it needs for every recorded cell of an ordering to flow
    /// or be determined. It does not restore entailment, so the result is preserving if `flow`
    /// is. Every cell must be recorded or admitted by expansion.
    pub fn with_flow_layer<A: AsRef<str>, T: AsRef<str>>(
        &self,
        flow: impl IntoIterator<Item = (A, T)>,
    ) -> Result<ReflowResult, ReflowError> {
        let recorded = recorded_cells_of(&self.grid);
        let mut cells = HashSet::new();
        for (a, t) in flow {
            let (a, t) = (a.as_ref(), t.as_ref());
            let cell = self
                .activities
                .iter()
                .position(|x| x == a)
                .zip(self.object_types.iter().position(|x| x == t))
                .filter(|c| recorded.contains(c) || self.added.iter().any(|x| x.cell == *c))
                .ok_or_else(|| ReflowError::UnknownCell {
                    activity: a.to_string(),
                    object_type: t.to_string(),
                })?;
            cells.insert(cell);
        }
        let mut out = self.clone();
        out.stats.seed = None;
        out.stats.work = 0;
        out.stats.limit_reached = false;
        out.settle(cells, true);
        Ok(out)
    }

    /// The flow layer as `(activity, object type)` pairs.
    pub fn flow_layer(&self) -> impl Iterator<Item = (&str, &str)> {
        self.cells
            .iter()
            .filter(|c| c.state == CellState::Flow)
            .map(|c| (c.activity.as_str(), c.object_type.as_str()))
    }

    /// The cell of an activity and an object type, if recorded or added.
    pub fn cell(&self, activity: &str, object_type: &str) -> Option<&ReflowCell> {
        self.cells
            .iter()
            .find(|c| c.activity == activity && c.object_type == object_type)
    }

    /// Tag `locel`: the participations of non-flow cells get the
    /// [`NON_FLOW_PREFIX`], and those of added cells are added with the [`ADDED_QUALIFIER`].
    ///
    /// `locel` is the log this result was computed from, or a log with the same names and ids.
    /// Tags it already carries are replaced.
    pub fn tag(&self, locel: &SlimLinkedOCEL) -> SlimLinkedOCEL {
        self.tag_log(locel, false)
    }

    /// The flow projection of [`Self::tag`] (what discovery reads), without the tagged log in
    /// between: equal to `flow_projection(&self.tag(locel))`.
    pub fn flow_log(&self, locel: &SlimLinkedOCEL) -> SlimLinkedOCEL {
        self.tag_log(locel, true)
    }

    fn tag_log(&self, locel: &SlimLinkedOCEL, flow_only: bool) -> SlimLinkedOCEL {
        let flow: HashSet<(&str, &str)> = self.flow_layer().collect();
        let flows_at: Vec<Vec<bool>> = locel
            .event_types()
            .iter()
            .map(|et| {
                locel
                    .object_types()
                    .iter()
                    .map(|ot| flow.contains(&(et.name.as_str(), ot.name.as_str())))
                    .collect()
            })
            .collect();
        let added: Vec<(EventIndex, ObjectIndex)> = self
            .added
            .iter()
            .filter(|a| {
                flow.contains(&(
                    self.activities[a.cell.0].as_str(),
                    self.object_types[a.cell.1].as_str(),
                ))
            })
            .flat_map(|a| &a.tuples)
            .filter_map(|(e, o)| Some((locel.get_ev_by_id(e)?, locel.get_ob_by_id(o)?)))
            .collect();
        tagged::tag_log(
            locel,
            |et, o| flows_at[et][o.get_ob(locel).object_type],
            &added,
            flow_only,
        )
    }

    fn arcs(&self, flow: &HashSet<Cell>) -> usize {
        log::count_arcs(&self.variants, |c| flow.contains(&c))
    }

    /// Repair `flow` and count the added cells if asked, then derive every cell's state and the
    /// counts. Only an edited layer needs the repair: the searches give repaired layers.
    fn settle(&mut self, mut flow: HashSet<Cell>, repair: bool) {
        if repair {
            let orderings = ordering_facts(&self.orderings);
            self.stats.repaired = search::repair(&self.grid, &orderings, &mut flow);
        }
        let mut cells = Vec::new();
        for (a, here) in self.grid.iter().enumerate() {
            let flows: Vec<bool> = here
                .present
                .iter()
                .map(|t| flow.contains(&(a, *t)))
                .collect();
            let via = here.determining(&flows);
            for (j, t) in here.present.iter().enumerate() {
                let (state, determined_by) = match via[j] {
                    _ if flows[j] => (CellState::Flow, None),
                    Some(i) => (
                        CellState::Implied,
                        here.recon(i, j).map(|reconstruction| Determination {
                            from: self.object_types[here.present[i]].clone(),
                            reconstruction: reconstruction.clone(),
                        }),
                    ),
                    None => (CellState::Involved, None),
                };
                cells.push(ReflowCell {
                    activity: self.activities[a].clone(),
                    object_type: self.object_types[*t].clone(),
                    state,
                    recorded: true,
                    participations: here.counts[j],
                    min_objects: here.objects[j].0,
                    max_objects: here.objects[j].1,
                    determined_by,
                });
            }
        }
        for a in self.added.iter().filter(|a| flow.contains(&a.cell)) {
            cells.push(ReflowCell {
                activity: self.activities[a.cell.0].clone(),
                object_type: self.object_types[a.cell.1].clone(),
                state: CellState::Flow,
                recorded: false,
                participations: a.tuples.len(),
                min_objects: a.objects.0,
                max_objects: a.objects.1,
                determined_by: None,
            });
        }
        cells.sort_by(|x, y| (&x.activity, &x.object_type).cmp(&(&y.activity, &y.object_type)));
        let count = |s: CellState| cells.iter().filter(|c| c.recorded && c.state == s).count();
        self.stats.flow = count(CellState::Flow);
        self.stats.implied = count(CellState::Implied);
        self.stats.involved = count(CellState::Involved);
        self.stats.added = cells.iter().filter(|c| !c.recorded).count();
        self.stats.arcs_flow = self.arcs(&flow);
        self.cells = cells;
    }
}

#[cfg(test)]
mod tests;
