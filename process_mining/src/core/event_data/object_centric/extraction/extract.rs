//! Executing a [`Blueprint`] against real data: the reference semantics everything else (a SQL
//! compiler, in particular) is checked against.

use std::collections::{HashMap, HashSet};

use super::blueprint::{Blueprint, MissingEndpointPolicy, Target};
use super::catalog::{Catalog, TableSchema};
use super::desugar::desugar_with_paths;
use super::expr::ValueExpression;
use super::graph::GraphExecutor;
use super::mapping_exec::{self, MappingPasses, Phase, RunCtx};
use super::provider::RowProvider;
use super::report::{
    DropReason, ErrorLog, ExtractionError, ExtractionReport, MappingRef, MappingStats,
    PushdownDeclined,
};
use super::row::{build_column_index, Row};
use super::sink::ExtractionSink;
use super::validate::validate;

/// Execute `blueprint` against `providers`, sending every event, object and relation to `sink`.
///
/// Refuses to run a blueprint that does not [`validate`] against `catalog`. `providers` is keyed
/// by `source_id`, as a [`NodeOp::Source`](super::blueprint::NodeOp::Source) names it.
///
/// # Three passes, not one
///
/// Every `Object` target runs first, across the whole blueprint, then every `Event` target, then
/// everything that relates two of them. A relation therefore resolves against every entity the
/// blueprint produces, so relation resolution does not depend on mapping order, and matches what
/// a compiled SQL view produces: a join against all objects. See `Phase`.
///
/// Objects and events are separate passes because an inline object reference on a `Target::Event`
/// with no `id` must be emitted alongside the event: a run-minted `UUID` cannot be re-derived
/// later. The cost is that a node read in more than one pass is scanned once per pass.
///
/// # What execution order still decides
///
/// Resolution is order-independent, the rest is not. Order decides which of two `Target::Object`
/// mappings naming one id creates it and which appends, under
/// [`DuplicateObjectPolicy::FirstWins`](super::blueprint::DuplicateObjectPolicy::FirstWins), which
/// of two same-name, same-time attribute writes the sink keeps, and under
/// [`MissingEndpointPolicy::Create`], which of two relation mappings declaring different types for
/// one missing id wins. Nothing here issues an `ORDER BY`, so that order is scan order.
///
/// "Whichever runs first" is not mapping order. Within a phase, mappings are grouped by the node
/// they read so each node is scanned once, and the groups run in first-seen node order across the
/// whole desugared list, making the real order `(phase, node position, mapping index)`. Adding an
/// unrelated mapping on a different node can move that node earlier and flip which of two
/// colliding `Target::Object` mappings wins. Two mappings on the same node still run in mapping
/// order. A blueprint that wants a stable answer should not collide at all (distinct ids, or
/// [`IdRendering::TypePrefixed`](super::blueprint::IdRendering::TypePrefixed)) or put the colliding
/// mappings on one node.
///
/// # Errors
/// Returns [`ExtractionError`] when the blueprint does not validate, a `Source` node names a
/// source absent from `providers`, a [`RowProvider`] call fails, or any [`ExtractionSink`] call
/// fails. A sink failure aborts the run however small the write was, since carrying on would leave
/// a half-written OCEL reporting success. A policy configured to error is collected into the
/// returned [`ExtractionReport::errors`] instead, so one bad row does not abort the whole run.
pub fn extract(
    blueprint: &Blueprint,
    catalog: &dyn Catalog,
    providers: &HashMap<String, &dyn RowProvider>,
    sink: &mut dyn ExtractionSink,
) -> Result<ExtractionReport, ExtractionError> {
    let validation_errors = validate(blueprint, catalog);
    if !validation_errors.is_empty() {
        return Err(ExtractionError::Invalid(validation_errors));
    }

    let desugared = desugar_with_paths(blueprint);
    let mapping_refs: Vec<MappingRef> = desugared
        .iter()
        .enumerate()
        .map(|(index, (path, m))| MappingRef::new(index, path.clone(), m))
        .collect();

    let exec = GraphExecutor::new(blueprint, catalog, providers, &desugared)?;

    // Only a sink that defers endpoint resolution needs this; it is the one that applies the
    // policy, at `finalize`, where the mapping that named the endpoint is long gone.
    sink.set_missing_endpoint_policy(blueprint.on_missing_endpoint)
        .map_err(|e| ExtractionError::Sink {
            context: "announcing the missing-endpoint policy".to_string(),
            source: e,
        })?;

    let mut errors = ErrorLog::new();
    let mut stats: Vec<MappingStats> = mapping_refs
        .iter()
        .cloned()
        .map(MappingStats::new)
        .collect();
    let mut declared_events: Vec<HashSet<String>> = vec![HashSet::new(); desugared.len()];
    let mut declared_objects: Vec<HashSet<String>> = vec![HashSet::new(); desugared.len()];
    let mut attr_types = mapping_exec::DeclaredAttrTypes::new();
    // Outlive the per-row `RunCtx`, so every row writes into the same allocation.
    let mut event_attrs = Vec::new();
    let mut object_attrs = Vec::new();

    // Statically-named types are declared up front, so the declared type set is a function of the
    // blueprint alone, not of which rows happen to match.
    for (i, (_, m)) in desugared.iter().enumerate() {
        let node_schema = exec.schema_of(&m.node);
        declare_static_types(
            sink,
            node_schema,
            &m.target,
            &mut declared_events[i],
            &mut declared_objects[i],
            &mut attr_types,
            &mut errors,
        )?;
    }

    let mut prepared_when = Vec::with_capacity(desugared.len());
    let mut prepared_splits = Vec::with_capacity(desugared.len());
    for (path, m) in &desugared {
        let node_schema = exec.schema_of(&m.node);
        let when = match &m.when {
            Some(p) => Some(
                p.prepare(node_schema)
                    .map_err(|e| ExtractionError::InvalidRegex {
                        pattern: format!("mapping '{path}' when"),
                        message: e.to_string(),
                    })?,
            ),
            None => None,
        };
        prepared_when.push(when);
        let splits =
            mapping_exec::prepare_splits(&m.target).map_err(|e| ExtractionError::InvalidRegex {
                pattern: format!("mapping '{path}' split"),
                message: e.to_string(),
            })?;
        prepared_splits.push(splits);
    }

    // Group mapping indices by the node they read, preserving first-seen node order, so all
    // mappings sharing one node share one scan.
    let mut node_order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, (_, m)) in desugared.iter().enumerate() {
        if !groups.contains_key(&m.node) {
            node_order.push(m.node.clone());
        }
        groups.entry(m.node.clone()).or_default().push(i);
    }

    let passes: Vec<MappingPasses> = desugared
        .iter()
        .map(|(_, m)| MappingPasses::of(&m.target))
        .collect();

    for phase in [Phase::Objects, Phase::Events, Phase::Relations] {
        for node_id in &node_order {
            let indices: Vec<usize> = groups[node_id]
                .iter()
                .copied()
                .filter(|&i| passes[i].runs_in(phase))
                .collect();
            if indices.is_empty() {
                continue;
            }
            let node_schema = exec.schema_of(node_id);
            let names: Vec<String> = node_schema
                .map(|s| s.columns.keys().cloned().collect())
                .unwrap_or_default();
            let refs: Vec<&str> = names.iter().map(String::as_str).collect();
            let index = build_column_index(&refs);

            exec.stream(node_id, &mut |vals| {
                let row = Row {
                    values: vals,
                    index: &index,
                };
                for &mi in &indices {
                    // A mapping read in more than one pass must not report its rows twice.
                    let counts_here = passes[mi].counting_phase() == phase;
                    if counts_here {
                        stats[mi].rows_read += 1;
                    }
                    let (_, m) = &desugared[mi];
                    let guard_ok = match &prepared_when[mi] {
                        Some(p) => p.evaluate(&row),
                        None => true,
                    };
                    if !guard_ok {
                        if counts_here {
                            stats[mi].drop(DropReason::PredicateExcluded);
                        }
                        continue;
                    }
                    let mut ctx = RunCtx {
                        blueprint,
                        sink,
                        declared_events: &mut declared_events[mi],
                        declared_objects: &mut declared_objects[mi],
                        errors: &mut errors,
                        attr_types: &mut attr_types,
                        event_attrs: &mut event_attrs,
                        object_attrs: &mut object_attrs,
                    };
                    mapping_exec::run_target(
                        &mut ctx,
                        &mapping_refs[mi],
                        exec.schema_of(&m.node),
                        &m.target,
                        &prepared_splits[mi],
                        &row,
                        &mut stats[mi],
                        phase,
                    )?;
                }
                Ok(())
            })?;
        }
    }

    // Fatal on failure: a sink that could not finish resolving what it deferred has produced an
    // incomplete log, and reporting that as a success would be worse than failing.
    let finalize = sink.finalize().map_err(|e| ExtractionError::Sink {
        context: "finalizing the sink".to_string(),
        source: e,
    })?;

    // A deferring sink applies `on_missing_endpoint` itself, at finalize, and can only count what
    // it dropped, so the `Error` policy's errors have to be raised here, from that count.
    // Without this the policy silently degraded to `Drop` for every such sink: the per-endpoint
    // `MissingEndpoint` errors are pushed where an endpoint is resolved, and a deferring sink
    // resolves none.
    if blueprint.on_missing_endpoint == MissingEndpointPolicy::Error
        && finalize.unresolved_endpoints > 0
    {
        errors.push(ExtractionError::MissingEndpointsAtFinalize {
            count: finalize.unresolved_endpoints,
        });
    }

    let (errors, errors_suppressed) = errors.into_parts();
    Ok(ExtractionReport {
        per_mapping: stats,
        errors,
        errors_suppressed,
        rows_materialized: exec.rows_materialized(),
        pushdown_declined: exec
            .take_pushdown_rejections()
            .into_iter()
            .map(|(node, reason)| PushdownDeclined { node, reason })
            .collect(),
        finalize,
        timing: None,
    })
}

fn constant_name(e: &ValueExpression) -> Option<String> {
    match e {
        ValueExpression::Constant { value } => Some(value.clone()),
        _ => None,
    }
}

/// Declare every statically-named (`Constant`) event/object type one mapping's target names: its
/// own type if it has one, and every relation endpoint's type. This keeps `Create`-policy
/// synthesis from racing an undeclared type, and lets a zero-match mapping still declare its
/// type.
#[allow(clippy::too_many_arguments)]
fn declare_static_types(
    sink: &mut dyn ExtractionSink,
    node_schema: Option<&TableSchema>,
    target: &Target,
    declared_events: &mut HashSet<String>,
    declared_objects: &mut HashSet<String>,
    attr_types: &mut mapping_exec::DeclaredAttrTypes,
    errors: &mut ErrorLog,
) -> Result<(), ExtractionError> {
    match target {
        Target::Event {
            event_type,
            attributes,
            objects,
            ..
        } => {
            if let Some(name) = constant_name(event_type) {
                let attrs = mapping_exec::reconcile_attr_types(
                    "event",
                    &name,
                    &mapping_exec::build_type_attrs(attributes, node_schema),
                    attr_types,
                    errors,
                );
                sink.declare_event_type(&name, &attrs)
                    .map_err(|e| ExtractionError::Sink {
                        context: format!("declaring event type '{name}'"),
                        source: e,
                    })?;
                declared_events.insert(name);
            }
            for o in objects {
                if let Some(name) = o.object.object_type.as_ref().and_then(constant_name) {
                    sink.declare_object_type(&name, &[])
                        .map_err(|e| ExtractionError::Sink {
                            context: format!("declaring object type '{name}'"),
                            source: e,
                        })?;
                    declared_objects.insert(name);
                }
            }
        }
        Target::Object {
            object_type,
            attributes,
            ..
        } => {
            if let Some(name) = constant_name(object_type) {
                let attrs = mapping_exec::reconcile_attr_types(
                    "object",
                    &name,
                    &mapping_exec::build_type_attrs(attributes, node_schema),
                    attr_types,
                    errors,
                );
                sink.declare_object_type(&name, &attrs)
                    .map_err(|e| ExtractionError::Sink {
                        context: format!("declaring object type '{name}'"),
                        source: e,
                    })?;
                declared_objects.insert(name);
            }
        }
        Target::E2O { event, object, .. } => {
            if let Some(name) = event.event_type.as_ref().and_then(constant_name) {
                sink.declare_event_type(&name, &[])
                    .map_err(|e| ExtractionError::Sink {
                        context: format!("declaring event type '{name}'"),
                        source: e,
                    })?;
                declared_events.insert(name);
            }
            if let Some(name) = object.object_type.as_ref().and_then(constant_name) {
                sink.declare_object_type(&name, &[])
                    .map_err(|e| ExtractionError::Sink {
                        context: format!("declaring object type '{name}'"),
                        source: e,
                    })?;
                declared_objects.insert(name);
            }
        }
        Target::O2O { source, target, .. } => {
            for endpoint in [source, target] {
                if let Some(name) = endpoint.object_type.as_ref().and_then(constant_name) {
                    sink.declare_object_type(&name, &[])
                        .map_err(|e| ExtractionError::Sink {
                            context: format!("declaring object type '{name}'"),
                            source: e,
                        })?;
                    declared_objects.insert(name);
                }
            }
        }
    }
    Ok(())
}
