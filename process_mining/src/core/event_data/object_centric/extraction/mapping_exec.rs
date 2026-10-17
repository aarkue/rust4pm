//! Turning one row into events, objects and relations, per [`Target`] kind.
//!
//! Every entity-and-relation path funnels through [`resolve_object_endpoint`] /
//! [`resolve_event_endpoint`], so `on_missing_endpoint` is honoured identically at an inline
//! object reference, an `E2O`'s object side and an `O2O`'s source and target.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, FixedOffset};

use super::blueprint::{
    Blueprint, DuplicateObjectPolicy, EventEndpoint, IdRendering, InlineObjectRef,
    MissingEndpointPolicy, ObjectEndpoint, Target,
};
use super::catalog::{ColumnSchema, TableSchema};
use super::expr::{AttributeMapping, PreparedSplit, SplitSpec, TimestampSource, ValueExpression};
use super::report::{DropReason, ErrorLog, ExtractionError, MappingRef, MappingStats};
use super::row::Row;
use super::sink::{EventRef, ExtractionSink, ObjectRef, Resolution, SinkError};
use super::validate::target_object_endpoints;
use super::value::{Value, ValueKind};
use crate::core::event_data::object_centric::{
    OCELAttributeType, OCELAttributeValue, OCELTypeAttribute,
};

/// Per-extraction-run state a mapping's row processing needs, threaded through instead of
/// passed as a long, ever-growing parameter list.
///
/// Everything here is sized by the blueprint, not by the data. Nothing tracks ids: the sink
/// answers both questions that would need to, deduplication (see [`MappingStats::deduplicated`])
/// and the first-wins rule on a repeated `(id, attribute, time)` (see
/// [`ExtractionSink::add_object_attribute`]).
pub(crate) struct RunCtx<'a> {
    /// The blueprint being executed, for its id-rendering and endpoint policies.
    pub(crate) blueprint: &'a Blueprint,
    /// Where entities and relations go.
    pub(crate) sink: &'a mut dyn ExtractionSink,
    /// Event type names this mapping has already declared. Reset per mapping. See
    /// [`ensure_event_declared`].
    pub(crate) declared_events: &'a mut HashSet<String>,
    /// Object type names this mapping has already declared. See [`declared_events`](Self::declared_events).
    pub(crate) declared_objects: &'a mut HashSet<String>,
    /// Non-fatal problems collected so far.
    pub(crate) errors: &'a mut ErrorLog,
    /// Every `(kind, type name, attribute name)` declared so far, and the type it was declared
    /// with. Run-level rather than per mapping, see [`reconcile_attr_types`].
    pub(crate) attr_types: &'a mut DeclaredAttrTypes,
    /// Scratch buffer for one event row's attribute values, owned by the run rather than by this
    /// context, which is rebuilt per row. See [`fill_event_attrs`].
    pub(crate) event_attrs: &'a mut Vec<(String, OCELAttributeValue)>,
    /// Scratch buffer for one object row's timed attribute values.
    pub(crate) object_attrs: &'a mut Vec<(String, DateTime<FixedOffset>, OCELAttributeValue)>,
}

/// `kind -> type name -> attribute name -> declared value type`, accumulated across every mapping
/// in a run. `kind` is `"event"` or `"object"`.
///
/// Nested rather than keyed by one tuple so [`effective_attr_type`], which runs once per attribute
/// per row, probes it with `&str` lookups instead of building an owning key.
pub(crate) type DeclaredAttrTypes =
    HashMap<&'static str, HashMap<String, HashMap<String, OCELAttributeType>>>;

/// The type `(kind, type_name, attribute)` was declared with, if it has been declared.
fn declared_attr_type(
    declared: &DeclaredAttrTypes,
    kind: &'static str,
    type_name: &str,
    attribute: &str,
) -> Option<OCELAttributeType> {
    declared.get(kind)?.get(type_name)?.get(attribute).copied()
}

/// Record `ty` as what `(kind, type_name, attribute)` is declared with.
fn record_attr_type(
    declared: &mut DeclaredAttrTypes,
    kind: &'static str,
    type_name: &str,
    attribute: &str,
    ty: OCELAttributeType,
) {
    declared
        .entry(kind)
        .or_default()
        .entry(type_name.to_string())
        .or_default()
        .insert(attribute.to_string(), ty);
}

/// Record what `attrs` declares for `type_name`, reporting any attribute two mappings declare
/// under genuinely different types.
///
/// Reported as [`ExtractionError::ConflictingAttributeType`], with the declaration widened via
/// [`OCELAttributeType::coalesce`]. Rows are converted to the widened type too, not only the
/// declaration: see [`effective_attr_type`].
pub(crate) fn reconcile_attr_types(
    kind: &'static str,
    type_name: &str,
    attrs: &[OCELTypeAttribute],
    seen: &mut DeclaredAttrTypes,
    errors: &mut ErrorLog,
) -> Vec<OCELTypeAttribute> {
    attrs
        .iter()
        .map(|a| {
            let declared = OCELAttributeType::from_type_str(&a.value_type);
            match declared_attr_type(seen, kind, type_name, &a.name) {
                Some(previous) if previous != declared => {
                    let widened = previous.coalesce(declared);
                    errors.push(ExtractionError::ConflictingAttributeType {
                        kind,
                        type_name: type_name.to_string(),
                        attribute: a.name.clone(),
                        declared: previous,
                        conflicting: declared,
                    });
                    record_attr_type(seen, kind, type_name, &a.name, widened);
                    OCELTypeAttribute::new(&a.name, &widened)
                }
                Some(previous) => OCELTypeAttribute::new(&a.name, &previous),
                None => {
                    record_attr_type(seen, kind, type_name, &a.name, declared);
                    a.clone()
                }
            }
        })
        .collect()
}

/// Prepare every [`ObjectEndpoint`] a target names, in the order [`target_object_endpoints`]
/// walks them, which is what this module's `run_*` functions index into.
pub(crate) fn prepare_splits(target: &Target) -> Result<Vec<Option<PreparedSplit>>, regex::Error> {
    target_object_endpoints(target)
        .into_iter()
        .map(|e| e.split.as_ref().map(SplitSpec::prepare).transpose())
        .collect()
}

/// One of [`extract`](super::extract::extract)'s three passes, run in the order they are
/// declared here.
///
/// Endpoint resolution is staged, not incremental: every `Object` target runs first, then every
/// `Event` target, then everything that relates two of them. Resolving incrementally would make
/// a relation mapping's output depend on mapping order, and is unreproducible in SQL, where a
/// relation view joins against all objects rather than those emitted so far.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    /// Every `Object` target.
    Objects,
    /// Every `Event` target, plus the inline object references of an event whose id this run
    /// mints itself (see [`MappingPasses`]). Runs after every object exists.
    Events,
    /// Every `E2O` and `O2O` target, plus the inline object references of an event with an
    /// author-given id.
    Relations,
}

/// Which of the three [`Phase`]s one mapping does work in.
///
/// Almost everything belongs to exactly one. The exception is a
/// [`Target::Event`] with inline object references: the event itself is an entity
/// ([`Phase::Events`]) and its references are relations ([`Phase::Relations`]), so such a
/// mapping's node is read once per pass.
///
/// Unless the event's `id` is `None`: the id is then a freshly minted `UUID`, which the relations
/// pass could not re-derive, so the references go in the events pass instead. That is sound
/// because the objects pass has already run.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MappingPasses {
    /// Runs in [`Phase::Objects`].
    pub(crate) objects: bool,
    /// Runs in [`Phase::Events`].
    pub(crate) events: bool,
    /// Runs in [`Phase::Relations`].
    pub(crate) relations: bool,
}

impl MappingPasses {
    /// Which passes `target` needs.
    pub(crate) fn of(target: &Target) -> Self {
        match target {
            Target::Event { id, objects, .. } => Self {
                objects: false,
                events: true,
                relations: id.is_some() && !objects.is_empty(),
            },
            Target::Object { .. } => Self {
                objects: true,
                events: false,
                relations: false,
            },
            Target::E2O { .. } | Target::O2O { .. } => Self {
                objects: false,
                events: false,
                relations: true,
            },
        }
    }

    /// Whether this mapping does anything in `phase`.
    pub(crate) fn runs_in(self, phase: Phase) -> bool {
        match phase {
            Phase::Objects => self.objects,
            Phase::Events => self.events,
            Phase::Relations => self.relations,
        }
    }

    /// The pass whose rows count toward this mapping's `rows_read` and `PredicateExcluded`
    /// tallies, so a mapping read in more than one pass does not report its rows twice.
    pub(crate) fn counting_phase(self) -> Phase {
        if self.objects {
            Phase::Objects
        } else if self.events {
            Phase::Events
        } else {
            Phase::Relations
        }
    }
}

/// Execute one mapping's target against one row.
///
/// # Errors
/// Returns [`ExtractionError`] for any sink failure, which aborts the run. Policy violations are
/// pushed to `ctx.errors` instead, so one bad row does not.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_target(
    ctx: &mut RunCtx<'_>,
    mapping_ref: &MappingRef,
    node_schema: Option<&TableSchema>,
    target: &Target,
    splits: &[Option<PreparedSplit>],
    row: &Row<'_>,
    stats: &mut MappingStats,
    phase: Phase,
) -> Result<(), ExtractionError> {
    match (target, phase) {
        (
            Target::Event {
                event_type,
                id,
                timestamp,
                attributes,
                objects,
            },
            Phase::Events,
        ) => run_event(
            ctx,
            mapping_ref,
            node_schema,
            splits,
            row,
            stats,
            event_type,
            id,
            timestamp,
            attributes,
            objects,
        ),
        (
            Target::Event {
                event_type,
                id,
                objects,
                ..
            },
            Phase::Relations,
        ) => run_event_inline_objects(
            ctx,
            mapping_ref,
            splits,
            row,
            stats,
            event_type,
            id.as_ref(),
            objects,
        ),
        (
            Target::Object {
                object_type,
                id,
                timestamp,
                attributes,
            },
            Phase::Objects,
        ) => run_object(
            ctx,
            mapping_ref,
            node_schema,
            row,
            stats,
            object_type,
            id,
            timestamp,
            attributes,
        ),
        (
            Target::E2O {
                event,
                object,
                qualifier,
            },
            Phase::Relations,
        ) => run_e2o(
            ctx,
            mapping_ref,
            splits,
            row,
            stats,
            event,
            object,
            qualifier,
        ),
        (
            Target::O2O {
                source,
                target,
                qualifier,
            },
            Phase::Relations,
        ) => run_o2o(
            ctx,
            mapping_ref,
            splits,
            row,
            stats,
            source,
            target,
            qualifier,
        ),
        // The combinations `MappingPasses::of` never schedules.
        (Target::Object { .. }, Phase::Events | Phase::Relations)
        | (Target::Event { .. }, Phase::Objects)
        | (Target::E2O { .. } | Target::O2O { .. }, Phase::Objects | Phase::Events) => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_event(
    ctx: &mut RunCtx<'_>,
    mapping_ref: &MappingRef,
    node_schema: Option<&TableSchema>,
    splits: &[Option<PreparedSplit>],
    row: &Row<'_>,
    stats: &mut MappingStats,
    event_type: &ValueExpression,
    id: &Option<ValueExpression>,
    timestamp: &TimestampSource,
    attributes: &[AttributeMapping],
    objects: &[InlineObjectRef],
) -> Result<(), ExtractionError> {
    let Some(type_name) = event_type.evaluate(row) else {
        stats.drop(DropReason::NullOrUnrenderableId);
        return Ok(());
    };
    ensure_event_declared(ctx, &type_name, attributes, node_schema)?;

    let raw_id = match id {
        Some(expr) => match identity(expr, row) {
            Some(s) => s,
            None => {
                stats.drop(DropReason::NullOrUnrenderableId);
                return Ok(());
            }
        },
        None => uuid::Uuid::new_v4().to_string(),
    };
    let rendered_id = render_id(ctx.blueprint.id_rendering, &raw_id, &type_name);

    let Some(ts) = timestamp.parse(row) else {
        stats.drop(drop_reason_for(timestamp, row));
        return Ok(());
    };

    let mut attrs = std::mem::take(ctx.event_attrs);
    fill_event_attrs(
        &mut attrs,
        attributes,
        ctx.attr_types,
        &type_name,
        node_schema,
        row,
        stats,
    );
    let added = ctx.sink.add_event(&type_name, ts, &rendered_id, &attrs);
    *ctx.event_attrs = attrs;

    let ev_ref = match added {
        Ok(r) => r,
        Err(SinkError::DuplicateEvent { .. }) => {
            // The dropped event's inline object references are not lost with it: the relations
            // pass emits them against the event this id already names.
            stats.deduplicated += 1;
            return Ok(());
        }
        Err(e) => {
            return Err(ExtractionError::Sink {
                context: format!("adding event '{rendered_id}'"),
                source: e,
            })
        }
    };
    stats.entities_emitted += 1;

    // Only when the id was minted here: see `MappingPasses`.
    if id.is_none() {
        for (i, o) in objects.iter().enumerate() {
            run_inline_object(
                ctx,
                mapping_ref,
                &ev_ref,
                o,
                splits.get(i).and_then(Option::as_ref),
                row,
                stats,
            )?;
        }
    }
    Ok(())
}

/// The relations-pass half of a [`Target::Event`] with an author-given id: re-derive which event
/// this row named, then emit its inline object references against it.
#[allow(clippy::too_many_arguments)]
fn run_event_inline_objects(
    ctx: &mut RunCtx<'_>,
    mapping_ref: &MappingRef,
    splits: &[Option<PreparedSplit>],
    row: &Row<'_>,
    stats: &mut MappingStats,
    event_type: &ValueExpression,
    id: Option<&ValueExpression>,
    objects: &[InlineObjectRef],
) -> Result<(), ExtractionError> {
    let (Some(type_name), Some(id_expr)) = (event_type.evaluate(row), id) else {
        // Already counted in the events pass, where this row produced no event either.
        return Ok(());
    };
    let Some(raw_id) = identity(id_expr, row) else {
        return Ok(());
    };
    let rendered_id = render_id(ctx.blueprint.id_rendering, &raw_id, &type_name);

    let Some(ev_ref) = ctx
        .sink
        .resolve_event(&rendered_id, Some(&type_name))
        .into_ref()
    else {
        // The event this row names does not exist, because its own events-pass run dropped it.
        for _ in objects {
            stats.drop(DropReason::UnresolvedEndpoint);
        }
        return Ok(());
    };

    for (i, o) in objects.iter().enumerate() {
        run_inline_object(
            ctx,
            mapping_ref,
            &ev_ref,
            o,
            splits.get(i).and_then(Option::as_ref),
            row,
            stats,
        )?;
    }
    Ok(())
}

fn run_inline_object(
    ctx: &mut RunCtx<'_>,
    mapping_ref: &MappingRef,
    ev_ref: &EventRef,
    o: &InlineObjectRef,
    split: Option<&PreparedSplit>,
    row: &Row<'_>,
    stats: &mut MappingStats,
) -> Result<(), ExtractionError> {
    let Some(raw) = identity(&o.object.id, row) else {
        stats.drop(DropReason::NullOrUnrenderableId);
        return Ok(());
    };
    let raw_ids = split_or_single(split, raw);
    if raw_ids.is_empty() {
        stats.drop(DropReason::NullOrUnrenderableId);
        return Ok(());
    }
    let qualifier = o
        .qualifier
        .as_ref()
        .and_then(|q| q.evaluate(row))
        .unwrap_or_default();
    for raw_id in raw_ids {
        match resolve_object_endpoint(ctx, mapping_ref, &o.object, &raw_id, "inline object", row)? {
            Some(obj_ref) => {
                ctx.sink
                    .add_e2o(ev_ref, &obj_ref, &qualifier)
                    .map_err(|e| ExtractionError::Sink {
                        context: "adding inline object relation".to_string(),
                        source: e,
                    })?;
                stats.entities_emitted += 1;
            }
            None => stats.drop(DropReason::UnresolvedEndpoint),
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_object(
    ctx: &mut RunCtx<'_>,
    mapping_ref: &MappingRef,
    node_schema: Option<&TableSchema>,
    row: &Row<'_>,
    stats: &mut MappingStats,
    object_type: &ValueExpression,
    id: &ValueExpression,
    timestamp: &Option<TimestampSource>,
    attributes: &[AttributeMapping],
) -> Result<(), ExtractionError> {
    let Some(type_name) = object_type.evaluate(row) else {
        stats.drop(DropReason::NullOrUnrenderableId);
        return Ok(());
    };
    ensure_object_declared(ctx, &type_name, attributes, node_schema)?;

    let Some(raw_id) = identity(id, row) else {
        stats.drop(DropReason::NullOrUnrenderableId);
        return Ok(());
    };
    let rendered_id = render_id(ctx.blueprint.id_rendering, &raw_id, &type_name);

    let ts = match timestamp {
        Some(source) => match source.parse(row) {
            Some(t) => t,
            None => {
                stats.drop(drop_reason_for(source, row));
                return Ok(());
            }
        },
        // A static object attribute has no instant of its own, so every row writes at the epoch
        // and the sink's first-wins rule keeps one value.
        None => DateTime::UNIX_EPOCH.into(),
    };
    let mut attrs = std::mem::take(ctx.object_attrs);
    fill_object_attrs(
        &mut attrs,
        attributes,
        ctx.attr_types,
        &type_name,
        node_schema,
        row,
        ts,
        stats,
    );

    let resolution = ctx.sink.resolve_object(&rendered_id, Some(&type_name));
    // A deferring sink's handle. It cannot say whether the object exists, so `add_object` below is
    // what finds out, and this handle is what the attributes are written against if it does.
    let mut deferred = None;
    let existing = match resolution {
        Resolution::Exists(r) => Some(r),
        Resolution::Deferred(r) => {
            deferred = Some(r);
            None
        }
        Resolution::Missing => None,
    };

    let outcome = if let Some(existing) = existing {
        write_to_existing_object(ctx, mapping_ref, stats, &existing, &rendered_id, &attrs)
    } else {
        match ctx.sink.add_object(&type_name, &rendered_id, &attrs) {
            Ok(_) => {
                stats.entities_emitted += 1;
                Ok(())
            }
            // The id already exists, which is what an eager sink answers `Exists` to, so this
            // takes the same path.
            Err(SinkError::DuplicateObject { .. }) if deferred.is_some() => {
                let existing = deferred.expect("guarded above");
                write_to_existing_object(ctx, mapping_ref, stats, &existing, &rendered_id, &attrs)
            }
            // The id is taken by an object of a different type, which only `IdRendering::Raw`
            // allows. Two distinct entities collided, so the row is dropped rather than written
            // onto the other type's object.
            Err(SinkError::DuplicateObject { .. } | SinkError::IdTypeCollision { .. }) => {
                stats.drop(DropReason::IdTypeCollision);
                ctx.errors.push(ExtractionError::IdTypeCollision {
                    mapping: mapping_ref.clone(),
                    id: rendered_id.clone(),
                    requested_type: type_name,
                });
                Ok(())
            }
            Err(e) => Err(ExtractionError::Sink {
                context: format!("adding object '{rendered_id}'"),
                source: e,
            }),
        }
    };
    *ctx.object_attrs = attrs;
    outcome
}

/// What a [`Target::Object`] row does when the object it names already exists, whether the sink
/// reported that as an eager `Exists` or as an `add_object` rejection.
///
/// Every attribute the row carries is offered to the sink unconditionally. A change-tracked
/// mapping's rows carry distinct timestamps and are all recorded. A static mapping's all carry
/// the epoch, so the sink's first-wins rule on `(id, name, time)` keeps exactly one. See
/// [`ExtractionSink::add_object_attribute`].
fn write_to_existing_object(
    ctx: &mut RunCtx<'_>,
    mapping_ref: &MappingRef,
    stats: &mut MappingStats,
    existing: &ObjectRef,
    rendered_id: &str,
    attrs: &[(String, DateTime<FixedOffset>, OCELAttributeValue)],
) -> Result<(), ExtractionError> {
    match ctx.blueprint.on_duplicate_object {
        DuplicateObjectPolicy::Error => {
            // A loss, not a deduplication: the row's attributes go nowhere.
            stats.drop(DropReason::DuplicateObjectRejected);
            ctx.errors.push(ExtractionError::DuplicateObject {
                mapping: mapping_ref.clone(),
                id: rendered_id.to_string(),
            });
        }
        DuplicateObjectPolicy::FirstWins => {
            stats.deduplicated += 1;
            for (name, t, v) in attrs {
                ctx.sink
                    .add_object_attribute(existing, name, *t, v.clone())
                    .map_err(|e| ExtractionError::Sink {
                        context: format!("appending attribute to object '{rendered_id}'"),
                        source: e,
                    })?;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_e2o(
    ctx: &mut RunCtx<'_>,
    mapping_ref: &MappingRef,
    splits: &[Option<PreparedSplit>],
    row: &Row<'_>,
    stats: &mut MappingStats,
    event: &EventEndpoint,
    object: &ObjectEndpoint,
    qualifier: &Option<ValueExpression>,
) -> Result<(), ExtractionError> {
    let Some(ev_raw) = identity(&event.id, row) else {
        stats.drop(DropReason::NullOrUnrenderableId);
        return Ok(());
    };
    let Some(ev_ref) = resolve_event_endpoint(ctx, mapping_ref, event, &ev_raw, row)? else {
        stats.drop(DropReason::UnresolvedEndpoint);
        return Ok(());
    };

    let Some(obj_raw) = identity(&object.id, row) else {
        stats.drop(DropReason::NullOrUnrenderableId);
        return Ok(());
    };
    let raw_ids = split_or_single(splits.first().and_then(Option::as_ref), obj_raw);
    if raw_ids.is_empty() {
        stats.drop(DropReason::NullOrUnrenderableId);
        return Ok(());
    }
    let q = qualifier
        .as_ref()
        .and_then(|e| e.evaluate(row))
        .unwrap_or_default();
    for raw_id in raw_ids {
        match resolve_object_endpoint(ctx, mapping_ref, object, &raw_id, "object", row)? {
            Some(obj_ref) => {
                ctx.sink
                    .add_e2o(&ev_ref, &obj_ref, &q)
                    .map_err(|e| ExtractionError::Sink {
                        context: "adding e2o relation".to_string(),
                        source: e,
                    })?;
                stats.entities_emitted += 1;
            }
            None => stats.drop(DropReason::UnresolvedEndpoint),
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_o2o(
    ctx: &mut RunCtx<'_>,
    mapping_ref: &MappingRef,
    splits: &[Option<PreparedSplit>],
    row: &Row<'_>,
    stats: &mut MappingStats,
    source: &ObjectEndpoint,
    target: &ObjectEndpoint,
    qualifier: &Option<ValueExpression>,
) -> Result<(), ExtractionError> {
    let Some(src_raw) = identity(&source.id, row) else {
        stats.drop(DropReason::NullOrUnrenderableId);
        return Ok(());
    };
    let Some(tgt_raw) = identity(&target.id, row) else {
        stats.drop(DropReason::NullOrUnrenderableId);
        return Ok(());
    };
    let src_ids = split_or_single(splits.first().and_then(Option::as_ref), src_raw);
    let tgt_ids = split_or_single(splits.get(1).and_then(Option::as_ref), tgt_raw);
    if src_ids.is_empty() || tgt_ids.is_empty() {
        stats.drop(DropReason::NullOrUnrenderableId);
        return Ok(());
    }
    let q = qualifier
        .as_ref()
        .and_then(|e| e.evaluate(row))
        .unwrap_or_default();
    for s_raw in &src_ids {
        let Some(s_ref) = resolve_object_endpoint(ctx, mapping_ref, source, s_raw, "source", row)?
        else {
            stats.drop(DropReason::UnresolvedEndpoint);
            continue;
        };
        for t_raw in &tgt_ids {
            match resolve_object_endpoint(ctx, mapping_ref, target, t_raw, "target", row)? {
                Some(t_ref) => {
                    ctx.sink
                        .add_o2o(&s_ref, &t_ref, &q)
                        .map_err(|e| ExtractionError::Sink {
                            context: "adding o2o relation".to_string(),
                            source: e,
                        })?;
                    stats.entities_emitted += 1;
                }
                None => stats.drop(DropReason::UnresolvedEndpoint),
            }
        }
    }
    Ok(())
}

/// Resolve an object endpoint to a handle, applying `on_missing_endpoint`. Every object-relating
/// position (inline references, `E2O`'s object, `O2O`'s source/target) goes through here, so the
/// policy is honoured identically at each.
///
/// Touches no counter, which is why it takes no [`MappingStats`]: resolving an endpoint that
/// already exists is the normal successful case, not a deduplication.
fn resolve_object_endpoint(
    ctx: &mut RunCtx<'_>,
    mapping_ref: &MappingRef,
    endpoint: &ObjectEndpoint,
    raw_id: &str,
    label: &'static str,
    row: &Row<'_>,
) -> Result<Option<ObjectRef>, ExtractionError> {
    let type_name = endpoint.object_type.as_ref().and_then(|e| e.evaluate(row));
    let rendered_id = match ctx.blueprint.id_rendering {
        IdRendering::Raw => raw_id.to_string(),
        IdRendering::TypePrefixed => match &type_name {
            Some(t) => format!("{t}-{raw_id}"),
            None => return Ok(None),
        },
    };
    // `Deferred` is a handle to write a relation against, not a promise the object exists: the
    // sink resolves it at `finalize` and applies `on_missing_endpoint` there.
    match ctx.sink.resolve_object(&rendered_id, type_name.as_deref()) {
        Resolution::Exists(r) | Resolution::Deferred(r) => return Ok(Some(r)),
        Resolution::Missing => {}
    }
    match ctx.blueprint.on_missing_endpoint {
        MissingEndpointPolicy::Drop => Ok(None),
        MissingEndpointPolicy::Error => {
            ctx.errors.push(ExtractionError::MissingEndpoint {
                mapping: mapping_ref.clone(),
                endpoint: label,
                id: rendered_id,
            });
            Ok(None)
        }
        MissingEndpointPolicy::Create => {
            let Some(t) = &type_name else {
                return Ok(None);
            };
            if ctx.declared_objects.insert(t.clone()) {
                ctx.sink
                    .declare_object_type(t, &[])
                    .map_err(|e| ExtractionError::Sink {
                        context: format!("declaring object type '{t}'"),
                        source: e,
                    })?;
            }
            match ctx.sink.add_object(t, &rendered_id, &[]) {
                Ok(r) => Ok(Some(r)),
                // The id is taken by an object of a different type, which is why
                // `resolve_object` answered `Missing`. Creating is impossible and merging the two
                // types would be worse, so the relation is dropped.
                Err(SinkError::DuplicateObject { .. }) => {
                    ctx.errors.push(ExtractionError::IdTypeCollision {
                        mapping: mapping_ref.clone(),
                        id: rendered_id,
                        requested_type: t.clone(),
                    });
                    Ok(None)
                }
                Err(e) => Err(ExtractionError::Sink {
                    context: format!("creating missing object '{rendered_id}'"),
                    source: e,
                }),
            }
        }
    }
}

/// Resolve an event endpoint (`E2O`'s event side) to a handle.
///
/// `Create` cannot synthesise a missing event, since there is no timestamp to give it, so it
/// behaves like `Drop` here. Only object endpoints can be created (see
/// [`resolve_object_endpoint`]).
fn resolve_event_endpoint(
    ctx: &mut RunCtx<'_>,
    mapping_ref: &MappingRef,
    endpoint: &EventEndpoint,
    raw_id: &str,
    row: &Row<'_>,
) -> Result<Option<EventRef>, ExtractionError> {
    let type_name = endpoint.event_type.as_ref().and_then(|e| e.evaluate(row));
    let rendered_id = match ctx.blueprint.id_rendering {
        IdRendering::Raw => raw_id.to_string(),
        IdRendering::TypePrefixed => match &type_name {
            Some(t) => format!("{t}-{raw_id}"),
            None => return Ok(None),
        },
    };
    match ctx.sink.resolve_event(&rendered_id, type_name.as_deref()) {
        Resolution::Exists(r) | Resolution::Deferred(r) => return Ok(Some(r)),
        Resolution::Missing => {}
    }
    match ctx.blueprint.on_missing_endpoint {
        MissingEndpointPolicy::Drop | MissingEndpointPolicy::Create => Ok(None),
        MissingEndpointPolicy::Error => {
            ctx.errors.push(ExtractionError::MissingEndpoint {
                mapping: mapping_ref.clone(),
                endpoint: "event",
                id: rendered_id,
            });
            Ok(None)
        }
    }
}

fn ensure_event_declared(
    ctx: &mut RunCtx<'_>,
    name: &str,
    attributes: &[AttributeMapping],
    node_schema: Option<&TableSchema>,
) -> Result<(), ExtractionError> {
    if ctx.declared_events.insert(name.to_string()) {
        let attrs = reconcile_attr_types(
            "event",
            name,
            &build_type_attrs(attributes, node_schema),
            ctx.attr_types,
            ctx.errors,
        );
        ctx.sink
            .declare_event_type(name, &attrs)
            .map_err(|e| ExtractionError::Sink {
                context: format!("declaring event type '{name}'"),
                source: e,
            })?;
    }
    Ok(())
}

fn ensure_object_declared(
    ctx: &mut RunCtx<'_>,
    name: &str,
    attributes: &[AttributeMapping],
    node_schema: Option<&TableSchema>,
) -> Result<(), ExtractionError> {
    if ctx.declared_objects.insert(name.to_string()) {
        let attrs = reconcile_attr_types(
            "object",
            name,
            &build_type_attrs(attributes, node_schema),
            ctx.attr_types,
            ctx.errors,
        );
        ctx.sink
            .declare_object_type(name, &attrs)
            .map_err(|e| ExtractionError::Sink {
                context: format!("declaring object type '{name}'"),
                source: e,
            })?;
    }
    Ok(())
}

/// The declared [`OCELTypeAttribute`] list for a target's attribute mappings, used both to
/// declare a type and (by [`resolve_attribute_type`]) to convert each row's values.
pub(crate) fn build_type_attrs(
    attributes: &[AttributeMapping],
    node_schema: Option<&TableSchema>,
) -> Vec<OCELTypeAttribute> {
    attributes
        .iter()
        .map(|a| {
            let t = resolve_attribute_type(a, node_schema);
            OCELTypeAttribute::new(&a.name, &t)
        })
        .collect()
}

/// An attribute's declared type: `value_type` if given, else the source column's declared kind
/// (from `node_schema`, which carries [`Predicate::prepare`](super::predicate::Predicate::prepare)'s
/// same catalog-derived types) mapped to an [`OCELAttributeType`], else `String`.
fn resolve_attribute_type(
    a: &AttributeMapping,
    node_schema: Option<&TableSchema>,
) -> OCELAttributeType {
    if let Some(t) = a.value_type {
        return t;
    }
    node_schema
        .and_then(|s| s.columns.get(&a.source_column))
        .and_then(ColumnSchema::declared_kind)
        .map(kind_to_attr_type)
        .unwrap_or(OCELAttributeType::String)
}

/// The type one row's value for `a` is converted to: the reconciled declaration for
/// `(kind, type_name, a.name)` when there is one, not this mapping's own `value_type`.
///
/// Two mappings declaring one attribute under different types have the declaration widened by
/// [`reconcile_attr_types`], and each mapping's rows convert to the widened type.
///
/// Only as good as what has been declared so far: a type named from a column declares lazily,
/// so rows written before a later mapping widens the declaration keep the narrower type.
fn effective_attr_type(
    declared: &DeclaredAttrTypes,
    kind: &'static str,
    type_name: &str,
    a: &AttributeMapping,
    node_schema: Option<&TableSchema>,
) -> OCELAttributeType {
    declared_attr_type(declared, kind, type_name, &a.name)
        .unwrap_or_else(|| resolve_attribute_type(a, node_schema))
}

/// Overwrite `buf` with this row's values for `attributes`, reusing the `Vec` and each name
/// `String`, since one mapping's attribute list is the same on every row.
fn fill_event_attrs(
    buf: &mut Vec<(String, OCELAttributeValue)>,
    attributes: &[AttributeMapping],
    declared: &DeclaredAttrTypes,
    type_name: &str,
    node_schema: Option<&TableSchema>,
    row: &Row<'_>,
    stats: &mut MappingStats,
) {
    buf.truncate(attributes.len());
    for (i, a) in attributes.iter().enumerate() {
        let ty = effective_attr_type(declared, "event", type_name, a, node_schema);
        let value = attribute_value(row.get(&a.source_column), ty, stats);
        match buf.get_mut(i) {
            Some(slot) => {
                set_name(&mut slot.0, &a.name);
                slot.1 = value;
            }
            None => buf.push((a.name.clone(), value)),
        }
    }
}

/// [`fill_event_attrs`] for an object's timed values, all at `ts`.
#[allow(clippy::too_many_arguments)]
fn fill_object_attrs(
    buf: &mut Vec<(String, DateTime<FixedOffset>, OCELAttributeValue)>,
    attributes: &[AttributeMapping],
    declared: &DeclaredAttrTypes,
    type_name: &str,
    node_schema: Option<&TableSchema>,
    row: &Row<'_>,
    ts: DateTime<FixedOffset>,
    stats: &mut MappingStats,
) {
    buf.truncate(attributes.len());
    for (i, a) in attributes.iter().enumerate() {
        let ty = effective_attr_type(declared, "object", type_name, a, node_schema);
        let value = attribute_value(row.get(&a.source_column), ty, stats);
        match buf.get_mut(i) {
            Some(slot) => {
                set_name(&mut slot.0, &a.name);
                slot.1 = ts;
                slot.2 = value;
            }
            None => buf.push((a.name.clone(), ts, value)),
        }
    }
}

fn set_name(slot: &mut String, name: &str) {
    slot.clear();
    slot.push_str(name);
}

fn kind_to_attr_type(k: ValueKind) -> OCELAttributeType {
    match k {
        ValueKind::Text => OCELAttributeType::String,
        ValueKind::Integer => OCELAttributeType::Integer,
        ValueKind::Float => OCELAttributeType::Float,
        ValueKind::Boolean => OCELAttributeType::Boolean,
        ValueKind::Timestamp => OCELAttributeType::Time,
    }
}

fn kind_from_attr_type(t: OCELAttributeType) -> Option<ValueKind> {
    match t {
        OCELAttributeType::String => Some(ValueKind::Text),
        OCELAttributeType::Integer => Some(ValueKind::Integer),
        OCELAttributeType::Float => Some(ValueKind::Float),
        OCELAttributeType::Boolean => Some(ValueKind::Boolean),
        OCELAttributeType::Time => Some(ValueKind::Timestamp),
        OCELAttributeType::Null => None,
    }
}

/// Render `v` as `declared`, coercing when it does not already match.
///
/// A value that will not coerce becomes `Null`, counted in
/// [`MappingStats::uncoercible_attributes`]. Keeping `v`'s own rendering instead would let the two
/// sinks disagree on the same input, since a typed column in `DuckDB` stores `NULL` regardless.
///
/// An attribute declared with no type at all ([`OCELAttributeType::Null`]) has nothing to convert
/// to, so its value is stored as it comes.
fn attribute_value(
    v: Option<&Value>,
    declared: OCELAttributeType,
    stats: &mut MappingStats,
) -> OCELAttributeValue {
    let Some(v) = v else {
        return OCELAttributeValue::Null;
    };
    let Some(kind) = kind_from_attr_type(declared) else {
        return natural(v);
    };
    match v.coerce_to(kind) {
        Some(coerced) => natural(&coerced),
        None => {
            // A `Null` cell is an absent value, not one the declaration could not hold.
            if !matches!(v, Value::Null) {
                stats.uncoercible_attributes += 1;
            }
            OCELAttributeValue::Null
        }
    }
}

fn natural(v: &Value) -> OCELAttributeValue {
    match v {
        Value::Null => OCELAttributeValue::Null,
        Value::Text(s) => OCELAttributeValue::String(s.clone()),
        Value::Integer(i) => OCELAttributeValue::Integer(*i),
        Value::Float(f) => OCELAttributeValue::Float(*f),
        Value::Boolean(b) => OCELAttributeValue::Boolean(*b),
        Value::Timestamp(t) => OCELAttributeValue::Time(*t),
    }
}

fn render_id(id_rendering: IdRendering, raw: &str, type_name: &str) -> String {
    match id_rendering {
        IdRendering::Raw => raw.to_string(),
        IdRendering::TypePrefixed => format!("{type_name}-{raw}"),
    }
}

/// The ids one endpoint cell yields: the split parts, or the cell itself when there is no split.
///
/// An empty cell yields nothing, so the caller counts it as
/// [`DropReason::NullOrUnrenderableId`] and drops the row: `''` is how an ERP export writes "no
/// id", and accepting it collapses every such row into one entity.
fn split_or_single(split: Option<&PreparedSplit>, raw: String) -> Vec<String> {
    match split {
        Some(s) => s.split(&raw),
        None => {
            if raw.is_empty() {
                Vec::new()
            } else {
                vec![raw]
            }
        }
    }
}

/// An id expression's value, or `None` when it renders to nothing usable as an identity --
/// `Null`, an unrenderable value, or the empty string (see [`split_or_single`]).
fn identity(expr: &ValueExpression, row: &Row<'_>) -> Option<String> {
    expr.evaluate(row).filter(|s| !s.is_empty())
}

/// Why a timestamp yielded nothing: a value that would not parse, or no value to parse. The two
/// call for opposite fixes, so the report keeps them apart.
fn drop_reason_for(timestamp: &TimestampSource, row: &Row<'_>) -> DropReason {
    if timestamp.has_input(row) {
        DropReason::UnparseableTimestamp
    } else {
        DropReason::MissingTimestamp
    }
}
