//! The [`Blueprint`] a container's manifest denotes.
//!
//! The bundled format is relational, so importing it is an extraction rather than a parser: one
//! `Source` node per declared table, one mapping per thing that table produces. That reuses the
//! rest of the extraction subsystem, including validation against the discovered schema, a
//! per-mapping [`ExtractionReport`](super::super::extraction::ExtractionReport), and the SQL
//! compiler, which turns a Parquet container into `DuckDB` views that never materialise the log.
//! A CSV container cannot be compiled, since every column is text and the timestamps and numeric
//! attributes need the row executor's parsing.

use super::meta::{
    columns, event_table, object_changes_table, object_table, AttributeDecl, BundleMeta, E2O_TABLE,
    O2O_TABLE,
};
use crate::core::event_data::object_centric::extraction::{
    AttributeMapping, Blueprint, CompareOp, DuplicateObjectPolicy, EventEndpoint, IdRendering,
    Literal, Mapping, MappingEntry, MissingEndpointPolicy, Node, NodeOp, ObjectEndpoint, Operand,
    Predicate, Target, TimestampSource, ValueExpression, MODEL_VERSION,
};

/// The source id a generated blueprint reads from.
pub const SOURCE_ID: &str = "bundle";

/// The blueprint that turns `meta`'s tables into the OCEL they encode.
///
/// # Why the policies are what they are
///
/// - [`IdRendering::Raw`]: OCEL ids are globally unique already, and `e2o`/`o2o` name an id with
///   no type column beside it, so there is nothing to prefix with.
/// - [`DuplicateObjectPolicy::FirstWins`]: an object appears once in its own table and again in
///   every change row, which is not a collision but the format working as designed. First-wins
///   creates it once and appends the later rows' attribute values at their own timestamps.
/// - [`MissingEndpointPolicy::Error`]: a relation naming an id no table declares is a broken
///   container, and should be reported rather than papered over by synthesising the object.
#[must_use]
pub fn blueprint_for(meta: &BundleMeta) -> Blueprint {
    let mut nodes = Vec::new();
    let mut mappings = Vec::new();

    for (ty, decl) in &meta.event_types {
        let table = event_table(ty);
        nodes.push(source(&table));
        mappings.push(single(
            &table,
            Target::Event {
                event_type: constant(ty),
                id: Some(column(columns::ID)),
                timestamp: TimestampSource::column(columns::TIME),
                attributes: attributes(&decl.attributes),
                objects: Vec::new(),
            },
        ));
    }

    for (ty, decl) in &meta.object_types {
        let table = object_table(ty);
        nodes.push(source(&table));
        // No timestamp: the format reads an object table's values as initial ones, held from the
        // Unix epoch, which is exactly what a `None` timestamp records.
        mappings.push(single(
            &table,
            Target::Object {
                object_type: constant(ty),
                id: column(columns::ID),
                timestamp: None,
                attributes: attributes(&decl.attributes),
            },
        ));

        if decl.changes_file.is_none() {
            continue;
        }
        let changes = object_changes_table(ty);
        nodes.push(source(&changes));
        // One filtered node per attribute, rather than one mapping carrying every attribute
        // column. A change row fills only the column `ocel_changed_field` names and leaves the
        // rest empty, so a single mapping would record those empty cells as an attribute being
        // set to null at that instant, a change the container never declared.
        for attr in &decl.attributes {
            let filtered = format!("{changes}#{}", attr.name);
            nodes.push(Node {
                id: filtered.clone(),
                label: None,
                op: NodeOp::Filter {
                    input: changes.clone(),
                    condition: Predicate::Compare {
                        left: Operand::Column {
                            column: columns::CHANGED_FIELD.to_string(),
                        },
                        op: CompareOp::Eq,
                        right: Operand::Literal {
                            value: Literal::Text(attr.name.clone()),
                        },
                    },
                },
            });
            mappings.push(single(
                &filtered,
                Target::Object {
                    object_type: constant(ty),
                    id: column(columns::ID),
                    timestamp: Some(TimestampSource::column(columns::TIME)),
                    attributes: attributes(std::slice::from_ref(attr)),
                },
            ));
        }
    }

    nodes.push(source(E2O_TABLE));
    mappings.push(single(
        E2O_TABLE,
        Target::E2O {
            event: EventEndpoint {
                id: column(columns::EVENT_ID),
                event_type: None,
            },
            object: endpoint(columns::OBJECT_ID),
            qualifier: Some(column(columns::QUALIFIER)),
        },
    ));

    nodes.push(source(O2O_TABLE));
    mappings.push(single(
        O2O_TABLE,
        Target::O2O {
            source: endpoint(columns::SOURCE_ID),
            target: endpoint(columns::TARGET_ID),
            qualifier: Some(column(columns::QUALIFIER)),
        },
    ));

    Blueprint {
        version: MODEL_VERSION,
        id_rendering: IdRendering::Raw,
        nodes,
        mappings,
        on_missing_endpoint: MissingEndpointPolicy::Error,
        on_duplicate_object: DuplicateObjectPolicy::FirstWins,
    }
}

fn source(table: &str) -> Node {
    Node {
        id: table.to_string(),
        label: None,
        op: NodeOp::Source {
            source_id: SOURCE_ID.to_string(),
            table: table.to_string(),
        },
    }
}

fn single(node: &str, target: Target) -> MappingEntry {
    MappingEntry::Single(Mapping {
        node: node.to_string(),
        label: None,
        when: None,
        target,
    })
}

fn column(name: &str) -> ValueExpression {
    ValueExpression::Column {
        column: name.to_string(),
    }
}

fn constant(value: &str) -> ValueExpression {
    ValueExpression::Constant {
        value: value.to_string(),
    }
}

/// A relation endpoint. No `object_type`: the relation tables carry ids only, which is legal
/// exactly because ids are rendered raw.
fn endpoint(id_column: &str) -> ObjectEndpoint {
    ObjectEndpoint {
        id: column(id_column),
        object_type: None,
        split: None,
    }
}

fn attributes(decls: &[AttributeDecl]) -> Vec<AttributeMapping> {
    decls
        .iter()
        .map(|a| AttributeMapping {
            source_column: a.name.clone(),
            name: a.name.clone(),
            value_type: Some(a.value_type.into()),
        })
        .collect()
}
