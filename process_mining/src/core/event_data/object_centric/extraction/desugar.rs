//! Rewrite surface sugar into the plain mappings everything downstream works with.

use super::blueprint::{Blueprint, Mapping, MappingEntry};
use super::predicate::Predicate;

/// Flatten a blueprint's mappings, rewriting ordered groups into independent mappings.
///
/// A group's `k`th mapping keeps its own guard and gains a negation of every earlier guard, so
/// first-match-wins holds without the evaluator needing to know about ordering. A member with no
/// guard of its own becomes the conjunction of those negations alone, which is the catch-all, and
/// ends the group: nothing written after a catch-all can match a row it left over.
#[must_use]
pub fn desugar(blueprint: &Blueprint) -> Vec<Mapping> {
    desugar_with_paths(blueprint)
        .into_iter()
        .map(|(_, m)| m)
        .collect()
}

/// Like [`desugar`], but paired with the JSON path of the authored entry each output mapping
/// came from, e.g. `mappings[1]` for a top-level `Single` or `mappings[0].mappings[2]` for the
/// third member of the first `Ordered` group. A diagnostic can then point back at what the author
/// wrote instead of a position in the flattened list.
pub(crate) fn desugar_with_paths(blueprint: &Blueprint) -> Vec<(String, Mapping)> {
    let mut out = Vec::with_capacity(blueprint.mappings.len());
    for (i, entry) in blueprint.mappings.iter().enumerate() {
        match entry {
            MappingEntry::Single(m) => out.push((format!("mappings[{i}]"), m.clone())),
            MappingEntry::Ordered { mappings } => {
                let mut earlier: Vec<Predicate> = Vec::new();
                for (j, m) in mappings.iter().enumerate() {
                    let mut conditions: Vec<Predicate> = Vec::with_capacity(earlier.len() + 1);
                    conditions.extend(m.when.clone());
                    conditions.extend(earlier.iter().map(|p| Predicate::Not {
                        condition: Box::new(p.clone()),
                    }));
                    let when = match conditions.len() {
                        0 => None,
                        1 => conditions.pop(),
                        _ => Some(Predicate::And { conditions }),
                    };
                    out.push((
                        format!("mappings[{i}].mappings[{j}]"),
                        Mapping { when, ..m.clone() },
                    ));
                    let Some(own) = &m.when else {
                        // An unguarded member matches every row the earlier ones did not, so no
                        // later member can ever fire.
                        break;
                    };
                    earlier.push(own.clone());
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event_data::object_centric::extraction::blueprint::*;
    use crate::core::event_data::object_centric::extraction::expr::*;
    use crate::core::event_data::object_centric::extraction::predicate::*;
    use crate::core::event_data::object_centric::extraction::row::with_row;
    use crate::core::event_data::object_centric::extraction::value::Value;

    fn eq(column: &str, value: &str) -> Predicate {
        Predicate::Compare {
            left: Operand::Column {
                column: column.into(),
            },
            op: CompareOp::Eq,
            right: Operand::Literal {
                value: Literal::Text(value.into()),
            },
        }
    }

    fn mapping(label: &str, when: Option<Predicate>) -> Mapping {
        Mapping {
            node: "n".into(),
            label: Some(label.into()),
            when,
            target: Target::Object {
                object_type: ValueExpression::Constant {
                    value: label.into(),
                },
                id: ValueExpression::Column {
                    column: "id".into(),
                },
                timestamp: None,
                attributes: vec![],
            },
        }
    }

    fn blueprint(mappings: Vec<MappingEntry>) -> Blueprint {
        Blueprint {
            version: 1,
            id_rendering: IdRendering::Raw,
            nodes: vec![],
            mappings,
            on_missing_endpoint: MissingEndpointPolicy::Drop,
            on_duplicate_object: DuplicateObjectPolicy::FirstWins,
        }
    }

    #[test]
    fn single_mappings_pass_through_untouched() {
        let bp = blueprint(vec![MappingEntry::Single(mapping("a", Some(eq("s", "x"))))]);
        let out = desugar(&bp);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].when, Some(eq("s", "x")));
    }

    #[test]
    fn ordered_mappings_become_mutually_exclusive_guards() {
        let bp = blueprint(vec![MappingEntry::Ordered {
            mappings: vec![
                mapping("completed", Some(eq("new", "C"))),
                mapping("changed", None),
            ],
        }]);
        let out = desugar(&bp);
        assert_eq!(out.len(), 2);

        // Row where the first rule matches: only the first mapping fires.
        let first = out[0].when.clone().unwrap().prepare(None).unwrap();
        let second = out[1].when.clone().unwrap().prepare(None).unwrap();
        with_row(&[("new", Value::Text("C".into()))], |row| {
            assert!(first.evaluate(row));
            assert!(
                !second.evaluate(row),
                "second must not fire when the first matched"
            );
        });
        // Row where it does not: the unguarded second mapping takes over.
        with_row(&[("new", Value::Text("B".into()))], |row| {
            assert!(!first.evaluate(row));
            assert!(second.evaluate(row));
        });
    }

    #[test]
    fn a_third_ordered_mapping_excludes_both_predecessors() {
        let bp = blueprint(vec![MappingEntry::Ordered {
            mappings: vec![
                mapping("a", Some(eq("s", "1"))),
                mapping("b", Some(eq("s", "2"))),
                mapping("c", None),
            ],
        }]);
        let out = desugar(&bp);
        let third = out[2].when.clone().unwrap().prepare(None).unwrap();
        for v in ["1", "2"] {
            with_row(&[("s", Value::Text(v.into()))], |row| {
                assert!(!third.evaluate(row))
            });
        }
        with_row(&[("s", Value::Text("3".into()))], |row| {
            assert!(third.evaluate(row))
        });
    }

    #[test]
    fn a_group_ends_at_its_catch_all() {
        let bp = blueprint(vec![MappingEntry::Ordered {
            mappings: vec![
                mapping("a", Some(eq("s", "1"))),
                mapping("catch-all", None),
                mapping("unreachable", Some(eq("s", "2"))),
            ],
        }]);
        let out = desugar(&bp);
        assert_eq!(
            out.iter().map(|m| m.label.clone()).collect::<Vec<_>>(),
            vec![Some("a".to_string()), Some("catch-all".to_string())]
        );
    }

    #[test]
    fn paths_point_back_at_the_authored_entry_not_the_flattened_position() {
        // With mappings = [Ordered{3 members}, Single], the Single is at flattened index 3 but
        // its authored JSON path is mappings[1].
        let bp = blueprint(vec![
            MappingEntry::Ordered {
                mappings: vec![
                    mapping("a", Some(eq("s", "1"))),
                    mapping("b", Some(eq("s", "2"))),
                    mapping("c", None),
                ],
            },
            MappingEntry::Single(mapping("d", None)),
        ]);
        let out = desugar_with_paths(&bp);
        let paths: Vec<&str> = out.iter().map(|(p, _)| p.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "mappings[0].mappings[0]",
                "mappings[0].mappings[1]",
                "mappings[0].mappings[2]",
                "mappings[1]",
            ]
        );
    }
}
