//! Synchronous product net used for efficiently computing alignments

use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    conformance::alignments::{
        cost::CostFunction,
        petri_net::{TokenCount, TracePos},
        AlignmentMove,
    },
    core::process_models::petri_net::{ArcType, PlaceID, TransitionID},
    PetriNet,
};

/// A transition in the synchronous product net
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SyncProdNetTransition {
    /// The move this transition represents (model-transition / trace-event indices)
    pub(crate) move_type: AlignmentMove,
    /// The pre-computed cost of firing this transition
    pub(crate) cost: u32,
    /// Incoming places (`place_index`, weight), i.e., which token to consume
    pub(crate) inputs: Vec<(usize, TokenCount)>,
    /// Outgoing places (`place_index`, weight), i.e., which token to produce
    pub(crate) outputs: Vec<(usize, TokenCount)>,
}

/// The synchronous product of a Petri net and a trace.
///
/// Only model places exist; the trace position is tracked additionally in the search
/// Transitions: [`model_moves` for model transitions, `log_moves`, `sync_moves`]
#[derive(Debug, PartialEq)]
pub(crate) struct SyncProductNet {
    /// Number of model places
    pub(crate) num_model_places: usize,
    /// Length of the trace
    pub(crate) trace_length: TracePos,
    /// All transitions in the sync product
    pub(crate) transitions: Vec<SyncProdNetTransition>,
    /// Initial marking (tokens per place)
    pub(crate) initial_marking: Vec<TokenCount>,
    /// Final marking (tokens per place)
    pub(crate) final_marking: Vec<TokenCount>,
    /// Log/sync transition indices grouped by trace position.
    /// `transitions_by_trace_pos[r]` holds the log/sync transitions for event `r`
    /// (fireable once r trace events have been advanced).
    pub(crate) transitions_by_trace_pos: Vec<Vec<usize>>,
    /// Number of model/silent transitions (fireable at any rank); they occupy indices `0..n`.
    pub(crate) num_model_trans: usize,
    /// Largest cost over all transitions (precomputed for the search's bucket sizing)
    pub(crate) max_edge_cost: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
/// Error when constructing the sync product net
pub enum SyncProdNetConstructionError {
    /// A unknown place id was referenced in a marking
    InvalidPlaceInMarking(PlaceID),
    /// No final marking found
    NoFinalMarking,
    /// No initial marking found
    NoInitialMarking,
}
impl std::fmt::Display for SyncProdNetConstructionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for SyncProdNetConstructionError {}

impl SyncProductNet {
    /// Construct the sync product net from a Petri net and trace
    pub(crate) fn construct(
        net: &PetriNet,
        trace: &[&str],
        cost_fn: &CostFunction,
    ) -> Result<Self, SyncProdNetConstructionError> {
        let mut transitions = Vec::new();
        let model_place_map: HashMap<&Uuid, usize> = net
            .places
            .iter()
            .enumerate()
            .map(|(i, p)| (p.0, i))
            .collect();
        // All sync-net places are model places
        let num_model_places = net.places.len();
        let mut model_trans_map: HashMap<&Uuid, usize> = HashMap::new();
        let mut model_trans_with_label: HashMap<&str, Vec<usize>> = HashMap::new();
        let num_model_trans = net.transitions.len();
        let mut transitions_by_rank = vec![vec![]; trace.len()];
        // Model moves
        for (trans_id, trans) in &net.transitions {
            let model_idx = transitions.len();
            model_trans_map.insert(trans_id, model_idx);
            if let Some(label) = &trans.label {
                model_trans_with_label
                    .entry(label)
                    .or_default()
                    .push(model_idx);
            }
            let inputs = net
                .arcs
                .iter()
                .filter_map(|arc| match arc.from_to {
                    ArcType::PlaceTransition(x, to) if &to == trans_id => {
                        Some((*model_place_map.get(&x)?, arc.weight as TokenCount))
                    }
                    _ => None,
                })
                .collect();
            let outputs = net
                .arcs
                .iter()
                .filter_map(|arc| match arc.from_to {
                    ArcType::TransitionPlace(from, x) if &from == trans_id => {
                        Some((*model_place_map.get(&x)?, arc.weight as TokenCount))
                    }
                    _ => None,
                })
                .collect();
            transitions.push(SyncProdNetTransition {
                move_type: AlignmentMove::ModelMove {
                    transition: TransitionID(*trans_id),
                },
                cost: if trans.label.is_none() {
                    cost_fn.silent_move_cost
                } else {
                    cost_fn.model_move_cost
                },
                inputs,
                outputs,
            });
        }
        // Log + Sync moves
        // There are no log places, instead the trace position is tracked by the
        // searcher (using trace_pos) A log move has no model effect; sync move uses the model
        // transition's preset/postset.
        for (index, activity) in trace.iter().enumerate() {
            transitions_by_rank[index].push(transitions.len());

            // Log moves
            transitions.push(SyncProdNetTransition {
                move_type: AlignmentMove::LogMove {
                    trace_event_index: index,
                },
                cost: cost_fn.log_move_cost,
                inputs: vec![],
                outputs: vec![],
            });
            // Sync moves
            let model_trans_that_can_sync = model_trans_with_label.get(activity);
            for s in model_trans_that_can_sync.into_iter().flatten() {
                transitions_by_rank[index].push(transitions.len());
                let trans_id = match transitions[*s].move_type {
                    AlignmentMove::ModelMove { transition } => transition,
                    _ => unreachable!("Has to be a model move"),
                };
                transitions.push(SyncProdNetTransition {
                    move_type: AlignmentMove::SyncMove {
                        transition: trans_id,
                        trace_event_index: index,
                    },
                    cost: cost_fn.sync_move_cost,
                    inputs: transitions[*s].inputs.clone(),
                    outputs: transitions[*s].outputs.clone(),
                });
            }
        }

        // Build initial marking
        let mut initial_marking: Vec<TokenCount> = vec![0; num_model_places];
        // Initial marking for model
        let im = net
            .initial_marking
            .as_ref()
            .ok_or(SyncProdNetConstructionError::NoInitialMarking)?;
        for (place_id, count) in im {
            let index = model_place_map.get(&place_id.0).ok_or(
                SyncProdNetConstructionError::InvalidPlaceInMarking(*place_id),
            )?;
            initial_marking[*index] = *count as TokenCount;
        }

        // Build final marking
        let mut final_marking: Vec<TokenCount> = vec![0; num_model_places];
        // Final marking for model
        let fm = net
            .final_markings
            .as_ref()
            .and_then(move |f| f.first())
            .ok_or(SyncProdNetConstructionError::NoFinalMarking)?;
        for (place_id, count) in fm {
            let index = model_place_map.get(&place_id.0).ok_or(
                SyncProdNetConstructionError::InvalidPlaceInMarking(*place_id),
            )?;
            final_marking[*index] = *count as TokenCount;
        }

        let max_edge_cost = transitions.iter().map(|t| t.cost).max().unwrap_or(1);
        Ok(Self {
            num_model_places,
            trace_length: trace.len() as TracePos,
            transitions,
            initial_marking,
            final_marking,
            transitions_by_trace_pos: transitions_by_rank,
            num_model_trans,
            max_edge_cost,
        })
    }
}
