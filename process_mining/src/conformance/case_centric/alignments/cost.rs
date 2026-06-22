//! Cost functions for alignments

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Cost function for alignment moves.
///
/// Defines the cost of model moves, log moves, synchronous moves, and silent moves.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CostFunction {
    /// Default cost for a model move (visible transition fires without matching log event)
    pub model_move_cost: u16,
    /// Default cost for a log move (log event not matched by model)
    pub log_move_cost: u16,
    /// Default cost for a synchronous move
    pub sync_move_cost: u16,
    /// Default cost for a silent/tau move
    pub silent_move_cost: u16,
}

impl CostFunction {
    /// Standard cost function: model and log moves cost 1, sync and silent moves cost 0.
    pub fn standard() -> Self {
        Self {
            model_move_cost: 1,
            log_move_cost: 1,
            sync_move_cost: 0,
            silent_move_cost: 0,
        }
    }
}
