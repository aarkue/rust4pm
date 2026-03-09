use std::mem::discriminant;
use crate::core::process_models::process_tree::Node;
use crate::discovery::case_centric::inductive_miner_app::splits::split::Split;
use crate::EventLog;

/// Represents the result of attempting to apply a fall-through rule.
///
/// Each variant corresponds to a specific fall-through strategy and
/// contains the resulting [`Node`], i.e. Operator-type and children if any, together with the
/// event log(s) derived during its application.
///
/// If no fall-through rule is applicable, the `Return` variant is used.
/// In this case, the original event log is returned unchanged.
///
/// Not to be confused with [`FallThroughLabel`]
pub enum Fallthrough {
    EmptyTraces(Node, EventLog),
    ActivityOncePerTrace(Node, EventLog),
    ActivityConcurrent(Node, EventLog, Split),
    StrictTauLoop(Node, EventLog),
    TauLoop(Node, EventLog),
    FlowerModel(Node),
    Return(EventLog),
}

impl Fallthrough {

    pub fn same_enum_variant(&self, other: &Self) -> bool {
        discriminant(self) == discriminant(other)
    }
}






