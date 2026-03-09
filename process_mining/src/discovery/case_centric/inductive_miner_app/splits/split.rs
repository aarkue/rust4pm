use crate::core::process_models::process_tree::OperatorType;
use crate::EventLog;


/// Helper struct to aggregate the returns of splitting algorithms.
///
/// # Parameters
/// - 'operator' : ['ImOperator'] defining the split type
/// - 'sub_logs': a vector containing all new logs
pub struct Split{
    operator: OperatorType,
    sub_logs: Vec<EventLog>,
}

impl Split{
    pub fn new(operator: OperatorType, sub_logs: Vec<EventLog>) -> Split{
        Self{operator, sub_logs}
    }

    pub fn len(&self) -> usize {
        self.sub_logs.len()
    }

    pub fn get_own(self) -> Vec<EventLog>{
        self.sub_logs
    }

    pub fn get_operator(&self) -> OperatorType {
        self.operator
    }

    pub fn is_empty(&self) -> bool{
        self.sub_logs.is_empty()
    }
}