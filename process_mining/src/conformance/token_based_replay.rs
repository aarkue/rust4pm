use crate::petri_net::petri_net_struct::Marking;
use crate::{EventLogActivityProjection, PetriNet};
use nalgebra::{DMatrix, DVector};
use std::collections::HashMap;
use uuid::Uuid;

///
/// Errors than can occur for the input of the token-based replay algorithm
///
#[derive(Debug, Clone)]
pub enum TokenBasedReplayError {
    /// Error if no initial marking is provided
    NoInitialMarking,
    /// Error if the no final marking is provided
    NoFinalMarking,
    /// Error if there are too many final markings are provided
    TooManyFinalMarkings,
    /// Error if Petri net contains duplicate labels or a silent transition
    DuplicateLabelOrSilentTransitionError,
}

impl std::fmt::Display for TokenBasedReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenBasedReplayError::NoInitialMarking => {
                write!(f, "No initial marking")
            }
            TokenBasedReplayError::NoFinalMarking => {
                write!(f, "No final marking")
            }
            TokenBasedReplayError::TooManyFinalMarkings => {
                write!(f, "Too many final markings")
            }
            TokenBasedReplayError::DuplicateLabelOrSilentTransitionError => {
                write!(
                    f,
                    "Petri net contains duplicate labels or silent transitions"
                )
            }
        }
    }
}

///
/// Result from the token-based replay computation
///
#[derive(Debug, Clone, Default)]
pub struct TokenBasedReplayResult {
    /// Produced tokens during token-based replay
    pub produced: u64,
    /// Consumed tokens during token-based replay
    pub consumed: u64,
    /// Missing tokens during token-based replay
    pub missing: u64,
    /// Remaining tokens during token-based replay
    pub remaining: u64,
}

impl TokenBasedReplayResult {
    /// Initializes a [`TokenBasedReplayResult`]
    pub fn new() -> TokenBasedReplayResult {
        Self::default()
    }

    /// Computes the fitness from the produced, consumed, missing, and remaining tokens
    pub fn compute_fitness(&self) -> f64 {
        0.5 * (1.0 - (self.missing as f64 / self.consumed as f64))
            + 0.5 * (1.0 - (self.remaining as f64 / self.produced as f64))
    }
}

///
/// Computes token-based replay for a Petri net that has unique labels and no silent transitions
///
#[cfg(feature = "token_based_replay")]
pub fn apply_token_based_replay(
    petri_net: &PetriNet,
    event_log: &EventLogActivityProjection,
) -> Result<TokenBasedReplayResult, TokenBasedReplayError> {
    if petri_net.initial_marking.is_none() {
        return Err(TokenBasedReplayError::NoInitialMarking);
    } else if petri_net.final_markings.as_ref().is_none()
        || petri_net.final_markings.as_ref().unwrap().len() == 0
    {
        return Err(TokenBasedReplayError::NoFinalMarking);
    } else if petri_net.final_markings.as_ref().unwrap().len() > 1 {
        return Err(TokenBasedReplayError::TooManyFinalMarkings);
    } else if petri_net.contains_duplicate_or_silent_transitions() {
        return Err(TokenBasedReplayError::DuplicateLabelOrSilentTransitionError);
    }

    let mut result = TokenBasedReplayResult::new();

    let node_to_pos = petri_net.create_vector_dictionary();

    let pre_matrix =
        change_matrix_type_to_i64(&petri_net.create_pre_incidence_matrix(&node_to_pos));
    let post_matrix =
        change_matrix_type_to_i64(&petri_net.create_post_incidence_matrix(&node_to_pos));

    let pos_array: Vec<Option<usize>> = event_log
        .activities
        .iter()
        .map(|activity| {
            let pos;

            if let Some((transition_id, _)) =
                petri_net.transitions.iter().find(|(_, transition)| {
                    transition
                        .label
                        .as_ref()
                        .is_some_and(|label| label.eq(activity))
                })
            {
                pos = Some(*node_to_pos.get(transition_id).unwrap());
            } else {
                pos = None;
            };
            pos
        })
        .collect();

    let m_init = marking_to_vector(
        petri_net.initial_marking.as_ref().unwrap(),
        &node_to_pos,
        petri_net.places.len(),
    );

    let m_final = marking_to_vector(
        petri_net.final_markings.as_ref().unwrap().get(0).unwrap(),
        &node_to_pos,
        petri_net.places.len(),
    );

    event_log.traces.iter().for_each(|(trace, freq)| {
        result.produced += m_init.sum() as u64 * *freq;
        result.consumed += m_final.sum() as u64 * *freq;

        let mut marking: DVector<i64> = DVector::zeros(petri_net.places.len());
        marking += &m_init * (*freq as i64);

        trace.iter().for_each(|event| {
            let pos_option: &Option<usize> = pos_array.get(*event).unwrap();
            if pos_option.is_some() {
                let pos = pos_option.unwrap();

                let t_in = pre_matrix.column(pos);
                let t_out = post_matrix.column(pos);

                marking -= t_in;
                result.consumed += t_in.sum() as u64 * *freq;

                result.missing += count_missing(&mut marking) * freq;

                marking += t_out;
                result.produced += t_out.sum() as u64 * *freq;
            }
        });

        marking -= &m_final * (*freq as i64);
        result.missing += count_missing(&mut marking) * freq;
        result.remaining += marking.sum() as u64 * *freq;
    });

    Ok(result)
}

///
/// Changes the [`DMatrix`]'s data type to be [`i64`] from [`u8`]
///
fn change_matrix_type_to_i64(input: &DMatrix<u8>) -> DMatrix<i64> {
    input.map(|e| e as i64)
}

///
/// Changes the [`Marking`] object into a [`DVector<i64>`]
///
pub fn marking_to_vector(
    marking: &Marking,
    node_to_pos: &HashMap<Uuid, usize>,
    vector_len: usize,
) -> DVector<i64> {
    let mut result: DVector<i64> = DVector::zeros(vector_len);

    marking.iter().for_each(|(place, count)| {
        result[*node_to_pos.get(&place.get_uuid()).unwrap()] += *count as i64;
    });

    result
}

///
/// Counts all missing tokens and resets the token counts of places with a negative token count
///
pub fn count_missing(marking: &mut DVector<i64>) -> u64 {
    let mut result = 0;

    marking.iter_mut().for_each(|place_tokens| {
        if *place_tokens < 0 {
            result += place_tokens.abs() as u64;
            *place_tokens = 0;
        }
    });

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_log::{Event, Trace};
    use crate::petri_net::petri_net_struct::ArcType;
    use crate::EventLog;

    #[test]
    fn token_based_replay_test() {
        let mut net = PetriNet::new();
        let p1 = net.add_place(None);
        let p2 = net.add_place(None);
        let p3 = net.add_place(None);
        let t1 = net.add_transition(Some("a".into()), None);
        let t2 = net.add_transition(Some("b".into()), None);
        let t3 = net.add_transition(Some("c".into()), None);
        let t4 = net.add_transition(Some("d".into()), None);
        net.add_arc(ArcType::place_to_transition(p1, t1), None);
        net.add_arc(ArcType::place_to_transition(p1, t2), None);
        net.add_arc(ArcType::transition_to_place(t1, p2), None);
        net.add_arc(ArcType::transition_to_place(t2, p2), None);
        net.add_arc(ArcType::place_to_transition(p2, t3), None);
        net.add_arc(ArcType::transition_to_place(t3, p3), None);
        net.add_arc(ArcType::transition_to_place(t4, p2), None);
        net.add_arc(ArcType::place_to_transition(p2, t4), None);

        let mut initial_marking = Marking::new();
        initial_marking.insert(p1, 1);
        net.initial_marking = Some(initial_marking);

        let mut final_marking = Marking::new();
        final_marking.insert(p3, 1);
        let mut final_markings = Vec::new();
        final_markings.push(final_marking);
        net.final_markings = Some(final_markings);

        let mut trace_1 = Trace::new();
        trace_1.events.push(Event::new("a".to_string()));
        trace_1.events.push(Event::new("b".to_string()));
        trace_1.events.push(Event::new("c".to_string()));
        trace_1.events.push(Event::new("c".to_string()));
        trace_1.events.push(Event::new("d".to_string()));

        let mut event_log = EventLog::new();
        event_log.traces.push(trace_1);

        let event_log_abstraction = EventLogActivityProjection::from(&event_log);
        let tbr_result = apply_token_based_replay(&net, &event_log_abstraction);

        println!("After replaying trace 1, the result is: {:?}", tbr_result);
        assert!(tbr_result.is_ok());
        let result = tbr_result.unwrap();
        assert_eq!(result.produced, 6);
        assert_eq!(result.consumed, 6);
        assert_eq!(result.missing, 2);
        assert_eq!(result.remaining, 2);

        let mut trace_2 = Trace::new();
        trace_2.events.push(Event::new("b".to_string()));
        trace_2.events.push(Event::new("b".to_string()));
        trace_2.events.push(Event::new("d".to_string()));
        trace_2.events.push(Event::new("b".to_string()));

        event_log.traces.push(trace_2);

        let event_log_abstraction_2 = EventLogActivityProjection::from(&event_log);
        let tbr_result_2 = apply_token_based_replay(&net, &event_log_abstraction_2);

        println!(
            "After replaying trace 1 and trace 2, the result is: {:?}",
            tbr_result_2
        );
        assert!(tbr_result_2.is_ok());
        let result_2 = tbr_result_2.unwrap();
        assert_eq!(result_2.produced, 6 + 5);
        assert_eq!(result_2.consumed, 6 + 5);
        assert_eq!(result_2.missing, 2 + 3);
        assert_eq!(result_2.remaining, 2 + 3);
    }
}
