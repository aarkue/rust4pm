//! Token-based Replay on Petri Nets
#[cfg(feature = "token-based-replay")]
use macros_process_mining::register_binding;
#[cfg(feature = "token-based-replay")]
use nalgebra::DVector;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::process_models::case_centric::petri_net::PlaceID;

#[cfg(feature = "token-based-replay")]
use crate::core::{
    event_data::case_centric::utils::activity_projection::EventLogActivityProjection,
    process_models::case_centric::petri_net::{AcceptingPetriNet, Marking},
};

///
/// Errors than can occur for the input of the token-based replay algorithm
///
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub enum TokenBasedReplayError {
    /// Error if Petri net contains duplicate labels or a silent transition
    DuplicateLabelOrSilentTransitionError,
    /// Error if a marking refers to a place that is not in the net
    InvalidPlaceInMarking(PlaceID),
}

impl std::fmt::Display for TokenBasedReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenBasedReplayError::DuplicateLabelOrSilentTransitionError => {
                write!(
                    f,
                    "Petri net contains duplicate labels or silent transitions"
                )
            }
            TokenBasedReplayError::InvalidPlaceInMarking(place) => {
                write!(
                    f,
                    "A marking refers to place {}, which is not in the net",
                    place.index()
                )
            }
        }
    }
}

///
/// Result from the token-based replay computation
///
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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
#[cfg(feature = "token-based-replay")]
#[register_binding]
pub fn apply_token_based_replay(
    petri_net: &AcceptingPetriNet,
    event_log: &EventLogActivityProjection,
) -> Result<TokenBasedReplayResult, TokenBasedReplayError> {
    let net = &petri_net.net;
    if net.contains_duplicate_or_silent_transitions() {
        return Err(TokenBasedReplayError::DuplicateLabelOrSilentTransitionError);
    }
    if let Some(place) = [&petri_net.initial_marking, &petri_net.final_marking]
        .into_iter()
        .find_map(|marking| net.place_outside(marking))
    {
        return Err(TokenBasedReplayError::InvalidPlaceInMarking(place));
    }

    let mut result = TokenBasedReplayResult::new();

    let pre_matrix = net.create_pre_incidence_matrix();
    let post_matrix = net.create_post_incidence_matrix();

    let pos_array: Vec<Option<usize>> = event_log
        .activities
        .iter()
        .map(|activity| {
            net.transitions()
                .find(|(_, t)| t.label.as_ref() == Some(activity))
                .map(|(id, _)| id.index())
        })
        .collect();

    let m_init = marking_to_vector(&petri_net.initial_marking, net.num_places());
    let m_final = marking_to_vector(&petri_net.final_marking, net.num_places());

    event_log.traces.iter().for_each(|(trace, freq)| {
        result.produced += m_init.sum() as u64 * *freq;
        result.consumed += m_final.sum() as u64 * *freq;

        let mut marking: DVector<i64> = DVector::zeros(net.num_places());
        marking += &m_init;

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

        marking -= &m_final;
        result.missing += count_missing(&mut marking) * freq;
        result.remaining += marking.sum() as u64 * *freq;
    });

    Ok(result)
}

///
/// Changes the [`Marking`] object into a [`DVector<i64>`]
///
/// Panics if a place of the marking is not below `vector_len`.
pub fn marking_to_vector(marking: &Marking, vector_len: usize) -> DVector<i64> {
    let mut result: DVector<i64> = DVector::zeros(vector_len);
    for (place, count) in marking.iter() {
        result[place.index()] += i64::from(count);
    }
    result
}

///
/// Counts all missing tokens and resets the token counts of places with a negative token count
///
pub fn count_missing(marking: &mut DVector<i64>) -> u64 {
    let mut result = 0;

    marking.iter_mut().for_each(|place_tokens| {
        if *place_tokens < 0 {
            result += place_tokens.unsigned_abs();
            *place_tokens = 0;
        }
    });

    result
}

#[cfg(test)]
mod tests {
    use crate::core::{
        event_data::case_centric::{Event, Trace},
        process_models::case_centric::petri_net::{ArcType, PetriNet, PlaceID},
        EventLog,
    };

    use super::*;

    #[test]
    fn foreign_marking_places_are_an_error() {
        let mut net = PetriNet::new();
        let p = net.add_place(None);
        let t = net.add_transition(Some("a".into()), None);
        net.add_arc(ArcType::place_to_transition(p, t), None);
        let foreign = PlaceID(99);
        let net = AcceptingPetriNet::new(net, Marking::from([(foreign, 1)]), Marking::new());
        let log = EventLogActivityProjection::from(&EventLog::new());
        assert!(matches!(
            apply_token_based_replay(&net, &log),
            Err(TokenBasedReplayError::InvalidPlaceInMarking(place)) if place == foreign
        ));
    }

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

        let net = AcceptingPetriNet::new(net, Marking::from([(p1, 1)]), Marking::from([(p3, 1)]));

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

        println!("After replaying trace 1, the result is: {tbr_result:?}");
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

        println!("After replaying trace 1 and trace 2, the result is: {tbr_result_2:?}");
        assert!(tbr_result_2.is_ok());
        let result_2 = tbr_result_2.unwrap();
        assert_eq!(result_2.produced, 6 + 5);
        assert_eq!(result_2.consumed, 6 + 5);
        assert_eq!(result_2.missing, 2 + 3);
        assert_eq!(result_2.remaining, 2 + 3);
    }
}
